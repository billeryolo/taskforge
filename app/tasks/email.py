"""Email delivery with exponential backoff, and campaign fan-out.

``emails.send`` retries on SMTP/network errors with ``retry_backoff`` (1s, 2s, 4s… capped,
with jitter so a burst of failures doesn't retry in lock-step). After ``max_retries`` the
task fails for good and the base class dead-letters it.

A campaign is a ``group`` of independent ``emails.send`` tasks. Instead of a chord (whose
callback would never fire if a single recipient fails permanently), every send — on success
*or* final failure — checks whether it was the last outstanding recipient and closes the
campaign. This is idempotent and safe under at-least-once delivery.
"""

import smtplib
from datetime import UTC, datetime

from celery import group
from sqlalchemy import func, select, update

from app import storage
from app.celery_app import celery_app
from app.config import get_settings
from app.db import session_scope
from app.logging import get_logger
from app.mail import Mail, get_mailer
from app.models import Campaign, CampaignRecipient, CampaignStatus
from app.tasks.base import TrackedTask

log = get_logger(__name__)


def _maybe_finish_campaign(campaign_id: str) -> None:
    with session_scope() as session:
        outstanding = session.scalar(
            select(func.count(CampaignRecipient.id)).where(
                CampaignRecipient.campaign_id == campaign_id,
                CampaignRecipient.sent_at.is_(None),
                CampaignRecipient.error.is_(None),
            )
        )
        if outstanding == 0:
            session.execute(
                update(Campaign)
                .where(Campaign.id == campaign_id, Campaign.status != CampaignStatus.DONE)
                .values(status=CampaignStatus.DONE, finished_at=datetime.now(UTC))
            )
            log.info("campaign_finished", campaign_id=campaign_id)


class CampaignEmailTask(TrackedTask):
    abstract = True

    def after_dead_letter(self, exc: Exception, kwargs: dict) -> None:
        recipient_id = kwargs.get("recipient_id")
        campaign_id = kwargs.get("campaign_id")
        if not (recipient_id and campaign_id):
            return
        with session_scope() as session:
            session.execute(
                update(CampaignRecipient)
                .where(CampaignRecipient.id == recipient_id, CampaignRecipient.error.is_(None))
                .values(error=f"{type(exc).__name__}: {exc}")
            )
            session.execute(
                update(Campaign)
                .where(Campaign.id == campaign_id)
                .values(failed_count=Campaign.failed_count + 1)
            )
        _maybe_finish_campaign(campaign_id)


@celery_app.task(
    base=CampaignEmailTask,
    bind=True,
    name="emails.send",
    autoretry_for=(smtplib.SMTPException, ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
    rate_limit=get_settings().email_rate_limit or None,
    soft_time_limit=30,
    time_limit=45,
)
def send_email(
    self: CampaignEmailTask,
    *,
    to: str,
    subject: str,
    body: str,
    attachment: str | None = None,
    job_id: str | None = None,
    campaign_id: str | None = None,
    recipient_id: str | None = None,
) -> dict:
    if recipient_id:
        # At-least-once delivery: if this message was already sent (worker died after SMTP
        # but before ack) do not send a duplicate.
        with session_scope() as session:
            already = session.scalar(
                select(CampaignRecipient.sent_at).where(CampaignRecipient.id == recipient_id)
            )
        if already is not None:
            log.info("email_already_sent", recipient_id=recipient_id)
            return {"to": to, "skipped": True}

    attachments = [storage.resolve(attachment)] if attachment else []
    get_mailer().send(Mail(to=to, subject=subject, body=body, attachments=attachments))
    log.info("email_sent", to=to, subject=subject, attempt=self.request.retries + 1)

    if recipient_id and campaign_id:
        with session_scope() as session:
            session.execute(
                update(CampaignRecipient)
                .where(CampaignRecipient.id == recipient_id)
                .values(sent_at=datetime.now(UTC))
            )
            session.execute(
                update(Campaign)
                .where(Campaign.id == campaign_id)
                .values(sent_count=Campaign.sent_count + 1)
            )
        _maybe_finish_campaign(campaign_id)
    return {"to": to, "attempts": self.request.retries + 1}


@celery_app.task(base=TrackedTask, bind=True, name="emails.send_campaign")
def send_campaign(self: TrackedTask, *, campaign_id: str, job_id: str | None = None) -> dict:
    with session_scope() as session:
        campaign = session.get(Campaign, campaign_id)
        if campaign is None:
            raise ValueError(f"campaign {campaign_id} not found")
        campaign.status = CampaignStatus.SENDING
        pending = [
            (str(r.id), r.email)
            for r in campaign.recipients
            if r.sent_at is None and r.error is None
        ]
        subject, body = campaign.subject, campaign.body

    if not pending:
        _maybe_finish_campaign(campaign_id)
        return {"campaign_id": campaign_id, "fanned_out": 0}

    group(
        send_email.s(
            to=email,
            subject=subject,
            body=body,
            campaign_id=campaign_id,
            recipient_id=recipient_id,
        )
        for recipient_id, email in pending
    ).apply_async()
    log.info("campaign_fanned_out", campaign_id=campaign_id, recipients=len(pending))
    return {"campaign_id": campaign_id, "fanned_out": len(pending)}
