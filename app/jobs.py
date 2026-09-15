"""Job registry: the single place where the API (or Beat) turns a *kind* of work into a Celery
message with a tracked ``jobs`` row.

Ordering matters: the row is committed **before** the message is published. If we published
first, a fast worker could pick the task up and try to update a row that is not visible yet.
"""

import uuid
from typing import Any

from sqlalchemy.orm import Session

from app.celery_app import celery_app
from app.logging import get_logger
from app.models import Job, JobStatus

log = get_logger(__name__)

# kind → Celery task name. Payload keys become task kwargs.
KINDS: dict[str, str] = {
    "sales_report": "reports.generate_sales_report",
    "campaign": "emails.send_campaign",
    "image": "media.process_image",
}


def enqueue(session: Session, kind: str, payload: dict[str, Any]) -> Job:
    if kind not in KINDS:
        raise ValueError(f"unknown job kind {kind!r}")
    job = Job(kind=kind, payload=payload, status=JobStatus.QUEUED)
    session.add(job)
    session.commit()
    dispatch(session, job)
    return job


def dispatch(session: Session, job: Job) -> None:
    """Publish (or re-publish) the message for an existing job row."""
    task = celery_app.tasks[KINDS[job.kind]]
    try:
        result = task.apply_async(kwargs={**job.payload, "job_id": str(job.id)})
    except Exception as exc:  # broker down: surface it on the job instead of a 500
        job.status = JobStatus.FAILED
        job.error = f"publish failed: {exc}"
        session.commit()
        log.error("job_publish_failed", job_id=str(job.id), error=str(exc))
        raise
    job.celery_task_id = result.id
    job.status = JobStatus.QUEUED
    session.commit()
    log.info("job_enqueued", job_id=str(job.id), kind=job.kind, task_id=result.id)


def get(session: Session, job_id: uuid.UUID) -> Job | None:
    return session.get(Job, job_id)
