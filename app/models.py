import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class JobStatus(enum.StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # exhausted retries → also lands in dead_letters


class Job(Base):
    """One row per unit of background work the API hands to Celery. The Celery task id is
    stored so the row can be correlated with worker logs and Flower."""

    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_status_updated", "status", "updated_at"),
        Index("ix_jobs_kind_created", "kind", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", values_callable=lambda e: [m.value for m in e]),
        default=JobStatus.QUEUED,
        nullable=False,
    )
    celery_task_id: Mapped[str | None] = mapped_column(String(64), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeadLetter(Base):
    """Tasks that exhausted their retries. Kept with full args so they can be replayed once
    the underlying problem (SMTP outage, corrupt file, bug) is fixed."""

    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), index=True
    )
    task_name: Mapped[str] = mapped_column(String(200), nullable=False)
    task_id: Mapped[str | None] = mapped_column(String(64))
    args: Mapped[list[Any]] = mapped_column(JSONB, default=list, nullable=False)
    kwargs: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    traceback: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Artifact(Base):
    """A file produced by a task (PDF report, resized image). Cleaned up by Beat after
    `artifact_retention_days`."""

    __tablename__ = "artifacts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), index=True
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)  # report | image
    path: Mapped[str] = mapped_column(String(512), nullable=False)
    content_type: Mapped[str] = mapped_column(String(100), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )


class CampaignStatus(enum.StrEnum):
    DRAFT = "draft"
    SENDING = "sending"
    DONE = "done"


class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    subject: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[CampaignStatus] = mapped_column(
        Enum(
            CampaignStatus, name="campaign_status", values_callable=lambda e: [m.value for m in e]
        ),
        default=CampaignStatus.DRAFT,
        nullable=False,
    )
    sent_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    recipients: Mapped[list["CampaignRecipient"]] = relationship(
        back_populates="campaign", cascade="all, delete-orphan", passive_deletes=True
    )


class CampaignRecipient(Base):
    __tablename__ = "campaign_recipients"
    __table_args__ = (Index("uq_campaign_recipient_email", "campaign_id", "email", unique=True),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False
    )
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)

    campaign: Mapped[Campaign] = relationship(back_populates="recipients")


class Sale(Base):
    """Demo business data that the PDF report and the cached stats endpoint aggregate."""

    __tablename__ = "sales"
    __table_args__ = (Index("ix_sales_sold_at", "sold_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    region: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    product: Mapped[str] = mapped_column(String(80), nullable=False)
    amount_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sold_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
