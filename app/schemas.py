import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.models import CampaignStatus, JobStatus


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class JobOut(ORMModel):
    id: uuid.UUID
    kind: str
    status: JobStatus
    celery_task_id: str | None
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    attempts: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class SalesReportRequest(BaseModel):
    period_days: int = Field(default=30, ge=1, le=365)
    email_to: list[EmailStr] = Field(default_factory=list, max_length=10)


class CampaignCreate(BaseModel):
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=20000)
    recipients: list[EmailStr] = Field(min_length=1, max_length=10000)


class RecipientOut(ORMModel):
    email: str
    sent_at: datetime | None
    error: str | None


class CampaignOut(ORMModel):
    id: uuid.UUID
    subject: str
    status: CampaignStatus
    sent_count: int
    failed_count: int
    recipients_total: int
    created_at: datetime
    finished_at: datetime | None


class CampaignDetail(CampaignOut):
    recipients: list[RecipientOut]


class ArtifactOut(ORMModel):
    id: uuid.UUID
    job_id: uuid.UUID | None
    kind: str
    content_type: str
    size_bytes: int
    meta: dict[str, Any]
    created_at: datetime


class DeadLetterOut(ORMModel):
    id: uuid.UUID
    job_id: uuid.UUID | None
    task_name: str
    task_id: str | None
    kwargs: dict[str, Any]
    error: str
    attempts: int
    created_at: datetime
    replayed_at: datetime | None


class SaleCreate(BaseModel):
    region: str = Field(min_length=1, max_length=40)
    product: str = Field(min_length=1, max_length=80)
    amount_cents: int = Field(ge=1, le=100_000_000)


class Page(BaseModel):
    items: list[Any]
    total: int
    page: int
    page_size: int
