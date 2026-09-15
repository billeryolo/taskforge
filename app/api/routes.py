import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app import cache, jobs, stats, storage
from app.db import get_session
from app.models import (
    Artifact,
    Campaign,
    CampaignRecipient,
    CampaignStatus,
    DeadLetter,
    Job,
    JobStatus,
    Sale,
)
from app.schemas import (
    ArtifactOut,
    CampaignCreate,
    CampaignDetail,
    CampaignOut,
    DeadLetterOut,
    JobOut,
    Page,
    SaleCreate,
    SalesReportRequest,
)

router = APIRouter()
DB = Annotated[Session, Depends(get_session)]

MAX_UPLOAD_BYTES = 20 * 1024 * 1024
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


# --- jobs -------------------------------------------------------------------------------


@router.get("/jobs", response_model=Page, tags=["jobs"])
def list_jobs(
    session: DB,
    job_status: JobStatus | None = Query(default=None, alias="status"),
    kind: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> Page:
    stmt = select(Job)
    if job_status:
        stmt = stmt.where(Job.status == job_status)
    if kind:
        stmt = stmt.where(Job.kind == kind)
    total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = session.scalars(
        stmt.order_by(Job.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    )
    return Page(
        items=[JobOut.model_validate(j) for j in rows], total=total, page=page, page_size=page_size
    )


@router.get("/jobs/{job_id}", response_model=JobOut, tags=["jobs"])
def get_job(job_id: uuid.UUID, session: DB) -> JobOut:
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return JobOut.model_validate(job)


# --- reports ----------------------------------------------------------------------------


@router.post(
    "/reports/sales",
    response_model=JobOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["reports"],
    summary="Generate a PDF sales report in the background (optionally emailed)",
)
def request_sales_report(data: SalesReportRequest, session: DB) -> JobOut:
    job = jobs.enqueue(session, "sales_report", data.model_dump(mode="json"))
    return JobOut.model_validate(job)


@router.get("/artifacts", response_model=list[ArtifactOut], tags=["artifacts"])
def list_artifacts(
    session: DB, kind: str | None = None, limit: int = Query(50, le=200)
) -> list[ArtifactOut]:
    stmt = select(Artifact).order_by(Artifact.created_at.desc()).limit(limit)
    if kind:
        stmt = stmt.where(Artifact.kind == kind)
    return [ArtifactOut.model_validate(a) for a in session.scalars(stmt)]


@router.get("/artifacts/{artifact_id}/download", tags=["artifacts"])
def download_artifact(artifact_id: uuid.UUID, session: DB) -> FileResponse:
    artifact = session.get(Artifact, artifact_id)
    if artifact is None:
        raise HTTPException(404, "artifact not found")
    path = storage.resolve(artifact.path)
    if not path.exists():
        raise HTTPException(410, "artifact file has been cleaned up")
    return FileResponse(path, media_type=artifact.content_type, filename=path.name)


# --- uploads ----------------------------------------------------------------------------


@router.post(
    "/uploads/images",
    response_model=JobOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["media"],
    summary="Upload an image; it is compressed and thumbnailed by a worker",
)
def upload_image(session: DB, file: UploadFile = File(...)) -> JobOut:
    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(415, f"unsupported type {file.content_type}")
    dest = storage.new_path("incoming", ".bin")
    written = 0
    with dest.open("wb") as out:
        while chunk := file.file.read(1024 * 1024):
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                out.close()
                dest.unlink(missing_ok=True)
                raise HTTPException(413, "file too large")
            out.write(chunk)
    job = jobs.enqueue(
        session,
        "image",
        {"source": storage.relative(dest), "original_name": file.filename or "upload"},
    )
    return JobOut.model_validate(job)


# --- campaigns --------------------------------------------------------------------------


def _campaign_out(c: Campaign, detail: bool = False) -> CampaignOut | CampaignDetail:
    base = {
        "id": c.id,
        "subject": c.subject,
        "status": c.status,
        "sent_count": c.sent_count,
        "failed_count": c.failed_count,
        "recipients_total": len(c.recipients),
        "created_at": c.created_at,
        "finished_at": c.finished_at,
    }
    if detail:
        return CampaignDetail(**base, recipients=c.recipients)
    return CampaignOut(**base)


@router.post("/campaigns", response_model=CampaignOut, status_code=201, tags=["campaigns"])
def create_campaign(data: CampaignCreate, session: DB) -> CampaignOut:
    campaign = Campaign(subject=data.subject, body=data.body)
    campaign.recipients = [CampaignRecipient(email=e) for e in dict.fromkeys(data.recipients)]
    session.add(campaign)
    session.commit()
    return _campaign_out(campaign)


@router.post(
    "/campaigns/{campaign_id}/send",
    response_model=JobOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["campaigns"],
)
def send_campaign(campaign_id: uuid.UUID, session: DB) -> JobOut:
    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        raise HTTPException(404, "campaign not found")
    if campaign.status != CampaignStatus.DRAFT:
        raise HTTPException(409, f"campaign is {campaign.status.value}")
    job = jobs.enqueue(session, "campaign", {"campaign_id": str(campaign.id)})
    return JobOut.model_validate(job)


@router.get("/campaigns/{campaign_id}", response_model=CampaignDetail, tags=["campaigns"])
def get_campaign(campaign_id: uuid.UUID, session: DB) -> CampaignDetail:
    campaign = session.scalar(
        select(Campaign)
        .where(Campaign.id == campaign_id)
        .options(selectinload(Campaign.recipients))
    )
    if campaign is None:
        raise HTTPException(404, "campaign not found")
    return _campaign_out(campaign, detail=True)


# --- sales + cached stats ---------------------------------------------------------------


@router.get("/stats/sales", tags=["stats"], summary="Aggregated sales (Redis-cached, 60 s)")
def sales_stats(
    session: DB, response: Response, region: str | None = None, days: int = Query(30, ge=1, le=365)
) -> dict:
    data = stats.sales_summary(session, region=region, days=days)
    response.headers["X-Cache"] = (stats.sales_summary.cache_status or "miss").upper()
    return data


@router.post("/sales", status_code=201, tags=["stats"])
def create_sale(data: SaleCreate, session: DB) -> dict:
    sale = Sale(**data.model_dump())
    session.add(sale)
    session.commit()
    # Write-through invalidation: every cached sales_summary variant is dropped at once.
    cache.invalidate("sales")
    return {"id": str(sale.id)}


# --- dead letters -----------------------------------------------------------------------


@router.get("/dead-letters", response_model=list[DeadLetterOut], tags=["dead letters"])
def list_dead_letters(session: DB, include_replayed: bool = False) -> list[DeadLetterOut]:
    stmt = select(DeadLetter).order_by(DeadLetter.created_at.desc()).limit(200)
    if not include_replayed:
        stmt = stmt.where(DeadLetter.replayed_at.is_(None))
    return [DeadLetterOut.model_validate(d) for d in session.scalars(stmt)]


@router.post(
    "/dead-letters/{dead_letter_id}/replay",
    response_model=DeadLetterOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["dead letters"],
    summary="Re-publish a dead-lettered task with its original arguments",
)
def replay_dead_letter(dead_letter_id: uuid.UUID, session: DB) -> DeadLetterOut:
    from app.celery_app import celery_app

    dl = session.get(DeadLetter, dead_letter_id)
    if dl is None:
        raise HTTPException(404, "dead letter not found")
    if dl.replayed_at is not None:
        raise HTTPException(409, "already replayed")
    if dl.job_id:
        job = session.get(Job, dl.job_id)
        if job is not None:
            job.status = JobStatus.QUEUED
            job.error = None
    task = celery_app.tasks.get(dl.task_name)
    if task is None:
        raise HTTPException(422, f"task {dl.task_name} is no longer registered")
    result = task.apply_async(args=dl.args, kwargs=dl.kwargs)
    dl.replayed_at = datetime.now(UTC)
    dl.task_id = result.id
    session.commit()
    return DeadLetterOut.model_validate(dl)
