"""Periodic housekeeping driven by Celery Beat (see ``beat_schedule`` in celery_app.py)."""

import contextlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from app import jobs, storage
from app.celery_app import celery_app
from app.config import get_settings
from app.db import session_scope
from app.logging import get_logger
from app.models import Artifact, Job, JobStatus

log = get_logger(__name__)


@celery_app.task(name="maintenance.enqueue_scheduled")
def enqueue_scheduled(kind: str, payload: dict) -> str:
    """Beat entry point: create a tracked job for a scheduled piece of work so scheduled
    runs show up in /jobs exactly like API-triggered ones."""
    with session_scope() as session:
        job = jobs.enqueue(session, kind, payload)
        return str(job.id)


@celery_app.task(name="maintenance.cleanup_artifacts")
def cleanup_artifacts() -> dict:
    cutoff = datetime.now(UTC) - timedelta(days=get_settings().artifact_retention_days)
    removed = 0
    with session_scope() as session:
        for artifact in session.scalars(select(Artifact).where(Artifact.created_at < cutoff)):
            with contextlib.suppress(ValueError):
                storage.resolve(artifact.path).unlink(missing_ok=True)
            session.delete(artifact)
            removed += 1
    log.info("artifacts_cleaned", removed=removed)
    return {"removed": removed}


@celery_app.task(name="maintenance.requeue_stuck_jobs")
def requeue_stuck_jobs() -> dict:
    """Jobs stuck in ``running`` past the threshold (worker OOM-killed, host rebooted) are
    re-published. Tasks are idempotent so a duplicate run is harmless."""
    cutoff = datetime.now(UTC) - timedelta(seconds=get_settings().stuck_job_seconds)
    requeued: list[str] = []
    with session_scope() as session:
        stuck = session.scalars(
            select(Job).where(Job.status == JobStatus.RUNNING, Job.updated_at < cutoff)
        ).all()
        for job in stuck:
            log.warning("job_stuck_requeue", job_id=str(job.id), kind=job.kind)
            jobs.dispatch(session, job)
            requeued.append(str(job.id))
    return {"requeued": requeued}
