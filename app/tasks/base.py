"""Base task that mirrors every execution into the ``jobs`` table and dead-letters final
failures.

Lifecycle hooks (called by Celery's tracer inside the worker):

    before_start  → status=running, attempts += 1, bind job_id/task_id to the log context
    on_retry      → status=retrying, error=<exc>          (autoretry_for / self.retry)
    on_success    → status=succeeded, result=<return value>
    on_failure    → status=failed, error=<exc>, INSERT INTO dead_letters   (retries exhausted)

Tasks opt in by passing ``job_id=<uuid>`` in kwargs; scheduled tasks without a job row are
still logged but not tracked.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from celery import Task
from sqlalchemy import update

from app.db import session_scope
from app.logging import get_logger
from app.models import DeadLetter, Job, JobStatus

log = get_logger("taskforge.tasks")


def _job_id(kwargs: dict[str, Any]) -> uuid.UUID | None:
    raw = kwargs.get("job_id")
    return uuid.UUID(raw) if raw else None


class TrackedTask(Task):
    abstract = True

    def before_start(self, task_id: str, args: tuple, kwargs: dict[str, Any]) -> None:
        job_id = _job_id(kwargs)
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            task=self.name, task_id=task_id, job_id=str(job_id) if job_id else None
        )
        log.info("task_started", retries=self.request.retries)
        if job_id:
            with session_scope() as session:
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        status=JobStatus.RUNNING,
                        celery_task_id=task_id,
                        attempts=Job.attempts + 1,
                        started_at=datetime.now(UTC),
                        error=None,
                    )
                )

    def on_retry(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo: Any) -> None:
        log.warning("task_retry", error=str(exc), retries=self.request.retries)
        job_id = _job_id(kwargs)
        if job_id:
            with session_scope() as session:
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(status=JobStatus.RETRYING, error=str(exc))
                )

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        log.info("task_succeeded")
        job_id = _job_id(kwargs)
        if job_id:
            with session_scope() as session:
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        status=JobStatus.SUCCEEDED,
                        result=retval if isinstance(retval, dict) else {"value": retval},
                        finished_at=datetime.now(UTC),
                    )
                )

    def on_failure(
        self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo: Any
    ) -> None:
        log.error("task_failed", error=str(exc), retries=self.request.retries)
        job_id = _job_id(kwargs)
        with session_scope() as session:
            attempts = self.request.retries + 1
            if job_id:
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        status=JobStatus.FAILED,
                        error=f"{type(exc).__name__}: {exc}",
                        finished_at=datetime.now(UTC),
                    )
                )
            session.add(
                DeadLetter(
                    job_id=job_id,
                    task_name=self.name,
                    task_id=task_id,
                    args=list(args),
                    kwargs=kwargs,
                    error=f"{type(exc).__name__}: {exc}",
                    traceback=str(einfo) if einfo else None,
                    attempts=attempts,
                )
            )
        self.after_dead_letter(exc, kwargs)

    def after_dead_letter(self, exc: Exception, kwargs: dict) -> None:
        """Hook for subclasses that need domain bookkeeping on final failure."""
