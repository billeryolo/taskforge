"""Celery application: broker/backend, queues, routing, time limits and the Beat schedule.

Reliability settings worth noticing:
- ``task_acks_late`` + ``task_reject_on_worker_lost``: a task is acknowledged only after it
  finishes, so a worker that dies mid-task hands the message back to the queue (at-least-once).
  Tasks are therefore written to be idempotent.
- ``worker_prefetch_multiplier = 1``: long-running tasks are not hoarded by one worker.
- Global soft/hard time limits, overridden per task where the work is heavier.
"""

from celery import Celery
from celery.schedules import crontab
from kombu import Queue

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "taskforge",
    include=[
        "app.tasks.reports",
        "app.tasks.email",
        "app.tasks.media",
        "app.tasks.maintenance",
    ],
)

celery_app.conf.update(
    broker_url=settings.celery_broker_url,
    result_backend=settings.celery_result_backend,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_hijack_root_logger=False,  # we configure structlog ourselves
    broker_connection_retry_on_startup=True,
    result_expires=3600,
    task_soft_time_limit=120,
    task_time_limit=180,
    task_default_queue="default",
    task_queues=(
        Queue("default"),
        Queue("reports"),
        Queue("emails"),
        Queue("media"),
    ),
    task_routes={
        "reports.*": {"queue": "reports"},
        "emails.*": {"queue": "emails"},
        "media.*": {"queue": "media"},
        "maintenance.*": {"queue": "default"},
    },
    beat_schedule={
        # Heavy PDF every morning, emailed to ops.
        "daily-sales-report": {
            "task": "maintenance.enqueue_scheduled",
            "schedule": crontab(hour=6, minute=0),
            "args": ["sales_report", {"period_days": 1, "email_to": settings.report_recipients}],
        },
        # Weekly roll-up on Mondays.
        "weekly-sales-report": {
            "task": "maintenance.enqueue_scheduled",
            "schedule": crontab(hour=7, minute=0, day_of_week="monday"),
            "args": ["sales_report", {"period_days": 7, "email_to": settings.report_recipients}],
        },
        "cleanup-expired-artifacts": {
            "task": "maintenance.cleanup_artifacts",
            "schedule": crontab(minute=30),  # hourly
        },
        "requeue-stuck-jobs": {
            "task": "maintenance.requeue_stuck_jobs",
            "schedule": 300.0,  # every 5 minutes
        },
    },
)
