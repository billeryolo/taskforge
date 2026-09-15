"""Test harness.

- Real PostgreSQL (schema via Alembic), tables truncated between tests.
- A real in-process Celery worker (solo pool, in-memory broker) so retries, ``on_failure``
  and dead-lettering run through Celery's actual tracer rather than eager-mode shortcuts.
- fakeredis for the cache and an in-memory mailer that can be told to fail.
- Exponential backoff is patched to zero so retry tests finish in milliseconds.
"""

import os
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+psycopg://taskforge:taskforge@localhost:5432/taskforge_test"
)
_STORAGE = Path(tempfile.mkdtemp(prefix="taskforge-test-"))
os.environ.update(
    {
        "DATABASE_URL": TEST_DATABASE_URL,
        "ENV": "test",
        "LOG_JSON": "false",
        "LOG_LEVEL": "WARNING",
        "MAIL_BACKEND": "memory",
        "EMAIL_RATE_LIMIT": "",
        "CELERY_BROKER_URL": "memory://",
        "CELERY_RESULT_BACKEND": "cache+memory://",
        "STORAGE_DIR": str(_STORAGE),
        "STUCK_JOB_SECONDS": "1",
    }
)

import celery.app.autoretry  # noqa: E402
import fakeredis  # noqa: E402
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from celery.contrib.testing.worker import start_worker  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app import cache, mail  # noqa: E402
from app.celery_app import celery_app  # noqa: E402
from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Job, JobStatus  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> Iterator[None]:
    cfg = Config("alembic.ini")
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")
    yield


@pytest.fixture(scope="session", autouse=True)
def no_backoff() -> Iterator[None]:
    original = celery.app.autoretry.get_exponential_backoff_interval
    celery.app.autoretry.get_exponential_backoff_interval = lambda *a, **k: 0
    yield
    celery.app.autoretry.get_exponential_backoff_interval = original


@pytest.fixture(scope="session", autouse=True)
def worker() -> Iterator[None]:
    # Registering tasks requires the modules to be imported before the worker starts.
    celery_app.loader.import_default_modules()
    with start_worker(celery_app, pool="solo", perform_ping_check=False, loglevel="WARNING"):
        yield


@pytest.fixture(autouse=True)
def clean_state() -> Iterator[None]:
    fake = fakeredis.FakeRedis(decode_responses=True)
    cache.set_client(fake)
    mailer = mail.MemoryMailer()
    mail.set_mailer(mailer)
    yield
    tables = ", ".join(t.name for t in reversed(Base.metadata.sorted_tables))
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {tables} CASCADE"))
    cache.set_client(None)
    mail.set_mailer(None)


@pytest.fixture
def mailer() -> mail.MemoryMailer:
    return mail.get_mailer()  # type: ignore[return-value]


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


@pytest.fixture
def wait_for_job() -> Callable[[str, float], Job]:
    def _wait(job_id: str, timeout: float = 15.0) -> Job:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with SessionLocal() as session:
                job = session.get(Job, uuid.UUID(job_id))
                if job and job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                    session.expunge(job)
                    return job
            time.sleep(0.05)
        raise TimeoutError(f"job {job_id} did not finish within {timeout}s")

    return _wait


def wait_until(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError("condition not met in time")
