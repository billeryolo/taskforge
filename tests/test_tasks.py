import io
import uuid
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from PIL import Image
from sqlalchemy import select, update

from app.db import SessionLocal
from app.mail import MemoryMailer
from app.models import Artifact, DeadLetter, Job, JobStatus
from app.tasks.email import send_email
from app.tasks.maintenance import requeue_stuck_jobs
from tests.conftest import wait_until

# --- retries + dead letters ---------------------------------------------------------------


def test_email_retries_with_backoff_then_succeeds(mailer: MemoryMailer, wait_for_job) -> None:
    mailer.fail_times = 2
    with SessionLocal() as session:
        job = Job(kind="email", payload={})
        session.add(job)
        session.commit()
        job_id = str(job.id)

    send_email.delay(to="a@example.com", subject="hi", body="there", job_id=job_id)
    job = wait_for_job(job_id)

    assert job.status == JobStatus.SUCCEEDED
    assert job.attempts == 3  # 1 initial + 2 retries
    assert job.result == {"to": "a@example.com", "attempts": 3}
    assert mailer.calls == 3 and len(mailer.sent) == 1
    with SessionLocal() as session:
        assert session.scalar(select(DeadLetter)) is None


def test_exhausted_retries_go_to_dead_letter_queue_and_can_be_replayed(
    client: TestClient, mailer: MemoryMailer, wait_for_job
) -> None:
    mailer.fail_times = 100  # permanent outage
    with SessionLocal() as session:
        job = Job(kind="email", payload={})
        session.add(job)
        session.commit()
        job_id = str(job.id)

    send_email.delay(to="b@example.com", subject="hi", body="there", job_id=job_id)
    job = wait_for_job(job_id)

    assert job.status == JobStatus.FAILED
    assert job.attempts == 6  # max_retries=5
    assert "SMTPServerDisconnected" in (job.error or "")

    r = client.get("/api/v1/dead-letters")
    assert r.status_code == 200
    (dl,) = r.json()
    assert dl["task_name"] == "emails.send"
    assert dl["attempts"] == 6
    assert dl["kwargs"]["to"] == "b@example.com"

    # Outage is over: replay the dead letter, the same job row goes green.
    mailer.fail_times = 0
    r = client.post(f"/api/v1/dead-letters/{dl['id']}/replay")
    assert r.status_code == 202
    assert r.json()["replayed_at"] is not None
    job = wait_for_job(job_id)
    assert job.status == JobStatus.SUCCEEDED
    assert len(mailer.sent) == 1

    assert client.get("/api/v1/dead-letters").json() == []
    r = client.post(f"/api/v1/dead-letters/{dl['id']}/replay")
    assert r.status_code == 409


# --- reports ------------------------------------------------------------------------------


def test_sales_report_generates_pdf_and_emails_it(
    client: TestClient, mailer: MemoryMailer, wait_for_job
) -> None:
    for region, amount in (("EU", 1200), ("US", 3400), ("EU", 800)):
        client.post(
            "/api/v1/sales", json={"region": region, "product": "Widget", "amount_cents": amount}
        )

    r = client.post(
        "/api/v1/reports/sales", json={"period_days": 7, "email_to": ["ops@example.com"]}
    )
    assert r.status_code == 202
    job = wait_for_job(r.json()["id"])
    assert job.status == JobStatus.SUCCEEDED, job.error
    assert job.result["regions"] == 2
    assert job.result["total_cents"] == 5400

    artifact_id = job.result["artifact_id"]
    r = client.get(f"/api/v1/artifacts/{artifact_id}/download")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.content.startswith(b"%PDF")

    wait_until(lambda: len(mailer.sent) == 1)
    assert mailer.sent[0].to == "ops@example.com"
    assert mailer.sent[0].attachments[0].suffix == ".pdf"


def test_report_validation(client: TestClient) -> None:
    assert client.post("/api/v1/reports/sales", json={"period_days": 0}).status_code == 422
    assert client.post("/api/v1/reports/sales", json={"email_to": ["nope"]}).status_code == 422


# --- media --------------------------------------------------------------------------------


def _png(size: tuple[int, int] = (2400, 1600)) -> bytes:
    buf = io.BytesIO()
    Image.effect_noise(size, 60).convert("RGB").save(buf, format="PNG")
    return buf.getvalue()


def test_image_is_compressed_and_thumbnailed(client: TestClient, wait_for_job) -> None:
    original = _png()
    r = client.post("/api/v1/uploads/images", files={"file": ("photo.png", original, "image/png")})
    assert r.status_code == 202
    job = wait_for_job(r.json()["id"])
    assert job.status == JobStatus.SUCCEEDED, job.error
    assert job.result["full_bytes"] < len(original)
    assert job.result["thumb_bytes"] < job.result["full_bytes"]

    with SessionLocal() as session:
        artifacts = session.scalars(select(Artifact).where(Artifact.job_id == job.id)).all()
    assert {a.meta["variant"] for a in artifacts} == {"full", "thumb"}
    thumb = next(a for a in artifacts if a.meta["variant"] == "thumb")
    r = client.get(f"/api/v1/artifacts/{thumb.id}/download")
    with Image.open(io.BytesIO(r.content)) as img:
        assert img.format == "WEBP"
        assert max(img.size) == 320


def test_corrupt_image_fails_fast_without_retries(client: TestClient, wait_for_job) -> None:
    r = client.post(
        "/api/v1/uploads/images", files={"file": ("x.png", b"definitely not a png", "image/png")}
    )
    job = wait_for_job(r.json()["id"])
    assert job.status == JobStatus.FAILED
    assert job.attempts == 1  # deterministic failure is not retried
    assert "not a valid image" in (job.error or "")
    with SessionLocal() as session:
        dl = session.scalar(select(DeadLetter).where(DeadLetter.job_id == job.id))
    assert dl is not None and dl.task_name == "media.process_image"


def test_upload_rejects_wrong_type(client: TestClient) -> None:
    r = client.post("/api/v1/uploads/images", files={"file": ("a.txt", b"hi", "text/plain")})
    assert r.status_code == 415


# --- campaigns ----------------------------------------------------------------------------


def test_campaign_fans_out_and_closes_even_with_permanent_failures(
    client: TestClient, mailer: MemoryMailer, wait_for_job
) -> None:
    class SelectiveMailer(MemoryMailer):
        def send(self, m):  # noqa: ANN001
            if m.to == "bad@example.com":
                raise ConnectionError("mailbox unavailable")
            super().send(m)

    from app import mail

    mail.set_mailer(SelectiveMailer())

    r = client.post(
        "/api/v1/campaigns",
        json={
            "subject": "Hello",
            "body": "World",
            "recipients": ["a@example.com", "bad@example.com", "c@example.com", "a@example.com"],
        },
    )
    assert r.status_code == 201
    campaign = r.json()
    assert campaign["recipients_total"] == 3  # duplicates collapsed

    r = client.post(f"/api/v1/campaigns/{campaign['id']}/send")
    assert r.status_code == 202
    wait_for_job(r.json()["id"])

    def done() -> bool:
        return client.get(f"/api/v1/campaigns/{campaign['id']}").json()["status"] == "done"

    wait_until(done)
    detail = client.get(f"/api/v1/campaigns/{campaign['id']}").json()
    assert detail["sent_count"] == 2
    assert detail["failed_count"] == 1
    failed = next(rcpt for rcpt in detail["recipients"] if rcpt["email"] == "bad@example.com")
    assert "ConnectionError" in failed["error"]
    assert len(mail.get_mailer().sent) == 2  # type: ignore[union-attr]

    # Sending twice is rejected.
    assert client.post(f"/api/v1/campaigns/{campaign['id']}/send").status_code == 409


# --- maintenance / beat -------------------------------------------------------------------


def test_stuck_jobs_are_requeued(client: TestClient, wait_for_job) -> None:
    client.post("/api/v1/sales", json={"region": "EU", "product": "W", "amount_cents": 1})
    with SessionLocal() as session:
        job = Job(kind="sales_report", payload={"period_days": 1}, status=JobStatus.RUNNING)
        session.add(job)
        session.commit()
        # Pretend the worker died 10 minutes ago.
        session.execute(
            update(Job)
            .where(Job.id == job.id)
            .values(updated_at=datetime.now(UTC) - timedelta(minutes=10))
        )
        session.commit()
        job_id = str(job.id)

    result = requeue_stuck_jobs.delay().get(timeout=10)
    assert result == {"requeued": [job_id]}
    job = wait_for_job(job_id)
    assert job.status == JobStatus.SUCCEEDED


def test_beat_schedule_references_registered_tasks() -> None:
    from app.celery_app import celery_app

    for name, entry in celery_app.conf.beat_schedule.items():
        assert entry["task"] in celery_app.tasks, f"{name} points at unknown task"


def test_scheduled_report_creates_tracked_job(wait_for_job) -> None:
    from app.tasks.maintenance import enqueue_scheduled

    job_id = enqueue_scheduled.delay("sales_report", {"period_days": 1}).get(timeout=10)
    job = wait_for_job(job_id)
    assert job.status == JobStatus.SUCCEEDED
    assert job.kind == "sales_report"


def test_jobs_listing_and_404(client: TestClient) -> None:
    r = client.get("/api/v1/jobs", params={"status": "failed"})
    assert r.status_code == 200 and r.json()["total"] == 0
    assert client.get(f"/api/v1/jobs/{uuid.uuid4()}").status_code == 404


def test_soft_time_limit_is_handled_and_not_retried(
    client: TestClient, wait_for_job, monkeypatch
) -> None:
    """The worker raises SoftTimeLimitExceeded inside the task when the soft limit fires; the
    task must clean up and fail with a clear error rather than being retried (it would only
    time out again)."""
    from celery.exceptions import SoftTimeLimitExceeded

    from app.tasks import media

    def slow_open(*_: object, **__: object) -> None:
        raise SoftTimeLimitExceeded()

    monkeypatch.setattr(media.Image, "open", slow_open)
    r = client.post(
        "/api/v1/uploads/images", files={"file": ("p.png", _png((64, 64)), "image/png")}
    )
    job = wait_for_job(r.json()["id"])
    assert job.status == JobStatus.FAILED
    assert job.attempts == 1
    assert "ImageTimeout" in (job.error or "")
