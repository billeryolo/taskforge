"""Heavy PDF report generation, chained into an email delivery task."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app import storage
from app.celery_app import celery_app
from app.db import session_scope
from app.logging import get_logger
from app.models import Artifact, Sale
from app.tasks.base import TrackedTask

log = get_logger(__name__)


class ReportTimeout(Exception):
    """Raised when the soft time limit fires; not retried (the next run would time out too)."""


def _aggregate(session: Session, since: datetime) -> tuple[list[tuple[str, int, int]], int]:
    rows = session.execute(
        select(Sale.region, func.count(Sale.id), func.sum(Sale.amount_cents))
        .where(Sale.sold_at >= since)
        .group_by(Sale.region)
        .order_by(func.sum(Sale.amount_cents).desc())
    ).all()
    total = sum(int(r[2]) for r in rows)
    return [(r[0], int(r[1]), int(r[2])) for r in rows], total


def _render_pdf(path: Path, title: str, rows: list[tuple[str, int, int]], total: int) -> None:
    c = canvas.Canvas(str(path), pagesize=A4)
    width, height = A4
    c.setTitle(title)

    c.setFont("Helvetica-Bold", 20)
    c.drawString(20 * mm, height - 25 * mm, title)
    c.setFont("Helvetica", 10)
    c.setFillColor(colors.grey)
    c.drawString(20 * mm, height - 31 * mm, f"Generated {datetime.now(UTC):%Y-%m-%d %H:%M UTC}")
    c.setFillColor(colors.black)

    # Table
    y = height - 45 * mm
    c.setFont("Helvetica-Bold", 11)
    for x, label in ((20, "Region"), (90, "Orders"), (130, "Revenue")):
        c.drawString(x * mm, y, label)
    c.line(20 * mm, y - 2 * mm, 190 * mm, y - 2 * mm)
    c.setFont("Helvetica", 11)
    for region, count, cents in rows:
        y -= 8 * mm
        c.drawString(20 * mm, y, region)
        c.drawRightString(105 * mm, y, str(count))
        c.drawRightString(160 * mm, y, f"${cents / 100:,.2f}")
    y -= 10 * mm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(20 * mm, y, "Total")
    c.drawRightString(160 * mm, y, f"${total / 100:,.2f}")

    # Bar chart
    if rows:
        y -= 20 * mm
        max_cents = max(r[2] for r in rows) or 1
        bar_w = min(20 * mm, 150 * mm / len(rows))
        for i, (region, _, cents) in enumerate(rows):
            h = 50 * mm * cents / max_cents
            x = 20 * mm + i * (bar_w + 4 * mm)
            c.setFillColor(colors.HexColor("#1f5f4b"))
            c.rect(x, y - 50 * mm, bar_w, h, fill=1, stroke=0)
            c.setFillColor(colors.black)
            c.setFont("Helvetica", 8)
            c.drawCentredString(x + bar_w / 2, y - 54 * mm, region[:10])
    c.showPage()
    c.save()


@celery_app.task(
    base=TrackedTask,
    bind=True,
    name="reports.generate_sales_report",
    autoretry_for=(OperationalError,),  # transient DB errors only
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    max_retries=3,
    soft_time_limit=120,
    time_limit=150,
)
def generate_sales_report(
    self: TrackedTask,
    *,
    job_id: str | None = None,
    period_days: int = 30,
    email_to: list[str] | None = None,
) -> dict:
    since = datetime.now(UTC) - timedelta(days=period_days)
    title = f"Sales report — last {period_days} day{'s' if period_days != 1 else ''}"
    path = storage.new_path("reports", ".pdf")
    try:
        with session_scope() as session:
            rows, total = _aggregate(session, since)
            _render_pdf(path, title, rows, total)
            artifact = Artifact(
                job_id=job_id,
                kind="report",
                path=storage.relative(path),
                content_type="application/pdf",
                size_bytes=path.stat().st_size,
                meta={"period_days": period_days, "regions": len(rows), "total_cents": total},
            )
            session.add(artifact)
            session.flush()
            artifact_id = str(artifact.id)
    except SoftTimeLimitExceeded as exc:
        path.unlink(missing_ok=True)
        log.error("report_timeout", period_days=period_days)
        raise ReportTimeout("report generation exceeded the soft time limit") from exc

    if email_to:
        from app.tasks.email import send_email

        for recipient in email_to:
            send_email.delay(
                to=recipient,
                subject=title,
                body=f"Attached: {title}. Total revenue ${total / 100:,.2f}.",
                attachment=storage.relative(path),
            )
    log.info("report_generated", artifact_id=artifact_id, bytes=path.stat().st_size)
    return {
        "artifact_id": artifact_id,
        "regions": len(rows),
        "total_cents": total,
        "emailed_to": email_to or [],
    }
