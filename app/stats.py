"""Read-heavy aggregations served through the Redis cache. Anything that writes to ``sales``
must call ``cache.invalidate("sales")``."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.cache import cached
from app.models import Sale


@cached(ttl=60, tags=["sales"], namespace="sales_summary")
def sales_summary(session: Session, *, region: str | None = None, days: int = 30) -> dict:
    since = datetime.now(UTC) - timedelta(days=days)
    stmt = select(func.count(Sale.id), func.coalesce(func.sum(Sale.amount_cents), 0)).where(
        Sale.sold_at >= since
    )
    if region:
        stmt = stmt.where(Sale.region == region)
    count, total = session.execute(stmt).one()

    by_region = session.execute(
        select(Sale.region, func.count(Sale.id), func.sum(Sale.amount_cents))
        .where(Sale.sold_at >= since)
        .group_by(Sale.region)
        .order_by(func.sum(Sale.amount_cents).desc())
    ).all()
    top_products = session.execute(
        select(Sale.product, func.sum(Sale.amount_cents).label("revenue"))
        .where(Sale.sold_at >= since)
        .group_by(Sale.product)
        .order_by(func.sum(Sale.amount_cents).desc())
        .limit(5)
    ).all()
    return {
        "days": days,
        "region": region,
        "orders": int(count),
        "revenue_cents": int(total),
        "by_region": [
            {"region": r, "orders": int(c), "revenue_cents": int(s)} for r, c, s in by_region
        ],
        "top_products": [{"product": p, "revenue_cents": int(s)} for p, s in top_products],
        "computed_at": datetime.now(UTC).isoformat(),
    }
