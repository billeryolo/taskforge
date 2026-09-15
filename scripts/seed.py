"""Seed demo sales so the report and stats endpoints have something to aggregate.

python -m scripts.seed
"""

import random
from datetime import UTC, datetime, timedelta

from app.db import session_scope
from app.models import Sale

REGIONS = ["EU", "US", "APAC", "LATAM"]
PRODUCTS = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]


def main() -> None:
    rng = random.Random(42)
    now = datetime.now(UTC)
    with session_scope() as session:
        session.add_all(
            Sale(
                region=rng.choice(REGIONS),
                product=rng.choice(PRODUCTS),
                amount_cents=rng.randint(500, 50_000),
                sold_at=now - timedelta(days=rng.random() * 60),
            )
            for _ in range(500)
        )
    print("seeded 500 sales")


if __name__ == "__main__":
    main()
