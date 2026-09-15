from fastapi.testclient import TestClient

from app import cache


def test_stats_are_cached_and_invalidated_on_write(client: TestClient) -> None:
    client.post("/api/v1/sales", json={"region": "EU", "product": "Widget", "amount_cents": 1000})

    r1 = client.get("/api/v1/stats/sales")
    assert r1.status_code == 200
    assert r1.headers["x-cache"] == "MISS"
    assert r1.json()["revenue_cents"] == 1000

    r2 = client.get("/api/v1/stats/sales")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.json()["computed_at"] == r1.json()["computed_at"]

    # Different parameters → different key → miss.
    r3 = client.get("/api/v1/stats/sales", params={"region": "EU"})
    assert r3.headers["x-cache"] == "MISS"

    # A write invalidates *every* variant tagged "sales" in one go.
    client.post("/api/v1/sales", json={"region": "EU", "product": "Gadget", "amount_cents": 500})
    r4 = client.get("/api/v1/stats/sales")
    assert r4.headers["x-cache"] == "MISS"
    assert r4.json()["revenue_cents"] == 1500
    r5 = client.get("/api/v1/stats/sales", params={"region": "EU"})
    assert r5.headers["x-cache"] == "MISS"


def test_invalidate_returns_count() -> None:
    fake = cache.get_client()
    fake.set("cache:x:1", "1")
    fake.set("cache:x:2", "2")
    fake.sadd("tag:sales", "cache:x:1", "cache:x:2")
    assert cache.invalidate("sales") == 2
    assert fake.get("cache:x:1") is None
    assert cache.invalidate("sales") == 0


def test_cache_outage_degrades_gracefully(client: TestClient) -> None:
    class Broken:
        def get(self, *_: object) -> None:
            raise cache.redis.ConnectionError("down")

        def pipeline(self) -> None:
            raise cache.redis.ConnectionError("down")

    cache.set_client(Broken())  # type: ignore[arg-type]
    r = client.get("/api/v1/stats/sales")
    assert r.status_code == 200
    assert r.headers["x-cache"] == "MISS"
