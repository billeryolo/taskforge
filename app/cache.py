"""Redis read-through cache with tag-based invalidation.

    @cached(ttl=60, tags=["sales"])
    def sales_summary(session, region=None): ...

    invalidate("sales")   # after any write that changes sales

Every cached value is stored under ``cache:<namespace>:<sha1(args)>`` and its key is added to
one Redis set per tag (``tag:<name>``). Invalidating a tag deletes all keys in that set in one
round-trip, so a write never has to know which parameter combinations were cached.

The client is created lazily and can be swapped for a fake in tests (``set_client``).
"""

import functools
import hashlib
import json
from collections.abc import Callable, Iterable
from typing import Any, ParamSpec, TypeVar

import redis

from app.config import get_settings
from app.logging import get_logger

log = get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
    return _client


def set_client(client: redis.Redis | None) -> None:
    global _client
    _client = client


def _key(namespace: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    raw = json.dumps([args, sorted(kwargs.items())], default=str, sort_keys=True)
    return f"cache:{namespace}:{hashlib.sha1(raw.encode()).hexdigest()}"


def cached(
    *, ttl: int | None = None, tags: Iterable[str] = (), namespace: str | None = None
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Cache the JSON-serialisable return value of ``fn``.

    The first positional argument is assumed to be a DB session and is excluded from the key.
    ``fn.cache_status`` is set to ``"hit"`` / ``"miss"`` after each call so callers (e.g. an
    HTTP layer adding an ``X-Cache`` header) can observe what happened without extra lookups.
    """
    tag_list = list(tags)

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        ns = namespace or f"{fn.__module__}.{fn.__qualname__}"

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = _key(ns, args[1:], kwargs)
            client = get_client()
            try:
                hit = client.get(key)
            except redis.RedisError as exc:  # cache outage must not take the API down
                log.warning("cache_unavailable", error=str(exc))
                hit = None
            if hit is not None:
                wrapper.cache_status = "hit"  # type: ignore[attr-defined]
                return json.loads(hit)

            value = fn(*args, **kwargs)
            wrapper.cache_status = "miss"  # type: ignore[attr-defined]
            try:
                pipe = client.pipeline()
                pipe.set(
                    key, json.dumps(value, default=str), ex=ttl or get_settings().cache_default_ttl
                )
                for tag in tag_list:
                    pipe.sadd(f"tag:{tag}", key)
                pipe.execute()
            except redis.RedisError as exc:
                log.warning("cache_write_failed", error=str(exc))
            return value

        wrapper.cache_status = None  # type: ignore[attr-defined]
        return wrapper

    return decorator


def invalidate(*tags: str) -> int:
    """Drop every cached entry carrying any of ``tags``. Returns the number of keys removed."""
    client = get_client()
    removed = 0
    for tag in tags:
        tag_key = f"tag:{tag}"
        keys = client.smembers(tag_key)
        pipe = client.pipeline()
        if keys:
            pipe.delete(*keys)
        pipe.delete(tag_key)
        results = pipe.execute()
        removed += int(results[0]) if keys else 0
    if removed:
        log.info("cache_invalidated", tags=list(tags), keys=removed)
    return removed
