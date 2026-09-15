import time
import uuid
from collections.abc import Awaitable, Callable

import sentry_sdk
import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sentry_sdk.integrations.celery import CeleryIntegration
from sqlalchemy import text

from app.api.routes import router
from app.cache import get_client
from app.config import get_settings
from app.db import engine
from app.logging import configure_logging, get_logger

settings = get_settings()
configure_logging(settings.log_level, json_output=settings.log_json)
log = get_logger("taskforge")

if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.env,
        integrations=[CeleryIntegration(monitor_beat_tasks=True)],
        traces_sample_rate=0.1,
    )

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description=(
        "Background-job platform. Every POST that does real work returns **202** with a job "
        "you can poll at `/jobs/{id}`. Workers retry transient failures with exponential "
        "backoff; exhausted tasks land in `/dead-letters` and can be replayed."
    ),
)


@app.middleware("http")
async def request_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id, method=request.method, path=request.url.path
    )
    started = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    if not request.url.path.startswith(("/health", "/docs", "/openapi")):
        log.info(
            "request",
            status=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
        )
    return response


@app.get("/health", tags=["ops"])
def health() -> JSONResponse:
    checks: dict[str, str] = {}
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = f"error: {exc}"
    try:
        get_client().ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"
    healthy = all(v == "ok" for v in checks.values())
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", **checks}, 200 if healthy else 503
    )


app.include_router(router, prefix="/api/v1")
