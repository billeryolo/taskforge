# TaskForge — background jobs done properly

FastAPI · Celery 5 · Celery Beat · Redis · PostgreSQL 16 · Flower · Mailpit · Docker Compose · GitHub Actions

A job platform for the kind of work that must never run inside a request: heavy PDF reports,
bulk email campaigns, image compression. Everything the API accepts comes back as `202` with a
job id; workers do the rest and the outcome is queryable. The interesting engineering is in the
failure paths — exponential-backoff retries, a replayable dead-letter queue, soft/hard time
limits, stuck-job recovery — and in a Redis cache whose invalidation is tag-based rather than
"remember every key".

```
docker compose up --build
```

| Service                               | URL                                 |
| ------------------------------------- | ----------------------------------- |
| API + Swagger UI                      | http://localhost:8000/docs          |
| Flower (worker monitoring)            | http://localhost:5555               |
| Mailpit (see the emails workers send) | http://localhost:8025               |
| Postgres / Redis                      | `localhost:5432` / `localhost:6379` |

Try it:

```bash
docker compose exec api python -m scripts.seed                                  # 500 demo sales
curl -X POST localhost:8000/api/v1/reports/sales -H 'content-type: application/json' \
     -d '{"period_days": 30, "email_to": ["ops@taskforge.dev"]}'                # → 202 + job
curl localhost:8000/api/v1/jobs/<id>                                            # → succeeded + artifact_id
curl -o report.pdf localhost:8000/api/v1/artifacts/<artifact_id>/download
curl -F file=@photo.jpg localhost:8000/api/v1/uploads/images                    # → webp + thumbnail
curl -i localhost:8000/api/v1/stats/sales                                       # X-Cache: MISS, then HIT
```

## Live on Railway

**https://app-production-b400.up.railway.app** — [`/docs`](https://app-production-b400.up.railway.app/docs) Swagger UI · [`/health`](https://app-production-b400.up.railway.app/health) · [`/api/v1/jobs`](https://app-production-b400.up.railway.app/api/v1/jobs)

On Railway's free plan the API, the Celery worker and Beat run in **one container**
(`PROCESS=all`, see `docker-entrypoint.sh`) because a volume can only be attached to a single
service and the API must serve the artifacts the worker writes. Postgres and Redis are Railway
managed services; email uses the in-memory backend there (no SMTP). `docker compose up` runs
the fully separated topology (api / worker / beat / flower / mailpit).

---

## Architecture

```
            POST /reports/sales, /campaigns/{id}/send, /uploads/images
                    │  1. INSERT jobs (status=queued)  COMMIT
                    │  2. apply_async(kwargs + job_id)          ┌────────────┐
┌──────────┐        ▼                                            │  Redis     │
│ FastAPI  │ ─────────────────────── broker (db 1) ────────────▶ │  broker    │
│  api     │ ◀────────────────────── cache  (db 0) ────────────▶ │  cache     │
└──────────┘                                                     │  results   │
      │                                                          └─────┬──────┘
      │ jobs, artifacts, dead_letters, campaigns, sales                │ queues: reports · emails · media · default
      ▼                                                                ▼
┌──────────┐                                             ┌────────────────────────────┐
│ Postgres │ ◀────── TrackedTask hooks (running/retry/ ──│ celery worker  (-Q all)    │
│          │         succeeded/failed + dead letter)     │ celery beat    (schedules) │
└──────────┘                                             │ flower         (:5555)     │
                                                         └────────────┬───────────────┘
                                                                      │ SMTP
                                                                      ▼
                                                                  Mailpit (:8025)
```

```
app/
  main.py            FastAPI app, request-id middleware, /health (checks Postgres + Redis), Sentry
  celery_app.py      broker/backend, queues + routing, acks-late, time limits, Beat schedule
  jobs.py            kind → task registry; commit-then-publish; re-dispatch for stuck jobs
  cache.py           Redis read-through cache with tag invalidation (+ graceful degradation)
  stats.py           the cached aggregation (`sales_summary`)
  mail.py            SMTP mailer + in-memory mailer for tests
  storage.py         artifact filesystem (Docker volume)
  models.py          jobs, dead_letters, artifacts, campaigns, campaign_recipients, sales
  tasks/
    base.py          TrackedTask: mirrors execution into `jobs`, dead-letters final failures
    reports.py       reportlab PDF → artifact → chained email
    email.py         send with backoff; campaign fan-out with idempotent completion
    media.py         Pillow: WebP re-encode + thumbnail; timeout + corrupt-file handling
    maintenance.py   Beat targets: scheduled report, artifact cleanup, stuck-job requeue
  api/routes.py      /jobs /reports /uploads /campaigns /artifacts /stats /sales /dead-letters
alembic/             migrations
tests/               real in-process Celery worker + Postgres + fakeredis
```

### Job tracking

`jobs.enqueue()` inserts a `jobs` row and **commits before publishing**. Publishing first would
let a fast worker start before the row is visible. The task receives `job_id` in its kwargs and
`TrackedTask` updates the row from Celery's lifecycle hooks:

| Hook           | Effect on `jobs`                                                                      |
| -------------- | ------------------------------------------------------------------------------------- |
| `before_start` | `running`, `attempts += 1`, `celery_task_id`, log context bound (`job_id`, `task_id`) |
| `on_retry`     | `retrying`, `error`                                                                   |
| `on_success`   | `succeeded`, `result` (JSONB)                                                         |
| `on_failure`   | `failed`, `error` + `INSERT dead_letters`                                             |

Beat-scheduled work goes through `maintenance.enqueue_scheduled` so it gets a job row too — a
6 a.m. report shows up in `/jobs` exactly like one requested through the API.

### Retries with exponential backoff

```python
@celery_app.task(
    autoretry_for=(smtplib.SMTPException, ConnectionError, TimeoutError),
    retry_backoff=True,        # 1s, 2s, 4s, 8s …
    retry_backoff_max=600,     # …capped at 10 minutes
    retry_jitter=True,         # randomised so a burst doesn't retry in lock-step
    max_retries=5,
)
```

Only _transient_ errors are retried. A corrupt image raises `UnidentifiedImageError`, which is
excluded via `dont_autoretry_for` — retrying a deterministic failure just delays the dead
letter (`tests/test_tasks.py::test_corrupt_image_fails_fast_without_retries`).

### Dead-letter queue

After the last retry, `on_failure` writes a `dead_letters` row with the task name, full
args/kwargs, error, traceback and attempt count. `GET /dead-letters` lists them,
`POST /dead-letters/{id}/replay` re-publishes the original message (and resets the linked job).
The replay test simulates an SMTP outage, watches the task exhaust 6 attempts, "fixes" the
mailer, replays, and asserts the same job row ends `succeeded`.

### Timeouts

Global `task_soft_time_limit=120 / task_time_limit=180`, tightened per task (`emails.send`:
30/45 s, `media.process_image`: 60/90 s). Tasks catch `SoftTimeLimitExceeded`, delete partial
output and raise a domain error that is _not_ in `autoretry_for`, so a pathological input fails
once and goes to the DLQ instead of timing out five times.

### At-least-once delivery, idempotent tasks

`task_acks_late=True` + `task_reject_on_worker_lost=True`: a task is acknowledged after it
finishes, so a worker killed mid-task hands the message back. That means tasks can run twice,
so they are written to tolerate it — `emails.send` checks `campaign_recipients.sent_at` before
sending. Beat's `requeue_stuck_jobs` (every 5 min) re-publishes jobs stuck in `running` longer
than `STUCK_JOB_SECONDS`.

### Campaign fan-out

`emails.send_campaign` expands into a Celery `group` of `emails.send` tasks, one per recipient.
A `chord` was deliberately avoided: its callback never fires if one header task fails
permanently. Instead every send — on success _or_ final failure — counts outstanding recipients
and closes the campaign when it hits zero. Per-recipient failures land in
`campaign_recipients.error`; counts use atomic `UPDATE … SET sent_count = sent_count + 1`.
The email task carries a per-worker `rate_limit` (`EMAIL_RATE_LIMIT`, default `120/m`).

### Beat schedule

| Entry                       | Schedule        | Task                                                     |
| --------------------------- | --------------- | -------------------------------------------------------- |
| `daily-sales-report`        | `06:00` daily   | PDF for the last day, emailed to `REPORT_RECIPIENTS`     |
| `weekly-sales-report`       | Mondays `07:00` | 7-day roll-up                                            |
| `cleanup-expired-artifacts` | hourly at `:30` | delete files + rows older than `ARTIFACT_RETENTION_DAYS` |
| `requeue-stuck-jobs`        | every 5 min     | re-publish jobs stuck in `running`                       |

### Redis cache with tag invalidation

```python
@cached(ttl=60, tags=["sales"])
def sales_summary(session, *, region=None, days=30): ...


cache.invalidate("sales")  # in POST /sales
```

Each cached value is stored under `cache:<ns>:<sha1(args)>` and its key is added to a Redis
set per tag. Invalidating a tag deletes every key in the set in one pipeline — a write never
needs to know which `(region, days)` combinations were cached. The HTTP layer reports what
happened in an `X-Cache: HIT|MISS` header. If Redis is down the decorator logs a warning and
falls through to the database (`test_cache_outage_degrades_gracefully`).

### Queues

| Queue     | Tasks                  | Why separate                            |
| --------- | ---------------------- | --------------------------------------- |
| `reports` | PDF generation         | CPU-heavy, long; shouldn't block emails |
| `emails`  | send, campaign fan-out | high volume, rate-limited               |
| `media`   | image processing       | memory-heavy                            |
| `default` | maintenance            | tiny, must not starve                   |

One worker consumes all four in compose; in production you'd run one worker per queue with
different concurrency.

### Observability

structlog JSON on both the API (with `request_id`) and the workers (with `task`, `task_id`,
`job_id` bound in `before_start`). Sentry via `SENTRY_DSN` with the Celery integration
(`monitor_beat_tasks=True`). Flower on :5555 for live worker/queue state.

---

## Database schema

```
jobs ──< dead_letters   (SET NULL)          campaigns ──< campaign_recipients (CASCADE)
jobs ──< artifacts      (SET NULL)          sales
```

| Table                 | Notes                                                                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `jobs`                | `status` enum, `payload`/`result` JSONB, `attempts`; indexes `(status, updated_at)` for the stuck-job scan and `(kind, created_at)` for listing |
| `dead_letters`        | `args`/`kwargs` JSONB so a replay is byte-for-byte the original message; `replayed_at`                                                          |
| `artifacts`           | on-disk path relative to the storage root (path traversal is rejected), `size_bytes`, `meta` JSONB                                              |
| `campaign_recipients` | unique `(campaign_id, email)`; `sent_at` doubles as the idempotency marker                                                                      |
| `sales`               | demo data; indexes on `sold_at` and `region` for the aggregations                                                                               |

## Tests

```bash
TEST_DATABASE_URL=postgresql+psycopg://taskforge:taskforge@localhost:5432/taskforge_test pytest
```

The suite starts a **real Celery worker in-process** (solo pool, `memory://` broker) instead of
`task_always_eager`, so retries, `on_failure` and dead-lettering run through Celery's actual
tracer. Backoff is patched to zero, Redis is `fakeredis`, mail is an in-memory mailer that can
be told to fail N times. 16 tests cover: cache hit/miss/invalidate/outage, retry-then-succeed
(attempts = 3), exhaust-then-DLQ-then-replay (attempts = 6), PDF generation + email attachment,
image compression + thumbnail, corrupt image fails once, soft-time-limit handling, campaign
fan-out with a permanently failing recipient, stuck-job requeue, Beat schedule sanity.

## CI

`ruff check` → `ruff format --check` → `pytest` (Postgres service) → `docker compose build`.

## Running without Docker

```bash
pip install -e ".[dev]"
cp .env.example .env          # point at your Postgres/Redis, MAIL_BACKEND=memory if no SMTP
alembic upgrade head
python -m scripts.seed
uvicorn app.main:app --reload
celery -A app.celery_app:celery_app worker -l INFO -Q default,reports,emails,media --pool=solo   # --pool=solo on Windows
celery -A app.celery_app:celery_app beat -l INFO
```
