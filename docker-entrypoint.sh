#!/bin/bash
# One image, several roles. `PROCESS` (or the first argument) picks what this container runs:
#   api     migrations + uvicorn            (honours $PORT for PaaS platforms)
#   worker  celery worker on all queues
#   beat    celery beat scheduler
#   flower  celery monitoring UI on 5555
#   all     api + worker + beat in one container (single-volume hosts such as Railway, where
#           the artifact volume can only be attached to one service)
set -e
# An explicit PROCESS variable wins over the image default (CMD ["api"]).
ROLE="${PROCESS:-${1:-api}}"
PORT="${PORT:-8000}"
CELERY="celery -A app.celery_app:celery_app"

case "$ROLE" in
  api)
    alembic upgrade head
    exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT" ;;
  worker)
    exec $CELERY worker --loglevel=INFO --concurrency="${CELERY_CONCURRENCY:-4}" -Q default,reports,emails,media ;;
  beat)
    exec $CELERY beat --loglevel=INFO ;;
  flower)
    exec $CELERY flower --port="${PORT:-5555}" ;;
  all)
    alembic upgrade head
    $CELERY worker --loglevel=INFO --concurrency="${CELERY_CONCURRENCY:-2}" -Q default,reports,emails,media &
    $CELERY beat --loglevel=INFO &
    uvicorn app.main:app --host 0.0.0.0 --port "$PORT" &
    # If any process dies, take the container down so the platform restarts it.
    wait -n
    exit 1 ;;
  *)
    exec "$@" ;;
esac
