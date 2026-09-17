FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml ./
RUN pip install --upgrade pip && pip install $(python -c "import tomllib; print(' '.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))")
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
COPY scripts ./scripts
COPY docker-entrypoint.sh ./
RUN pip install --no-deps -e . && chmod +x docker-entrypoint.sh

RUN useradd --create-home --uid 1000 app && mkdir -p /app/storage && chown -R app:app /app
USER app
EXPOSE 8000

# The same image runs the API, the worker, beat and flower: see docker-entrypoint.sh.
ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["api"]
