"""Synchronous SQLAlchemy: Celery workers are synchronous, and FastAPI runs `def` endpoints in a
threadpool, so one engine/session factory serves both processes."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


engine = create_engine(get_settings().database_url, pool_pre_ping=True, pool_size=5)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


@contextmanager
def session_scope() -> Iterator[Session]:
    """Commit-or-rollback unit of work for code outside the request cycle (tasks, scripts)."""
    with SessionLocal() as session:
        try:
            yield session
            session.commit()
        except BaseException:
            session.rollback()
            raise
