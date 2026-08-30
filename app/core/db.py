"""Async engine, sessionmaker, and the session context manager.

One engine per process. The conductor needs a *dedicated* connection for its
advisory lock -- held on the same session it writes through, so that losing the
lock means losing the ability to write and fencing is automatic rather than
something we implement. :func:`dedicated_connection` is that seam.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.settings import Settings, get_settings

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def create_engine(settings: Settings | None = None) -> AsyncEngine:
    s = settings or get_settings()
    return create_async_engine(
        s.async_database_url,
        echo=s.db_echo,
        pool_size=s.db_pool_size,
        max_overflow=s.db_max_overflow,
        pool_pre_ping=True,
    )


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_engine()
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _sessionmaker


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """A transactional session: commit on clean exit, roll back on exception."""
    async with get_sessionmaker()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency."""
    async with session_scope() as session:
        yield session


@asynccontextmanager
async def dedicated_connection() -> AsyncIterator[AsyncConnection]:
    """A connection checked out for the lifetime of the caller.

    The conductor's leader lock lives here: ``pg_try_advisory_lock`` is tied to
    a Postgres *session*, so the lock and the writes it fences must share one
    connection. Conductor writes go through ``AsyncSession(bind=conn)`` on this
    connection, never ``session_scope()`` -- otherwise losing the lock does not
    lose the ability to write, and the fencing claim is false.
    """
    async with get_engine().connect() as conn:
        yield conn


async def ping() -> bool:
    """``SELECT 1``. Backs ``GET /api/health``'s ``db`` field."""
    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        return False
    return True


async def dispose_engine() -> None:
    """Close the pool. Called on shutdown so SIGTERM drains cleanly."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None


__all__ = [
    "create_engine",
    "dedicated_connection",
    "dispose_engine",
    "get_engine",
    "get_session",
    "get_sessionmaker",
    "ping",
    "session_scope",
]
