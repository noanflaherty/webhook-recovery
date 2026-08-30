"""Test fixtures.

The session fixture exists now so that Phase 2's fairness tests -- the ones that
genuinely earn their keep, because a subtly wrong scheduler still draws a
plausible chart -- are a file to add rather than infrastructure to build under
time pressure.

Every test runs inside a transaction that is rolled back afterwards, so tests
share one migrated database and never see each other's rows.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.core.models import Base
from app.core.settings import normalize_database_url

#: Defaults to the compose postgres. Safe to share with development data:
#: every test runs inside a transaction that is rolled back.
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5433/webhook_recovery",
)


def _database_available(url: str) -> bool:
    import socket
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    if parts.hostname is None:
        return False
    try:
        with socket.create_connection((parts.hostname, parts.port or 5432), timeout=0.5):
            return True
    except OSError:
        return False


requires_db = pytest.mark.skipif(
    not _database_available(TEST_DATABASE_URL),
    reason=f"no postgres reachable at {TEST_DATABASE_URL}",
)


@pytest_asyncio.fixture(scope="session")
async def engine() -> AsyncIterator[AsyncEngine]:
    from sqlalchemy.ext.asyncio import create_async_engine

    eng = create_async_engine(normalize_database_url(TEST_DATABASE_URL), poolclass=NullPool)
    async with eng.begin() as conn:
        # create_all rather than `alembic upgrade head`: the migration's fidelity
        # to the models is asserted separately by `alembic check`, and tests
        # should fail on a model bug, not on a migration that lags it.
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def connection(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """An outer transaction that is always rolled back."""
    async with engine.connect() as conn:
        transaction = await conn.begin()
        try:
            yield conn
        finally:
            await transaction.rollback()


@pytest_asyncio.fixture
async def session(connection: AsyncConnection) -> AsyncIterator[AsyncSession]:
    """A session bound to the rolled-back outer transaction.

    ``join_transaction_mode="create_savepoint"`` lets code under test call
    ``commit()`` normally -- it commits to a savepoint inside the outer
    transaction, which the fixture then discards.
    """
    maker = async_sessionmaker(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    async with maker() as s:
        yield s
