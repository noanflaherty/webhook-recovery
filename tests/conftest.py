"""Test fixtures.

Every test runs inside a transaction that is rolled back afterwards, so tests
share one migrated database and never see each other's rows. The fairness tests
depend on that isolation: a subtly wrong scheduler still draws a plausible
chart, so they are only worth anything if the rows they assert on are their
own.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.core.clock import start_config, wall_now
from app.core.models import Base, Simulation
from app.core.settings import normalize_database_url

#: Marks simulations created by the test suite. Distinct from anything the
#: product writes, so a sweep can never take a real row with it.
FIXTURE_SCENARIO = "pytest-fixture"

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


@pytest_asyncio.fixture
async def client(session: AsyncSession) -> AsyncIterator[AsyncClient]:
    """The api, served in-process against the rolled-back session.

    The dependency override is what makes it share the fixture's transaction:
    without it the routes would open their own connections, commit for real, and
    leave rows behind for the next test to trip over.
    """
    from app.api.main import create_app
    from app.core import db

    app = create_app()

    async def _session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[db.get_session] = _session
    transport = ASGITransport(app=app)
    # No lifespan: it would start the producer, which emits into every running
    # simulation and would make every count in every test a moving target.
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


def a_simulation(**overrides: object) -> Simulation:
    """A running simulation at virtual time zero.

    ``speed_multiplier=1.0`` unless overridden: tests assert on virtual
    timestamps, and at 20x a test that takes 50ms of wall time has moved a
    virtual second, which turns an exact assertion into a flaky one.

    The scenario name is :data:`FIXTURE_SCENARIO` so that the one test file that
    genuinely commits -- ``test_claim.py``, which needs rows two transactions can
    both see -- can find and sweep its own strays after an interrupted run.
    """
    config = start_config(1.0)
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "created_at_wall": wall_now(),
        "scenario_name": FIXTURE_SCENARIO,
        "status": config.status.value,
        "virtual_epoch": config.virtual_epoch,
        "resumed_at_wall": config.resumed_at_wall,
        "paused_at_virtual": None,
        "speed_multiplier": config.speed_multiplier,
        "fair_drain_enabled": True,
        "global_attempts_per_s": 30.0,
        "outage_override": None,
    }
    fields.update(overrides)
    return Simulation(**fields)
