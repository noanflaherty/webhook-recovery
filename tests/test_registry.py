"""Process registry growth is bounded.

The registry is observability only, so none of this is about correctness. It is
about the table not growing without limit: the original "stale rows accumulate
harmlessly" decision assumed a 45-second demo run, and a continuously-running
deployment filled a Postgres volume.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import registry
from app.core.clock import wall_now
from app.core.enums import ProcessKind
from app.core.models import Process
from app.core.settings import get_settings
from tests.conftest import requires_db

pytestmark = requires_db


#: The fixtures below tag their rows with this, so the assertions count only
#: what the test created. The database is shared with a running compose stack
#: whose live processes are legitimately in this table and must not be counted.
TEST_HOST = "prune-test-host"


def _stale_by(seconds: float) -> Process:
    now = wall_now()
    return Process(
        id=uuid.uuid4(),
        kind=ProcessKind.WORKER.value,
        hostname=TEST_HOST,
        pid=1,
        started_at_wall=now - timedelta(seconds=seconds),
        last_heartbeat_wall=now - timedelta(seconds=seconds),
        is_leader=False,
    )


async def _count(session: AsyncSession) -> int:
    return int(
        await session.scalar(select(func.count()).select_from(Process).where(Process.hostname == TEST_HOST))
        or 0
    )


async def test_long_dead_rows_are_pruned(session: AsyncSession) -> None:
    window = get_settings().process_liveness_window_s
    cutoff = window * registry.PRUNE_WINDOW_MULTIPLE

    ancient = _stale_by(cutoff * 2)
    session.add(ancient)
    await session.commit()

    await registry._prune_dead(session, wall_now())
    await session.commit()

    assert await session.get(Process, ancient.id) is None


async def test_recent_rows_survive(session: AsyncSession) -> None:
    """Pruning must not eat rows a reviewer might still want to look at.

    A process that died a minute ago is already invisible to /api/process --
    the read-time filter handles that -- and deleting it would throw away the
    only evidence of a restart that just happened.
    """
    window = get_settings().process_liveness_window_s
    recent = _stale_by(window * 4)
    live = _stale_by(0)
    session.add_all([recent, live])
    await session.commit()

    await registry._prune_dead(session, wall_now())
    await session.commit()

    assert await session.get(Process, recent.id) is not None
    assert await session.get(Process, live.id) is not None


async def test_registration_bounds_the_table(session: AsyncSession) -> None:
    """Registering repeatedly must not grow the table without limit."""
    window = get_settings().process_liveness_window_s
    cutoff = window * registry.PRUNE_WINDOW_MULTIPLE

    session.add_all([_stale_by(cutoff * 3) for _ in range(25)])
    await session.commit()
    assert await _count(session) >= 25

    await registry._prune_dead(session, wall_now())
    await session.commit()

    assert await _count(session) == 0
