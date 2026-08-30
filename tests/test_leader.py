"""Leader election, asserted against a real Postgres rather than reasoned about.

The claim being tested is narrow and load-bearing: **two conductors cannot both
hold the lock, and losing the session releases it.** Everything the design says
about fencing follows from that -- the writes are fenced because they go through
the connection the lock lives on, so "lost the lock" and "cannot write" are the
same event by construction.

These use their own connections rather than the rolled-back fixture: an advisory
lock is a property of a *session*, so a test that took it on one connection
would prove nothing about a second.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.conductor.leader import LEADER_LOCK_KEY, LeaderLock
from app.core import db
from tests.conftest import requires_db

pytestmark = requires_db

#: Not the real key. The compose stack may well have a live conductor holding
#: that one against the same database, and a test that fails because the product
#: is running correctly is a bad test.
TEST_LOCK_KEY = 0x5748_424B_7E57


@pytest_asyncio.fixture
async def _engine_installed(engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point ``db.get_engine`` at the test engine.

    ``LeaderLock`` reaches for the process-wide engine through
    ``dedicated_connection``, which is the right shape for production and means
    a test has to say which engine it means.
    """
    monkeypatch.setattr(db, "get_engine", lambda: engine)


async def test_only_one_holder_at_a_time(_engine_installed: None) -> None:
    """The property the singleton rests on."""
    first, second = LeaderLock(TEST_LOCK_KEY), LeaderLock(TEST_LOCK_KEY)
    try:
        assert await first.acquire() is not None
        assert first.is_leader

        assert await second.acquire() is None, "two conductors both believed they were leader"
        assert not second.is_leader
    finally:
        await first.release()
        await second.release()


async def test_releasing_lets_the_standby_take_over(_engine_installed: None) -> None:
    """Failover, in the one form the demo can actually show: a graceful stop.

    An ungraceful kill is the same event from Postgres's point of view -- the
    session ends and the lock goes with it -- which is why there is no lease to
    wait out and no failure detector anywhere in this system.
    """
    leader, standby = LeaderLock(TEST_LOCK_KEY), LeaderLock(TEST_LOCK_KEY)
    try:
        assert await leader.acquire() is not None
        assert await standby.acquire() is None

        await leader.release()
        assert not leader.is_leader

        assert await standby.acquire() is not None, "the lock outlived the session that held it"
    finally:
        await leader.release()
        await standby.release()


async def test_the_leader_writes_through_the_locked_connection(_engine_installed: None) -> None:
    """Fencing is automatic because there is only one connection to write on.

    If ``acquire`` handed back a lock and the caller wrote through the pool, a
    demoted leader could still write. It hands back the connection instead.
    """
    lock = LeaderLock(TEST_LOCK_KEY)
    try:
        conn = await lock.acquire()
        assert conn is not None
        held = await conn.scalar(
            text(
                "SELECT count(*) FROM pg_locks "
                "WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND objid = :key"
            ),
            {"key": TEST_LOCK_KEY & 0xFFFFFFFF},
        )
        assert held == 1, "the connection handed to the caller is not the one holding the lock"
    finally:
        await lock.release()


async def test_a_released_lock_is_reacquirable_by_the_same_process(_engine_installed: None) -> None:
    """A conductor that hit an error and dropped its connection retries as a standby."""
    lock = LeaderLock(TEST_LOCK_KEY)
    try:
        assert await lock.acquire() is not None
        await lock.release()
        assert await lock.acquire() is not None
    finally:
        await lock.release()


def test_the_real_key_is_not_the_test_key() -> None:
    """Guards the test above from silently fighting a running conductor."""
    assert TEST_LOCK_KEY != LEADER_LOCK_KEY
