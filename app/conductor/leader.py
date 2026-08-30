"""Leader election, via ``pg_try_advisory_lock``.

The conductor is a singleton by necessity, not by convenience: admission is a
read-modify-write over a sliding attempt window, and two conductors running it
concurrently both read the same window and both admit against it. So the point
of leader election is not availability theatre -- it is what makes the
arithmetic correct.

**The lock is held on the same connection every conductor write goes through.**
That is the whole design. A Postgres advisory lock is session-scoped, so if the
session dies the lock is released *and* the connection that would have done the
writing is gone with it. Fencing is therefore automatic rather than something we
implement: there is no window in which a demoted leader can still write, because
losing the lock and losing the ability to write are the same event.

The alternative -- a lock table with an expiry the conductor refreshes -- needs a
fencing token on every write to be safe, because a process that stalls past its
lease can wake up believing it still holds one. Advisory locks make that class of
bug unrepresentable rather than merely tested for.
"""

from __future__ import annotations

import contextlib
import logging
from contextlib import AsyncExitStack
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.db import dedicated_connection

log = logging.getLogger(__name__)

#: Advisory locks share one namespace per database, so this only has to not
#: collide with anything else using the same database. One key for the whole
#: process, not one per simulation: a pass schedules across every running
#: simulation against a shared attempt-rate budget, and per-simulation locks
#: would let two conductors interleave passes over it.
LEADER_LOCK_KEY: Final = 0x5748_424B  # "WHBK"

_TRY_LOCK = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


class LeaderLock:
    """Hold leadership, and hand out the connection it is held on.

    Acquired once and kept across iterations rather than taken per pass. Both
    are correct -- a pass is a single read-modify-write and the lock spans it
    either way -- but re-taking it every 50ms makes the leader badge in the
    process strip flap, which reads as broken.
    """

    __slots__ = ("_conn", "_key", "_stack")

    def __init__(self, key: int = LEADER_LOCK_KEY) -> None:
        self._key = key
        self._stack: AsyncExitStack | None = None
        self._conn: AsyncConnection | None = None

    @property
    def is_leader(self) -> bool:
        return self._conn is not None

    async def acquire(self) -> AsyncConnection | None:
        """The fenced connection if we lead, ``None`` if someone else does.

        Standbys land here every tick. ``pg_try_advisory_lock`` never blocks, so
        a standby costs one cheap round trip per pass and can take over the
        instant the leader's session ends -- including when it ends because the
        process was killed, since that closes the socket and Postgres drops the
        lock with it. No lease to wait out, no failure detector.
        """
        if self._conn is not None:
            return self._conn

        stack = AsyncExitStack()
        conn = await stack.enter_async_context(dedicated_connection())
        try:
            acquired = bool(await conn.scalar(_TRY_LOCK, {"key": self._key}))
        except Exception:
            await stack.aclose()
            raise

        if not acquired:
            await stack.aclose()
            return None

        self._stack, self._conn = stack, conn
        log.info("acquired leadership (advisory lock %#x)", self._key)
        return conn

    async def release(self) -> None:
        """Give up leadership and close the connection holding it.

        Called on shutdown, and on any error from the fenced connection: a
        connection that just raised may or may not still hold the lock, and
        closing it settles the question rather than leaving behind a leader that
        cannot write.
        """
        stack, conn = self._stack, self._conn
        self._stack, self._conn = None, None
        if stack is None or conn is None:
            return

        log.info("released leadership")
        # Closing the connection is what actually drops the lock. The explicit
        # unlock is politeness for a connection that goes back to the pool, and
        # is suppressed because the connection may be exactly why we are here.
        with contextlib.suppress(Exception):
            await conn.execute(_UNLOCK, {"key": self._key})
        await stack.aclose()


__all__ = ["LEADER_LOCK_KEY", "LeaderLock"]
