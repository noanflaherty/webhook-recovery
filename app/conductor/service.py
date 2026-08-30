"""One conductor pass: lead, measure, admit.

The conductor **never creates work.** It only transitions rows ingest already
wrote, which is what keeps acceptance independent of scheduling -- events land in
the ledger whether or not a conductor is running, and a leaderless minute costs
throughput, never data.

Everything here goes through the connection the advisory lock is held on. Not
``session_scope()``, ever: a write that reaches the database on some other
connection is a write the lock does not fence, and the fencing claim would be
false rather than merely untested.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncConnection

from app.conductor.admission import compute_budget, mark_ready, select_candidates
from app.conductor.leader import LeaderLock
from app.conductor.metrics import MetricsWriter
from app.core.clock import SimulationClockConfig, VirtualClock
from app.core.enums import SimStatus
from app.core.models import Simulation
from app.core.runner import ProcessRunner
from app.core.scenario import is_outage

log = logging.getLogger(__name__)

#: Exactly the columns a pass needs: the five the clock derives from, plus the
#: three scheduling knobs. Selected explicitly rather than as an ORM entity so
#: the whole pass stays on the fenced connection with no session in sight.
_SIM_COLUMNS = (
    Simulation.id,
    Simulation.status,
    Simulation.virtual_epoch,
    Simulation.resumed_at_wall,
    Simulation.paused_at_virtual,
    Simulation.speed_multiplier,
    Simulation.global_attempts_per_s,
    Simulation.outage_override,
)


class Conductor:
    """The policy plane. One leader, however many replicas."""

    def __init__(self, runner: ProcessRunner | None = None) -> None:
        self._runner = runner
        self._lock = LeaderLock()
        self._metrics = MetricsWriter()

    async def run_once(self, process_id: uuid.UUID) -> None:
        """One pass. Matches ``LoopBody``, so ``ProcessRunner`` drives it."""
        try:
            conn = await self._lock.acquire()
        except Exception:
            self._mark_leader(False)
            raise

        if conn is None:
            # A standby. One cheap non-blocking round trip per tick, and ready
            # to take over the moment the leader's session ends.
            self._mark_leader(False)
            return
        self._mark_leader(True)

        try:
            for sim in await self._running_simulations(conn):
                await self._pass(conn, sim)
            await conn.commit()
        except Exception:
            # The connection may or may not still hold the lock, and may or may
            # not still be usable. Dropping it settles both questions: a standby
            # takes over, and this process retries as one on its next tick.
            await self._lock.release()
            self._mark_leader(False)
            raise

    async def aclose(self) -> None:
        """Wired to the runner's shutdown hook.

        The lock lives on a connection held *across* iterations, so there is no
        ``finally`` inside the loop body to release it from -- which is the whole
        reason the runner grew a teardown seam.
        """
        await self._lock.release()

    # -- internals ----------------------------------------------------------

    def _mark_leader(self, value: bool) -> None:
        if self._runner is not None:
            self._runner.mark_leader(value)

    async def _running_simulations(self, conn: AsyncConnection) -> list[Row[Any]]:
        result = await conn.execute(select(*_SIM_COLUMNS).where(Simulation.status == SimStatus.RUNNING.value))
        return list(result)

    async def _pass(self, conn: AsyncConnection, sim: Row[Any]) -> None:
        # Rebuilt every pass, not held: a clock is a frozen snapshot of five
        # columns and has no way to notice a pause on its own.
        clock = VirtualClock(SimulationClockConfig.from_row(sim))
        now = clock.now()
        elapsed = clock.elapsed_virtual_s()

        # Written before the outage check, deliberately: an outage is a period
        # with no attempts, not a period with no data, and a chart that goes
        # blank for five virtual minutes is not showing the backlog climbing.
        await self._metrics.write(conn, sim.id, now)

        if is_outage(elapsed, outage_override=sim.outage_override):
            # The delivery pipeline is down. Events keep landing in the ledger
            # and nothing is admitted -- this one branch is what makes backlogs
            # climb through the outage and drain after it.
            return

        budget = await compute_budget(conn, sim.id, sim.global_attempts_per_s, now)
        if budget.slots <= 0:
            return

        candidates = await select_candidates(conn, sim.id, now, budget.slots)
        admitted = await mark_ready(conn, candidates, now)
        if admitted:
            log.debug(
                "admitted %d (buffer=%d rate=%d) at virtual %.1fs",
                admitted,
                budget.buffer_slots,
                budget.rate_slots,
                elapsed,
            )


__all__ = ["Conductor"]
