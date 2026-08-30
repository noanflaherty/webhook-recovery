"""One worker iteration: claim, deliver, complete.

Workers are the data plane and contain **no policy logic** -- they execute
decisions the conductor already made. There is exactly one exception, in
:func:`app.worker.claim.claim_batch`: a final ``max_staleness`` re-check
immediately before attempting, because an event can go stale in the gap between
being admitted and being reached. It is one comparison, sharing the conductor's
own predicate. Coalescing stays conductor-only because it needs a query.

Nothing here consults the process registry, or knows how many other workers
exist. ``SKIP LOCKED`` is the entire coordination mechanism: N workers, disjoint
sets, no partitioning scheme and no coordinator to be a single point of failure.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from sqlalchemy import Row, select

from app.core.clock import SimulationClockConfig, VirtualClock
from app.core.db import session_scope
from app.core.enums import SimStatus
from app.core.models import Simulation
from app.core.settings import get_settings
from app.worker.claim import Claimed, Completion, claim_batch, complete_batch
from app.worker.transport import ConsumerTransport, SimulatedTransport

log = logging.getLogger(__name__)

_SIM_COLUMNS = (
    Simulation.id,
    Simulation.status,
    Simulation.virtual_epoch,
    Simulation.resumed_at_wall,
    Simulation.paused_at_virtual,
    Simulation.speed_multiplier,
)


class Worker:
    """Drains the ready buffer of every running simulation."""

    def __init__(self, transport: ConsumerTransport | None = None) -> None:
        #: Injected in tests. The default is built per simulation, because the
        #: transport sleeps through *that* simulation's clock.
        self._transport = transport

    async def run_once(self, process_id: uuid.UUID) -> None:
        async with session_scope() as session:
            sims = list(
                await session.execute(
                    select(*_SIM_COLUMNS).where(Simulation.status == SimStatus.RUNNING.value)
                )
            )

        for sim in sims:
            await self._drain(sim, process_id)

    async def _drain(self, sim: Row[Any], worker_id: uuid.UUID) -> None:
        clock = VirtualClock(SimulationClockConfig.from_row(sim))
        if clock.is_paused:
            return
        transport = self._transport or SimulatedTransport(clock)
        batch_size = get_settings().worker_batch_size

        # Transaction one: take the work and record that the attempts started.
        async with session_scope() as session:
            claimed = await claim_batch(session, sim.id, worker_id, clock.now(), batch_size)
        if not claimed:
            return

        # No transaction, no row locks: this is the part that sleeps.
        results = await asyncio.gather(*(transport.attempt(c.request) for c in claimed))

        # Transaction two: record every outcome. `clock.now()` is re-read
        # because virtual time has genuinely moved on during the attempts --
        # that is what the sleep was for.
        async with session_scope() as session:
            await complete_batch(
                session,
                [Completion(claimed=c, result=r) for c, r in zip(claimed, results, strict=True)],
                clock.now(),
            )

        log.debug("delivered batch of %d for simulation %s", len(claimed), sim.id)


__all__ = ["Claimed", "Worker"]
