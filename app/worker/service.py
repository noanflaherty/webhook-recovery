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
import time
import uuid
from typing import Any

from sqlalchemy import Row, select

from app.core.clock import SimulationClockConfig, VirtualClock
from app.core.db import session_scope
from app.core.enums import SimStatus
from app.core.models import Simulation
from app.core.runner import ProcessRunner
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

#: How long a worker asked to die will hold out for a batch to die in the middle
#: of, before dying anyway.
#:
#: The mid-batch death is the one worth simulating -- it is the only way to
#: strand a lease -- so a kill waits for one rather than taking the first
#: opportunity, which between batches would strand nothing and prove nothing.
#: But it cannot wait forever: a worker with no running simulation, a paused
#: run, or simply a long unlucky streak of empty claims would leave the request
#: pending indefinitely, and a control that silently does nothing is worse than
#: no control. Several seconds is hundreds of claim attempts at a 20ms loop, so
#: reaching this bound means there was genuinely no work to strand.
_CRASH_GRACE_S = 5.0


class Worker:
    """Drains the ready buffer of every running simulation."""

    def __init__(
        self,
        transport: ConsumerTransport | None = None,
        runner: ProcessRunner | None = None,
    ) -> None:
        #: Injected in tests. The default is built per simulation, because the
        #: transport sleeps through *that* simulation's clock.
        self._transport = transport
        #: Held only to read its crash flag. A worker that is not driven by a
        #: runner -- every test, and any direct use -- simply cannot be killed.
        self._runner = runner
        #: When this worker first noticed it had been asked to die with no batch
        #: in hand. See :data:`_CRASH_GRACE_S`.
        self._crash_seen_at: float | None = None

    async def run_once(self, process_id: uuid.UUID) -> None:
        async with session_scope() as session:
            sims = list(
                await session.execute(
                    select(*_SIM_COLUMNS).where(Simulation.status == SimStatus.RUNNING.value)
                )
            )

        for sim in sims:
            await self._drain(sim, process_id)

        # Nothing was stranded this iteration, so this is the reluctant path:
        # start the clock, and give up only once it runs out.
        self._honour_crash_request(stranding=False)

    def _honour_crash_request(self, *, stranding: bool) -> None:
        """Act on a pending kill, preferring to die with work in hand.

        ``stranding`` says a batch has been claimed and not yet completed, which
        is the state the whole control exists to reach -- taken immediately.
        Otherwise the worker holds out for one, and dies anyway once
        :data:`_CRASH_GRACE_S` has passed without ever getting one.
        """
        if self._runner is None or not self._runner.crash.is_set():
            self._crash_seen_at = None
            return
        if stranding:
            self._runner.die()
            return
        now = time.monotonic()
        if self._crash_seen_at is None:
            self._crash_seen_at = now
        elif now - self._crash_seen_at >= _CRASH_GRACE_S:
            log.warning("no batch to strand after %.0fs; dying anyway", _CRASH_GRACE_S)
            self._runner.die()

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

        # Here, with the claim committed and the completion not yet run, is the
        # state the whole control exists to reach: a full batch of leases that
        # nothing is going to answer for. A crash taken at an arbitrary moment
        # would usually strand nothing -- a batch is a few milliseconds inside a
        # 20ms loop, so an arbitrary moment is most likely an idle one.
        self._honour_crash_request(stranding=True)

        # No transaction, no row locks: this is the part that sleeps.
        results = await asyncio.gather(*(transport.attempt(c.request) for c in claimed))

        # Transaction two: record every outcome. `clock.now()` is re-read
        # because virtual time has genuinely moved on during the attempts --
        # that is what the sleep was for.
        async with session_scope() as session:
            await complete_batch(
                session,
                worker_id,
                [Completion(claimed=c, result=r) for c, r in zip(claimed, results, strict=True)],
                clock.now(),
            )

        log.debug("delivered batch of %d for simulation %s", len(claimed), sim.id)


__all__ = ["Claimed", "Worker"]
