"""The demo producer: simulation scaffolding behind a real interface.

In production the callers of ingest are the provider's own internal services.
Here a background task in the api process plays them, on the Stripe-flavoured
event mix in ``app.core.scenario``. It calls ``ingest_events`` directly rather
than looping back over HTTP: the same code path the route runs, without a socket
in the middle, and a tick's events batch into one transaction.

**The producer never stops during an outage.** That is the whole point -- the
provider keeps emitting, the ledger keeps accepting, and only *delivery* is
down. Backlogs climbing through the outage is the direct consequence.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.core.clock import VIRTUAL_EPOCH_ZERO, SimulationClockConfig, VirtualClock
from app.core.db import session_scope
from app.core.enums import SimStatus
from app.core.models import Event, Simulation
from app.core.scenario import EVENT_MIX, EventTypeSpec, entity_key, payload_for
from app.core.settings import get_settings

log = logging.getLogger(__name__)


@dataclass
class _SimulationState:
    """Per-simulation producer bookkeeping, held in memory.

    ``last_emitted`` is recovered from ``MAX(event.occurred_at)`` on first
    sight, so an api restart resumes where it left off instead of re-emitting
    the whole run. The RNG is not recovered: the producer is a traffic
    generator, not a fixture, and nothing downstream claims reproducibility
    across a restart. The *transport* is the piece that is deterministic per
    attempt, and it seeds itself from the row.
    """

    rng: random.Random
    last_emitted: datetime | None = None
    #: Monotonic counter behind the unique-key event types, so two events a
    #: microsecond apart cannot collide on a key that is supposed to be unique.
    sequence: int = field(default=0)


class Producer:
    """Emits the event mix into every running simulation."""

    def __init__(self) -> None:
        self._state: dict[uuid.UUID, _SimulationState] = {}

    async def tick(self) -> None:
        async with session_scope() as session:
            sims = (
                (
                    await session.execute(
                        select(Simulation).where(Simulation.status == SimStatus.RUNNING.value)
                    )
                )
                .scalars()
                .all()
            )
            for sim in sims:
                await self._emit_for(session, sim)

        # Simulations that ended or were deleted should not pin memory.
        live = {sim.id for sim in sims}
        for stale in set(self._state) - live:
            del self._state[stale]

    async def _emit_for(self, session: AsyncSession, sim: Simulation) -> None:
        settings = get_settings()
        clock = VirtualClock(SimulationClockConfig.from_row(sim))
        if clock.is_paused:
            return
        now = clock.now()

        state = self._state.get(sim.id)
        if state is None:
            state = _SimulationState(rng=random.Random(sim.id.int))
            state.last_emitted = await _last_emitted(session, sim.id)
            self._state[sim.id] = state

        # VIRTUAL_EPOCH_ZERO, never `sim.virtual_epoch`: the latter is rebased on
        # every pause, resume and speed change, so it is not the start of the run.
        since = state.last_emitted or VIRTUAL_EPOCH_ZERO
        window_s = (now - since).total_seconds()
        if window_s <= 0:
            return
        if window_s > settings.producer_max_catchup_virtual_s:
            # An api restart against a long-running simulation would otherwise
            # emit the whole gap in one burst, which reads as a spike the
            # scenario never scripted.
            window_s = settings.producer_max_catchup_virtual_s
            since = now - timedelta(seconds=window_s)

        specs = _draw(state, window_s, since)
        if specs:
            await ingest_events(session, sim, specs)
        state.last_emitted = now


def _draw(state: _SimulationState, window_s: float, since: datetime) -> list[EventSpec]:
    """Draw one window's worth of events from the mix.

    The fractional part of the expected count is resolved by a coin flip rather
    than truncated. Truncating would bias every rate downward -- at a 0.1s tick
    and 20x, a window is ~2 virtual seconds and `invoice.paid` expects 1.6
    events, so rounding down would lose 37% of Clover's traffic and quietly make
    the low-volume consumer even lower-volume than the scenario says.
    """
    specs: list[EventSpec] = []
    for spec in EVENT_MIX:
        expected = spec.rate_per_virtual_s * window_s
        count = int(expected)
        if state.rng.random() < expected - count:
            count += 1
        for _ in range(count):
            specs.append(_one(state, spec, since, window_s))

    specs.sort(key=lambda s: s.occurred_at or since)
    return specs


def _one(
    state: _SimulationState,
    spec: EventTypeSpec,
    since: datetime,
    window_s: float,
) -> EventSpec:
    state.sequence += 1
    draw = state.rng.randrange(spec.key_pool) if spec.key_pool is not None else state.sequence
    key = entity_key(spec, draw)
    # Spread across the window rather than stacking on its edge, so arrival
    # looks like a stream rather than a comb.
    occurred_at = since + timedelta(seconds=state.rng.uniform(0.0, window_s))
    return EventSpec(
        event_type=spec.event_type,
        entity_key=key,
        payload=payload_for(spec, key, occurred_at),
        occurred_at=occurred_at,
    )


async def _last_emitted(session: AsyncSession, simulation_id: uuid.UUID) -> datetime | None:
    latest: datetime | None = await session.scalar(
        select(func.max(Event.occurred_at)).where(Event.simulation_id == simulation_id)
    )
    return latest


async def run_producer(stop: asyncio.Event) -> None:
    """Tick until stopped. Started from the api's lifespan.

    Failures are logged and the loop continues: the producer is scaffolding, and
    an api that stops serving reads because the traffic generator hit a bad
    iteration is strictly worse than one that just stops generating traffic.
    """
    producer = Producer()
    interval = get_settings().producer_tick_interval_s
    log.info("producer running (every %.2fs real)", interval)
    while not stop.is_set():
        try:
            await producer.tick()
        except Exception:
            log.exception("producer tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue
    log.info("producer stopped")


__all__ = ["Producer", "run_producer"]
