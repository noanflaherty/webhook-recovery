"""The producer's rate arithmetic, which sets the shape of every chart.

Nothing downstream can correct a producer that emits at the wrong rate -- the
backlog curve, the catch-up times and the per-consumer volume ratios are all
consequences of it. And it fails quietly: a producer running 40% slow still
produces a plausible outage and a plausible recovery, just not the ones the
scenario describes or the frontend fixtures were drawn against.
"""

from __future__ import annotations

import random
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.producer import Producer, _draw, _SimulationState
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.models import Delivery, Event
from app.core.scenario import EVENT_MIX, seed_simulation
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db

#: Acme and Bolt take all four types; Clover takes `invoice.paid` alone.
EXPECTED_RATE = sum(spec.rate_per_virtual_s for spec in EVENT_MIX)


def test_the_emitted_rate_matches_the_configured_mix() -> None:
    """The fractional part of an expected count is a coin flip, not a truncation.

    At a 0.1s tick and 20x a window is ~2 virtual seconds, in which
    `invoice.paid` expects 1.6 events. Truncating would emit 1 every time --
    losing 37% of Clover's traffic and making the low-volume consumer quietly
    lower-volume than the scenario says, which reads as a fairness result rather
    than as an arithmetic bug.
    """
    state = _SimulationState(rng=random.Random(20260830))
    window_s = 2.0
    ticks = 2000

    emitted = sum(len(_draw(state, window_s, VIRTUAL_EPOCH_ZERO)) for _ in range(ticks))

    actual = emitted / (ticks * window_s)
    assert abs(actual - EXPECTED_RATE) < 0.05 * EXPECTED_RATE


def test_events_are_spread_across_the_window_not_stacked_on_its_edge() -> None:
    """Arrival should look like a stream. All-at-once would draw a comb."""
    state = _SimulationState(rng=random.Random(1))
    since = VIRTUAL_EPOCH_ZERO

    specs = _draw(state, window_s=10.0, since=since)

    times = [spec.occurred_at for spec in specs if spec.occurred_at is not None]
    assert len(times) == len(specs)
    assert times == sorted(times), "a batch should be handed to ingest in event order"
    assert all(since <= t <= since + timedelta(seconds=10.0) for t in times)
    assert len(set(times)) == len(times)


def test_pooled_types_repeat_their_keys_and_unique_types_do_not() -> None:
    """Coalescing needs something to collapse, and payments must never collapse.

    ``payment_intent.succeeded`` has no key pool at all, so "every payment is
    still delivered" is structural rather than a policy anyone has to remember
    not to configure.
    """
    state = _SimulationState(rng=random.Random(7))
    specs = _draw(state, window_s=200.0, since=VIRTUAL_EPOCH_ZERO)

    subs = [s.entity_key for s in specs if s.event_type == "customer.subscription.updated"]
    pays = [s.entity_key for s in specs if s.event_type == "payment_intent.succeeded"]

    assert len(subs) > len(set(subs)), "subscription keys never repeat -- nothing to coalesce"
    assert len(pays) == len(set(pays)), "a payment key repeated; coalescing could drop money"


async def test_a_tick_ledgers_and_fans_out(session: AsyncSession) -> None:
    """End to end through the real ingest path, not a mock of it."""
    sim = a_simulation(speed_multiplier=20.0)
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)

    producer = Producer()
    await producer._emit_for(session, sim)

    events = await session.scalar(
        select(func.count()).select_from(Event).where(Event.simulation_id == sim.id)
    )
    deliveries = await session.scalar(
        select(func.count()).select_from(Delivery).where(Delivery.simulation_id == sim.id)
    )
    assert events and events > 0
    assert deliveries and deliveries >= events, "every event should reach at least one consumer"


async def test_a_long_gap_is_clamped_rather_than_burst(session: AsyncSession) -> None:
    """An api restart against an old simulation must not emit the whole gap.

    Without the clamp the first tick after a redeploy emits minutes of traffic
    in one transaction -- a spike the scenario never scripted, arriving at
    whatever moment the deploy happened to land on.
    """
    from app.core.settings import get_settings

    sim = a_simulation()
    # A simulation whose clock is a long way past anything ever emitted for it.
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=600)
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)

    await Producer()._emit_for(session, sim)

    events = await session.scalar(
        select(func.count()).select_from(Event).where(Event.simulation_id == sim.id)
    )
    ceiling = EXPECTED_RATE * get_settings().producer_max_catchup_virtual_s
    assert events is not None
    assert events <= ceiling * 2, f"{events} events for a clamped window of {ceiling:.0f}"


async def test_a_paused_simulation_emits_nothing(session: AsyncSession) -> None:
    """A paused clock makes no progress, so there is no window to emit into."""
    from app.core.enums import SimStatus

    sim = a_simulation(status=SimStatus.PAUSED.value, paused_at_virtual=VIRTUAL_EPOCH_ZERO)
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)

    await Producer()._emit_for(session, sim)

    events = await session.scalar(
        select(func.count()).select_from(Event).where(Event.simulation_id == sim.id)
    )
    assert events == 0
