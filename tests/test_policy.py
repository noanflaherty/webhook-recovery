"""The second claim: the consumer decides what is still worth replaying.

Both mechanisms are cheap to test and expensive to get wrong, because both fail
*quietly*. An over-eager coalesce drops a webhook nobody asked it to drop and
the backlog chart just looks like it drained faster; a staleness bound that
never fires shows up as nothing at all. Neither has a symptom on any chart the
UI draws, which is exactly the profile of a bug that ships.

The cast is the shipped one (``app.core.scenario.CONSUMERS``) rather than a
fixture built to pass: Acme carries no policies, Bolt coalesces
``customer.subscription.updated`` and bounds ``balance.available`` at 120s, and
the pair of them under the same event stream is the comparison the demo makes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.conductor.admission import mark_ready
from app.conductor.policy import stale_by
from app.conductor.service import _SIM_COLUMNS, Conductor
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import DeliveryState
from app.core.models import Attempt, Consumer, Delivery, Simulation
from app.core.scenario import CONSUMERS, seed_simulation
from app.worker.claim import claim_batch
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db

#: Bolt bounds this; Acme has no policy for it. Both subscribe, so one ingested
#: event produces one of each.
STALENESS_TYPE = "balance.available"

#: Read from the shipped cast rather than written down here. The bound is a
#: scenario-tuning knob -- it has already moved once -- and a test that hardcodes
#: it fails on a deliberate tuning change while proving nothing about policy.
BOLT_STALENESS_S = next(
    policy.max_staleness_s
    for spec in CONSUMERS
    if spec.name == "Bolt Billing"
    for policy in spec.policies
    if policy.event_type == STALENESS_TYPE
)

#: Bolt coalesces this by entity key; Acme does not.
COALESCE_TYPE = "customer.subscription.updated"

#: Far enough past Bolt's 120s bound that no test is measuring a boundary it
#: did not mean to.
LONG_AGO = VIRTUAL_EPOCH_ZERO - timedelta(seconds=1000)

#: Where a run's virtual clock sits a few milliseconds after it starts.
#: Ingest stamps ``next_attempt_at`` from the event's own timestamp, so an event
#: dated in the *future* is not due and is not a candidate at all -- a test that
#: dates its events forward from the epoch is exercising the retry-backoff gate
#: and quietly not exercising policy.
_DUE_BASE = VIRTUAL_EPOCH_ZERO - timedelta(seconds=60)


def due_at(offset_s: float) -> datetime:
    """A timestamp that is already due, ordered by `offset_s`."""
    return _DUE_BASE + timedelta(seconds=offset_s)


async def _seeded(session: AsyncSession, events: list[EventSpec]) -> Simulation:
    sim = a_simulation()
    sim.outage_override = False
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(session, sim, events)
    await session.flush()
    return sim


async def _pass(connection: AsyncConnection, simulation_id: uuid.UUID) -> None:
    """One real conductor pass, exactly as the deployed process runs it."""
    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == simulation_id))).one()
    await Conductor()._pass(connection, sim_row)


async def _deliveries(
    conn: AsyncConnection, simulation_id: uuid.UUID, consumer: str
) -> list[tuple[int, str, str | None]]:
    """One consumer's deliveries as ``(id, state, terminal_reason)``, oldest first."""
    rows = (
        await conn.execute(
            select(Delivery.id, Delivery.state, Delivery.terminal_reason)
            .join(Consumer, Consumer.id == Delivery.consumer_id)
            .where(Delivery.simulation_id == simulation_id, Consumer.name == consumer)
            .order_by(Delivery.id)
        )
    ).all()
    return [(row[0], row[1], row[2]) for row in rows]


def _states(rows: list[tuple[int, str, str | None]]) -> list[str]:
    return [state for _, state, _ in rows]


# ---------------------------------------------------------------------------
# stale_by, the predicate both planes share
# ---------------------------------------------------------------------------


def test_no_bound_is_never_stale() -> None:
    """An absent policy row means deliver everything, not deliver nothing."""
    assert stale_by(VIRTUAL_EPOCH_ZERO, LONG_AGO, None) is None


def test_inside_the_bound_is_not_stale() -> None:
    at = VIRTUAL_EPOCH_ZERO
    assert stale_by(at, at - timedelta(seconds=119), 120.0) is None


def test_the_overage_is_reported_not_just_the_fact() -> None:
    """The decision feed says "stale by 43s", which means this has to be a number."""
    at = VIRTUAL_EPOCH_ZERO
    overage = stale_by(at, at - timedelta(seconds=163), 120.0)
    assert overage is not None
    assert round(overage) == 43


# ---------------------------------------------------------------------------
# Staleness, at dispatch time
# ---------------------------------------------------------------------------


async def test_a_stale_delivery_expires_and_its_neighbour_does_not(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The same event, two consumers, two different answers.

    This is the whole feature in one assertion: policy is the *consumer's*
    preference, not a property of the event. Acme wants every balance update
    however old; Bolt has said a two-minute-old balance is worthless. One
    ingested event, and the system honours both.
    """
    sim = await _seeded(session, [EventSpec(STALENESS_TYPE, "acct_1", occurred_at=LONG_AGO)])
    await _pass(connection, sim.id)

    (bolt,) = await _deliveries(connection, sim.id, "Bolt Billing")
    (acme,) = await _deliveries(connection, sim.id, "Acme Analytics")

    assert bolt[1] == DeliveryState.EXPIRED
    assert bolt[2] is not None and bolt[2].startswith("stale by ")
    assert f"max {BOLT_STALENESS_S:.0f}s" in bolt[2]
    assert acme[1] == DeliveryState.READY


async def test_an_expired_delivery_is_completed_and_never_attempted(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Two invariants that fail silently in different places.

    Without ``completed_at`` the drop is invisible to both the decision feed and
    the metrics writer, so the backlog falls with nothing to explain it. With an
    ``attempt`` row it would be charged to the consumer's slice of the fairness
    window -- capacity it never used, and would then be starved for.
    """
    sim = await _seeded(session, [EventSpec(STALENESS_TYPE, "acct_1", occurred_at=LONG_AGO)])
    await _pass(connection, sim.id)

    completed = await connection.scalar(
        select(func.count())
        .select_from(Delivery)
        .where(
            Delivery.simulation_id == sim.id,
            Delivery.state == DeliveryState.EXPIRED.value,
            Delivery.completed_at.is_not(None),
        )
    )
    assert completed == 1

    attempts = await connection.scalar(
        select(func.count()).select_from(Attempt).where(Attempt.simulation_id == sim.id)
    )
    assert attempts == 0


# ---------------------------------------------------------------------------
# Coalescing
# ---------------------------------------------------------------------------


async def test_only_the_newest_of_a_key_survives(session: AsyncSession, connection: AsyncConnection) -> None:
    """Forty churning subscriptions, one state each worth delivering.

    Acme is the control: identical events, no policy, everything kept. The two
    consumers diverging under the same stream is what makes this a consumer
    choice rather than the system deciding on their behalf.
    """
    sim = await _seeded(
        session,
        [EventSpec(COALESCE_TYPE, "sub_1", occurred_at=due_at(n)) for n in range(5)],
    )
    await _pass(connection, sim.id)

    bolt = await _deliveries(connection, sim.id, "Bolt Billing")
    acme = await _deliveries(connection, sim.id, "Acme Analytics")

    assert _states(bolt) == [DeliveryState.SUPERSEDED] * 4 + [DeliveryState.READY]
    assert _states(acme) == [DeliveryState.READY] * 5

    # The reason names the winner, so the feed reads as a sentence a reviewer
    # can check against the row it points at.
    winner_id = bolt[-1][0]
    assert bolt[0][2] == f"superseded by delivery {winner_id}"


async def test_different_keys_do_not_supersede_each_other(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Coalescing is per entity key. Nothing collapses across subscriptions."""
    sim = await _seeded(
        session,
        [EventSpec(COALESCE_TYPE, f"sub_{n}", occurred_at=due_at(n)) for n in range(5)],
    )
    await _pass(connection, sim.id)

    bolt = await _deliveries(connection, sim.id, "Bolt Billing")
    assert _states(bolt) == [DeliveryState.READY] * 5


async def test_identical_timestamps_leave_exactly_one_survivor(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The trap the ordering exists for, and the reason it is a *pair*.

    The producer spreads a tick's events across the virtual window it covers, so
    two events for one key landing on the same microsecond is a thing that
    happens rather than a thing to hand-wave. Ordered on ``created_at`` alone,
    each of these is "newer" than the other, both get superseded, and nothing is
    ever delivered for that subscription -- with no error, no gap in the chart,
    and a backlog that drained slightly faster than it should have.

    Pairing the timestamp with ``delivery.id`` makes the order total, so there
    is always exactly one winner.
    """
    sim = await _seeded(
        session,
        [EventSpec(COALESCE_TYPE, "sub_1", occurred_at=VIRTUAL_EPOCH_ZERO) for _ in range(3)],
    )
    await _pass(connection, sim.id)

    bolt = await _deliveries(connection, sim.id, "Bolt Billing")
    assert _states(bolt).count(DeliveryState.READY) == 1
    assert _states(bolt).count(DeliveryState.SUPERSEDED) == 2
    # Ties break toward the row the database assigned last, which is the one a
    # human would call newest.
    assert bolt[-1][1] == DeliveryState.READY


async def test_a_coalescing_consumer_still_fills_its_share(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The interlock between the two claims, pinned.

    Bolt's backlog here is 95% droppable. Fairness rations *attempts*, and a
    drop is not an attempt -- so a pass that rationed candidates instead would
    hand Bolt its share, watch policy eat almost all of it, and admit one or two
    deliveries while Acme took the rest. Bolt would then look starved by a
    scheduler that was working correctly, which is the worst kind of bug to
    debug on camera.

    So: every droppable row goes in the same pass that admits the survivors.
    """
    sim = await _seeded(
        session,
        [
            EventSpec(
                COALESCE_TYPE,
                f"sub_{n % 5}",
                occurred_at=due_at(n / 1000),
            )
            for n in range(100)
        ],
    )
    await _pass(connection, sim.id)

    bolt = _states(await _deliveries(connection, sim.id, "Bolt Billing"))
    assert bolt.count(DeliveryState.SUPERSEDED) == 95
    assert bolt.count(DeliveryState.READY) == 5, (
        "every surviving key should be admitted in the same pass that dropped the rest"
    )


async def test_policy_applies_with_fair_drain_off(session: AsyncSession, connection: AsyncConnection) -> None:
    """The toggle changes scheduling, not what a consumer asked for.

    If turning fairness off quietly turned policy off too, the before/after
    would be moving two variables at once and would prove neither claim.
    """
    sim = await _seeded(
        session,
        [EventSpec(COALESCE_TYPE, "sub_1", occurred_at=due_at(n)) for n in range(5)],
    )
    sim.fair_drain_enabled = False
    await session.flush()
    await _pass(connection, sim.id)

    bolt = _states(await _deliveries(connection, sim.id, "Bolt Billing"))
    assert bolt.count(DeliveryState.SUPERSEDED) == 4


# ---------------------------------------------------------------------------
# The worker's re-check
# ---------------------------------------------------------------------------


async def test_a_delivery_that_goes_stale_before_the_attempt_is_expired(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The one piece of policy logic the data plane contains.

    A delivery can go stale in the gap between the conductor admitting it and a
    worker reaching it. The conductor cannot close that gap from its side -- it
    is the worker's own queue latency -- so the bound is re-checked immediately
    before the attempt. Admitted while fresh, claimed long after: no attempt,
    and no delivery of a balance the consumer already said it did not want.
    """
    sim = await _seeded(session, [EventSpec(STALENESS_TYPE, "acct_1", occurred_at=VIRTUAL_EPOCH_ZERO)])
    ids = [
        row[0]
        for row in (
            await connection.execute(
                select(Delivery.id).where(Delivery.simulation_id == sim.id).order_by(Delivery.id)
            )
        ).all()
    ]
    await mark_ready(connection, ids, VIRTUAL_EPOCH_ZERO)

    late = VIRTUAL_EPOCH_ZERO + timedelta(seconds=600)
    claimed = await claim_batch(session, sim.id, uuid.uuid4(), late, limit=16)

    # Acme has no bound and is still claimed; Bolt's copy is expired instead.
    assert len(claimed) == 1
    (acme,) = await _deliveries(connection, sim.id, "Acme Analytics")
    (bolt,) = await _deliveries(connection, sim.id, "Bolt Billing")
    assert acme[1] == DeliveryState.IN_FLIGHT
    assert bolt[1] == DeliveryState.EXPIRED
    assert bolt[2] is not None and bolt[2].startswith("stale by ")

    # And the expiry was not charged to Bolt as an attempt.
    bolt_attempts = await connection.scalar(
        select(func.count())
        .select_from(Attempt)
        .join(Consumer, Consumer.id == Attempt.consumer_id)
        .where(Attempt.simulation_id == sim.id, Consumer.name == "Bolt Billing")
    )
    assert bolt_attempts == 0
