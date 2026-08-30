"""Lease reclamation: what happens to the work a dead worker was holding.

Every failure here is silent. A stranded ``in_flight`` row draws no chart of its
own -- it shows up as a consumer that mysteriously stops using its share, and as
a run whose backlog floors above zero and never retires. So the assertions are
about the *invariants* rather than about the outcome being plausible:

* exactly one ``attempt`` row per attempt, whoever closed it, because the
  fairness window is a count of those rows and a duplicate silently charges a
  consumer for capacity it never used;
* the concurrency the dead worker held comes back, which is the actual cost of
  not reclaiming;
* a worker that was slow rather than dead cannot undo the reclamation when it
  finally arrives.

The cast is the shipped one (``app.core.scenario.CONSUMERS``), so the
concurrency numbers are the real ones.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.conductor.admission import mark_ready, read_consumer_states
from app.conductor.metrics import read_gauges
from app.conductor.reaper import reclaim_expired_leases
from app.conductor.service import _SIM_COLUMNS, Conductor
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import AttemptOutcome, DeliveryState
from app.core.models import Attempt, Consumer, Delivery, Simulation
from app.core.scenario import CONSUMERS, seed_simulation
from app.core.settings import get_settings
from app.worker.claim import Claimed, Completion, backoff_s, claim_batch, complete_batch
from app.worker.transport import AttemptResult
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db

#: All three consumers subscribe to this one, so one event is three deliveries.
EVENT_TYPE = "invoice.paid"

#: Dated behind the epoch so ingest stamps a `next_attempt_at` that is already
#: due. An event dated forward is not a candidate at all, and a test that got
#: that wrong would be exercising retry backoff while believing it was
#: exercising the reaper.
INGEST_AT = VIRTUAL_EPOCH_ZERO - timedelta(seconds=60)

#: When the claim happens.
CLAIMED_AT = VIRTUAL_EPOCH_ZERO

#: Comfortably past the lease stamped at `CLAIMED_AT`.
EXPIRED_AT = CLAIMED_AT + timedelta(seconds=get_settings().lease_duration_virtual_s + 1)

CLOVER = "Clover CRM"


async def _stranded(
    session: AsyncSession,
    connection: AsyncConnection,
    *,
    count: int,
) -> tuple[Simulation, uuid.UUID, list[Claimed]]:
    """A simulation with ``count`` deliveries claimed and never completed.

    This is exactly the state an ungracefully killed worker leaves behind: the
    claim transaction committed, the completion transaction never ran.
    """
    sim = a_simulation()
    sim.outage_override = False
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(
        session,
        sim,
        [
            EventSpec(event_type=EVENT_TYPE, entity_key=f"ent_{i}", occurred_at=INGEST_AT)
            for i in range(count)
        ],
    )
    await session.flush()

    ids = [
        row[0]
        for row in await connection.execute(
            select(Delivery.id).where(Delivery.simulation_id == sim.id).order_by(Delivery.id).limit(count)
        )
    ]
    await mark_ready(connection, ids, CLAIMED_AT)

    worker_id = uuid.uuid4()
    claimed = await claim_batch(session, sim.id, worker_id, CLAIMED_AT, limit=count)
    await session.flush()
    assert len(claimed) == count
    return sim, worker_id, claimed


async def _delivery(conn: AsyncConnection, delivery_id: int) -> Delivery:
    row = (await conn.execute(select(Delivery).where(Delivery.id == delivery_id))).one()
    return row  # type: ignore[return-value]


async def _attempts(conn: AsyncConnection, delivery_id: int) -> list[tuple[str | None, datetime | None]]:
    return [
        (row[0], row[1])
        for row in await conn.execute(
            select(Attempt.outcome, Attempt.finished_at)
            .where(Attempt.delivery_id == delivery_id)
            .order_by(Attempt.id)
        )
    ]


# ---------------------------------------------------------------------------
# The sweep itself
# ---------------------------------------------------------------------------


async def test_an_expired_lease_goes_back_to_pending_with_backoff(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The delivery is requeued, not lost and not left where it was."""
    _, _, claimed = await _stranded(session, connection, count=1)
    delivery_id = claimed[0].request.delivery_id

    reclaimed = await reclaim_expired_leases(connection, claimed[0].request.simulation_id, EXPIRED_AT)

    assert (reclaimed.requeued, reclaimed.exhausted) == (1, 0)
    row = await _delivery(connection, delivery_id)
    assert row.state == DeliveryState.PENDING.value
    assert row.completed_at is None
    # Charged the same backoff a 5xx would earn: an expired lease is a failed
    # attempt, not a free one.
    assert row.next_attempt_at == EXPIRED_AT + timedelta(seconds=backoff_s(1))


async def test_reclamation_clears_the_lease(session: AsyncSession, connection: AsyncConnection) -> None:
    """Without this the fence in ``complete_batch`` has nothing to test against."""
    _, _, claimed = await _stranded(session, connection, count=1)

    await reclaim_expired_leases(connection, claimed[0].request.simulation_id, EXPIRED_AT)

    row = await _delivery(connection, claimed[0].request.delivery_id)
    assert row.leased_by is None
    assert row.lease_expires_at is None


async def test_a_live_lease_is_left_alone(session: AsyncSession, connection: AsyncConnection) -> None:
    """The predicate is the whole safety story: reap early and work is sent twice."""
    _, _, claimed = await _stranded(session, connection, count=1)

    still_leased = CLAIMED_AT + timedelta(seconds=get_settings().lease_duration_virtual_s - 1)
    reclaimed = await reclaim_expired_leases(connection, claimed[0].request.simulation_id, still_leased)

    assert reclaimed.total == 0
    row = await _delivery(connection, claimed[0].request.delivery_id)
    assert row.state == DeliveryState.IN_FLIGHT.value


async def test_the_open_attempt_is_closed_rather_than_duplicated(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """One attempt, one row -- whichever process ends up closing it.

    ``claim_batch`` writes the ``attempt`` row when the attempt *starts*,
    because that is what the fairness window counts. A reaper that inserted its
    own row would charge the consumer twice inside the window its share is
    computed from, and the consumer would then be throttled for capacity a dead
    worker never spent on its behalf.
    """
    _, _, claimed = await _stranded(session, connection, count=1)
    delivery_id = claimed[0].request.delivery_id

    await reclaim_expired_leases(connection, claimed[0].request.simulation_id, EXPIRED_AT)

    assert await _attempts(connection, delivery_id) == [(AttemptOutcome.LEASE_EXPIRED.value, EXPIRED_AT)]


async def test_reclamation_does_not_inflate_the_fairness_window(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The same invariant, read the way the conductor reads it -- and it bites.

    A lease outlives the fairness window several times over: 30 virtual seconds
    against 5. So the attempt a dead worker started has already aged out by the
    time its lease expires, and a reaper that inserted a *fresh* ``attempt`` row
    would land it squarely inside the current window -- charging the consumer,
    right now, for an attempt that happened half a minute ago and was already
    paid for. It would then be throttled for capacity it never used, at the
    exact moment it is being handed its work back.
    """
    sim, _, _ = await _stranded(session, connection, count=3)

    await reclaim_expired_leases(connection, sim.id, EXPIRED_AT)

    gauges = await read_gauges(connection, sim.id)
    states = await read_consumer_states(connection, sim.id, EXPIRED_AT, gauges)
    assert sum(state.attempts_in_window for state in states) == 0

    # Not because the rows vanished: three attempts happened, three are recorded.
    total = await connection.scalar(
        select(func.count()).select_from(Attempt).where(Attempt.simulation_id == sim.id)
    )
    assert total == 3


async def test_a_lease_that_expires_out_of_retries_is_terminal(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """A delivery whose worker keeps dying must stop coming back.

    Without the retry cap it is the same delivery at the head of the queue
    forever, claimed by whichever worker is next to fall over.
    """
    sim, _, claimed = await _stranded(session, connection, count=1)
    delivery_id = claimed[0].request.delivery_id

    # Stand it up as if this were its final attempt.
    await connection.execute(
        update(Delivery).where(Delivery.id == delivery_id).values(attempt_count=get_settings().max_attempts)
    )

    reclaimed = await reclaim_expired_leases(connection, sim.id, EXPIRED_AT)

    assert (reclaimed.requeued, reclaimed.exhausted) == (0, 1)
    row = await _delivery(connection, delivery_id)
    assert row.state == DeliveryState.FAILED.value
    assert row.completed_at == EXPIRED_AT
    assert row.terminal_reason is not None and "lease expired" in row.terminal_reason


# ---------------------------------------------------------------------------
# The cost of *not* reclaiming
# ---------------------------------------------------------------------------


async def test_reclaiming_returns_the_concurrency_the_dead_worker_held(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The actual bug, stated as an assertion.

    ``ConsumerState.headroom`` gates on ``in_flight < concurrency_cap``, so a
    consumer holding a full cap of stranded rows is not dispatchable -- and,
    with nothing to reclaim them, never becomes dispatchable again. Its share of
    the provider is not idle, it is gone.
    """
    cap = next(spec.concurrency_cap for spec in CONSUMERS if spec.name == CLOVER)
    sim, _, _ = await _stranded(session, connection, count=cap * len(CONSUMERS))
    window_s = get_settings().fairness_window_virtual_s

    async def clover_headroom(at: datetime) -> int:
        gauges = await read_gauges(connection, sim.id)
        states = await read_consumer_states(connection, sim.id, at, gauges)
        clover_id = (
            await connection.execute(
                select(Consumer.id).where(Consumer.simulation_id == sim.id, Consumer.name == CLOVER)
            )
        ).scalar_one()
        state = next(s for s in states if s.id == clover_id)
        return state.headroom(window_s, pooled_concurrency=False)

    assert await clover_headroom(EXPIRED_AT) == 0

    await reclaim_expired_leases(connection, sim.id, EXPIRED_AT)

    # Still zero at the instant of reclamation -- the rows come back with a
    # backoff, so they are requeued but not yet due. Past the backoff they are.
    due = EXPIRED_AT + timedelta(seconds=backoff_s(1))
    assert await clover_headroom(due) > 0


# ---------------------------------------------------------------------------
# The race: a worker that was slow rather than dead
# ---------------------------------------------------------------------------


async def test_a_late_completion_cannot_undo_a_reclamation(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The lease fence, which is the difference between this and a double send.

    A worker sleeping in the transport against a consumer that is down can have
    its lease expire while it is still perfectly alive. It then arrives here
    holding a ``Claimed`` for a delivery that has already been requeued -- and
    without the fence it would mark it ``delivered``, retiring a webhook that
    was never accepted, or resurrect a row another worker now legitimately owns.
    """
    sim, worker_id, claimed = await _stranded(session, connection, count=1)
    delivery_id = claimed[0].request.delivery_id
    await reclaim_expired_leases(connection, sim.id, EXPIRED_AT)

    late = EXPIRED_AT + timedelta(seconds=1)
    await complete_batch(
        session,
        worker_id,
        [Completion(claimed=claimed[0], result=AttemptResult(AttemptOutcome.OK, 200))],
        late,
    )
    await session.flush()

    row = await _delivery(connection, delivery_id)
    assert row.state == DeliveryState.PENDING.value
    assert row.completed_at is None
    # And the attempt keeps the true story of how it ended.
    assert await _attempts(connection, delivery_id) == [(AttemptOutcome.LEASE_EXPIRED.value, EXPIRED_AT)]


# ---------------------------------------------------------------------------
# Where the sweep sits in the pass
# ---------------------------------------------------------------------------


async def test_the_conductor_reclaims_even_during_an_outage(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Ordering, asserted rather than commented.

    A pass returns early while the pipeline is down, and leases go on expiring
    through an outage. If the sweep sat behind that early return, a worker that
    died just before an outage would hold its rows for the whole of it -- which
    is precisely the window in which its consumer's capacity matters most.
    """
    sim, _, claimed = await _stranded(session, connection, count=1)
    await connection.execute(update(Simulation).where(Simulation.id == sim.id).values(outage_override=True))

    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == sim.id))).one()
    # A pass reads its own clock, so move virtual time rather than passing an
    # instant: `_pass` is the thing under test, not `reclaim_expired_leases`.
    await connection.execute(
        update(Simulation)
        .where(Simulation.id == sim.id)
        .values(virtual_epoch=EXPIRED_AT, resumed_at_wall=sim_row.resumed_at_wall)
    )
    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == sim.id))).one()
    await Conductor()._pass(connection, sim_row)

    row = await _delivery(connection, claimed[0].request.delivery_id)
    assert row.state == DeliveryState.PENDING.value


async def test_a_pass_over_a_healthy_run_reclaims_nothing(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The sweep is on the hot path, so it has to be inert when there is no work."""
    sim, _, _ = await _stranded(session, connection, count=2)

    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == sim.id))).one()
    await Conductor()._pass(connection, sim_row)

    still_leased = await connection.scalar(
        select(func.count())
        .select_from(Delivery)
        .where(Delivery.simulation_id == sim.id, Delivery.state == DeliveryState.IN_FLIGHT.value)
    )
    assert still_leased == 2
