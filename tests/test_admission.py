"""Admission control: the buffer ceiling, the rate ceiling, and the outage gate.

The outage gate is the one that shows up on camera. If admission does not stop
during the scripted outage there is no backlog to burn down, and the entire demo
is a flat line -- so it is asserted here rather than discovered at recording
time.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.conductor.admission import Budget, compute_budget, mark_ready, select_candidates
from app.conductor.metrics import read_gauges
from app.conductor.policy import load_policies
from app.conductor.service import _SIM_COLUMNS, Conductor
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import DeliveryState, SimStatus
from app.core.models import Attempt, Delivery, Simulation
from app.core.scenario import (
    OUTAGE_STARTS_AT_S,
    SCENARIO_ENDS_AT_S,
    SCENARIO_MAX_VIRTUAL_S,
    seed_simulation,
)
from app.core.settings import get_settings
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db

#: Three consumers at concurrency_cap 8.
TOTAL_CONCURRENCY_CAP = 24


async def _admit(
    connection: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    slots: int,
    *,
    fair_drain: bool = False,
) -> list[int]:
    """What a pass would admit, with the surrounding reads the conductor does.

    Defaults to the fair-drain-off arm, because everything in this file is about
    the ceilings and the outage gate -- the ceilings bind identically on both
    arms, and the fair arm gets its own file.
    """
    selection = await select_candidates(
        connection,
        simulation_id,
        now,
        budget=Budget(buffer_slots=slots, rate_slots=slots),
        gauges=await read_gauges(connection, simulation_id),
        policies=await load_policies(connection, simulation_id),
        fair_drain=fair_drain,
    )
    return selection.admit


async def _backlog(session: AsyncSession, count: int) -> Simulation:
    """A seeded simulation carrying `count` payment events of pending backlog.

    ``payment_intent.succeeded`` fans out to Acme and Bolt, so the backlog is
    ``2 x count`` deliveries. Every assertion below is about a ceiling rather
    than an exact backlog, so the multiplier only has to be large enough that
    the ceiling is what binds.
    """
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(
        session,
        sim,
        [
            EventSpec("payment_intent.succeeded", f"pi_{n}", occurred_at=VIRTUAL_EPOCH_ZERO)
            for n in range(count)
        ],
    )
    return sim


async def _sim_row(conn: AsyncConnection, simulation_id: uuid.UUID) -> object:
    """The row the conductor actually reads, not a stand-in for it."""
    return (await conn.execute(select(*_SIM_COLUMNS).where(Simulation.id == simulation_id))).one()


async def _ready_count(conn: AsyncConnection, simulation_id: uuid.UUID) -> int:
    count = await conn.scalar(
        select(func.count())
        .select_from(Delivery)
        .where(
            Delivery.simulation_id == simulation_id,
            Delivery.state == DeliveryState.READY.value,
        )
    )
    return int(count or 0)


async def test_nothing_is_admitted_during_the_outage(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The single branch the whole demo curve rests on.

    Without it, events still land in the ledger during the outage but are also
    delivered during it -- backlogs stay flat, there is nothing to burn down,
    and the fairness claim has no stage to be proved on.
    """
    sim = await _backlog(session, count=50)
    sim.outage_override = True
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    assert await _ready_count(connection, sim.id) == 0


async def test_work_is_admitted_outside_the_outage(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The same call, with the gate open -- so the test above proves the gate."""
    sim = await _backlog(session, count=50)
    sim.outage_override = False
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    assert await _ready_count(connection, sim.id) > 0


async def test_the_scripted_outage_needs_no_override(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """``outage_override`` is the reviewer's switch; the script is the default."""
    sim = await _backlog(session, count=50)
    # Park the clock inside the scripted outage by rebasing its epoch there.
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=OUTAGE_STARTS_AT_S + 60)
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    assert await _ready_count(connection, sim.id) == 0


async def test_the_ready_buffer_stays_shallow(session: AsyncSession, connection: AsyncConnection) -> None:
    """Buffer depth *is* the granularity of fairness, so it is a hard ceiling.

    A conductor that marked the whole backlog ready in one pass would still be
    arithmetically correct at that instant, and would draw an attempts-share
    chart of ~100% one consumer then ~100% the next -- the exact opposite of
    what the chart is there to show.
    """
    settings = get_settings()
    sim = await _backlog(session, count=400)
    sim.outage_override = False
    # Take the rate ceiling out of the picture so the buffer is what binds.
    sim.global_attempts_per_s = 10_000.0
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    expected = math.ceil(settings.ready_buffer_depth_multiplier * TOTAL_CONCURRENCY_CAP)
    assert await _ready_count(connection, sim.id) == expected


async def test_the_rate_cap_counts_admitted_but_unattempted_work(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The subtlety the singleton exists for.

    Work that has been admitted but not yet claimed has no ``attempt`` row, so a
    pure sliding-window count cannot see it and would admit against the same
    budget twice. Subtracting the outstanding ready buffer is what closes that
    -- and it is a read-modify-write, which is why two conductors running this
    concurrently is not merely wasteful but wrong.
    """
    settings = get_settings()
    sim = await _backlog(session, count=400)
    sim.outage_override = False
    sim.global_attempts_per_s = 4.0
    await session.flush()
    now = VIRTUAL_EPOCH_ZERO

    budget = await compute_budget(connection, sim.id, sim.global_attempts_per_s, now)
    allowance = int(4.0 * settings.fairness_window_virtual_s)
    assert budget.rate_slots == allowance

    admitted = await mark_ready(connection, await _admit(connection, sim.id, now, budget.slots), now)
    assert admitted == allowance

    # Nothing has been attempted, so the window is still empty -- but the budget
    # is spent, and a second pass must see that.
    assert (
        await connection.scalar(
            select(func.count()).select_from(Attempt).where(Attempt.simulation_id == sim.id)
        )
        == 0
    )
    again = await compute_budget(connection, sim.id, sim.global_attempts_per_s, now)
    assert again.rate_slots == 0


async def test_candidates_come_back_oldest_first(session: AsyncSession, connection: AsyncConnection) -> None:
    """FIFO by event time -- the fair-drain-off arm, and what fairness is measured against."""
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(
        session,
        sim,
        [
            EventSpec(
                "invoice.paid",
                f"in_{n}",
                occurred_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=n),
            )
            for n in range(10)
        ],
    )

    now = VIRTUAL_EPOCH_ZERO + timedelta(seconds=100)
    chosen = await _admit(connection, sim.id, now, 6)

    due = (
        await connection.execute(
            select(Delivery.id)
            .where(Delivery.simulation_id == sim.id)
            .order_by(Delivery.next_attempt_at, Delivery.id)
            .limit(6)
        )
    ).scalars()
    assert sorted(chosen) == sorted(due.all())


async def test_work_that_is_not_due_yet_is_left_alone(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """A delivery in retry backoff is pending, and must not be re-admitted early."""
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(
        session,
        sim,
        [EventSpec("invoice.paid", "in_1", occurred_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=60))],
    )

    now = VIRTUAL_EPOCH_ZERO + timedelta(seconds=10)
    assert await _admit(connection, sim.id, now, 10) == []


async def test_a_drained_run_is_retired(session: AsyncSession, connection: AsyncConnection) -> None:
    """A finished simulation stops costing the conductor anything.

    A pass covers *every* running simulation, so one that nobody retires goes on
    consuming throughput forever -- and the cost lands on whichever run a
    reviewer is currently watching. Invisible locally, where there is one
    simulation; on a shared deployment it compounds with every visit and
    presents as a backlog that tracks its arrival rate and never drains.
    """
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    # Past the end of the script, with nothing left to deliver.
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=SCENARIO_ENDS_AT_S + 30)
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    status = await connection.scalar(select(Simulation.status).where(Simulation.id == sim.id))
    assert status == SimStatus.DONE.value


async def test_a_run_with_work_left_is_not_retired(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Backlog outranks the clock -- a run still draining is not finished."""
    sim = await _backlog(session, count=50)
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=SCENARIO_ENDS_AT_S + 30)
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    status = await connection.scalar(select(Simulation.status).where(Simulation.id == sim.id))
    assert status == SimStatus.RUNNING.value


async def test_an_empty_backlog_mid_run_is_not_finished(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """A fresh simulation has nothing queued, and must not be retired on the spot.

    This is the case that makes `is_finished` more than "backlog == 0": while the
    producer is still emitting, an empty backlog means the pipeline is keeping
    up, which is the system working rather than the run being over. Retiring on
    an empty backlog alone also *races* the producer, which at 20x commits
    another ~20 deliveries in the time one pass takes -- so the run would freeze
    with a residue that looks like a failure to drain.
    """
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    status = await connection.scalar(select(Simulation.status).where(Simulation.id == sim.id))
    assert status == SimStatus.RUNNING.value


async def test_a_stuck_run_is_retired_on_the_virtual_time_backstop(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """A run that will never drain still has to stop consuming the conductor."""
    sim = await _backlog(session, count=50)
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=SCENARIO_MAX_VIRTUAL_S + 1)
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    status = await connection.scalar(select(Simulation.status).where(Simulation.id == sim.id))
    assert status == SimStatus.DONE.value


async def test_a_retired_run_freezes_its_clock(session: AsyncSession, connection: AsyncConnection) -> None:
    """Mirrors what PATCH does for a manual finish, so final numbers stop moving."""
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    sim.virtual_epoch = VIRTUAL_EPOCH_ZERO + timedelta(seconds=SCENARIO_ENDS_AT_S + 30)
    await session.flush()

    conductor = Conductor()
    await conductor._pass(connection, await _sim_row(connection, sim.id))

    frozen = await connection.scalar(select(Simulation.paused_at_virtual).where(Simulation.id == sim.id))
    assert frozen is not None
