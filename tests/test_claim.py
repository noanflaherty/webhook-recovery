"""``SKIP LOCKED``: the entire coordination mechanism between workers.

There is no partitioning scheme, no work assignment, and no coordinator. Three
workers stay off each other's rows because Postgres skips a row another
transaction has locked -- so if that one predicate is wrong, two workers deliver
the same webhook twice and the failure is invisible in every chart the UI draws.

**This file cannot use the rolled-back session fixture.** Two transactions have
to see the same committed rows, which is exactly what a fixture that never
commits cannot provide. So it takes the engine directly and cleans up after
itself: the ``simulation`` row cascades, so deleting it deletes everything.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest_asyncio
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.ingest import EventSpec, ingest_events
from app.conductor.admission import Budget, mark_ready, select_candidates
from app.conductor.metrics import read_gauges
from app.conductor.policy import load_policies
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import AttemptOutcome, DeliveryState, SimStatus
from app.core.models import Attempt, Delivery, Simulation
from app.core.scenario import seed_simulation
from app.worker.claim import Completion, backoff_s, claim_batch, complete_batch
from app.worker.transport import AttemptResult
from tests.conftest import FIXTURE_SCENARIO, a_simulation, requires_db

pytestmark = requires_db

NOW = VIRTUAL_EPOCH_ZERO + timedelta(seconds=30)

#: `invoice.paid` is the one type all three consumers subscribe to, so ten
#: events is thirty deliveries. Stated once here because every count below is
#: derived from it.
EVENTS = 10
READY = EVENTS * 3


@pytest_asyncio.fixture
async def committed(engine: AsyncEngine) -> AsyncIterator[tuple[uuid.UUID, async_sessionmaker[AsyncSession]]]:
    """A simulation with `READY` ready deliveries, genuinely committed.

    Cleanup is a single delete: every table carries ``simulation_id`` with
    ``ON DELETE CASCADE``, which is the same property that lets concurrent
    simulations run without interfering.

    It sweeps before it builds as well as after. This is the only file in the
    suite that commits, so an interrupted run leaves rows behind -- and the next
    run would fail in ``test_db_fixture``, blaming the rollback fixture for a
    mess this file made.

    The simulation is created **paused**, which is what keeps a live
    ``docker compose up`` stack out of it. Committed ``ready`` rows in a
    *running* simulation are exactly what a real worker exists to claim, and the
    README points the suite at the compose database -- so without this the tests
    lose races to the product and fail with an empty batch. Pausing costs the
    test nothing, because every call here passes ``now`` explicitly.
    """
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async with maker() as session:
        await session.execute(delete(Simulation).where(Simulation.scenario_name == FIXTURE_SCENARIO))
        await session.commit()

    async with maker() as session:
        sim = a_simulation(status=SimStatus.PAUSED.value, paused_at_virtual=VIRTUAL_EPOCH_ZERO)
        session.add(sim)
        await session.flush()
        await seed_simulation(session, sim.id)
        await ingest_events(
            session,
            sim,
            [EventSpec("invoice.paid", f"in_{n}", occurred_at=VIRTUAL_EPOCH_ZERO) for n in range(EVENTS)],
        )
        await session.commit()
        simulation_id = sim.id

    async with engine.connect() as conn:
        selection = await select_candidates(
            conn,
            simulation_id,
            NOW,
            budget=Budget(buffer_slots=100, rate_slots=100),
            gauges=await read_gauges(conn, simulation_id),
            policies=await load_policies(conn, simulation_id),
            fair_drain=False,
        )
        await mark_ready(conn, selection.admit, NOW)
        await conn.commit()

    yield simulation_id, maker

    async with maker() as session:
        await session.execute(delete(Simulation).where(Simulation.id == simulation_id))
        await session.commit()


async def test_two_concurrent_claims_return_disjoint_rows(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """The property. Overlap here means the same webhook delivered twice."""
    simulation_id, maker = committed
    worker_a, worker_b = uuid.uuid4(), uuid.uuid4()

    async def claim(worker_id: uuid.UUID, limit: int) -> set[int]:
        async with maker() as session:
            claimed = await claim_batch(session, simulation_id, worker_id, NOW, limit)
            await session.commit()
            return {c.request.delivery_id for c in claimed}

    a, b = await asyncio.gather(claim(worker_a, 6), claim(worker_b, 6))

    assert a and b, "one worker got nothing at all -- SKIP LOCKED should not starve"
    assert not (a & b), f"both workers claimed {a & b}"
    assert len(a | b) == 12


async def test_a_claim_leases_the_row_and_records_the_attempt(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """Attempts are written at claim time, because fairness counts attempts *started*."""
    simulation_id, maker = committed
    worker_id = uuid.uuid4()

    async with maker() as session:
        claimed = await claim_batch(session, simulation_id, worker_id, NOW, limit=4)
        await session.commit()

    assert len(claimed) == 4
    async with maker() as session:
        rows = (
            (
                await session.execute(
                    select(Delivery).where(
                        Delivery.id.in_([c.request.delivery_id for c in claimed]),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert all(row.state == DeliveryState.IN_FLIGHT.value for row in rows)
        assert all(row.leased_by == worker_id for row in rows)
        assert all(row.lease_expires_at is not None and row.lease_expires_at > NOW for row in rows)
        assert all(row.attempt_count == 1 for row in rows)

        attempts = (
            (await session.execute(select(Attempt).where(Attempt.simulation_id == simulation_id)))
            .scalars()
            .all()
        )
        assert len(attempts) == 4
        # Started, not finished: the outcome does not exist yet.
        assert all(a.started_at == NOW and a.outcome is None for a in attempts)


async def test_a_successful_attempt_stamps_completed_at(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """The decision feed filters on ``completed_at IS NOT NULL``.

    A worker that sets ``state='delivered'`` and forgets the timestamp produces
    deliveries that are delivered and invisible -- an empty decision feed with
    no error anywhere to explain it.
    """
    simulation_id, maker = committed
    finished_at = NOW + timedelta(seconds=1)

    async with maker() as session:
        claimed = await claim_batch(session, simulation_id, uuid.uuid4(), NOW, limit=2)
        await session.commit()
    async with maker() as session:
        await complete_batch(
            session,
            [Completion(claimed=c, result=AttemptResult(AttemptOutcome.OK, 200)) for c in claimed],
            finished_at,
        )
        await session.commit()

    async with maker() as session:
        rows = (
            (
                await session.execute(
                    select(Delivery).where(Delivery.id.in_([c.request.delivery_id for c in claimed]))
                )
            )
            .scalars()
            .all()
        )
        assert all(row.state == DeliveryState.DELIVERED.value for row in rows)
        assert all(row.completed_at == finished_at for row in rows)


async def test_a_failed_attempt_goes_back_to_pending_with_backoff(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """Retry is a state transition plus a due time, and nothing else."""
    simulation_id, maker = committed
    finished_at = NOW + timedelta(seconds=1)

    async with maker() as session:
        claimed = await claim_batch(session, simulation_id, uuid.uuid4(), NOW, limit=1)
        await session.commit()
    async with maker() as session:
        await complete_batch(
            session,
            [
                Completion(
                    claimed=claimed[0],
                    result=AttemptResult(AttemptOutcome.SERVER_ERROR, 503),
                )
            ],
            finished_at,
        )
        await session.commit()

    async with maker() as session:
        row = await session.get(Delivery, claimed[0].request.delivery_id)
        assert row is not None
        assert row.state == DeliveryState.PENDING.value
        assert row.completed_at is None
        assert row.next_attempt_at == finished_at + timedelta(seconds=backoff_s(1))


async def test_the_retry_cap_is_terminal(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """A delivery that has exhausted its attempts stops consuming capacity."""
    simulation_id, maker = committed

    async with maker() as session:
        claimed = await claim_batch(session, simulation_id, uuid.uuid4(), NOW, limit=1)
        await session.commit()

    # Stand the delivery up as if it were on its final attempt.
    from dataclasses import replace

    exhausted = replace(claimed[0], request=replace(claimed[0].request, attempt_no=5))

    async with maker() as session:
        await complete_batch(
            session,
            [Completion(claimed=exhausted, result=AttemptResult(AttemptOutcome.TIMEOUT))],
            NOW,
        )
        await session.commit()

    async with maker() as session:
        row = await session.get(Delivery, claimed[0].request.delivery_id)
        assert row is not None
        assert row.state == DeliveryState.FAILED.value
        assert row.completed_at == NOW
        assert row.terminal_reason is not None and "retry cap" in row.terminal_reason


async def test_claiming_an_empty_buffer_is_free(
    committed: tuple[uuid.UUID, async_sessionmaker[AsyncSession]],
) -> None:
    """Workers poll far faster than the conductor admits, so this is the common path."""
    simulation_id, maker = committed

    async with maker() as session:
        everything = await claim_batch(session, simulation_id, uuid.uuid4(), NOW, limit=100)
        await session.commit()
    assert len(everything) == READY

    async with maker() as session:
        assert await claim_batch(session, simulation_id, uuid.uuid4(), NOW, limit=100) == []
        assert (
            await session.scalar(
                select(func.count()).select_from(Attempt).where(Attempt.simulation_id == simulation_id)
            )
            == READY
        )
