"""The metrics writer, which is the one component that can lie convincingly.

Every other bug in this system announces itself: nothing gets delivered, or the
backlog never drains, or a process crashes. A metrics bug produces a chart --
smooth, plausible, and wrong. And the fairness proof is a *100% stacked* chart,
so a bug that undercounts every consumer equally draws a picture that looks
exactly right.

Hence the two assertions that matter here: buckets are **contiguous** (a gap is
a hole in the chart that a client cannot distinguish from a zero), and per-bucket
attempts **sum to the total** (which is what a sampled counter would silently
fail).
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.conductor.metrics import MetricsWriter, bucket_index, read_gauges
from app.core.clock import VIRTUAL_EPOCH_ZERO, SimulationClockConfig, set_speed
from app.core.enums import AttemptOutcome, DeliveryState
from app.core.models import Attempt, Consumer, Delivery, MetricsSnapshot, Simulation
from app.core.scenario import seed_simulation
from app.core.settings import get_settings
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db


async def _seeded(session: AsyncSession) -> Simulation:
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    return sim


async def _attempts_at(session: AsyncSession, sim: Simulation, offsets: list[float]) -> None:
    """One attempt row per offset, in virtual seconds since the start of the run."""
    consumer_id = await session.scalar(
        select(Consumer.id).where(Consumer.simulation_id == sim.id).order_by(Consumer.id).limit(1)
    )
    await ingest_events(session, sim, [EventSpec("invoice.paid", f"in_{n}") for n in range(len(offsets))])
    delivery_ids = (
        (
            await session.execute(
                select(Delivery.id)
                .where(Delivery.simulation_id == sim.id, Delivery.consumer_id == consumer_id)
                .order_by(Delivery.id)
            )
        )
        .scalars()
        .all()
    )
    for offset, delivery_id in zip(offsets, delivery_ids, strict=True):
        session.add(
            Attempt(
                simulation_id=sim.id,
                delivery_id=delivery_id,
                consumer_id=consumer_id,
                worker_id=uuid.uuid4(),
                started_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=offset),
                finished_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=offset + 0.2),
                outcome=AttemptOutcome.OK.value,
                status_code=200,
            )
        )
    await session.flush()


async def _buckets(conn: AsyncConnection, simulation_id: uuid.UUID) -> list[Mapping[str, Any]]:
    result = await conn.execute(
        select(MetricsSnapshot)
        .where(MetricsSnapshot.simulation_id == simulation_id)
        .order_by(MetricsSnapshot.bucket_virtual_s, MetricsSnapshot.consumer_id)
    )
    return list(result.mappings().all())


async def test_buckets_are_contiguous_and_cover_every_consumer(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """A missing point is indistinguishable from a zero, so there are none.

    The rows are written as the full consumer x bucket cross product precisely
    so a client never has to guess which one it is looking at.
    """
    sim = await _seeded(session)
    now = VIRTUAL_EPOCH_ZERO + timedelta(seconds=10)

    await MetricsWriter().write(connection, sim.id, now, await read_gauges(connection, sim.id))

    rows = await _buckets(connection, sim.id)
    written = sorted({row["bucket_virtual_s"] for row in rows})
    # Two buckets of lag: a bucket is written once, so it must be complete first.
    assert written == list(range(0, 9))
    assert len(rows) == 3 * len(written)


async def test_per_bucket_attempts_sum_to_the_total(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The trap. A conductor that sampled "the current bucket" would fail here.

    At 20x a pass covers a whole virtual second, so sampling would record one
    bucket's worth of attempts and drop the rest -- an undercount by a roughly
    constant factor, which on a 100% stacked chart is invisible.
    """
    sim = await _seeded(session)
    offsets = [0.5, 1.5, 1.7, 3.2, 3.9, 7.9]
    await _attempts_at(session, sim, offsets)

    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=10), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    assert sum(row["attempts"] for row in rows) == len(offsets)
    # And landed in the right buckets, not merely the right total.
    by_bucket = {row["bucket_virtual_s"]: row["attempts"] for row in rows if row["attempts"]}
    assert by_bucket == {0: 1, 1: 2, 3: 2, 7: 1}


async def test_a_second_pass_continues_rather_than_repeating(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The cursor only moves forward, and leaves no gap where it moved."""
    sim = await _seeded(session)
    await _attempts_at(session, sim, [0.5, 4.5, 11.5, 12.5])
    writer = MetricsWriter()

    await writer.write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=10), await read_gauges(connection, sim.id)
    )
    await writer.write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=20), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    written = sorted({row["bucket_virtual_s"] for row in rows})
    assert written == list(range(0, 19))
    assert sum(row["attempts"] for row in rows) == 4


async def test_a_new_leader_backfills_the_gap_it_inherited(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Failover must not leave a permanent hole in the chart.

    A writer with a memory-only cursor starts from nothing and would either
    re-write from zero or skip the gap entirely. Recovering the cursor from
    ``MAX(bucket_virtual_s)`` is what makes the new leader fill in exactly the
    buckets the old one never got to -- at the moment the demo is showing off
    failover, which is the worst possible moment for the chart to break.
    """
    sim = await _seeded(session)
    await _attempts_at(session, sim, [1.5, 12.5, 25.5])

    # The old leader gets as far as bucket 8, then its process ends.
    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=10), await read_gauges(connection, sim.id)
    )

    # A different process, with no memory of any of that.
    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=30), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    written = sorted({row["bucket_virtual_s"] for row in rows})
    assert written == list(range(0, 29)), "the gap between leaders was not backfilled"
    assert sum(row["attempts"] for row in rows) == 3


async def test_backfill_is_capped_per_pass(
    session: AsyncSession,
    connection: AsyncConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long gap catches up over several passes, not one enormous write."""
    monkeypatch.setattr(get_settings(), "metrics_max_backfill_buckets", 10)

    sim = await _seeded(session)
    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=100), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    written = sorted({row["bucket_virtual_s"] for row in rows})
    assert written == list(range(89, 99))


async def test_gauges_use_the_same_backlog_definition_as_the_consumer_cards(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """The card and the chart must not disagree about the same number."""
    sim = await _seeded(session)
    await ingest_events(session, sim, [EventSpec("invoice.paid", f"in_{n}") for n in range(4)])

    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=5), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    latest = [row for row in rows if row["bucket_virtual_s"] == max(r["bucket_virtual_s"] for r in rows)]
    assert sum(row["backlog"] for row in latest) == 12  # 4 events x 3 subscribers
    assert all(row["ready"] == 0 and row["in_flight"] == 0 for row in latest)


async def test_terminal_states_land_in_the_bucket_they_completed_in(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Counters are derived from ``completed_at``, not from when anyone looked."""
    sim = await _seeded(session)
    await ingest_events(session, sim, [EventSpec("invoice.paid", "in_1")])
    delivery = (
        await session.execute(select(Delivery).where(Delivery.simulation_id == sim.id).limit(1))
    ).scalar_one()
    delivery.state = DeliveryState.DELIVERED.value
    delivery.completed_at = VIRTUAL_EPOCH_ZERO + timedelta(seconds=6.4)
    await session.flush()

    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=12), await read_gauges(connection, sim.id)
    )

    rows = await _buckets(connection, sim.id)
    delivered = {row["bucket_virtual_s"]: row["delivered"] for row in rows if row["delivered"]}
    assert delivered == {6: 1}


def test_the_bucket_key_survives_a_speed_change() -> None:
    """Trap one, as arithmetic rather than as a chart.

    ``sim.virtual_epoch`` is rebased on every speed change, so a bucket key
    derived from it renumbers the whole series -- new rows collide with old ones
    through the upsert, and ``?since_bucket=`` stops being monotonic, which
    freezes the chart. Keying off VIRTUAL_EPOCH_ZERO makes that unrepresentable.
    """
    instant = VIRTUAL_EPOCH_ZERO + timedelta(seconds=137.5)
    before = bucket_index(instant, 1.0)

    config = set_speed(SimulationClockConfig.from_row(a_simulation()), 5.0)
    assert config.virtual_epoch != VIRTUAL_EPOCH_ZERO, "the epoch really does move"

    assert bucket_index(instant, 1.0) == before == 137


async def test_a_simulation_with_no_consumers_writes_nothing(
    session: AsyncSession, connection: AsyncConnection
) -> None:
    """Rows are the consumer x bucket cross product, and an empty cast is empty."""
    sim = a_simulation()
    session.add(sim)
    await session.flush()

    await MetricsWriter().write(
        connection, sim.id, VIRTUAL_EPOCH_ZERO + timedelta(seconds=10), await read_gauges(connection, sim.id)
    )

    count = await connection.scalar(
        select(func.count()).select_from(MetricsSnapshot).where(MetricsSnapshot.simulation_id == sim.id)
    )
    assert count == 0
