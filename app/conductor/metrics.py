"""The chart series: one ``metrics_snapshot`` row per consumer per virtual second.

Two traps live here, and both fail *silently* -- they produce a plausible chart
that is wrong, which is the worst possible failure mode for the artefact whose
entire job is to prove a fairness claim.

**Trap one: the bucket key must come from a fixed origin.** ``sim.virtual_epoch``
is rebased on every pause, resume and speed change (that is how the derived clock
keeps virtual time continuous across those events), so bucketing against it
renumbers every bucket the first time somebody touches the speed slider: new rows
collide with old ones through the upsert, and ``?since_bucket=`` stops being
monotonic, which freezes the chart. Buckets are keyed off
:data:`~app.core.clock.VIRTUAL_EPOCH_ZERO`, the same origin behind
``SimulationRead.virtual_now_s``, so the key and the x-axis agree by construction.

**Trap two: counters cannot be sampled.** A conductor pass covers
``interval x speed`` virtual seconds -- at the shipped defaults, a whole bucket
per pass, and more if a pass runs long. Writing "the current bucket's count"
would undercount attempts by a roughly constant factor. And because the fairness
proof is a *100% stacked* chart, an equal undercount across three consumers draws
a chart that looks exactly right. So counters are **derived, not sampled**: two
grouped queries over ``attempt.started_at`` and ``delivery.completed_at``, which
are exact and gap-free by construction. Only the three gauges are sampled.

*(Considered and rejected: Redis counters incremented by workers. They attribute
buckets correctly with no backfill, but they are lossy -- a restart leaves the
``attempt`` table as the only source of truth, so this query gets written anyway,
as a second mechanism. It would also spend "Postgres as the only shared state"
on observability rather than on the fairness window, which is the read-modify-write
that actually forces the conductor to be a singleton and the place the design
already earmarks Redis for.)*
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Integer, SQLColumnExpression, cast, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.elements import ColumnElement

from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import ACTIVE_DELIVERY_STATES, DeliveryState
from app.core.models import Attempt, Consumer, Delivery, MetricsSnapshot
from app.core.settings import get_settings

#: How many buckets behind the current one to stop writing at.
#:
#: A bucket is written once and never revisited, so it must be *complete* first
#: -- writing the in-progress bucket would record half its attempts and then
#: never correct them. One bucket of lag covers completeness; the second covers
#: commit visibility, because a worker that stamped ``started_at`` at the very
#: end of a bucket may not have committed by the time this query runs. At the
#: shipped defaults that is 100ms of real lag on the chart, which is invisible,
#: and it is the difference between exact counters and quietly lossy ones.
_COMPLETE_BUCKET_LAG = 2

#: Terminal states that get their own counter column.
_TERMINAL_COUNTERS = (
    DeliveryState.DELIVERED,
    DeliveryState.EXPIRED,
    DeliveryState.SUPERSEDED,
    DeliveryState.FAILED,
)


def bucket_index(virtual_time: datetime, bucket_s: float) -> int:
    """Which bucket a virtual instant falls in.

    Keyed off :data:`VIRTUAL_EPOCH_ZERO`, never ``sim.virtual_epoch`` -- see the
    module docstring. At the default bucket size of one virtual second the index
    *is* the virtual second, which is what the chart's x-axis assumes; changing
    ``metrics_bucket_virtual_s`` means teaching the frontend to scale it.
    """
    return int((virtual_time - VIRTUAL_EPOCH_ZERO).total_seconds() // bucket_s)


def _bucket_of(column: SQLColumnExpression[Any], bucket_s: float) -> ColumnElement[int]:
    """The same arithmetic, evaluated in Postgres over a whole column."""
    seconds = func.extract("epoch", column - VIRTUAL_EPOCH_ZERO)
    return cast(func.floor(seconds / bucket_s), Integer)


#: Per consumer, per active state. Read once per pass and used twice: written
#: into the buckets as the three gauges, and summed to decide whether the run has
#: anything left to do.
Gauges = dict[int, dict[str, int]]


async def read_gauges(conn: AsyncConnection, simulation_id: uuid.UUID) -> Gauges:
    """Point-in-time queue depths -- the one thing here that genuinely is a sample.

    Same grouping as ``GET /api/consumer``'s counters, deliberately: the card and
    the chart must not disagree about the same number.
    """
    result = await conn.execute(
        select(Delivery.consumer_id, Delivery.state, func.count())
        .where(
            Delivery.simulation_id == simulation_id,
            Delivery.state.in_([s.value for s in ACTIVE_DELIVERY_STATES]),
        )
        .group_by(Delivery.consumer_id, Delivery.state)
    )
    out: Gauges = defaultdict(dict)
    for consumer_id, state, count in result:
        out[consumer_id][state] = count
    return out


def total_backlog(gauges: Gauges) -> int:
    """Everything still on its way somewhere, across every consumer."""
    return sum(count for states in gauges.values() for count in states.values())


class MetricsWriter:
    """Writes complete buckets, backfilling any the last leader missed."""

    __slots__ = ("_last_written",)

    def __init__(self) -> None:
        #: Cache only. The authority is ``MAX(bucket_virtual_s)``, because a new
        #: leader has no memory of the old one's progress -- deriving the cursor
        #: from the table is what makes failover backfill the gap instead of
        #: stranding it. A memory-only cursor leaves a permanent hole in the
        #: chart at exactly the moment the demo is showing off failover.
        self._last_written: dict[uuid.UUID, int] = {}

    async def write(
        self,
        conn: AsyncConnection,
        simulation_id: uuid.UUID,
        now: datetime,
        gauges: Gauges,
    ) -> None:
        settings = get_settings()
        bucket_s = settings.metrics_bucket_virtual_s
        end = bucket_index(now, bucket_s) - _COMPLETE_BUCKET_LAG

        last = self._last_written.get(simulation_id)
        if last is None:
            last = await self._recover_cursor(conn, simulation_id)
        start = last + 1 if last is not None else 0
        if start > end:
            return

        # Bound one pass's write. A long conductor gap -- a failover, a redeploy
        # -- then catches up over several passes rather than one write that
        # stalls the loop for as long as the gap was.
        start = max(start, end - settings.metrics_max_backfill_buckets + 1)

        consumer_ids = await self._consumer_ids(conn, simulation_id)
        if not consumer_ids:
            return

        window_from = VIRTUAL_EPOCH_ZERO + timedelta(seconds=start * bucket_s)
        window_to = VIRTUAL_EPOCH_ZERO + timedelta(seconds=(end + 1) * bucket_s)

        attempts = await self._attempts(conn, simulation_id, bucket_s, window_from, window_to)
        terminals = await self._terminals(conn, simulation_id, bucket_s, window_from, window_to)

        rows = []
        for consumer_id in consumer_ids:
            gauge = gauges.get(consumer_id, {})
            backlog = sum(gauge.get(state, 0) for state in ACTIVE_DELIVERY_STATES)
            for bucket in range(start, end + 1):
                terminal = terminals.get((consumer_id, bucket), {})
                rows.append(
                    {
                        "simulation_id": simulation_id,
                        "consumer_id": consumer_id,
                        "bucket_virtual_s": bucket,
                        # Gauges are the value *now*, carried across every bucket
                        # this pass writes. Exact only for the newest one; the
                        # backfilled ones are a straight line through a gap the
                        # conductor was not there to observe, which is the honest
                        # thing to draw and is why counters are not done this way.
                        "backlog": backlog,
                        "ready": gauge.get(DeliveryState.READY, 0),
                        "in_flight": gauge.get(DeliveryState.IN_FLIGHT, 0),
                        "attempts": attempts.get((consumer_id, bucket), 0),
                        "delivered": terminal.get(DeliveryState.DELIVERED, 0),
                        "expired": terminal.get(DeliveryState.EXPIRED, 0),
                        "superseded": terminal.get(DeliveryState.SUPERSEDED, 0),
                        "failed": terminal.get(DeliveryState.FAILED, 0),
                    }
                )

        # The full consumer x bucket cross product, so a bucket in which a
        # consumer did nothing is a zero rather than a hole. A chart that has to
        # guess whether a missing point means zero or means "no data" will guess
        # wrong at least once, on camera.
        await self._upsert(conn, rows)
        self._last_written[simulation_id] = end

    # -- reads --------------------------------------------------------------

    async def _recover_cursor(self, conn: AsyncConnection, simulation_id: uuid.UUID) -> int | None:
        highest: int | None = await conn.scalar(
            select(func.max(MetricsSnapshot.bucket_virtual_s)).where(
                MetricsSnapshot.simulation_id == simulation_id
            )
        )
        return highest

    async def _consumer_ids(self, conn: AsyncConnection, simulation_id: uuid.UUID) -> list[int]:
        result = await conn.execute(
            select(Consumer.id).where(Consumer.simulation_id == simulation_id).order_by(Consumer.id)
        )
        return [row[0] for row in result]

    async def _attempts(
        self,
        conn: AsyncConnection,
        simulation_id: uuid.UUID,
        bucket_s: float,
        window_from: datetime,
        window_to: datetime,
    ) -> dict[tuple[int, int], int]:
        bucket = _bucket_of(Attempt.started_at, bucket_s)
        result = await conn.execute(
            select(Attempt.consumer_id, bucket, func.count())
            .where(
                Attempt.simulation_id == simulation_id,
                Attempt.started_at >= window_from,
                Attempt.started_at < window_to,
            )
            .group_by(Attempt.consumer_id, bucket)
        )
        return {(consumer_id, b): count for consumer_id, b, count in result}

    async def _terminals(
        self,
        conn: AsyncConnection,
        simulation_id: uuid.UUID,
        bucket_s: float,
        window_from: datetime,
        window_to: datetime,
    ) -> dict[tuple[int, int], dict[str, int]]:
        bucket = _bucket_of(Delivery.completed_at, bucket_s)
        result = await conn.execute(
            select(Delivery.consumer_id, bucket, Delivery.state, func.count())
            .where(
                Delivery.simulation_id == simulation_id,
                Delivery.completed_at >= window_from,
                Delivery.completed_at < window_to,
                Delivery.state.in_([s.value for s in _TERMINAL_COUNTERS]),
            )
            .group_by(Delivery.consumer_id, bucket, Delivery.state)
        )
        out: dict[tuple[int, int], dict[str, int]] = defaultdict(dict)
        for consumer_id, b, state, count in result:
            out[(consumer_id, b)][state] = count
        return out

    async def _gauges(self, conn: AsyncConnection, simulation_id: uuid.UUID) -> dict[int, dict[str, int]]:
        """Point-in-time depths, the one thing here that genuinely is a sample.

        Same grouping as ``GET /api/consumer``'s counters, deliberately: the card
        and the chart must not disagree about the same number.
        """
        result = await conn.execute(
            select(Delivery.consumer_id, Delivery.state, func.count())
            .where(
                Delivery.simulation_id == simulation_id,
                Delivery.state.in_([s.value for s in ACTIVE_DELIVERY_STATES]),
            )
            .group_by(Delivery.consumer_id, Delivery.state)
        )
        out: dict[int, dict[str, int]] = defaultdict(dict)
        for consumer_id, state, count in result:
            out[consumer_id][state] = count
        return out

    # -- write --------------------------------------------------------------

    async def _upsert(self, conn: AsyncConnection, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        stmt = pg_insert(MetricsSnapshot).values(rows)
        # Idempotent, so a pass that is retried after a failed commit costs
        # nothing. Under normal operation it never fires: the cursor only ever
        # moves forward, so a bucket is written exactly once.
        await conn.execute(
            stmt.on_conflict_do_update(
                constraint="uq_metrics_snapshot_simulation_id_consumer_id_bucket_virtual_s",
                set_={
                    column: stmt.excluded[column]
                    for column in (
                        "backlog",
                        "ready",
                        "in_flight",
                        "attempts",
                        "delivered",
                        "expired",
                        "superseded",
                        "failed",
                    )
                },
            )
        )


__all__ = ["Gauges", "MetricsWriter", "bucket_index", "read_gauges", "total_backlog"]
