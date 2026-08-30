"""Claim, lease, complete -- the two transactions a worker iteration is made of.

Batched, because per-attempt round trips do not survive 20x: one claim
transaction, then every transport call concurrently, then one completion
transaction. Two round trips per batch of sixteen rather than two per delivery.

Two properties fall out of that shape for free, and both are load-bearing:

* **No row lock is held across the network.** The claim transaction commits
  before any transport runs, so a slow consumer blocks nothing but its own
  concurrency slots.
* **The drain guarantee still covers it.** The work stays inside a single
  ``loop_body`` call rather than escaping into background tasks, so the runner's
  "an iteration that has started is allowed to finish" applies unchanged, with
  no task bookkeeping to write. Lease reaping is out of scope, which is exactly
  why the graceful shutdown path has to strand nothing.

``attempt`` rows are inserted at **claim** time, not completion, because the
fairness window counts attempts *started*. Batching them into the claim
transaction makes that free rather than an extra write.

Every statement runs on ``session.connection()`` rather than through the session
itself. These are Core statements over rows nothing ever loaded as objects, so
there is no identity map to keep in step -- and the ORM, asked to execute an
executemany UPDATE that carries its own WHERE criteria, tries to interpret it as
a bulk update by primary key and refuses. The session still owns the
transaction; it just is not asked to interpret the SQL.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Row, bindparam, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.core.enums import AttemptOutcome, DeliveryState
from app.core.models import Attempt, Consumer, Delivery
from app.core.settings import get_settings
from app.worker.transport import AttemptRequest, AttemptResult, ConsumerProfile


@dataclass(frozen=True, slots=True)
class Claimed:
    """One leased delivery, with everything an attempt needs already in hand."""

    request: AttemptRequest
    attempt_id: int


async def claim_batch(
    session: AsyncSession,
    simulation_id: uuid.UUID,
    worker_id: uuid.UUID,
    now: datetime,
    limit: int,
) -> list[Claimed]:
    """Take up to ``limit`` ready deliveries, exclusively.

    ``FOR UPDATE ... SKIP LOCKED`` is the whole concurrency story: a row another
    worker is mid-claim on is skipped rather than waited on, so N workers make
    progress on disjoint sets with no coordinator and no partitioning scheme.
    ``OF delivery`` keeps the lock off the joined ``consumer`` row, which every
    worker reads on every iteration and none of them is updating.

    Scoped per simulation so the predicate matches ``ix_delivery_ready``
    exactly, and so the lease can be stamped with *that* simulation's clock.
    """
    if limit <= 0:
        return []

    conn = await session.connection()
    rows = (
        await conn.execute(
            select(
                Delivery.id,
                Delivery.consumer_id,
                Delivery.event_type,
                Delivery.entity_key,
                Delivery.attempt_count,
                Consumer.sim_latency_s,
                Consumer.sim_jitter_s,
                Consumer.sim_failure_rate,
                Consumer.sim_down,
            )
            .join(Consumer, Consumer.id == Delivery.consumer_id)
            .where(
                Delivery.simulation_id == simulation_id,
                Delivery.state == DeliveryState.READY.value,
            )
            .order_by(Delivery.ready_at)
            .limit(limit)
            .with_for_update(skip_locked=True, of=Delivery)
        )
    ).all()

    if not rows:
        return []

    settings = get_settings()
    lease_expires_at = now + timedelta(seconds=settings.lease_duration_virtual_s)
    delivery_ids = [row.id for row in rows]

    await conn.execute(
        update(Delivery)
        .where(Delivery.id.in_(delivery_ids))
        .values(
            state=DeliveryState.IN_FLIGHT.value,
            leased_by=worker_id,
            lease_expires_at=lease_expires_at,
            attempt_count=Delivery.attempt_count + 1,
        )
    )

    attempt_ids = await _insert_attempts(conn, simulation_id, worker_id, rows, now)

    return [
        Claimed(
            request=AttemptRequest(
                simulation_id=simulation_id,
                delivery_id=row.id,
                consumer_id=row.consumer_id,
                attempt_no=row.attempt_count + 1,
                event_type=row.event_type,
                entity_key=row.entity_key,
                profile=ConsumerProfile(
                    latency_s=row.sim_latency_s,
                    jitter_s=row.sim_jitter_s,
                    failure_rate=row.sim_failure_rate,
                    down=row.sim_down,
                ),
            ),
            attempt_id=attempt_ids[row.id],
        )
        for row in rows
    ]


async def _insert_attempts(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    worker_id: uuid.UUID,
    rows: Sequence[Row[Any]],
    now: datetime,
) -> dict[int, int]:
    """Insert one ``attempt`` per claimed delivery; return delivery id -> attempt id.

    Keyed by ``delivery_id`` rather than trusting ``RETURNING`` to come back in
    ``VALUES`` order, which Postgres happens to do and does not promise.
    """
    result = await conn.execute(
        insert(Attempt)
        .values(
            [
                {
                    "simulation_id": simulation_id,
                    "delivery_id": row.id,
                    "consumer_id": row.consumer_id,
                    "worker_id": worker_id,
                    "started_at": now,
                }
                for row in rows
            ]
        )
        .returning(Attempt.id, Attempt.delivery_id)
    )
    return {delivery_id: attempt_id for attempt_id, delivery_id in result}


@dataclass(frozen=True, slots=True)
class Completion:
    claimed: Claimed
    result: AttemptResult


async def complete_batch(
    session: AsyncSession,
    completions: list[Completion],
    now: datetime,
) -> None:
    """Record every outcome, in one transaction.

    Terminal rows **must** stamp ``completed_at``: the decision feed filters on
    it being non-null, so a delivery that reaches ``delivered`` without one is
    delivered and invisible.
    """
    if not completions:
        return

    settings = get_settings()
    conn = await session.connection()

    # executemany with explicit bindparams: one prepared statement, one round
    # trip, N parameter sets. The `b_` prefix keeps the WHERE parameter from
    # colliding with a column name in the SET clause.
    await conn.execute(
        update(Attempt)
        .where(Attempt.id == bindparam("b_attempt_id"))
        .values(
            finished_at=now,
            outcome=bindparam("b_outcome"),
            status_code=bindparam("b_status_code"),
        ),
        [
            {
                "b_attempt_id": c.claimed.attempt_id,
                "b_outcome": c.result.outcome.value,
                "b_status_code": c.result.status_code,
            }
            for c in completions
        ],
    )

    delivered: list[int] = []
    failed: list[dict[str, object]] = []
    retry: list[dict[str, object]] = []

    for c in completions:
        if c.result.outcome is AttemptOutcome.OK:
            delivered.append(c.claimed.request.delivery_id)
        elif c.claimed.request.attempt_no >= settings.max_attempts:
            failed.append(
                {
                    "b_delivery_id": c.claimed.request.delivery_id,
                    "b_terminal_reason": (f"retry cap reached after {c.claimed.request.attempt_no} attempts"),
                }
            )
        else:
            retry.append(
                {
                    "b_delivery_id": c.claimed.request.delivery_id,
                    "b_next_attempt_at": (now + timedelta(seconds=backoff_s(c.claimed.request.attempt_no))),
                }
            )

    # The lease columns are deliberately left as they are. They are a record of
    # the last worker to hold the row, and every reader that cares -- the
    # process strip, and the reaper predicate if one is ever built -- gates on
    # `state = 'in_flight'` anyway, so a stale lease on a settled row is inert.
    if delivered:
        await conn.execute(
            update(Delivery)
            .where(Delivery.id.in_(delivered))
            .values(state=DeliveryState.DELIVERED.value, completed_at=now)
        )
    if failed:
        await conn.execute(
            update(Delivery)
            .where(Delivery.id == bindparam("b_delivery_id"))
            .values(
                state=DeliveryState.FAILED.value,
                completed_at=now,
                terminal_reason=bindparam("b_terminal_reason"),
            ),
            failed,
        )
    if retry:
        await conn.execute(
            update(Delivery)
            .where(Delivery.id == bindparam("b_delivery_id"))
            .values(
                state=DeliveryState.PENDING.value,
                next_attempt_at=bindparam("b_next_attempt_at"),
            ),
            retry,
        )


def backoff_s(attempt_no: int) -> float:
    """``base * 2^(n-1)``, capped. Virtual seconds, like every other delay."""
    settings = get_settings()
    uncapped: float = settings.retry_backoff_base_virtual_s * (2 ** (attempt_no - 1))
    return min(uncapped, settings.retry_backoff_cap_virtual_s)


__all__ = ["Claimed", "Completion", "backoff_s", "claim_batch", "complete_batch"]
