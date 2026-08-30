"""Admission control: which pending deliveries become ``ready``, and how many.

``ready`` is an **admission-control token materialized as a row state** -- the
conductor has decided this specific delivery may be attempted now. Its buffer
depth *is* the granularity of fairness, which is why the buffer is kept shallow
and topped up continuously rather than filled in bulk.

Phase 1 is deliberately naive: global FIFO under one budget, no weights, no
per-consumer shares, no policy. That is not throwaway code -- it is the
``fair_drain = OFF`` arm, which Phase 2 needs anyway as the thing the toggle
compares against. It has to be a plausible implementation, not a strawman: this
is what most systems actually ship.

The split into :func:`select_candidates` and :func:`mark_ready` is the seam
Phase 2a slots into. Fairness changes *which* rows come back from the first;
the second does not change at all.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.enums import DeliveryState
from app.core.models import Attempt, Consumer, Delivery
from app.core.settings import get_settings


@dataclass(frozen=True, slots=True)
class Budget:
    """How many deliveries this pass may admit, and why.

    Both terms are kept rather than just their minimum because which one binds
    is the single most useful thing to know when the pipeline is not draining:
    ``buffer`` binding means the workers are behind, ``rate`` binding means the
    provider's global cap is the constraint, which is the *intended* steady
    state during recovery.
    """

    buffer_slots: int
    rate_slots: int

    @property
    def slots(self) -> int:
        return max(0, min(self.buffer_slots, self.rate_slots))


async def compute_budget(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    global_attempts_per_s: float,
    now: datetime,
) -> Budget:
    """The two ceilings on this pass.

    The rate term is a **sliding window over the ``attempt`` table**, not a
    token-bucket column: one mechanism, no mutable counter for three process
    types to keep consistent, and it is exactly the query Phase 2 needs
    per-consumer, on the index built for it (``ix_attempt_window``).
    """
    settings = get_settings()
    window_s = settings.fairness_window_virtual_s

    total_cap = await conn.scalar(
        select(func.coalesce(func.sum(Consumer.concurrency_cap), 0)).where(
            Consumer.simulation_id == simulation_id
        )
    )
    ready_count = await conn.scalar(
        select(func.count())
        .select_from(Delivery)
        .where(
            Delivery.simulation_id == simulation_id,
            Delivery.state == DeliveryState.READY.value,
        )
    )
    attempts_in_window = await conn.scalar(
        select(func.count())
        .select_from(Attempt)
        .where(
            Attempt.simulation_id == simulation_id,
            Attempt.started_at >= now - timedelta(seconds=window_s),
        )
    )

    buffer_target = math.ceil(settings.ready_buffer_depth_multiplier * int(total_cap or 0))

    # The subtlety the singleton exists for: work already admitted but not yet
    # attempted has no `attempt` row, so a pure window count cannot see it and
    # would admit against the same budget twice. Subtracting the outstanding
    # ready buffer closes that -- and it is a read-modify-write, which is
    # precisely why two conductors running this concurrently is not merely
    # wasteful but wrong.
    rate_budget = int(global_attempts_per_s * window_s)
    return Budget(
        buffer_slots=buffer_target - int(ready_count or 0),
        rate_slots=rate_budget - int(attempts_in_window or 0) - int(ready_count or 0),
    )


async def select_candidates(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    limit: int,
) -> list[int]:
    """The oldest deliveries that are due. **Phase 2a replaces this function.**

    Global FIFO by ``next_attempt_at``, which ingest stamps with the event's own
    ``occurred_at`` and a retry pushes forward -- so this is FIFO by event time
    with retries correctly held back through their backoff.

    Ordering globally rather than per consumer means this sorts rather than
    walking ``ix_delivery_pending`` (whose second column is ``consumer_id``).
    At demo backlog sizes that is nothing, and Phase 2's per-consumer queries
    use the index as designed.
    """
    if limit <= 0:
        return []
    result = await conn.execute(
        select(Delivery.id)
        .where(
            Delivery.simulation_id == simulation_id,
            Delivery.state == DeliveryState.PENDING.value,
            Delivery.next_attempt_at <= now,
        )
        .order_by(Delivery.next_attempt_at)
        .limit(limit)
    )
    return [row[0] for row in result]


async def mark_ready(
    conn: AsyncConnection,
    delivery_ids: list[int],
    now: datetime,
) -> int:
    """Admit. ``ready_at`` is the order workers will claim in."""
    if not delivery_ids:
        return 0
    result = await conn.execute(
        update(Delivery)
        .where(Delivery.id.in_(delivery_ids))
        .values(state=DeliveryState.READY.value, ready_at=now)
    )
    return result.rowcount


__all__ = ["Budget", "compute_budget", "mark_ready", "select_candidates"]
