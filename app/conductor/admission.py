"""Admission control: which pending deliveries become ``ready``, and how many.

``ready`` is an **admission-control token materialized as a row state** -- the
conductor has decided this specific delivery may be attempted now. Its buffer
depth *is* the granularity of fairness, which is why the buffer is kept shallow
and topped up continuously rather than filled in bulk.

Two arms, and the toggle between them is the first claim's whole argument:

* **ON** -- weighted round-robin across dispatchable consumers, work-conserving.
* **OFF** -- global FIFO under one shared pool. This is the naive implementation
  most systems ship, and it has to be *plausible* rather than a strawman built to
  lose, so per-consumer rate caps still apply to it. They are a consumer
  protection contract, not a fairness mechanism.

**Policy runs on both arms.** Turning fairness off must not quietly turn Bolt's
policies off too, or the toggle stops isolating one variable and the comparison
proves nothing.

Deciding (:func:`select_candidates`) is kept separate from writing
(:func:`mark_ready`): the fairness arm changes *which* rows come back from the
first, and the second is identical on both arms.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, Select, and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from app.conductor.metrics import Gauges
from app.conductor.policy import Candidate, Drop, PolicyMap, evaluate
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


@dataclass(frozen=True, slots=True)
class Selection:
    """What a pass decided.

    ``drops`` is not a by-product of rationing: these are deliveries their own
    consumer's policy says not to send, and they are dropped whether or not
    there was capacity to admit them.
    """

    admit: list[int]
    drops: list[Drop]


@dataclass(frozen=True, slots=True)
class ConsumerState:
    """One consumer's knobs and its current claim on the provider."""

    id: int
    weight: float
    concurrency_cap: int
    max_attempts_per_s: float
    #: Pending deliveries that are due now. Zero means not dispatchable.
    demand: int
    #: Attempts started inside the fairness window.
    attempts_in_window: int
    in_flight: int
    ready: int

    def headroom(self, window_s: float, *, pooled_concurrency: bool) -> int:
        """How many more deliveries this consumer could take right now.

        The rate term subtracts ``ready`` as well as attempts: work already
        admitted but not yet attempted has no ``attempt`` row, so a window query
        cannot see it and would spend the same slot twice. That is exactly the
        correction :func:`compute_budget` makes globally, applied per consumer,
        and it is the read-modify-write the conductor has to be a singleton for.

        **Concurrency is a gate, not a quantity** (§Fairness: dispatchable when
        ``COUNT(*) WHERE state='in_flight' < cap``). It has to be, because the
        ready buffer is deliberately ~1.5x the sum of the caps so workers never
        starve waiting on a pass -- so bounding admissions by
        ``cap - in_flight - ready`` per consumer would hold the buffer at exactly
        ``sum(cap)`` and quietly make the buffer multiplier dead. The cost is
        that the cap is enforced with about one pass of lag rather than as a
        hard reservation: a consumer sitting just under its cap can be admitted
        more work than it has slots for, and stops being admitted any on the
        next pass. At a 50ms loop that lag is invisible, and calling it a hard
        cap would be the dishonest way to describe it.

        ``pooled_concurrency`` is the fair-drain-off arm: §Fairness puts the
        naive path under one shared concurrency pool, which the global ready
        buffer already expresses, so only the rate cap is per-consumer there.
        """
        if not pooled_concurrency and self.in_flight >= self.concurrency_cap:
            return 0
        used = self.attempts_in_window + self.ready
        return max(0, min(self.demand, int(self.max_attempts_per_s * window_s) - used))


async def compute_budget(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    global_attempts_per_s: float,
    now: datetime,
) -> Budget:
    """The two ceilings on this pass.

    The rate term is a **sliding window over the ``attempt`` table**, not a
    token-bucket column: one mechanism, no mutable counter for three process
    types to keep consistent, and it is exactly the query fairness needs
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


def allocate(states: list[ConsumerState], headroom: dict[int, int], budget: int) -> dict[int, int]:
    """Weighted round-robin: hand out slots to whoever is furthest behind.

    One slot at a time, always to the consumer with the lowest
    ``granted / weight`` -- the textbook weighted-fair-queueing choice, in
    integer form because slots are indivisible.

    Work-conserving falls out for free: a consumer that reaches its headroom
    leaves the active set, and the remaining slots go to whoever is left. That
    is what makes Clover's segment go to zero once it drains, rather than
    reserving a third of the provider for a consumer with nothing to send.

    Deliberately *not* a deficit carried across passes. Repaying a consumer that
    was idle for a whole window would hand it the entire window's share the
    instant it had work again -- a burst that spikes the share chart to ~100%
    and is the opposite of the smooth handover the claim is about. Fairness is
    per pass; the sliding window is what enforces the *caps*.

    ``budget`` is small (~36 at the shipped settings), so a slot-at-a-time loop
    costs nothing and is worth far more than a faster formulation nobody can
    check by eye.
    """
    granted = {state.id: 0 for state in states}
    weights = {state.id: state.weight for state in states}
    active = {state.id for state in states if headroom.get(state.id, 0) > 0}

    for _ in range(max(0, budget)):
        if not active:
            break
        # Tie-broken on consumer id so a pass is deterministic and a test can
        # assert an exact split rather than a distribution.
        winner = min(active, key=lambda cid: (granted[cid] / weights[cid], cid))
        granted[winner] += 1
        if granted[winner] >= headroom[winner]:
            active.discard(winner)

    return granted


async def read_consumer_states(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    gauges: Gauges,
) -> list[ConsumerState]:
    """The knobs, the demand, and the window usage -- in two queries.

    The demand count rides along on the consumer read as a ``LEFT JOIN``, so a
    consumer with nothing due comes back with ``demand = 0`` rather than being
    missing. That distinction is load-bearing: it is the difference between "not
    dispatchable, redistribute its share" and "absent from the result, so
    invisible to the allocator".

    ``in_flight`` and ``ready`` come from the gauges the pass already read for
    the metric buckets, so they cost nothing here.
    """
    settings = get_settings()
    window_s = settings.fairness_window_virtual_s

    due = and_(
        Delivery.consumer_id == Consumer.id,
        Delivery.state == DeliveryState.PENDING.value,
        Delivery.next_attempt_at <= now,
    )
    consumers = await conn.execute(
        select(
            Consumer.id,
            Consumer.weight,
            Consumer.concurrency_cap,
            Consumer.max_attempts_per_s,
            func.count(Delivery.id).label("demand"),
        )
        .select_from(Consumer)
        .outerjoin(Delivery, due)
        .where(Consumer.simulation_id == simulation_id)
        .group_by(Consumer.id)
        .order_by(Consumer.id)
    )

    window = await conn.execute(
        select(Attempt.consumer_id, func.count())
        .where(
            Attempt.simulation_id == simulation_id,
            Attempt.started_at >= now - timedelta(seconds=window_s),
        )
        .group_by(Attempt.consumer_id)
    )
    attempts: dict[int, int] = {row[0]: row[1] for row in window}

    return [
        ConsumerState(
            id=row.id,
            weight=row.weight,
            concurrency_cap=row.concurrency_cap,
            max_attempts_per_s=row.max_attempts_per_s,
            demand=row.demand,
            attempts_in_window=attempts.get(row.id, 0),
            in_flight=gauges.get(row.id, {}).get(DeliveryState.IN_FLIGHT, 0),
            ready=gauges.get(row.id, {}).get(DeliveryState.READY, 0),
        )
        for row in consumers
    ]


async def select_candidates(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    *,
    budget: Budget,
    gauges: Gauges,
    policies: PolicyMap,
    fair_drain: bool,
) -> Selection:
    """Decide what to admit, and what its owner's policy says to drop instead.

    The interlock between the two claims lives here. **Fairness rations
    attempts, and a policy drop is not an attempt** -- so a pass deliberately
    over-fetches candidates, runs policy across all of them, drops every one the
    policy condemns, and only then rations what survived. A loop that rationed
    *candidates* would leave Bolt unable to use its share the moment coalescing
    started killing most of them, which during recovery is nearly all of them.

    Over-fetching past what could be admitted is therefore not waste: it is how
    a backlog of superseded work collapses in a few passes instead of trickling
    out at the admission rate. Bolt's backlog falling off a cliff is the second
    claim, not a scheduling accident.
    """
    if budget.slots <= 0:
        return Selection(admit=[], drops=[])

    settings = get_settings()
    states = await read_consumer_states(conn, simulation_id, now, gauges)
    if not states:
        return Selection(admit=[], drops=[])

    headroom = {
        state.id: state.headroom(settings.fairness_window_virtual_s, pooled_concurrency=not fair_drain)
        for state in states
    }
    allowances = (
        allocate(states, headroom, budget.slots)
        if fair_drain
        # The naive arm: one shared pool, first come first served, with each
        # consumer still protected by its own rate cap.
        else {state.id: min(headroom[state.id], budget.slots) for state in states}
    )
    if not any(allowances.values()):
        return Selection(admit=[], drops=[])

    overfetch = settings.admission_overfetch
    candidates = (
        await _fair_candidates(conn, simulation_id, now, allowances, overfetch)
        if fair_drain
        else await _fifo_candidates(conn, simulation_id, now, budget.slots * overfetch)
    )

    verdict = await evaluate(conn, simulation_id, candidates, policies, now)

    admit: list[int] = []
    taken = {state.id: 0 for state in states}
    for candidate in verdict.admit:
        if len(admit) >= budget.slots:
            break
        if taken.get(candidate.consumer_id, 0) >= allowances.get(candidate.consumer_id, 0):
            continue
        taken[candidate.consumer_id] += 1
        admit.append(candidate.id)

    return Selection(admit=admit, drops=verdict.drops)


def _candidate_columns() -> Select[tuple[int, int, str, str, datetime]]:
    return select(
        Delivery.id,
        Delivery.consumer_id,
        Delivery.event_type,
        Delivery.entity_key,
        Delivery.created_at,
    )


def _due(simulation_id: uuid.UUID, now: datetime) -> tuple[ColumnElement[bool], ...]:
    return (
        Delivery.simulation_id == simulation_id,
        Delivery.state == DeliveryState.PENDING.value,
        Delivery.next_attempt_at <= now,
    )


async def _fifo_candidates(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    limit: int,
) -> list[Candidate]:
    """The oldest deliveries that are due, globally. The fair-drain-off arm.

    FIFO by ``next_attempt_at``, which ingest stamps with the event's own
    ``occurred_at`` and a retry pushes forward -- so this is FIFO by event time
    with retries correctly held back through their backoff. Under contention it
    hands each consumer a share of the provider proportional to its share of the
    *backlog*, which is precisely the failure the first claim is about.
    """
    result = await conn.execute(
        _candidate_columns().where(*_due(simulation_id, now)).order_by(Delivery.next_attempt_at).limit(limit)
    )
    return [Candidate(*row) for row in result]


async def _fair_candidates(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
    allowances: dict[int, int],
    overfetch: int,
) -> list[Candidate]:
    """The oldest due deliveries *per consumer*, in one round trip.

    ``ROW_NUMBER() OVER (PARTITION BY consumer_id ...)`` keeps this a single
    query no matter how many consumers there are, rather than one query each --
    which at a 50ms loop interval is the difference between a conductor pass
    that scales and one that does not.

    The bound is the largest allowance times the over-fetch factor, applied to
    every consumer rather than tailored per consumer. Fetching more than a small
    consumer could be granted is harmless and slightly useful: policy still runs
    over the extra rows, and anything it condemns is dropped now instead of
    next pass.
    """
    wanted = max(allowances.values(), default=0)
    if wanted <= 0:
        return []

    ranked = (
        _candidate_columns()
        .add_columns(
            func.row_number()
            .over(partition_by=Delivery.consumer_id, order_by=Delivery.next_attempt_at)
            .label("rank")
        )
        .where(*_due(simulation_id, now), Delivery.consumer_id.in_(sorted(allowances)))
        .subquery()
    )
    result = await conn.execute(
        select(
            ranked.c.id,
            ranked.c.consumer_id,
            ranked.c.event_type,
            ranked.c.entity_key,
            ranked.c.created_at,
        )
        .where(ranked.c.rank <= wanted * overfetch)
        .order_by(ranked.c.consumer_id, ranked.c.rank)
    )
    return [Candidate(*row) for row in result]


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


__all__ = [
    "Budget",
    "ConsumerState",
    "Selection",
    "allocate",
    "compute_budget",
    "mark_ready",
    "read_consumer_states",
    "select_candidates",
]
