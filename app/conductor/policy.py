"""Consumer-defined policy: which queued deliveries are still worth sending.

This is the second of the two claims. A consumer states, per event type, what it
wants replayed after an outage -- and the system honours that *before* spending
provider capacity, rather than delivering ten minutes of stale balance updates
because they happen to be in the queue.

Two mechanisms, deliberately one per event type in the shipped cast so that every
drop in the demo is attributable to a single cause:

* ``max_staleness_s`` -> ``expired``. A balance from five minutes ago is not
  the balance; a consumer that says so should not be sent it.
* ``coalesce: latest_by_key`` -> ``superseded``. Twenty subscriptions churned
  hundreds of times; only the latest state of each is worth delivering.

**Neither writes an ``attempt`` row.** Expired and superseded deliveries never
reach a worker, which is the whole point -- and it is also why fairness and
policy interlock the way they do: a drop consumes a *candidate* slot but not an
*attempt*, and fairness is measured in attempts. See
:func:`app.conductor.admission.select_candidates` for how that is resolved.

Evaluation happens at **dispatch time, not ingest time.** Whether an event is
stale depends on when the pipeline recovers, and whether it has been superseded
depends on what queued up behind it -- neither is knowable when the event lands.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import bindparam, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.enums import CoalesceMode, DeliveryState
from app.core.models import Delivery, DeliveryPolicy


@dataclass(frozen=True, slots=True)
class Policy:
    """What one consumer wants done with one event type."""

    max_staleness_s: float | None = None
    coalesce: CoalesceMode = CoalesceMode.NONE


#: An absent key means "deliver everything" -- which is what lets Bolt's third
#: policy row, the one that sets neither field, read as *chose not to* rather
#: than *forgot to*.
DELIVER_EVERYTHING = Policy()

PolicyMap = dict[tuple[int, str], Policy]


@dataclass(frozen=True, slots=True)
class Candidate:
    """Enough of a ``delivery`` row to decide its fate without a second query.

    ``created_at`` is the event's own ``occurred_at``: ingest stamps both from
    it and only a retry moves ``next_attempt_at`` forward
    (:func:`app.api.ingest.ingest_events`). So staleness is measured against the
    fact's own timestamp without ever joining ``event``.
    """

    id: int
    consumer_id: int
    event_type: str
    entity_key: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Drop:
    """A delivery the consumer's own policy says not to send."""

    delivery_id: int
    state: DeliveryState
    reason: str


@dataclass(frozen=True, slots=True)
class Verdict:
    """The split. ``admit`` keeps candidate order, so callers can still ration."""

    admit: list[Candidate]
    drops: list[Drop]


async def load_policies(conn: AsyncConnection, simulation_id: uuid.UUID) -> PolicyMap:
    """Every policy for a simulation, keyed by ``(consumer_id, event_type)``.

    One query per pass over a table holding a handful of rows -- small enough
    that caching it would only buy a staleness bug the first time somebody edits
    a policy live.
    """
    result = await conn.execute(
        select(
            DeliveryPolicy.consumer_id,
            DeliveryPolicy.event_type,
            DeliveryPolicy.max_staleness_s,
            DeliveryPolicy.coalesce,
        ).where(DeliveryPolicy.simulation_id == simulation_id)
    )
    return {
        (consumer_id, event_type): Policy(
            max_staleness_s=max_staleness_s,
            coalesce=CoalesceMode(coalesce),
        )
        for consumer_id, event_type, max_staleness_s, coalesce in result
    }


def stale_by(now: datetime, created_at: datetime, max_staleness_s: float | None) -> float | None:
    """Virtual seconds this delivery is *past* its staleness bound, or ``None``.

    Returns the overage rather than a bool so the terminal reason can say how
    late it was. "stale by 43s (max 120s)" is a sentence a reviewer can check
    against the clock; "expired" is one they have to take on faith.

    Takes the bound rather than a :class:`Policy` because the worker calls this
    too -- it is the one piece of policy logic the data plane contains, and it
    reads ``max_staleness_s`` straight off a join. Sharing the predicate rather
    than writing the comparison twice is what stops the dispatch-time check and
    the pre-attempt re-check from ever disagreeing about the same delivery.
    """
    if max_staleness_s is None:
        return None
    overage = (now - created_at).total_seconds() - max_staleness_s
    return overage if overage > 0 else None


def stale_reason(overage: float, max_staleness_s: float) -> str:
    """The decision-feed sentence, so both planes phrase it identically."""
    return f"stale by {overage:.0f}s (max {max_staleness_s:.0f}s)"


async def evaluate(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    candidates: list[Candidate],
    policies: PolicyMap,
    now: datetime,
) -> Verdict:
    """Split candidates into what to send and what to drop, in that order.

    Staleness first, because it is pure and free; coalescing second, over only
    what survived, because it costs a query.
    """
    if not candidates:
        return Verdict(admit=[], drops=[])

    drops: list[Drop] = []
    survived: list[Candidate] = []

    for candidate in candidates:
        policy = policies.get((candidate.consumer_id, candidate.event_type), DELIVER_EVERYTHING)
        overage = stale_by(now, candidate.created_at, policy.max_staleness_s)
        if overage is not None:
            assert policy.max_staleness_s is not None  # implied by a non-None overage
            drops.append(
                Drop(
                    delivery_id=candidate.id,
                    state=DeliveryState.EXPIRED,
                    reason=stale_reason(overage, policy.max_staleness_s),
                )
            )
        else:
            survived.append(candidate)

    coalescing = [
        c
        for c in survived
        if policies.get((c.consumer_id, c.event_type), DELIVER_EVERYTHING).coalesce
        is CoalesceMode.LATEST_BY_KEY
    ]
    newest = await _newest_by_key(conn, simulation_id, coalescing)

    admit: list[Candidate] = []
    for candidate in survived:
        winner = newest.get((candidate.consumer_id, candidate.event_type, candidate.entity_key))
        # Strict, and on the *pair* -- see _newest_by_key. A candidate that is
        # itself the winner compares equal and is admitted.
        if winner is not None and (candidate.created_at, candidate.id) < winner:
            drops.append(
                Drop(
                    delivery_id=candidate.id,
                    state=DeliveryState.SUPERSEDED,
                    reason=f"superseded by delivery {winner[1]}",
                )
            )
        else:
            admit.append(candidate)

    return Verdict(admit=admit, drops=drops)


async def _newest_by_key(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    candidates: list[Candidate],
) -> dict[tuple[int, str, str], tuple[datetime, int]]:
    """The latest queued delivery per ``(consumer, event_type, entity_key)``.

    One ``DISTINCT ON`` over ``ix_delivery_coalesce``, which covers exactly this
    predicate and deliberately spans both ``pending`` and ``ready``: a ready
    delivery has not been attempted yet, so it is still the thing a newer event
    should supersede.

    **The order is on ``(created_at, id)``, never ``created_at`` alone.** The
    producer spreads a tick's events across the virtual window it represents,
    so two events for one entity key can land on the identical timestamp. Under
    a non-total order each would be "newer" than the other, both would be
    superseded, and *nothing* would be delivered for that key -- silently, with
    a perfectly healthy-looking chart. ``delivery.id`` is monotonic within a
    simulation, so pairing it with the timestamp makes the order total and the
    winner unique.
    """
    if not candidates:
        return {}

    keys = {(c.consumer_id, c.event_type, c.entity_key) for c in candidates}
    key_columns = (Delivery.consumer_id, Delivery.event_type, Delivery.entity_key)

    result = await conn.execute(
        select(*key_columns, Delivery.created_at, Delivery.id)
        .where(
            Delivery.simulation_id == simulation_id,
            Delivery.state.in_((DeliveryState.PENDING.value, DeliveryState.READY.value)),
            tuple_(*key_columns).in_(sorted(keys)),
        )
        .distinct(*key_columns)
        .order_by(*key_columns, Delivery.created_at.desc(), Delivery.id.desc())
    )
    return {
        (consumer_id, event_type, entity_key): (created_at, delivery_id)
        for consumer_id, event_type, entity_key, created_at, delivery_id in result
    }


async def apply_drops(conn: AsyncConnection, drops: list[Drop], now: datetime) -> None:
    """Write the terminal rows. No ``attempt`` row -- these never reach a worker.

    ``completed_at`` is not optional. The decision feed filters on it being
    non-null (``GET /api/simulation/{id}/decisions``) and the metrics writer
    buckets terminal counters by it, so a drop written without one is invisible
    in both places at once -- the backlog falls and nothing says why.
    """
    if not drops:
        return

    by_state: dict[DeliveryState, list[dict[str, object]]] = {}
    for drop in drops:
        by_state.setdefault(drop.state, []).append(
            {"b_delivery_id": drop.delivery_id, "b_terminal_reason": drop.reason}
        )

    for state, rows in by_state.items():
        # executemany: one prepared statement per terminal state, N parameter
        # sets. The `b_` prefix keeps the WHERE parameter from colliding with a
        # column name in the SET clause, as in the worker's completion path.
        await conn.execute(
            update(Delivery)
            .where(Delivery.id == bindparam("b_delivery_id"))
            .values(
                state=state.value,
                completed_at=now,
                terminal_reason=bindparam("b_terminal_reason"),
            ),
            rows,
        )


__all__ = [
    "DELIVER_EVERYTHING",
    "Candidate",
    "Drop",
    "Policy",
    "PolicyMap",
    "Verdict",
    "apply_drops",
    "evaluate",
    "load_policies",
    "stale_by",
    "stale_reason",
]
