"""Lease reclamation: returning the work a dead worker was holding.

A worker stamps ``leased_by`` and ``lease_expires_at`` when it claims a delivery
(:func:`app.worker.claim.claim_batch`). If it dies before completing, the row
stays ``in_flight`` forever: it counts against its consumer's
``concurrency_cap``, so the capacity is gone rather than merely idle, and it
counts in ``total_backlog``, so the run can never satisfy ``is_finished`` and is
swept by every conductor pass for the rest of the deployment's life.

The reaper is the answer, and its shape is the point: it asks **"has this lease
expired?", never "is that worker alive?"** Timeouts replace liveness detection,
which is why the correctness path never reads the ``process`` table and why a
dead worker needs no failure detector to be tolerated.

Two properties come from living inside the conductor pass rather than in a
process of its own:

* **One reaper, by construction.** The pass runs under the advisory lock, on the
  connection that lock is held on, so there is no second sweeper to race.
* **A lease cannot expire during a pause.** Leases are stamped in *virtual*
  time, virtual time is frozen while a simulation is paused, and a pass only
  covers simulations whose status is ``running``.

**The ``attempt`` row already exists.** It is inserted at claim time, because
the fairness window counts attempts *started* -- so reclamation closes the open
row rather than inserting a second one. A second row would charge the consumer
twice inside the very window its share is computed from, which is the same trap
:func:`app.worker.claim._expire` is written to avoid.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import bindparam, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.enums import AttemptOutcome, DeliveryState
from app.core.models import Attempt, Delivery
from app.core.settings import get_settings
from app.worker.claim import backoff_s


@dataclass(frozen=True, slots=True)
class Reclaimed:
    """What one sweep recovered.

    The split is kept rather than just the total because the two mean opposite
    things operationally: ``requeued`` is the system absorbing a worker death,
    ``exhausted`` is work that has now died with several of them.
    """

    requeued: int
    exhausted: int

    @property
    def total(self) -> int:
        return self.requeued + self.exhausted


NOTHING = Reclaimed(requeued=0, exhausted=0)


async def reclaim_expired_leases(
    conn: AsyncConnection,
    simulation_id: uuid.UUID,
    now: datetime,
) -> Reclaimed:
    """Return every delivery whose lease has run out to the queue.

    ``SKIP LOCKED`` matters more here than it looks. A worker that is merely
    *slow* rather than dead holds a row lock through its completion
    transaction; without the skip, a 50ms conductor pass would block on it, and
    the reaper -- the thing that exists so a stuck worker costs nothing -- would
    be the thing a stuck worker stalls.
    """
    settings = get_settings()

    rows = (
        await conn.execute(
            select(Delivery.id, Delivery.attempt_count)
            .where(
                Delivery.simulation_id == simulation_id,
                Delivery.state == DeliveryState.IN_FLIGHT.value,
                Delivery.lease_expires_at < now,
            )
            .with_for_update(skip_locked=True)
        )
    ).all()
    if not rows:
        return NOTHING

    # An expired lease is a failed attempt, so it is charged against the retry
    # budget like any other. Without this a delivery whose worker keeps dying --
    # the same delivery, claimed by whichever worker is next to fall over --
    # cycles forever at the head of the queue.
    requeue: list[dict[str, object]] = []
    exhaust: list[dict[str, object]] = []
    for row in rows:
        if row.attempt_count >= settings.max_attempts:
            exhaust.append(
                {
                    "b_delivery_id": row.id,
                    "b_terminal_reason": (
                        f"lease expired on attempt {row.attempt_count} of {settings.max_attempts}"
                    ),
                }
            )
        else:
            requeue.append(
                {
                    "b_delivery_id": row.id,
                    "b_next_attempt_at": now + timedelta(seconds=backoff_s(row.attempt_count)),
                }
            )

    # Clearing the lease is what makes the fence in `complete_batch` decisive: a
    # worker that was slow rather than dead will still try to complete these,
    # and `leased_by = :me` no longer matches.
    if requeue:
        await conn.execute(
            update(Delivery)
            .where(Delivery.id == bindparam("b_delivery_id"))
            .values(
                state=DeliveryState.PENDING.value,
                next_attempt_at=bindparam("b_next_attempt_at"),
                leased_by=None,
                lease_expires_at=None,
            ),
            requeue,
        )
    if exhaust:
        await conn.execute(
            update(Delivery)
            .where(Delivery.id == bindparam("b_delivery_id"))
            .values(
                state=DeliveryState.FAILED.value,
                completed_at=now,
                terminal_reason=bindparam("b_terminal_reason"),
                leased_by=None,
                lease_expires_at=None,
            ),
            exhaust,
        )

    # `finished_at IS NULL` picks out exactly the attempt the dead worker opened
    # and never closed. It is also what stops a late completion from overwriting
    # this outcome with its own.
    await conn.execute(
        update(Attempt)
        .where(
            Attempt.delivery_id.in_([row.id for row in rows]),
            Attempt.finished_at.is_(None),
        )
        .values(finished_at=now, outcome=AttemptOutcome.LEASE_EXPIRED.value)
    )

    return Reclaimed(requeued=len(requeue), exhausted=len(exhaust))


__all__ = ["NOTHING", "Reclaimed", "reclaim_expired_leases"]
