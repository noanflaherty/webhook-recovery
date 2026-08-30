"""Ingest: the ledger write and the fan-out, in one transaction.

Fan-out is cheap, deterministic, and contains no scheduling judgment, so it
belongs here rather than in the conductor. That placement is what gives
conductor failure its correct shape: **acceptance never depends on scheduling.**
Events keep landing in the ledger while no conductor holds the lock; they simply
sit ``pending`` until one does.

``ingest_events`` is the function; ``POST /api/simulation/{id}/event`` is a thin
wrapper over it. The demo producer calls the function directly rather than
looping back over HTTP -- same code path, no socket, and a tick's worth of
events becomes one transaction instead of a dozen.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import SimulationClockConfig, VirtualClock
from app.core.enums import DeliveryState
from app.core.models import Delivery, Event, Simulation, Subscription


@dataclass(frozen=True, slots=True)
class EventSpec:
    """One event to ledger.

    ``occurred_at`` is normally left to the caller's clock. The producer sets it
    explicitly so a tick's events are spread across the virtual window they
    represent rather than all landing on its trailing edge -- which would make
    the arrival rate look like a comb rather than a stream.
    """

    event_type: str
    entity_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime | None = None


async def ingest_events(
    session: AsyncSession,
    sim: Simulation,
    specs: list[EventSpec],
) -> list[Event]:
    """Ledger each event and fan it out to every subscribed consumer.

    One transaction, and one subscription lookup for the whole batch: the
    ``ix_subscription_fanout`` index covers ``(simulation_id, event_type)``, and
    a tick's events are drawn from a handful of types.

    An event with no subscribers is still ledgered. The ledger is the record of
    what the provider emitted, not of what anyone wanted.
    """
    if not specs:
        return []

    now = VirtualClock(SimulationClockConfig.from_row(sim)).now()

    events = [
        Event(
            simulation_id=sim.id,
            event_type=spec.event_type,
            entity_key=spec.entity_key,
            occurred_at=spec.occurred_at or now,
            payload=spec.payload,
        )
        for spec in specs
    ]
    session.add_all(events)
    # Deliveries carry event_id, which the database assigns.
    await session.flush()

    subscribers = await _subscribers_by_type(session, sim.id, {spec.event_type for spec in specs})

    for event in events:
        for consumer_id in subscribers.get(event.event_type, ()):
            session.add(
                Delivery(
                    simulation_id=sim.id,
                    event_id=event.id,
                    consumer_id=consumer_id,
                    # Denormalized off the event so the coalesce lookup and
                    # policy evaluation never join.
                    event_type=event.event_type,
                    entity_key=event.entity_key,
                    state=DeliveryState.PENDING.value,
                    attempt_count=0,
                    # `delivery.created_at` has no server default: virtual time
                    # is not the database's to know. Both timestamps start at
                    # the event's own -- a first attempt is due immediately, and
                    # only a retry pushes next_attempt_at forward.
                    created_at=event.occurred_at,
                    next_attempt_at=event.occurred_at,
                )
            )

    await session.flush()
    return events


async def _subscribers_by_type(
    session: AsyncSession,
    simulation_id: uuid.UUID,
    event_types: set[str],
) -> dict[str, list[int]]:
    rows = (
        await session.execute(
            select(Subscription.event_type, Subscription.consumer_id).where(
                Subscription.simulation_id == simulation_id,
                Subscription.event_type.in_(event_types),
            )
        )
    ).all()

    by_type: dict[str, list[int]] = {}
    for event_type, consumer_id in rows:
        by_type.setdefault(event_type, []).append(consumer_id)
    return by_type


__all__ = ["EventSpec", "ingest_events"]
