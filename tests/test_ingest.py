"""Fan-out: the one write that decides what the rest of the system will see.

Every count downstream -- backlogs, attempt shares, the whole fairness proof --
is a count of rows this function created. A fan-out that quietly delivers to the
wrong set of consumers produces a system that works perfectly and measures
something else.
"""

from __future__ import annotations

from datetime import timedelta

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import DeliveryState
from app.core.models import Consumer, Delivery, Event
from app.core.scenario import seed_simulation
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db


async def _seeded(session: AsyncSession) -> tuple[object, dict[str, int]]:
    sim = a_simulation()
    session.add(sim)
    await session.flush()
    consumers = await seed_simulation(session, sim.id)
    return sim, {c.name: c.id for c in consumers}


async def test_an_event_reaches_only_its_subscribers(session: AsyncSession) -> None:
    """Clover subscribes to `invoice.paid` alone, and must see nothing else."""
    sim, by_name = await _seeded(session)

    await ingest_events(session, sim, [EventSpec("payment_intent.succeeded", "pi_1")])

    consumer_ids = (
        (await session.execute(select(Delivery.consumer_id).where(Delivery.simulation_id == sim.id)))
        .scalars()
        .all()
    )
    assert sorted(consumer_ids) == sorted([by_name["Acme Analytics"], by_name["Bolt Billing"]])


async def test_the_low_volume_consumer_gets_its_own_type(session: AsyncSession) -> None:
    """`invoice.paid` routes to all three -- Clover included. That is its whole traffic."""
    sim, by_name = await _seeded(session)

    await ingest_events(session, sim, [EventSpec("invoice.paid", "in_1")])

    consumer_ids = (
        (await session.execute(select(Delivery.consumer_id).where(Delivery.simulation_id == sim.id)))
        .scalars()
        .all()
    )
    assert by_name["Clover CRM"] in consumer_ids
    assert len(consumer_ids) == 3


async def test_deliveries_denormalize_the_event_and_start_due_now(session: AsyncSession) -> None:
    """The fields the conductor and the worker read without ever joining.

    ``created_at`` has no server default -- virtual time is not the database's
    to know -- so an ingest that forgot to supply it would fail loudly here
    rather than quietly stamping wall time in production.
    """
    sim, _ = await _seeded(session)
    occurred = VIRTUAL_EPOCH_ZERO + timedelta(seconds=42)

    await ingest_events(
        session,
        sim,
        [EventSpec("customer.subscription.updated", "sub_007", occurred_at=occurred)],
    )

    delivery = (
        await session.execute(select(Delivery).where(Delivery.simulation_id == sim.id).limit(1))
    ).scalar_one()

    assert delivery.event_type == "customer.subscription.updated"
    assert delivery.entity_key == "sub_007"
    assert delivery.state == DeliveryState.PENDING.value
    assert delivery.created_at == occurred
    # Due immediately: only a retry pushes next_attempt_at forward.
    assert delivery.next_attempt_at == occurred
    assert delivery.ready_at is None


async def test_an_event_with_no_subscribers_is_still_ledgered(session: AsyncSession) -> None:
    """The ledger records what the provider emitted, not what anyone wanted.

    This is the property that makes replay possible at all: a consumer that
    subscribes tomorrow can be backfilled from events nobody was listening to
    today.
    """
    sim, _ = await _seeded(session)

    events = await ingest_events(session, sim, [EventSpec("charge.disputed", "dp_1")])

    assert len(events) == 1
    fanned_out = (await session.execute(select(Delivery).where(Delivery.event_id == events[0].id))).all()
    assert fanned_out == []


async def test_a_batch_is_one_fan_out_query(session: AsyncSession) -> None:
    """A tick's events share a handful of types, so the lookup is done once."""
    sim, _ = await _seeded(session)

    specs = [EventSpec("payment_intent.succeeded", f"pi_{n}") for n in range(5)]
    specs += [EventSpec("invoice.paid", f"in_{n}") for n in range(3)]
    events = await ingest_events(session, sim, specs)

    assert len(events) == 8
    deliveries = (
        (await session.execute(select(Delivery).where(Delivery.simulation_id == sim.id))).scalars().all()
    )
    # 5 payment events x 2 subscribers + 3 invoice events x 3 subscribers.
    assert len(deliveries) == 5 * 2 + 3 * 3


async def test_seeding_is_idempotent_per_simulation(session: AsyncSession) -> None:
    """Creating a simulation seeds once; a second call must not double the cast."""
    sim, _ = await _seeded(session)
    await seed_simulation(session, sim.id)

    consumers = (
        (await session.execute(select(Consumer).where(Consumer.simulation_id == sim.id))).scalars().all()
    )
    assert len(consumers) == 3


async def test_ingest_route_reports_its_fan_out(client: AsyncClient, session: AsyncSession) -> None:
    """The route is the real interface; the producer just skips the socket."""
    sim, _ = await _seeded(session)

    response = await client.post(
        f"/api/simulation/{sim.id}/event",
        json={"event_type": "invoice.paid", "entity_key": "in_99"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["delivery_count"] == 3
    assert body["entity_key"] == "in_99"

    ledgered = (await session.execute(select(Event).where(Event.simulation_id == sim.id))).scalars().all()
    assert len(ledgered) == 1
