"""The read/write plane.

The API process holds no simulation state at all -- it is a thin layer over
Postgres. Every virtual timestamp it returns is computed from the ``simulation``
row it just read, exactly as a worker would compute it.

It owns the two things that must not depend on scheduling: the **simulation
lifecycle**, and **ingest**. Keeping ingest here is what gives conductor failure
its correct shape -- acceptance never depends on a conductor holding the lock, so
a leaderless minute costs throughput and never data.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.api.schemas import (
    ConsumerRead,
    DecisionRead,
    DecisionsPage,
    EventCreate,
    EventRead,
    HealthRead,
    MetricsBucket,
    MetricsPage,
    ProcessRead,
    SimulationCreate,
    SimulationPatch,
    SimulationRead,
)
from app.core import db
from app.core.clock import (
    SimulationClockConfig,
    VirtualClock,
    pause,
    resume,
    set_speed,
    start_config,
    wall_now,
)
from app.core.enums import (
    ACTIVE_DELIVERY_STATES,
    TERMINAL_DELIVERY_STATES,
    DeliveryState,
    ProcessKind,
    SimStatus,
)
from app.core.models import Consumer, Delivery, Event, MetricsSnapshot, Process, Simulation
from app.core.scenario import phase_at, seed_simulation
from app.core.settings import get_settings

router = APIRouter(prefix="/api")

#: The decision feed is capped rather than cursored -- see DecisionsPage.
DECISIONS_DEFAULT_LIMIT = 50
DECISIONS_MAX_LIMIT = 200


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthRead)
async def health() -> HealthRead:
    """Liveness plus a real database round-trip.

    Deliberately still 200 when the database is down: the process *is* up, and a
    platform health check that restarts the api because Postgres blinked turns
    one outage into two. The ``db`` field is what to alert on.
    """
    return HealthRead(status="ok", db="ok" if await db.ping() else "error")


# ---------------------------------------------------------------------------
# Simulation lifecycle
# ---------------------------------------------------------------------------


def _to_read(sim: Simulation) -> SimulationRead:
    clock = VirtualClock(SimulationClockConfig.from_row(sim))
    now = clock.now()
    elapsed = clock.to_virtual_seconds(now)
    return SimulationRead(
        id=sim.id,
        scenario_name=sim.scenario_name,
        status=SimStatus(sim.status),
        speed_multiplier=sim.speed_multiplier,
        fair_drain_enabled=sim.fair_drain_enabled,
        global_attempts_per_s=sim.global_attempts_per_s,
        outage_override=sim.outage_override,
        virtual_now=now,
        virtual_now_s=elapsed,
        phase=phase_at(elapsed, outage_override=sim.outage_override, done=sim.status == SimStatus.DONE),
        created_at_wall=sim.created_at_wall,
    )


async def _load_simulation(session: AsyncSession, simulation_id: uuid.UUID) -> Simulation:
    sim = await session.get(Simulation, simulation_id)
    if sim is None:
        raise HTTPException(status_code=404, detail="simulation not found")
    return sim


@router.post("/simulation", response_model=SimulationRead, status_code=201)
async def create_simulation(
    body: SimulationCreate | None = None,
    session: AsyncSession = Depends(db.get_session),
) -> SimulationRead:
    """Create a simulation and seed its cast, in one transaction.

    Seeding belongs here because fan-out reads ``subscription``: a simulation
    with no consumers accepts events and delivers them to nobody.

    Creating a simulation *is* Reset. Everything is namespaced by
    ``simulation_id``, so runs persist side by side and one can be left running
    while another starts.
    """
    settings = get_settings()
    body = body or SimulationCreate()
    config = start_config(body.speed_multiplier or settings.default_speed_multiplier)

    sim = Simulation(
        id=uuid.uuid4(),
        created_at_wall=wall_now(),
        scenario_name=body.scenario_name or settings.default_scenario_name,
        status=config.status.value,
        virtual_epoch=config.virtual_epoch,
        resumed_at_wall=config.resumed_at_wall,
        paused_at_virtual=config.paused_at_virtual,
        speed_multiplier=config.speed_multiplier,
        fair_drain_enabled=True if body.fair_drain_enabled is None else body.fair_drain_enabled,
        global_attempts_per_s=body.global_attempts_per_s or 30.0,
        outage_override=None,
    )
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    return _to_read(sim)


@router.get("/simulation/{simulation_id}", response_model=SimulationRead)
async def read_simulation(
    simulation_id: uuid.UUID,
    session: AsyncSession = Depends(db.get_session),
) -> SimulationRead:
    return _to_read(await _load_simulation(session, simulation_id))


@router.patch("/simulation/{simulation_id}", response_model=SimulationRead)
async def patch_simulation(
    simulation_id: uuid.UUID,
    body: SimulationPatch,
    session: AsyncSession = Depends(db.get_session),
) -> SimulationRead:
    """Pause / resume / change speed / flip fair drain.

    Pause, resume and speed change are all epoch rewrites -- one shared piece of
    arithmetic in ``app.core.clock``, applied here and nowhere else. The order
    matters: the speed change is applied to the *post-status* config, so
    pausing and speeding up in one request behaves like doing them in sequence.
    """
    sim = await _load_simulation(session, simulation_id)
    config = SimulationClockConfig.from_row(sim)
    at = wall_now()

    if body.status is not None and body.status is not SimStatus(sim.status):
        if body.status is SimStatus.PAUSED:
            config = pause(config, at_wall=at)
        elif body.status is SimStatus.RUNNING:
            config = resume(config, at_wall=at)
        else:  # DONE -- freeze the clock so the final numbers stop moving.
            config = pause(config, at_wall=at)

    if body.speed_multiplier is not None and body.speed_multiplier != config.speed_multiplier:
        config = set_speed(config, body.speed_multiplier, at_wall=at)

    sim.status = (body.status or config.status).value
    sim.virtual_epoch = config.virtual_epoch
    sim.resumed_at_wall = config.resumed_at_wall
    sim.paused_at_virtual = config.paused_at_virtual
    sim.speed_multiplier = config.speed_multiplier

    if body.fair_drain_enabled is not None:
        sim.fair_drain_enabled = body.fair_drain_enabled
    if body.global_attempts_per_s is not None:
        sim.global_attempts_per_s = body.global_attempts_per_s
    if body.outage_override is not None:
        sim.outage_override = body.outage_override

    await session.flush()
    return _to_read(sim)


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


@router.post("/simulation/{simulation_id}/event", response_model=EventRead, status_code=201)
async def create_event(
    simulation_id: uuid.UUID,
    body: EventCreate,
    session: AsyncSession = Depends(db.get_session),
) -> EventRead:
    """Ledger one event and fan it out to every subscribed consumer.

    In production the callers here are the provider's own internal services. In
    the demo a background task in this same process plays them -- through
    ``ingest_events`` directly rather than back through this route, which is the
    same code path without a loopback socket. This route is the real interface,
    and that is the part of the claim that matters.
    """
    sim = await _load_simulation(session, simulation_id)
    events = await ingest_events(
        session,
        sim,
        [EventSpec(event_type=body.event_type, entity_key=body.entity_key, payload=body.payload)],
    )
    event = events[0]

    fanned_out = await session.scalar(
        select(func.count()).select_from(Delivery).where(Delivery.event_id == event.id)
    )
    return EventRead(
        id=event.id,
        simulation_id=simulation_id,
        event_type=event.event_type,
        entity_key=event.entity_key,
        occurred_at=event.occurred_at,
        delivery_count=fanned_out or 0,
    )


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


@router.get("/simulation/{simulation_id}/consumer", response_model=list[ConsumerRead])
async def list_consumers(
    simulation_id: uuid.UUID,
    session: AsyncSession = Depends(db.get_session),
) -> list[ConsumerRead]:
    """Consumers with their live counters, seeded at simulation creation."""
    await _load_simulation(session, simulation_id)

    consumers = (
        (
            await session.execute(
                select(Consumer).where(Consumer.simulation_id == simulation_id).order_by(Consumer.id)
            )
        )
        .scalars()
        .all()
    )
    if not consumers:
        return []

    counts = (
        await session.execute(
            select(Delivery.consumer_id, Delivery.state, func.count())
            .where(Delivery.simulation_id == simulation_id)
            .group_by(Delivery.consumer_id, Delivery.state)
        )
    ).all()

    by_consumer: dict[int, dict[str, int]] = {}
    for consumer_id, state, count in counts:
        by_consumer.setdefault(consumer_id, {})[state] = count

    def backlog_of(states: dict[str, int]) -> int:
        return sum(states.get(state, 0) for state in ACTIVE_DELIVERY_STATES)

    return [
        ConsumerRead(
            id=c.id,
            name=c.name,
            weight=c.weight,
            concurrency_cap=c.concurrency_cap,
            max_attempts_per_s=c.max_attempts_per_s,
            backlog=backlog_of(by_consumer.get(c.id, {})),
            in_flight=by_consumer.get(c.id, {}).get(DeliveryState.IN_FLIGHT, 0),
            delivered=by_consumer.get(c.id, {}).get(DeliveryState.DELIVERED, 0),
            expired=by_consumer.get(c.id, {}).get(DeliveryState.EXPIRED, 0),
            superseded=by_consumer.get(c.id, {}).get(DeliveryState.SUPERSEDED, 0),
            failed=by_consumer.get(c.id, {}).get(DeliveryState.FAILED, 0),
            caught_up_after_s=None,
        )
        for c in consumers
    ]


# ---------------------------------------------------------------------------
# Metrics -- a real cursor, because bucket_virtual_s really is monotonic
# ---------------------------------------------------------------------------


@router.get("/simulation/{simulation_id}/metrics", response_model=MetricsPage)
async def read_metrics(
    simulation_id: uuid.UUID,
    since_bucket: int = Query(default=-1, description="Return buckets strictly greater than this."),
    session: AsyncSession = Depends(db.get_session),
) -> MetricsPage:
    await _load_simulation(session, simulation_id)

    rows = (
        await session.execute(
            select(MetricsSnapshot, Consumer.name)
            .join(Consumer, Consumer.id == MetricsSnapshot.consumer_id)
            .where(
                MetricsSnapshot.simulation_id == simulation_id,
                MetricsSnapshot.bucket_virtual_s > since_bucket,
            )
            .order_by(MetricsSnapshot.bucket_virtual_s, MetricsSnapshot.consumer_id)
        )
    ).all()

    buckets = [
        MetricsBucket(
            consumer_id=snapshot.consumer_id,
            consumer_name=name,
            bucket_virtual_s=snapshot.bucket_virtual_s,
            backlog=snapshot.backlog,
            ready=snapshot.ready,
            in_flight=snapshot.in_flight,
            attempts=snapshot.attempts,
            delivered=snapshot.delivered,
            expired=snapshot.expired,
            superseded=snapshot.superseded,
            failed=snapshot.failed,
        )
        for snapshot, name in rows
    ]
    return MetricsPage(
        simulation_id=simulation_id,
        buckets=buckets,
        # Unchanged when the page is empty, so a client that polls ahead of the
        # conductor does not lose its place.
        next_since_bucket=buckets[-1].bucket_virtual_s if buckets else since_bucket,
    )


# ---------------------------------------------------------------------------
# Decision feed -- newest-first, replace-on-poll
# ---------------------------------------------------------------------------


@router.get("/simulation/{simulation_id}/decisions", response_model=DecisionsPage)
async def read_decisions(
    simulation_id: uuid.UUID,
    limit: int = Query(default=DECISIONS_DEFAULT_LIMIT, ge=1, le=DECISIONS_MAX_LIMIT),
    session: AsyncSession = Depends(db.get_session),
) -> DecisionsPage:
    """The most recent terminal decisions, newest first.

    Deliberately not cursored: ``delivery.id`` is assigned at ingest, not at
    completion, so a ``since_id`` cursor over it would silently skip decisions.
    """
    await _load_simulation(session, simulation_id)

    rows = (
        await session.execute(
            select(Delivery, Consumer.name, Event.occurred_at)
            .join(Consumer, Consumer.id == Delivery.consumer_id)
            .join(Event, Event.id == Delivery.event_id)
            .where(
                Delivery.simulation_id == simulation_id,
                Delivery.state.in_([s.value for s in TERMINAL_DELIVERY_STATES]),
                Delivery.completed_at.is_not(None),
            )
            .order_by(Delivery.completed_at.desc(), Delivery.id.desc())
            .limit(limit)
        )
    ).all()

    return DecisionsPage(
        simulation_id=simulation_id,
        decisions=[
            DecisionRead(
                delivery_id=delivery.id,
                consumer_id=delivery.consumer_id,
                consumer_name=name,
                event_type=delivery.event_type,
                entity_key=delivery.entity_key,
                state=DeliveryState(delivery.state),
                terminal_reason=delivery.terminal_reason,
                attempt_count=delivery.attempt_count,
                occurred_at=occurred_at,
                completed_at=delivery.completed_at,
            )
            for delivery, name, occurred_at in rows
        ],
    )


# ---------------------------------------------------------------------------
# Processes -- liveness as a read-time filter
# ---------------------------------------------------------------------------


@router.get("/process", response_model=list[ProcessRead])
async def list_processes(session: AsyncSession = Depends(db.get_session)) -> list[ProcessRead]:
    """Live workers and conductors.

    Liveness is this ``WHERE`` clause and nothing else. There is no reaper:
    stale rows from prior deploys accumulate harmlessly and are never read,
    which is what keeps "nothing in the delivery path consults it" literally
    true rather than approximately true.
    """
    settings = get_settings()
    now = wall_now()
    cutoff = now - timedelta(seconds=settings.process_liveness_window_s)

    rows = (
        (
            await session.execute(
                select(Process)
                .where(Process.last_heartbeat_wall > cutoff)
                .order_by(Process.kind, Process.started_at_wall)
            )
        )
        .scalars()
        .all()
    )

    lease_counts = (
        await session.execute(
            select(Delivery.leased_by, func.count())
            .where(
                Delivery.state == DeliveryState.IN_FLIGHT.value,
                Delivery.leased_by.is_not(None),
            )
            .group_by(Delivery.leased_by)
        )
    ).all()
    in_flight_by_worker: dict[uuid.UUID, int] = {
        worker_id: count for worker_id, count in lease_counts if worker_id is not None
    }

    return [
        ProcessRead(
            id=p.id,
            kind=ProcessKind(p.kind),
            hostname=p.hostname,
            pid=p.pid,
            started_at_wall=p.started_at_wall,
            last_heartbeat_wall=p.last_heartbeat_wall,
            is_leader=p.is_leader,
            heartbeat_age_s=(now - p.last_heartbeat_wall).total_seconds(),
            in_flight=in_flight_by_worker.get(p.id, 0),
        )
        for p in rows
    ]
