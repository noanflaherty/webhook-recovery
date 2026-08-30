"""The canned scenario: phase boundaries, the cast, and the event mix.

Two halves, and the split matters.

**Phase boundaries** are derived from virtual time rather than stored, for the
same reason the clock is -- a stored phase is another thing three processes have
to agree about. ``is_outage`` is the conductor's admission gate and
``phase_at`` backs ``SimulationRead.phase``.

**The cast** -- three consumers, their subscriptions, Bolt's policies, and the
producer's event mix -- is a constants table with one function that writes it.
The phase plan put seeding in Phase 3, but fan-out reads ``subscription``: with
no rows the walking skeleton produces nothing and cannot walk. Policies come
along for free and sit unread until Phase 2 evaluates them.

Rates are per *virtual* second and are back-derived from the committed frontend
fixtures, so the charts a reviewer sees against real data have the same shape as
the ones the frontend was built against.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CoalesceMode
from app.core.models import Consumer, DeliveryPolicy, Subscription

#: ~15 virtual minutes, which is ~45 real seconds at 20x.
OUTAGE_STARTS_AT_S: Final = 120.0  # 2:00
OUTAGE_ENDS_AT_S: Final = 420.0  # 7:00

PHASE_NORMAL: Final = "normal"
PHASE_OUTAGE: Final = "outage"
PHASE_RECOVERY: Final = "recovery"
PHASE_DONE: Final = "done"


def phase_at(virtual_s: float, *, outage_override: bool | None = None, done: bool = False) -> str:
    """Which act of the scenario a given virtual second falls in.

    ``outage_override`` is the reviewer's manual switch: ``True`` forces the
    outage on, ``False`` forces it off, ``None`` follows the script.
    """
    if done:
        return PHASE_DONE
    if outage_override is True:
        return PHASE_OUTAGE
    if outage_override is False:
        return PHASE_NORMAL if virtual_s < OUTAGE_STARTS_AT_S else PHASE_RECOVERY
    if virtual_s < OUTAGE_STARTS_AT_S:
        return PHASE_NORMAL
    if virtual_s < OUTAGE_ENDS_AT_S:
        return PHASE_OUTAGE
    return PHASE_RECOVERY


#: The end of the scripted run: ~45 real seconds at 20x. The producer stops
#: emitting here, which is what lets the backlog reach a *stable* zero -- and
#: only then does "the run is finished" mean anything. It is set comfortably
#: after the recovery drain completes (~830 virtual s in practice), so a
#: reviewer watches the backlog reach zero while traffic is still arriving,
#: which is the convincing version of catching up.
SCENARIO_ENDS_AT_S: Final = 900.0

#: A backstop, not the normal way a run ends. It only ever catches a run that
#: stopped draining -- one whose consumer was left `down`, say.
SCENARIO_MAX_VIRTUAL_S: Final = 1800.0


def is_producing(virtual_s: float) -> bool:
    """Whether the provider is still emitting events.

    The producer runs straight through the outage -- that is the whole point,
    the provider keeps emitting and only *delivery* is down -- and stops at the
    end of the script.
    """
    return virtual_s < SCENARIO_ENDS_AT_S


def is_finished(virtual_s: float, backlog: int) -> bool:
    """Whether a run is over: the script has ended and nothing is left to deliver.

    This exists because **the conductor works on every running simulation in a
    single pass**, so a simulation nobody retires keeps costing throughput
    forever -- and the cost is paid by whichever run a reviewer is actually
    watching. Every visit to the deployment leaves one behind, so it compounds.

    Invisible locally, where there is one simulation. On the deployment it
    presented as a backlog that tracked its arrival rate and never drained,
    which reads exactly like a broken scheduler.

    Both terms are load-bearing. Retiring on an empty backlog alone races the
    producer -- at 20x it commits another ~20 deliveries in the time a pass
    takes, so the run freezes with a small residue that looks like a failure to
    drain. Waiting for the script to end first means the zero is stable.
    """
    if virtual_s >= SCENARIO_MAX_VIRTUAL_S:
        return True
    return not is_producing(virtual_s) and backlog == 0


def is_outage(virtual_s: float, *, outage_override: bool | None = None) -> bool:
    """Whether the delivery pipeline is down.

    The conductor skips admission entirely while this is true, which is what
    makes backlogs climb through the outage and drain after it.
    """
    if outage_override is not None:
        return outage_override
    return OUTAGE_STARTS_AT_S <= virtual_s < OUTAGE_ENDS_AT_S


# ---------------------------------------------------------------------------
# The producer's event mix (TECHNICAL_DESIGN.md §Producer event mix)
# ---------------------------------------------------------------------------
#
# Each of the three high-volume types exists to isolate exactly one behaviour,
# so every drop in the demo is attributable to a single cause. Deliberately no
# type carries two policies: it would be realistic, and a reviewer could no
# longer tell which mechanism dropped a given event.


@dataclass(frozen=True, slots=True)
class EventTypeSpec:
    """One row of the event mix."""

    event_type: str
    #: Events per virtual second, across the whole simulation.
    rate_per_virtual_s: float
    key_prefix: str
    #: How many distinct entity keys exist. ``None`` means a fresh key every
    #: time -- which is what makes an event un-coalescable by construction.
    key_pool: int | None


EVENT_MIX: Final[tuple[EventTypeSpec, ...]] = (
    # Money moved. Unique key, so `latest_by_key` could never collapse it even
    # if someone configured it -- the guarantee is structural, not just policy.
    EventTypeSpec("payment_intent.succeeded", 1.75, "pi", None),
    # Coalesce candidate: a small pool of subscriptions churning, where only the
    # latest state of each is worth delivering.
    EventTypeSpec("customer.subscription.updated", 1.75, "sub", 40),
    # Staleness candidate: a ten-minute-old balance is worthless.
    EventTypeSpec("balance.available", 1.75, "acct", 25),
    # Low volume, and routed to Clover alone.
    EventTypeSpec("invoice.paid", 0.80, "in", None),
)


# ---------------------------------------------------------------------------
# The cast (TECHNICAL_DESIGN.md §Simulated Consumers)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PolicySpec:
    event_type: str
    max_staleness_s: float | None = None
    coalesce: CoalesceMode = CoalesceMode.NONE


@dataclass(frozen=True, slots=True)
class ConsumerSpec:
    name: str
    subscribes_to: tuple[str, ...]
    policies: tuple[PolicySpec, ...] = field(default_factory=tuple)

    # The three delivery knobs. Equal weights to start, so the attempts-share
    # chart proves fairness by converging on equal thirds rather than on
    # whatever ratio the weights were set to.
    weight: float = 1.0
    concurrency_cap: int = 8
    max_attempts_per_s: float = 20.0

    # SimulatedTransport profile. Phase 1 seeds a perfectly healthy consumer --
    # zero failure rate -- so the skeleton walks with "always 200" behaviour
    # even though the worker's state machine handles every outcome.
    sim_latency_s: float = 0.2
    sim_jitter_s: float = 0.05
    sim_failure_rate: float = 0.0
    sim_down: bool = False


ALL_EVENT_TYPES: Final[tuple[str, ...]] = tuple(spec.event_type for spec in EVENT_MIX)

CONSUMERS: Final[tuple[ConsumerSpec, ...]] = (
    # Baseline. No policies, so the whole backlog has to be delivered -- this is
    # what a naive consumer suffers on recovery, and the thing Bolt is measured
    # against.
    ConsumerSpec(name="Acme Analytics", subscribes_to=ALL_EVENT_TYPES),
    # Hero. Policies shrink the backlog before it is ever sent, while every
    # payment still lands. The third policy row is deliberately a no-op: an
    # absent row already means "deliver everything", so writing it is how the
    # data says *chose not to*, rather than *forgot to*.
    ConsumerSpec(
        name="Bolt Billing",
        subscribes_to=ALL_EVENT_TYPES,
        policies=(
            PolicySpec("customer.subscription.updated", coalesce=CoalesceMode.LATEST_BY_KEY),
            PolicySpec("balance.available", max_staleness_s=120.0),
            PolicySpec("payment_intent.succeeded"),
        ),
    ),
    # Fairness case. Tiny backlog; with fair drain on it should catch up in
    # seconds, and with it off it should sit starved behind the other two.
    ConsumerSpec(name="Clover CRM", subscribes_to=("invoice.paid",)),
)


async def seed_simulation(session: AsyncSession, simulation_id: uuid.UUID) -> list[Consumer]:
    """Write the cast for a fresh simulation. Idempotent per simulation.

    Called from ``POST /api/simulation``, in the same transaction that creates
    the row -- a simulation that exists but has no consumers would accept events
    and fan them out to nobody.
    """
    existing = (
        (await session.execute(select(Consumer).where(Consumer.simulation_id == simulation_id)))
        .scalars()
        .all()
    )
    if existing:
        return list(existing)

    consumers: list[Consumer] = []
    for spec in CONSUMERS:
        consumer = Consumer(
            simulation_id=simulation_id,
            name=spec.name,
            weight=spec.weight,
            concurrency_cap=spec.concurrency_cap,
            max_attempts_per_s=spec.max_attempts_per_s,
            sim_latency_s=spec.sim_latency_s,
            sim_jitter_s=spec.sim_jitter_s,
            sim_failure_rate=spec.sim_failure_rate,
            sim_down=spec.sim_down,
        )
        session.add(consumer)
        consumers.append(consumer)

    # The consumer ids that subscriptions and policies point at are assigned by
    # the database, so the flush is load-bearing rather than incidental.
    await session.flush()

    for spec, consumer in zip(CONSUMERS, consumers, strict=True):
        for event_type in spec.subscribes_to:
            session.add(
                Subscription(
                    simulation_id=simulation_id,
                    consumer_id=consumer.id,
                    event_type=event_type,
                )
            )
        for policy in spec.policies:
            session.add(
                DeliveryPolicy(
                    simulation_id=simulation_id,
                    consumer_id=consumer.id,
                    event_type=policy.event_type,
                    max_staleness_s=policy.max_staleness_s,
                    coalesce=policy.coalesce.value,
                )
            )

    await session.flush()
    return consumers


def entity_key(spec: EventTypeSpec, draw: int) -> str:
    """The entity key for one event.

    ``draw`` is an index into the type's key pool, or an arbitrary number for a
    type that has none. Formatting is display-only -- what matters is that a
    pooled type repeats its keys often enough for coalescing to have something
    to collapse.
    """
    if spec.key_pool is None:
        return f"{spec.key_prefix}_{draw:08x}"
    return f"{spec.key_prefix}_{draw % spec.key_pool:03d}"


def payload_for(spec: EventTypeSpec, key: str, occurred_at: datetime) -> dict[str, object]:
    """A small, plausible body. Stored per event, so it stays small."""
    return {"id": key, "type": spec.event_type, "occurred_at": occurred_at.isoformat()}


__all__ = [
    "ALL_EVENT_TYPES",
    "CONSUMERS",
    "EVENT_MIX",
    "OUTAGE_ENDS_AT_S",
    "OUTAGE_STARTS_AT_S",
    "PHASE_DONE",
    "PHASE_NORMAL",
    "PHASE_OUTAGE",
    "PHASE_RECOVERY",
    "SCENARIO_ENDS_AT_S",
    "SCENARIO_MAX_VIRTUAL_S",
    "ConsumerSpec",
    "EventTypeSpec",
    "PolicySpec",
    "entity_key",
    "is_finished",
    "is_outage",
    "is_producing",
    "payload_for",
    "phase_at",
    "seed_simulation",
]
