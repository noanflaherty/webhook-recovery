"""Generate the committed JSON fixtures the frontend builds against.

Two things matter here, and they are separate:

1. **Generated from the Pydantic models.** Every object is constructed as a
   real response model and dumped through it, so a fixture cannot drift from
   the frozen contract -- rename a field in ``app.api.schemas`` and this script
   fails rather than silently producing stale JSON.

2. **Outage-shaped, not placeholder-shaped.** Three consumers, an outage at
   2:00, recovery at 7:00, backlogs climbing and draining. A chart tuned
   against flat placeholder data looks wrong the moment real data arrives --
   which is a Phase 3 problem manufactured in Phase 0.

The numbers are synthesized, not simulated: this is a drawing of the curve the
real system should produce, precise enough to build axes, legends and colour
scales against. It deliberately does not import the conductor, because the
conductor does not exist yet -- that is the entire point of shipping it now.

    uv run python scripts/gen_fixtures.py
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from app.api.schemas import (
    ConsumerRead,
    DecisionRead,
    DecisionsPage,
    MetricsBucket,
    MetricsPage,
    ProcessRead,
    SimulationRead,
)
from app.core.clock import VIRTUAL_EPOCH_ZERO
from app.core.enums import DeliveryState, ProcessKind, SimStatus
from app.core.scenario import (
    OUTAGE_ENDS_AT_S,
    OUTAGE_STARTS_AT_S,
    phase_at,
)

OUT_DIR = Path("frontend/src/fixtures")
OPENAPI_PATH = Path("openapi.json")

#: Fixed, so regenerating produces a byte-identical diff unless the shape
#: genuinely changed.
SEED = 20260830

#: One bucket per virtual second, through recovery and a little past the drain.
RUN_LENGTH_S = 640
BUCKET_S = 1

SIMULATION_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")

#: A fixed wall reference, not the real clock. Everything in these fixtures has
#: to be reproducible or `make fixtures-check` can never pass and the committed
#: JSON churns on every run -- the *_wall timestamps are display-only here, so
#: pinning them costs nothing and buys a meaningful staleness check.
FIXTURE_WALL = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


#: Set *below* the sum of per-consumer rate caps (3 x 20), so the provider is
#: genuinely the contended resource during recovery. If the fair-drain toggle
#: ever looks like a no-op, check this ratio first.
GLOBAL_ATTEMPTS_PER_S = 30.0

#: Virtual seconds an attempt occupies a concurrency slot. Turns an attempt rate
#: into an in-flight count via Little's law.
SIM_LATENCY_S = 0.2


@dataclass(frozen=True)
class ConsumerProfile:
    """One row of TECHNICAL_DESIGN.md §Simulated Consumers."""

    id: int
    name: str
    #: Events per virtual second arriving for this consumer.
    arrival_rate: float
    #: Share of *candidates* policy discards before any attempt, during recovery.
    expired_share: float
    superseded_share: float

    weight: float = 1.0
    concurrency_cap: int = 8
    max_attempts_per_s: float = 20.0


PROFILES = [
    # Baseline: no policies, so the whole backlog has to be delivered.
    ConsumerProfile(1, "Acme Analytics", arrival_rate=6.0, expired_share=0.0, superseded_share=0.0),
    # Hero: policies shrink the backlog before it is ever sent, while every
    # payment still lands.
    ConsumerProfile(2, "Bolt Billing", arrival_rate=6.0, expired_share=0.34, superseded_share=0.22),
    # Fairness case: tiny backlog, catches up in seconds.
    ConsumerProfile(3, "Clover CRM", arrival_rate=0.8, expired_share=0.0, superseded_share=0.0),
]

EVENT_TYPES = [
    "payment_intent.succeeded",
    "customer.subscription.updated",
    "balance.available",
    "invoice.paid",
]


# ---------------------------------------------------------------------------
# The curve
# ---------------------------------------------------------------------------


@dataclass
class ConsumerRun:
    """Per-consumer running totals as the scenario plays out."""

    profile: ConsumerProfile
    backlog: float = 0.0
    delivered: int = 0
    expired: int = 0
    superseded: int = 0
    failed: int = 0
    caught_up_at_s: float | None = None


def _jitter(rng: random.Random, value: float, spread: float = 0.12) -> float:
    """Multiplicative noise, so the lines look measured rather than plotted."""
    return value * (1.0 + rng.uniform(-spread, spread))


def fair_allocate(demand: dict[int, float], budget: float) -> dict[int, float]:
    """Split a contended budget by weight, work-conserving.

    This is the shape of the thing Phase 2 has to get right, and the reason the
    fixture bothers to model it rather than assign each consumer a fixed drain
    rate: the **attempts-share chart is the fairness proof**. With equal weights
    and all three backlogged the segments must be equal thirds; once Clover
    drains, its segment correctly goes to zero and the other two absorb its
    share. A chart built against fixed drain rates would never show that
    handover, and the legend explaining it would never get written.

    Unused share is redistributed to whoever still has work (§Fairness,
    "work-conserving"), which is what the repeat-until-stable loop does.
    """
    profiles = {p.id: p for p in PROFILES}
    alloc = dict.fromkeys(demand, 0.0)
    active = {cid for cid, d in demand.items() if d > 0}

    while active:
        remaining = budget - sum(alloc.values())
        if remaining <= 1e-6:
            break
        total_weight = sum(profiles[cid].weight for cid in active)
        satisfied: set[int] = set()
        for cid in sorted(active):
            share = remaining * profiles[cid].weight / total_weight
            # A consumer is dispatchable only under *both* its own caps.
            headroom = min(demand[cid], profiles[cid].max_attempts_per_s * BUCKET_S) - alloc[cid]
            grant = min(share, headroom)
            alloc[cid] += max(grant, 0.0)
            if grant >= headroom - 1e-6:
                satisfied.add(cid)
        if not satisfied:
            break
        active -= satisfied

    return alloc


def build_metrics(rng: random.Random) -> tuple[list[MetricsBucket], dict[int, ConsumerRun]]:
    runs = {p.id: ConsumerRun(p) for p in PROFILES}
    buckets: list[MetricsBucket] = []

    for second in range(0, RUN_LENGTH_S, BUCKET_S):
        outage = OUTAGE_STARTS_AT_S <= second < OUTAGE_ENDS_AT_S
        recovering = second >= OUTAGE_ENDS_AT_S

        for run in runs.values():
            run.backlog += _jitter(rng, run.profile.arrival_rate * BUCKET_S)

        if outage:
            # Delivery is down: events still land in the ledger, nothing is
            # marked ready. Backlogs climb -- Acme and Bolt fast, Clover slowly.
            allocation = dict.fromkeys(runs, 0.0)
        else:
            allocation = fair_allocate(
                {cid: run.backlog for cid, run in runs.items()},
                _jitter(rng, GLOBAL_ATTEMPTS_PER_S * BUCKET_S, spread=0.05),
            )

        for cid, run in runs.items():
            p = run.profile
            attempts = int(allocation[cid])
            expired = superseded = 0

            drop_share = p.expired_share + p.superseded_share
            if recovering and drop_share > 0 and run.backlog > 1:
                # A dropped candidate consumed a *candidate slot* but not an
                # *attempt*, and fairness is measured in attempts -- so filling
                # a share of `attempts` costs roughly attempts/(1-drop_share)
                # candidates. This is the 2a/2b integration subtlety from
                # PHASED_PLAN.md, and modelling it is what makes Bolt's backlog
                # visibly collapse faster than its attempt count alone explains.
                candidates = attempts / max(1.0 - drop_share, 0.01)
                dropped = min(run.backlog - attempts, candidates - attempts)
                dropped = max(dropped, 0.0)
                expired = int(dropped * p.expired_share / drop_share)
                superseded = int(dropped) - expired
                run.backlog -= expired + superseded

            attempts = int(min(attempts, run.backlog))
            # A small trickle of 5xx. Retried, not failed -- a genuine failure
            # needs the retry cap, which takes several buckets to reach.
            failed = 1 if attempts > 0 and rng.random() < 0.015 else 0
            delivered = max(attempts - failed, 0)
            run.backlog = max(run.backlog - delivered - failed, 0.0)

            run.delivered += delivered
            run.expired += expired
            run.superseded += superseded
            run.failed += failed

            if run.caught_up_at_s is None and recovering and run.backlog < max(2.0, p.arrival_rate):
                run.caught_up_at_s = second - OUTAGE_ENDS_AT_S

            # Little's law: an attempt rate times its latency is the number of
            # slots it occupies. Capped, because the cap is the whole point.
            in_flight = min(p.concurrency_cap, round(attempts * SIM_LATENCY_S))
            # The ready buffer is admission control materialized as a row state,
            # and is deliberately kept shallow (~1-2x the concurrency cap).
            ready = 0 if outage else min(int(run.backlog), p.concurrency_cap)

            buckets.append(
                MetricsBucket(
                    consumer_id=cid,
                    consumer_name=p.name,
                    bucket_virtual_s=second,
                    backlog=int(run.backlog),
                    ready=ready,
                    in_flight=in_flight,
                    attempts=attempts,
                    delivered=delivered,
                    expired=expired,
                    superseded=superseded,
                    failed=failed,
                )
            )

    return buckets, runs


def build_decisions(rng: random.Random, runs: dict[int, ConsumerRun]) -> list[DecisionRead]:
    """The most recent ~50 terminal decisions, newest first.

    Weighted so Bolt's expired/superseded decisions are visible -- the feed
    exists to make policy behaviour legible, and a feed of nothing but
    "delivered" would not do that.
    """
    outcomes: list[tuple[int, DeliveryState, str | None]] = []
    for run in runs.values():
        cid = run.profile.id
        outcomes += [(cid, DeliveryState.DELIVERED, None)] * 6
        if run.expired:
            outcomes += [(cid, DeliveryState.EXPIRED, "stale by {n}s past a 120s bound")] * 5
        if run.superseded:
            outcomes += [(cid, DeliveryState.SUPERSEDED, "newer delivery for the same entity key")] * 5
        outcomes += [(cid, DeliveryState.FAILED, "retry cap reached after 5 attempts")]

    names = {p.id: p.name for p in PROFILES}
    decisions: list[DecisionRead] = []

    for i in range(50):
        consumer_id, state, reason = rng.choice(outcomes)
        completed_s = RUN_LENGTH_S - i * rng.uniform(0.2, 0.9)
        event_type = (
            "balance.available"
            if state is DeliveryState.EXPIRED
            else "customer.subscription.updated"
            if state is DeliveryState.SUPERSEDED
            else rng.choice(EVENT_TYPES)
        )
        entity_key = {
            "payment_intent.succeeded": f"pi_{rng.randrange(10**8):08x}",
            "customer.subscription.updated": f"sub_{rng.randrange(900) + 100}",
            "balance.available": f"acct_{rng.randrange(90) + 10}",
            "invoice.paid": f"in_{rng.randrange(10**6):06x}",
        }[event_type]

        decisions.append(
            DecisionRead(
                # Ingest order, deliberately not completion order -- the reason
                # /decisions is not cursored. Shuffled ids here make that
                # concrete for anyone building against the fixture.
                delivery_id=rng.randrange(1000, 90000),
                consumer_id=consumer_id,
                consumer_name=names[consumer_id],
                event_type=event_type,
                entity_key=entity_key,
                state=state,
                terminal_reason=(reason or "").format(n=rng.randrange(20, 400)) or None,
                attempt_count=5 if state is DeliveryState.FAILED else (0 if reason else 1),
                occurred_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=completed_s - rng.uniform(30, 300)),
                completed_at=VIRTUAL_EPOCH_ZERO + timedelta(seconds=completed_s),
            )
        )

    decisions.sort(key=lambda d: d.completed_at, reverse=True)
    return decisions


def build_consumers(runs: dict[int, ConsumerRun]) -> list[ConsumerRead]:
    return [
        ConsumerRead(
            id=run.profile.id,
            name=run.profile.name,
            weight=run.profile.weight,
            concurrency_cap=run.profile.concurrency_cap,
            max_attempts_per_s=run.profile.max_attempts_per_s,
            backlog=int(run.backlog),
            in_flight=0,
            delivered=run.delivered,
            expired=run.expired,
            superseded=run.superseded,
            failed=run.failed,
            caught_up_after_s=run.caught_up_at_s,
        )
        for run in runs.values()
    ]


def build_simulation() -> SimulationRead:
    virtual_now_s = float(RUN_LENGTH_S)
    return SimulationRead(
        id=SIMULATION_ID,
        scenario_name="outage_recovery",
        status=SimStatus.RUNNING,
        speed_multiplier=20.0,
        fair_drain_enabled=True,
        global_attempts_per_s=30.0,
        outage_override=None,
        virtual_now=VIRTUAL_EPOCH_ZERO + timedelta(seconds=virtual_now_s),
        virtual_now_s=virtual_now_s,
        phase=phase_at(virtual_now_s),
        created_at_wall=FIXTURE_WALL,
    )


def build_processes(rng: random.Random) -> list[ProcessRead]:
    """One conductor and three workers, as compose runs them."""
    started = FIXTURE_WALL - timedelta(seconds=90)
    rows: list[ProcessRead] = [
        ProcessRead(
            id=uuid.UUID(int=0xC0DE0001),
            kind=ProcessKind.CONDUCTOR,
            hostname="conductor-1",
            pid=1,
            started_at_wall=started,
            last_heartbeat_wall=started + timedelta(seconds=88),
            is_leader=True,
            heartbeat_age_s=round(rng.uniform(0.1, 2.9), 2),
            in_flight=0,
        )
    ]
    for n in range(1, 4):
        rows.append(
            ProcessRead(
                id=uuid.UUID(int=0xB0B00000 + n),
                kind=ProcessKind.WORKER,
                hostname=f"worker-{n}",
                pid=n,
                started_at_wall=started,
                last_heartbeat_wall=started + timedelta(seconds=88),
                is_leader=False,
                heartbeat_age_s=round(rng.uniform(0.1, 2.9), 2),
                in_flight=rng.randrange(0, 9),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def write(path: Path, model: BaseModel | list[BaseModel]) -> None:
    """Dump through the model, so the fixture is exactly what the API returns."""
    payload: Any
    if isinstance(model, list):
        payload = [m.model_dump(mode="json") for m in model]
    else:
        payload = model.model_dump(mode="json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")

    if isinstance(payload, list):
        note = f"{len(payload)} rows"
    else:
        # Report the interesting count for a page wrapper, not its key count.
        rows = next((v for v in payload.values() if isinstance(v, list)), None)
        note = f"{len(rows)} rows" if rows is not None else "1 object"
    print(f"  {path}  ({note})")


def main() -> None:
    rng = random.Random(SEED)

    buckets, runs = build_metrics(rng)
    metrics = MetricsPage(
        simulation_id=SIMULATION_ID,
        buckets=buckets,
        next_since_bucket=buckets[-1].bucket_virtual_s,
    )
    decisions = DecisionsPage(simulation_id=SIMULATION_ID, decisions=build_decisions(rng, runs))

    print("writing fixtures:")
    write(OUT_DIR / "simulation.json", build_simulation())
    write(OUT_DIR / "metrics.json", metrics)
    write(OUT_DIR / "decisions.json", decisions)
    write(OUT_DIR / "process.json", build_processes(rng))
    write(OUT_DIR / "consumer.json", build_consumers(runs))

    from app.api.main import create_app

    OPENAPI_PATH.write_text(json.dumps(create_app().openapi(), indent=2) + "\n")
    print(f"  {OPENAPI_PATH}")

    # A one-line sanity read, because a fixture that is shaped wrong is worse
    # than no fixture: the charts get built against it before anyone notices.
    peak = max(b.backlog for b in buckets if b.consumer_id == 1)
    final = [b for b in buckets if b.bucket_virtual_s == buckets[-1].bucket_virtual_s]
    print(
        f"\nshape: Acme peaks at {peak} backlog during the outage; "
        f"at t={buckets[-1].bucket_virtual_s}s backlogs are "
        + ", ".join(f"{b.consumer_name.split()[0]}={b.backlog}" for b in final)
    )
    for run in runs.values():
        caught = f"{run.caught_up_at_s:.0f}s" if run.caught_up_at_s is not None else "still draining"
        print(
            f"  {run.profile.name:<16} delivered={run.delivered:<6} "
            f"expired={run.expired:<5} superseded={run.superseded:<5} caught up after {caught}"
        )


if __name__ == "__main__":
    main()
