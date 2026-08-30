# Technical Design

## User Stories

As a consumer of a provider webhook...

- I should be able to register per endpoint, per event type, a policy representing whether I want a given event to be
  delivered. Specifically:
    - `max_staleness`: how late is too late. Older events are recorded as `expired`, not delivered.
    - `coalesce: latest_by_key`: when multiple queued events share an entity key, deliver only the newest.
    - `retry`: backoff strategy and cap.
- If I did not specify a delivery policy, then it should default to "deliver everything"
- Should not have the delivery rate of my event backlog slowed down by the size of another consumer's backlog. I expect
  the provider to perform "fair draining" where delivery is scheduled per consumer with weighted round-robin and a
  per-consumer concurrency cap. Every consumer makes progress proportional to its share, not its backlog. Another consumer
  holding connections open or returning 5xx should burn its own budget, not mine.

As the provider operating the webhook system...

- I can set each consumer's `weight` and `concurrency_cap`.
- I can toggle fair draining on/off (for the demo, to show its effect).

## Core Concepts

| Term | Meaning |
|---|---|
| **Event** | An immutable fact emitted by the producer (`payment_intent.succeeded`, ...). Stored once in the ledger. |
| **Consumer** | A subscriber with one endpoint, a set of subscribed event types, a `weight`, and a `concurrency_cap`. |
| **Delivery** | The (event, consumer) pair — the unit of work in the queue. One event fans out to N deliveries. |
| **Attempt** | One try at delivering a delivery. Fairness is measured in attempts. |
| **Policy** | Per (consumer, event_type): `max_staleness_s`, `coalesce` (`none` \| `latest_by_key`). Absent policy = deliver everything. |
| **Simulation** | A namespace holding all of the above plus a virtual clock. Each reviewer visit creates a fresh one. |

### Delivery lifecycle

```
pending ──▶ in_flight ──▶ delivered
   │            │
   │            └──▶ pending (retry, next_attempt_at = now + backoff)   ──▶ failed (retry cap hit)
   ├──▶ expired     (max_staleness exceeded at dispatch time)
   └──▶ superseded  (coalesce: a newer pending delivery shares the entity key)
```

Policies are evaluated **at dispatch time**, not at ingest time. This is the key design choice: an event that was fresh
when it was ledgered may be stale by the time the provider recovers, and whether it has been superseded depends on what
else has queued up behind it. Evaluating lazily means the ledger is always complete (audit/replay-able) and the
"drop" decisions are recorded as explicit terminal states, never silent.

## Scheduler

Runs once per **tick** (1 virtual second). Each tick:

1. **Producer** emits this tick's events into the ledger and fans them out into `pending` deliveries.
2. **Consumer simulators** resolve any in-flight attempts whose virtual latency has elapsed (success / 5xx / timeout),
   freeing concurrency slots and scheduling retries.
3. **Dispatch** fills free slots, subject to a global provider attempt budget per tick (`GLOBAL_ATTEMPTS_PER_TICK`):
    - **Fair drain ON** — weighted round-robin across consumers that have dispatchable work. Each consumer's share of the
      tick budget is `weight / Σweights(active consumers)`. Unused share (consumer has no free slots or no work) is
      redistributed to the others (work-conserving). A consumer's own in-flight attempts count against *its*
      `concurrency_cap` only, so a slow or 5xx-ing consumer stalls itself and nobody else.
    - **Fair drain OFF** — global FIFO by `occurred_at` under one shared concurrency cap (`Σ concurrency_cap`). This is
      the naive implementation most systems ship: a consumer with a large backlog crowds out everyone else, and a slow
      consumer holds shared slots.
4. For each delivery picked, apply the consumer's policy for that event type **before** attempting:
    - `now - event.occurred_at > max_staleness_s` → mark `expired`, no attempt, pick next.
    - `coalesce = latest_by_key` and a newer `pending` delivery exists for the same (consumer, event_type, entity_key) →
      mark `superseded`, no attempt, pick next.
    - Otherwise mark `in_flight`, record an attempt, hand to the consumer simulator.
5. Write a **metrics snapshot** row per consumer for this tick and push it over SSE.

Retry: exponential backoff (`base * 2^n`, capped) with a max attempt count. Kept simple and global for the demo;
per-consumer retry policy is a documented extension.

### Clock

`Clock` is a protocol with `now() -> datetime`. Two implementations:

- `VirtualClock` — advanced explicitly by the simulation loop. The whole system (scheduling, staleness, backoff, latency)
  reads time only through the clock, so a 15-virtual-minute scenario plays out in ~30–60 real seconds and can be paused
  or stepped.
- `WallClock` — production; the scheduler loop sleeps between ticks instead of advancing time.

Nothing else in the codebase calls `datetime.now()`.

### Consumer transport seam

`ConsumerTransport` is a protocol with `async attempt(delivery) -> AttemptOutcome`. Two implementations:

- `SimulatedTransport` — per-consumer profile: base latency (virtual seconds), jitter, failure rate, `down` flag,
  optional "hold connection open" behavior. No network. Chosen so the demo is deterministic and runs on a virtual clock.
- `HttpTransport` — real POST with signed payload (HMAC), timeout, status-code mapping. Stub + docstring only unless
  time allows.

## Data Model (Postgres)

All tables carry `simulation_id` so multiple reviewers can run independent simulations concurrently and "Reset" is just
"create a new simulation." Timestamps are *virtual* timestamps.

```
simulations        id, created_at (wall), virtual_now, status (running|paused|done), fair_drain_enabled,
                   global_attempts_per_tick, scenario_name

consumers          id, simulation_id, name, weight, concurrency_cap,
                   sim_latency_s, sim_failure_rate, sim_down            -- SimulatedTransport profile

subscriptions      consumer_id, event_type

delivery_policies  consumer_id, event_type, max_staleness_s (nullable), coalesce ('none'|'latest_by_key')

events             id, simulation_id, event_type, entity_key, occurred_at, payload jsonb        -- the ledger

deliveries         id, simulation_id, event_id, consumer_id, state, attempt_count,
                   next_attempt_at, in_flight_until, terminal_reason, created_at, completed_at
                   INDEX (simulation_id, consumer_id, state, next_attempt_at)                  -- dispatch
                   INDEX (consumer_id, event_type, entity_key) WHERE state='pending'           -- coalesce lookup

attempts           id, delivery_id, consumer_id, started_at, finished_at, outcome (ok|5xx|timeout), status_code

metrics_snapshots  simulation_id, consumer_id, tick, backlog, attempts, delivered, expired, superseded, failed,
                   in_flight
```

Dispatch selects with `SELECT ... FOR UPDATE SKIP LOCKED` even though there is one scheduler process today, so that
adding scheduler replicas later is a config change rather than a redesign (see "At real scale").

## Simulated Consumers

| Consumer | Subscribes to | Volume | Policies | Sim profile | Purpose |
|---|---|---|---|---|---|
| **Acme Analytics** | all four event types | high | none (deliver everything) | healthy, 200ms | Baseline: what a naive consumer suffers on recovery |
| **Bolt Billing** | all four event types | high | `customer.subscription.updated`: coalesce by `subscription_id`; `balance.available`: max_staleness 120s | healthy, 200ms | Hero: policies shrink the backlog before it's ever sent |
| **Clover CRM** | `invoice.paid` only | low | none | healthy, 200ms | Fairness victim/beneficiary: tiny backlog, should catch up in seconds |

All weights = 1, `concurrency_cap` = 8 to start. Provider budget `GLOBAL_ATTEMPTS_PER_TICK` sized so recovery takes a
visible ~1–2 virtual minutes for the big consumers.

### Producer event mix (Stripe-flavoured)

| Event type | Rate (per virtual sec) | entity_key | Notes |
|---|---|---|---|
| `payment_intent.succeeded` | high | `payment_intent_id` (unique each time) | Never droppable — money moved |
| `customer.subscription.updated` | high, bursty per subscription | `subscription_id` (small pool, repeats) | Coalesce candidate: only the latest state matters |
| `balance.available` | medium | `account_id` | Staleness candidate: a stale balance notification is useless |
| `invoice.paid` | low | `invoice_id` | Routes to Clover only → low-volume consumer |

## Canned Scenario

Virtual timeline (~15 virtual minutes ≈ 30–60 real seconds at default speed):

| Phase | Virtual time | What happens | What the reviewer sees |
|---|---|---|---|
| **Normal** | 0:00 – 2:00 | Producer emits, all consumers keep up | Flat backlogs, attempts share ≈ proportional to volume |
| **Outage** | 2:00 – 7:00 | Provider delivery pipeline down: events still ledgered, no dispatch | Backlogs climb; Acme & Bolt grow fast, Clover slowly |
| **Recovery** | 7:00 → drained | Dispatch resumes | With fair drain ON: Clover catches up in seconds, Bolt's backlog collapses via expired/superseded, Acme grinds. OFF: everyone finishes together, Clover starved |
| **Done** | — | All backlogs at zero | Catch-up times per consumer |

The reviewer runs it twice — once with fair drain on, once off — via the toggle. Since simulations are namespaced, both
runs can be kept side-by-side (stretch: a "compare" view).

Stretch knobs (later milestone): take a single consumer down, make a consumer slow / hold connections, change weights,
edit Bolt's policies live, change outage duration, speed slider.

## UI

Single page, kept minimal:

1. **Control bar** — Play/Pause, speed, Fair drain toggle, Reset (= new simulation), phase indicator with virtual clock.
2. **Backlog over time** — one line per consumer. The "shape" of recovery.
3. **Attempts share over time** — 100% stacked bar per tick (or per 5-tick bucket), one segment per consumer. The
   fairness proof: with fair drain on and equal weights, segments are equal whenever all three have backlog.
4. **Consumer cards** — per consumer: backlog, in-flight / cap, delivered / expired / superseded / failed counts, and
   **time to catch up** (virtual seconds from recovery to backlog = 0).
5. **Event feed** (small) — recent terminal decisions, e.g. "superseded sub_123 ×14 → delivered latest", so the policy
   behavior is legible, not just a number.

Data flows over SSE: the server pushes one message per tick containing the per-consumer snapshot; the client appends to
its chart series. On (re)connect the client fetches the history for the simulation, then subscribes.

## Tech Stack

- **Python 3.12, FastAPI, asyncio** — API + SSE + scheduler loop in one process. Also serves the built Vite bundle so
  there is exactly one deployable.
- **Postgres** (SQLAlchemy Core / asyncpg, Alembic migrations) — ledger, queue, policies, metrics.
- **React + TypeScript + Vite**, charts via Recharts (or similar lightweight lib).
- **Docker** — single image.

Why one process for the demo: fewer moving parts to deploy and to fail during evaluation. The seams that would split it
(`Clock`, `ConsumerTransport`, `SKIP LOCKED` dispatch, `simulation_id` namespacing) are in place from day one.

## Deployment

**Recommendation: Railway.** Deploy the repo as one service from the Dockerfile, add the managed Postgres plugin,
`DATABASE_URL` is injected automatically. Supports long-lived SSE connections, no cold-start sleep, ~$5/mo hobby plan.
Considered:

- *Fly.io* — equally capable, cheaper at the margin, but more CLI setup (`fly launch`, volumes/Postgres app) for no
  demo benefit.
- *Render* — free tier spins down after inactivity (bad first impression for a reviewer) and paid tier is pricier.
- *Vercel / serverless* — no long-lived process for the scheduler loop or SSE; would force a redesign.

## At Real Scale

What changes when this leaves the demo:

| Concern | Demo | Production |
|---|---|---|
| Clock | `VirtualClock` | `WallClock`; scheduler loop sleeps between ticks |
| Delivery | `SimulatedTransport` | `HttpTransport`: HMAC signatures, timeouts, TLS, circuit breaker per endpoint |
| Scheduler replicas | 1 | N workers, each running the same loop; `SKIP LOCKED` already prevents double-dispatch. Fairness accounting moves from in-memory per-tick budget to a per-consumer token bucket in Redis (or a `consumer_budget` row updated atomically) so workers share one view of "who has spent what" |
| Queue | Postgres table | Postgres remains fine to ~thousands of deliveries/sec with partitioning by `consumer_id`/time; beyond that, a per-consumer Kafka partition or SQS queue with the same policy evaluation at dequeue |
| Concurrency cap | In-memory counter | `in_flight` count per consumer in Redis with TTL-based lease so a crashed worker's slots are reclaimed |
| Policies | Per (consumer, event_type) | Same, plus per-consumer retry policy, filter expressions on payload, and "replay window" opt-in after an outage |
| Ledger | Same table | Partitioned by time, retained N days; replays read from ledger, not queue |
| Idempotency | — | `event.id` in headers; consumers dedupe. Coalescing makes this more important, not less, since a consumer may see a *later* event than it "expected" |
| Multi-tenancy | `simulation_id` | Becomes `tenant_id`/`provider_id`; same shape |
| Observability | `metrics_snapshots` + SSE | Prometheus counters per consumer/outcome; the attempts-share chart is exactly the SLO a provider would alert on |
