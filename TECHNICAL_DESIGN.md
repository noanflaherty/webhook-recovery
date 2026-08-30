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

- I can set each consumer's `weight`, `concurrency_cap`, and `max_attempts_per_s`.
- I can toggle fair draining on/off (for the demo, to show its effect).
- I can lose a delivery worker mid-flight without permanently losing capacity or dropping events.

## Core Concepts

| Term | Meaning |
|---|---|
| **Event** | An immutable fact emitted by the producer (`payment_intent.succeeded`, ...). Stored once in the ledger. |
| **Consumer** | A subscriber with one endpoint, a set of subscribed event types, and three delivery knobs: `weight`, `concurrency_cap`, `max_attempts_per_s`. |
| **Delivery** | The (event, consumer) pair — the unit of work in the queue. One event fans out to N deliveries. |
| **Attempt** | One try at delivering a delivery. Fairness is measured in attempts. |
| **Policy** | Per (consumer, event_type): `max_staleness_s`, `coalesce` (`none` \| `latest_by_key`). Absent policy = deliver everything. |
| **Lease** | A time-bounded claim by one worker on one delivery. Expiry, not liveness detection, is what makes the system self-healing. |
| **Simulation** | A namespace holding all of the above plus a virtual clock. Each reviewer visit creates a fresh one. |

## Architecture

Three process types, sharing nothing but Postgres.

```
                    ┌────────────────────────────┐
                    │        api  (×1)           │  ─ ingest + read plane ─
                    │  · POST /event  → ledger   │
                    │  · fan out to `pending`    │
                    │  · serve UI + REST reads   │
                    └────────────┬───────────────┘
                                 │
                    ┌────────────▼─────────────────────────┐
                    │            Postgres                  │
                    │  ledger · queue · policy · metrics   │
                    └──────▲───────────────────┬───────────┘
                           │                   │
        ┌──────────────────┴───┐        ┌──────▼──────────────┐
        │   conductor (×1)     │        │   worker (×3)       │
        │  ─ policy plane ─    │        │  ─ data plane ─     │
        │  · evaluate policy   │        │  · claim `ready`    │
        │  · fairness + rates  │        │  · lease + attempt  │
        │  · mark `ready`      │        │  · record outcome   │
        │  · reap dead leases  │        │                     │
        │  · write metrics     │        │  (no policy logic)  │
        └──────────────────────┘        └─────────────────────┘
```

Three planes, sharing nothing but Postgres:

- **Ingest** (api) creates work. In production these are the provider's own internal services `POST`ing events; in the
  demo a background task plays the same role against the same endpoint.
- **Policy** (conductor) decides *what may be attempted right now*. It never creates work and never performs I/O against
  consumers — it only transitions rows.
- **Data** (worker) executes. Stateless, interchangeable, safe to kill.

Because ingest is independent of scheduling, losing the conductor stops *delivery* but never *acceptance*: events keep
landing in the ledger and the backlog grows until a standby takes over. That is the correct production semantic, and it
is also what makes conductor failover demonstrable (§Conductor is a singleton).

**Conductor is a singleton**, guarded by `pg_try_advisory_lock` rather than by convention — run two and one leads, the
other stands by. Its in-memory state must be fully reconstructible from Postgres, which is why fairness is a query
(§Fairness) rather than an accumulator.

### Conductor is a singleton — and why that isn't a single point of failure

Two conductors would corrupt admission control, not merely duplicate work. Fairness is a read-modify-write over a
sliding window ("Acme has had 40 attempts in 5s, its share allows 60, admit 20"), and work that is *admitted but not yet
attempted* is not in that window yet — so two conductors both read 40 and both admit 20, and the consumer gets twice its
share. The same race breaks `max_attempts_per_s`, and coalescing could mark two deliveries for the same entity key
`ready`. Serialization has to live somewhere: in one process, or in atomic operations on shared state.

It is a **coordination point, not a throughput point**. The conductor performs no network I/O against consumers; it only
writes state transitions. All the expensive work is in workers, which scale horizontally. One brain, many hands — the
same shape as the Kubernetes scheduler or the Kafka controller.

When it dies:

| | |
|---|---|
| Workers | Keep delivering — they drain the `ready` buffer (~1–2× `Σ concurrency_cap` of runway) |
| Ingest | Unaffected; events keep landing in the ledger |
| State | Nothing lost or corrupted. All conductor state is already in Postgres, which is exactly why fairness is a query and not a counter |
| Stops | New admissions once the buffer drains, lease reaping, metrics snapshots |

So the failure mode is *throughput degrades to zero, then fully resumes* — an availability blip, not a durability or
correctness event. Backlog grows during the gap and drains after.

**Failover is ~1s and nearly free.** `pg_try_advisory_lock` is tied to a Postgres *session*: when the conductor process
dies its connection drops and Postgres releases the lock automatically. Liveness detection comes from the connection
itself — no heartbeat interval, no lease TTL to tune. Standbys polling once a second acquire almost immediately.

One gotcha to design around: if the conductor holds the lock on one pooled connection but writes on others, a dropped
lock connection does not stop its writes, and that is genuine split-brain. So the conductor **holds the lock on the same
session it writes through** — losing the lock means losing the ability to write, and fencing becomes automatic rather
than something we implement.

**The real single point of failure is Postgres**, not the conductor: it is the queue, the ledger, and the leader-election
arbiter. The answer to that is managed HA Postgres with a replica — infrastructure, not application cleverness. Making
the conductor highly available while sitting on a single database would be theater.

Removing the singleton entirely, in escalating order (both under Future Work): shard the advisory lock by `consumer_id`
range so N conductors own disjoint consumer sets and blast radius drops to 1/N; or move the sliding window and token
bucket into atomic Redis counters, at which point any number of conductors can run and Redis becomes the arbiter.

## The Virtual Clock

The clock is the first thing a multi-process split breaks: three process types must agree on what time it is, and
virtual latency must mean something across a process boundary. Storing `virtual_now` in a row and having the conductor
increment it creates a distributed barrier problem — the conductor must not advance time while a worker is mid-attempt,
or a "200ms" attempt sees the clock jump three seconds.

**Solution: the clock is derived, never stored.** Virtual time is wall time with an epoch and a multiplier:

```python
def now(self) -> datetime:
    if sim.status == "paused":
        return sim.paused_at_virtual
    elapsed_real = wall_now() - sim.resumed_at_wall
    return sim.virtual_epoch + elapsed_real * sim.speed_multiplier
```

Every process computes virtual time locally from a handful of near-immutable fields on the `simulation` row. **No
coordination, no barrier, no polling.** Pause / resume / speed-change are writes of a new epoch, which every process
picks up on its next config read.

Consequences:

- Workers sleep in *real* time scaled by the multiplier: 200ms of virtual latency at 20× is 10ms of real sleep. Virtual
  latency stays physically honest, and a worker genuinely holds its lease for the duration.
- `WallClock` for production is the same class with `speed_multiplier = 1` and a zero epoch. Nothing else in the codebase
  calls `datetime.now()`.
- Clock skew between processes exists, but is sub-millisecond on one host — and it is the same skew production already
  lives with. Documented, not mechanised.
- We lose bit-for-bit determinism (a single-process discrete-event sim has it; this does not). Recovered in the part that
  matters by seeding the consumer simulator's RNG with `(simulation_id, delivery_id, attempt_no)`, so an attempt's
  outcome is reproducible regardless of which worker runs it.

## Delivery Lifecycle

```
pending ──(conductor: fairness + rate + policy)──▶ ready ──(worker claims + leases)──▶ in_flight ──▶ delivered
   │                                                                                       │
   │                                                                                       ├──▶ pending   (retry: backoff)
   │                                                                                       ├──▶ pending   (lease expired)
   ├──▶ expired      (conductor: max_staleness exceeded)                                   └──▶ failed    (retry cap hit)
   └──▶ superseded   (conductor: newer delivery for same entity key)
```

Policies are evaluated **at dispatch time, not ingest time**. This is the central design choice: an event that was fresh
when it was ledgered may be stale by the time the provider recovers, and whether it has been superseded depends on what
queued up behind it. Evaluating lazily keeps the ledger complete and replayable, and makes every drop an explicit
terminal state rather than a silent discard.

`expired` and `superseded` are decided by the conductor and never reach a worker.

### The three delivery knobs

Deliberately separate, because each constrains a different thing:

| Knob | Constrains | Without it |
|---|---|---|
| `weight` | *Relative* share of contended provider capacity | Fairness is all-or-nothing; you can't say "this consumer matters more" |
| `concurrency_cap` | Max simultaneous in-flight attempts | A slow consumer's open connections pile up unboundedly |
| `max_attempts_per_s` | *Absolute* ceiling on attempt rate | A **fast** consumer gets hammered: at 20ms latency, a concurrency cap of 8 is ~400 req/s. Concurrency caps parallelism, not throughput |

`weight` is a scheduling preference; the other two are hard caps the consumer is protected by even when nobody else is
competing. A consumer is dispatchable only if it is under *both* caps.

## Conductor

Loop, every ~150ms real. The conductor **never creates work** — it only transitions rows that ingest already wrote:

1. **Reap expired leases** — `state='in_flight' AND lease_expires_at < now()` → back to `pending` with
   `attempt_count += 1`, recording an `attempt` row with `outcome='lease_expired'`. Never consults the worker registry;
   only the lease timestamp matters.
2. **Top up the ready buffer** — the core scheduling step, below.
3. **Write metrics** — one `metrics_snapshot` row per consumer per virtual second.

### Fairness

With no global tick there is no "this tick's budget," so fairness is computed as a **sliding window**: attempts started
per consumer over the last N virtual seconds, queried from the `attempt` table. The conductor admits work to push each
consumer toward its target share `weight / Σweights(dispatchable consumers)`.

The same window enforces `max_attempts_per_s` — one mechanism, two uses, and no mutable token-bucket column to keep
consistent across processes.

A consumer is **dispatchable** when it has work due, free concurrency slots
(`COUNT(*) WHERE state='in_flight'` < cap), and headroom under its rate cap. Unused share is redistributed to the others
(work-conserving).

- **Fair drain ON** — weighted round-robin across dispatchable consumers, as above. A consumer's in-flight attempts
  count against *its* cap only, so a slow or 5xx-ing consumer stalls itself and nobody else.
- **Fair drain OFF** — global FIFO by `occurred_at` under one shared concurrency pool (`Σ concurrency_cap`). Per-consumer
  rate caps still apply; they are a consumer-protection contract, not a fairness mechanism. This is the naive
  implementation most systems ship, and it is what the toggle compares against — it must be plausible, not a strawman.

### The ready buffer must stay shallow

`ready` is an **admission-control token materialized as a row state**: the conductor has decided this specific delivery
may be attempted now. Its buffer depth **is the granularity of fairness**, so it is kept to roughly
`Σ concurrency_cap` (~1–2×) and continuously topped up.

If the conductor instead marked thousands ready in one pass, then even though its arithmetic was correct at that instant:

- Fairness would go bursty. Workers drain the pool in `ORDER BY ready_at`, so a large batch is consumed consumer-by-consumer
  — the attempts-share chart would show ~100% Acme, then ~100% Bolt, which is the opposite of what we claim to prove.
- Work-conservation would happen at the wrong time. A low-volume consumer allocated more share than it has work for
  leaves capacity idle until the next pass.
- Policy decisions would go stale in exactly the way dispatch-time evaluation exists to prevent.

Shallow enough that decisions don't age; deep enough that workers never starve waiting on the conductor. Same reason a
CPU scheduler doesn't assign the next 10,000 timeslices in advance.

### Policy evaluation

For each candidate delivery, before marking it `ready`:

- `now - event.occurred_at > max_staleness_s` → `expired`, no attempt.
- `coalesce = latest_by_key` and a newer `pending`/`ready` delivery exists for the same
  (consumer, event_type, entity_key) → `superseded`, no attempt.
  *(A `ready` delivery has not been attempted yet, so it remains supersedable — hence the index covers both states.)*
- Otherwise → `ready`.

Retry: exponential backoff (`base * 2^n`, capped) with a max attempt count. Global for the demo; per-consumer retry
policy is documented as future work.

## Worker

Stateless, interchangeable, three replicas. The entire claim loop:

```sql
SELECT * FROM delivery
WHERE simulation_id = $1 AND state = 'ready'
ORDER BY ready_at
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

→ flip to `in_flight`, set `leased_by` and `lease_expires_at` (fixed 30 virtual seconds), call the transport, record the
`attempt` row and terminal state.

Workers contain **no policy logic**, with one exception: a final `max_staleness` re-check immediately before attempting,
since an event can go stale in the ready→attempt gap. It's one comparison. Coalescing stays conductor-only because it
needs a query.

### Consumer transport seam

`ConsumerTransport` is a protocol with `async attempt(delivery) -> AttemptOutcome`:

- `SimulatedTransport` — per-consumer profile: base latency, jitter, failure rate, `down` flag, "hold connection open"
  behaviour. Sleeps in real time scaled by the clock multiplier. No network, so the demo is deterministic per-attempt and
  runs at 20× speed.
- `HttpTransport` — real POST, HMAC-signed, timeouts, status-code mapping. Stub + docstring only.

### Leases, and why worker liveness doesn't matter

If a worker dies holding an `in_flight` delivery, that row would otherwise be stuck forever — **permanently burning one
of that consumer's concurrency slots**. Enough crashes and the consumer silently drops to zero throughput. It's a real
bug class, and it is invisible in a single-process design.

The reaper fixes it *without any failure detection*: it asks "has this lease expired?", never "is that worker alive?"
Timeouts replace liveness. This is the property worth demonstrating.

### The `process` table is simulation scaffolding

It registers workers and conductors so the UI can list them, show which conductor holds the lock, and kill one.
**It is not required for correctness, and neither the reaper nor leader election ever reads it** — the reaper trusts
lease expiry, and leadership is decided entirely by the Postgres advisory lock. In production this view comes from the
orchestrator and metrics, not a database table. The one legitimate production use — reclaiming a known-dead worker's
leases immediately rather than waiting out the TTL — is an optimization layered on the timeout, never a replacement for
it, and is listed under Future Work.

**Kill** simulates an *ungraceful* death: `crash_requested` is set, the process sees it and stops dead **without
releasing anything**. A graceful shutdown would release its leases (or its advisory lock) cleanly and demonstrate
nothing.

- **Kill Worker** → that consumer's in-flight count sits stuck, the lease timer runs out, the conductor reclaims the
  work and it is redelivered. No events lost.
- **Kill Conductor** (stretch, needs a 2nd replica) → the killed process's DB connection drops, Postgres releases the
  advisory lock, the standby acquires it within ~1s. The attempts chart shows a small dip while the `ready` buffer
  drains, backlog ticks up because ingest is unaffected, then delivery resumes.

## API

FastAPI. Owns **ingest** and all reads, and serves the built Vite bundle.

### Ingest

```
POST /api/simulation/{id}/event      { event_type, entity_key, payload }
```

In one transaction: insert the `event` row (the ledger), look up matching `subscription` rows, and insert one `pending`
`delivery` per subscribed consumer. Fan-out is cheap, deterministic, and has no scheduling judgment in it, so it belongs
at ingest rather than in the conductor.

**In production, callers are the provider's own internal services.** In the demo, a background task in the API process
plays the producer against this same endpoint on the scenario's event mix — simulation scaffolding sitting behind a real
interface, not a special path.

Keeping ingest here is what gives conductor failure its correct shape: acceptance never depends on scheduling.

### Control and reads

```
POST   /api/simulation                      create (= reset), seed consumers/policies/scenario
GET    /api/simulation/{id}                 config + current virtual time + phase
PATCH  /api/simulation/{id}                 pause/resume, speed, fair_drain_enabled, outage override
GET    /api/simulation/{id}/metrics?since_bucket=N   → new metrics_snapshot rows
GET    /api/simulation/{id}/decisions?since_id=N     → recent terminal decisions for the event feed
GET    /api/process                         live workers + conductors, with current leader
POST   /api/process/{id}/crash              set crash_requested
POST   /api/process/{id}/revive             clear it
```

**No SSE.** The client polls `/metrics?since_bucket=N` every ~500ms and appends to its chart series. One fewer moving
part, trivially resumable, no long-lived-connection requirement on the host, and at demo volumes the cost is nil. The
API process holds no simulation state at all — it is a thin read/write layer over Postgres.

## Data Model

All tables carry `simulation_id` so reviewers run independent simulations concurrently and "Reset" is just "create a new
simulation." Timestamps are *virtual* unless suffixed `_wall`. Table names are singular — a row is one delivery, and it
keeps FK columns and table names in agreement (`delivery.consumer_id` → `consumer.id`).

```
simulation        id, created_at_wall, scenario_name, status (running|paused|done),
                  virtual_epoch, resumed_at_wall, paused_at_virtual, speed_multiplier,   -- derived clock
                  fair_drain_enabled, global_attempts_per_s, outage_override

consumer          id, simulation_id, name, weight, concurrency_cap, max_attempts_per_s,
                  sim_latency_s, sim_jitter_s, sim_failure_rate, sim_down    -- SimulatedTransport profile

subscription      consumer_id, event_type

delivery_policy   consumer_id, event_type, max_staleness_s (nullable), coalesce ('none'|'latest_by_key')

event             id, simulation_id, event_type, entity_key, occurred_at, payload jsonb          -- the ledger

delivery          id, simulation_id, event_id, consumer_id, event_type, entity_key,
                  state, attempt_count, next_attempt_at, ready_at,
                  leased_by, lease_expires_at, terminal_reason, created_at, completed_at

attempt           id, delivery_id, consumer_id, worker_id, started_at, finished_at,
                  outcome (ok|5xx|timeout|lease_expired), status_code

metrics_snapshot  simulation_id, consumer_id, bucket_virtual_s, backlog, ready, in_flight,
                  attempts, delivered, expired, superseded, failed

process           id, kind ('worker'|'conductor'), hostname, pid, started_at_wall,
                  last_heartbeat_wall, is_leader, state (running|crashed),
                  crash_requested                                   -- scaffolding; see above
```

Indexes, one per access path:

```sql
-- worker claim
CREATE INDEX ON delivery (simulation_id, ready_at) WHERE state = 'ready';
-- conductor candidate scan
CREATE INDEX ON delivery (simulation_id, consumer_id, next_attempt_at) WHERE state = 'pending';
-- coalesce lookup (ready deliveries are still supersedable)
CREATE INDEX ON delivery (consumer_id, event_type, entity_key) WHERE state IN ('pending', 'ready');
-- lease reaper
CREATE INDEX ON delivery (lease_expires_at) WHERE state = 'in_flight';
-- sliding-window fairness + rate cap
CREATE INDEX ON attempt (consumer_id, started_at);
```

`delivery` denormalizes `event_type` and `entity_key` off `event` so the coalesce lookup and policy evaluation never join.

## Simulated Consumers

| Consumer | Subscribes to | Volume | Policies | Purpose |
|---|---|---|---|---|
| **Acme Analytics** | all four event types | high | none (deliver everything) | Baseline: what a naive consumer suffers on recovery |
| **Bolt Billing** | all four event types | high | `customer.subscription.updated`: coalesce by `subscription_id`; `balance.available`: max_staleness 120s | Hero: policies shrink the backlog before it's ever sent |
| **Clover CRM** | `invoice.paid` only | low | none | Fairness case: tiny backlog, should catch up in seconds |

All weights = 1, `concurrency_cap` = 8, `max_attempts_per_s` = 20, `sim_latency_s` = 0.2 to start.

`global_attempts_per_s` is set **below** `Σ max_attempts_per_s` so the provider is genuinely the contended resource
during recovery. Otherwise every consumer simply runs at its own cap, nothing is ever contended, and the fair-drain
toggle is a visible no-op. If the toggle ever looks like it does nothing, check this ratio first.

### Producer event mix (Stripe-flavoured)

| Event type | Rate | entity_key | Notes |
|---|---|---|---|
| `payment_intent.succeeded` | high | `payment_intent_id` (unique each time) | Never droppable — money moved |
| `customer.subscription.updated` | high, bursty per subscription | `subscription_id` (small pool, repeats) | Coalesce candidate: only latest state matters |
| `balance.available` | medium | `account_id` | Staleness candidate: a stale balance is useless |
| `invoice.paid` | low | `invoice_id` | Routes to Clover only → low-volume consumer |

## Canned Scenario

~15 virtual minutes ≈ 45 real seconds at 20×.

| Phase | Virtual time | What happens | What the reviewer sees |
|---|---|---|---|
| **Normal** | 0:00 – 2:00 | All consumers keep up | Flat backlogs; attempts share ≈ proportional to volume |
| **Outage** | 2:00 – 7:00 | Delivery pipeline down: events still ledgered, nothing marked ready | Backlogs climb; Acme & Bolt fast, Clover slowly |
| **Recovery** | 7:00 → drained | Dispatch resumes | Fair drain ON: Clover catches up in seconds; Bolt's backlog collapses via expired/superseded; Acme grinds. OFF: Clover starved behind the others |
| **Done** | — | Backlogs at zero | Per-consumer catch-up times |

Run twice — once per toggle state. Simulations are namespaced, so both runs persist side by side.

Stretch knobs: kill a worker, kill the leading conductor, take a consumer down, make one slow / hold connections, change
weights, edit Bolt's policies live, adjust outage duration, speed slider.

## UI

Single page:

1. **Control bar** — Play/Pause, speed, Fair drain toggle, Reset, phase indicator + virtual clock.
2. **Backlog over time** — one line per consumer. The shape of recovery.
3. **Attempts share over time** — 100% stacked bar per virtual-second bucket. The fairness proof: with fair drain on and
   equal weights, segments are equal *whenever all three have backlog*. Once Clover drains, its segment correctly goes to
   zero — the legend must say so, or it reads as unfairness.
4. **Consumer cards** — backlog, in-flight / cap, delivered / expired / superseded / failed, and **time to catch up**.
5. **Process strip** — workers (heartbeat, in-flight count) and conductors (with the leader marked), each with
   Kill / Revive.
6. **Decision feed** — recent terminal decisions ("superseded sub_123 ×14 → delivered latest", "reclaimed 6 leases from
   worker-2"), so policy and recovery behaviour are legible, not just numeric.

## Tech Stack

- **Python 3.12, FastAPI, asyncio** — three entrypoints from one codebase and one image.
- **Postgres** (SQLAlchemy Core / asyncpg, Alembic) — ledger, queue, policy, metrics, and the only shared state.
- **React + TypeScript + Vite**, Recharts.
- **Docker** — one image, three start commands.

## Deployment

**Railway**, one repo → one image → three services differing only by start command, sharing the managed Postgres plugin's
injected `DATABASE_URL`:

| Service | Command | Replicas |
|---|---|---|
| `api` | `uvicorn app.api:app` | 1 |
| `conductor` | `python -m app.conductor` | 2 — one leads, one stands by (advisory lock) |
| `worker` | `python -m app.worker` | 3 |

Every process self-registers at boot with a generated id, so replicas need no per-instance config. No long-lived
connections required (polling, not SSE), no cold-start sleep.

Rejected: *Fly.io* (equally capable, more CLI ceremony for no demo benefit); *Render* (free tier spins down — bad first
impression for a reviewer); *Vercel/serverless* (no long-lived process for conductor or workers).

## At Real Scale

How the *existing* design changes under production load. No new features here — the same system with the demo's
simplifying assumptions removed.

| Concern | Demo | Production |
|---|---|---|
| **Clock** | `VirtualClock`, 20× | `WallClock`: same class, `speed_multiplier = 1`. Nothing else changes, because time is only read through the `Clock` protocol |
| **Delivery** | `SimulatedTransport` | `HttpTransport`: HMAC-signed payloads, connect/read timeouts, TLS, per-endpoint circuit breaker |
| **Workers** | 3 replicas | N replicas; already stateless and lease-based, so this is a replica-count change. Claim contention is handled by `SKIP LOCKED` |
| **Conductor** | 1, advisory-locked | Same. It's a leader, not a bottleneck — it writes state transitions, never performs I/O against consumers. If it does become one, shard the advisory lock by `consumer_id` range so N conductors own disjoint consumer sets |
| **Fairness + rate window** | Query over `attempt` each loop | Materialize into Redis counters (`INCRBY` + TTL) per consumer, or a rolling summary table. The query is the bottleneck long before the algorithm is |
| **Concurrency count** | `COUNT(*) WHERE state='in_flight'` | Same Redis counter, incremented on lease and decremented on release/reap |
| **Queue** | `delivery` table | Postgres holds into the low thousands of deliveries/sec with time partitioning and the partial indexes above. Beyond that: per-consumer Kafka partitions or SQS, with policy still evaluated at dequeue |
| **Ledger** | `event` table | Time-partitioned with a retention window; replay reads the ledger, never the queue |
| **Backpressure** | Producer always ingests | Ingest is the last thing to shed. If the queue can't drain, alert — `expired` is a consumer's stated preference, never a capacity release valve |
| **Metrics transport** | Client polls the API | Same polling for the UI; Prometheus counters per (consumer, outcome) for ops. The attempts-share chart *is* the fairness SLO — it's the graph you'd alert on |

## Key Design Decisions and Tradeoffs

Running list of the decisions that shaped the system, each with what it cost.

**1. Policies are evaluated at dispatch time, not ingest time.**
An event that was fresh when it was ledgered may be stale by the time the provider recovers, and whether it has been
superseded depends on what queued up behind it — neither is knowable at ingest.
*Cost:* real work on the hot dispatch path, and partial indexes to keep it cheap.
*Gain:* the ledger stays complete and replayable, and every drop is an explicit terminal state rather than a silent
discard. This is the decision the whole product thesis rests on.

**2. The conductor is a singleton (policy plane / data plane split).**
Admission control is a read-modify-write over a sliding window; two conductors would both read the same pre-admission
count and both admit, handing a consumer twice its share. Serialization has to live somewhere.
*Cost:* a coordination point and a ~1s failover gap.
*Gain:* correct admission control, all scheduling judgment in one auditable place, and stateless workers. Crucially it
is a coordination point, not a throughput point — the conductor performs no I/O against consumers, so it does not cap
delivery rate. Losing it degrades throughput to zero and then fully resumes; nothing is lost or corrupted.

**3. Leader election via Postgres advisory lock, held on the writing session.**
*Cost:* Postgres becomes the arbiter as well as the datastore.
*Gain:* liveness detection comes free from the TCP connection — no heartbeat interval or lease TTL to tune — and because
the lock is held on the same session the conductor writes through, losing the lock means losing the ability to write.
Fencing is automatic rather than implemented. (The real SPOF is Postgres itself; the honest answer to that is managed HA
Postgres, not application cleverness.)

**4. `ready` as an explicit admission state, with a deliberately shallow buffer.**
*Cost:* one more state and a depth knob to tune.
*Gain:* workers hold zero policy logic, and the buffer depth *is* the granularity of fairness — a deep buffer would make
the attempts-share chart show bursts of one consumer at a time, which is the opposite of what we claim to prove.

**5. Lease expiry instead of worker liveness detection.**
The reaper asks "has this lease expired?", never "is that worker alive?"
*Cost:* recovery latency is bounded by the lease TTL (30 virtual seconds).
*Gain:* no failure detector to build or tune, and the correctness path never reads the process registry. Without it, a
crashed worker permanently burns one of its consumer's concurrency slots — a real bug class that is invisible in a
single-process design.

**6. Fairness as a sliding-window query over `attempt`, not an in-memory budget.**
*Cost:* a query every conductor loop; at real scale this is the first thing that needs materializing into Redis.
*Gain:* conductor state is fully reconstructible from Postgres, which is what makes failover free — and the same window
enforces `max_attempts_per_s`, so one mechanism serves two purposes with no mutable token-bucket column to keep
consistent across processes.

**7. A derived virtual clock rather than a stored, ticked one.**
Virtual time is wall time with an epoch and a multiplier, computed locally in every process.
*Cost:* bit-for-bit determinism (recovered where it matters by seeding the transport RNG per attempt), and a dependency
on modest clock skew.
*Gain:* no distributed barrier, no coordination, no polling — and `WallClock` for production is the same class with
`speed_multiplier = 1`.

**8. Three processes instead of one.**
*Cost:* several hours of build time, lost determinism, and more deploy surface.
*Gain:* it forced the production-correct shape everywhere — derived clock, sliding-window fairness, leases — and made
crash recovery something a reviewer can *watch* rather than read about.

**9. Ingest lives in the API, not the conductor.**
*Cost:* none material; fan-out is cheap deterministic work with no scheduling judgment in it.
*Gain:* acceptance never depends on scheduling, which is both the correct production semantic and what makes conductor
failure legible — backlog visibly grows during failover instead of freezing.

**10. Postgres as the only shared state.**
*Cost:* it is the real SPOF and the eventual scaling ceiling.
*Gain:* one dependency for queue, ledger, leader election, and metrics; transactional correctness for free; and
`SKIP LOCKED` gives safe multi-worker claiming with no broker.

**11. Simulated transport instead of real HTTP.**
*Cost:* it is not "real" delivery.
*Gain:* deterministic per-attempt outcomes, a 15-minute scenario in 45 seconds, and no external dependency for a
reviewer. The `ConsumerTransport` seam is defined so `HttpTransport` drops in without touching the scheduler.

**12. Polling instead of SSE.**
*Cost:* up to ~500ms of staleness and a chattier client.
*Gain:* no long-lived connections (which removes a real hosting constraint), trivially resumable, and the API process
holds no simulation state at all.

**13. Coalescing breaks event-log semantics — accepted, and stated.**
A consumer with `latest_by_key` may receive a later state than expected and never see the intermediate ones. That is
correct for `customer.subscription.updated` and wrong for anything treated as an append-only log.
*Mitigation:* it is opt-in per (consumer, event_type) and off by default, and the drop is recorded as `superseded`
rather than vanishing. A documented idempotency contract is the proper fix (Future Work).

## Future Work

Features out of scope for the prototype, in rough priority order.

| Feature | Why |
|---|---|
| **Multi-tenancy** | `simulation_id` becomes `tenant_id`/`provider_id` — already threaded through every table, so it's auth and isolation work, not a schema change |
| **Fast lease reclamation** | Use worker heartbeats to reclaim a known-dead worker's leases immediately instead of waiting out the TTL. Strictly an optimization on top of expiry — the timeout stays the source of truth |
| **Per-consumer retry policy** | Backoff base, cap, and max attempts are global today. Real consumers have different tolerances |
| **Payload filter expressions** | Subscribe to `invoice.paid` *where* `amount > 10000`, evaluated at the same dispatch-time seam as existing policies |
| **More coalesce strategies** | `batch_by_key` (deliver the run as an array), `first_by_key` |
| **Explicit replay window** | After an outage, let a consumer opt into "replay the last N minutes" rather than inheriting whatever staleness decided |
| **Idempotency contract** | `event.id` in a header plus documented consumer-side dedupe. Coalescing makes this *more* important: a consumer may see a later state than expected and never see the intermediate ones |
| **Dead-letter queue + manual replay** | `failed` deliveries just sit terminal today. Both sides want to see and retry them |
| **Consumer-facing dashboard** | The UI today is the provider's view, but the whole thesis is consumer experience |
| **Real HTTP delivery** | `HttpTransport` behind the existing seam, against a reviewer-supplied endpoint |
