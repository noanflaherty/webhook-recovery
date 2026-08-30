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

## Core Concepts

| Term | Meaning |
|---|---|
| **Event** | An immutable fact emitted by the producer (`payment_intent.succeeded`, ...). Stored once in the ledger. |
| **Consumer** | A subscriber with one endpoint, a set of subscribed event types, and three delivery knobs: `weight`, `concurrency_cap`, `max_attempts_per_s`. |
| **Delivery** | The (event, consumer) pair — the unit of work in the queue. One event fans out to N deliveries. |
| **Attempt** | One try at delivering a delivery. Fairness is measured in attempts. |
| **Policy** | Per (consumer, event_type): `max_staleness_s`, `coalesce` (`none` \| `latest_by_key`). Absent policy = deliver everything. |
| **Lease** | A claim by one worker on one delivery, stamped with an expiry. The conductor reclaims it once the expiry passes, which is how a dead worker's work comes back (§Leases). |
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
        │  · reclaim leases    │        │                     │
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
is what keeps a conductor failover from costing anything but latency (§Conductor is a singleton).

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
| Stops | New admissions once the buffer drains, and metrics snapshots |

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

1. **Top up the ready buffer** — the core scheduling step, below.
2. **Write metrics** — one `metrics_snapshot` row per consumer per virtual second.

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

Two details the implementation forced, both of which are wrong in the obvious direction:

**Every ceiling subtracts the consumer's outstanding `ready` rows, not just its attempts.** Admitted-but-unattempted work
has no `attempt` row, so a window query cannot see it — and at a 50ms loop, successive passes would each spend the same
slice of the same window and every rate cap in the system would be roughly double what it says. It is the same correction
the global budget makes, applied per consumer, and it is a read-modify-write, which is precisely why two conductors
running this concurrently would be *wrong* rather than merely wasteful.

**Fairness is per pass, not a deficit carried across passes.** Repaying a consumer that was idle for a whole window would
hand it the entire window's share the instant it had work again — a burst that spikes the share chart toward 100% for one
consumer, which is the opposite of the smooth handover being claimed. The sliding window enforces the *caps*; the split
of each pass's budget is what enforces the *shares*. Concurrency is a gate rather than a per-row reservation for a
related reason: the ready buffer is deliberately ~1.5 × `Σ concurrency_cap` so workers never starve, so bounding
admissions by `cap - in_flight - ready` would pin the buffer at exactly `Σ cap` and make the buffer multiplier dead. The
cost is that the cap binds with about one pass of lag rather than as a hard reservation — invisible at 50ms, and worth
stating plainly rather than calling it hard.

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
  "Newer" is ordered on `(created_at, id)`, never `created_at` alone: the producer spreads a tick's events across the
  virtual window it covers, so two events for one key can share a timestamp, and under a non-total order each would
  supersede the other and *both* would be dropped — silently, with a healthy-looking chart.
- Otherwise → `ready`.

Only `pending` candidates are evaluated. A delivery that is already `ready` when a newer sibling arrives is attempted
anyway — the index spans both states for the *lookup* side, not to re-scan the buffer. That is a consequence of the
shallow ready buffer rather than a gap: at ~1.5 × `Σ concurrency_cap` the buffer holds well under a virtual second of
work, which is the third argument in [The ready buffer must stay shallow](#the-ready-buffer-must-stay-shallow) showing
up in practice.

**Policy drops are not rationed by fairness.** Fairness rations *attempts*, and an `expired` or `superseded` delivery
never becomes one. So a pass deliberately over-fetches candidates, evaluates policy across all of them, drops every one
the policy condemns, and only then rations the survivors. A loop that rationed candidates instead would leave a consumer
whose backlog is mostly coalescable — Bolt's, during recovery — unable to fill its share, looking starved by a scheduler
that was working correctly.

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

### Leases: reclaimed on expiry, never on liveness

A worker stamps `leased_by` and `lease_expires_at` when it claims a delivery. If it dies before completing, the row is
stuck `in_flight`: it counts against its consumer's `concurrency_cap`, so that capacity is *gone* rather than merely
idle, and it counts in the backlog, so the run can never satisfy `is_finished` and is swept by every conductor pass
from then on.

The reaper (`app/conductor/reaper.py`) runs first in each pass:

```sql
SELECT id, attempt_count FROM delivery
WHERE simulation_id = $1 AND state = 'in_flight' AND lease_expires_at < $2
FOR UPDATE SKIP LOCKED;
```

Each row goes back to `pending` with the retry backoff a 5xx would have earned — an expired lease *is* a failed attempt
— or to `failed` if that was its last one, so a delivery whose worker keeps dying stops cycling at the head of the
queue. `leased_by` and `lease_expires_at` are cleared as it goes.

**The shape is the point: it asks "has this lease expired?", never "is that worker alive?"** Timeouts replace liveness
detection, which is why the correctness path never reads the `process` table and why a dead worker needs no failure
detector to be tolerated. Four details are load-bearing:

- **The `attempt` row already exists.** It is written when the attempt *starts*, because that is what the fairness
  window counts, so reclamation *closes* the open row with `outcome='lease_expired'` rather than inserting a second
  one. A lease outlives the fairness window six times over (30 virtual seconds against 5), so a fresh row would land
  inside the current window and charge a consumer, now, for an attempt it already paid for half a minute ago — and it
  would then be throttled for capacity a dead worker never spent on its behalf.
- **`SKIP LOCKED`, not a wait.** A worker that is slow rather than dead holds a row lock through its completion
  transaction. Without the skip, a 50ms conductor pass blocks on it, and the mechanism that exists so a stuck worker
  costs nothing becomes the thing a stuck worker stalls.
- **Completion is fenced on the lease.** A worker sleeping in the transport against a consumer that is down can have
  its lease expire while it is perfectly alive, and then arrive holding a claim on a delivery that has already been
  requeued. Every write in `complete_batch` carries `AND state = 'in_flight' AND leased_by = :worker_id`, so the late
  completion writes zero rows instead of retiring a webhook that was never accepted.
- **It lives in the conductor, not a fourth process.** The pass already runs under the advisory lock, so there is one
  reaper by construction; and it already holds each simulation's virtual clock, which is what leases are stamped in. A
  lease therefore cannot expire while a simulation is paused — virtual time is frozen, and a pass only covers running
  simulations.

Reclamation is *correct* rather than *fast*: it waits out the TTL even when the worker is provably gone. Using
heartbeats to reclaim a known-dead worker's leases immediately is a strict optimization on top, and is listed under
Future Work — the timeout stays the source of truth either way, because that is what makes the mechanism independent of
the failure detector being right.

### The `process` table is observability only

Processes self-register at boot and heartbeat, so the UI can show that the architecture is real: three workers and two
conductors, with the leader marked. Without it a reviewer sees a single-page app and has to take the process split on
faith.

**It is not required for correctness, and leader election never reads it** — leadership is decided entirely by the
Postgres advisory lock. Nothing in the delivery path consults it.
In production this view comes from the orchestrator and metrics, not a database table.

It carries one flag that is written *to* a process rather than by one: `crash_requested`, set by
`POST /api/process/{id}/kill`. The API cannot reach another container and does not try — the target reads the flag back
on the heartbeat it was already making, so the kill costs no round trip of its own and arrives within one heartbeat
interval. This is the one place the table is not purely observational, and it is still outside the delivery path:
a process reads its own row, and nothing reads it back.

**Where a process acts on the flag is the whole design.** A worker that exited at an arbitrary moment would usually
strand nothing — a batch is a couple of milliseconds inside a 20ms loop — so it acts between the committed claim and
the completion that answers it, where a full batch of leases is stranded every time. A conductor acts at the top of its
pass, while the advisory lock's session is still open, so the lock drops the way a crash drops it: Postgres notices the
connection go rather than reading a release. That is the failover path a graceful stop can never exercise, because a
graceful stop releases the lock.

`os._exit`, not `sys.exit`: it skips `finally` blocks, the drain, deregistration and the teardown seam. **There is no
revive** and none is needed — compose and Railway both restart a process that exits non-zero, and the replacement
registers under a fresh id while the dead row ages out of the liveness window on its own.

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
POST   /api/process/{id}/kill               ask that process to exit without draining
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
                  outcome (ok|5xx|timeout), status_code

metrics_snapshot  simulation_id, consumer_id, bucket_virtual_s, backlog, ready, in_flight,
                  attempts, delivered, expired, superseded, failed

process           id, kind ('worker'|'conductor'), hostname, pid, started_at_wall,
                  last_heartbeat_wall, is_leader              -- observability only; see above
```

Indexes, one per access path:

```sql
-- worker claim
CREATE INDEX ON delivery (simulation_id, ready_at) WHERE state = 'ready';
-- conductor candidate scan
CREATE INDEX ON delivery (simulation_id, consumer_id, next_attempt_at) WHERE state = 'pending';
-- coalesce lookup (ready deliveries are still supersedable)
CREATE INDEX ON delivery (consumer_id, event_type, entity_key) WHERE state IN ('pending', 'ready');
-- sliding-window fairness + rate cap
CREATE INDEX ON attempt (consumer_id, started_at);
```

`delivery` denormalizes `event_type` and `entity_key` off `event` so the coalesce lookup and policy evaluation never join.

## Simulated Consumers

| Consumer | Subscribes to | Volume | Policies | Purpose |
|---|---|---|---|---|
| **Acme Analytics** | all four event types | high | none (deliver everything) | Baseline: what a naive consumer suffers on recovery |
| **Bolt Billing** | all four event types | high | `customer.subscription.updated`: coalesce by `subscription_id`; `balance.available`: max_staleness 60s; `payment_intent.succeeded`: none | Hero: policies shrink the backlog before it's ever sent — while every payment still lands |
| **Clover CRM** | `invoice.paid` only | low | none | Fairness case: tiny backlog, should catch up in seconds |

All weights = 1, `concurrency_cap` = 8, `max_attempts_per_s` = 20, `sim_latency_s` = 0.2 to start.

`global_attempts_per_s` is set **below** `Σ max_attempts_per_s` so the provider is genuinely the contended resource
during recovery. Otherwise every consumer simply runs at its own cap, nothing is ever contended, and the fair-drain
toggle is a visible no-op. If the toggle ever looks like it does nothing, check this ratio first.

### Producer event mix (Stripe-flavoured)

| Event type | Rate | entity_key | Notes |
|---|---|---|---|
| `payment_intent.succeeded` | high | `payment_intent_id` (unique each time) | Never droppable — money moved. No policy applies, deliberately |
| `customer.subscription.updated` | high, bursty per subscription | `subscription_id` (small pool, repeats) | Coalesce candidate: only latest state matters |
| `balance.available` | high | `account_id` | Staleness candidate: a ten-minute-old balance is worthless |
| `invoice.paid` | low | `invoice_id` | Routes to Clover only → low-volume consumer |

Each of the three high-volume types exists to isolate one behaviour, so the demo can attribute every drop to a single
cause: coalescing collapses subscription churn, staleness discards expired balances, and **every payment is still
delivered**. That contrast is the point — policies are per-event-type, and a consumer keeps the ones that matter.

Deliberately *not* stacking both policies on one event type: it would be realistic, but a reviewer could no longer tell
which mechanism dropped a given event.

At the default 5-minute outage, everything Bolt receives in the first ~4 minutes of it is already past a 60s staleness
bound by the time recovery starts — so a large majority of its `balance.available` backlog expires without an attempt,
which is what makes the policy visible in the chart rather than a footnote.

**The mix is weighted toward the policy-bearing types on purpose**, and that is a demo decision rather than a modelling
one. At an even split across the four types only ~58% of Bolt's stream was droppable, and its backlog line ran close
enough to Acme's that the mechanism worked without *reading* — the whole point of building the instrument before the
scheduler was to catch exactly this. Volume was moved between types rather than added, so the total stays at
6.05/virtual s and peak backlog, drain time and the contention ratio are unchanged.

## Canned Scenario

~15 virtual minutes ≈ 45 real seconds at 20×.

| Phase | Virtual time | What happens | What the reviewer sees |
|---|---|---|---|
| **Normal** | 0:00 – 2:00 | All consumers keep up | Flat backlogs; attempts share ≈ proportional to volume |
| **Outage** | 2:00 – 7:00 | Delivery pipeline down: events still ledgered, nothing marked ready | Backlogs climb; Acme & Bolt fast, Clover slowly |
| **Recovery** | 7:00 → drained | Dispatch resumes | Fair drain ON: Clover catches up in seconds; Bolt's backlog collapses via expired/superseded; Acme grinds. OFF: Clover starved behind the others |
| **Done** | — | Backlogs at zero | Per-consumer catch-up times |

Run twice — once per toggle state. Simulations are namespaced, so both runs persist side by side.

Stretch knobs: take a consumer down, make one slow / hold connections, change weights, edit Bolt's policies live, adjust
outage duration, speed slider.

## UI

Single page:

1. **Transport** — virtual clock, scheduler toggle (FIFO / Fair drain), speed, Play/Pause, Reset, and a **phase
   track**: the scripted run drawn to scale — normal, the hatched outage, recovery — with a playhead on it.

   *(The track replaced a chip naming the current phase. The word was the least useful part of it: "provider down" is
   already written across both plots, and what a label cannot give you is proportion — that the outage is a third of
   the run, that recovery is most of it, and how far through the clock is. It is drawn on the charts' own x-axis so
   the playhead and the band below it line up, and the cold-landing screen renders the same component without a
   playhead, where it says what a run *is* better than the sentence it replaced.)*

   *(The toggle ships as a two-arm segmented control rather than a "fair drain" checkbox. A checkbox names only one arm and
   leaves the other as its absence, which is backwards here: the off arm is global FIFO — a real scheduler, and the
   thing the entire comparison is against — not the lack of a feature. Runs start on fair drain, matching the server
   default and the system's actual behaviour, so the demo flip is **to** FIFO: the naive arm is the hypothetical
   being argued against, not the baseline this ships in.)*
2. **Backlog over time** — one line per consumer. The shape of recovery.
3. **Attempts share over time** — 100% stacked areas over 5-virtual-second windows. The fairness proof: with fair drain
   on and equal weights, segments are equal *whenever all three have backlog*. Once Clover drains, its segment correctly
   goes to zero — the legend must say so, or it reads as unfairness. A vertical marker records each point at which
   `fair_drain_enabled` was toggled, so the before/after is one image rather than a claim about what happened off-screen.

   *(Built as 5s windows rather than the per-virtual-second bars originally specified here: over a 900-second run a
   per-second bar is sub-pixel, and 5s is `fairness_window_virtual_s` — the window the scheduler's own rate term averages
   over — so the display granularity matches the mechanism's rather than being chosen for looks. Shares are also computed
   before rendering rather than via a stacked `expand` offset, because the row total is legitimately zero for the whole
   outage and dividing by it draws nothing, which is indistinguishable from a consumer genuinely getting no share.)*
4. **Consumer cards** — who each consumer is and what it is there to demonstrate, **time to catch up**, delivered, and
   expired / superseded / failed.

   *(Backlog, peak backlog and in-flight/cap were specified here and were built, then cut. Panels 2 and 3 sit directly
   beneath these cards and draw per-consumer backlog over time; a live counter that restates the line below it costs a
   glance and settles nothing. What survives is the two numbers no chart carries, which are also exactly the two
   claims: how long this consumer took to catch up, and how much of its backlog its own policies made unnecessary to
   send — with `delivered` kept beside the drops purely as their denominator.)*

5. **Run list** (`?view=runs`) — every run this browser has opened, with the scheduler arm named on each row, and
   `retire` on the ones still going.

   *(Not in the original spec, and it exists because of one: runs are already permanent and addressable by
   `?sim=<uuid>`, so the before/after comparison is literally two URLs — and nothing was keeping them. The history is
   client-side, in localStorage, because the server has no notion of a user; a `GET /api/simulation` listing would be
   every run by everyone who ever opened the deployment, which is not what "my runs" means. The cost of that choice is
   stated in the UI rather than hidden: the list does not survive clearing site data, does not follow you to another
   browser, and can name a run the server has dropped — which renders as a `gone` row you can forget, not a filtered
   one. `retire` is there because the producer feeds every `running` simulation, so abandoned runs spend the shared
   attempt budget and slow down whichever run you are actually watching; a list of runs is the first place that is
   visible.)*

6. **Process strip** — workers (heartbeat, in-flight count) and conductors (leader marked), each with a **kill**
   control.

   *(Removed in Phase 3 and brought back for one reason: it stopped being observational. The argument for cutting it
   was that it proved something the page never disputes — and that was true while it only reported. It now carries the
   only control that acts on a process rather than a run, and the two failure paths it makes watchable are ones no
   chart can show: a killed worker's in-flight count holds its stranded batch until the lease expires and the conductor
   reclaims it, and a killed leader hands the advisory lock to the standby. The strip judges liveness a second time and
   more tightly than `GET /api/process` does, because a process killed ungracefully never deregisters — see §Leases.)*

**Built and then removed.** One further panel was specified here, shipped in Phase 2, and cut in Phase 3:

- **Decision feed** — recent terminal decisions ("superseded by 913", "stale by 43s (max 120s)"), so policy behaviour
  was legible as sentences rather than only as counters.

It did its job during development and did not survive contact with the finished argument: once the consumer cards carry
per-consumer expired/superseded totals, the feed is a slower way to read the same fact. It was removed to keep the page
to the two claims and nothing else.

**The recorded run, also removed.** The UI shipped a second data source: `ReplaySource`, which served ~520 kB of
committed fixtures against a locally-driven virtual clock at `?source=replay`, so the deployed URL stayed useful when
the platform had spun the stack down. It was cut because it answered a question nobody was asking and raised one
nobody wanted: a reviewer landing on a page showing a *recording* has to work out what is live, what is canned, which
controls do anything, and whether the numbers in front of them were measured or drawn. The fixtures were synthesized
rather than recorded, which made the last of those genuinely ambiguous.

Removing it took the `DataSource` interface with it. Its entire justification was having two implementations; an
interface with one, whose docstring cites a sibling that no longer exists, costs a reader a detour to discover nothing
is there. `LiveSource` is the concrete type now, and `useRun` takes it directly.

The cost is real and worth stating: **a sleeping deployment now shows an error rather than a reference run.** The
mitigation is that runs are permanent and addressable, so a link to a finished run is the durable artifact the
recording was standing in for — but that link needs the backend awake too. `frontend/src/transform/series.test.ts`
used to build against those fixtures and now constructs its own series, which is an improvement: the properties the
tests depend on are stated in the file that depends on them rather than being emergent facts about an opaque blob.

`GET /api/simulation/{id}/decisions` is **unchanged and still served** — `scripts/verify.sh` asserts against it, and it
remains the honest way to inspect a run. What was removed is the decision to put it on the page, not the data. That is
also why `completed_at` is still non-negotiable on every terminal write (`app/conductor/policy.py`,
`app/worker/claim.py`): the route filters on it, and so does the metrics writer that feeds the counters the cards now
carry.

### Visual language

The page is treated as a **strip-chart recorder** rather than a dashboard, because that is what it is: it plays a
run at 20× virtual speed, draws three signal traces against a scripted timeline, and marks a fault window. Four rules
follow from that and are worth stating, since each one is the opposite of the web-dashboard default:

- **The plot field is recessed** — darker than the panels around it, the way a screen is inset into a chassis.
  Dashboards make charts *lighter* than the page. Getting this the right way round is most of why the page reads as an
  instrument, and it also stops three saturated traces from having to fight a bright ground.
- **Hairlines only.** No shadow, no gradient, no glow. Depth is three flat planes — chassis, panel, well — and one rule
  colour.
- **Every numeral is monospaced.** A figure that changes width as it counts is the one thing an instrument may not do.
  The corollary is that a value which is *not* a number (`still draining`) is deliberately not set as one.
- **The outage is hatched, not tinted.** This is load-bearing on the share chart, not decoration: the areas stack over
  the band, and a flat wash under three translucent fills just shifts every consumer colour inside the window — the
  reader sees three slightly-wrong colours rather than a marked region. Hatching also carries the right meaning, which
  is "no reading was taken here", not "a different value was read here".

Consumer colours are CSS custom properties rather than hex in `theme.ts`, because the page has a light theme too and a
trace tuned for a near-black field is washed out on a near-white one. Each consumer card carries its channel colour as
a left rail, which is what binds a card to its line in the two charts below without a legend lookup.

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
| **Concurrency count** | `COUNT(*) WHERE state='in_flight'` | Same Redis counter, incremented on lease and decremented on release or expiry |
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

**5. Lease expiry as the only failure detector.**
Reclamation triggers on `lease_expires_at < now()`, never on a worker being known dead.
*Cost:* a worker's work sits idle for up to a full lease TTL after it dies, even when the process registry already knows
it is gone. Reclamation is correct rather than prompt.
*Gain:* the correctness path has no failure detector to be wrong. Nothing in it reads the process registry, so a
partition, a paused heartbeat or a mis-tuned liveness window can cost throughput and can never cost a double send. The
optimization — reclaim a known-dead worker's leases immediately — is available on top and listed under Future Work, but
the timeout stays the source of truth underneath it.

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
*Gain:* it forced the production-correct shape everywhere — derived clock, sliding-window fairness, leases, leader
election. In a single process every one of those could have stayed an in-memory shortcut.

**8b. A kill button, in the product rather than in a test.**
`POST /api/process/{id}/kill` is a chaos control shipped in the UI, not a fixture.
*Cost:* a destructive operation on the process registry with no authentication in front of it, and a flag on a table
that is otherwise purely observational.
*Gain:* recovery is watched rather than described. It is also the only way to reach the state the reaper exists for —
an ungraceful death mid-batch — which means the recovery path is exercised on the real deployment rather than only
under `pytest`. Reclamation and the kill control were built together because neither is worth much without the other:
a reaper nobody can trigger is a claim, and a kill button with no reaper behind it is just damage.

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
| **Fast lease reclamation** | Use worker heartbeats to reclaim a known-dead worker's leases immediately instead of waiting out the TTL. Strictly an optimization on top of expiry — the timeout stays the source of truth, so a wrong failure detector costs throughput and never correctness |
| **Per-consumer lease duration** | The TTL is one global setting. A consumer that legitimately holds a connection for a minute and one that answers in 50ms want very different answers to "how long before we assume the worker is gone" |
| **Per-consumer retry policy** | Backoff base, cap, and max attempts are global today. Real consumers have different tolerances |
| **Payload filter expressions** | Subscribe to `invoice.paid` *where* `amount > 10000`, evaluated at the same dispatch-time seam as existing policies |
| **More coalesce strategies** | `batch_by_key` (deliver the run as an array), `first_by_key` |
| **Explicit replay window** | After an outage, let a consumer opt into "replay the last N minutes" rather than inheriting whatever staleness decided |
| **Idempotency contract** | `event.id` in a header plus documented consumer-side dedupe. Coalescing makes this *more* important: a consumer may see a later state than expected and never see the intermediate ones |
| **Dead-letter queue + manual replay** | `failed` deliveries just sit terminal today. Both sides want to see and retry them |
| **Consumer-facing dashboard** | The UI today is the provider's view, but the whole thesis is consumer experience |
| **Real HTTP delivery** | `HttpTransport` behind the existing seam, against a reviewer-supplied endpoint |
