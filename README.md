# webhook-recovery

A webhook delivery system built for **graceful recovery after a provider outage**, resting on two claims:

1. **Fair backlog burndown** — one consumer's backlog never slows another's.
2. **Consumer-defined policy** — the consumer decides what is worth replaying at all.

Design: [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) · Plan: [`PHASED_PLAN.md`](PHASED_PLAN.md) ·
Rationale: [`DESIGN_RATIONALE.md`](DESIGN_RATIONALE.md)

---

## Status: Phase 1 — the walking skeleton

**Deployed:** https://api-production-7c78a.up.railway.app

Events now flow end to end — **producer → ledger → fan-out → admission → worker → `delivered`** — across
five processes coordinating only through Postgres, with `pg_try_advisory_lock` leader election. Create a
simulation and its backlog climbs through the scripted outage and drains after it, on real data.

The scheduling is deliberately naive: **global FIFO under one rate budget, no fairness and no policy.**
That is not throwaway code — it is the `fair_drain = OFF` arm, which Phase 2 needs anyway as the thing
the toggle is measured against.

| | |
|---|---|
| **Built** | ingest + fan-out, the producer, admission control with the outage gate, derived metric buckets, leader election, `SKIP LOCKED` claim, the full attempt state machine (deliver / retry / fail), consumer seeding |
| **Deliberately absent** | weights and per-consumer shares, `max_staleness` → `expired`, coalescing → `superseded`, the fair-drain toggle, charts, lease reaping |

One run, backlog (`b`) and delivered (`d`) per consumer, at 20× — about 60 real seconds end to end:

```
t=  50s normal     Acme b=14   d=287   Bolt b=14   d=287   Clover b=2   d=39
t= 171s outage     Acme b=305  d=726   Bolt b=305  d=726   Clover b=43  d=97
t= 292s outage     Acme b=1034 d=726   Bolt b=1034 d=726   Clover b=141 d=97
t= 413s outage     Acme b=1751 d=726   Bolt b=1751 d=726   Clover b=235 d=97
t= 535s recovery   Acme b=1276 d=1940  Bolt b=1277 d=1939  Clover b=168 d=262
t= 656s recovery   Acme b=756  d=3204  Bolt b=756  d=3204  Clover b=99  d=429
t= 777s recovery   Acme b=204  d=4474  Bolt b=204  d=4474  Clover b=26  d=593
t= 899s recovery   Acme b=13   d=5408  Bolt b=13   d=5408  Clover b=2   d=719
t=1020s done       Acme b=0    d=5421  Bolt b=0    d=5421  Clover b=0   d=721
```

Deliveries stop entirely between 2:00 and 7:00 while events keep landing in the ledger — that is the one
`if` in the conductor pass, and it is what gives the rest of the project something to burn down. The
producer stops at 900 virtual seconds; the backlog reaches a stable zero and the run retires itself.

Acme and Bolt track each other exactly, because they subscribe to the same four event types and nothing
yet distinguishes them. Phase 2's policies are what pull those two lines apart — which is precisely why
the baseline is worth running first.

## Run it

```bash
make up                          # postgres -> migrate -> api + 2 conductors + 3 workers
open http://localhost:8000
curl -sX POST localhost:8000/api/simulation -H 'content-type: application/json' -d '{}'
```

All from one image. Postgres is published on host port **5433** (5432 is usually taken); override with
`POSTGRES_HOST_PORT`. `make help` lists everything; the ones you will actually use:

```bash
make install                     # uv sync + npm install
make db                          # postgres alone, migrations applied
make api                         # uvicorn on :8000 with reload
make worker / make conductor     # one of each, on the host
make web                         # vite on :5173, proxying /api to :8000
make psql                        # a shell on the compose database
make check                       # lint + typecheck + test
make fixtures                    # regenerate fixtures and openapi.json from the models
```

## Verify

With the stack up:

```bash
make check                       # ruff, mypy strict, pytest
make migration-check             # the migration still matches the models
make fixtures-check              # openapi.json and the fixtures are not stale
make verify                      # the exit criteria, as executable checks
```

`make verify` runs [`scripts/verify.sh`](scripts/verify.sh). It works against the deployment too:

```bash
EXPECTED_PROCESSES=4 ./scripts/verify.sh https://api-production-7c78a.up.railway.app
```

```
== health             api up, database reachable
== processes          5 live (2 conductors + 3 workers), exactly one leader
== schema             9 tables, 3 partial indexes on delivery
== clock              +20.16 virtual s per real second at 20x
                      frozen while paused; no jump across the pause
== delivery pipeline  3 consumers seeded at creation
                      1513 deliveries reached 'delivered'; decision feed populated
                      241 buckets (0..240) x 3 consumers, contiguous and complete
== served bundle      SPA index served; unknown /api path 404s
```

Two of those checks exist because the failures they catch are **invisible**, and both are in the metrics
path — the one component of this system that can lie convincingly. Everything else announces itself:
nothing gets delivered, or the backlog never drains, or a process crashes. A metrics bug produces a
chart that is smooth, plausible and wrong.

- **Contiguity.** A gap in `bucket_virtual_s` is a hole in the chart that a client cannot distinguish
  from a zero. Rows are written as the full consumer × bucket cross product so it never has to guess.
- **Counters are derived, not sampled.** A conductor pass covers `interval × speed` virtual seconds — a
  whole bucket at the shipped defaults — so writing "the current bucket's count" undercounts attempts by
  a roughly constant factor. On a *100% stacked* chart, an equal undercount across three consumers draws
  a picture that looks exactly right. So they come from two grouped queries over `attempt.started_at`
  and `delivery.completed_at` instead, and the invariant is exact:

  ```
  SUM(metrics_snapshot.attempts)  ==  COUNT(attempt)     -- over the written bucket range
  13001                           ==  13001
  ```

The clock is the check worth reading carefully. It is the only Phase 0 output that three separate
processes have to agree on, and a wrong answer there is invisible until Phase 2, where it presents as a
*fairness* bug rather than a clock bug.

[`scripts/check_clock.py`](scripts/check_clock.py) is deliberately **latency-aware**. The naive form of
this check — read, sleep a second, read, assert ~20 — only holds on localhost: every read is a round
trip, and at 20x a 265ms RTT is worth five virtual seconds, so it fails against a deployed URL on a
clock that is perfectly correct. It asserts the invariant that holds everywhere instead, timestamping
each sample at the midpoint of its request window:

```
virtual elapsed  ==  wall elapsed  x  speed_multiplier
```

Against Railway that lands within 0.1 virtual seconds — which is a much stronger statement about the
derived clock than the localhost version ever was.

## Layout

```
app/
  core/
    settings.py   tunables + the DATABASE_URL normalizer
    db.py         async engine, session scope, the conductor's dedicated-connection seam
    enums.py      TEXT + CHECK enums, backed by StrEnum
    models.py     all 9 tables and the partial indexes -- the single source of schema truth
    clock.py      Clock protocol, VirtualClock, WallClock, and the epoch arithmetic
    scenario.py   phase boundaries, the cast, the event mix, and seeding
    registry.py   self-registration + heartbeats
    runner.py     the shared entrypoint: register, heartbeat, loop, drain on SIGTERM
  api/
    routes.py     the read/write plane
    ingest.py     ledger write + fan-out, in one transaction
    producer.py   the demo traffic generator, in the process that owns ingest
    schemas.py    the frozen response models
  conductor/
    leader.py     pg_try_advisory_lock, held on the connection it writes through
    admission.py  the budget, the candidate query, the ready flip
    metrics.py    derived counters, sampled gauges, backfill on failover
    service.py    one pass: lead, measure, admit
  worker/
    transport.py  the ConsumerTransport seam: simulated, and an HTTP stub
    claim.py      SKIP LOCKED claim, lease, and the completion state machine
    service.py    one iteration: claim, gather, complete
alembic/          one migration
scripts/          gen_fixtures.py, verify.sh, check_clock.py, start-api.sh
frontend/         Vite + React stub, and the committed fixtures Track B builds against
```

## The frozen contract

`app/api/schemas.py` is the Phase 0 deliverable that matters most: freezing these shapes is what lets
the frontend and backend proceed in parallel. `frontend/src/fixtures/*.json` are generated **from those
models**, so a fixture cannot drift from the contract, and `openapi.json` is committed alongside.

```
POST   /api/simulation                                create (= reset), seeds consumers + policies
POST   /api/simulation/{id}/event                     ledger one event and fan it out
GET    /api/simulation/{id}                           config + current virtual time + phase
PATCH  /api/simulation/{id}                           pause/resume, speed, fair drain, outage override
GET    /api/simulation/{id}/consumer                  consumers with live counters
GET    /api/simulation/{id}/metrics?since_bucket=N    cursor page of chart buckets
GET    /api/simulation/{id}/decisions?limit=50        newest-first decision feed
GET    /api/process                                   live processes, 15s heartbeat filter
GET    /api/health
```

The fixtures are **outage-shaped, not placeholder-shaped**: three consumers, an outage at 2:00, recovery
at 7:00, with attempts split as equal thirds while all three are backlogged and Clover's segment
correctly falling to zero once it drains. A chart tuned against flat placeholder data looks wrong the
moment real data arrives.

```
Acme Analytics   delivered=3832  expired=0     superseded=0    caught up after 173s
Bolt Billing     delivered=2571  expired=737   superseded=517  caught up after  82s
Clover CRM       delivered=508   expired=0     superseded=0    caught up after  27s
```

Regenerate with `uv run python scripts/gen_fixtures.py`. The RNG is seeded, so the diff is empty unless
the shape genuinely changed.

## Decisions made in Phase 1

- **Leadership is the connection, not a flag.** `pg_try_advisory_lock` is session-scoped, and every
  conductor write goes through the connection the lock is held on — never `session_scope()`. So fencing
  is automatic rather than implemented: losing the lock and losing the ability to write are the *same
  event*, and there is no window in which a demoted leader can still write. A lock table with an expiry
  would need a fencing token on every write to be equally safe; this makes that class of bug
  unrepresentable. It is also why failover needs no failure detector — a killed process closes its
  socket, and Postgres drops the lock with it.
- **The bucket key comes from a fixed origin.** `sim.virtual_epoch` is *rebased* on every pause, resume
  and speed change — that is how the derived clock keeps virtual time continuous across them. Bucketing
  against it would renumber the whole series the first time anyone touched the speed slider: new rows
  collide with old ones through the upsert, and `?since_bucket=` stops being monotonic, which freezes
  the chart. Buckets key off `VIRTUAL_EPOCH_ZERO`, the same origin behind `virtual_now_s`.
- **The metrics cursor is read from the table, not from memory.** `MAX(bucket_virtual_s)` per
  simulation, cached in memory only as an optimisation. A new leader has no memory of the old one's
  progress, so deriving it is what makes failover *backfill* the gap rather than strand it — a
  memory-only cursor leaves a permanent hole in the chart at exactly the moment the demo is showing off
  failover. Capped at 300 buckets per pass so a long gap catches up over several passes.
- **Attempts are recorded at claim time, not completion.** The fairness window counts attempts
  *started*, and batching the insert into the claim transaction makes that free rather than an extra
  write. It also means admission has to subtract the outstanding `ready` buffer from its rate budget:
  work already admitted has no `attempt` row yet, so a pure window count would admit against the same
  budget twice. That subtraction is a read-modify-write — which is the actual reason the conductor must
  be a singleton, rather than tidiness.
- **The workers batch; the conductor does not deepen its buffer.** Per-attempt round trips do not
  survive 20×, so an iteration is one claim transaction, then every transport call concurrently, then
  one completion transaction. The claim commits *before* any transport runs, so no row lock is ever held
  across the network. And because the work stays inside a single `loop_body` call rather than escaping
  into background tasks, the runner's existing drain guarantee still covers it with no task bookkeeping.
- **The admission ceiling is the loop interval, not the buffer depth.** Available throughput is
  `ready buffer depth ÷ conductor_loop_interval_s`. The buffer must stay shallow — its depth *is* the
  granularity of fairness — so the interval is the only free variable: 0.05s gives ~720 attempts/s
  against ~600/s of demand at 20×. Raise it and the backlog stops draining, which presents as a
  scheduler bug rather than a tuning problem. Worth knowing which knob it is.
- **Consumer seeding moved from Phase 3 to here.** Not a choice — fan-out reads `subscription`, so a
  simulation with no consumers accepts events and delivers them to nobody. Policies came along for free
  and sit unread until Phase 2 evaluates them.
- **Finished runs retire themselves.** A conductor pass covers *every* running simulation, so one that
  nobody retires goes on costing throughput forever — and the cost lands on whichever run a reviewer is
  currently watching. Every visit to the deployment leaves another one behind, so it compounds. Also
  deployment-only: see below.
- **The transport is finished, not stubbed.** `SimulatedTransport` handles latency, jitter, failure
  rates and a `down` flag, and the worker handles every outcome — deliver, retry with backoff, fail at
  the cap. Seeded consumers have `sim_failure_rate = 0.0`, so Phase 1 still *observes* "always 200"; the
  state machine is simply complete. The RNG is seeded from `(simulation_id, delivery_id, attempt_no)`
  rather than carried as transport state, so an attempt's outcome does not depend on which worker picked
  it up or how the batch interleaved — otherwise every retry test is a coin flip.

## Decisions frozen in Phase 0

Recorded because they are expensive to reverse. Full reasoning in `TECHNICAL_DESIGN.md`.

- **The clock is derived, never stored.** Virtual time is wall time with an epoch and a multiplier,
  computed locally in every process from four fields on the `simulation` row. No coordination, no
  barrier. `Clock.sleep()` is the other half: 200 virtual ms at 20× is 10 real ms, so a worker genuinely
  holds its lease for the duration. Nothing outside `app/core/clock.py` reads the wall clock — enforced
  by a ruff rule, because it is unenforceable once there are fifty call sites.
- **Enums are `TEXT` + `CHECK`**, not native Postgres enums. `attempt.outcome` is a live candidate to
  gain `lease_expired`; `terminal_reason` is inherently open-ended. Adding a value to a native enum needs
  a migration, a `CHECK` needs an edit.
- **IDs are `bigint` except `simulation` and `process`, which are UUIDs** — the two things generated
  without a database round-trip, and which appear in URLs.
- **`/decisions` takes `?limit=`, not `?since_id=`.** `delivery.id` is assigned at *ingest*, not at
  completion, so a cursor over it would silently skip decisions. `/metrics?since_bucket=` keeps its
  cursor — `bucket_virtual_s` genuinely is monotonic.
- **Process liveness is a read-time filter, not a reaper.** Stale rows accumulate harmlessly and are
  never read, which keeps the design's claim — *nothing in the delivery path consults it* — literally
  true rather than approximately true.
- **Graceful SIGTERM drain.** Lease reaping is out of scope, so a worker dying mid-attempt strands
  `in_flight` rows. Finishing the current iteration on SIGTERM means the one shutdown path we control
  strands nothing.
- **Migrations run once, from a dedicated one-shot step.** Three services racing `alembic upgrade head`
  on boot can deadlock: compose gates on `service_completed_successfully`, Railway on a pre-deploy
  command on `api` only.

## Deploy (Railway)

One repo → one image → three services differing only by start command, sharing the Postgres plugin's
injected `DATABASE_URL`. `app/core/settings.py` normalizes what Railway injects — the `postgresql://`
scheme needs `+asyncpg`, and asyncpg rejects the `sslmode` parameter libpq-style URLs carry.

The topology is [`.railway/railway.ts`](.railway/railway.ts) — Infrastructure-as-Code, applied from the
CLI:

```bash
railway login                                   # browser-based
railway init --name webhook-recovery
railway add --database postgres
railway add --service api --variables 'DATABASE_URL=${{Postgres.DATABASE_URL}}'   # and conductor, worker

npm install                                     # the Railway IaC SDK (needs node >= 22)
railway config plan                             # preview
railway config apply --yes

railway up --service api --detach               # and conductor, worker
```

| Service | Command | Replicas |
|---|---|---|
| `api` | `alembic upgrade head && uvicorn app.api.main:app --host 0.0.0.0 --port $PORT` | 1 |
| `conductor` | `python -m app.conductor` | 2 — one leads, one stands by on the advisory lock |
| `worker` | `python -m app.worker` | 3 |

Then `make verify API=https://…` against the public URL.

**Why IaC rather than `railway.json`.** A root `railway.json` applies to *every* service in the project,
so it cannot give three services three different start commands, and the per-service config-file path is
a dashboard-only setting. IaC is the only form the CLI can apply. It is also the non-deprecated one —
config-as-code stops working 2026-12-01.

**Why migrations are in the api's start command.** The plan called for a Railway pre-deploy command on
`api` only. The IaC DSL has no `preDeployCommand` (Railway's own `config migrate` comments the field
out), and putting it in `railway.json` would apply it to all three services and reintroduce exactly the
race the one-shot step exists to prevent. Running it in the api's start command — with `api` pinned to a
single replica — keeps the guarantee that migrations run once from one place, and has the side benefit
of being platform-independent rather than a Railway feature.

### What deploying actually caught

None of these would have been distinguishable from a *business logic* bug had they surfaced in Phase 2.
This is the entire argument for deploying before there is anything interesting to deploy.

**Phase 1: abandoned simulations starve the live one.** Locally there is one simulation and everything
drains in 45 seconds. On the deployment, every `verify.sh` run and every reviewer visit leaves a
`running` simulation behind — and the conductor schedules *all* of them in a single pass, so each one
divides the admission throughput. It presented as a backlog that tracked its arrival rate exactly and
never drained: 12.4 attempts/virtual second against a budget of 30, with the ready buffer pinned at 4 of
its 36-slot target. That reads like a broken scheduler, and it is not — it is the admission-ceiling
invariant, arrived at from an unexpected direction.

The fix is the `done` transition the phase plan had put in Phase 3: a run retires once the script has
ended and its backlog is zero, plus a virtual-time backstop for a run that will never drain. Both terms
of that condition are load-bearing — retiring on an empty backlog alone *races the producer*, which at
20× commits another ~20 deliveries in the time one pass takes, so the run freezes with a small residue
that looks exactly like a failure to drain. So the producer stops at the end of the script, and only
then is the zero stable.

**Phase 0, all four invisible locally:**

| Problem | How it presented | Fix |
|---|---|---|
| Boot ordering is not guaranteed | Worker came up mid-migration and died inserting into a table that did not exist yet | The runner retries registration with capped backoff — also the right production behaviour when a database blinks |
| Railway's start-command parser does no POSIX expansion | `${PORT:-8000}` reached the process as that literal string; uvicorn exited on the unparseable port, silently | Everything shell-shaped moved into `scripts/start-api.sh`, which `docker run` can test |
| Railway silently re-detects the builder | Worker built with Railpack → `No interpreter found for Python ==3.12.*`, for a repo with a working Dockerfile at its root | Pinned `RAILWAY_DOCKERFILE_PATH` |
| Free tier caps replicas at 2 | Worker at 3 replicas rejected outright, with no logs at all | Railway runs 2; compose still runs 3. Workers are stateless, so this is a capacity difference and nothing else |

Compose gates the workers on the migrate step with `service_completed_successfully`; Railway starts
every service concurrently. Neither platform guarantees ordering, so the runner treats a missing schema
as a transient condition to wait out rather than a reason to exit.

## Next

**Phase 2** — the two claims. Fairness (weighted shares, concurrency caps and `max_attempts_per_s` from
the same sliding window, plus the `fair_drain_enabled` toggle) and policy (`max_staleness` → `expired`,
`coalesce: latest_by_key` → `superseded`). Both slot into seams this phase left open:
`select_candidates()` for fairness, and a policy check between it and `mark_ready()`.
