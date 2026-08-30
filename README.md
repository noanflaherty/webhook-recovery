# webhook-recovery

A webhook delivery system built for **graceful recovery after a provider outage**, resting on two claims:

1. **Fair backlog burndown** — one consumer's backlog never slows another's.
2. **Consumer-defined policy** — the consumer decides what is worth replaying at all.

Design: [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) · Rationale: [`DESIGN_RATIONALE.md`](DESIGN_RATIONALE.md) ·
Plan: [`PHASED_PLAN.md`](PHASED_PLAN.md)

---

## What you are looking at

**Live:** https://api-production-7c78a.up.railway.app

A simulated provider emits events to three consumers, goes dark for five minutes, and comes back. The
whole thing runs on a virtual clock at 20×, so a fifteen-minute incident plays out in about a minute of
real time, and the browser draws it live.

<!-- screenshot of a live run goes here -->

| Virtual time | What happens |
|---|---|
| 0:00 – 2:00 | **normal** — events flow, backlogs stay near zero |
| 2:00 – 7:00 | **outage** — the provider keeps emitting and the ledger keeps accepting; only *delivery* stops, so backlogs climb |
| 7:00 – 15:00 | **recovery** — the backlog burns down while fresh traffic is still arriving |
| 15:00 | the producer stops, the backlog reaches a stable zero, and the run retires itself |

Three consumers, each demonstrating exactly one thing:

| Consumer | Subscribes to | Demonstrates |
|---|---|---|
| **Acme Analytics** | all four event types | The baseline. No policies, so its entire backlog has to be delivered — what a naive consumer suffers on recovery. |
| **Bolt Billing** | all four event types | The second claim. Same stream as Acme, but its policies shrink the backlog *before* anything is sent. |
| **Clover CRM** | `invoice.paid` only | The first claim. A small backlog that should not have to queue behind the other two. |

**What to touch.** Start a run, then flip **Fair drain** mid-outage-recovery: the attempts-share chart
changes slope within a tick, and Clover's segment goes from a sliver to an equal third. **Pause**,
**speed** and **Force outage** are the other three knobs. If the backend is asleep, *view the recorded
run* serves a committed fixture of a real run against a local clock — no backend needed.

A representative fair run, with policy on:

```
Acme Analytics   delivered=5408  expired=0    superseded=0
Bolt Billing     delivered=3823  expired=581  superseded=1004
Clover CRM       delivered=707   expired=0    superseded=0
```

Acme and Bolt see the identical event stream. Bolt delivered 1,585 fewer events because it said, in
advance, which ones it did not want — and every payment still landed.

## The two claims, as behaviour

- **Fair backlog burn-down.** The conductor admits by weighted round-robin across dispatchable
  consumers, work-conserving, with `concurrency_cap` and `max_attempts_per_s` enforced from the same
  sliding window over `attempt`. `fair_drain_enabled` is re-read every pass, so flipping it mid-run
  changes the slope of the attempt-share chart within a tick.
- **Consumer-defined policy.** `max_staleness_s` → `expired` and `coalesce: latest_by_key` →
  `superseded`, evaluated at dispatch time — because whether an event is stale depends on when the
  pipeline recovers, and whether it has been superseded depends on what queued up behind it. Neither is
  knowable at ingest.

**Deliberately absent:** lease reaping, per-consumer retry policy, and `batch_by_key` coalescing. See
[`DESIGN_RATIONALE.md`](DESIGN_RATIONALE.md) for what that costs.

## How it works

Three process types, one image, one codebase. They share no memory, no queue and no message bus —
**Postgres is the only thing they coordinate through.**

```
     producer  (a task inside the api process)
                          │
  ┌──────────────────────────────────────────────┐
  │ api                                          │
  │   ledger write + fan-out, in one txn         │
  │   serves the SPA and the data behind it      │
  └──────────────────────────────────────────────┘
                          │
  ┌──────────────────────────────────────────────┐
  │ postgres  —  the only shared state           │
  │   + pg_try_advisory_lock (leader election)   │
  └──────────────────────────────────────────────┘
           ▲                              ▲
           │                              │
     ┌───────────────────────────┐  ┌───────────────────────────────┐
     │ conductor  ×2             │  │ worker  ×N                    │
     │                           │  │                               │
     │ one leads, one stands     │  │ SKIP LOCKED claim, lease,     │
     │ by on the advisory lock   │  │ deliver, complete             │
     │                           │  │                               │
     │ policy → fairness →       │  │ stateless; scale freely       │
     │ admission                 │  └───────────────────────────────┘
     └───────────────────────────┘
```

A delivery moves `pending → ready → in_flight → delivered`, or terminates at `expired`, `superseded` or
`failed`. The interesting state is **`ready`**: it is an *admission-control token materialized as a row
state* — the conductor has decided this specific delivery may be attempted now. The buffer is kept
shallow, because its depth is the granularity of fairness.

One conductor pass, per running simulation:

1. **Measure** — write the metric buckets for the virtual seconds that have elapsed.
2. **Gate** — if the simulation is in its outage, admit nothing and stop here.
3. **Select** — pull candidate deliveries, evaluate each consumer's policy against them (drops are
   recorded as `expired`/`superseded` and never reach a worker), then ration what survives across
   consumers by weight.
4. **Admit** — flip the survivors to `ready`.

Policy runs *inside* candidate selection rather than before it, because a policy drop consumes a
candidate slot but not an *attempt* — and fairness is measured in attempts. Rationing candidates
instead would hand a policy-heavy consumer its share, watch policy eat almost all of it, and starve it
with a scheduler that was working correctly.

Conductor failure degrades throughput to zero and then fully resumes: acceptance never depends on
scheduling, so events keep landing in the ledger while no conductor holds the lock. Full reasoning in
[`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md).

## Run it

```bash
make up                          # postgres -> migrate -> api + 2 conductors + 3 workers
open http://localhost:8000
```

All from one image; the api serves the SPA as well as the API. Postgres is published on host port
**5433** (5432 is usually taken); override with `POSTGRES_HOST_PORT`, and the api's with
`API_HOST_PORT`.

`make help` lists everything. The ones you will actually use:

```bash
make install                     # uv sync + npm install
make db                          # postgres alone, migrations applied
make api                         # uvicorn on :8000 with reload
make worker / make conductor     # one of each, on the host
make web                         # vite on :5173, proxying /api to :8000
make psql                        # a shell on the compose database
make check                       # lint + typecheck + test, backend and frontend
make fixtures                    # regenerate fixtures and openapi.json from the models
```

## Verify

```bash
make check                       # ruff, mypy --strict, pytest, frontend lint/build/test
make migration-check             # the migration still matches the models
make fixtures-check              # openapi.json and the fixtures are not stale
make verify                      # health, processes, schema, clock, pipeline, bundle
```

`make verify` runs [`scripts/verify.sh`](scripts/verify.sh) against a *running* stack, and starts its
own throwaway simulations to do it. Against compose:

```
== health
   {"status":"ok","db":"ok"}
   OK  api up
   OK  database reachable

== processes
   conductor f9ead236950f   pid 1       2.0s ago  (leader)
   conductor 0f6e4af65bfe   pid 1       1.9s ago
   worker    7c5ee66fef56   pid 1       2.0s ago
   worker    cc2161b38318   pid 1       1.9s ago
   worker    d60dc15471bb   pid 1       1.7s ago
   OK  2 conductors + 3 workers live
   OK  exactly one leader
   OK  all heartbeats inside the 15s window

== schema
   OK  9 tables
   OK  3 partial indexes on delivery

== clock
   20.19 virtual s over 1.020 real s (expected ~20.39)
   OK  virtual time advances at 20x
   OK  frozen while paused
   OK  no jump across the pause

== delivery pipeline
   OK  3 consumers seeded at creation
   OK  1498 deliveries reached 'delivered'
   OK  decision feed is populated
   243 buckets (0..242) x 3 consumers, 1498 attempts
   OK  metric buckets are contiguous and complete

== served bundle
   OK  SPA index served
   OK  unknown /api path 404s rather than serving the shell
```

It works against the deployment too. The replica counts differ, so pass the expected ones — and the
schema section is skipped, since there is no direct psql route to a Railway database:

```bash
EXPECTED_WORKERS=2 ./scripts/verify.sh https://api-production-7c78a.up.railway.app
```

The clock and metrics checks are the ones worth reading: they are the two places this system can fail
*silently*, producing a chart that is smooth, plausible and wrong. Everything else announces itself.
[`scripts/check_clock.py`](scripts/check_clock.py) is latency-aware on purpose — the naive form of the
check only holds on localhost. See [`DESIGN_RATIONALE.md`](DESIGN_RATIONALE.md) for why the metrics
counters are derived rather than sampled.

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
    main.py       the FastAPI app, the producer's lifespan, and the SPA mount
    routes.py     the read/write plane
    ingest.py     ledger write + fan-out, in one transaction
    producer.py   the traffic generator, in the process that owns ingest
    schemas.py    the frozen response models
  conductor/
    leader.py     pg_try_advisory_lock, held on the connection it writes through
    policy.py     max_staleness_s and latest_by_key, evaluated at dispatch
    admission.py  the budget, the candidate query, the fair allocation, the ready flip
    metrics.py    derived counters, sampled gauges, backfill on failover
    service.py    one pass: lead, measure, admit
  worker/
    transport.py  the ConsumerTransport seam: simulated, and an HTTP stub
    claim.py      SKIP LOCKED claim, lease, and the completion state machine
    service.py    one iteration: claim, gather, complete
frontend/src/
  App.tsx         layout, run identity in the URL
  api/            the DataSource seam: live (polling) and replay (fixtures)
  hooks/useRun.ts all polling and derived state, in one place
  transform/      API rows -> chart series
  components/     the two charts, consumer cards, decision feed, process strip, controls
  fixtures/       a complete recorded run, generated from the Pydantic models
alembic/          one migration
scripts/          gen_fixtures.py, verify.sh, check_clock.py, start-api.sh, deploy_railway.sh
```

## The API contract

`app/api/schemas.py` holds the response models. `frontend/src/fixtures/*.json` are generated **from
those models**, so a fixture cannot drift from the contract, and `openapi.json` is committed alongside
as the generated witness of it. `make fixtures-check` fails if either is stale.

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

## Deploy (Railway)

One repo → one image → three services differing only by start command, sharing the Postgres plugin's
injected `DATABASE_URL`. `app/core/settings.py` normalizes what Railway injects — the `postgresql://`
scheme needs `+asyncpg`, and asyncpg rejects the `sslmode` parameter libpq-style URLs carry.

| Service | Command | Replicas |
|---|---|---|
| `api` | `alembic upgrade head && uvicorn app.api.main:app --host 0.0.0.0 --port $PORT` | 1 |
| `conductor` | `python -m app.conductor` | 2 — one leads, one stands by on the advisory lock |
| `worker` | `python -m app.worker` | 2 — the free tier's ceiling; compose runs 3 |

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

**Why IaC rather than `railway.json`.** A root `railway.json` applies to *every* service in the project,
so it cannot give three services three different start commands, and the per-service config-file path is
a dashboard-only setting. IaC is the only form the CLI can apply, and the non-deprecated one —
config-as-code stops working 2026-12-01.

**Why migrations run from the api's start command.** The IaC DSL has no `preDeployCommand`, and putting
one in `railway.json` would apply it to all three services and reintroduce the boot race the one-shot
step exists to prevent. Running it in the api's start command — with `api` pinned to a single replica —
keeps migrations running once from one place, and is platform-independent rather than a Railway feature.

## CI/CD

[`.github/workflows/ci.yml`](.github/workflows/ci.yml). Four jobs; the first three run on every pull
request and push, the fourth only on `main`.

| Job | What it protects |
|---|---|
| **backend** | ruff, `mypy --strict`, and pytest against a **real Postgres service** — without one, `tests/test_db_fixture.py` skips itself and CI silently stops covering the transactional fixture every scheduler test is built on. Also `alembic check`, so a model edit without a migration fails here rather than at the next deploy, and a fixture-staleness check, so the contract cannot drift out from under the frontend. |
| **bundle** | `npm run build`, which is `tsc -b && vite build` — so the frontend type-checks here too. `oxlint` and the `transform/` unit tests run locally through `make check`, not in CI. |
| **image** | Builds the Dockerfile, then **boots it**: starts Postgres, runs the real `scripts/start-api.sh`, waits for `/api/health`, and checks a worker registers. A green `docker build` only says the image compiles, and the Dockerfile is what actually ships. |
| **deploy** | `main` only, gated on the other three. Deploys api → conductor → worker via [`scripts/deploy_railway.sh`](scripts/deploy_railway.sh), then runs `verify.sh` against the public URL. |

`railway up --detach` returns before a deployment is healthy and **exits 0 even when the deployment
then fails**, which in CI is indistinguishable from success. `deploy_railway.sh` polls to a terminal
state and dumps build and deploy logs on anything but `SUCCESS`.

**One-time setup.** The deploy job is inert until a token exists:

1. **`RAILWAY_TOKEN`** — Settings → Secrets and variables → Actions → *New repository secret*. Create
   the token in Railway under the project's Settings → Tokens, scoped to the `production` environment.
2. **`RAILWAY_PUBLIC_URL`** *(optional)* — a repository **variable** (not a secret). Present, the deploy
   runs `verify.sh` against it; absent, that step is skipped.
3. The job targets a `production` **environment**, so required reviewers or a deployment branch rule can
   be added from Settings → Environments without editing the workflow.

Each `railway up` triggers its own image build, so a merge to `main` costs three Railway builds. To cut
that, drop the `conductor` and `worker` steps — they only need redeploying when their code changes.
