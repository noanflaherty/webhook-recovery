# webhook-recovery

A webhook delivery system built for **graceful recovery after a provider outage**, resting on two claims:

1. **Fair backlog burndown** — one consumer's backlog never slows another's.
2. **Consumer-defined policy** — the consumer decides what is worth replaying at all.

Design: [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) · Plan: [`PHASED_PLAN.md`](PHASED_PLAN.md) ·
Rationale: [`DESIGN_RATIONALE.md`](DESIGN_RATIONALE.md)

---

## Status: Phase 0 — foundations and frozen contracts

**Deployed:** https://api-production-7c78a.up.railway.app

The schema, the clock, the API response shapes and the deployment topology exist. **No business logic
does**: the conductor and worker loops are empty on purpose. Phase 0 builds the parts that are expensive
to change and nothing that is interesting to demo.

| | |
|---|---|
| **Built** | 9-table schema in one migration, the virtual clock, simulation lifecycle API, process registry, one image / three start commands, committed fixtures |
| **Deliberately empty** | producer, fan-out, admission control, policy evaluation, transport, leader election, charts |

## Run it

```bash
make up                          # postgres -> migrate -> api + conductor + 3 workers
open http://localhost:8000
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
make verify                      # health, processes, schema, clock, served bundle
```

`make verify` runs [`scripts/verify.sh`](scripts/verify.sh), which is the Phase 0 exit criteria as
executable checks. It works against the deployment too:

```bash
EXPECTED_PROCESSES=3 ./scripts/verify.sh https://api-production-7c78a.up.railway.app
```

```
== health          api up, database reachable
== processes       4 live (1 conductor + 3 workers), all inside the 15s window
== schema          9 tables, 3 partial indexes on delivery
== clock           +20.92 virtual s per real second at 20x
                   frozen while paused; +0.26s jump on resume
== served bundle   SPA index served; unknown /api path 404s
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
    scenario.py   phase boundaries (the scenario engine itself is Phase 3)
    registry.py   self-registration + heartbeats
    runner.py     the shared entrypoint: register, heartbeat, loop, drain on SIGTERM
  api/            FastAPI app, the frozen response models, routes
  conductor/      empty loop (Phase 1: leader election, admission, metrics)
  worker/         empty loop (Phase 1: SKIP LOCKED claim, lease, attempt)
alembic/          one migration
scripts/          gen_fixtures.py, verify.sh, check_clock.py, start-api.sh, deploy_railway.sh
frontend/         Vite + React stub, and the committed fixtures Track B builds against
```

## The frozen contract

`app/api/schemas.py` is the Phase 0 deliverable that matters most: freezing these shapes is what lets
the frontend and backend proceed in parallel. `frontend/src/fixtures/*.json` are generated **from those
models**, so a fixture cannot drift from the contract, and `openapi.json` is committed alongside.

```
POST   /api/simulation                                create (Phase 3 also seeds consumers)
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
| `conductor` | `python -m app.conductor` | 1 → 2 once leader election lands in Phase 1 |
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

### What deploying the skeleton actually caught

All four were invisible locally, and none of them would have been distinguishable from a *business
logic* bug had they surfaced in Phase 2. This is the entire argument for deploying before there is
anything interesting to deploy.

| Problem | How it presented | Fix |
|---|---|---|
| Boot ordering is not guaranteed | Worker came up mid-migration and died inserting into a table that did not exist yet | The runner retries registration with capped backoff — also the right production behaviour when a database blinks |
| Railway's start-command parser does no POSIX expansion | `${PORT:-8000}` reached the process as that literal string; uvicorn exited on the unparseable port, silently | Everything shell-shaped moved into `scripts/start-api.sh`, which `docker run` can test |
| Railway silently re-detects the builder | Worker built with Railpack → `No interpreter found for Python ==3.12.*`, for a repo with a working Dockerfile at its root | Pinned `RAILWAY_DOCKERFILE_PATH` |
| Free tier caps replicas at 2 | Worker at 3 replicas rejected outright, with no logs at all | Railway runs 2; compose still runs 3. Workers are stateless, so this is a capacity difference and nothing else |

Compose gates the workers on the migrate step with `service_completed_successfully`; Railway starts
every service concurrently. Neither platform guarantees ordering, so the runner treats a missing schema
as a transient condition to wait out rather than a reason to exit.

## CI/CD

[`.github/workflows/ci.yml`](.github/workflows/ci.yml). Four jobs; the first three run on every pull
request and push, the fourth only on `main`.

| Job | What it protects |
|---|---|
| **backend** | ruff, `mypy --strict`, and pytest against a **real Postgres service** — without one, `tests/test_db_fixture.py` skips itself and CI silently stops covering the transactional fixture Phase 2's scheduler tests are built on. Also `alembic check`, so a model edit without a migration fails here rather than at the next deploy, and a fixture-staleness check, so the frozen contract cannot drift out from under the frontend track. |
| **frontend** | `tsc -b && vite build` |
| **image** | Builds the Dockerfile, then **boots it**: starts Postgres, runs the real `scripts/start-api.sh`, waits for `/api/health`, and checks a worker registers. A green `docker build` only says the image compiles. This job exists because the Dockerfile is what actually ships and it has broken twice in ways nothing else caught — a missing `README.md` that hatchling needed, and a builder that silently was not the Dockerfile at all. |
| **deploy** | `main` only, gated on the other three. Deploys api → conductor → worker via [`scripts/deploy_railway.sh`](scripts/deploy_railway.sh), then runs the same `verify.sh` against the public URL. |

`railway up --detach` returns before a deployment is healthy and **exits 0 even when the deployment
then fails**, which in CI is indistinguishable from success. `deploy_railway.sh` polls to a terminal
state and dumps build and deploy logs on anything but `SUCCESS`.

### One-time setup

The deploy job is inert until a token exists:

1. **`RAILWAY_TOKEN`** — Settings → Secrets and variables → Actions → *New repository secret*. Create
   the token in Railway under the project's Settings → Tokens, scoped to the `production` environment.
2. **`RAILWAY_PUBLIC_URL`** *(optional)* — a repository **variable** (not a secret), e.g.
   `https://api-production-7c78a.up.railway.app`. Present, the deploy runs `verify.sh` against it;
   absent, that step is skipped.
3. The job targets a `production` **environment**, so required reviewers or a deployment branch rule
   can be added from Settings → Environments without editing the workflow.

Each `railway up` triggers its own image build, so a merge to `main` costs three Railway builds. If
that is too much on the free tier, delete the `conductor` and `worker` steps — they only need
redeploying when their code changes — or drop the `deploy` job entirely and connect the services to
GitHub directly with `railway service source connect --repo <owner>/<repo> --branch main`, which moves
CD to Railway's own integration and out of Actions.

## Next

**Phase 1** — the walking skeleton: ingest → fan-out → naive conductor → worker → `delivered`, plus
`pg_try_advisory_lock` leader election held on the same session the conductor writes through.
