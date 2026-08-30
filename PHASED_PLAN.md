# Phased Plan

## Principles

1. **Walking skeleton first.** Get all three process types talking through Postgres and *deployed* with the dumbest
   possible logic, then add intelligence. Every hard problem in this project is a coordination problem; discovering a
   deployment or migration issue at hour seven is the worst possible outcome.
2. **Submittable at the end of every phase from 1 onward.** No phase leaves the system broken. If we stop early, we
   still have something deployed that demonstrates a real idea.
3. **Contract-first.** The API response shapes are frozen in Phase 0 — that single act is what lets frontend and backend
   proceed in parallel for the rest of the project.
4. **De-risk in order of blast radius**: deployment → the fairness claim landing visually → coordination bugs → polish.

## Overview

| Phase | Goal | Rough size | Cumulative |
|---|---|---|---|
| **0** | Foundations & frozen contracts | 1–1.5h | 1.5h |
| **1** | Walking skeleton, deployed | 1–1.5h | 3h |
| **2** | The two claims: fairness + policy | 1.5–2h | 4.5–5h |
| **3** | Scenario + UI — **minimum viable submission** | 1.5–2h | 7h |
| **4** | Docs + video (**required deliverable**) | 1–1.5h | 8.5h |

Scoped out: **the worker-failure story** — both simulated process kills and lease reclamation, cut together since
neither is worth much without the other. Workers still stamp `leased_by` / `lease_expires_at`, so the reaper stays a
~20-line addition rather than a migration, but nothing reclaims an expired lease today: a worker dying mid-attempt
strands its `in_flight` rows and permanently costs that consumer capacity. Acceptable because `simulation_id`
namespacing confines the damage to one ~45-second run and Reset clears it. **Leader election survives the cut** — it is
~10 lines and answers the loudest architectural objection. See [Cut List](#cut-list) for what goes next.

## Dependency Graph

```
Phase 0 ─── schema + clock + contracts ───┬─────────────────────────────┐
                                          │                             │
                                    ┌─────▼─────┐                 ┌─────▼─────┐
                                    │ TRACK A   │                 │ TRACK B   │
                                    │ backend   │                 │ frontend  │
                                    └─────┬─────┘                 └─────┬─────┘
   Phase 1  ingest → conductor → worker ──┤                             │
            + leader election             │        (builds against      │
                                          │         frozen contract     │
   Phase 2  fairness ║ policy ────────────┤         + seeded fixtures)  │
                                          │                             │
   Phase 3  scenario ─────────────────────┴──────────── charts ─────────┘
                                          │
   Phase 4  docs · video · tuning pass
```

**Track C (infra)** runs alongside everything from Phase 0: Dockerfile, compose, Railway services, migrations on deploy.

---

## Phase 0 — Foundations & Contracts

Blocking. Nothing else starts until this lands.

- Repo scaffold: `uv`, ruff, mypy, `app/{api,conductor,worker,core}` layout
- **Full schema in one Alembic migration.** Cheaper to write the whole model once than to migrate five times — the
  design is settled enough.
- `Clock` protocol, `VirtualClock` (derived from the `simulation` row), `WallClock`
- Settings/config, DB session management
- Three entrypoints that boot, self-register in `process`, and heartbeat — no business logic
- Dockerfile + `docker-compose.yml` (PG + services)
- **Freeze the API contract**: response shapes for `/metrics`, `/decisions`, `/process`, `/simulation`. Write them as
  Pydantic models *and* commit a static JSON fixture per endpoint.

**Exit:** `docker compose up` → migrations apply, processes register and heartbeat.
**Unlocks:** Track B can begin immediately against the JSON fixtures.

## Phase 1 — Walking Skeleton

End-to-end with deliberately stupid logic. **This is the highest-value phase per hour** because it retires the
deployment risk.

- Ingest: `POST /event` → ledger row + fan-out to `pending` in one transaction
- Producer background task in the API on the Stripe-flavoured event mix
- Conductor: naive admission — mark everything `ready` FIFO, no fairness, no policy. Write `metrics_snapshot` rows.
- Conductor: `pg_try_advisory_lock` leader election, **held on the same session it writes through** so fencing is
  automatic. Cheap now, painful to retrofit once the loop exists.
- Worker: `SKIP LOCKED` claim → set lease fields → `SimulatedTransport` (fixed latency, always 200) → `delivered`
- **Deploy to Railway**: services from one image, managed Postgres, migrations on release

**Exit:** deployed URL where events flow producer → ledger → ready → delivered and `metrics_snapshot` accumulates.
**Risk retired:** the entire deploy topology.

## Phase 2 — The Two Claims

The actual product. Two workstreams that parallelize *if* the seam is defined first:

```python
def select_candidates(sim, budget) -> list[Delivery]:   # 2a — fairness: who, and how many
def evaluate_policy(delivery) -> Decision:              # 2b — policy: ready | expired | superseded
```

**2a — Fairness**
- Sliding-window attempt counts per consumer
- Weighted share, concurrency cap, `max_attempts_per_s` from the same window
- Ready-buffer depth control (~1–2× `Σ concurrency_cap`)
- `fair_drain_enabled` toggle + the FIFO/shared-pool fallback path
- Retry backoff

**2b — Policy**
- `max_staleness` → `expired`
- `coalesce: latest_by_key` → `superseded` (index covers `pending` *and* `ready`)
- Worker-side staleness re-check guard

**⚠️ Integration subtlety** — 2a and 2b aren't as independent as they look. A dropped candidate consumed a *candidate
slot* but not an *attempt*, and fairness is measured in attempts. If Bolt's coalescing kills 90% of its candidates, a
naive loop leaves Bolt under-using its share. So `select_candidates` must keep pulling for a consumer until its share is
filled with genuinely-`ready` deliveries or its pending pool is exhausted. Agree on this before splitting the work.

**Tests worth writing here** (and nowhere else): fairness bugs are *invisible* — a subtly wrong scheduler still produces
a plausible-looking chart. Assert that with equal weights and all consumers backlogged, attempt shares converge to 1/3 ± ε.
Same for coalesce and staleness, which are pure functions and cheap to test.

**Exit:** flipping the toggle produces measurably different per-consumer drain rates.

## Phase 3 — Scenario + UI

**Minimum viable submission ends here.**

**3a — Scenario engine**
- Seed the three consumers, subscriptions, policies
- Phases derived from virtual time (normal → outage → recovery), outage override
- Tune `global_attempts_per_s` **below** `Σ max_attempts_per_s` so the provider is genuinely contended — otherwise the
  fair-drain toggle is a visible no-op

**3b — UI** (has been in progress since Phase 0 against fixtures)
- Control bar: play/pause, speed, fair-drain toggle, reset, virtual clock + phase
- Backlog-over-time chart
- Attempts-share 100% stacked bar — the fairness proof. Legend must explain that a drained consumer's segment correctly
  goes to zero, or it reads as unfairness.
- Consumer cards with time-to-catch-up
- Process strip (read-only): workers and conductors with the leader marked, so the multi-process architecture is visible
  rather than claimed

**Exit:** a reviewer lands on the URL, hits play, and watches outage → recovery twice with the toggle flipped.

## Phase 4 — Docs + Video

Required, not optional.

- README with the deployed link and a 60-second "what am I looking at"
- Update `DESIGN_RATIONALE.md`: the coalescing/event-log tradeoff, time spent, extensions
- Tuning pass on the scenario so the demo reads clearly on first watch
- ~5 min video. Worth 30 seconds naming what was scoped out and why — worker-failure recovery is designed and
  documented but not built, and saying so is stronger than hoping nobody asks

---

## Parallel Tracks

| Track | Owns | Blocked by | Notes |
|---|---|---|---|
| **A — Backend** | conductor, worker, ingest | Phase 0 schema + clock | The critical path. Within Phase 2, 2a and 2b split cleanly once the seam is agreed |
| **B — Frontend** | React app, charts, controls | Phase 0 **contract only** | Longest genuinely-parallel stretch. Works off JSON fixtures until Phase 3 |
| **C — Infra** | Docker, Railway, migrations | Phase 0 entrypoint stubs | Front-loaded into Phase 1, then near-zero |

**What does *not* parallelize:** the conductor's admission loop is one coherent piece of reasoning. 2a/2b split only
because we define the function boundary first; splitting it further would cost more in merge friction than it saves.

## Risk Register

| Risk | When it bites | Mitigation |
|---|---|---|
| Railway topology fights us | Late = fatal | Deploy in Phase 1, before any real logic exists |
| Fair-drain toggle is a visible no-op | Phase 3 demo | The `global_attempts_per_s` ratio. First thing to check if the chart looks flat |
| Silent fairness bug | Never — that's the problem | Unit-test share convergence in Phase 2 |
| Coalesce/fairness interaction starves a consumer | Phase 2 integration | Agree the candidate-refill rule before splitting 2a/2b |
| Scope overrun | Phase 3 | The cut list below, decided in advance rather than at hour 7 |

## Cut List

Ordered. Everything above the line goes before anything below it.

1. Decision feed → keep counters on the consumer cards only
2. Process strip → the architecture is described in the docs instead of shown
3. Second conductor replica → keep leader election with one process; HA becomes claim, not fact
4. Consumer-card catch-up timers → the charts already tell the story
5. — **the line.** Below this, cutting starts damaging the thesis —
6. `max_staleness` → coalescing alone carries the policy story
7. Fair-drain toggle → without the comparison there is no proof, only an assertion
