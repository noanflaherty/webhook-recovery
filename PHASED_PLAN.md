# Phased Plan

## Principles

1. **Walking skeleton first.** Get all three process types talking through Postgres and *deployed* with the dumbest
   possible logic, then add intelligence. Every hard problem in this project is a coordination problem; discovering a
   deployment or migration issue at hour seven is the worst possible outcome.
2. **Build the instrument before the thing it measures.** Fairness bugs are invisible in code and obvious in a chart. The
   UI moved ahead of the scheduler so that every scheduling change from that point on is watched rather than assumed —
   and so the naive run becomes a recorded "before" instead of a thing we describe from memory.
3. **Contract-first.** The API response shapes are frozen in Phase 0 — that single act is what lets frontend and backend
   proceed in parallel for the rest of the project.
4. **Submittable from Phase 1 onward, thesis complete at Phase 3.** No phase leaves the system broken, but be honest
   about what stopping early costs: stopping after Phase 2 ships a polished view of a system that doesn't yet make its
   argument. Phase 3 is where the project becomes the project.
5. **De-risk in order of blast radius**: deployment → the fairness claim landing visually → coordination bugs → polish.

## Overview

| Phase | Goal | Rough size | Cumulative |
|---|---|---|---|
| **0** | Foundations & frozen contracts | 1–1.5h | 1.5h |
| **1** | Walking skeleton + the cast, deployed | 1–1.5h | 3h |
| **2** | Instrument panel — UI live on the naive run | 1–1.5h | 4.5h |
| **3** | The two claims: fairness + policy — **minimum viable submission** | 1.5–2h | 6.5h |
| **4** | Polish + docs + video (**required deliverable**) | 1.5–2h | 8.5h |

**What changed from the original sequencing:** the UI was Phase 3b, behind fairness and policy. It is now Phase 2, in
front of them. Two things made that cheap. Phase 0 froze the contract *and* committed a fixture per endpoint, so the
frontend has never been blocked. Phase 1 pulled consumer/subscription seeding forward out of the old Phase 3a — fan-out
reads `subscription`, so the skeleton could not walk without it — which means that by the end of Phase 1 the real API
serves a real three-consumer outage-and-recovery run. There is a live system worth pointing a chart at a full phase
earlier than the plan assumed. The old Phase 3a is now almost entirely gone: seeding landed in Phase 1, and the
`global_attempts_per_s` tuning splits into a Phase 2 sanity check and the Phase 4 tuning pass.

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
            + leader election + the cast  │        (builds against      │
                                          │         frozen contract     │
                                          └──────────┐   + fixtures)    │
                                                     │                  │
   Phase 2  ┄┄ naive run is the data source ┄┄┄┄┄┄┄┄▶└──── charts ──────┤
                                                                        │
                                          ┌─────────────────────────────┘
                                          │  the charts are now the debugger
   Phase 3  fairness ║ policy ────────────┤
                                          │
   Phase 4  polish · docs · video · tuning
```

**Track C (infra)** runs alongside everything from Phase 0: Dockerfile, compose, Railway services, migrations on deploy.

The arrow that matters is the new one: Phase 2 consumes Phase 1's output, and Phase 3 is developed *through* Phase 2's
charts. The tracks stop being independent at Phase 2 — that is the cost of the swap, and it is worth paying once.

---

## Phase 0 — Foundations & Contracts

Blocking. Nothing else started until this landed.

- Repo scaffold: `uv`, ruff, mypy, `app/{api,conductor,worker,core}` layout
- **Full schema in one Alembic migration.** Cheaper to write the whole model once than to migrate five times — the
  design is settled enough.
- `Clock` protocol, `VirtualClock` (derived from the `simulation` row), `WallClock`
- Settings/config, DB session management
- Three entrypoints that boot, self-register in `process`, and heartbeat — no business logic
- Dockerfile + `docker-compose.yml` (PG + services)
- **Freeze the API contract**: response shapes for `/metrics`, `/decisions`, `/process`, `/simulation`. Written as
  Pydantic models *and* a static JSON fixture per endpoint.

**Exit:** `docker compose up` → migrations apply, processes register and heartbeat.
**Unlocked:** Track B, immediately — which is what made the resequencing below possible at all.

## Phase 1 — Walking Skeleton

End-to-end with deliberately stupid logic. **This is the highest-value phase per hour** because it retires the
deployment risk.

- Ingest: `POST /event` → ledger row + fan-out to `pending` in one transaction
- Producer background task in the API on the Stripe-flavoured event mix
- **The cast** — three consumers, subscriptions, Bolt's policies — seeded from a constants table. Pulled forward out of
  the old Phase 3a because fan-out reads `subscription` and a skeleton with no subscriptions cannot walk. Policies come
  along for free and sit unread until Phase 3.
- Conductor: naive admission — mark everything `ready` FIFO under one global budget, no fairness, no policy. Write
  `metrics_snapshot` rows.
- Conductor: `pg_try_advisory_lock` leader election, **held on the same session it writes through** so fencing is
  automatic. Cheap now, painful to retrofit once the loop exists.
- Worker: `SKIP LOCKED` claim → set lease fields → `SimulatedTransport` (fixed latency, always 200) → `delivered`
- **Deploy to Railway**: services from one image, managed Postgres, migrations on release

**Exit:** deployed URL where events flow producer → ledger → ready → delivered and `metrics_snapshot` accumulates over a
real normal → outage → recovery run.
**Risk retired:** the entire deploy topology.
**Unlocks:** Phase 2 has live data, not just fixtures.

## Phase 2 — Instrument Panel

The UI, pointed at Phase 1's naive run. Two jobs, and the second is the one that justifies the reordering:

1. It makes the project feel real, and produces the **"before" picture** — a recorded naive drain, where Acme's outage
   backlog starves Clover and hours-stale events get delivered anyway. The original plan never produced this artifact;
   it went straight to the toggle and asked a reviewer to imagine the alternative.
2. It becomes **the development instrument for Phase 3.** A subtly wrong scheduler still produces a plausible-looking
   chart, which is exactly why you want the chart in front of you while writing the scheduler rather than after.

**Build (time-boxed to the four instruments):**
- Control bar: play/pause, speed, reset, virtual clock + phase indicator. The **fair-drain toggle ships wired but
  inert** — the API field exists and `PATCH` accepts it; nothing reads it until Phase 3. Label it so a reviewer looking
  early isn't misled.
- Backlog-over-time chart — one line per consumer. The shape of recovery.
- Attempts-share 100% stacked bar. Under naive FIFO this is the *unfairness* exhibit: shares track raw volume, and
  Clover's sliver is the whole problem stated in one chart.
- Consumer cards: backlog, in-flight / cap, delivered / expired / superseded / failed. The policy counters sit at zero
  until Phase 3 — render them from the committed fixtures too, so the populated state is verified now rather than
  discovered at hour six.

**Two decisions to make here, both cheap now and annoying later:**
- **Key the UI off a `simulation_id` from the URL** (`?sim=<uuid>`), not "whatever the latest run is". The API is
  already parameterized that way, and simulations are `simulation_id`-namespaced and persist side by side — so the
  Phase 2 naive run stays permanently viewable after fairness lands. That is a before/after that survives independently
  of the toggle working.
- **Keep a fixtures/live source switch.** The committed fixtures are a complete post-Phase-3 run; the live API in
  Phase 2 is not. Developing against both means no UI surface is blocked on backend work that hasn't happened.

**Deliberately deferred to Phase 4:** decision feed, process strip, catch-up timers. They are polish, and polish here
eats the phase that carries the thesis.

**Exit:** the deployed URL shows a naive outage → recovery run, and the attempts-share chart visibly fails to be fair.
**Risk retired early:** *"the fair-drain toggle is a visible no-op."* The listed mitigation is tuning
`global_attempts_per_s` below `Σ max_attempts_per_s` so the provider is genuinely contended. Under the old ordering that
was only checkable at hour seven. Now you can look at the naive chart and see whether contention exists before writing a
line of the scheduler — if the backlog lines drain smoothly under naive FIFO, the knob is wrong and the whole demo would
have been flat.

## Phase 3 — The Two Claims

The actual product, and now the **minimum viable submission**. Two workstreams that parallelize *if* the seam is defined
first:

```python
def select_candidates(sim, budget) -> list[Delivery]:   # 3a — fairness: who, and how many
def evaluate_policy(delivery) -> Decision:              # 3b — policy: ready | expired | superseded
```

**3a — Fairness**
- Sliding-window attempt counts per consumer
- Weighted share, concurrency cap, `max_attempts_per_s` from the same window
- Ready-buffer depth control (~1–2× `Σ concurrency_cap`)
- `fair_drain_enabled` toggle + the FIFO/shared-pool fallback path — the Phase 1 admission loop *is* the fallback path,
  so this is a branch, not a rewrite
- Retry backoff

**3b — Policy**
- `max_staleness` → `expired`
- `coalesce: latest_by_key` → `superseded` (index covers `pending` *and* `ready`)
- Worker-side staleness re-check guard

**⚠️ Integration subtlety** — 3a and 3b aren't as independent as they look. A dropped candidate consumed a *candidate
slot* but not an *attempt*, and fairness is measured in attempts. If Bolt's coalescing kills 90% of its candidates, a
naive loop leaves Bolt under-using its share. So `select_candidates` must keep pulling for a consumer until its share is
filled with genuinely-`ready` deliveries or its pending pool is exhausted. Agree on this before splitting the work.

**Tests worth writing here** (and nowhere else): the charts catch gross breakage now, but a scheduler that is fair *on
average* while starving someone for four seconds looks fine at chart resolution. Assert that with equal weights and all
consumers backlogged, attempt shares converge to 1/3 ± ε. Same for coalesce and staleness, which are pure functions and
cheap to test. The instrument does not replace these — it tells you *where* to point them.

**Exit:** flipping the toggle produces measurably different per-consumer drain rates, watched live in the Phase 2
charts, and against the retained Phase 2 naive `simulation_id` as a permanent baseline.

## Phase 4 — Polish, Docs + Video

Required, not optional.

- The deferred UI surfaces, in cut-list order: decision feed, then process strip, then consumer-card catch-up timers
- Tuning pass on the scenario so the demo reads clearly on first watch
- Legend work on the attempts-share chart: a drained consumer's segment correctly goes to zero, and unless the legend
  says so it reads as unfairness
- README with the deployed link and a 60-second "what am I looking at"
- Update `DESIGN_RATIONALE.md`: the coalescing/event-log tradeoff, time spent, extensions
- ~5 min video. Worth 30 seconds naming what was scoped out and why — worker-failure recovery is designed and
  documented but not built, and saying so is stronger than hoping nobody asks

---

## Parallel Tracks

| Track | Owns | Blocked by | Notes |
|---|---|---|---|
| **A — Backend** | conductor, worker, ingest | Phase 0 schema + clock | The critical path in Phases 1 and 3. Within Phase 3, 3a and 3b split cleanly once the seam is agreed |
| **B — Frontend** | React app, charts, controls | Phase 0 **contract only** | Ran parallel through Phases 0–1 off fixtures; becomes the critical path in Phase 2, then a debugging surface in Phase 3 |
| **C — Infra** | Docker, Railway, migrations | Phase 0 entrypoint stubs | Front-loaded into Phase 1, then near-zero |

**What does *not* parallelize:** the conductor's admission loop is one coherent piece of reasoning. 3a/3b split only
because we define the function boundary first; splitting it further would cost more in merge friction than it saves.

**What the reorder costs:** Phases 1 and 2 no longer overlap the way 1 and 2 did under the old plan — Phase 2 wants
Phase 1's data. In exchange, Phase 3 gains a live instrument, which is the phase where being wrong is hardest to notice.

## Risk Register

| Risk | When it bites | Mitigation |
|---|---|---|
| Railway topology fights us | Late = fatal | Deploy in Phase 1, before any real logic exists |
| Fair-drain toggle is a visible no-op | ~~Phase 3 demo~~ **Phase 2, now visible** | The `global_attempts_per_s` ratio. Read it off the naive backlog chart before building the scheduler |
| Silent fairness bug | Never — that's the problem | The Phase 2 charts make gross breakage visible; unit-test share convergence in Phase 3 for the rest |
| **UI polish eats the Phase 3 budget** | **Phase 3, fatally** | **The new failure mode of this ordering. Phase 2 ships four instruments and stops; its exit criterion is an ugly honest chart, not a beautiful one** |
| Coalesce/fairness interaction starves a consumer | Phase 3 integration | Agree the candidate-refill rule before splitting 3a/3b |
| Scope overrun | Phase 4 | The cut list below, decided in advance rather than at hour 7 |

## Cut List

Ordered. Everything above the line goes before anything below it. Reordered for the new sequencing: the charts are no
longer a final-phase deliverable, they are the tool Phase 3 is built with, so they became much more expensive to cut —
and by the time cutting is on the table, they are already built.

1. Decision feed → keep counters on the consumer cards only
2. Process strip → the architecture is described in the docs instead of shown
3. Second conductor replica → keep leader election with one process; HA becomes claim, not fact
4. Consumer-card catch-up timers → the charts already tell the story
5. Chart polish — legends, axis labels, transitions → the shape carries the argument without them
6. — **the line.** Below this, cutting starts damaging the thesis —
7. `max_staleness` → coalescing alone carries the policy story
8. Fair-drain toggle → with the Phase 2 naive run retained by `simulation_id`, the before/after survives as two URLs
   rather than one switch. Weaker, but not fatal the way it used to be — which is a second, unplanned dividend of
   building the UI early
