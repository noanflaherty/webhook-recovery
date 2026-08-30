# Design Rationale

## Intro

I chose to build within Theme 3: Systems & Reliability because I believe it best showcases the types of technical
challenges I enjoy solving and the types of products I enjoy building.

I'm a backend-leaning product engineer that enjoys creating platforms as products. Some of the most fun I had at Vellum
was building out our enterprise AI Development platform, which other software companies used as the backend that powers
the AI features within their own end-user-facing products.

There's something about creating a platform that others build on top of and come to rely on that gives me fulfillment –
it often comes with a tight feedback loop with the end user, the responsibility of ensuring it works well, and the need
to evolve it as requirements and the market changes.

## What I built

I created a webhook delivery system designed for graceful recovery. If there is an outage on either the producer or
consumer side, then, upon recovery, not only is the backlog of events replayed from producer to consumer, but also:

1. The backlog is burned down fairly between consumers such that one consumer with a large backlog does not impact the
   burndown of another consumer's backlog; and
2. Consumers can define policies around which events are worth replaying in the first place

The end goal: Improve the developer experience for consumers of webhooks, and the reliability of the systems they build
to process webhook events, following a provider outage.

### Why I build it

Nearly every company I've worked at has integrated with vendor webhooks and provided webhooks as an integration point of
our own. As both someone who had built webhooks and consumed them, I often found myself dissatisfied with the
surrounding tooling.

As a customer of webhooks, I was always frustrated when my vendor had an outage and, upon recovery, my services were
bombarded with delayed, replayed events, many of which I no longer cared about. This backlog of stale events clogged up
my queues and could take hours to churn through before the queues reach manageable sizes again and could begin
processing fresh events once more.

As a developer of webhooks, I was always surprised by how much code it took to build the same thing again and again in
house. More recently, off-the-shelf products like Svix and Hookdeck have come out and help with many of the basics, but
I still haven't seen any of these go deep into improving the lives of consumers following a provider outage.

### What's unique

Many home-grown webhook solutions and webhook infra products offer event retries and replays following provider/consumer
outages, but none that I've seen optimize for the webhook consumer's experience following an outage. Sure, the outage is
bad – you're missing live data – but the hours following the outage can sometimes be worse: clogged up queues, delays
before you can process fresh events again, overwhelmed workers, etc.

This is a webhook infra system that puts the consumer first.

## Key decisions and what they cost

The full list, with the alternatives I weighed, is in
[`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) §Key Design Decisions. These are the ones I'd defend in a
review.

**Policies are evaluated at dispatch, not at ingest.** Whether an event is stale depends on when the
pipeline recovers. Whether it's been superseded depends on what queued up behind it. Neither is knowable
when the event arrives, so a system that filters at ingest can't make either decision. This is the whole
thesis, and everything else in the design is downstream of it. The cost is that every queued delivery
has to be re-examined on the way out, which is why policy evaluation lives in the conductor's hot path.

**The conductor is a singleton — a policy plane over stateless workers.** Admission is a
read-modify-write over a sliding attempt window, so two conductors running it concurrently both read the
same window and both admit against it. Serialization has to live somewhere. I put it in a coordination
point that does no I/O against consumers, so it's cheap to be a singleton: the conductor decides, the
workers deliver, and throughput scales by adding workers. The cost is that a conductor outage drops
throughput to zero — but acceptance never depends on scheduling, so events keep landing in the ledger
and delivery fully resumes when a new leader takes the lock. That was the trade I wanted.

**Postgres is the only shared state, leader election included.** No Redis, no queue, no message bus. The
advisory lock is held on the same connection the conductor writes through, so losing the lock and losing
the ability to write are the *same event* — fencing is automatic rather than something I implemented and
hoped was correct. The cost is that Postgres is doing work a purpose-built queue would do better, and at
real scale the fairness window is the first thing I'd move. The gain is that there is exactly one thing
to reason about when something goes wrong, which at this size is worth more.

**`ready` is an admission token materialized as a row state, and the buffer stays shallow.** The
conductor doesn't hand out work, it marks specific deliveries eligible. Keeping the buffer shallow is
what makes fairness fine-grained — the buffer's depth *is* the granularity, because everything already
admitted is already committed. The cost is that available throughput becomes `buffer depth ÷ loop
interval`, so the loop interval is a real tuning knob rather than a detail, and setting it wrong looks
like a broken scheduler rather than a slow one.

**Fairness is a sliding-window query over `attempt`, not an in-memory budget.** The conductor keeps no
authoritative state of its own, so a new leader is immediately correct with no handoff and no warm-up.
The same window enforces `max_attempts_per_s`, so consumer rate limits and fairness are one mechanism
rather than two that can disagree. The cost is a grouped query every pass — real, and the reason the
window is indexed.

**The virtual clock is derived, never stored or ticked.** Virtual time is wall time with an epoch and a
multiplier, computed independently in every process from four fields on one row. There's no ticker to
drift and no clock to synchronize. Pause, resume and speed changes are epoch rewrites. `WallClock` is
the same class at 1× — production doesn't run a different clock, it runs this one with the simulation
turned off.

**Metric counters are derived, not sampled.** The attempts-share chart *is* the fairness proof, which
makes it the component most worth distrusting: on a 100% stacked chart, an equal undercount across three
consumers draws a picture that looks exactly right. So the counters come from grouped queries over
`attempt.started_at` and `delivery.completed_at` rather than from a running total, and `make verify`
asserts `SUM(metrics_snapshot.attempts) == COUNT(attempt)` over the written range. I'd rather the proof
be checkable than fast.

**Coalescing breaks event-log semantics, and I'd rather say so than hide it.** `latest_by_key` means a
consumer genuinely does not receive events that happened. That's the right default for a state feed and
the wrong one for an audit log, so it's opt-in per (consumer, event type), off unless asked for, and
every drop is recorded as `superseded` with the id of the delivery that replaced it. Nothing silently
disappears; it's a decision the consumer made, written down where they can see it.

**Three processes instead of one.** One process would have been faster to build and deterministic to
test. Splitting into api, conductor and worker cost me hours and bought the shape the argument actually
needs: leader election is real, the workers are genuinely stateless, and "the conductor is a singleton"
is something `GET /api/process` will show you, and `scripts/verify.sh` asserts, rather than something I
claim in a document.

## What I left out, and why

**Lease reclamation.** Leases are recorded but never reclaimed. A worker that dies mid-attempt strands
its `in_flight` rows, and because those rows count against the consumer's `concurrency_cap`, that
consumer permanently loses slots. This is a real bug, not a simplification — it's bounded here only
because runs are namespaced by `simulation_id` and Reset starts a clean one. Fixing it is a reaper that
sweeps expired leases back to `ready`, plus an `attempt.outcome` of `lease_expired`; the enum is `TEXT`
+ `CHECK` partly so adding it is an edit rather than a migration. I chose to spend the time on the two
claims instead, and I'd rather name it than have it found.

**Chaos controls.** There's no button to kill a worker mid-flight, because without lease reclamation the
system has no good answer for what happens next. Graceful stop is the failure I do handle, and it's the
one leader election actually turns on.

**Per-consumer retry policy.** Backoff and the retry cap are global settings. Making them per-consumer is
another column and another join, and it doesn't demonstrate anything the staleness bound doesn't already.

**`batch_by_key` coalescing.** Designed, not built. `latest_by_key` proves the mechanism; a second
coalescing mode would only prove it twice.

## Where I'd take it next

The first thing I'd fix is lease reclamation, because it's the one outstanding correctness gap. After
that: real HTTP delivery against a live consumer (the transport seam is already there and the stub is
written), per-consumer retry policy, and moving the fairness window out of Postgres once the attempt
table is large enough that the grouped query stops being cheap. Longer list, with the scaling analysis,
in [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) §Future Work and §At Real Scale.
