# Technical Design

## User Stories

As a consumer of a provider webhook...

- I should be able to register per endpoint, per event type, a policy representing whether I want a given event to be
  delivered. Specifically:
    - `max_staleness`: how late is too late. Older events are recorded as `expired`, not delivered.
    - `coalesce: latest_by_key`: when multiple queued events share an entity key, deliver only the newest.
    - `retry`: backoff strategy and cap.
- If I did not specify a delivery policy, then it should default ot "deliver everything"
- Should not have the delivery rate of my event backlog slowed down by the size of another consumer's backlog. I expect
  the provider to perform "fair draining" where delivery is scheduled per consumer with weighted round-robin and a
  per-consumer concurrency cap. Every consumer makes progress proportional to its share, not its backlog. Another consumer
  holding connections open or returning 5xx should burn its own budget, not mine.

## Simulating Outages
This repo should come with a deployed UI and a virtual clock that's used to simulate system behavior during normal traffic, an outage, and recovery.

We'll include three simulated consumers:
TODO: Define three consumers and their properties. One normal (no policies), one with policies, one with low volume.

Scenario:
TODO: Document the steps the scenario will walk through, from normal day, to outage, to recovery. Define UIs to show key metrics and prove fairness.

## Tech Stack
For this interview and demo purposes, we will keep the tech stack simple. More below in "At real scale" on how this system would change before implementing in production.

- Python
- Single process for demo reliability. Maybe revisit if time allows
- Postgres for data model, event ledger, and queue
- React, Typescript, Vite
- SSE for streamed live data for the UI to display
