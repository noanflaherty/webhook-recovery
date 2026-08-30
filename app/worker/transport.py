"""The consumer transport seam.

One protocol, two implementations: the simulated one the demo runs on, and the
HTTP one production would. The seam matters more than either -- it is what makes
"this is a real delivery system with a fake network" a structural claim rather
than an assertion, because nothing above this line knows which is installed.

``SimulatedTransport`` sleeps through the ``Clock``, so a 200ms attempt at 20x
really occupies its slot for 10 real milliseconds. That is what makes virtual
latency physically honest: lease durations and concurrency caps mean what they
say, rather than being numbers in a column that nothing enforces.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from typing import Protocol

from app.core.clock import Clock
from app.core.enums import AttemptOutcome

#: How long a request to a consumer that is down hangs before timing out, in
#: virtual seconds. A down consumer holding the connection open is the expensive
#: failure -- it burns a concurrency slot for the whole duration, which is
#: exactly what `concurrency_cap` exists to bound.
TIMEOUT_VIRTUAL_S = 5.0


@dataclass(frozen=True, slots=True)
class ConsumerProfile:
    """The transport-visible slice of a ``consumer`` row."""

    latency_s: float
    jitter_s: float
    failure_rate: float
    down: bool


@dataclass(frozen=True, slots=True)
class AttemptRequest:
    simulation_id: uuid.UUID
    delivery_id: int
    consumer_id: int
    #: 1-based, and part of the RNG seed: a retry of the same delivery must be
    #: able to succeed where the first try failed, or backoff proves nothing.
    attempt_no: int
    event_type: str
    entity_key: str
    profile: ConsumerProfile


@dataclass(frozen=True, slots=True)
class AttemptResult:
    outcome: AttemptOutcome
    status_code: int | None = None


class ConsumerTransport(Protocol):
    """What the worker delivers through. The only thing it knows about consumers."""

    async def attempt(self, request: AttemptRequest) -> AttemptResult: ...


class SimulatedTransport:
    """No network: latency, jitter, failures and outages from the consumer row.

    **Deterministic per attempt.** The RNG is seeded from
    ``(simulation_id, delivery_id, attempt_no)`` rather than carried as
    transport state, so the same attempt has the same outcome no matter which of
    the three workers picks it up, whether it is retried after a restart, or in
    what order the batch happens to run. A shared ``random.Random`` on the
    transport would make outcomes depend on claim interleaving, which is
    unreproducible by construction and would make every flaky test a real one.
    """

    __slots__ = ("_clock",)

    def __init__(self, clock: Clock) -> None:
        self._clock = clock

    async def attempt(self, request: AttemptRequest) -> AttemptResult:
        profile = request.profile
        rng = random.Random(f"{request.simulation_id}:{request.delivery_id}:{request.attempt_no}")

        if profile.down:
            await self._clock.sleep(TIMEOUT_VIRTUAL_S)
            return AttemptResult(AttemptOutcome.TIMEOUT)

        latency = max(0.0, profile.latency_s + rng.uniform(-profile.jitter_s, profile.jitter_s))
        await self._clock.sleep(latency)

        if rng.random() < profile.failure_rate:
            return AttemptResult(AttemptOutcome.SERVER_ERROR, 503)
        return AttemptResult(AttemptOutcome.OK, 200)


class HttpTransport:
    """What production would install here. Documented, not built.

    The shape is unsurprising and that is the point -- everything above this
    class is already written against the protocol, so this is the only file that
    would change:

    * ``POST`` the event payload to the consumer's endpoint with a bounded
      total timeout, since an unbounded one is how a slow consumer takes a
      concurrency slot forever.
    * Sign the body (HMAC-SHA256 over ``timestamp.body``, sent as a header) so
      the consumer can authenticate the provider and reject replays.
    * Map the response: 2xx to ``ok``; 5xx and 429 to ``5xx`` (retryable); a
      connect or read timeout to ``timeout``. 4xx other than 429 is a consumer
      bug that retrying cannot fix and belongs in a dead-letter state the demo
      does not model.

    Not built because it would need a real HTTP server to deliver to, which is
    scaffolding that proves nothing the ``SimulatedTransport`` does not -- and
    ``httpx`` stays a dev-only dependency, which is why the image is built
    ``--no-dev``.
    """

    __slots__ = ()

    async def attempt(self, request: AttemptRequest) -> AttemptResult:
        raise NotImplementedError(
            "HttpTransport is a documented stub -- the demo delivers through SimulatedTransport"
        )


__all__ = [
    "TIMEOUT_VIRTUAL_S",
    "AttemptRequest",
    "AttemptResult",
    "ConsumerProfile",
    "ConsumerTransport",
    "HttpTransport",
    "SimulatedTransport",
]
