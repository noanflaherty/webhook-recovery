"""The simulated transport.

Determinism is the property worth asserting. An attempt's outcome is a function
of ``(simulation_id, delivery_id, attempt_no)`` and nothing else -- not of which
worker picked it up, not of how the batch interleaved, not of how many attempts
have run before it. Without that, a retry test is a coin flip and every failure
is arguably flaky.

No database here: the transport is a pure function of a consumer row plus a
clock, which is the point of the seam.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest

from app.core.enums import AttemptOutcome
from app.worker.transport import (
    TIMEOUT_VIRTUAL_S,
    AttemptRequest,
    ConsumerProfile,
    HttpTransport,
    SimulatedTransport,
)

SIM = uuid.UUID("11111111-2222-4333-8444-555555555555")

HEALTHY = ConsumerProfile(latency_s=0.2, jitter_s=0.05, failure_rate=0.0, down=False)
FLAKY = ConsumerProfile(latency_s=0.2, jitter_s=0.05, failure_rate=0.5, down=False)
DOWN = ConsumerProfile(latency_s=0.2, jitter_s=0.05, failure_rate=0.0, down=True)


class RecordingClock:
    """A clock that records sleeps instead of taking them.

    Virtual latency is what makes lease durations and concurrency caps mean
    something, so the assertion is on the duration asked for, not on wall time
    elapsed -- which would make this test slow *and* flaky.
    """

    def __init__(self) -> None:
        self.slept: list[float] = []

    @property
    def speed_multiplier(self) -> float:
        return 1.0

    def now(self) -> datetime:
        raise AssertionError("the transport has no business reading the clock")

    async def sleep(self, virtual_seconds: float) -> None:
        self.slept.append(virtual_seconds)


def _request(delivery_id: int, attempt_no: int, profile: ConsumerProfile) -> AttemptRequest:
    return AttemptRequest(
        simulation_id=SIM,
        delivery_id=delivery_id,
        consumer_id=1,
        attempt_no=attempt_no,
        event_type="invoice.paid",
        entity_key="in_1",
        profile=profile,
    )


async def test_the_same_attempt_always_has_the_same_outcome() -> None:
    """Seeded per attempt, not carried as transport state.

    A shared RNG on the transport would make outcomes depend on the order the
    batch happened to run in, which is unreproducible by construction -- and
    would make a genuinely broken retry path indistinguishable from a flaky test.
    """
    request = _request(delivery_id=99, attempt_no=1, profile=FLAKY)

    first = await SimulatedTransport(RecordingClock()).attempt(request)
    # A different transport instance entirely -- a different worker, in effect.
    second = await SimulatedTransport(RecordingClock()).attempt(request)

    assert first == second


async def test_a_retry_can_succeed_where_the_first_attempt_failed() -> None:
    """``attempt_no`` is in the seed, so backoff is not delivering the same verdict twice."""
    transport = SimulatedTransport(RecordingClock())
    outcomes = {
        (await transport.attempt(_request(7, attempt_no=n, profile=FLAKY))).outcome for n in range(1, 12)
    }
    assert outcomes == {AttemptOutcome.OK, AttemptOutcome.SERVER_ERROR}


async def test_different_deliveries_do_not_share_a_verdict() -> None:
    """At a 50% failure rate, eleven deliveries agreeing would mean the seed is not used."""
    transport = SimulatedTransport(RecordingClock())
    outcomes = {
        (await transport.attempt(_request(n, attempt_no=1, profile=FLAKY))).outcome for n in range(11)
    }
    assert len(outcomes) == 2


async def test_a_healthy_consumer_always_answers_200() -> None:
    """What the Phase 1 demo runs on: the state machine is complete, the data is calm."""
    transport = SimulatedTransport(RecordingClock())
    for n in range(20):
        result = await transport.attempt(_request(n, attempt_no=1, profile=HEALTHY))
        assert result.outcome is AttemptOutcome.OK
        assert result.status_code == 200


async def test_a_down_consumer_times_out_after_holding_the_slot() -> None:
    """The expensive failure: a slot burned for the whole timeout, not just a fast 5xx.

    That cost is exactly what ``concurrency_cap`` exists to bound, so the
    transport has to actually spend it rather than fail immediately.
    """
    clock = RecordingClock()
    result = await SimulatedTransport(clock).attempt(_request(1, attempt_no=1, profile=DOWN))

    assert result.outcome is AttemptOutcome.TIMEOUT
    assert result.status_code is None
    assert clock.slept == [TIMEOUT_VIRTUAL_S]


async def test_latency_is_spent_through_the_clock_and_stays_in_its_jitter_band() -> None:
    """Sleeps go through the ``Clock``, which is what makes 20x honest.

    A transport that called ``asyncio.sleep`` directly would hold its lease for
    200 real milliseconds at every speed, and the concurrency numbers would stop
    meaning anything the moment the speed slider moved.
    """
    clock = RecordingClock()
    transport = SimulatedTransport(clock)
    for n in range(30):
        await transport.attempt(_request(n, attempt_no=1, profile=HEALTHY))

    assert len(clock.slept) == 30
    assert all(0.15 <= slept <= 0.25 for slept in clock.slept)
    assert len(set(clock.slept)) > 1, "jitter is configured but the latency never varies"


async def test_the_http_transport_is_an_honest_stub() -> None:
    """Documented, not silently returning success."""
    with pytest.raises(NotImplementedError, match="documented stub"):
        await HttpTransport().attempt(_request(1, attempt_no=1, profile=HEALTHY))
