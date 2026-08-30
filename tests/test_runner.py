"""The runner's boot and drain behaviour.

Neither property shows up locally, where the database is already migrated and
processes are stopped by hand. Both decide whether a deploy comes up.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from app.core import runner as runner_module
from app.core.enums import ProcessKind
from app.core.runner import LoopBody, ProcessRunner


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the backoff arithmetic, drop the wall-clock cost."""
    monkeypatch.setattr(runner_module, "_REGISTER_RETRY_INITIAL_S", 0.001)
    monkeypatch.setattr(runner_module, "_REGISTER_RETRY_MAX_S", 0.005)
    monkeypatch.setattr(runner_module, "_ERROR_BACKOFF_S", 0.001)


async def _noop(_: uuid.UUID) -> None:
    return None


def _runner(body: LoopBody = _noop) -> ProcessRunner:
    return ProcessRunner(ProcessKind.WORKER, body, interval_s=0.001)


async def test_registration_retries_until_the_schema_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    """A worker that boots mid-migration waits rather than dying.

    Compose gates workers on the one-shot migrate step; Railway starts every
    service concurrently, so this is the only thing standing between a worker
    and a crash-loop against a table that does not exist yet.
    """
    attempts = 0

    async def flaky(kind: ProcessKind, process_id: uuid.UUID) -> uuid.UUID:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError('relation "process" does not exist')
        return process_id

    monkeypatch.setattr(runner_module, "register_process", flaky)

    r = _runner()
    await r._register_when_ready()
    assert attempts == 3


async def test_registration_gives_up_eventually(monkeypatch: pytest.MonkeyPatch) -> None:
    """Waiting forever would hide a genuinely broken DATABASE_URL."""

    async def always_fails(kind: ProcessKind, process_id: uuid.UUID) -> uuid.UUID:
        raise RuntimeError("nope")

    monkeypatch.setattr(runner_module, "register_process", always_fails)
    monkeypatch.setattr(runner_module, "_REGISTER_TIMEOUT_S", 0.02)

    with pytest.raises(RuntimeError, match="nope"):
        await _runner()._register_when_ready()


async def test_a_stop_request_abandons_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIGTERM during the wait must not be swallowed by the retry loop."""

    async def always_fails(kind: ProcessKind, process_id: uuid.UUID) -> uuid.UUID:
        raise RuntimeError("nope")

    monkeypatch.setattr(runner_module, "register_process", always_fails)

    r = _runner()
    r.request_stop()
    with pytest.raises(RuntimeError, match="nope"):
        await r._register_when_ready()


async def test_the_loop_body_finishes_before_the_drain_completes() -> None:
    """The drain guarantee: an iteration that has started is allowed to finish.

    Lease reaping is out of scope, so a worker killed mid-attempt strands its
    in_flight rows permanently. This is what keeps the shutdown path we *do*
    control from doing that.
    """
    started = asyncio.Event()
    finished = False

    async def slow_body(_: uuid.UUID) -> None:
        nonlocal finished
        started.set()
        await asyncio.sleep(0.05)
        finished = True

    r = _runner(slow_body)
    task = asyncio.create_task(r._loop())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    r.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    assert finished, "the in-flight iteration was cancelled instead of drained"


async def test_a_failing_loop_body_does_not_kill_the_process() -> None:
    """A worker that hits one bad row keeps working on the others."""
    calls = 0

    async def sometimes_raises(_: uuid.UUID) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("one bad iteration")

    r = _runner(sometimes_raises)
    task = asyncio.create_task(r._loop())
    await asyncio.sleep(0.05)
    r.request_stop()
    await asyncio.wait_for(task, timeout=3.0)

    assert calls > 1, "the loop stopped after a single failure"
