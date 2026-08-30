"""The shared entrypoint for the conductor and the worker.

Register in ``process``, heartbeat, run a loop body until told to stop, drain.

**The drain is the point.** Lease reaping is out of scope, so a worker that dies
mid-attempt strands its ``in_flight`` rows permanently and each stranded row
permanently costs that consumer a concurrency slot. Handling SIGTERM by letting
the current iteration *finish* means the one shutdown path we control -- a
deploy, a ``docker compose stop``, a Railway restart -- strands nothing. Only an
ungraceful kill does, and chaos controls are out of scope, so that path is now
hard to hit even by accident.

Fifteen lines that materially shrink the cost of a known scope cut.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import uuid
from collections.abc import Awaitable, Callable

from app.core.db import dispose_engine
from app.core.enums import ProcessKind
from app.core.registry import deregister_process, heartbeat_loop, register_process
from app.core.settings import get_settings

log = logging.getLogger(__name__)

#: A loop body: one iteration of work. Receives the process's own id, because a
#: worker stamps it on the leases it takes.
LoopBody = Callable[[uuid.UUID], Awaitable[None]]

#: Teardown, run once after the last iteration drains. The conductor releases
#: its advisory lock here: the lock is held on a connection that lives *across*
#: iterations, so there is no `finally` inside the loop body to release it from.
Teardown = Callable[[], Awaitable[None]]

#: How long a booting process waits for its schema to appear before giving up.
#: Generous: the only thing it is waiting on is one `alembic upgrade head`, and
#: failing early here costs a crash-loop for no benefit.
_REGISTER_TIMEOUT_S = 120.0
_REGISTER_RETRY_INITIAL_S = 0.5
_REGISTER_RETRY_MAX_S = 5.0

#: Pause after a failed loop iteration, so a persistent failure does not become
#: a hot spin against the database.
_ERROR_BACKOFF_S = 1.0


class ProcessRunner:
    """Boot, heartbeat, loop, drain."""

    def __init__(
        self,
        kind: ProcessKind,
        loop_body: LoopBody,
        *,
        interval_s: float,
        process_id: uuid.UUID | None = None,
        on_shutdown: Teardown | None = None,
    ) -> None:
        self.kind = kind
        self.loop_body = loop_body
        self.interval_s = interval_s
        self.process_id = process_id or uuid.uuid4()
        self.on_shutdown = on_shutdown
        self.stop = asyncio.Event()
        self._is_leader = False

    def mark_leader(self, value: bool) -> None:
        """Called by the conductor each pass; read by the heartbeat.

        Observability only. ``process.is_leader`` is served by ``GET
        /api/process`` and asserted by ``scripts/verify.sh``; it is never read
        back to decide anything, because leadership is the advisory lock and
        nothing else.
        """
        self._is_leader = value

    def request_stop(self, signum: int | None = None) -> None:
        if not self.stop.is_set():
            log.info("%s %s draining (signal %s)", self.kind.value, self.process_id, signum)
            self.stop.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop, sig)

    async def _register_when_ready(self) -> None:
        """Register, retrying until the schema exists.

        Boot ordering is not guaranteed. Compose gates the workers on the
        one-shot migrate step, but Railway has no equivalent -- services start
        concurrently, so a worker can come up while the api is still running
        `alembic upgrade head` and find no `process` table to insert into.

        Dying there is the wrong answer twice over: it is a transient condition,
        and the platform's restart policy turns it into a crash-loop that can
        exhaust its retry budget before the migration finishes. So this waits
        instead, which is also what a worker should do when its database blinks
        in production.
        """
        delay = _REGISTER_RETRY_INITIAL_S
        deadline = asyncio.get_running_loop().time() + _REGISTER_TIMEOUT_S
        attempt = 0
        while True:
            attempt += 1
            try:
                await register_process(self.kind, self.process_id)
                return
            except Exception:
                if asyncio.get_running_loop().time() >= deadline or self.stop.is_set():
                    raise
                log.warning(
                    "%s could not register (attempt %d); retrying in %.1fs -- "
                    "the schema is probably still being migrated",
                    self.kind.value,
                    attempt,
                    delay,
                )
            await self._wait(delay)
            delay = min(delay * 2, _REGISTER_RETRY_MAX_S)

    async def run(self) -> None:
        self.install_signal_handlers()
        await self._register_when_ready()
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(self.process_id, self.stop, is_leader=lambda: self._is_leader),
            name=f"heartbeat-{self.kind.value}",
        )
        log.info("%s %s running", self.kind.value, self.process_id)
        try:
            await self._loop()
        finally:
            # The loop body has already returned, so nothing is in flight here.
            self.stop.set()
            if self.on_shutdown is not None:
                # Logged and swallowed: this runs on the way out, and a failure
                # to release a lock the dying session is about to drop anyway
                # must not mask whatever actually stopped the loop.
                try:
                    await self.on_shutdown()
                except Exception:
                    log.warning("%s shutdown hook failed", self.kind.value, exc_info=True)
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await deregister_process(self.process_id)
            await dispose_engine()
            log.info("%s %s stopped", self.kind.value, self.process_id)

    async def _loop(self) -> None:
        while not self.stop.is_set():
            try:
                # Not cancelled on stop: an iteration that has started is
                # allowed to finish. That is the whole drain guarantee.
                await self.loop_body(self.process_id)
            except Exception:
                log.exception("%s loop body failed", self.kind.value)
                # Back off a little so a persistent failure (a database that is
                # still starting) does not become a hot spin.
                await self._wait(max(self.interval_s, _ERROR_BACKOFF_S))
                continue
            await self._wait(self.interval_s)

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake immediately on a stop request.

        Real seconds, not virtual: this is the process's own cadence, not
        simulated latency. Virtual sleeps happen inside the loop body, through
        the ``Clock``.
        """
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)


def configure_logging() -> None:
    """One log format for every process type.

    Factored out of :func:`run_process` so the conductor -- which builds its own
    ``ProcessRunner`` in order to hold a reference to it for ``mark_leader`` --
    does not have to duplicate it or lose its logs.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def default_interval_s(kind: ProcessKind) -> float:
    settings = get_settings()
    return (
        settings.conductor_loop_interval_s
        if kind is ProcessKind.CONDUCTOR
        else settings.worker_loop_interval_s
    )


def run_process(
    kind: ProcessKind,
    loop_body: LoopBody,
    interval_s: float | None = None,
    *,
    on_shutdown: Teardown | None = None,
) -> None:
    """Synchronous entrypoint for ``python -m app.worker`` and friends."""
    configure_logging()
    runner = ProcessRunner(
        kind,
        loop_body,
        interval_s=interval_s if interval_s is not None else default_interval_s(kind),
        on_shutdown=on_shutdown,
    )
    asyncio.run(runner.run())


__all__ = [
    "LoopBody",
    "ProcessRunner",
    "Teardown",
    "configure_logging",
    "default_interval_s",
    "run_process",
]
