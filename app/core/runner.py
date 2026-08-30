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


class ProcessRunner:
    """Boot, heartbeat, loop, drain."""

    def __init__(
        self,
        kind: ProcessKind,
        loop_body: LoopBody,
        *,
        interval_s: float,
        process_id: uuid.UUID | None = None,
    ) -> None:
        self.kind = kind
        self.loop_body = loop_body
        self.interval_s = interval_s
        self.process_id = process_id or uuid.uuid4()
        self.stop = asyncio.Event()
        self._is_leader = False

    def mark_leader(self, value: bool) -> None:
        """Set by the conductor once leader election lands (Phase 1)."""
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

    async def run(self) -> None:
        self.install_signal_handlers()
        await register_process(self.kind, self.process_id)
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
                await self._wait(max(self.interval_s, 1.0))
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


def run_process(kind: ProcessKind, loop_body: LoopBody, interval_s: float | None = None) -> None:
    """Synchronous entrypoint for ``python -m app.worker`` and friends."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    settings = get_settings()
    if interval_s is None:
        interval_s = (
            settings.conductor_loop_interval_s
            if kind is ProcessKind.CONDUCTOR
            else settings.worker_loop_interval_s
        )
    runner = ProcessRunner(kind, loop_body, interval_s=interval_s)
    asyncio.run(runner.run())


__all__ = ["LoopBody", "ProcessRunner", "run_process"]
