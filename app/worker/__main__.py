"""The worker: the data plane.

Stateless, interchangeable, safe to kill -- three replicas locally, two on
Railway's free tier. Nothing distinguishes them, which is the point: capacity is
the only difference between one worker and three.

Builds its own ``ProcessRunner`` rather than going through ``run_process``,
because the ``Worker`` needs a reference back to it: the runner's heartbeat is
what learns that this process has been asked to die, and the worker is what
decides where in a batch to act on it.
"""

from __future__ import annotations

import asyncio

from app.core.enums import ProcessKind
from app.core.runner import ProcessRunner, configure_logging, default_interval_s
from app.worker.service import Worker


async def _run() -> None:
    runner = ProcessRunner(
        ProcessKind.WORKER,
        # Late-bound so the worker can hold the runner and the runner can drive
        # the worker, without either constructing the other.
        lambda process_id: worker.run_once(process_id),
        interval_s=default_interval_s(ProcessKind.WORKER),
    )
    worker = Worker(runner=runner)
    await runner.run()


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
