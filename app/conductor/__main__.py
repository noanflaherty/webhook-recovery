"""The conductor: the policy plane.

Two replicas, one leader. The standby is not decoration -- it is what makes
"singleton" a property of the *work* rather than of the deployment, and it costs
one non-blocking round trip per tick.

Builds its own ``ProcessRunner`` rather than going through ``run_process``,
because the ``Conductor`` needs a reference back to it: ``mark_leader`` is how
the advisory lock reaches the ``process`` row the UI's leader badge renders.
"""

from __future__ import annotations

import asyncio

from app.conductor.service import Conductor
from app.core.enums import ProcessKind
from app.core.runner import ProcessRunner, configure_logging, default_interval_s


async def _run() -> None:
    runner = ProcessRunner(
        ProcessKind.CONDUCTOR,
        # Late-bound so the conductor can hold the runner and the runner can
        # drive the conductor, without either constructing the other.
        lambda process_id: conductor.run_once(process_id),
        interval_s=default_interval_s(ProcessKind.CONDUCTOR),
    )
    conductor = Conductor(runner)
    runner.on_shutdown = conductor.aclose
    await runner.run()


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
