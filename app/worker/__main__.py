"""The worker: the data plane.

Stateless, interchangeable, safe to kill -- three replicas locally, two on
Railway's free tier. Nothing distinguishes them, which is the point: capacity is
the only difference between one worker and three.
"""

from __future__ import annotations

from app.core.enums import ProcessKind
from app.core.runner import run_process
from app.worker.service import Worker


def main() -> None:
    worker = Worker()
    run_process(ProcessKind.WORKER, worker.run_once)


if __name__ == "__main__":
    main()
