"""The worker: the data plane.

Phase 0 is an empty loop. Stateless, interchangeable, safe to kill -- and in
Phase 0 there is nothing to kill it out of.

Coming in Phase 1: ``SELECT ... FOR UPDATE SKIP LOCKED`` claim, lease stamp,
``SimulatedTransport`` attempt, terminal state. Workers contain no policy logic,
with one exception -- a final ``max_staleness`` re-check immediately before
attempting, since an event can go stale in the ready-to-attempt gap.
"""

from __future__ import annotations

import uuid

from app.core.enums import ProcessKind
from app.core.runner import run_process


async def loop_body(process_id: uuid.UUID) -> None:
    """One claim-and-attempt pass.

    ``process_id`` is what gets stamped into ``delivery.leased_by`` and
    ``attempt.worker_id``.
    """
    return None


def main() -> None:
    run_process(ProcessKind.WORKER, loop_body)


if __name__ == "__main__":
    main()
