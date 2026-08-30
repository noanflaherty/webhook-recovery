"""The conductor: the policy plane.

Phase 0 is an empty loop. It exists so the process topology is real and
deployable before any scheduling logic depends on it.

Coming in Phase 1: ``pg_try_advisory_lock`` leader election, held on the same
session it writes through so fencing is automatic. Compose runs a single
conductor for now -- two idle conductors prove nothing.
"""

from __future__ import annotations

import uuid

from app.core.enums import ProcessKind
from app.core.runner import run_process


async def loop_body(process_id: uuid.UUID) -> None:
    """One conductor pass.

    Phase 1: acquire/refresh leadership, top up the ready buffer, write metrics.
    """
    return None


def main() -> None:
    run_process(ProcessKind.CONDUCTOR, loop_body)


if __name__ == "__main__":
    main()
