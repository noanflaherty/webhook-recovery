"""Scenario phase boundaries.

The scenario *engine* is Phase 3. What lives here in Phase 0 is only the part
the frozen contract depends on: ``SimulationRead.phase``. Phases are derived
from virtual time rather than stored, for the same reason the clock is -- a
stored phase is another thing three processes have to agree about.
"""

from __future__ import annotations

from typing import Final

#: ~15 virtual minutes, which is ~45 real seconds at 20x.
OUTAGE_STARTS_AT_S: Final = 120.0  # 2:00
OUTAGE_ENDS_AT_S: Final = 420.0  # 7:00

PHASE_NORMAL: Final = "normal"
PHASE_OUTAGE: Final = "outage"
PHASE_RECOVERY: Final = "recovery"
PHASE_DONE: Final = "done"


def phase_at(virtual_s: float, *, outage_override: bool | None = None, done: bool = False) -> str:
    """Which act of the scenario a given virtual second falls in.

    ``outage_override`` is the reviewer's manual switch: ``True`` forces the
    outage on, ``False`` forces it off, ``None`` follows the script.
    """
    if done:
        return PHASE_DONE
    if outage_override is True:
        return PHASE_OUTAGE
    if outage_override is False:
        return PHASE_NORMAL if virtual_s < OUTAGE_STARTS_AT_S else PHASE_RECOVERY
    if virtual_s < OUTAGE_STARTS_AT_S:
        return PHASE_NORMAL
    if virtual_s < OUTAGE_ENDS_AT_S:
        return PHASE_OUTAGE
    return PHASE_RECOVERY


def is_outage(virtual_s: float, *, outage_override: bool | None = None) -> bool:
    """Whether the delivery pipeline is down.

    The conductor skips admission entirely while this is true, which is what
    makes backlogs climb through the outage and drain after it.
    """
    if outage_override is not None:
        return outage_override
    return OUTAGE_STARTS_AT_S <= virtual_s < OUTAGE_ENDS_AT_S


__all__ = [
    "OUTAGE_ENDS_AT_S",
    "OUTAGE_STARTS_AT_S",
    "PHASE_DONE",
    "PHASE_NORMAL",
    "PHASE_OUTAGE",
    "PHASE_RECOVERY",
    "is_outage",
    "phase_at",
]
