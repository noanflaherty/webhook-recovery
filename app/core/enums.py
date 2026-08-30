"""Enumerations shared across the three process types.

These are stored as ``TEXT`` with a ``CHECK`` constraint rather than as native
Postgres enums. Adding a value to a native enum needs a migration; a ``CHECK``
needs an edit. ``AttemptOutcome`` is a live candidate to gain ``lease_expired``
if the reaper is ever un-scoped, and ``terminal_reason`` is inherently
open-ended, so the flexibility is worth more than the storage.
"""

from __future__ import annotations

from enum import StrEnum


class SimStatus(StrEnum):
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"


class DeliveryState(StrEnum):
    """The delivery lifecycle (TECHNICAL_DESIGN.md §Delivery Lifecycle).

    ``expired`` and ``superseded`` are decided by the conductor and never reach
    a worker.
    """

    PENDING = "pending"
    READY = "ready"
    IN_FLIGHT = "in_flight"
    DELIVERED = "delivered"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"
    FAILED = "failed"


#: Backlog: work that still has somewhere to go. The one definition of it --
#: the consumer cards and the metrics writer must agree, or the chart and the
#: card disagree about the same number.
ACTIVE_DELIVERY_STATES: frozenset[DeliveryState] = frozenset(
    {DeliveryState.PENDING, DeliveryState.READY, DeliveryState.IN_FLIGHT}
)

#: Terminal states that are a *decision* rather than an outcome -- these are what
#: the decision feed shows.
TERMINAL_DELIVERY_STATES: frozenset[DeliveryState] = frozenset(
    {
        DeliveryState.DELIVERED,
        DeliveryState.EXPIRED,
        DeliveryState.SUPERSEDED,
        DeliveryState.FAILED,
    }
)


class AttemptOutcome(StrEnum):
    OK = "ok"
    SERVER_ERROR = "5xx"
    TIMEOUT = "timeout"


class CoalesceMode(StrEnum):
    NONE = "none"
    LATEST_BY_KEY = "latest_by_key"


class ProcessKind(StrEnum):
    """Only the two process types the registry is about.

    The api deliberately does not register: the registry exists so the UI can
    show that the *delivery* architecture is real, and ``GET /api/process``
    returning the process serving it proves nothing.
    """

    CONDUCTOR = "conductor"
    WORKER = "worker"


def check_values(enum_cls: type[StrEnum]) -> list[str]:
    """The literal values for a ``CHECK (col IN (...))`` constraint."""
    return [member.value for member in enum_cls]


__all__ = [
    "ACTIVE_DELIVERY_STATES",
    "TERMINAL_DELIVERY_STATES",
    "AttemptOutcome",
    "CoalesceMode",
    "DeliveryState",
    "ProcessKind",
    "SimStatus",
    "check_values",
]
