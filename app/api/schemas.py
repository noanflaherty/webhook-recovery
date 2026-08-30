"""The frozen API contract.

These shapes are the boundary between the backend and the frontend.
``frontend/src/api/types.ts`` is a hand-written mirror of them, and the
committed ``openapi.json`` -- generated from these models by
``scripts/gen_openapi.py`` -- is the witness CI diffs that mirror against.

Changing a field here is a breaking change to the frontend. Add before you
rename.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.enums import DeliveryState, ProcessKind, SimStatus


class ApiModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthRead(ApiModel):
    status: str = Field(description="'ok' when the process is serving")
    db: str = Field(description="'ok' when SELECT 1 succeeds, else 'error'")


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


class SimulationCreate(ApiModel):
    scenario_name: str | None = None
    speed_multiplier: float | None = Field(default=None, gt=0)
    fair_drain_enabled: bool | None = None
    global_attempts_per_s: float | None = Field(default=None, gt=0)


class SimulationPatch(ApiModel):
    """Every field optional -- omitted means "leave alone".

    ``status`` is how pause and resume are expressed: they are epoch rewrites,
    not a separate endpoint.
    """

    status: SimStatus | None = None
    speed_multiplier: float | None = Field(default=None, gt=0)
    fair_drain_enabled: bool | None = None
    global_attempts_per_s: float | None = Field(default=None, gt=0)
    outage_override: bool | None = Field(
        default=None,
        description="Force the outage on/off. Null in the response means the scenario script decides.",
    )


class SimulationRead(ApiModel):
    id: uuid.UUID
    scenario_name: str
    status: SimStatus
    speed_multiplier: float
    fair_drain_enabled: bool
    global_attempts_per_s: float
    outage_override: bool | None

    #: Current virtual time, computed by the process serving the request.
    virtual_now: datetime
    #: The same instant as seconds since the start of the run -- what the charts
    #: put on their x-axis, and what /metrics buckets by.
    virtual_now_s: float
    #: Which act of the canned scenario the virtual clock is in.
    phase: str

    created_at_wall: datetime


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


class EventCreate(ApiModel):
    """One event from the provider.

    ``occurred_at`` is deliberately not a field: it is server-assigned from the
    simulation's virtual clock. Letting a caller set it would let them backdate
    an event past a staleness bound, and staleness is a policy the *consumer*
    owns.
    """

    event_type: str = Field(max_length=64)
    entity_key: str = Field(max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)


class EventRead(ApiModel):
    id: int
    simulation_id: uuid.UUID
    event_type: str
    entity_key: str
    occurred_at: datetime
    #: How many consumers this event fanned out to. Zero is a legitimate
    #: answer -- the ledger records what the provider emitted, not what anyone
    #: subscribed to.
    delivery_count: int


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


class ConsumerRead(ApiModel):
    id: int
    name: str
    weight: float
    concurrency_cap: int
    max_attempts_per_s: float

    # Live counters, so the consumer cards render from one request.
    backlog: int = 0
    in_flight: int = 0
    delivered: int = 0
    expired: int = 0
    superseded: int = 0
    failed: int = 0
    #: Virtual seconds from the end of the outage until this consumer's backlog
    #: hit zero. Null while it is still draining.
    caught_up_after_s: float | None = None


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class MetricsBucket(ApiModel):
    """One consumer, one virtual second."""

    consumer_id: int
    consumer_name: str
    bucket_virtual_s: int

    backlog: int
    ready: int
    in_flight: int
    attempts: int
    delivered: int
    expired: int
    superseded: int
    failed: int


class MetricsPage(ApiModel):
    """A cursor page of metrics buckets.

    ``bucket_virtual_s`` genuinely is monotonic, so this cursor works as
    designed: poll ``?since_bucket=next_since_bucket`` and append.
    """

    simulation_id: uuid.UUID
    buckets: list[MetricsBucket]
    #: Pass back as ``?since_bucket=``. Unchanged when the page is empty.
    next_since_bucket: int


# ---------------------------------------------------------------------------
# Decision feed
# ---------------------------------------------------------------------------


class DecisionRead(ApiModel):
    """One terminal decision, for the event feed.

    Newest-first, and the client *replaces* rather than appends -- see
    :class:`DecisionsPage`.
    """

    delivery_id: int
    consumer_id: int
    consumer_name: str
    event_type: str
    entity_key: str
    state: DeliveryState
    #: Free text: "superseded by delivery 913", "stale by 43s". Deliberately not
    #: an enum -- terminal reasons are open-ended and this is display-only.
    terminal_reason: str | None
    attempt_count: int
    occurred_at: datetime
    completed_at: datetime


class DecisionsPage(ApiModel):
    """Newest-first, replace-on-poll -- deliberately not a cursor.

    ``delivery.id`` is assigned at *ingest*, not at completion, so it is not
    monotonic in decision order: a ``?since_id=`` cursor over it would silently
    skip decisions. A properly completion-ordered cursor needs a dedicated
    sequence, which is real work for a feed capped at ~50 rows. At a 500ms poll,
    replace-on-poll is visually identical to appending.
    """

    simulation_id: uuid.UUID
    decisions: list[DecisionRead]


# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------


class ProcessRead(ApiModel):
    """A live process. Observability only.

    Only rows that heartbeat inside the liveness window are returned; liveness
    is a read-time filter: nothing reclaims a stale row in order to decide it.
    """

    id: uuid.UUID
    kind: ProcessKind
    hostname: str
    pid: int
    started_at_wall: datetime
    last_heartbeat_wall: datetime
    is_leader: bool
    #: Real seconds since the last heartbeat, so the UI can render a pulse
    #: without needing the client and server clocks to agree.
    heartbeat_age_s: float
    #: Deliveries this worker currently holds a lease on. Always 0 for a conductor.
    in_flight: int = 0


__all__ = [
    "ApiModel",
    "ConsumerRead",
    "DecisionRead",
    "DecisionsPage",
    "EventCreate",
    "EventRead",
    "HealthRead",
    "MetricsBucket",
    "MetricsPage",
    "ProcessRead",
    "SimulationCreate",
    "SimulationPatch",
    "SimulationRead",
]
