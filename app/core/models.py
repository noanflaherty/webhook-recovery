"""SQLAlchemy declarative models -- the single source of schema truth.

The design calls for SQLAlchemy Core, and that intent holds where it matters:
the worker claim loop and the fairness window query are written as explicit
``select()`` statements against these tables, not ORM traversals. Declarative is
used for schema definition and trivial reads, which is what buys Alembic
autogenerate.

Conventions (TECHNICAL_DESIGN.md §Data Model):
  * Table names are singular -- a row is one delivery.
  * Every table except ``process`` carries ``simulation_id``.
  * Timestamps are *virtual* unless suffixed ``_wall``.
  * Enums are ``TEXT`` + ``CHECK``, never native Postgres enums.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.core.enums import (
    AttemptOutcome,
    CoalesceMode,
    DeliveryState,
    ProcessKind,
    SimStatus,
    check_values,
)

# Named constraint conventions, so Alembic can autogenerate reversible
# migrations instead of emitting unnamed constraints it cannot later drop.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _enum_check(column: str, enum_cls: Any, name: str) -> CheckConstraint:
    values = ", ".join(f"'{v}'" for v in check_values(enum_cls))
    return CheckConstraint(f"{column} IN ({values})", name=name)


def _nullable_enum_check(column: str, enum_cls: Any, name: str) -> CheckConstraint:
    values = ", ".join(f"'{v}'" for v in check_values(enum_cls))
    return CheckConstraint(f"{column} IS NULL OR {column} IN ({values})", name=name)


# --------------------------------------------------------------------------
# Simulation -- the namespace, and the source of every process's clock
# --------------------------------------------------------------------------


class Simulation(Base):
    """A namespace holding a whole run, plus the four fields the clock derives from.

    UUID id: created without a database round-trip and appears in URLs.
    """

    __tablename__ = "simulation"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at_wall: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    scenario_name: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default=SimStatus.RUNNING.value)

    # --- Derived clock (TECHNICAL_DESIGN.md §The Virtual Clock) -----------
    #: Virtual time at the last resume/speed-change. Paired with resumed_at_wall.
    virtual_epoch: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    #: Wall time at the last resume/speed-change.
    resumed_at_wall: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    #: Frozen virtual time while paused; NULL while running.
    paused_at_virtual: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    speed_multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    # --- Scheduling knobs -------------------------------------------------
    fair_drain_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    global_attempts_per_s: Mapped[float] = mapped_column(Float, nullable=False, default=30.0)
    #: Manual override of the scenario's outage phase: NULL follows the script.
    outage_override: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    __table_args__ = (
        _enum_check("status", SimStatus, "status"),
        CheckConstraint("speed_multiplier > 0", name="speed_positive"),
    )


# --------------------------------------------------------------------------
# Consumers, subscriptions, policies
# --------------------------------------------------------------------------


class Consumer(Base):
    __tablename__ = "consumer"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(64), nullable=False)

    # --- The three delivery knobs (TECHNICAL_DESIGN.md §The three knobs) --
    #: Relative share of contended provider capacity.
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    #: Hard cap on simultaneous in_flight attempts.
    concurrency_cap: Mapped[int] = mapped_column(Integer, nullable=False, default=8)
    #: Hard ceiling on attempt rate -- concurrency caps parallelism, not throughput.
    max_attempts_per_s: Mapped[float] = mapped_column(Float, nullable=False, default=20.0)

    # --- SimulatedTransport profile --------------------------------------
    sim_latency_s: Mapped[float] = mapped_column(Float, nullable=False, default=0.2)
    sim_jitter_s: Mapped[float] = mapped_column(Float, nullable=False, default=0.05)
    sim_failure_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sim_down: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("simulation_id", "name", name="uq_consumer_simulation_id_name"),
        CheckConstraint("weight > 0", name="weight_positive"),
        CheckConstraint("concurrency_cap > 0", name="concurrency_positive"),
        CheckConstraint("max_attempts_per_s > 0", name="rate_positive"),
    )


class Subscription(Base):
    __tablename__ = "subscription"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    consumer_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("consumer.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)

    __table_args__ = (
        UniqueConstraint("consumer_id", "event_type", name="uq_subscription_consumer_id_event_type"),
        # Ingest fan-out: given (simulation, event_type), which consumers?
        Index("ix_subscription_fanout", "simulation_id", "event_type"),
    )


class DeliveryPolicy(Base):
    """Per (consumer, event_type). An absent row means "deliver everything"."""

    __tablename__ = "delivery_policy"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    consumer_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("consumer.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    #: How late is too late, in virtual seconds. NULL = no staleness bound.
    max_staleness_s: Mapped[float | None] = mapped_column(Float, nullable=True)
    coalesce: Mapped[str] = mapped_column(Text, nullable=False, default=CoalesceMode.NONE.value)

    __table_args__ = (
        UniqueConstraint("consumer_id", "event_type", name="uq_delivery_policy_consumer_id_event_type"),
        _enum_check("coalesce", CoalesceMode, "coalesce"),
        CheckConstraint("max_staleness_s IS NULL OR max_staleness_s > 0", name="staleness_positive"),
    )


# --------------------------------------------------------------------------
# The ledger and the queue
# --------------------------------------------------------------------------


class Event(Base):
    """An immutable fact emitted by the producer. The ledger."""

    __tablename__ = "event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(128), nullable=False)
    #: Virtual time the fact occurred. Staleness is measured from here.
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class Delivery(Base):
    """The (event, consumer) pair -- the unit of work in the queue.

    ``event_type`` and ``entity_key`` are denormalized off ``event`` so the
    coalesce lookup and policy evaluation never join.
    """

    __tablename__ = "delivery"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("event.id", ondelete="CASCADE"), nullable=False
    )
    consumer_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("consumer.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(128), nullable=False)

    state: Mapped[str] = mapped_column(Text, nullable=False, default=DeliveryState.PENDING.value)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Virtual time this delivery becomes a retry candidate again.
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: Virtual time the conductor admitted it. Workers claim in this order.
    ready_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # --- Lease: recorded, not reclaimed (TECHNICAL_DESIGN.md §Leases) -----
    # Written from day one precisely so adding a reaper later is a function
    # rather than a migration.
    leased_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    terminal_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        _enum_check("state", DeliveryState, "state"),
        # --- One partial index per access path -----------------------------
        # Worker claim: ... WHERE simulation_id = $1 AND state = 'ready' ORDER BY ready_at
        Index(
            "ix_delivery_ready",
            "simulation_id",
            "ready_at",
            postgresql_where=text("state = 'ready'"),
        ),
        # Conductor candidate scan.
        Index(
            "ix_delivery_pending",
            "simulation_id",
            "consumer_id",
            "next_attempt_at",
            postgresql_where=text("state = 'pending'"),
        ),
        # Coalesce lookup. A ready delivery has not been attempted yet, so it is
        # still supersedable -- hence both states.
        Index(
            "ix_delivery_coalesce",
            "consumer_id",
            "event_type",
            "entity_key",
            postgresql_where=text("state IN ('pending', 'ready')"),
        ),
        # Decision feed + per-consumer counters.
        Index("ix_delivery_completed", "simulation_id", "completed_at"),
    )


class Attempt(Base):
    """One try at delivering a delivery. Fairness is measured in attempts."""

    __tablename__ = "attempt"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    delivery_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("delivery.id", ondelete="CASCADE"), nullable=False, index=True
    )
    consumer_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("consumer.id", ondelete="CASCADE"), nullable=False
    )
    #: The process.id of the worker that ran it.
    worker_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        # Nullable: the row is written when the attempt *starts*, so an
        # in-flight attempt has no outcome yet.
        _nullable_enum_check("outcome", AttemptOutcome, "outcome"),
        # Sliding-window fairness + rate cap. The single hottest read in the
        # conductor loop, so it gets its own index.
        Index("ix_attempt_window", "consumer_id", "started_at"),
    )


# --------------------------------------------------------------------------
# Metrics and observability
# --------------------------------------------------------------------------


class MetricsSnapshot(Base):
    """One row per consumer per virtual second. The chart series.

    ``bucket_virtual_s`` genuinely is monotonic, which is what lets
    ``/metrics?since_bucket=N`` be a real cursor.
    """

    __tablename__ = "metrics_snapshot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    simulation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation.id", ondelete="CASCADE"), nullable=False
    )
    consumer_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("consumer.id", ondelete="CASCADE"), nullable=False
    )
    bucket_virtual_s: Mapped[int] = mapped_column(Integer, nullable=False)

    backlog: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ready: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    in_flight: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    delivered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expired: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    superseded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint(
            "simulation_id",
            "consumer_id",
            "bucket_virtual_s",
            name="uq_metrics_snapshot_simulation_id_consumer_id_bucket_virtual_s",
        ),
        Index("ix_metrics_snapshot_cursor", "simulation_id", "bucket_virtual_s"),
    )


class Process(Base):
    """Self-registered process, for the UI's process strip.

    Observability only: not required for correctness, and leader election never
    reads it -- leadership is decided entirely by the Postgres advisory lock.
    Nothing in the delivery path consults it.

    Liveness is a read-time filter, not a reaper: stale rows from prior deploys
    accumulate harmlessly and are never read. UUID id because a booting worker
    generates it without a database round-trip.

    Deliberately has no ``simulation_id``: a process outlives any one run.
    """

    __tablename__ = "process"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at_wall: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_heartbeat_wall: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_leader: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        _enum_check("kind", ProcessKind, "kind"),
        Index("ix_process_liveness", "last_heartbeat_wall"),
    )


# Re-exported so ``from app.core.models import AttemptOutcome`` reads naturally
# at call sites that write an outcome.
__all__ = [
    "Attempt",
    "AttemptOutcome",
    "Base",
    "Consumer",
    "Delivery",
    "DeliveryPolicy",
    "Event",
    "MetricsSnapshot",
    "Process",
    "Simulation",
    "Subscription",
]
