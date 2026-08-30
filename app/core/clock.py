"""Virtual time.

The clock is the first thing a multi-process split breaks: three process types
must agree on what time it is, and virtual latency must mean something across a
process boundary. Storing ``virtual_now`` in a row and having the conductor
increment it creates a distributed barrier problem -- the conductor must not
advance time while a worker is mid-attempt, or a "200ms" attempt sees the clock
jump three seconds.

**The clock is derived, never stored.** Virtual time is wall time with an epoch
and a multiplier, computed locally in every process from four near-immutable
fields on the ``simulation`` row. No coordination, no barrier, no polling.
Pause / resume / speed-change are writes of a new epoch, which every process
picks up on its next config read.

This module is the *only* place in the codebase permitted to read the wall
clock; a ruff ``banned-api`` rule enforces that everywhere else.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

from app.core.enums import SimStatus

#: Virtual time zero. Every simulation's virtual clock starts here, so a virtual
#: timestamp reads as an offset from the start of the run.
VIRTUAL_EPOCH_ZERO = datetime(2024, 1, 1, tzinfo=UTC)


def wall_now() -> datetime:
    """The one sanctioned wall-clock read in the codebase."""
    return datetime.now(UTC)


@runtime_checkable
class Clock(Protocol):
    """What every process reads time through.

    Two methods, not one. ``sleep`` is the half that makes virtual latency
    physically honest across a process boundary: a worker sleeping 200 virtual
    ms at 20x really holds its lease for 10 real ms, so concurrency caps and
    lease durations mean what they say.
    """

    def now(self) -> datetime:
        """Current virtual time."""
        ...

    async def sleep(self, virtual_seconds: float) -> None:
        """Sleep for ``virtual_seconds`` of *virtual* time."""
        ...

    @property
    def speed_multiplier(self) -> float:
        """Virtual seconds elapsed per real second while running."""
        ...


class ClockFields(Protocol):
    """The four-and-a-bit ``simulation`` columns a clock is built from.

    Structural, so ``SimulationClockConfig.from_row`` accepts the ORM row
    without ``app.core.clock`` importing ``app.core.models`` -- the clock stays
    usable in tests with no database in sight.
    """

    status: str
    virtual_epoch: datetime
    resumed_at_wall: datetime
    paused_at_virtual: datetime | None
    speed_multiplier: float


#: How long a paused clock's ``sleep`` blocks before returning, in real seconds.
#: Short enough that a resume is picked up promptly, long enough not to spin.
_PAUSED_POLL_REAL_S = 0.1


@dataclass(frozen=True, slots=True)
class SimulationClockConfig:
    """The snapshot a ``VirtualClock`` is built from.

    Deliberately a plain value object rather than the ORM row: a clock must be
    constructible in a worker that read these four fields once and detached, and
    it must be trivially constructible in a test.
    """

    status: SimStatus
    virtual_epoch: datetime
    resumed_at_wall: datetime
    paused_at_virtual: datetime | None
    speed_multiplier: float

    @classmethod
    def from_row(cls, row: ClockFields) -> SimulationClockConfig:
        """Snapshot a ``Simulation`` row (or anything with the same fields)."""
        return cls(
            status=SimStatus(row.status),
            virtual_epoch=row.virtual_epoch,
            resumed_at_wall=row.resumed_at_wall,
            paused_at_virtual=row.paused_at_virtual,
            speed_multiplier=float(row.speed_multiplier),
        )


class VirtualClock:
    """Virtual time derived from a ``simulation`` snapshot.

    A pure function of wall time once constructed, so it is cheap to hold and
    safe to share. Long-running loops refresh the snapshot each iteration to
    pick up pause / resume / speed changes.
    """

    __slots__ = ("_config",)

    def __init__(self, config: SimulationClockConfig) -> None:
        if config.speed_multiplier <= 0:
            raise ValueError("speed_multiplier must be positive")
        self._config = config

    @property
    def config(self) -> SimulationClockConfig:
        return self._config

    @property
    def speed_multiplier(self) -> float:
        return self._config.speed_multiplier

    @property
    def is_paused(self) -> bool:
        return self._config.status is SimStatus.PAUSED

    def now(self) -> datetime:
        c = self._config
        if c.status is SimStatus.PAUSED:
            # A paused clock is frozen at the virtual time of the pause. If the
            # column is somehow unset, fall back to the epoch rather than
            # letting time run on -- a frozen clock is the safe wrong answer.
            return c.paused_at_virtual if c.paused_at_virtual is not None else c.virtual_epoch
        elapsed_real = (wall_now() - c.resumed_at_wall).total_seconds()
        return c.virtual_epoch + timedelta(seconds=elapsed_real * c.speed_multiplier)

    async def sleep(self, virtual_seconds: float) -> None:
        """Sleep ``virtual_seconds`` of virtual time, in real time.

        A paused clock makes no progress, so a virtual sleep would never end.
        Callers poll instead: this returns after a short real delay so the loop
        can re-read the config and notice a resume.
        """
        if virtual_seconds <= 0:
            return
        if self._config.status is SimStatus.PAUSED:
            await asyncio.sleep(_PAUSED_POLL_REAL_S)
            return
        await asyncio.sleep(virtual_seconds / self._config.speed_multiplier)

    def to_virtual_seconds(self, virtual_time: datetime) -> float:
        """Virtual time as seconds since :data:`VIRTUAL_EPOCH_ZERO`.

        The metrics bucket key, and what the UI labels its x-axis with.
        """
        return (virtual_time - VIRTUAL_EPOCH_ZERO).total_seconds()

    def elapsed_virtual_s(self) -> float:
        """Virtual seconds since the start of the run."""
        return self.to_virtual_seconds(self.now())


class WallClock:
    """Production's clock: the same math with ``speed_multiplier = 1``.

    Nothing else in the system changes when this is swapped in, because time is
    only ever read through the :class:`Clock` protocol.
    """

    __slots__ = ()

    @property
    def speed_multiplier(self) -> float:
        return 1.0

    def now(self) -> datetime:
        return wall_now()

    async def sleep(self, virtual_seconds: float) -> None:
        if virtual_seconds > 0:
            await asyncio.sleep(virtual_seconds)


# ---------------------------------------------------------------------------
# Epoch arithmetic for the control plane
# ---------------------------------------------------------------------------
#
# Pause, resume and speed-change are all the same operation: rebase the epoch so
# that elapsed virtual time is continuous across the change. Getting this wrong
# is invisible -- the clock keeps returning plausible timestamps -- so it lives
# in three named functions with tests rather than inline in a route handler.


def start_config(speed_multiplier: float, at_wall: datetime | None = None) -> SimulationClockConfig:
    """A fresh, running clock at virtual time zero."""
    return SimulationClockConfig(
        status=SimStatus.RUNNING,
        virtual_epoch=VIRTUAL_EPOCH_ZERO,
        resumed_at_wall=at_wall if at_wall is not None else wall_now(),
        paused_at_virtual=None,
        speed_multiplier=speed_multiplier,
    )


def pause(config: SimulationClockConfig, at_wall: datetime | None = None) -> SimulationClockConfig:
    """Freeze the clock, recording the virtual time at which it froze.

    Idempotent: pausing an already-paused clock is a no-op, so a double-click on
    the pause button cannot lose time.
    """
    if config.status is SimStatus.PAUSED:
        return config
    frozen_at = VirtualClock(config).now() if at_wall is None else _now_at(config, at_wall)
    return SimulationClockConfig(
        status=SimStatus.PAUSED,
        virtual_epoch=config.virtual_epoch,
        resumed_at_wall=config.resumed_at_wall,
        paused_at_virtual=frozen_at,
        speed_multiplier=config.speed_multiplier,
    )


def resume(config: SimulationClockConfig, at_wall: datetime | None = None) -> SimulationClockConfig:
    """Restart the clock from where it froze -- no jump across the pause.

    The new epoch is the frozen virtual time, and the new wall reference is now,
    so the pause interval contributes zero virtual seconds.
    """
    if config.status is not SimStatus.PAUSED:
        return config
    resumed_at = at_wall if at_wall is not None else wall_now()
    frozen_at = config.paused_at_virtual if config.paused_at_virtual is not None else config.virtual_epoch
    return SimulationClockConfig(
        status=SimStatus.RUNNING,
        virtual_epoch=frozen_at,
        resumed_at_wall=resumed_at,
        paused_at_virtual=None,
        speed_multiplier=config.speed_multiplier,
    )


def set_speed(
    config: SimulationClockConfig,
    speed_multiplier: float,
    at_wall: datetime | None = None,
) -> SimulationClockConfig:
    """Change the multiplier without discontinuity in virtual time.

    The epoch is rebased to the virtual time *at the moment of the change*, so
    the new rate applies from here forward and elapsed virtual time is
    continuous. Rebasing is the whole trick: leave the epoch alone and every
    timestamp already in the database is retroactively reinterpreted.
    """
    if speed_multiplier <= 0:
        raise ValueError("speed_multiplier must be positive")
    at = at_wall if at_wall is not None else wall_now()

    if config.status is SimStatus.PAUSED:
        # Frozen: nothing to rebase, the new rate takes effect on resume.
        return SimulationClockConfig(
            status=config.status,
            virtual_epoch=config.virtual_epoch,
            resumed_at_wall=config.resumed_at_wall,
            paused_at_virtual=config.paused_at_virtual,
            speed_multiplier=speed_multiplier,
        )

    return SimulationClockConfig(
        status=config.status,
        virtual_epoch=_now_at(config, at),
        resumed_at_wall=at,
        paused_at_virtual=None,
        speed_multiplier=speed_multiplier,
    )


def _now_at(config: SimulationClockConfig, at_wall: datetime) -> datetime:
    """Virtual time this config reads at a given wall time. Testable without sleeping."""
    if config.status is SimStatus.PAUSED:
        return config.paused_at_virtual if config.paused_at_virtual is not None else config.virtual_epoch
    elapsed_real = (at_wall - config.resumed_at_wall).total_seconds()
    return config.virtual_epoch + timedelta(seconds=elapsed_real * config.speed_multiplier)


__all__ = [
    "VIRTUAL_EPOCH_ZERO",
    "Clock",
    "SimulationClockConfig",
    "VirtualClock",
    "WallClock",
    "pause",
    "resume",
    "set_speed",
    "start_config",
    "wall_now",
]
