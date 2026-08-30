"""Clock arithmetic.

The pause/resume rebase is the one piece of arithmetic in Phase 0 that is easy
to get wrong and invisible when it is: the clock keeps returning plausible
timestamps either way. It is also the only Phase 0 output that three separate
processes have to agree on, and a wrong answer here surfaces in Phase 2 as a
*fairness* bug rather than a clock bug.

Every test drives wall time explicitly through ``at_wall`` rather than sleeping,
so the assertions are exact rather than tolerance-based.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import (
    VIRTUAL_EPOCH_ZERO,
    SimulationClockConfig,
    VirtualClock,
    WallClock,
    pause,
    resume,
    set_speed,
    start_config,
)
from app.core.enums import SimStatus

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    """Wall time, ``seconds`` real seconds after T0."""
    return T0 + timedelta(seconds=seconds)


def virtual_s(config: SimulationClockConfig, wall: datetime) -> float:
    """Virtual seconds elapsed, as the clock would report them at ``wall``."""
    from app.core.clock import _now_at

    return (_now_at(config, wall) - VIRTUAL_EPOCH_ZERO).total_seconds()


# ---------------------------------------------------------------------------
# The basic rate
# ---------------------------------------------------------------------------


def test_virtual_time_runs_at_the_multiplier() -> None:
    config = start_config(20.0, at_wall=T0)
    # One real second at 20x is twenty virtual seconds.
    assert virtual_s(config, at(1)) == pytest.approx(20.0)
    assert virtual_s(config, at(2.25)) == pytest.approx(45.0)


def test_a_fresh_clock_starts_at_virtual_zero() -> None:
    assert virtual_s(start_config(20.0, at_wall=T0), T0) == pytest.approx(0.0)


def test_speed_multiplier_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        VirtualClock(
            SimulationClockConfig(
                status=SimStatus.RUNNING,
                virtual_epoch=VIRTUAL_EPOCH_ZERO,
                resumed_at_wall=T0,
                paused_at_virtual=None,
                speed_multiplier=0.0,
            )
        )


# ---------------------------------------------------------------------------
# Pause: the clock freezes
# ---------------------------------------------------------------------------


def test_pause_freezes_the_clock() -> None:
    config = start_config(20.0, at_wall=T0)
    paused = pause(config, at_wall=at(1))  # 20 virtual seconds in

    assert paused.status is SimStatus.PAUSED
    assert virtual_s(paused, at(1)) == pytest.approx(20.0)
    # Ten more real seconds pass. The clock must not move.
    assert virtual_s(paused, at(11)) == pytest.approx(20.0)


def test_pause_is_idempotent() -> None:
    """A double-click on the pause button must not lose time."""
    config = pause(start_config(20.0, at_wall=T0), at_wall=at(1))
    twice = pause(config, at_wall=at(5))
    assert twice == config
    assert virtual_s(twice, at(30)) == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Resume: no jump across the pause
# ---------------------------------------------------------------------------


def test_resume_does_not_jump_across_the_pause() -> None:
    """The pause interval must contribute exactly zero virtual seconds."""
    config = start_config(20.0, at_wall=T0)
    config = pause(config, at_wall=at(1))  # frozen at 20 virtual s
    config = resume(config, at_wall=at(60))  # 59 real seconds of pause

    # Immediately on resume, still 20 -- not 20 + 59*20.
    assert virtual_s(config, at(60)) == pytest.approx(20.0)
    # And it runs again from there at the same rate.
    assert virtual_s(config, at(61)) == pytest.approx(40.0)


def test_resume_on_a_running_clock_is_a_no_op() -> None:
    config = start_config(20.0, at_wall=T0)
    assert resume(config, at_wall=at(5)) == config


def test_pause_resume_cycles_accumulate_only_running_time() -> None:
    config = start_config(10.0, at_wall=T0)
    config = pause(config, at_wall=at(2))  # +20 virtual
    config = resume(config, at_wall=at(100))
    config = pause(config, at_wall=at(103))  # +30 virtual
    config = resume(config, at_wall=at(500))

    # 2 + 3 = 5 real seconds of running time at 10x.
    assert virtual_s(config, at(500)) == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Speed change: the epoch is rebased, elapsed virtual time is continuous
# ---------------------------------------------------------------------------


def test_speed_change_rebases_the_epoch() -> None:
    config = start_config(1.0, at_wall=T0)
    # 10 real seconds at 1x = 10 virtual seconds.
    assert virtual_s(config, at(10)) == pytest.approx(10.0)

    config = set_speed(config, 20.0, at_wall=at(10))
    # Continuous at the moment of the change: still 10, not rescaled to 200.
    assert virtual_s(config, at(10)) == pytest.approx(10.0)
    # And the new rate applies from here forward.
    assert virtual_s(config, at(11)) == pytest.approx(30.0)


def test_speed_change_while_paused_takes_effect_on_resume() -> None:
    config = start_config(1.0, at_wall=T0)
    config = pause(config, at_wall=at(10))  # frozen at 10 virtual s
    config = set_speed(config, 20.0, at_wall=at(12))

    assert virtual_s(config, at(30)) == pytest.approx(10.0)  # still frozen
    config = resume(config, at_wall=at(30))
    assert virtual_s(config, at(30)) == pytest.approx(10.0)  # no jump
    assert virtual_s(config, at(31)) == pytest.approx(30.0)  # new rate


def test_slowing_down_is_also_continuous() -> None:
    config = start_config(20.0, at_wall=T0)
    config = set_speed(config, 1.0, at_wall=at(5))  # 100 virtual s so far
    assert virtual_s(config, at(5)) == pytest.approx(100.0)
    assert virtual_s(config, at(15)) == pytest.approx(110.0)


def test_speed_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        set_speed(start_config(1.0, at_wall=T0), 0.0, at_wall=at(1))


# ---------------------------------------------------------------------------
# The sleep half of the protocol
# ---------------------------------------------------------------------------


async def test_sleep_is_scaled_by_the_multiplier() -> None:
    """200 virtual ms at 20x is 10 real ms -- what makes leases mean what they say."""
    clock = VirtualClock(start_config(20.0))
    loop = asyncio.get_running_loop()
    started = loop.time()
    await clock.sleep(0.2)
    elapsed = loop.time() - started

    assert 0.005 <= elapsed < 0.15


async def test_sleep_on_a_paused_clock_returns_rather_than_hanging() -> None:
    """A paused clock makes no progress, so a virtual sleep would never end."""
    clock = VirtualClock(pause(start_config(20.0)))
    await asyncio.wait_for(clock.sleep(3600.0), timeout=1.0)


async def test_zero_sleep_is_free() -> None:
    clock = VirtualClock(start_config(20.0))
    await clock.sleep(0)
    await clock.sleep(-1)


# ---------------------------------------------------------------------------
# WallClock: the same class at 1x
# ---------------------------------------------------------------------------


async def test_wall_clock_is_the_same_shape_at_1x() -> None:
    clock = WallClock()
    assert clock.speed_multiplier == 1.0

    before = clock.now()
    await clock.sleep(0.01)
    assert clock.now() >= before
    assert clock.now().tzinfo is not None


def test_both_clocks_satisfy_the_protocol() -> None:
    from app.core.clock import Clock

    assert isinstance(WallClock(), Clock)
    assert isinstance(VirtualClock(start_config(20.0)), Clock)


# ---------------------------------------------------------------------------
# The snapshot boundary
# ---------------------------------------------------------------------------


def test_config_round_trips_through_a_row_like_object() -> None:
    """What every process does on each loop iteration: re-snapshot the row."""

    class Row:
        status = "running"
        virtual_epoch = VIRTUAL_EPOCH_ZERO
        resumed_at_wall = T0
        paused_at_virtual = None
        speed_multiplier = 20.0

    config = SimulationClockConfig.from_row(Row())
    assert config.status is SimStatus.RUNNING
    assert virtual_s(config, at(1)) == pytest.approx(20.0)


def test_a_paused_row_with_no_frozen_time_stops_rather_than_runs() -> None:
    """The safe wrong answer for a malformed row is a stopped clock, not a racing one."""
    config = SimulationClockConfig(
        status=SimStatus.PAUSED,
        virtual_epoch=VIRTUAL_EPOCH_ZERO + timedelta(seconds=7),
        resumed_at_wall=T0,
        paused_at_virtual=None,
        speed_multiplier=20.0,
    )
    assert virtual_s(config, at(1000)) == pytest.approx(7.0)


def test_two_processes_reading_the_same_snapshot_agree() -> None:
    """The whole point of a derived clock: no coordination, same answer."""
    config = start_config(20.0, at_wall=T0)
    api = VirtualClock(config)
    worker = VirtualClock(SimulationClockConfig.from_row(_RowOf(config)))

    # Same pure function of wall time, so the only difference is when each was
    # called -- sub-millisecond, and the same skew production already lives with.
    assert abs((api.now() - worker.now()).total_seconds()) < 0.01


class _RowOf:
    def __init__(self, config: SimulationClockConfig) -> None:
        self.status = config.status.value
        self.virtual_epoch = config.virtual_epoch
        self.resumed_at_wall = config.resumed_at_wall
        self.paused_at_virtual = config.paused_at_virtual
        self.speed_multiplier = config.speed_multiplier
