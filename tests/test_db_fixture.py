"""Proof that the transactional session fixture actually works.

Not a test of the product -- a test of the test infrastructure. The scheduler
tests all assume that rows do not leak between tests, and a rollback fixture
that is never itself asserted on is a guess.

Skipped when no Postgres is reachable, so `uv run pytest` stays green on a
machine with nothing running.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import start_config, wall_now
from app.core.models import Simulation
from tests.conftest import requires_db

pytestmark = requires_db


def _a_simulation() -> Simulation:
    config = start_config(20.0)
    return Simulation(
        id=uuid.uuid4(),
        created_at_wall=wall_now(),
        scenario_name="test",
        status=config.status.value,
        virtual_epoch=config.virtual_epoch,
        resumed_at_wall=config.resumed_at_wall,
        paused_at_virtual=None,
        speed_multiplier=config.speed_multiplier,
        fair_drain_enabled=True,
        global_attempts_per_s=30.0,
        outage_override=None,
    )


async def test_a_committed_row_is_visible_within_the_test(session: AsyncSession) -> None:
    sim = _a_simulation()
    session.add(sim)
    # A real commit -- code under test must not have to know it is in a fixture.
    await session.commit()

    found = await session.get(Simulation, sim.id)
    assert found is not None
    assert found.scenario_name == "test"


async def test_the_previous_test_left_nothing_behind(session: AsyncSession) -> None:
    """The rollback guarantee, asserted rather than assumed."""
    leaked = await session.scalar(
        select(func.count()).select_from(Simulation).where(Simulation.scenario_name == "test")
    )
    assert leaked == 0


async def test_check_constraints_are_live(session: AsyncSession) -> None:
    """The enums really are TEXT + CHECK, not just Python-side StrEnums."""
    from sqlalchemy.exc import IntegrityError

    sim = _a_simulation()
    sim.status = "nonsense"
    session.add(sim)
    with pytest.raises(IntegrityError):
        await session.commit()
