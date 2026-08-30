"""The kill flag: asking a process to die in a way that leaves work stranded.

This exists to give :mod:`app.conductor.reaper` something to reclaim. That makes
*where* a process acts on the flag the whole point, and the reason it is tested
rather than commented: a worker that exits at an arbitrary moment strands
nothing at all. A batch is a couple of milliseconds inside a 20ms loop, so an
arbitrary moment is overwhelmingly likely to be an idle one, and a chaos control
that usually does nothing is worse than none -- it teaches you to trust a
recovery path you have not actually exercised.

So the death is taken between the committed claim and the completion that
answers it, where a full batch of leases is stranded every time.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.ingest import EventSpec, ingest_events
from app.conductor.admission import mark_ready
from app.core import registry
from app.core.clock import VIRTUAL_EPOCH_ZERO, wall_now
from app.core.enums import DeliveryState, ProcessKind
from app.core.models import Attempt, Delivery, Process, Simulation
from app.core.runner import ProcessRunner
from app.core.scenario import seed_simulation
from app.worker import service as worker_service
from app.worker.service import _SIM_COLUMNS, Worker
from tests.conftest import a_simulation, requires_db

pytestmark = requires_db

INGEST_AT = VIRTUAL_EPOCH_ZERO - timedelta(seconds=60)


class Killed(Exception):
    """Stands in for ``os._exit``.

    A spy that merely *records* the call would let execution fall through into
    the completion transaction and the test would pass against code the real
    ``os._exit`` would never reach. Raising is the closest a test can get to
    "does not return".
    """


def _runner_that_records(crashes: list[int]) -> ProcessRunner:
    def crash_action(code: int) -> None:
        crashes.append(code)
        raise Killed(code)

    async def _noop(_: uuid.UUID) -> None:
        return None

    return ProcessRunner(ProcessKind.WORKER, _noop, interval_s=0.001, crash_action=crash_action)


def _a_process(**overrides: object) -> Process:
    now = wall_now()
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "kind": ProcessKind.WORKER.value,
        "hostname": "chaos-test-host",
        "pid": 1,
        "started_at_wall": now,
        "last_heartbeat_wall": now,
        "is_leader": False,
        "crash_requested": False,
    }
    fields.update(overrides)
    return Process(**fields)


@pytest.fixture
def _registry_on_the_fixture_session(session: AsyncSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point ``registry``'s own transactions at the rolled-back session.

    ``heartbeat`` opens its own ``session_scope``, which would reach the
    configured database rather than the test one. The SQL is the thing worth
    testing -- the flag is read back through the write's ``RETURNING`` -- so it
    is redirected rather than reimplemented.
    """

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        yield session

    monkeypatch.setattr(registry, "session_scope", _scope)


# ---------------------------------------------------------------------------
# The flag, and the heartbeat that carries it
# ---------------------------------------------------------------------------


async def test_a_heartbeat_reads_the_kill_flag_back(
    session: AsyncSession, _registry_on_the_fixture_session: None
) -> None:
    """The flag costs no round trip of its own: it rides on the heartbeat write."""
    process = _a_process()
    session.add(process)
    await session.flush()

    assert await registry.heartbeat(process.id) is False

    process.crash_requested = True
    await session.flush()

    assert await registry.heartbeat(process.id) is True


async def test_a_heartbeat_still_stamps_liveness_when_it_carries_a_kill(
    session: AsyncSession, _registry_on_the_fixture_session: None
) -> None:
    """``RETURNING`` is added to the write, not substituted for it.

    A process asked to die is still alive until it acts, and must go on looking
    alive: it is about to strand a batch, and the strip is where that is read.
    """
    stamped_at = wall_now() - timedelta(seconds=90)
    process = _a_process(last_heartbeat_wall=stamped_at, crash_requested=True)
    session.add(process)
    await session.flush()

    await registry.heartbeat(process.id, is_leader=True)
    await session.refresh(process)

    assert process.last_heartbeat_wall > stamped_at
    assert process.is_leader is True


async def test_the_heartbeat_loop_raises_the_event_it_is_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop only raises the flag; acting on it belongs to the loop body."""
    stop = asyncio.Event()
    crash = asyncio.Event()

    async def _flagged(process_id: uuid.UUID, *, is_leader: bool = False) -> bool:
        stop.set()
        return True

    monkeypatch.setattr(registry, "heartbeat", _flagged)
    await registry.heartbeat_loop(uuid.uuid4(), stop, on_crash_requested=crash.set)

    assert crash.is_set()


async def test_dying_is_not_a_graceful_stop_in_disguise() -> None:
    """``die`` must not go through the drain, or it strands nothing."""
    crashes: list[int] = []
    runner = _runner_that_records(crashes)

    with pytest.raises(Killed):
        runner.die()

    assert crashes == [1]
    assert not runner.stop.is_set()


# ---------------------------------------------------------------------------
# Where a worker acts on it
# ---------------------------------------------------------------------------


async def test_a_killed_worker_strands_the_batch_it_just_claimed(
    session: AsyncSession, connection: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The claim is committed and the completion never runs. That is the point.

    Anywhere earlier and there is nothing to strand; anywhere later and the
    attempts have already been recorded against real outcomes.
    """
    sim = a_simulation()
    sim.outage_override = False
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await ingest_events(
        session,
        sim,
        [
            EventSpec(event_type="invoice.paid", entity_key=f"ent_{i}", occurred_at=INGEST_AT)
            for i in range(3)
        ],
    )
    await session.flush()

    ids = [
        row[0]
        for row in await connection.execute(
            select(Delivery.id).where(Delivery.simulation_id == sim.id).order_by(Delivery.id)
        )
    ]
    await mark_ready(connection, ids, VIRTUAL_EPOCH_ZERO)

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        yield session

    monkeypatch.setattr(worker_service, "session_scope", _scope)

    crashes: list[int] = []
    runner = _runner_that_records(crashes)
    runner.crash.set()
    worker = Worker(runner=runner)

    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == sim.id))).one()
    with pytest.raises(Killed):
        await worker._drain(sim_row, uuid.uuid4())

    assert crashes == [1]

    leased = (
        await connection.execute(
            select(Delivery.state, Delivery.leased_by, Delivery.lease_expires_at).where(
                Delivery.simulation_id == sim.id
            )
        )
    ).all()
    assert [row.state for row in leased] == [DeliveryState.IN_FLIGHT.value] * len(ids)
    assert all(row.leased_by is not None and row.lease_expires_at is not None for row in leased)

    # And the attempts are open, which is what the reaper closes.
    outcomes = (
        await connection.execute(
            select(Attempt.outcome, Attempt.finished_at).where(Attempt.simulation_id == sim.id)
        )
    ).all()
    assert len(outcomes) == len(ids)
    assert all(row.outcome is None and row.finished_at is None for row in outcomes)


async def test_a_worker_with_nothing_to_strand_holds_out_first(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty iteration is not an excuse to die cleanly.

    Dying between batches strands nothing, and at a 20ms loop most iterations
    claim nothing -- so honouring the kill on the first empty pass would make
    the mid-batch death the exception rather than the rule, which is the whole
    point of the control lost to a one-line convenience.
    """

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        yield session

    monkeypatch.setattr(worker_service, "session_scope", _scope)

    async def _idle(self: Worker, sim: object, worker_id: uuid.UUID) -> None:
        return None

    monkeypatch.setattr(Worker, "_drain", _idle)

    crashes: list[int] = []
    runner = _runner_that_records(crashes)
    runner.crash.set()

    # Well inside the grace period, so it waits.
    await Worker(runner=runner).run_once(uuid.uuid4())

    assert crashes == []


async def test_an_idle_worker_dies_once_it_gives_up_waiting(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """But it cannot hold out forever.

    A worker with no running simulation, a paused run, or a long unlucky streak
    of empty claims would otherwise sit heartbeating with the flag raised while
    the UI says `killing…` and nothing happens -- which is the state a
    deployment spends most of its time in, and the one anyone poking the control
    is most likely to be looking at.
    """
    monkeypatch.setattr(worker_service, "_CRASH_GRACE_S", 0.0)

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        yield session

    monkeypatch.setattr(worker_service, "session_scope", _scope)

    # Stubbed rather than relying on there being no running simulations: this
    # database is shared with whatever else is running against it, and a test
    # that passes because `_drain` happened to find work would be asserting the
    # opposite of what it says.
    async def _idle(self: Worker, sim: object, worker_id: uuid.UUID) -> None:
        return None

    monkeypatch.setattr(Worker, "_drain", _idle)

    crashes: list[int] = []
    runner = _runner_that_records(crashes)
    runner.crash.set()
    worker = Worker(runner=runner)

    # The first pass starts the clock; the second finds it already expired.
    await worker.run_once(uuid.uuid4())
    with pytest.raises(Killed):
        await worker.run_once(uuid.uuid4())

    assert crashes == [1]


async def test_a_worker_with_no_runner_cannot_be_killed(
    session: AsyncSession, connection: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every test and every direct use constructs a ``Worker`` without a runner."""
    sim = a_simulation()
    sim.outage_override = False
    session.add(sim)
    await session.flush()
    await seed_simulation(session, sim.id)
    await session.flush()

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        yield session

    monkeypatch.setattr(worker_service, "session_scope", _scope)

    sim_row = (await connection.execute(select(*_SIM_COLUMNS).where(Simulation.id == sim.id))).one()
    await Worker()._drain(sim_row, uuid.uuid4())


# ---------------------------------------------------------------------------
# The route that raises it
# ---------------------------------------------------------------------------


async def test_kill_raises_the_flag_without_touching_the_process(
    client: AsyncClient, session: AsyncSession
) -> None:
    """The api cannot reach another container and does not try.

    It writes a flag the target reads on the heartbeat it was already making,
    so the response says "asked", never "dead" -- the process is still live and
    still heartbeating at the moment this returns.
    """
    process = _a_process()
    session.add(process)
    await session.flush()

    response = await client.post(f"/api/process/{process.id}/kill")

    assert response.status_code == 200
    assert response.json()["id"] == str(process.id)
    await session.refresh(process)
    assert process.crash_requested is True


async def test_killing_an_unknown_process_is_a_404(client: AsyncClient) -> None:
    response = await client.post(f"/api/process/{uuid.uuid4()}/kill")
    assert response.status_code == 404
