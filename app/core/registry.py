"""Process self-registration and heartbeats.

Observability only. It exists so a reviewer can see that three workers and a
conductor are really separate processes rather than taking the split on faith.
Nothing in the delivery path consults it, and leader election never reads it --
leadership is decided entirely by the Postgres advisory lock.

Liveness is a **read-time filter, not a reaper**: ``GET /api/process`` returns
rows whose ``last_heartbeat_wall`` is inside the window. Nothing reclaims a
stale row to decide liveness, which keeps the design's claim -- "it is not
required for correctness" -- literally true rather than approximately true.

Long-dead rows are still deleted on registration, but only to bound the table.
The original decision was that they "accumulate harmlessly"; that held for a
45-second demo and did not survive contact with a service that runs
continuously, where every restart adds a row and every process rewrites one
every few seconds. See :func:`_prune_dead`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

from sqlalchemy import delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import wall_now
from app.core.db import session_scope
from app.core.enums import ProcessKind
from app.core.models import Process
from app.core.settings import get_settings

log = logging.getLogger(__name__)

#: Rows older than this many liveness windows are dropped on the next
#: registration. Generous on purpose: recent history is useful when debugging a
#: restart, and the point is bounding growth, not tidiness.
PRUNE_WINDOW_MULTIPLE = 240


async def register_process(kind: ProcessKind, process_id: uuid.UUID | None = None) -> uuid.UUID:
    """Insert this process's row and return its id.

    The id is generated locally rather than by the database, so a booting worker
    knows what to stamp on its leases before its first round-trip.
    """
    pid = process_id or uuid.uuid4()
    now = wall_now()
    async with session_scope() as session:
        await _prune_dead(session, now)
        session.add(
            Process(
                id=pid,
                kind=kind.value,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                started_at_wall=now,
                last_heartbeat_wall=now,
                is_leader=False,
            )
        )
    log.info("registered %s process %s", kind.value, pid)
    return pid


async def _prune_dead(session: AsyncSession, now: datetime) -> None:
    """Drop rows that went stale long ago.

    ``GET /api/process`` is a read-time filter and never reads these, so this is
    not about correctness -- it is about the table not growing forever. The
    original decision ("stale rows accumulate harmlessly") assumed a run lasting
    45 seconds; deployed, every restart adds a row and every process rewrites
    one every few seconds, and a Postgres volume really did fill up.

    The cutoff is generous -- many multiples of the liveness window -- so rows
    stay around long enough to be useful when debugging a recent restart, and
    pruning on registration means no reaper task to own.
    """
    window = get_settings().process_liveness_window_s
    cutoff = now - timedelta(seconds=window * PRUNE_WINDOW_MULTIPLE)
    await session.execute(delete(Process).where(Process.last_heartbeat_wall < cutoff))


async def heartbeat(process_id: uuid.UUID, *, is_leader: bool = False) -> None:
    """Stamp one heartbeat."""
    async with session_scope() as session:
        await session.execute(
            update(Process)
            .where(Process.id == process_id)
            .values(last_heartbeat_wall=wall_now(), is_leader=is_leader)
        )


async def heartbeat_loop(
    process_id: uuid.UUID,
    stop: asyncio.Event,
    *,
    is_leader: Callable[[], bool] | None = None,
) -> None:
    """Heartbeat until ``stop`` is set.

    Heartbeats are wall-clock, not virtual: a paused simulation must not make
    live processes look dead. Failures are logged and swallowed -- a process
    that cannot heartbeat is still perfectly able to deliver webhooks, and
    taking it down over an observability write would be the tail wagging the dog.
    """
    interval = get_settings().heartbeat_interval_s
    while not stop.is_set():
        try:
            await heartbeat(process_id, is_leader=bool(is_leader and is_leader()))
        except Exception:
            log.warning("heartbeat failed for %s", process_id, exc_info=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


async def deregister_process(process_id: uuid.UUID) -> None:
    """Age a row out immediately on a graceful shutdown.

    Backdating the heartbeat past the liveness window is enough -- the read-time
    filter does the rest, and the row stays for post-hoc inspection.
    """
    window = get_settings().process_liveness_window_s
    stale = wall_now() - timedelta(seconds=window * 2)
    try:
        async with session_scope() as session:
            await session.execute(
                update(Process)
                .where(Process.id == process_id)
                .values(last_heartbeat_wall=stale, is_leader=False)
            )
    except Exception:
        log.warning("deregister failed for %s", process_id, exc_info=True)


__all__ = ["deregister_process", "heartbeat", "heartbeat_loop", "register_process"]
