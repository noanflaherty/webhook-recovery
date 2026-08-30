"""Lease reclamation: the reaper's index, its attempt outcome, and the kill flag.

Three changes, one feature. The reaper sweeps ``delivery`` for expired leases
(``ix_delivery_lease``), closes the ``attempt`` row the dead worker left open
(``lease_expired``), and ``process.crash_requested`` is how a process is asked
to die ungracefully so that there is something to reclaim.

``ck_attempt_outcome`` is dropped and recreated rather than altered: a ``CHECK``
has no ``ADD VALUE``, and rewriting the whole predicate is what makes the
downgrade exact rather than approximate.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OUTCOMES_BEFORE = "outcome IS NULL OR outcome IN ('ok', '5xx', 'timeout')"
_OUTCOMES_AFTER = "outcome IS NULL OR outcome IN ('ok', '5xx', 'timeout', 'lease_expired')"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_delivery_lease",
        "delivery",
        ["simulation_id", "lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("state = 'in_flight'"),
    )
    op.drop_constraint(op.f("ck_attempt_outcome"), "attempt", type_="check")
    op.create_check_constraint(op.f("ck_attempt_outcome"), "attempt", _OUTCOMES_AFTER)
    op.add_column(
        "process",
        sa.Column("crash_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    # The default was only ever needed to fill the existing rows. Dropping it
    # keeps the column's default where every other one in this schema lives --
    # in the model, not in the database.
    op.alter_column("process", "crash_requested", server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("process", "crash_requested")
    # Any row the reaper wrote fails the old constraint, so it has to go first.
    # `attempt` is per-simulation and reconstructible; a lease that expired is
    # not a fact worth blocking a rollback over.
    op.execute("UPDATE attempt SET outcome = NULL WHERE outcome = 'lease_expired'")
    op.drop_constraint(op.f("ck_attempt_outcome"), "attempt", type_="check")
    op.create_check_constraint(op.f("ck_attempt_outcome"), "attempt", _OUTCOMES_BEFORE)
    op.drop_index(
        "ix_delivery_lease",
        table_name="delivery",
        postgresql_where=sa.text("state = 'in_flight'"),
    )
