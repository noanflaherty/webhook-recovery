"""The DATABASE_URL normalizer.

A classic twenty-minute deployment loss, and five minutes to prevent: Railway
injects ``postgresql://...``, SQLAlchemy needs an explicit async driver, and
asyncpg raises on the ``sslmode`` query parameter libpq-style URLs carry.
"""

from __future__ import annotations

import pytest

from app.core.settings import normalize_database_url, sync_database_url


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        # What Railway actually injects.
        (
            "postgresql://u:p@host.railway.internal:5432/railway",
            "postgresql+asyncpg://u:p@host.railway.internal:5432/railway",
        ),
        # Heroku's older spelling.
        ("postgres://u:p@h:5432/db", "postgresql+asyncpg://u:p@h:5432/db"),
        # The parameter that breaks asyncpg.
        (
            "postgresql://u:p@h:5432/db?sslmode=require",
            "postgresql+asyncpg://u:p@h:5432/db",
        ),
        # Several libpq-only parameters at once, one meaningful one kept.
        (
            "postgresql://u:p@h/db?sslmode=verify-full&connect_timeout=10&application_name=x",
            "postgresql+asyncpg://u:p@h/db",
        ),
        # Idempotent: already normalized, passes through untouched.
        ("postgresql+asyncpg://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
        # Already normalized but still carrying the bad parameter.
        ("postgresql+asyncpg://u:p@h/db?sslmode=require", "postgresql+asyncpg://u:p@h/db"),
        # postgres+asyncpg is a legal spelling SQLAlchemy does not accept.
        ("postgres+asyncpg://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
    ],
)
def test_normalize_database_url(given: str, expected: str) -> None:
    assert normalize_database_url(given) == expected


def test_normalize_preserves_password_special_characters() -> None:
    """A percent-encoded password must survive the round trip intact."""
    url = "postgresql://user:p%40ss%2Fword@host:5432/db"
    assert normalize_database_url(url) == "postgresql+asyncpg://user:p%40ss%2Fword@host:5432/db"


def test_normalize_rejects_non_postgres() -> None:
    with pytest.raises(ValueError, match="Not a Postgres URL"):
        normalize_database_url("mysql://u:p@h/db")


def test_sync_url_swaps_the_driver() -> None:
    assert sync_database_url("postgresql://u:p@h/db?sslmode=require") == "postgresql+psycopg2://u:p@h/db"
