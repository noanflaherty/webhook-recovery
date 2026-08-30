"""Process configuration.

Everything tunable lives here rather than in code, so retuning the scheduler is
an env change rather than an edit.
"""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic_settings import BaseSettings, SettingsConfigDict

# asyncpg does not accept libpq's connection query parameters; it takes ``ssl``
# through connect_args instead. Managed Postgres providers (Railway, Heroku,
# Supabase) routinely append these to the URL they inject.
_LIBPQ_ONLY_PARAMS = frozenset(
    {
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "sslcrl",
        "channel_binding",
        "target_session_attrs",
        "connect_timeout",
        "application_name",
        "options",
        "gssencmode",
    }
)

_ASYNC_DRIVER = "postgresql+asyncpg"


def normalize_database_url(url: str) -> str:
    """Coerce a libpq-style Postgres URL into one SQLAlchemy + asyncpg accepts.

    Railway injects ``postgresql://user:pw@host:5432/db``, sometimes with
    ``?sslmode=require``. SQLAlchemy needs an explicit async driver in the
    scheme, and asyncpg raises ``TypeError: connect() got an unexpected keyword
    argument 'sslmode'`` on the query parameters libpq tolerates.

    Idempotent: a URL that is already ``postgresql+asyncpg://`` passes through
    with only its libpq-only parameters stripped.
    """
    parts = urlsplit(url)

    scheme = parts.scheme
    if scheme in ("postgres", "postgresql"):
        scheme = _ASYNC_DRIVER
    elif scheme.startswith("postgresql+") or scheme.startswith("postgres+"):
        # An explicit driver was requested; only normalize the dialect spelling.
        _, _, driver = scheme.partition("+")
        scheme = f"postgresql+{driver}"
    else:
        raise ValueError(f"Not a Postgres URL: {url!r}")

    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in _LIBPQ_ONLY_PARAMS]
    query = urlencode(kept)

    return urlunsplit((scheme, parts.netloc, parts.path, query, parts.fragment))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Connection -------------------------------------------------------
    database_url: str = "postgresql://postgres:postgres@localhost:5432/webhook_recovery"
    db_pool_size: int = 5
    db_max_overflow: int = 5
    db_echo: bool = False

    # --- HTTP -------------------------------------------------------------
    host: str = "0.0.0.0"  # containers bind all interfaces
    port: int = 8000
    frontend_dist: str = "frontend/dist"

    # --- Process registry -------------------------------------------------
    heartbeat_interval_s: float = 3.0
    #: A process is "live" in GET /api/process if it heartbeat within this
    #: window. Liveness is a read-time filter, never a reaper: stale rows from
    #: prior deploys accumulate harmlessly and are never read.
    process_liveness_window_s: float = 15.0

    # --- Conductor --------------------------------------------------------
    #: The admission ceiling is `ready buffer depth / this interval` real
    #: attempts per second, and the buffer must stay shallow (its depth *is* the
    #: granularity of fairness), so the interval is the only free variable. At
    #: 1.5 x 24 slots this is ~720/s against ~600/s of demand at 20x. Raise the
    #: interval and the backlog stops draining -- which presents as a scheduler
    #: bug, not as a tuning problem, so it is worth knowing this is the knob.
    conductor_loop_interval_s: float = 0.05
    #: Ready-buffer depth as a multiple of the sum of consumer concurrency caps.
    #: Shallow enough that admission decisions don't age; deep enough that
    #: workers never starve waiting on the conductor.
    ready_buffer_depth_multiplier: float = 1.5
    fairness_window_virtual_s: float = 5.0
    #: How many candidates a pass fetches per admittable slot.
    #:
    #: Fairness rations attempts; a policy drop is not an attempt. So a pass has
    #: to look past what it could admit to find work that survives policy --
    #: otherwise a consumer whose backlog is mostly superseded (Bolt's, during
    #: recovery) can never fill its share. The extra rows are not wasted: policy
    #: condemns them now rather than next pass, which is what makes a coalesced
    #: backlog collapse rather than trickle.
    admission_overfetch: int = 8
    metrics_bucket_virtual_s: float = 1.0
    #: Ceiling on how many buckets one metrics pass will backfill. A conductor
    #: gap (failover, a redeploy) is caught up over several passes rather than
    #: one enormous write that stalls the loop.
    metrics_max_backfill_buckets: int = 300

    # --- Worker -----------------------------------------------------------
    worker_loop_interval_s: float = 0.02
    #: Deliveries claimed per transaction. Per-attempt round trips do not
    #: survive 20x; this is what makes an iteration two round trips instead of
    #: two per delivery.
    worker_batch_size: int = 16
    lease_duration_virtual_s: float = 30.0
    retry_backoff_base_virtual_s: float = 1.0
    retry_backoff_cap_virtual_s: float = 60.0
    max_attempts: int = 5

    # --- Producer ---------------------------------------------------------
    #: Real seconds between producer ticks. Each tick emits the events that fall
    #: in the virtual window since the last one, so this sets granularity, not
    #: volume -- volume is the event mix in app.core.scenario.
    producer_tick_interval_s: float = 0.1
    #: Ceiling on the virtual window one tick will emit for. Without it, an api
    #: restart against a long-running simulation emits the whole gap at once.
    producer_max_catchup_virtual_s: float = 5.0

    # --- Simulation defaults ---------------------------------------------
    default_speed_multiplier: float = 20.0
    default_scenario_name: str = "outage_recovery"

    @property
    def async_database_url(self) -> str:
        return normalize_database_url(self.database_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings: Settings = get_settings()

__all__ = [
    "Settings",
    "get_settings",
    "normalize_database_url",
    "settings",
]
