#!/bin/sh
# Migrate, then serve.
#
# A single argv for the platform to exec, rather than a compound
# `alembic ... && uvicorn ...` start command: Railway's start-command parser
# does its own variable interpolation and word splitting, which makes shell
# operators and `${VAR:-default}` expansion unreliable there. Everything that
# needs a shell happens inside this file, where a real /bin/sh runs it.
#
# Migrations run here, and only here, because `api` is pinned to one replica --
# the same "exactly once, from one place" guarantee compose gets from its
# one-shot migrate service. Workers and the conductor wait for the schema to
# appear rather than racing it (see app/core/runner.py).
set -e

: "${PORT:=8000}"

echo "start-api: migrating"
alembic upgrade head

echo "start-api: serving on 0.0.0.0:${PORT}"
exec uvicorn app.api.main:app --host 0.0.0.0 --port "$PORT"
