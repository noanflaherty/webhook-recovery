#!/usr/bin/env bash
# Check a running stack against the Phase 0 exit criteria.
#
#   ./scripts/verify.sh [base-url]
#
# Works against compose (the default) or a deployed Railway URL. The clock check
# is the one that matters: it is the only Phase 0 output three separate
# processes have to agree on, and a wrong answer is invisible until Phase 2,
# where it presents as a fairness bug rather than a clock bug.
set -uo pipefail

BASE="${1:-http://localhost:8000}"
EXPECTED_PROCESSES="${EXPECTED_PROCESSES:-4}"
failures=0

pass() { printf '   \033[32mOK\033[0m  %s\n' "$1"; }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$1"; failures=$((failures + 1)); }

field() { python3 -c 'import sys,json; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }

# --------------------------------------------------------------------------
echo "== health"
HEALTH=$(curl -sf "$BASE/api/health") || { fail "GET /api/health did not respond"; exit 1; }
echo "   $HEALTH"
[ "$(printf '%s' "$HEALTH" | field status)" = "ok" ] && pass "api up" || fail "api not ok"
[ "$(printf '%s' "$HEALTH" | field db)" = "ok" ] && pass "database reachable" || fail "database not ok"

# --------------------------------------------------------------------------
echo
echo "== processes"
PROCS=$(curl -sf "$BASE/api/process")
printf '%s' "$PROCS" | python3 -c '
import sys, json
rows = json.load(sys.stdin)
for p in rows:
    leader = "  (leader)" if p["is_leader"] else ""
    print("   %-9s %-14s pid %-5s %5.1fs ago%s"
          % (p["kind"], p["hostname"], p["pid"], p["heartbeat_age_s"], leader))
'
N=$(printf '%s' "$PROCS" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
# Report the actual mix rather than assuming it. Replica counts differ between
# compose and the deployment, and a hardcoded "1 conductor + N workers" label
# was printing a breakdown that contradicted the rows listed directly above it.
BREAKDOWN=$(printf '%s' "$PROCS" | python3 -c '
import sys, json, collections
counts = collections.Counter(p["kind"] for p in json.load(sys.stdin))
print(", ".join(f"{n} {kind}" + ("s" if n != 1 else "") for kind, n in sorted(counts.items())))
')
if [ "$N" -eq "$EXPECTED_PROCESSES" ]; then
  pass "$N live ($BREAKDOWN)"
else
  fail "expected $EXPECTED_PROCESSES live processes, got $N ($BREAKDOWN)"
fi

STALE=$(printf '%s' "$PROCS" | python3 -c '
import sys, json
print(sum(1 for p in json.load(sys.stdin) if p["heartbeat_age_s"] > 15))
')
[ "$STALE" -eq 0 ] && pass "all heartbeats inside the 15s window" \
                   || fail "$STALE processes returned past the liveness window"

# --------------------------------------------------------------------------
echo
echo "== schema"
# Only when the target is the local stack. These read the compose database
# directly, so running them against a deployed URL would silently verify
# localhost and report it as if it were the deployment.
case "$BASE" in
  http://localhost:*|http://127.0.0.1:*) LOCAL_TARGET=1 ;;
  *) LOCAL_TARGET=0 ;;
esac

if [ "$LOCAL_TARGET" = "0" ]; then
  echo "   (remote target -- schema is verified through the API's own migration,"
  echo "    which ran at deploy time; skipping direct psql checks)"
elif docker compose ps -q postgres >/dev/null 2>&1 && [ -n "$(docker compose ps -q postgres 2>/dev/null)" ]; then
  TABLES=$(docker compose exec -T postgres psql -qtA -U postgres -d webhook_recovery -c \
    "SELECT count(*) FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name <> 'alembic_version';" | tr -d '[:space:]')
  [ "$TABLES" = "9" ] && pass "9 tables" || fail "expected 9 tables, found $TABLES"

  PARTIAL=$(docker compose exec -T postgres psql -qtA -U postgres -d webhook_recovery -c \
    "SELECT indexname FROM pg_indexes
      WHERE tablename = 'delivery' AND indexdef LIKE '%WHERE%' ORDER BY indexname;")
  echo "$PARTIAL" | sed 's/^/   /'
  COUNT=$(printf '%s\n' "$PARTIAL" | grep -c . || true)
  [ "$COUNT" = "3" ] && pass "3 partial indexes on delivery" \
                     || fail "expected 3 partial indexes on delivery, found $COUNT"
else
  echo "   (compose postgres not running -- skipping direct schema checks)"
fi

# --------------------------------------------------------------------------
echo
echo "== clock"
if python3 ./scripts/check_clock.py "$BASE"; then :; else failures=$((failures + $?)); fi

# --------------------------------------------------------------------------
echo
echo "== served bundle"
CODE=$(curl -so /dev/null -w '%{http_code}' "$BASE/")
[ "$CODE" = "200" ] && pass "SPA index served" || fail "GET / returned $CODE"
CODE=$(curl -so /dev/null -w '%{http_code}' "$BASE/api/nope")
[ "$CODE" = "404" ] && pass "unknown /api path 404s rather than serving the shell" \
                    || fail "GET /api/nope returned $CODE, expected 404"

# --------------------------------------------------------------------------
echo
if [ "$failures" -eq 0 ]; then
  printf '\033[32mall checks passed\033[0m against %s\n' "$BASE"
else
  printf '\033[31m%d check(s) failed\033[0m against %s\n' "$failures" "$BASE"
fi
exit "$failures"
