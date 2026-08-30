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
if [ "$N" -eq "$EXPECTED_PROCESSES" ]; then
  pass "$N live (1 conductor + $((EXPECTED_PROCESSES - 1)) workers)"
else
  fail "expected $EXPECTED_PROCESSES live processes, got $N"
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
if docker compose ps -q postgres >/dev/null 2>&1 && [ -n "$(docker compose ps -q postgres 2>/dev/null)" ]; then
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
SIM=$(curl -sf -X POST "$BASE/api/simulation" -H 'content-type: application/json' -d '{}' | field id)
echo "   simulation $SIM"

patch() { curl -sf -X PATCH "$BASE/api/simulation/$SIM" -H 'content-type: application/json' -d "$1" >/dev/null; }
read_s() { curl -sf "$BASE/api/simulation/$SIM" | field virtual_now_s; }

patch '{"speed_multiplier":20}'
A=$(read_s); sleep 1; B=$(read_s)
python3 -c "
d = $B - $A
print('   two reads one real second apart at 20x: %+.2f virtual s' % d)
raise SystemExit(0 if 18 < d < 23 else 1)
" && pass "virtual time advances at the multiplier" \
  || fail "virtual time did not advance at ~20x"

patch '{"status":"paused"}'
P=$(read_s); sleep 1; Q=$(read_s)
[ "$P" = "$Q" ] && pass "frozen while paused" || fail "clock moved while paused ($P -> $Q)"

patch '{"status":"running"}'
R=$(read_s)
python3 -c "
j = $R - $Q
print('   resume jump: %+.2f virtual s' % j)
raise SystemExit(0 if j < 2 else 1)
" && pass "no jump across the pause" || fail "time jumped across the pause"

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
