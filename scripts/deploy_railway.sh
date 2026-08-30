#!/usr/bin/env bash
# Deploy one Railway service and wait for the result.
#
#   ./scripts/deploy_railway.sh <service>
#
# `railway up --detach` starts an upload and returns immediately -- it does not
# wait for the deployment to become healthy, and it exits 0 even when the
# deployment goes on to fail. In CI that is indistinguishable from success, so
# this polls until the deployment reaches a terminal state and exits non-zero
# on anything but SUCCESS.
#
# Requires RAILWAY_TOKEN in the environment. It is never echoed.
set -euo pipefail

SERVICE="${1:?usage: deploy_railway.sh <service>}"
TIMEOUT_S="${DEPLOY_TIMEOUT_S:-900}"
POLL_S="${DEPLOY_POLL_S:-15}"

if [ -z "${RAILWAY_TOKEN:-}" ]; then
  echo "::error::RAILWAY_TOKEN is not set -- add it under Settings > Secrets > Actions"
  exit 1
fi

latest_status() {
  railway deployment list --service "$SERVICE" --json 2>/dev/null \
    | python3 -c 'import sys, json
rows = json.load(sys.stdin)
print(rows[0]["status"] if rows else "NONE")' 2>/dev/null || echo "UNKNOWN"
}

echo "==> deploying $SERVICE"
# --ci streams build logs and returns when the build finishes. Its exit code is
# not the verdict we want -- a green build can still fail to deploy -- so the
# status poll below decides, and a non-zero here only gets reported.
if ! railway up --service "$SERVICE" --ci; then
  echo "::warning::railway up returned non-zero for $SERVICE; checking deployment status"
fi

deadline=$((SECONDS + TIMEOUT_S))
status="UNKNOWN"
while [ $SECONDS -lt $deadline ]; do
  status=$(latest_status)
  case "$status" in
    BUILDING | DEPLOYING | INITIALIZING | QUEUED | WAITING | NONE | UNKNOWN)
      sleep "$POLL_S"
      ;;
    *)
      break
      ;;
  esac
done

echo "==> $SERVICE: $status"

if [ "$status" != "SUCCESS" ]; then
  echo "::error::deploying $SERVICE ended in $status"
  echo "--- build logs ---"
  railway logs --service "$SERVICE" -b --lines 60 2>&1 | tail -40 || true
  echo "--- deploy logs ---"
  railway logs --service "$SERVICE" -d --lines 60 2>&1 | tail -40 || true
  exit 1
fi
