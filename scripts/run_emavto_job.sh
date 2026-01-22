#!/usr/bin/env bash
# Wrapper to run emavto nightly job and emit JSON status.
# Uses existing scripts/nightly_emavto.sh.

set -euo pipefail

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
START_SEC=$(date -u +%s)

LOG_DIR="backend/app/runtime/jobs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/emavto_$(date -u +%F).log"
RESULT_FILE="$LOG_DIR/emavto_last.json"

STATUS="ok"
ERR=""

if [[ -f "${EMAVTO_STOP_FILE:-/tmp/emavto_stop}" ]]; then
  rm -f "${EMAVTO_STOP_FILE:-/tmp/emavto_stop}"
fi

cat >"$RESULT_FILE" <<EOF
{
  "job": "emavto",
  "started_at": "${START_TS}",
  "finished_at": null,
  "duration_sec": 0,
  "status": "running",
  "stats": {
    "cars_total_processed": null,
    "cars_inserted": null,
    "cars_updated": null,
    "cars_skipped": null,
    "cars_deactivated": null,
    "cars_without_photos": null
  },
  "errors": []
}
EOF

python -m backend.app.tools.notify_tg --job emavto --result "$RESULT_FILE" || true

{
  echo "[emavto] start ${START_TS}"
  bash scripts/nightly_emavto.sh
} >>"$LOG_FILE" 2>&1 || STATUS="fail"

FIN_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
FIN_SEC=$(date -u +%s)
DUR=$((FIN_SEC - START_SEC))

cat >"$RESULT_FILE" <<EOF
{
  "job": "emavto",
  "started_at": "${START_TS}",
  "finished_at": "${FIN_TS}",
  "duration_sec": ${DUR},
  "status": "${STATUS}",
  "stats": {
    "cars_total_processed": null,
    "cars_inserted": null,
    "cars_updated": null,
    "cars_skipped": null,
    "cars_deactivated": null,
    "cars_without_photos": null
  },
  "errors": []
}
EOF

python -m backend.app.tools.notify_tg --job emavto --result "$RESULT_FILE" || true

echo "[emavto] done status=${STATUS} duration=${DUR}s"
