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

DOCKER_BIN="${DOCKER_BIN:-/usr/bin/docker}"
DOCKER_CMD="${DOCKER_CMD:-${DOCKER_BIN} compose}"
LOCK_DIR="${EMAVTO_LOCK_DIR:-/tmp/emavto_job.lock}"
PID_FILE="${LOCK_DIR}/pid"
WAIT_ON_LOCK="${EMAVTO_WAIT_ON_LOCK:-1}"
WAIT_TIMEOUT_SEC="${EMAVTO_WAIT_TIMEOUT_SEC:-14400}"
WAIT_POLL_SEC="${EMAVTO_WAIT_POLL_SEC:-30}"

cleanup() {
  rm -f "${PID_FILE}" 2>/dev/null || true
  rmdir "${LOCK_DIR}" 2>/dev/null || true
}

lock_owner_alive() {
  local pid=""
  local cmd=""
  local probe=""
  if [[ -f "${PID_FILE}" ]]; then
    pid=$(tr -cd '0-9' < "${PID_FILE}" 2>/dev/null || true)
    if [[ -n "${pid}" ]]; then
      cmd=$(ps -p "${pid}" -o command= 2>/dev/null || true)
      if [[ "${cmd}" == *"scripts/run_emavto_job.sh"* || "${cmd}" == *"scripts/nightly_emavto.sh"* || "${cmd}" == *"backend.app.tools.emavto_chunk_runner"* ]]; then
        return 0
      fi
    fi
  fi
  probe=$(ps -ax -o command= 2>/dev/null | grep -E 'scripts/nightly_emavto\.sh|backend\.app\.tools\.emavto_chunk_runner' | grep -v 'grep' || true)
  if [[ -n "${probe}" ]]; then
    return 0
  fi
  return 1
}

clear_stale_lock() {
  echo "[emavto] clearing stale lock at ${LOCK_DIR}"
  rm -f "${PID_FILE}" 2>/dev/null || true
  rmdir "${LOCK_DIR}" 2>/dev/null || true
}

while true; do
  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    printf '%s\n' "$$" > "${PID_FILE}"
    trap cleanup EXIT
    break
  fi
  echo "[emavto] already running, lock exists at ${LOCK_DIR}"
  if lock_owner_alive; then
    # Nightly KR must not race ahead into inference/recalc while another import
    # is still holding the emavto lock.
    if [[ "${WAIT_ON_LOCK}" != "1" ]]; then
      exit 0
    fi
    wait_started_sec=$(date -u +%s)
    cleared_stale=0
    while [[ -d "${LOCK_DIR}" ]]; do
      if ! lock_owner_alive; then
        clear_stale_lock
        cleared_stale=1
        break
      fi
      now_sec=$(date -u +%s)
      waited=$((now_sec - wait_started_sec))
      if (( waited >= WAIT_TIMEOUT_SEC )); then
        echo "[emavto] wait timeout after ${waited}s for lock ${LOCK_DIR}"
        exit 1
      fi
      echo "[emavto] waiting for active job to finish (${waited}s elapsed)"
      sleep "${WAIT_POLL_SEC}"
    done
    if (( cleared_stale == 1 )); then
      continue
    fi
    echo "[emavto] previous job finished, continuing without starting a second run"
    exit 0
  fi
  clear_stale_lock
done

run_web_python() {
  if command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
    ${DOCKER_CMD} exec -T web python "$@"
  else
    python "$@"
  fi
}

send_job_notify() {
  if [[ "${TELEGRAM_ENABLED:-1}" == "0" ]]; then
    return 0
  fi
  run_web_python -m backend.app.tools.notify_tg --job emavto --result "$RESULT_FILE" || true
}

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

send_job_notify

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

send_job_notify

echo "[emavto] done status=${STATUS} duration=${DUR}s"

if [[ "${STATUS}" != "ok" ]]; then
  exit 1
fi
