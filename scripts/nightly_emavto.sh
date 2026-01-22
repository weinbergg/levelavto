#!/usr/bin/env bash
# Nightly incremental scan for new/updated cars.
# Expected to be run from project root (/opt/levelavto) via cron/systemd.
# Example cron (UTC): 00:00 daily
# 0 0 * * * cd /opt/levelavto && /bin/bash scripts/nightly_emavto.sh >> /var/log/nightly_emavto.log 2>&1

set -euo pipefail

# Allow overriding via env/cron
PAGES="${CHUNK_PAGES:-10}"
PAUSE="${CHUNK_PAUSE_SEC:-60}"
RUNTIME="${CHUNK_MAX_RUNTIME_SEC:-3600}"
TOTAL="${CHUNK_TOTAL_PAGES:-0}"
STOP_FILE="${EMAVTO_STOP_FILE:-/tmp/emavto_stop}"

echo "[nightly] start $(date -Iseconds) pages=${PAGES} pause=${PAUSE}s runtime=${RUNTIME}s total=${TOTAL}"

if [[ -f "${STOP_FILE}" ]]; then
  rm -f "${STOP_FILE}"
fi

docker compose exec web python -m backend.app.tools.emavto_chunk_runner \
  --chunk-pages "${PAGES}" \
  --pause-sec "${PAUSE}" \
  --max-runtime-sec "${RUNTIME}" \
  --total-pages "${TOTAL}" \
  --mode incremental \
  --backfill-missing \
  --start-page 1

echo "[nightly] done $(date -Iseconds)"
