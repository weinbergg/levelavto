#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

STATE_FILE="${MIRROR_DE_STATE_FILE:-/app/artifacts/mirror_de_all_state.json}"
LIMIT="${MIRROR_DE_LIMIT:-15000}"
WORKERS="${MIRROR_DE_WORKERS:-12}"
TIMEOUT="${MIRROR_DE_TIMEOUT:-6}"
BATCH="${MIRROR_DE_BATCH:-500}"
TELEGRAM="${MIRROR_DE_TELEGRAM:-1}"
TG_INTERVAL="${MIRROR_DE_TELEGRAM_INTERVAL:-1800}"

ARGS=(
  --region EU
  --source-key mobile
  --country DE
  --only-missing-local
  --batch "$BATCH"
  --limit "$LIMIT"
  --workers "$WORKERS"
  --timeout "$TIMEOUT"
  --state-file "$STATE_FILE"
  --cycle
  --report-json /app/artifacts/mirror_de_all_chunk.json
  --report-csv /app/artifacts/mirror_de_all_chunk.csv
)

if [ "$TELEGRAM" = "1" ]; then
  ARGS+=(--telegram --telegram-interval "$TG_INTERVAL")
fi

echo "[mirror_de_all_chunk] start $(date -Iseconds) limit=$LIMIT workers=$WORKERS timeout=$TIMEOUT state=$STATE_FILE"
docker compose exec -T web python -m backend.app.scripts.mirror_mobilede_thumbs "${ARGS[@]}"
echo "[mirror_de_all_chunk] clear missing local paths"
docker compose exec -T web python -m backend.app.scripts.clear_missing_local_thumbs --batch 5000
echo "[mirror_de_all_chunk] done $(date -Iseconds)"
