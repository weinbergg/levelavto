#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

LIMIT="${HERO_LIMIT:-25000}"
WORKERS="${HERO_WORKERS:-8}"
TIMEOUT="${HERO_TIMEOUT:-8}"
QUALITY="${HERO_QUALITY:-82}"
MAX_WIDTH="${HERO_MAX_WIDTH:-1280}"
UPDATED_SINCE_HOURS="${HERO_UPDATED_SINCE_HOURS:-0}"
REPORT_JSON="${HERO_REPORT_JSON:-/app/artifacts/mirror_de_hero_images.json}"
ORDER_BY="${HERO_ORDER_BY:-updated_desc}"
OFFSET="${HERO_OFFSET:-0}"
TELEGRAM="${HERO_TELEGRAM:-0}"
TELEGRAM_INTERVAL="${HERO_TELEGRAM_INTERVAL:-1800}"

ARGS=(
  --region EU
  --country DE
  --source-key mobile
  --limit-cars "$LIMIT"
  --offset-cars "$OFFSET"
  --max-images-per-car 1
  --order-by "$ORDER_BY"
  --workers "$WORKERS"
  --timeout "$TIMEOUT"
  --max-width "$MAX_WIDTH"
  --quality "$QUALITY"
  --format webp
  --skip-sync-thumbnail
  --report-json "$REPORT_JSON"
)

if [ "${UPDATED_SINCE_HOURS}" != "0" ]; then
  ARGS+=(--updated-since-hours "$UPDATED_SINCE_HOURS")
fi

if [ "${TELEGRAM}" = "1" ]; then
  ARGS+=(--telegram --telegram-interval "$TELEGRAM_INTERVAL")
fi

echo "[mirror_de_hero_images] start $(date -Iseconds) limit=${LIMIT} offset=${OFFSET} updated_since_hours=${UPDATED_SINCE_HOURS} order_by=${ORDER_BY} workers=${WORKERS} width=${MAX_WIDTH} telegram=${TELEGRAM}"
docker compose exec -T web python -m backend.app.scripts.mirror_car_images_local "${ARGS[@]}"
echo "[mirror_de_hero_images] done $(date -Iseconds)"
