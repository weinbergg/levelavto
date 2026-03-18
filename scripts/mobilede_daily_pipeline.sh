#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[mobilede_pipeline] start $(date -Iseconds)"

echo "[mobilede_pipeline] step=mobilede_daily"
DAILY_ARGS=()
if [ "${MOBILEDE_ALLOW_DEACTIVATE:-0}" = "1" ]; then
  DAILY_ARGS+=(--allow-deactivate)
fi
docker compose run --rm web python -m backend.app.tools.mobilede_daily "${DAILY_ARGS[@]}"

echo "[mobilede_pipeline] step=car_counts_refresh"
docker compose exec -T web python -m backend.app.tools.car_counts_refresh --report

echo "[mobilede_pipeline] step=mirror_mobilede_thumbs"
MIRROR_TG_ARGS=()
if [ "${MIRROR_TELEGRAM:-0}" = "1" ]; then
  MIRROR_TG_ARGS+=(--telegram --telegram-interval "${MIRROR_TELEGRAM_INTERVAL:-1800}")
fi
docker compose exec -T web python -m backend.app.scripts.mirror_mobilede_thumbs \
  --region EU \
  --source-key mobile \
  --only-missing-local \
  --updated-since-hours "${MOBILEDE_MIRROR_SINCE_HOURS:-36}" \
  --limit "${MOBILEDE_MIRROR_LIMIT:-30000}" \
  --workers "${MOBILEDE_MIRROR_WORKERS:-12}" \
  --timeout "${MOBILEDE_MIRROR_TIMEOUT:-8}" \
  "${MIRROR_TG_ARGS[@]}" \
  --report-json /app/artifacts/mirror_mobilede_daily.json \
  --report-csv /app/artifacts/mirror_mobilede_daily.csv

if [ "${MOBILEDE_HERO_ENABLED:-1}" = "1" ]; then
  echo "[mobilede_pipeline] step=mirror_mobilede_hero"
  HERO_UPDATED_SINCE_HOURS="${MOBILEDE_HERO_SINCE_HOURS:-${MOBILEDE_MIRROR_SINCE_HOURS:-36}}" \
  HERO_LIMIT="${MOBILEDE_HERO_LIMIT:-30000}" \
  HERO_WORKERS="${MOBILEDE_HERO_WORKERS:-8}" \
  HERO_TIMEOUT="${MOBILEDE_HERO_TIMEOUT:-8}" \
  HERO_MAX_WIDTH="${MOBILEDE_HERO_MAX_WIDTH:-1280}" \
  HERO_QUALITY="${MOBILEDE_HERO_QUALITY:-82}" \
  HERO_TELEGRAM="${MOBILEDE_HERO_TELEGRAM:-${MIRROR_TELEGRAM:-0}}" \
  HERO_TELEGRAM_INTERVAL="${MOBILEDE_HERO_TELEGRAM_INTERVAL:-${MIRROR_TELEGRAM_INTERVAL:-1800}}" \
  bash scripts/mirror_de_hero_images.sh
fi

echo "[mobilede_pipeline] step=recalc_missing_prices"
docker compose exec -T web python -m backend.app.scripts.recalc_missing_prices \
  --region EU \
  --batch "${MISSING_PRICE_BATCH:-2000}" \
  --limit "${MISSING_PRICE_LIMIT:-50000}" \
  --only-missing-total \
  --report-json /app/artifacts/recalc_missing_prices_daily.json

if [ "${MOBILEDE_PRUNE_UNUSED_MEDIA:-1}" = "1" ]; then
  echo "[mobilede_pipeline] step=prune_unused_local_media"
  docker compose exec -T web python -m backend.app.scripts.prune_unused_local_media \
    --report-json /app/artifacts/prune_unused_local_media_daily.json
fi

echo "[mobilede_pipeline] step=prewarm"
PREWARM_MAX_SEC="${PREWARM_MAX_SEC:-900}" \
PREWARM_INCLUDE_BRAND_CTX=0 \
PREWARM_INCLUDE_MODEL_CTX=0 \
PREWARM_INCLUDE_BRAND_LISTS=1 \
PREWARM_INCLUDE_BRAND_COUNTS=1 \
PREWARM_COUNTRY_SWEEP=0 \
PREWARM_EU_COUNTRY="${PREWARM_EU_COUNTRY:-DE}" \
docker compose exec -T web python -m backend.app.scripts.prewarm_cache || true

echo "[mobilede_pipeline] done $(date -Iseconds)"
