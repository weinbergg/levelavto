#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[kr_pipeline] start $(date -Iseconds)"

echo "[kr_pipeline] step=ensure_services"
docker compose up -d db redis web

echo "[kr_pipeline] step=emavto_job"
TELEGRAM_ENABLED="${TELEGRAM_ENABLED:-0}" bash scripts/run_emavto_job.sh

echo "[kr_pipeline] step=refresh_spec_inference"
docker compose exec -T web python -m backend.app.scripts.refresh_spec_inference \
  --region KR \
  --since-minutes "${KR_SPEC_INFERENCE_SINCE_MINUTES:-10080}" \
  --batch "${KR_SPEC_INFERENCE_BATCH:-2000}" \
  --chunk "${KR_SPEC_INFERENCE_CHUNK:-50000}" \
  --year-window "${KR_SPEC_INFERENCE_YEAR_WINDOW:-2}"

echo "[kr_pipeline] step=recalc_cached_prices"
docker compose exec -T web python -m backend.app.scripts.recalc_cached_prices \
  --region KR \
  --batch "${KR_RECALC_BATCH:-2000}"

echo "[kr_pipeline] step=car_counts_refresh"
docker compose exec -T web python -m backend.app.tools.car_counts_refresh --report

echo "[kr_pipeline] step=prewarm"
PREWARM_MAX_SEC="${KR_PREWARM_MAX_SEC:-600}" \
PREWARM_INCLUDE_BRAND_CTX=0 \
PREWARM_INCLUDE_MODEL_CTX=0 \
PREWARM_INCLUDE_BRAND_LISTS=1 \
PREWARM_INCLUDE_BRAND_COUNTS=1 \
PREWARM_COUNTRY_SWEEP=0 \
PREWARM_EU_COUNTRY="${PREWARM_EU_COUNTRY:-DE}" \
docker compose exec -T web python -m backend.app.scripts.prewarm_cache || true

echo "[kr_pipeline] done $(date -Iseconds)"
