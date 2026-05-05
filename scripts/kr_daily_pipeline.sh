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

echo "[kr_pipeline] step=recalc_inferred_specs"
docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
  --region KR \
  --only-inferred-specs \
  --since-minutes "${KR_SPEC_INFERENCE_SINCE_MINUTES:-10080}" \
  --batch "${KR_RECALC_BATCH:-2000}"

echo "[kr_pipeline] step=recalc_recoverable_fallbacks"
docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
  --region KR \
  --only-recoverable-fallback \
  --since-minutes "${KR_RECOVERABLE_FALLBACK_SINCE_MINUTES:-10080}" \
  --batch "${KR_RECOVERABLE_BATCH:-2000}"

echo "[kr_pipeline] step=car_counts_refresh"
docker compose exec -T web python -m backend.app.tools.car_counts_refresh --report

echo "[kr_pipeline] step=cache_maintenance"
PURGE_SOFT=1 BUMP_DATASET=1 bash scripts/cache_maintenance.sh

echo "[kr_pipeline] step=prewarm"
PREWARM_MAX_SEC="${KR_PREWARM_MAX_SEC:-1200}" \
PREWARM_INCLUDE_BRAND_CTX=0 \
PREWARM_INCLUDE_MODEL_CTX=0 \
PREWARM_INCLUDE_BROAD_BASE="${KR_PREWARM_INCLUDE_BROAD_BASE:-1}" \
PREWARM_INCLUDE_BROAD_COUNTS="${KR_PREWARM_INCLUDE_BROAD_COUNTS:-1}" \
PREWARM_INCLUDE_BROAD_LISTS="${KR_PREWARM_INCLUDE_BROAD_LISTS:-1}" \
PREWARM_INCLUDE_BRAND_LISTS="${KR_PREWARM_INCLUDE_BRAND_LISTS:-0}" \
PREWARM_INCLUDE_BRAND_COUNTS="${KR_PREWARM_INCLUDE_BRAND_COUNTS:-0}" \
PREWARM_INCLUDE_ENGINE_LISTS="${KR_PREWARM_INCLUDE_ENGINE_LISTS:-0}" \
PREWARM_COUNTRY_SWEEP=0 \
PREWARM_LIST_SORTS="${KR_PREWARM_LIST_SORTS:-price_asc}" \
PREWARM_EU_COUNTRY="${PREWARM_EU_COUNTRY:-DE}" \
PREWARM_INCLUDE_KR_PUBLIC=1 \
bash scripts/prewarm_public_site.sh

echo "[kr_pipeline] done $(date -Iseconds)"
