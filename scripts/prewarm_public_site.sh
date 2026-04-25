#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[prewarm_public] start $(date -Iseconds)"

docker compose exec -T web env \
  PREWARM_MAX_SEC="${PREWARM_MAX_SEC:-900}" \
  PREWARM_INCLUDE_PAYLOAD="${PREWARM_INCLUDE_PAYLOAD:-1}" \
  PREWARM_INCLUDE_BRAND_CTX="${PREWARM_INCLUDE_BRAND_CTX:-0}" \
  PREWARM_INCLUDE_MODEL_CTX="${PREWARM_INCLUDE_MODEL_CTX:-0}" \
  PREWARM_INCLUDE_BRAND_LISTS="${PREWARM_INCLUDE_BRAND_LISTS:-1}" \
  PREWARM_INCLUDE_BRAND_COUNTS="${PREWARM_INCLUDE_BRAND_COUNTS:-1}" \
  PREWARM_COUNTRY_SWEEP="${PREWARM_COUNTRY_SWEEP:-0}" \
  PREWARM_LIST_SORTS="${PREWARM_LIST_SORTS:-price_asc}" \
  PREWARM_BRAND_REGIONS="${PREWARM_BRAND_REGIONS:-EU}" \
  PREWARM_EU_COUNTRY="${PREWARM_EU_COUNTRY:-DE}" \
  python -m backend.app.scripts.prewarm_cache || true

echo "[prewarm_public] done $(date -Iseconds)"
