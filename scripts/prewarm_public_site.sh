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
  PREWARM_INCLUDE_BROAD_BASE="${PREWARM_INCLUDE_BROAD_BASE:-0}" \
  PREWARM_INCLUDE_BROAD_COUNTS="${PREWARM_INCLUDE_BROAD_COUNTS:-0}" \
  PREWARM_INCLUDE_BROAD_LISTS="${PREWARM_INCLUDE_BROAD_LISTS:-0}" \
  PREWARM_INCLUDE_BRAND_LISTS="${PREWARM_INCLUDE_BRAND_LISTS:-0}" \
  PREWARM_INCLUDE_BRAND_COUNTS="${PREWARM_INCLUDE_BRAND_COUNTS:-0}" \
  PREWARM_COUNTRY_SWEEP="${PREWARM_COUNTRY_SWEEP:-0}" \
  PREWARM_LIST_SORTS="${PREWARM_LIST_SORTS:-price_asc}" \
  PREWARM_BRAND_REGIONS="${PREWARM_BRAND_REGIONS:-EU}" \
  PREWARM_INCLUDE_ENGINE_LISTS="${PREWARM_INCLUDE_ENGINE_LISTS:-0}" \
  PREWARM_EU_COUNTRY="${PREWARM_EU_COUNTRY:-DE}" \
  python -m backend.app.scripts.prewarm_cache || true

# Warm the real SSR entrypoints too. The Python prewarm above hydrates
# filters/list/count caches, but not the rendered home/catalog HTML.
curl -fsS --max-time 20 "http://localhost:8000/" >/dev/null || true
curl -fsS --max-time 20 "http://localhost:8000/catalog?region=EU&country=${PREWARM_EU_COUNTRY:-DE}&sort=price_asc" >/dev/null || true

echo "[prewarm_public] done $(date -Iseconds)"
