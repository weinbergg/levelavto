#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[fx_daily_update] start $(date -Iseconds)"
echo "[fx_daily_update] step=ensure_services"
/usr/bin/docker compose up -d db redis web

FX_ARGS=(--batch "${BATCH:-2000}" --sleep "${SLEEP_SEC:-1}")
if [ -n "${COUNTRY:-}" ]; then
  FX_ARGS+=(--country "$COUNTRY")
fi
if [ -n "${ONLY_IDS:-}" ]; then
  FX_ARGS+=(--only-ids "$ONLY_IDS")
fi
if [ "${DRY_RUN:-0}" = "1" ]; then
  FX_ARGS+=(--dry-run)
fi
if [ "${TELEGRAM:-0}" = "1" ]; then
  FX_ARGS+=(--telegram)
fi

/usr/bin/docker compose exec -T web python -m backend.app.scripts.update_fx_prices "${FX_ARGS[@]}"
echo "[fx_daily_update] step=cache_maintenance"
PURGE_SOFT=1 BUMP_DATASET=1 bash scripts/cache_maintenance.sh
echo "[fx_daily_update] done $(date -Iseconds)"
