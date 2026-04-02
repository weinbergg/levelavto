#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[mobilede_pipeline] start $(date -Iseconds)"

echo "[mobilede_pipeline] step=ensure_services"
docker compose up -d db redis web

echo "[mobilede_pipeline] step=mobilede_daily"
DAILY_ARGS=()
RUN_ENV_ARGS=()
MOBILEDE_STRICT_DEACTIVATION_GUARD="${MOBILEDE_STRICT_DEACTIVATION_GUARD:-1}"
if [ "${MOBILEDE_ALLOW_DEACTIVATE:-0}" = "1" ]; then
  DAILY_ARGS+=(--allow-deactivate)
fi
for env_name in KEEP_CSV MOBILEDE_HOST MOBILEDE_LOGIN MOBILEDE_PASSWORD MOBILEDE_USER MOBILEDE_PASS MOBILEDE_TMP_DIR MOBILEDE_MIN_FREE_GB RUN_EU_CALC_AFTER_DAILY EU_CALC_SINCE_MIN MOBILEDE_DEACTIVATE_MODE MOBILEDE_DEACTIVATE_MIN_RATIO MOBILEDE_DEACTIVATE_MIN_SEEN MOBILEDE_SKIP_DEACTIVATE MOBILEDE_STRICT_DEACTIVATION_GUARD TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID TELEGRAM_ADMIN_CHAT_ID TELEGRAM_ALLOWED_IDS EURO_RATE USD_RATE FX_ADD_RUB; do
  if [ -n "${!env_name:-}" ]; then
    RUN_ENV_ARGS+=(-e "${env_name}=${!env_name}")
  fi
done
docker compose run --rm "${RUN_ENV_ARGS[@]}" web python -m backend.app.tools.mobilede_daily "${DAILY_ARGS[@]}"

echo "[mobilede_pipeline] step=verify_deactivation_gate"
VERIFY_ENV_ARGS=(
  -e "MOBILEDE_STRICT_DEACTIVATION_GUARD=${MOBILEDE_STRICT_DEACTIVATION_GUARD}"
)
if [ -n "${MOBILEDE_TMP_DIR:-}" ]; then
  VERIFY_ENV_ARGS+=(-e "MOBILEDE_TMP_DIR=${MOBILEDE_TMP_DIR}")
fi
docker compose exec -T "${VERIFY_ENV_ARGS[@]}" web python - <<'PY'
import json
import os
import sys
from pathlib import Path

stats_path = Path(os.getenv("MOBILEDE_TMP_DIR", "/app/tmp")) / "mobilede_import_stats.json"
strict = os.getenv("MOBILEDE_STRICT_DEACTIVATION_GUARD", "0") == "1"

if not stats_path.exists():
    msg = f"[mobilede_pipeline] deactivation stats missing: {stats_path}"
    print(msg, flush=True)
    if strict:
        raise SystemExit(msg)
    raise SystemExit(0)

stats = json.loads(stats_path.read_text(encoding="utf-8"))
allowed = stats.get("deactivation_allowed")
mode = str(stats.get("deactivate_mode") or "")
reason = str(stats.get("deactivate_reason") or "-")
print(
    f"[mobilede_pipeline] deactivation stats mode={mode or '-'} "
    f"allowed={allowed!r} reason={reason}",
    flush=True,
)
if strict and mode == "auto" and allowed is not True:
    raise SystemExit(
        "[mobilede_pipeline] strict deactivation guard failed: "
        f"mode={mode or '-'} allowed={allowed!r} reason={reason}"
    )
PY

echo "[mobilede_pipeline] step=update_fx_prices"
FX_ARGS=(--batch "${FX_UPDATE_BATCH:-2000}" --sleep "${FX_UPDATE_SLEEP_SEC:-0}")
if [ "${FX_UPDATE_TELEGRAM:-0}" = "1" ]; then
  FX_ARGS+=(--telegram)
fi
docker compose exec -T web python -m backend.app.scripts.update_fx_prices "${FX_ARGS[@]}"

echo "[mobilede_pipeline] step=backfill_missing_registration"
docker compose exec -T web python -m backend.app.scripts.backfill_missing_registration \
  --region EU \
  --batch "${MISSING_REG_BACKFILL_BATCH:-2000}" \
  --chunk "${MISSING_REG_BACKFILL_CHUNK:-50000}"

echo "[mobilede_pipeline] step=refresh_spec_inference"
SPEC_INFERENCE_ARGS=()
if [ "${SPEC_INFERENCE_FULL_REBUILD:-0}" = "1" ]; then
  SPEC_INFERENCE_ARGS+=(--full-rebuild)
fi
docker compose exec -T web python -m backend.app.scripts.refresh_spec_inference \
  --region EU \
  --since-minutes "${SPEC_INFERENCE_SINCE_MINUTES:-2880}" \
  --batch "${SPEC_INFERENCE_BATCH:-2000}" \
  --chunk "${SPEC_INFERENCE_CHUNK:-50000}" \
  --year-window "${SPEC_INFERENCE_YEAR_WINDOW:-2}" \
  "${SPEC_INFERENCE_ARGS[@]}"

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

echo "[mobilede_pipeline] step=recalc_defaulted_registration"
docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
  --region EU \
  --only-defaulted-registration \
  --since-minutes "${MISSING_REG_CALC_SINCE_MINUTES:-2880}" \
  --batch "${MISSING_REG_CALC_BATCH:-2000}"

echo "[mobilede_pipeline] step=recalc_inferred_specs"
docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
  --region EU \
  --only-inferred-specs \
  --since-minutes "${SPEC_INFERENCE_SINCE_MINUTES:-2880}" \
  --batch "${SPEC_INFERENCE_CALC_BATCH:-2000}"

echo "[mobilede_pipeline] step=recalc_recoverable_fallbacks"
docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
  --region EU \
  --only-recoverable-fallback \
  --since-minutes "${RECOVERABLE_FALLBACK_SINCE_MINUTES:-10080}" \
  --batch "${RECOVERABLE_FALLBACK_BATCH:-2000}"

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
