#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   REBUILD=1 COUNTRIES="DE,FR,IT,ES" ./scripts/post_update_pipeline.sh
#   REBUILD=0 ./scripts/post_update_pipeline.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

REBUILD="${REBUILD:-0}"
COUNTRIES="${COUNTRIES:-DE,FR,IT,ES}"
BATCH="${BATCH:-2000}"
SINCE_MIN="${SINCE_MIN:-360}"
THREADS="${THREADS:-4}"

echo "[pipeline] start $(date -Iseconds)"
echo "[pipeline] rebuild=$REBUILD countries=$COUNTRIES batch=$BATCH since_min=$SINCE_MIN threads=$THREADS"

if [ "$REBUILD" = "1" ]; then
  echo "[pipeline] rebuild"
  docker compose build web
  docker compose up -d --force-recreate web
fi

echo "[pipeline] migrations"
docker compose exec -T web alembic -c migrations/alembic.ini upgrade head

echo "[pipeline] recalc KR cached prices"
docker compose exec -T web python -m backend.app.scripts.recalc_cached_prices --region KR --batch "$BATCH"

echo "[pipeline] recalc EU calc cache (parallel by country)"
IFS=',' read -r -a COUNTRY_LIST <<< "$COUNTRIES"
PIDS=()
for c in "${COUNTRY_LIST[@]}"; do
  c_trim="$(echo "$c" | xargs)"
  if [ -z "$c_trim" ]; then
    continue
  fi
  docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
    --region EU --country "$c_trim" --only-missing --batch "$BATCH" --since-minutes "$SINCE_MIN" &
  PIDS+=("$!")
  if [ "${#PIDS[@]}" -ge "$THREADS" ]; then
    wait "${PIDS[0]}"
    PIDS=("${PIDS[@]:1}")
  fi
done
if [ "${#PIDS[@]}" -gt 0 ]; then
  wait "${PIDS[@]}"
fi

echo "[pipeline] prewarm cache"
docker compose run --rm prewarm || true

echo "[pipeline] cleanup tmp/logs/thumb cache"
DRY_RUN=0 bash scripts/cleanup_tmp_files.sh

echo "[pipeline] telegram notify"
docker compose exec -T web python - <<'PY'
import os
from backend.app.utils.telegram import send_telegram_message
token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")
msg = "[pipeline] post-update done"
if token and chat_id:
    ok = send_telegram_message(token, chat_id, msg)
    print(f"[pipeline] telegram ok={ok}")
else:
    print("[pipeline] telegram disabled: TELEGRAM_BOT_TOKEN/CHAT_ID missing")
PY

echo "[pipeline] done $(date -Iseconds)"
