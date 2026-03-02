#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts logs

PRIORITY_BRANDS="${PRIORITY_BRANDS:-BMW,Mercedes-Benz,Audi,Volkswagen,Skoda,Toyota}"
TG_INTERVAL_SEC="${TG_INTERVAL_SEC:-3600}"
PRICE_BATCH="${PRICE_BATCH:-2000}"
MIRROR_BATCH="${MIRROR_BATCH:-500}"
MIRROR_WORKERS="${MIRROR_WORKERS:-12}"
MIRROR_TIMEOUT="${MIRROR_TIMEOUT:-8}"
MIRROR_LIMIT_PRIORITY="${MIRROR_LIMIT_PRIORITY:-70000}"
MIRROR_LIMIT_REST="${MIRROR_LIMIT_REST:-250000}"
MIRROR_MAX_WIDTH="${MIRROR_MAX_WIDTH:-1280}"
MIRROR_QUALITY="${MIRROR_QUALITY:-82}"
PROGRESS_TOP="${PROGRESS_TOP:-15}"
RATE_IMG_SEC="${RATE_IMG_SEC:-0}"
S3_SYNC="${S3_SYNC:-0}"
S3_REMOTE="${S3_REMOTE:-regs3}"
S3_BUCKET="${S3_BUCKET:-levelavto1}"
S3_PREFIX="${S3_PREFIX:-media}"

get_env() {
  local key="$1"
  local env_file="$ROOT_DIR/.env"
  if [ ! -f "$env_file" ]; then
    return 0
  fi
  sed -n "s/^${key}=//p" "$env_file" | head -n1
}

TG_TOKEN="${TELEGRAM_BOT_TOKEN:-$(get_env TELEGRAM_BOT_TOKEN)}"
TG_CHAT="${TELEGRAM_CHAT_ID:-$(get_env TELEGRAM_CHAT_ID)}"
if [ -z "${TG_CHAT:-}" ]; then
  TG_CHAT="${TELEGRAM_ADMIN_CHAT_ID:-$(get_env TELEGRAM_ADMIN_CHAT_ID)}"
fi
if [ -z "${TG_CHAT:-}" ]; then
  TG_CHAT="$(get_env TELEGRAM_ALLOWED_IDS | cut -d',' -f1)"
fi

tg() {
  local msg="$1"
  if [ -z "${TG_TOKEN:-}" ] || [ -z "${TG_CHAT:-}" ]; then
    echo "[night] tg skipped: token/chat missing"
    return 0
  fi
  curl -sS -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${TG_CHAT}" \
    --data-urlencode "text=${msg}" >/dev/null || true
}

run_step() {
  local name="$1"
  shift
  local t0
  t0="$(date +%s)"
  echo "[night] step=${name} start $(date -Iseconds)"
  tg "night_de: ${name} start"
  "$@"
  local t1
  t1="$(date +%s)"
  local dt="$((t1 - t0))"
  echo "[night] step=${name} done ${dt}s"
  tg "night_de: ${name} done (${dt}s)"
}

progress_tick() {
  local mode="$1"
  while true; do
    sleep "$TG_INTERVAL_SEC"
    if [ "$mode" = "priority" ]; then
      docker compose exec -T web python -m backend.app.scripts.brand_photo_progress \
        --country DE \
        --source-key mobile \
        --brands "$PRIORITY_BRANDS" \
        --rate-img-sec "$RATE_IMG_SEC" \
        --top "$PROGRESS_TOP" \
        --telegram \
        --report-json /app/artifacts/brand_photo_progress_de_priority.json \
        --report-csv /app/artifacts/brand_photo_progress_de_priority.csv || true
    fi
    docker compose exec -T web python -m backend.app.scripts.brand_photo_progress \
      --country DE \
      --source-key mobile \
      --rate-img-sec "$RATE_IMG_SEC" \
      --top "$PROGRESS_TOP" \
      --telegram \
      --report-json /app/artifacts/brand_photo_progress_de_all.json \
      --report-csv /app/artifacts/brand_photo_progress_de_all.csv || true
  done
}

start_progress_tick() {
  local mode="$1"
  progress_tick "$mode" &
  echo $!
}

stop_progress_tick() {
  local pid="$1"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
    wait "$pid" 2>/dev/null || true
  fi
}

tg "night_de: pipeline start; priority=${PRIORITY_BRANDS}"

run_step "migrations" \
  docker compose exec -T web alembic -c migrations/alembic.ini upgrade head

run_step "prices_de_priority" \
  docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
    --region EU \
    --country DE \
    --only-missing \
    --brands "$PRIORITY_BRANDS" \
    --batch "$PRICE_BATCH" \
    --telegram \
    --telegram-interval "$TG_INTERVAL_SEC"

run_step "prices_de_rest" \
  docker compose exec -T web python -m backend.app.scripts.recalc_calc_cache \
    --region EU \
    --country DE \
    --only-missing \
    --batch "$PRICE_BATCH" \
    --telegram \
    --telegram-interval "$TG_INTERVAL_SEC"

tick_pid=""
tick_pid="$(start_progress_tick priority)"
run_step "thumbs_de_priority" \
  docker compose exec -T web python -m backend.app.scripts.mirror_mobilede_thumbs \
    --region EU \
    --source-key mobile \
    --country DE \
    --brands "$PRIORITY_BRANDS" \
    --only-missing-local \
    --batch "$MIRROR_BATCH" \
    --limit "$MIRROR_LIMIT_PRIORITY" \
    --workers "$MIRROR_WORKERS" \
    --timeout "$MIRROR_TIMEOUT" \
    --max-width "$MIRROR_MAX_WIDTH" \
    --quality "$MIRROR_QUALITY" \
    --telegram \
    --telegram-interval "$TG_INTERVAL_SEC" \
    --state-file /app/artifacts/mirror_de_priority_state.json \
    --report-json /app/artifacts/mirror_de_priority.json \
    --report-csv /app/artifacts/mirror_de_priority.csv
stop_progress_tick "$tick_pid"

tick_pid="$(start_progress_tick rest)"
run_step "thumbs_de_rest" \
  docker compose exec -T web python -m backend.app.scripts.mirror_mobilede_thumbs \
    --region EU \
    --source-key mobile \
    --country DE \
    --only-missing-local \
    --batch "$MIRROR_BATCH" \
    --limit "$MIRROR_LIMIT_REST" \
    --workers "$MIRROR_WORKERS" \
    --timeout "$MIRROR_TIMEOUT" \
    --max-width "$MIRROR_MAX_WIDTH" \
    --quality "$MIRROR_QUALITY" \
    --telegram \
    --telegram-interval "$TG_INTERVAL_SEC" \
    --state-file /app/artifacts/mirror_de_rest_state.json \
    --report-json /app/artifacts/mirror_de_rest.json \
    --report-csv /app/artifacts/mirror_de_rest.csv
stop_progress_tick "$tick_pid"

run_step "clear_missing_local_thumbs" \
  docker compose exec -T web python -m backend.app.scripts.clear_missing_local_thumbs --batch 5000

if [ "$S3_SYNC" = "1" ]; then
  run_step "s3_sync_media" \
    rclone sync "$ROOT_DIR/фото-видео/" "${S3_REMOTE}:${S3_BUCKET}/${S3_PREFIX}/" \
      --transfers 12 \
      --checkers 24 \
      --fast-list \
      --stats 60s \
      --stats-one-line
fi

tg "night_de: all done"
echo "[night] done $(date -Iseconds)"
