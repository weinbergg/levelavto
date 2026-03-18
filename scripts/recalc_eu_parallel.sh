#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs

SHARDS="${SHARDS:-4}"
REGION="${REGION:-EU}"
COUNTRY="${COUNTRY:-}"
BATCH="${BATCH:-2000}"
CHUNK="${CHUNK:-50000}"
SLEEP="${SLEEP:-0}"
TELEGRAM="${TELEGRAM:-0}"
TELEGRAM_INTERVAL="${TELEGRAM_INTERVAL:-3600}"
RATE_SHIFT="${RATE_SHIFT:-0}"
BRANDS="${BRANDS:-}"
ONLY_MISSING="${ONLY_MISSING:-0}"
ONLY_MISSING_REGISTRATION="${ONLY_MISSING_REGISTRATION:-0}"
SINCE_MINUTES="${SINCE_MINUTES:-}"

if ! [[ "$SHARDS" =~ ^[0-9]+$ ]] || [ "$SHARDS" -lt 1 ]; then
  echo "[recalc_eu_parallel] invalid SHARDS=$SHARDS"
  exit 1
fi

for shard in $(seq 0 $((SHARDS - 1))); do
  log_file="logs/recalc_${REGION,,}_shard_${shard}.log"
  cmd=(
    docker compose exec -T web
    python -m backend.app.scripts.recalc_calc_cache
    --region "$REGION"
    --batch "$BATCH"
    --chunk "$CHUNK"
    --sleep "$SLEEP"
    --shard-total "$SHARDS"
    --shard-index "$shard"
  )

  if [ -n "$COUNTRY" ]; then
    cmd+=(--country "$COUNTRY")
  fi
  if [ -n "$SINCE_MINUTES" ]; then
    cmd+=(--since-minutes "$SINCE_MINUTES")
  fi
  if [ -n "$BRANDS" ]; then
    cmd+=(--brands "$BRANDS")
  fi
  if [ "$ONLY_MISSING" = "1" ]; then
    cmd+=(--only-missing)
  fi
  if [ "$ONLY_MISSING_REGISTRATION" = "1" ]; then
    cmd+=(--only-missing-registration)
  fi
  if [ "$TELEGRAM" = "1" ]; then
    cmd+=(--telegram --telegram-interval "$TELEGRAM_INTERVAL")
  fi
  if [ "$RATE_SHIFT" != "0" ]; then
    cmd+=(--rate-shift "$RATE_SHIFT")
  fi

  echo "[recalc_eu_parallel] start shard=$shard/$SHARDS log=$log_file"
  nohup "${cmd[@]}" >"$log_file" 2>&1 &
done

echo "[recalc_eu_parallel] launched shards=$SHARDS region=$REGION country=${COUNTRY:-ALL}"
