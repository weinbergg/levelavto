#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8000}
CAR_ID=${CAR_ID:-285235}
EUR_RATE=${EUR_RATE:-91.28}

resp=$(curl -fsS "$BASE_URL/api/calc_debug?car_id=$CAR_ID&eur_rate=$EUR_RATE")

if ! echo "$resp" | jq -e '.result.total_rub and (.steps|length>0)' >/dev/null; then
  echo "[smoke] FAIL: calc_debug structure invalid"
  exit 1
fi

if ! echo "$resp" | jq -e '.result.config_version or .config.version' >/dev/null; then
  echo "[smoke] FAIL: config version missing"
  exit 1
fi

echo "[smoke] OK calc_debug structure"
