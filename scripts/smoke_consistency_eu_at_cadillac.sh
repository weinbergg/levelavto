#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
REGION="EU"
COUNTRY="AT"
BRAND="Cadillac"

echo "[smoke] consistency ${REGION}/${COUNTRY}/${BRAND}"

count="$(curl -fsS "${BASE_URL}/api/cars_count?region=${REGION}&country=${COUNTRY}&brand=${BRAND}" | jq -r '.count')"
cars="$(curl -fsS "${BASE_URL}/api/cars?region=${REGION}&country=${COUNTRY}&brand=${BRAND}&page_size=200" | jq '.items')"

echo "[smoke] count=${count}"

bad_brand="$(echo "${cars}" | jq -r 'map(select(.brand != "'"${BRAND}"'")) | length')"
if [ "${bad_brand}" != "0" ]; then
  echo "[smoke] FAIL: found cars with brand != ${BRAND}"
  exit 1
fi

models_api="$(curl -fsS "${BASE_URL}/api/filter_ctx_brand?region=${REGION}&country=${COUNTRY}&brand=${BRAND}" | jq -r '.models[].value' | sort -u)"
models_cars="$(echo "${cars}" | jq -r 'map(.model) | map(select(. != null and . != "")) | unique | .[]' | sort -u)"

if [ -n "${models_cars}" ]; then
  missing="$(comm -23 <(printf "%s\n" "${models_cars}") <(printf "%s\n" "${models_api}") || true)"
  if [ -n "${missing}" ]; then
    echo "[smoke] FAIL: models missing in filter_ctx_brand:"
    echo "${missing}"
    exit 1
  fi
fi

echo "[smoke] OK"
