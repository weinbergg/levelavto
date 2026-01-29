#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
REGION="EU"
COUNTRY="AT"
BRAND="Cadillac"

echo "[smoke] consistency ${REGION}/${COUNTRY}/${BRAND}"

count_resp="$(curl -fsS "${BASE_URL}/api/cars_count?region=${REGION}&country=${COUNTRY}&brand=${BRAND}" || true)"
if [ -z "${count_resp}" ]; then
  echo "[smoke] FAIL: empty response from /api/cars_count"
  exit 1
fi
count="$(echo "${count_resp}" | jq -r '.count' 2>/dev/null || echo "")"
if [ -z "${count}" ]; then
  echo "[smoke] FAIL: /api/cars_count response: ${count_resp}"
  exit 1
fi

cars_resp="$(curl -fsS "${BASE_URL}/api/cars?region=${REGION}&country=${COUNTRY}&brand=${BRAND}&page_size=100" || true)"
if [ -z "${cars_resp}" ]; then
  echo "[smoke] FAIL: empty response from /api/cars"
  exit 1
fi
cars="$(echo "${cars_resp}" | jq '.items' 2>/dev/null || echo "")"
if [ -z "${cars}" ]; then
  echo "[smoke] FAIL: /api/cars response: ${cars_resp}"
  exit 1
fi

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

items_len="$(echo "${cars}" | jq 'length')"
if [ "${count}" -lt "${items_len}" ]; then
  echo "[smoke] FAIL: count ${count} < items.length ${items_len}"
  exit 1
fi

echo "[smoke] OK (count=${count}, items=${items_len})"
