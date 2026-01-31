#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "[smoke] color groups"
json="$(curl -fsS "${BASE_URL}/api/filter_ctx_base?region=EU")"
count="$(echo "${json}" | jq '.colors_basic | length')"

if [ "${count}" -gt 12 ]; then
  echo "[smoke] FAIL: colors_basic count=${count} > 12"
  exit 1
fi

echo "[smoke] OK colors_basic=${count}"
