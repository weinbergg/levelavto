#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "[smoke] thumbs"
html="$(curl -fsS "${BASE_URL}/catalog?region=EU&country=DE")"
thumbs="$(echo "${html}" | rg -o '/thumb\\?u=[^"&]+' | head -n 3)"

if [ -z "${thumbs}" ]; then
  echo "[smoke] FAIL: no /thumb URLs found in catalog HTML"
  exit 1
fi

while read -r t; do
  url="${BASE_URL}${t}"
  ct="$(curl -fsSI "${url}" | rg -i "^content-type" | head -n 1 || true)"
  if ! echo "${ct}" | rg -qi "image/"; then
    echo "[smoke] FAIL: ${url} content-type not image"
    exit 1
  fi
done <<< "${thumbs}"

echo "[smoke] OK"
