#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://localhost:8000}"

html=$(curl -fsS "$BASE_URL/catalog?region=EU&country=AT&brand=Cadillac")
if ! echo "$html" | rg -q 'option value="Cadillac"[^>]*selected'; then
  echo "Cadillac not selected in catalog HTML" >&2; exit 1;
fi

count=$(curl -fsS "$BASE_URL/api/cars_count?region=EU&country=AT&brand=Cadillac" | jq -r '.count')
if [ -z "$count" ] || [ "$count" = "null" ] || [ "$count" -le 0 ]; then
  echo "cars_count invalid: $count" >&2; exit 1;
fi

brand=$(curl -fsS "$BASE_URL/api/cars?region=EU&country=AT&brand=Cadillac&page_size=5" | jq -r '.items[0].brand')
if [ "$brand" != "Cadillac" ]; then
  echo "api cars brand mismatch: $brand" >&2; exit 1;
fi

echo "OK"
