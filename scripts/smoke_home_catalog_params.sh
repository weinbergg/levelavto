#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "[smoke] home form"
html_home=$(curl -fsS "$BASE_URL/")
if ! echo "$html_home" | rg -q 'id="home-search"'; then
  echo "home-search form missing" >&2; exit 1;
fi
if ! echo "$html_home" | rg -q 'name="brand"'; then
  echo "brand select missing" >&2; exit 1;
fi
if ! echo "$html_home" | rg -q 'id="home-region-slot-select"'; then
  echo "region slot select missing" >&2; exit 1;
fi

echo "[smoke] catalog brand preservation"
html_cat=$(curl -fsS "$BASE_URL/catalog?region=EU&country=AT&brand=Cadillac")
if echo "$html_cat" | rg -q 'eu_country'; then
  echo "eu_country leaked into catalog HTML" >&2; exit 1;
fi
if ! echo "$html_cat" | rg -q 'option value="Cadillac"[^>]*selected'; then
  echo "Cadillac not selected in catalog" >&2; exit 1;
fi

echo "[smoke] api count + cars"
count=$(curl -fsS "$BASE_URL/api/cars_count?region=EU&country=AT&brand=Cadillac" | jq -r '.count')
if [ -z "$count" ] || [ "$count" = "null" ] || [ "$count" -le 0 ]; then
  echo "cars_count invalid: $count" >&2; exit 1;
fi
brand=$(curl -fsS "$BASE_URL/api/cars?region=EU&country=AT&brand=Cadillac&page_size=1" | jq -r '.items[0].brand')
if [ "$brand" != "Cadillac" ]; then
  echo "api cars brand mismatch: $brand" >&2; exit 1;
fi

echo "OK"
