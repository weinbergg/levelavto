#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://localhost:8000}"

html_cat=$(curl -fsS "$BASE_URL/catalog?region=EU&country=AT&eu_country=AT&brand=Cadillac")
if echo "$html_cat" | rg -q 'eu_country'; then
  echo "eu_country leaked into catalog HTML" >&2; exit 1;
fi
if ! echo "$html_cat" | rg -q 'select name="country"'; then
  echo "country select missing" >&2; exit 1;
fi
if ! echo "$html_cat" | rg -q 'option value="AT"[^>]*selected'; then
  echo "country AT not selected" >&2; exit 1;
fi

echo "OK"
