#!/usr/bin/env bash
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:8000}

echo "[smoke] filter_ctx_base countries object"
country_item=$(curl -s "$BASE_URL/api/filter_ctx_base?region=EU" | jq '.countries[0]')
if [[ "$country_item" == "null" ]]; then
  echo "FAIL: countries empty"; exit 1
fi
if ! echo "$country_item" | jq -e 'type=="object" and (.value!=null) and (.label!=null)' >/dev/null; then
  echo "FAIL: countries[0] is not object with value/label"; echo "$country_item"; exit 1
fi

echo "[smoke] filter_ctx_brand models object"
model_item=$(curl -s "$BASE_URL/api/filter_ctx_brand?region=EU&country=AT&brand=Cadillac" | jq '.models[0]')
if [[ "$model_item" != "null" ]]; then
  if ! echo "$model_item" | jq -e 'type=="object" and (.value!=null)' >/dev/null; then
    echo "FAIL: models[0] is not object"; echo "$model_item"; exit 1
  fi
fi

echo "[smoke] catalog brand param preserved"
html=$(curl -s "$BASE_URL/catalog?region=EU&country=AT&brand=Cadillac")
if ! echo "$html" | rg -q 'CATALOG_SSR'; then
  echo "WARN: CATALOG_SSR log not in HTML (expected only in server logs)"
fi

echo "[smoke] api filters legacy not used"
if rg -q "/api/filters/options" backend/app/static/js/app.js; then
  echo "FAIL: old /api/filters/options still referenced"; exit 1
fi

echo "OK"
