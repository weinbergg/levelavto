#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "[smoke] country labels"
json="$(curl -fsS "${BASE_URL}/api/filter_ctx_base?region=EU")"

for code in DE AT IT NL; do
  label="$(echo "${json}" | jq -r --arg c "${code}" '.countries[] | select(.value==$c) | .label' | head -n 1)"
  if [ -z "${label}" ] || [ "${label}" = "${code}" ]; then
    echo "[smoke] FAIL: label for ${code} is missing or equal to code"
    exit 1
  fi
done

echo "[smoke] OK"
