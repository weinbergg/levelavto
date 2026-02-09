#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

set -a
if [ -f "$ROOT_DIR/.env" ]; then
  . "$ROOT_DIR/.env"
fi
set +a

exec /usr/bin/docker compose exec -T web python -m backend.app.scripts.update_fx_prices \
  --batch "${BATCH:-2000}" \
  --sleep "${SLEEP_SEC:-1}" \
  ${COUNTRY:+--country "$COUNTRY"} \
  ${ONLY_IDS:+--only-ids "$ONLY_IDS"} \
  ${DRY_RUN:+--dry-run} \
  ${TELEGRAM:+--telegram}
