#!/usr/bin/env bash
# Fetch latest mobilede_active_offers.csv and import into DB.
# Requires env:
#   MOBILEDE_HOST (default https://parsers1-valdez.auto-parser.ru)
#   MOBILEDE_USER
#   MOBILEDE_PASS
#   WEB_SERVICE (docker-compose service name, default "web")

set -euo pipefail

HOST="${MOBILEDE_HOST:-https://parsers1-valdez.auto-parser.ru}"
USER="${MOBILEDE_USER:-}"
PASS="${MOBILEDE_PASS:-}"
WEB="${WEB_SERVICE:-web}"

if [[ -z "$USER" || -z "$PASS" ]]; then
  echo "[fetch-mobilede] MOBILEDE_USER/PASS not set" >&2
  exit 1
fi

auth=$(printf "%s:%s" "$USER" "$PASS" | base64)
today_utc=$(date -u +%Y-%m-%d)
yesterday_utc=$(date -u -d "yesterday" +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)

fetch_date="$today_utc"
dest_dir="backend/app/imports/mobilede"
mkdir -p "$dest_dir"

download() {
  local d="$1"
  local url="${HOST}/mobilede/${d}/mobilede_active_offers.csv"
  local dest="${dest_dir}/mobilede_active_offers_${d}.csv"
  echo "[fetch-mobilede] trying ${url}"
  code=$(curl -w "%{http_code}" -s -L \
    -H "authorization: Basic ${auth}" \
    -o "${dest}" "${url}")
  if [[ "$code" == "200" ]]; then
    size=$(stat -c%s "${dest}" 2>/dev/null || stat -f%z "${dest}")
    echo "[fetch-mobilede] ok ${d} size=${size}"
    # symlink relative to imports/ so it works inside container
    ln -sf "mobilede/mobilede_active_offers_${d}.csv" backend/app/imports/mobilede_active_offers.csv
    return 0
  else
    echo "[fetch-mobilede] failed ${d} code=${code}"
    rm -f "${dest}"
    return 1
  fi
}

if ! download "$fetch_date"; then
  echo "[fetch-mobilede] retry with ${yesterday_utc}"
  download "$yesterday_utc"
fi

# Import without deactivation (skip missing)
echo "[fetch-mobilede] importing..."
docker compose exec "$WEB" python -m backend.app.tools.mobilede_csv_import --file backend/app/imports/mobilede_active_offers.csv --skip-deactivate --stats-file backend/app/runtime/jobs/mobilede_last.json
python -m backend.app.tools.notify_tg --job mobilede --result backend/app/runtime/jobs/mobilede_last.json || true
echo "[fetch-mobilede] done"
