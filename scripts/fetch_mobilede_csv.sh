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
TMP_DIR="${MOBILEDE_TMP_DIR:-/opt/levelavto/tmp}"
KEEP_CSV="${KEEP_CSV:-0}"
MIN_FREE_GB="${MOBILEDE_MIN_FREE_GB:-20}"

if [[ -z "$USER" || -z "$PASS" ]]; then
  echo "[fetch-mobilede] MOBILEDE_USER/PASS not set" >&2
  exit 1
fi

auth=$(printf "%s:%s" "$USER" "$PASS" | base64)
today_utc=$(date -u +%Y-%m-%d)
yesterday_utc=$(date -u -d "yesterday" +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)

fetch_date="$today_utc"
mkdir -p "$TMP_DIR"

avail_kb=$(df -Pk "$TMP_DIR" | awk 'NR==2{print $4}')
min_kb=$((MIN_FREE_GB * 1024 * 1024))
if [[ -n "$avail_kb" && "$avail_kb" -lt "$min_kb" ]]; then
  echo "[fetch-mobilede] not enough free space: need ${MIN_FREE_GB}GB free on ${TMP_DIR}" >&2
  exit 2
fi

download() {
  local d="$1"
  local url="${HOST}/mobilede/${d}/mobilede_active_offers.csv"
  local dest="${TMP_DIR}/mobilede_active_offers_${d}.csv"
  echo "[fetch-mobilede] trying ${url}"
  code=$(curl -w "%{http_code}" -s -L \
    -H "authorization: Basic ${auth}" \
    -o "${dest}" "${url}")
  if [[ "$code" == "200" ]]; then
    size=$(stat -c%s "${dest}" 2>/dev/null || stat -f%z "${dest}")
    echo "[fetch-mobilede] ok ${d} size=${size}"
    return 0
  else
    echo "[fetch-mobilede] failed ${d} code=${code}"
    rm -f "${dest}"
    return 1
  fi
}

if ! download "$fetch_date"; then
  echo "[fetch-mobilede] retry with ${yesterday_utc}"
  fetch_date="$yesterday_utc"
  download "$fetch_date"
fi

# Import without deactivation (skip missing)
echo "[fetch-mobilede] importing..."
docker compose exec "$WEB" python -m backend.app.tools.mobilede_csv_import --file "/app/tmp/mobilede_active_offers_${fetch_date}.csv" --skip-deactivate --stats-file backend/app/runtime/jobs/mobilede_last.json
python -m backend.app.tools.notify_tg --job mobilede --result backend/app/runtime/jobs/mobilede_last.json || true
echo "[fetch-mobilede] invalidating redis cache keys..."
docker compose exec -T redis redis-cli KEYS "cars_count:*" | xargs -r docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli KEYS "cars_list:*" | xargs -r docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli KEYS "filter_ctx_*" | xargs -r docker compose exec -T redis redis-cli DEL
if [[ "$KEEP_CSV" != "1" ]]; then
  rm -f "${TMP_DIR}/mobilede_active_offers_${fetch_date}.csv" || true
  rm -f "${TMP_DIR}/mobilede_active_offers_${yesterday_utc}.csv" || true
fi
echo "[fetch-mobilede] done"
