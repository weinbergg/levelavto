#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p artifacts logs

TOP_N="${TOP_N:-6}"
BRANDS_CSV="${BRANDS_CSV:-}"

HOT_ENABLED="${HOT_ENABLED:-1}"
HOT_HOURS="${HOT_HOURS:-720}"
HOT_LIMIT="${HOT_LIMIT:-20000}"

CHUNK_LIMIT="${CHUNK_LIMIT:-20000}"
WORKERS="${WORKERS:-8}"
TIMEOUT="${TIMEOUT:-8}"
MAX_WIDTH="${MAX_WIDTH:-1280}"
QUALITY="${QUALITY:-82}"
FORMAT="${FORMAT:-webp}"
ORDER_BY="${ORDER_BY:-id_asc}"

TELEGRAM="${TELEGRAM:-1}"
TELEGRAM_INTERVAL="${TELEGRAM_INTERVAL:-1800}"
MAX_RUNTIME_HOURS="${MAX_RUNTIME_HOURS:-72}"

STATE_DIR="${STATE_DIR:-$ROOT_DIR/artifacts/mirror_de_top_brands_state}"
mkdir -p "$STATE_DIR"

START_TS="$(date +%s)"

get_env() {
  local key="$1"
  local env_file="$ROOT_DIR/.env"
  if [ ! -f "$env_file" ]; then
    return 0
  fi
  sed -n "s/^${key}=//p" "$env_file" | head -n1
}

TG_TOKEN="${TELEGRAM_BOT_TOKEN:-$(get_env TELEGRAM_BOT_TOKEN)}"
TG_CHAT="${TELEGRAM_CHAT_ID:-$(get_env TELEGRAM_CHAT_ID)}"
if [ -z "${TG_CHAT:-}" ]; then
  TG_CHAT="${TELEGRAM_ADMIN_CHAT_ID:-$(get_env TELEGRAM_ADMIN_CHAT_ID)}"
fi
if [ -z "${TG_CHAT:-}" ]; then
  TG_CHAT="$(get_env TELEGRAM_ALLOWED_IDS | cut -d',' -f1)"
fi

tg() {
  local msg="$1"
  if [ "${TELEGRAM}" != "1" ] || [ -z "${TG_TOKEN:-}" ] || [ -z "${TG_CHAT:-}" ]; then
    return 0
  fi
  curl -sS -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${TG_CHAT}" \
    --data-urlencode "text=${msg}" >/dev/null || true
}

safe_brand() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cs '[:alnum:]' '_'
}

state_get() {
  local brand="$1"
  local key="$2"
  local safe
  safe="$(safe_brand "$brand")"
  local path="$STATE_DIR/${safe}.state"
  if [ ! -f "$path" ]; then
    case "$key" in
      offset) echo 0 ;;
      hot_done) echo 0 ;;
      total) echo 0 ;;
      *) echo "" ;;
    esac
    return 0
  fi
  awk -F= -v k="$key" '$1==k {print substr($0, index($0, "=")+1)}' "$path" | tail -n1
}

state_set() {
  local brand="$1"
  local key="$2"
  local value="$3"
  local safe path tmp
  safe="$(safe_brand "$brand")"
  path="$STATE_DIR/${safe}.state"
  tmp="${path}.tmp"
  touch "$path"
  awk -F= -v k="$key" '$1!=k {print $0}' "$path" >"$tmp" || true
  printf '%s=%s\n' "$key" "$value" >>"$tmp"
  mv "$tmp" "$path"
}

runtime_exceeded() {
  local now elapsed limit_sec
  now="$(date +%s)"
  elapsed="$((now - START_TS))"
  limit_sec="$((MAX_RUNTIME_HOURS * 3600))"
  [ "$elapsed" -ge "$limit_sec" ]
}

query_top_brands() {
  docker compose exec -T db sh -lc "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -At -F '|' -c \"
with brand_counts as (
  select
    coalesce(nullif(trim(c.brand), ''), 'UNKNOWN') as brand,
    count(*) as cars_total
  from cars c
  join sources s on s.id = c.source_id
  where c.is_available = true
    and upper(c.country) = 'DE'
    and lower(s.key) like '%mobile%'
  group by 1
)
select brand, cars_total
from brand_counts
where brand <> 'UNKNOWN'
order by cars_total desc, brand asc
limit ${TOP_N};
\""
}

query_brand_total() {
  local brand="$1"
  docker compose exec -T db sh -lc "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -At -c \"
select count(*)
from cars c
join sources s on s.id = c.source_id
where c.is_available = true
  and upper(c.country) = 'DE'
  and lower(s.key) like '%mobile%'
  and lower(coalesce(nullif(trim(c.brand), ''), 'UNKNOWN')) = lower('$(printf "%s" "$brand" | sed "s/'/''/g")');
\"" | tail -n1
}

parse_report_field() {
  local path="$1"
  local key="$2"
  python3 - "$path" "$key" <<'PY'
import json, sys
path, key = sys.argv[1], sys.argv[2]
with open(path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)
value = data.get(key)
print(value if value is not None else "")
PY
}

run_one_chunk() {
  local brand="$1"
  local offset="$2"
  local limit="$3"
  local report_host="$4"
  local report_container="$5"
  local safe log_file
  local tg_args=()
  safe="$(safe_brand "$brand")"
  log_file="logs/mirror_de_top_${safe}_${offset}.log"
  if [ "${TELEGRAM}" = "1" ]; then
    tg_args+=(--telegram --telegram-interval "$TELEGRAM_INTERVAL")
  fi

  docker compose exec -T web python -m backend.app.scripts.mirror_car_images_local \
    --region EU \
    --country DE \
    --source-key mobile \
    --brands "$brand" \
    --limit-cars "$limit" \
    --offset-cars "$offset" \
    --max-images-per-car 1 \
    --order-by "$ORDER_BY" \
    --workers "$WORKERS" \
    --timeout "$TIMEOUT" \
    --max-width "$MAX_WIDTH" \
    --quality "$QUALITY" \
    --format "$FORMAT" \
    --skip-sync-thumbnail \
    "${tg_args[@]}" \
    --report-json "$report_container" 2>&1 | tee "$log_file" >&2

  local checked mirrored rewritten failed
  checked="$(parse_report_field "$report_host" checked)"
  mirrored="$(parse_report_field "$report_host" mirrored)"
  rewritten="$(parse_report_field "$report_host" rewritten)"
  failed="$(parse_report_field "$report_host" failed)"
  printf '%s\t%s\t%s\t%s\n' "$checked" "$mirrored" "$rewritten" "$failed"
}

run_hot_pass() {
  local brand="$1"
  local safe report_host report_container result checked mirrored rewritten failed
  local tg_args=()
  safe="$(safe_brand "$brand")"
  report_host="$ROOT_DIR/artifacts/mirror_de_top_hot_${safe}.json"
  report_container="/app/artifacts/mirror_de_top_hot_${safe}.json"
  if [ "${TELEGRAM}" = "1" ]; then
    tg_args+=(--telegram --telegram-interval "$TELEGRAM_INTERVAL")
  fi

  echo "[mirror_de_top_brands] hot brand=${brand} hours=${HOT_HOURS} limit=${HOT_LIMIT}"
  tg "de_top_brands hot start: ${brand} limit=${HOT_LIMIT} hours=${HOT_HOURS}"

  docker compose exec -T web python -m backend.app.scripts.mirror_car_images_local \
    --region EU \
    --country DE \
    --source-key mobile \
    --brands "$brand" \
    --limit-cars "$HOT_LIMIT" \
    --max-images-per-car 1 \
    --order-by listing_desc \
    --updated-since-hours "$HOT_HOURS" \
    --workers "$WORKERS" \
    --timeout "$TIMEOUT" \
    --max-width "$MAX_WIDTH" \
    --quality "$QUALITY" \
    --format "$FORMAT" \
    --skip-sync-thumbnail \
    "${tg_args[@]}" \
    --report-json "$report_container" | tee "logs/mirror_de_top_hot_${safe}.log"

  checked="$(parse_report_field "$report_host" checked)"
  mirrored="$(parse_report_field "$report_host" mirrored)"
  rewritten="$(parse_report_field "$report_host" rewritten)"
  failed="$(parse_report_field "$report_host" failed)"
  state_set "$brand" hot_done 1
  tg "de_top_brands hot done: ${brand} checked=${checked} mirrored=${mirrored} rewritten=${rewritten} failed=${failed}"
}

brands=()
brand_totals=()

if [ -n "$BRANDS_CSV" ]; then
  IFS=',' read -r -a brands <<<"$BRANDS_CSV"
  for i in "${!brands[@]}"; do
    brands[$i]="$(printf '%s' "${brands[$i]}" | xargs)"
    brand_totals[$i]="$(query_brand_total "${brands[$i]}")"
  done
else
  while IFS='|' read -r brand total; do
    [ -n "${brand:-}" ] || continue
    brands+=("$brand")
    brand_totals+=("${total:-0}")
  done < <(query_top_brands)
fi

if [ "${#brands[@]}" -eq 0 ]; then
  echo "[mirror_de_top_brands] no brands selected"
  exit 1
fi

tg "de_top_brands start: brands=$(IFS=,; echo "${brands[*]}") chunk=${CHUNK_LIMIT} hot=${HOT_ENABLED}"

for idx in "${!brands[@]}"; do
  brand="${brands[$idx]}"
  total="${brand_totals[$idx]:-0}"
  [ -n "$total" ] || total=0
  hot_done="$(state_get "$brand" hot_done)"
  offset="$(state_get "$brand" offset)"
  if [ -z "$offset" ]; then
    offset=0
  fi
  state_set "$brand" total "$total"

  echo "[mirror_de_top_brands] brand=${brand} total=${total} offset=${offset} hot_done=${hot_done}"
  tg "de_top_brands brand=${brand} total=${total} offset=${offset}"

  if [ "${HOT_ENABLED}" = "1" ] && [ "$hot_done" != "1" ]; then
    run_hot_pass "$brand"
  fi

  while [ "$offset" -lt "$total" ]; do
    if runtime_exceeded; then
      tg "de_top_brands paused: runtime limit ${MAX_RUNTIME_HOURS}h reached"
      echo "[mirror_de_top_brands] runtime limit reached"
      exit 0
    fi

    safe="$(safe_brand "$brand")"
    report_host="$ROOT_DIR/artifacts/mirror_de_top_${safe}_${offset}.json"
    report_container="/app/artifacts/mirror_de_top_${safe}_${offset}.json"

    echo "[mirror_de_top_brands] chunk brand=${brand} offset=${offset} limit=${CHUNK_LIMIT}/${total}"
    result="$(run_one_chunk "$brand" "$offset" "$CHUNK_LIMIT" "$report_host" "$report_container" | tail -n1)"
    checked="$(printf '%s' "$result" | awk -F'\t' '{print $1}')"
    mirrored="$(printf '%s' "$result" | awk -F'\t' '{print $2}')"
    rewritten="$(printf '%s' "$result" | awk -F'\t' '{print $3}')"
    failed="$(printf '%s' "$result" | awk -F'\t' '{print $4}')"

    tg "de_top_brands chunk ${brand}: offset=${offset}/${total} checked=${checked} mirrored=${mirrored} rewritten=${rewritten} failed=${failed}"

    offset="$((offset + CHUNK_LIMIT))"
    state_set "$brand" offset "$offset"
  done

  tg "de_top_brands brand done: ${brand} total=${total}"
done

tg "de_top_brands done"
echo "[mirror_de_top_brands] done $(date -Iseconds)"
