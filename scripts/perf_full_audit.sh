#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-http://localhost:8000}"
REQUESTS="${REQUESTS:-30}"
WARMUP="${WARMUP:-5}"
TIMEOUT_SEC="${TIMEOUT_SEC:-12}"
COUNTRY="${COUNTRY:-DE}"
OUT_DIR="${OUT_DIR:-logs/perf_audit}"
TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$OUT_DIR/$TS"
RAW_DIR="$RUN_DIR/raw"
REPORT_MD="$RUN_DIR/report.md"
SUMMARY_JSON="$RUN_DIR/summary.json"

mkdir -p "$RAW_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[perf] missing required command: $1" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd python3
require_cmd docker

{
  echo "# Performance Audit"
  echo
  echo "- Time: $(date -Iseconds)"
  echo "- Base URL: $BASE_URL"
  echo "- Requests per endpoint: $REQUESTS"
  echo "- Warmup requests: $WARMUP"
  echo "- Timeout per request: ${TIMEOUT_SEC}s"
  echo
} >"$REPORT_MD"

append_section() {
  echo >>"$REPORT_MD"
  echo "## $1" >>"$REPORT_MD"
  echo >>"$REPORT_MD"
}

capture_cmd() {
  local title="$1"
  shift
  local out_file="$RAW_DIR/$title.txt"
  append_section "$title"
  if "$@" >"$out_file" 2>&1; then
    echo '```' >>"$REPORT_MD"
    sed -n '1,120p' "$out_file" >>"$REPORT_MD"
    echo '```' >>"$REPORT_MD"
  else
    echo '```' >>"$REPORT_MD"
    sed -n '1,120p' "$out_file" >>"$REPORT_MD"
    echo '```' >>"$REPORT_MD"
    echo "- Command failed: $*" >>"$REPORT_MD"
  fi
}

bench_endpoint() {
  local name="$1"
  local path="$2"
  local target="$BASE_URL$path"
  local out_file="$RAW_DIR/bench_${name}.tsv"
  : >"$out_file"

  local i
  for ((i=1; i<=WARMUP; i++)); do
    curl -sS -o /dev/null --max-time "$TIMEOUT_SEC" "$target" || true
  done
  for ((i=1; i<=REQUESTS; i++)); do
    curl -sS -o /dev/null --max-time "$TIMEOUT_SEC" -w "%{time_total}\t%{http_code}\n" "$target" >>"$out_file" || echo "-1\t000" >>"$out_file"
  done
}

append_section "Host Snapshot"
capture_cmd "uname" uname -a
capture_cmd "uptime" uptime
capture_cmd "free_mb" free -m
capture_cmd "disk_root" df -h /

append_section "Docker Snapshot"
capture_cmd "docker_compose_ps" docker compose ps
capture_cmd "docker_system_df" docker system df

append_section "Service Snapshot"
capture_cmd "health" curl -sS --max-time "$TIMEOUT_SEC" "$BASE_URL/health"
capture_cmd "web_env_perf_flags" docker compose exec -T web sh -lc "env | grep -E 'CAR_API_TIMING|CAR_API_SQL|CATALOG_USE_FAST_COUNT|CATALOG_WITH_PHOTO_STATS|PRICE_ROUND_STEP_RUB|REDIS_URL' || true"

append_section "Data Snapshot"
capture_cmd "db_counts" docker compose exec -T web python - <<'PY'
from sqlalchemy import text
from backend.app.db import SessionLocal
with SessionLocal() as db:
    total = db.execute(text("select count(*) from cars")).scalar_one()
    available = db.execute(text("select count(*) from cars where is_available is true")).scalar_one()
    no_price = db.execute(text("select count(*) from cars where is_available is true and total_price_rub_cached is null and price_rub_cached is null")).scalar_one()
    missing_share = (float(no_price) / float(available) * 100.0) if available else 0.0
    print(f"cars_total={total}")
    print(f"cars_available={available}")
    print(f"cars_no_cached_price={no_price}")
    print(f"cars_no_cached_price_pct={missing_share:.2f}")
PY
capture_cmd "redis_memory" docker compose exec -T redis sh -lc "redis-cli INFO memory | sed -n '1,40p'" 

append_section "API Benchmarks"
bench_endpoint "health" "/health"
bench_endpoint "filter_ctx_base_eu" "/api/filter_ctx_base?region=EU"
bench_endpoint "filter_payload_eu" "/api/filter_payload?region=EU&country=${COUNTRY}"
bench_endpoint "cars_count_eu" "/api/cars_count?region=EU&country=${COUNTRY}"
bench_endpoint "cars_list_eu" "/api/cars?region=EU&country=${COUNTRY}&sort=price_asc&page=1&page_size=12"
bench_endpoint "cars_list_search" "/api/cars?region=EU&country=${COUNTRY}&q=diesel&sort=price_asc&page=1&page_size=12"

python3 - "$RAW_DIR" "$SUMMARY_JSON" >>"$REPORT_MD" <<'PY'
import json
import pathlib
import statistics
import sys

raw_dir = pathlib.Path(sys.argv[1])
summary_json = pathlib.Path(sys.argv[2])

def percentile(values, pct):
    if not values:
        return None
    xs = sorted(values)
    idx = max(0, min(len(xs) - 1, int(round((pct / 100.0) * (len(xs) - 1)))))
    return xs[idx]

rows = []
summary = {"benchmarks": []}
for f in sorted(raw_dir.glob("bench_*.tsv")):
    values = []
    codes = []
    for line in f.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        try:
            t = float(parts[0])
        except Exception:
            t = -1.0
        code = parts[1]
        codes.append(code)
        if t >= 0:
            values.append(t)
    total = len(codes)
    ok = sum(1 for c in codes if c.startswith("2"))
    errors = total - ok
    p50 = percentile(values, 50)
    p95 = percentile(values, 95)
    p99 = percentile(values, 99)
    avg = statistics.mean(values) if values else None
    item = {
        "name": f.stem.replace("bench_", ""),
        "requests": total,
        "ok": ok,
        "errors": errors,
        "avg_s": avg,
        "p50_s": p50,
        "p95_s": p95,
        "p99_s": p99,
        "max_s": max(values) if values else None,
    }
    summary["benchmarks"].append(item)
    rows.append(item)

summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

print("| endpoint | req | ok | err | avg, s | p50, s | p95, s | p99, s | max, s |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    def fmt(v):
        return "-" if v is None else f"{v:.3f}"
    print(
        f"| {r['name']} | {r['requests']} | {r['ok']} | {r['errors']} | "
        f"{fmt(r['avg_s'])} | {fmt(r['p50_s'])} | {fmt(r['p95_s'])} | {fmt(r['p99_s'])} | {fmt(r['max_s'])} |"
    )
PY

append_section "Recent Timing Logs"
capture_cmd "web_logs_timing" docker compose logs --tail=300 web

echo
echo "[perf] report: $REPORT_MD"
echo "[perf] summary: $SUMMARY_JSON"

