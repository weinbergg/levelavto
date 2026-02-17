#!/usr/bin/env bash
set -euo pipefail

# Full VPS deploy pipeline:
# - optional git pull
# - docker rebuild/recreate
# - alembic migrations
# - cache prewarm
# - smoke HTTP checks (health/count/list/filter/model groups)
# - optional price recalc
# - optional perf audit
# - optional cleanup
#
# Usage (from /opt/levelavto):
#   chmod +x scripts/deploy_full_rebuild_check.sh
#   ./scripts/deploy_full_rebuild_check.sh
#
# Extended mode:
#   RUN_PULL=1 RUN_RECALC=1 RUN_PERF=1 RUN_CLEANUP=1 ./scripts/deploy_full_rebuild_check.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

RUN_PULL="${RUN_PULL:-0}"
RUN_BUILD="${RUN_BUILD:-1}"
RUN_RECREATE="${RUN_RECREATE:-1}"
RUN_MIGRATIONS="${RUN_MIGRATIONS:-1}"
RUN_PREWARM="${RUN_PREWARM:-1}"
RUN_RECALC="${RUN_RECALC:-0}"
RUN_PERF="${RUN_PERF:-1}"
RUN_CLEANUP="${RUN_CLEANUP:-0}"

BASE_URL="${BASE_URL:-http://localhost:8000}"
PERF_COUNTRY="${PERF_COUNTRY:-DE}"
PERF_BRAND="${PERF_BRAND:-BMW}"
PERF_MODEL="${PERF_MODEL:-X5}"
PERF_REQUESTS="${PERF_REQUESTS:-20}"
PERF_WARMUP="${PERF_WARMUP:-3}"
PERF_TIMEOUT="${PERF_TIMEOUT:-15}"

LOG_DIR="${LOG_DIR:-logs/deploy}"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/deploy_${TS}.log"
mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

step() {
  echo
  echo "==== $* ===="
}

http_probe() {
  local name="$1"
  local url="$2"
  local out
  out="$(curl -sS -o /tmp/deploy_probe_body.txt -w "%{http_code} %{time_total}" "$url" || echo "000 999")"
  local code="${out%% *}"
  local t="${out##* }"
  echo "[probe] ${name} code=${code} time=${t}s url=${url}"
  if [[ "$code" != 2* ]]; then
    echo "[probe] ${name} FAILED"
    sed -n '1,80p' /tmp/deploy_probe_body.txt || true
    return 1
  fi
  return 0
}

step "START"
echo "[deploy] ts=${TS}"
echo "[deploy] log=${LOG_FILE}"
echo "[deploy] flags: pull=${RUN_PULL} build=${RUN_BUILD} recreate=${RUN_RECREATE} migrations=${RUN_MIGRATIONS} prewarm=${RUN_PREWARM} recalc=${RUN_RECALC} perf=${RUN_PERF} cleanup=${RUN_CLEANUP}"

if [[ "$RUN_PULL" == "1" ]]; then
  step "GIT PULL"
  git pull
fi

if [[ "$RUN_BUILD" == "1" ]]; then
  step "DOCKER BUILD"
  docker compose build web
fi

if [[ "$RUN_RECREATE" == "1" ]]; then
  step "DOCKER RECREATE"
  docker compose up -d --force-recreate web
fi

step "DOCKER PS"
docker compose ps

if [[ "$RUN_MIGRATIONS" == "1" ]]; then
  step "MIGRATIONS"
  docker compose exec -T web alembic upgrade head
fi

if [[ "$RUN_PREWARM" == "1" ]]; then
  step "PREWARM CACHE"
  docker compose exec -T web python -m backend.app.scripts.prewarm_cache
fi

if [[ "$RUN_RECALC" == "1" ]]; then
  step "RECALC PRICES"
  docker compose exec -T web python -m backend.app.scripts.update_fx_prices --batch 1000 --limit 0
  docker compose exec -T web python -m backend.app.scripts.recalc_cached_prices
fi

step "SMOKE CHECKS"
http_probe "health" "${BASE_URL}/health"
http_probe "cars_count_de_bmw" "${BASE_URL}/api/cars_count?region=EU&country=DE&brand=BMW"
http_probe "advanced_count_de_bmw_x5" "${BASE_URL}/api/advanced_count?region=EU&country=DE&brand=BMW&model=X5"
http_probe "cars_list_de_bmw" "${BASE_URL}/api/cars?region=EU&country=DE&brand=BMW&sort=price_asc&page=1&page_size=12"
http_probe "filter_ctx_brand_grouped" "${BASE_URL}/api/filter_ctx_brand?region=EU&country=DE&brand=BMW"

step "CHECK PAYLOAD SNIPPETS"
curl -sS "${BASE_URL}/api/cars?region=EU&country=DE&brand=BMW&sort=price_asc&page=1&page_size=3" | python3 - <<'PY'
import json,sys
data=json.load(sys.stdin)
print("total=",data.get("total"))
for i,c in enumerate(data.get("items",[])[:3],1):
    print(f"#{i} id={c.get('id')} model={c.get('model')} thumb={bool(c.get('thumbnail_url'))} price_note={c.get('price_note')}")
PY
curl -sS "${BASE_URL}/api/filter_ctx_brand?region=EU&country=DE&brand=BMW" | python3 - <<'PY'
import json,sys
data=json.load(sys.stdin)
print("models=",len(data.get("models",[])),"groups=",len(data.get("model_groups",[])))
print("group_labels=", [g.get("label") for g in data.get("model_groups",[])[:8]])
PY

if [[ "$RUN_PERF" == "1" ]]; then
  step "PERF AUDIT"
  COUNTRY="$PERF_COUNTRY" \
  BRAND="$PERF_BRAND" \
  MODEL="$PERF_MODEL" \
  REQUESTS="$PERF_REQUESTS" \
  WARMUP="$PERF_WARMUP" \
  TIMEOUT_SEC="$PERF_TIMEOUT" \
  scripts/perf_full_audit.sh
  latest="$(ls -1dt logs/perf_audit/* | head -n1)"
  echo "[deploy] perf_latest=${latest}"
  sed -n '1,120p' "${latest}/report.md"
fi

if [[ "$RUN_CLEANUP" == "1" ]]; then
  step "CLEANUP"
  DRY_RUN=0 scripts/system_auto_cleanup.sh
fi

step "FINAL SNAPSHOT"
df -h /
docker system df
docker compose logs --since=15m web | grep -E "ADVANCED_COUNT|CARS_COUNT|CARS_LIST|count_slow|list_slow|ERROR" || true

step "DONE"
echo "[deploy] ok log=${LOG_FILE}"

