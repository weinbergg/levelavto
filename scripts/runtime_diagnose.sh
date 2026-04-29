#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

section() {
  printf '\n=== %s ===\n' "$1"
}

section "time"
date -Iseconds || true

section "host"
uname -a || true
uptime || true
free -m || true
df -h / || true

section "docker"
docker compose ps || true

section "active_jobs"
ps aux | grep -E 'pytest|prewarm|recalc|refresh_spec_inference|mirror_mobilede_thumbs|mobilede_daily' | grep -v grep || true

section "health"
curl -sS "http://localhost:8000/health" || true
printf '\n'

section "timings"
for url in \
  "http://localhost:8000/api/filter_ctx_base?region=EU&country=DE" \
  "http://localhost:8000/api/filter_payload?region=EU&country=DE" \
  "http://localhost:8000/api/cars_count?region=EU&country=DE" \
  "http://localhost:8000/api/cars?region=EU&country=DE&sort=price_asc&page=1&page_size=12" \
  "http://localhost:8000/catalog?region=EU&country=DE&sort=price_asc"
do
  printf '\nURL=%s\n' "$url"
  curl -sS -o /dev/null -D - --max-time 20 -w 'TTFB=%{time_starttransfer} TOTAL=%{time_total}\n' "$url" || true
done

section "web_logs"
docker compose logs --since 20m web | grep -E 'count_slow|list_slow|CARS_COUNT|CARS_LIST|FILTER_CTX|FILTER_PAYLOAD|thumb_fetch_failed|thumb_fetch_exhausted|thumb_lock_busy|visible_price_refresh|price_sensitive_recalc|lazy_recalc' | tail -n 200 || true

section "db_activity"
docker compose exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -P pager=off -c "
select pid,
       now() - query_start as age,
       state,
       wait_event_type,
       wait_event,
       left(query, 220) as query
from pg_stat_activity
where datname = current_database()
  and pid <> pg_backend_pid()
order by query_start asc
limit 20;
"' || true

section "db_counts"
docker compose exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -P pager=off -c "
select
  max(updated_at) as max_updated_at,
  max(last_seen_at) as max_last_seen_at,
  count(*) filter (where first_seen_at >= now() - interval '\''24 hours'\'') as new_24h,
  count(*) filter (where updated_at >= now() - interval '\''24 hours'\'') as updated_24h,
  count(*) filter (where last_seen_at >= now() - interval '\''24 hours'\'') as seen_24h,
  count(*) filter (where is_available and total_price_rub_cached is null) as missing_total,
  count(*) filter (where is_available and calc_updated_at is null) as never_calculated,
  count(*) filter (where is_available and calc_updated_at < updated_at) as stale_calc,
  max(calc_updated_at) as max_calc_updated_at
from cars;
"' || true

section "job_markers"
ls -l backend/app/runtime/jobs/*_last.json 2>/dev/null || true

section "recent_perf_audit"
ls -lt logs/perf_audit 2>/dev/null | head || true
