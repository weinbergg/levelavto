#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

CSV_PATH_HOST="${1:-}"

if [ -z "$CSV_PATH_HOST" ]; then
  CSV_PATH_HOST="$(ls -1t /opt/levelavto/tmp/mobilede_active_offers_*.csv 2>/dev/null | head -n1 || true)"
fi

echo "[mobilede_gap] root=$ROOT_DIR"
echo "[mobilede_gap] csv=${CSV_PATH_HOST:-<missing>}"

if [ -n "$CSV_PATH_HOST" ] && [ -f "$CSV_PATH_HOST" ]; then
  python3 - "$CSV_PATH_HOST" <<'PY'
import csv, sys
path = sys.argv[1]
raw = 0
with_inner_id = 0
with_url = 0
with_title_or_mark_model = 0
importable = 0
with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
    reader = csv.reader(f, delimiter="|", quotechar='"', strict=False)
    header = next(reader, None)
    idx = {name.strip(): i for i, name in enumerate(header or [])}
    def get(row, name):
        i = idx.get(name)
        if i is None or i >= len(row):
            return ""
        return (row[i] or "").strip()
    for row in reader:
        raw += 1
        inner_id = get(row, "inner_id")
        url = get(row, "url")
        mark = get(row, "mark")
        model = get(row, "model")
        title = get(row, "title")
        if inner_id:
            with_inner_id += 1
        if url:
            with_url += 1
        if mark or model or title:
            with_title_or_mark_model += 1
        if inner_id and url and (mark or model or title):
            importable += 1
    print(f"[mobilede_gap] csv_raw_rows={raw}")
    print(f"[mobilede_gap] csv_with_inner_id={with_inner_id}")
    print(f"[mobilede_gap] csv_with_url={with_url}")
    print(f"[mobilede_gap] csv_with_title_or_mark_model={with_title_or_mark_model}")
    print(f"[mobilede_gap] csv_importable_by_current_rules={importable}")
PY
else
  echo "[mobilede_gap] csv_missing=1"
fi

docker compose exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
select
  pr.started_at,
  prs.total_seen,
  prs.inserted,
  prs.updated,
  prs.deactivated
from parser_run_sources prs
join parser_runs pr on pr.id = prs.parser_run_id
join sources s on s.id = prs.source_id
where lower(s.key) like '\''%mobile%'\''
order by pr.started_at desc
limit 5;"'

docker compose exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
select
  count(*) as active_db,
  count(*) filter (where coalesce(c.last_seen_at, c.updated_at) < now() - interval '\''7 days'\'') as stale_7d,
  count(*) filter (where coalesce(c.last_seen_at, c.updated_at) < now() - interval '\''14 days'\'') as stale_14d
from cars c
join sources s on s.id = c.source_id
where c.is_available = true
  and upper(c.country) = '\''DE'\''
  and lower(s.key) like '\''%mobile%'\'';"'

docker compose exec -T db sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
select
  count(*) as total_active,
  count(*) filter (where c.total_price_rub_cached is null) as missing_total,
  count(*) filter (where c.calc_updated_at is null or c.calc_updated_at < c.updated_at) as stale_calc
from cars c
where c.is_available = true
  and (c.country is null or upper(c.country) not like '\''KR%'\'');"'
