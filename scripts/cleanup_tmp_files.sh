#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DRY_RUN="${DRY_RUN:-0}"
CSV_DAYS="${CSV_DAYS:-1}"
LOG_DAYS="${LOG_DAYS:-7}"
DUMP_DAYS="${DUMP_DAYS:-7}"

echo "[cleanup] root=$ROOT_DIR dry_run=$DRY_RUN"

targets=(
  "$ROOT_DIR/tmp"
  "$ROOT_DIR/imports"
  "$ROOT_DIR/backend/app/imports"
  "$ROOT_DIR/logs"
)

run_find() {
  local dir="$1"
  local expr="$2"
  if [ "$DRY_RUN" = "1" ]; then
    eval "find \"$dir\" $expr -print"
  else
    eval "find \"$dir\" $expr -print -delete"
  fi
}

for dir in "${targets[@]}"; do
  if [ -d "$dir" ]; then
    echo "[cleanup] scan $dir"
    # CSV/logs older than CSV_DAYS
    run_find "$dir" "-type f \\( -name \"*.csv\" -o -name \"*.log\" \\) -mtime +${CSV_DAYS}"
    # Large files >1GB older than 1 day
    run_find "$dir" "-type f -size +1G -mtime +1"
  fi
done

# Optional dumps cleanup in project root
run_find "$ROOT_DIR" "-maxdepth 1 -type f -name \"*.sql.gz\" -mtime +${DUMP_DAYS}"

echo "[cleanup] done"
