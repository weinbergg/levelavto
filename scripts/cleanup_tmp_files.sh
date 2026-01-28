#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
echo "[cleanup] root=$ROOT_DIR"

targets=(
  "$ROOT_DIR/tmp"
  "$ROOT_DIR/imports"
  "$ROOT_DIR/backend/app/imports"
)

for dir in "${targets[@]}"; do
  if [ -d "$dir" ]; then
    echo "[cleanup] scan $dir"
    # Remove CSV/log files older than 1 day
    find "$dir" -type f \( -name "*.csv" -o -name "*.log" \) -mtime +1 -print -delete
  fi
done

echo "[cleanup] done"
