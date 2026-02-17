#!/usr/bin/env bash
set -euo pipefail

# Redis cache maintenance for levelavto
#
# Modes:
#   default: remove only stale lock keys
#   PURGE_SOFT=1: also remove heavy short-lived caches (cars_list_full, filter_ctx)
#   PURGE_HARD=1: remove all cars_* and filter_* cache keys
#   BUMP_DATASET=1: bump dataset_version (forces cache version rollover)
#
# Usage:
#   scripts/cache_maintenance.sh
#   PURGE_SOFT=1 scripts/cache_maintenance.sh
#   PURGE_HARD=1 BUMP_DATASET=1 scripts/cache_maintenance.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PURGE_SOFT="${PURGE_SOFT:-0}"
PURGE_HARD="${PURGE_HARD:-0}"
BUMP_DATASET="${BUMP_DATASET:-0}"

echo "[cache-maint] start purge_soft=${PURGE_SOFT} purge_hard=${PURGE_HARD} bump_dataset=${BUMP_DATASET}"

docker compose exec -T web env \
  PURGE_SOFT="$PURGE_SOFT" \
  PURGE_HARD="$PURGE_HARD" \
  BUMP_DATASET="$BUMP_DATASET" \
  python - <<'PY'
import os
from backend.app.utils.redis_cache import redis_delete_by_pattern, bump_dataset_version

purge_soft = os.getenv("PURGE_SOFT", "0") == "1"
purge_hard = os.getenv("PURGE_HARD", "0") == "1"
bump_ds = os.getenv("BUMP_DATASET", "0") == "1"

deleted = 0

# Always clean lock keys.
for p in ("*:lock", "cars_*:lock", "filter_*:lock"):
    deleted += redis_delete_by_pattern(p)

if purge_soft:
    for p in (
        "cars_list_full:*",
        "filter_ctx:*",
        "filter_ctx_*",
    ):
        deleted += redis_delete_by_pattern(p)

if purge_hard:
    for p in (
        "cars_count:*",
        "cars_list:*",
        "cars_list_full:*",
        "filter_ctx:*",
        "filter_ctx_*",
        "filter_payload:*",
        "total_cars:*",
    ):
        deleted += redis_delete_by_pattern(p)

new_ver = None
if bump_ds:
    new_ver = bump_dataset_version()

print(f"[cache-maint] deleted={deleted} dataset_version={new_ver}")
PY

echo "[cache-maint] done"
