#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIN_FREE_GB="${MIN_FREE_GB:-12}"
TARGET_USAGE_PCT="${TARGET_USAGE_PCT:-85}"
DRY_RUN="${DRY_RUN:-0}"

CSV_DAYS="${CSV_DAYS:-7}"
LOG_DAYS="${LOG_DAYS:-14}"
PERF_DAYS="${PERF_DAYS:-10}"
THUMB_DAYS="${THUMB_DAYS:-10}"

log() {
  echo "[auto-cleanup] $*"
}

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "dry-run: $*"
  else
    eval "$@"
  fi
}

free_gb() {
  df -Pk / | awk 'NR==2 {printf "%d", $4/1024/1024}'
}

used_pct() {
  df -Pk / | awk 'NR==2 {gsub("%","",$5); print $5}'
}

cleanup_project_files() {
  log "project cleanup: tmp/imports/logs/thumb cache"
  run "find \"$ROOT_DIR/tmp\" -type f -mtime +${LOG_DAYS} -delete 2>/dev/null || true"
  run "find \"$ROOT_DIR/imports\" -type f -mtime +${CSV_DAYS} -delete 2>/dev/null || true"
  run "find \"$ROOT_DIR/backend/app/imports\" -type f -mtime +${CSV_DAYS} -delete 2>/dev/null || true"
  run "find \"$ROOT_DIR/logs\" -type f -mtime +${LOG_DAYS} -delete 2>/dev/null || true"
  run "find \"$ROOT_DIR/logs/perf_audit\" -mindepth 1 -maxdepth 1 -type d -mtime +${PERF_DAYS} -exec rm -rf {} + 2>/dev/null || true"
  run "find \"$ROOT_DIR/thumb_cache\" -type f -mtime +${THUMB_DAYS} -delete 2>/dev/null || true"
}

cleanup_docker() {
  log "docker prune"
  run "docker builder prune -af >/dev/null 2>&1 || true"
  run "docker image prune -af --filter 'until=168h' >/dev/null 2>&1 || true"
  run "docker container prune -f >/dev/null 2>&1 || true"
}

cleanup_system_logs() {
  log "system logs cleanup"
  run "find /var/log -type f -name '*.log' -size +200M -exec truncate -s 0 {} \\; 2>/dev/null || true"
  run "journalctl --vacuum-size=300M >/dev/null 2>&1 || true"
}

log "start root_free_gb=$(free_gb) root_used_pct=$(used_pct)% min_free_gb=$MIN_FREE_GB target_usage_pct=$TARGET_USAGE_PCT dry_run=$DRY_RUN"
cleanup_project_files

if [[ "$(free_gb)" -lt "$MIN_FREE_GB" || "$(used_pct)" -ge "$TARGET_USAGE_PCT" ]]; then
  cleanup_docker
  cleanup_system_logs
fi

log "done root_free_gb=$(free_gb) root_used_pct=$(used_pct)%"

