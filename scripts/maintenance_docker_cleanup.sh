#!/usr/bin/env bash
set -euo pipefail

echo "[cleanup] df -h"
df -h
echo
echo "[cleanup] docker system df"
docker system df
echo
echo "[cleanup] docker builder prune -af"
docker builder prune -af
echo
echo "[cleanup] done"
