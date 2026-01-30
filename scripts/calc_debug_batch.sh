#!/usr/bin/env bash
set -euo pipefail

OUT_DIR=${1:-artifacts/calc_debug}
EUR_RATE=${2:-91.28}

python -m backend.app.scripts.calc_debug_batch --eur-rate "$EUR_RATE" --out-dir "$OUT_DIR"

printf "[calc_debug_batch] saved to %s\n" "$OUT_DIR"
