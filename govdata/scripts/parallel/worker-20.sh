#!/usr/bin/env bash
# Worker 20: Geographic Data (TIGER, HUD, USDA, Gazetteer, Watersheds)
# Schedule: daily (each table gates itself via incrementalTtlDays + releaseWindow)
# Heap: 4 GB min / 6 GB max (TIGER shapefiles need extra heap for geometry parsing)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"; load_env
"$SCRIPT_DIR/worker-geo.sh"
