#!/usr/bin/env bash
# Worker 81: Patents daily/recurring load — current year only.
# Gated by quarterly release window: exits immediately in months outside {3,6,9,12}.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-patents.sh" daily
