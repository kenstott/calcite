#!/usr/bin/env bash
# Worker 64: Cyber weekly — CWE, OSV, MITRE ATT&CK, GitHub advisories, ATT&CK→NIST mappings
# Schedule: weekly (e.g., Sunday 02:00 UTC via cron or run-pool.sh 64)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-cyber.sh" weekly
