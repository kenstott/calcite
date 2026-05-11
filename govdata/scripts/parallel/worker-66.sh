#!/usr/bin/env bash
# Worker 66: Cyber static — re-run static standards (NIST 800-53, NIST CSF 2.0, CIS Controls,
# OWASP Top 10, ATT&CK→NIST mappings). Run after a framework publishes a new version.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-cyber.sh" static
