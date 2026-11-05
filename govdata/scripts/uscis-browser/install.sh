#!/usr/bin/env bash
# Installs the pinned browser-automation dependency (puppeteer 23.11.1 and the Chrome it
# downloads) for the uscis.gov workbook fetch. Run once per host; needs node >= 18 and npm.
# Prints the helper path to set as USCIS_BROWSER_SCRIPT.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
command -v node >/dev/null || { echo "node is not installed" >&2; exit 1; }
command -v npm >/dev/null || { echo "npm is not installed" >&2; exit 1; }
cd "$dir"
if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi
echo "USCIS_BROWSER_SCRIPT=$dir/fetch-uscis.js"
