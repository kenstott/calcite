#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POOL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
UNIT_DIR="${HOME}/.config/systemd/user"
SCHEDULED="$POOL_DIR/run-scheduled.sh"
LOG_DIR="$POOL_DIR/runs"
ERROR_LOG="$LOG_DIR/errors.log"

if [ "${1:-}" = "--uninstall" ]; then
  systemctl --user disable --now govdata-pool.service 2>/dev/null || true
  # Remove legacy timer units if present from a prior installation
  systemctl --user disable --now govdata-pool-daily.timer govdata-pool-historical.timer 2>/dev/null || true
  rm -f "$UNIT_DIR/govdata-pool.service"
  rm -f "$UNIT_DIR/govdata-pool-daily.service"
  rm -f "$UNIT_DIR/govdata-pool-daily.timer"
  rm -f "$UNIT_DIR/govdata-pool-historical.service"
  rm -f "$UNIT_DIR/govdata-pool-historical.timer"
  systemctl --user daemon-reload
  echo "Uninstalled govdata pool service."
  exit 0
fi

mkdir -p "$UNIT_DIR" "$LOG_DIR" "$LOG_DIR/pids"

# ── Perpetual pool service ─────────────────────────────────────────────────────
# run-scheduled.sh cycles historical/daily windows forever and auto-restarts on
# pool crashes.  systemd Restart=on-failure provides a safety net for the runner
# process itself (e.g. script errors); RestartSec=60 avoids tight restart loops.
cat > "$UNIT_DIR/govdata-pool.service" <<EOF
[Unit]
Description=GovData ETL perpetual pool (historical/daily cycling)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=${POOL_DIR}
ExecStart=${SCHEDULED}
StandardOutput=append:${LOG_DIR}/pool-service.log
StandardError=append:${ERROR_LOG}
Restart=on-failure
RestartSec=60

[Install]
WantedBy=default.target
EOF

# Remove legacy timer units if present
for u in govdata-pool-daily.service govdata-pool-daily.timer govdata-pool-historical.service govdata-pool-historical.timer; do
  systemctl --user disable --now "$u" 2>/dev/null || true
  rm -f "$UNIT_DIR/$u"
done

systemctl --user daemon-reload
systemctl --user enable --now govdata-pool.service

echo "Installed and enabled: govdata-pool.service"
echo ""
echo "  Status:  systemctl --user status govdata-pool.service"
echo "  Logs:    tail -f ${LOG_DIR}/pool-service.log"
echo "  Errors:  tail -f ${ERROR_LOG}"
echo "  Stop:    systemctl --user stop govdata-pool.service"
echo "  Uninstall: $0 --uninstall"
