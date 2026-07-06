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
PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

# Verify shadow JAR exists
source "$SCRIPT_DIR/common.sh"
load_env
resolve_classpath > /dev/null

echo "=== Launching 20 parallel ETL workers ==="
echo ""

for i in $(seq -w 1 20); do
  worker_script="$SCRIPT_DIR/worker-${i}.sh"
  log_file="$SCRIPT_DIR/runs/worker-${i}/launch.log"
  mkdir -p "$(dirname "$log_file")"

  echo "Starting worker-${i}..."
  nohup bash "$worker_script" > "$log_file" 2>&1 &
  pid=$!
  echo "$pid" > "$PID_DIR/worker-${i}.pid"
  echo "  PID: $pid  Log: $log_file"
done

echo ""
echo "=== All workers launched ==="
echo "PIDs written to: $PID_DIR/"
echo ""
echo "Monitor progress:"
echo "  tail -f $SCRIPT_DIR/runs/worker-*/launch.log"
echo "  bash $SCRIPT_DIR/status.sh"
echo ""

# Show summary
echo "Worker status:"
for i in $(seq -w 1 20); do
  pid_file="$PID_DIR/worker-${i}.pid"
  if [ -f "$pid_file" ]; then
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
      echo "  worker-${i}: running (PID $pid)"
    else
      echo "  worker-${i}: exited (PID $pid)"
    fi
  fi
done
