#!/bin/bash
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
# Stateless GPU embedding via ephemeral Vultr instance — file handoff only.
#
# Supersedes vss-gpu-runner.sh + vss-bulk-gpu.py, which made the GPU box reach
# object storage directly (and therefore "could not reach the local MinIO").
# Here the GPU box is pure compute: it receives a parquet, runs embed.py — the
# SAME embedder as ETL and the airgapped query worker — and returns a parquet.
# All storage I/O and the Iceberg merge happen on the caller side (Java).
#
# Usage:
#   vss-embed-gpu.sh <input.parquet> <id_col> <text_col> <output.parquet>
#
# Flow: provision GPU -> init (NVIDIA + venv + fastembed-gpu + embed.py)
#       -> scp input up -> embed.py embed-parquet -> scp output down -> destroy.
#
# Exit 0 with the output parquet written; non-zero on failure (instance is always
# destroyed via the cleanup trap). No object-storage credentials are used.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

IN_PARQUET="${1:?usage: vss-embed-gpu.sh <input.parquet> <id_col> <text_col> <output.parquet>}"
ID_COL="${2:?missing id_col}"
TEXT_COL="${3:?missing text_col}"
OUT_PARQUET="${4:?missing output.parquet}"

[[ -f "$IN_PARQUET" ]] || { echo "ERROR: input parquet not found: $IN_PARQUET" >&2; exit 1; }

# --- Vultr / SSH configuration (same knobs as the legacy runner) ---
VULTR_API="https://api.vultr.com/v2"
GPU_PLAN="${VSS_GPU_PLAN:-vcg-a100-1c-6g-4vram}"
GPU_REGION="${VSS_GPU_REGION:-ewr}"
GPU_OS="${VSS_GPU_OS:-2136}"
GPU_LABEL="vss-embed-$(date +%Y%m%d-%H%M%S)"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/id_rsa}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

log()  { echo "[$(date '+%H:%M:%S')] $1" >&2; }
die()  { echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2; exit 1; }

: "${VULTR_API_KEY:?VULTR_API_KEY not set}"
: "${VULTR_SSH_KEY_ID:?VULTR_SSH_KEY_ID not set (run legacy runner list-ssh-keys to find it)}"
[[ -f "$SSH_KEY_PATH" ]] || die "SSH key not found: $SSH_KEY_PATH"

INSTANCE_ID=""
cleanup() {
  local code=$?
  if [[ -n "$INSTANCE_ID" ]]; then
    log "Destroying instance $INSTANCE_ID (exit $code)"
    for _ in 1 2 3; do
      curl -s -X DELETE "${VULTR_API}/instances/${INSTANCE_ID}" \
        -H "Authorization: Bearer ${VULTR_API_KEY}" >/dev/null 2>&1 || true
      sleep 3
      local status
      status=$(curl -s "${VULTR_API}/instances/${INSTANCE_ID}" \
        -H "Authorization: Bearer ${VULTR_API_KEY}" 2>/dev/null \
        | python3 -c "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))" 2>/dev/null) || status=""
      [[ -z "$status" || "$status" == "null" ]] && { log "Instance destroyed"; break; }
    done
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

vultr() { # method endpoint [data]
  local m="$1" ep="$2" data="${3:-}"
  local args=(-s -X "$m" "${VULTR_API}${ep}" -H "Authorization: Bearer ${VULTR_API_KEY}" -H "Content-Type: application/json")
  [[ -n "$data" ]] && args+=(-d "$data")
  curl "${args[@]}"
}

remote()      { timeout 7200 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$1" "${@:2}"; }
remote_short(){ timeout 120  ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$1" "${@:2}"; }

provision() {
  log "Creating GPU instance ($GPU_PLAN / $GPU_REGION)..."
  local resp id
  resp=$(vultr POST "/instances" "{\"plan\":\"$GPU_PLAN\",\"region\":\"$GPU_REGION\",\"os_id\":$GPU_OS,\"label\":\"$GPU_LABEL\",\"hostname\":\"vss-gpu\",\"sshkey_id\":[\"$VULTR_SSH_KEY_ID\"],\"backups\":\"disabled\",\"activation_email\":false}")
  id=$(echo "$resp" | python3 -c "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('id',''))" 2>/dev/null) || id=""
  [[ -n "$id" && "$id" != "null" ]] || { echo "$resp" >&2; die "instance create failed"; }
  INSTANCE_ID="$id"
  log "Instance $INSTANCE_ID created; waiting for IP + SSH..."
  local ip="" elapsed=0
  while (( elapsed < 900 )); do
    ip=$(vultr GET "/instances/$INSTANCE_ID" | python3 -c "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('main_ip',''))" 2>/dev/null) || ip=""
    [[ -n "$ip" && "$ip" != "0.0.0.0" ]] && remote_short "$ip" "echo ok" >/dev/null 2>&1 && { echo "$ip"; return 0; }
    sleep 10; (( elapsed += 10 ))
  done
  die "timeout waiting for instance SSH"
}

init_instance() { # ip
  local ip="$1"
  log "Initializing GPU box (NVIDIA + venv + fastembed-gpu + embed.py)..."
  # NVIDIA driver (Vultr metadata installer), Python venv, fastembed-gpu (onnxruntime CUDA).
  remote "$ip" 'bash -s' <<'INIT'
set -e
export DEBIAN_FRONTEND=noninteractive
nvidia-smi >/dev/null 2>&1 || {
  wget -q -O /tmp/nv.zip "http://169.254.169.254/latest/nvidia_linux_driver_installer_url" && \
  unzip -qq -o /tmp/nv.zip -d / && bash /opt/nvidia/install.sh
}
apt-get update -qq && apt-get install -y -qq python3-venv python3-pip
python3 -m venv /root/venv
/root/venv/bin/pip install -q --upgrade pip
# fastembed-gpu pulls onnxruntime-gpu (CUDA EP, CPU fallback). Same model as embed.py.
/root/venv/bin/pip install -q fastembed-gpu pyarrow
nvidia-smi --query-gpu=name --format=csv,noheader || true
INIT
  # embed.py is the single embedder shared by ETL, this GPU job, and the query worker.
  timeout 120 scp $SSH_OPTS -i "$SSH_KEY_PATH" "$SCRIPT_DIR/embed.py" "root@$ip:/root/embed.py"
}

main() {
  local ip; ip="$(provision)"
  log "Instance IP: $ip"
  init_instance "$ip"

  log "Uploading $IN_PARQUET ..."
  timeout 600 scp $SSH_OPTS -i "$SSH_KEY_PATH" "$IN_PARQUET" "root@$ip:/root/in.parquet"

  log "Embedding on GPU via embed.py ..."
  remote "$ip" "/root/venv/bin/python3 /root/embed.py embed-parquet /root/in.parquet '$ID_COL' '$TEXT_COL' /root/out.parquet"

  log "Downloading embeddings -> $OUT_PARQUET ..."
  timeout 600 scp $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip:/root/out.parquet" "$OUT_PARQUET"
  [[ -s "$OUT_PARQUET" ]] || die "no output parquet returned"

  log "Done. Instance will be destroyed by cleanup trap."
}

main
