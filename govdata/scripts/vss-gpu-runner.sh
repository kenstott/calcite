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
set -e

# Ignore SIGPIPE - prevents script death when parent Java process drops stdout pipe
trap '' PIPE

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# Vultr configuration
VULTR_API="https://api.vultr.com/v2"
GPU_PLAN="${VSS_GPU_PLAN:-vcg-a100-1c-6g-4vram}"  # Fractional A100 (~$0.12/hr)
GPU_REGION="${VSS_GPU_REGION:-ewr}"                 # New Jersey
GPU_OS="${VSS_GPU_OS:-2136}"                        # Ubuntu 22.04 LTS
GPU_LABEL="vss-embedding-$(date +%Y%m%d-%H%M%S)"

# Processing options
YEARS="${VSS_YEARS:-all}"
DRY_RUN="${VSS_DRY_RUN:-0}"

# SSH settings
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/id_rsa}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1" >&2; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1" >&2; }
error() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1" >&2; exit 1; }

# Cleanup function - ensures instance is destroyed on any exit
cleanup() {
    local exit_code=$?
    if [[ -n "$INSTANCE_ID" ]]; then
        warn "Cleaning up - destroying instance $INSTANCE_ID (exit code: $exit_code)"
        # Direct API call to destroy - don't rely on destroy_instance which may fail
        for i in 1 2 3; do
            curl -s -X DELETE "${VULTR_API}/instances/${INSTANCE_ID}" \
                -H "Authorization: Bearer ${VULTR_API_KEY}" \
                -H "Content-Type: application/json" 2>/dev/null || true
            sleep 3
            # Verify it's gone
            local status
            status=$(curl -s "${VULTR_API}/instances/${INSTANCE_ID}" \
                -H "Authorization: Bearer ${VULTR_API_KEY}" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('instance',{}).get('status',''))" 2>/dev/null) || status=""
            if [[ -z "$status" || "$status" == "null" ]]; then
                log "Instance destroyed successfully on cleanup"
                return 0
            fi
            warn "Destroy attempt $i - instance still present (status: $status), retrying..."
        done
        warn "Failed to destroy instance $INSTANCE_ID after 3 attempts!"
    fi
}

# Trap all exit scenarios
trap cleanup EXIT
trap 'exit 130' INT   # Ctrl+C
trap 'exit 143' TERM  # kill

#------------------------------------------------------------------------------
# Vultr API Functions
#------------------------------------------------------------------------------

vultr_api() {
    local method="$1"
    local endpoint="$2"
    local data="$3"

    local curl_args=(-s -X "$method" "${VULTR_API}${endpoint}")
    curl_args+=(-H "Authorization: Bearer ${VULTR_API_KEY}")
    curl_args+=(-H "Content-Type: application/json")

    if [[ -n "$data" ]]; then
        curl_args+=(-d "$data")
    fi

    curl "${curl_args[@]}"
}

list_gpu_plans() {
    log "Available GPU plans:"
    vultr_api GET "/plans?type=vcg" | jq -r '.plans[] | "\(.id) - \(.vcpu_count) vCPU, \(.ram)MB RAM, $\(.monthly_cost)/mo"'
}

list_regions() {
    log "GPU-available regions:"
    vultr_api GET "/regions" | jq -r '.regions[] | select(.options | contains(["ddos_protection"])) | "\(.id) - \(.city), \(.country)"' | head -20
}

list_ssh_keys() {
    log "SSH keys registered in Vultr:"
    vultr_api GET "/ssh-keys" | jq -r '.ssh_keys[] | "\(.id) - \(.name)"'
    echo ""
    echo "Add VULTR_SSH_KEY_ID=<uuid> to your .env.prod file"
}

create_instance() {
    log "Creating GPU instance..."
    log "  Plan: $GPU_PLAN"
    log "  Region: $GPU_REGION"
    log "  SSH Key: $VULTR_SSH_KEY_ID"
    log "  Label: $GPU_LABEL"

    local response
    # Note: script_id omitted - Vultr cloud-init vendor scripts don't execute
    # reliably on GPU instances (permission issue). We install NVIDIA manually.
    response=$(vultr_api POST "/instances" "{
        \"plan\": \"$GPU_PLAN\",
        \"region\": \"$GPU_REGION\",
        \"os_id\": $GPU_OS,
        \"label\": \"$GPU_LABEL\",
        \"hostname\": \"vss-gpu\",
        \"sshkey_id\": [\"$VULTR_SSH_KEY_ID\"],
        \"backups\": \"disabled\",
        \"ddos_protection\": false,
        \"activation_email\": false
    }")

    local instance_id
    instance_id=$(echo "$response" | jq -r '.instance.id // empty')

    if [[ -z "$instance_id" || "$instance_id" == "null" ]]; then
        echo "$response" | jq . >&2
        return 1
    fi

    echo "$instance_id"
}

get_instance() {
    local instance_id="$1"
    vultr_api GET "/instances/$instance_id"
}

get_instance_ip() {
    local instance_id="$1"
    get_instance "$instance_id" | jq -r '.instance.main_ip // empty'
}

get_instance_status() {
    local instance_id="$1"
    get_instance "$instance_id" | jq -r '.instance.status // empty'
}

get_instance_power() {
    local instance_id="$1"
    get_instance "$instance_id" | jq -r '.instance.power_status // empty'
}

wait_for_instance() {
    local instance_id="$1"
    local max_wait="${2:-900}"  # 15 minutes default (GPU VMs take longer)
    local interval=10
    local elapsed=0

    log "Waiting for instance $instance_id to be ready..."

    while [[ $elapsed -lt $max_wait ]]; do
        local status power ip
        status=$(get_instance_status "$instance_id")
        power=$(get_instance_power "$instance_id")
        ip=$(get_instance_ip "$instance_id")

        log "  Status: $status, Power: $power, IP: $ip"

        if [[ "$status" == "active" && "$power" == "running" && -n "$ip" && "$ip" != "0.0.0.0" ]]; then
            log "Instance is ready!"
            echo "$ip"
            return 0
        fi

        sleep $interval
        elapsed=$((elapsed + interval))
    done

    error "Timeout waiting for instance"
}

wait_for_ssh() {
    local ip="$1"
    local max_wait="${2:-1200}"  # 20 minutes default (GPU VMs take 10-15 min)
    local interval=10
    local elapsed=0

    log "Waiting for SSH on $ip..."

    while [[ $elapsed -lt $max_wait ]]; do
        if ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "echo ok" 2>/dev/null; then
            log "SSH is ready!"
            return 0
        fi

        sleep $interval
        elapsed=$((elapsed + interval))
    done

    error "Timeout waiting for SSH"
}

wait_for_nvidia() {
    local ip="$1"
    local max_wait="${2:-120}"  # 2 minutes - quick check if startup script worked
    local interval=15
    local elapsed=0

    log "Checking for NVIDIA driver..."

    while [[ $elapsed -lt $max_wait ]]; do
        local nvidia_ok=0
        if timeout 60 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "nvidia-smi > /dev/null 2>&1" 2>/dev/null; then
            nvidia_ok=1
        fi

        if [[ $nvidia_ok -eq 1 ]]; then
            log "NVIDIA driver is ready!"
            run_remote_short "$ip" "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader" || true
            return 0
        fi

        log "  NVIDIA status: not ready (elapsed: ${elapsed}s/${max_wait}s)"
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    warn "NVIDIA driver not found, will do manual install"
    return 1
}

destroy_instance() {
    local instance_id="$1"
    log "Destroying instance $instance_id..."
    vultr_api DELETE "/instances/$instance_id"

    # Verify the instance was actually destroyed (wait up to 60s)
    local elapsed=0
    while [[ $elapsed -lt 60 ]]; do
        sleep 5
        elapsed=$((elapsed + 5))
        local status
        status=$(get_instance_status "$instance_id" 2>/dev/null) || status=""
        if [[ -z "$status" || "$status" == "null" ]]; then
            log "Instance $instance_id destroyed successfully (verified)"
            return 0
        fi
        log "  Waiting for instance destruction... status=$status (${elapsed}s)"
    done

    warn "Instance $instance_id may not be fully destroyed (status: $status)"
    # Try once more
    vultr_api DELETE "/instances/$instance_id" 2>/dev/null || true
    return 0
}

#------------------------------------------------------------------------------
# Remote Execution
#------------------------------------------------------------------------------

run_remote() {
    local ip="$1"
    shift
    # Use timeout to prevent hanging (2 hour max for embedding pipeline)
    timeout 7200 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "$@"
}

run_remote_short() {
    local ip="$1"
    shift
    # Short timeout for quick commands (2 min)
    timeout 120 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "$@"
}

copy_to_remote() {
    local ip="$1"
    local src="$2"
    local dst="$3"
    timeout 300 scp $SSH_OPTS -i "$SSH_KEY_PATH" "$src" "root@$ip:$dst"
}

setup_gpu_instance() {
    local ip="$1"

    log "Setting up GPU instance..."

    # Wait for NVIDIA driver (installed by startup script)
    if ! wait_for_nvidia "$ip"; then
        # Fallback: manual installation if startup script failed
        log "Falling back to manual NVIDIA driver installation..."
        run_remote "$ip" << 'NVIDIA_INSTALL'
set -e
export DEBIAN_FRONTEND=noninteractive

echo "Killing apt/dpkg processes and waiting for locks..."
systemctl stop apt-daily.timer apt-daily-upgrade.timer unattended-upgrades 2>/dev/null || true
systemctl disable apt-daily.timer apt-daily-upgrade.timer unattended-upgrades 2>/dev/null || true
pkill -9 -f unattended-upgr 2>/dev/null || true
pkill -9 -f apt.systemd 2>/dev/null || true
sleep 2

# Wait and retry apt-get with exponential backoff
apt_install() {
    local max_attempts=10
    for i in $(seq 1 $max_attempts); do
        # Kill any holding processes
        pkill -9 -f unattended-upgr 2>/dev/null || true
        rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/cache/apt/archives/lock 2>/dev/null || true
        dpkg --configure -a 2>/dev/null || true

        if apt-get update -qq && apt-get install -y -qq "$@"; then
            echo "apt-get succeeded"
            return 0
        fi
        echo "apt-get attempt $i failed, retrying in ${i}s..."
        sleep $i
    done
    echo "apt-get failed after $max_attempts attempts"
    return 1
}

apt_install build-essential linux-headers-$(uname -r) wget unzip dkms
wget -q -O /tmp/vultr_nvidia_driver.zip "http://169.254.169.254/latest/nvidia_linux_driver_installer_url"
unzip -qq -o /tmp/vultr_nvidia_driver.zip -d /
rm -f /tmp/vultr_nvidia_driver.zip
bash /opt/nvidia/install.sh
NVIDIA_INSTALL
    fi

    # Install Python environment and packages
    run_remote "$ip" << 'REMOTE_SCRIPT'
set -e
export DEBIAN_FRONTEND=noninteractive

echo "Verifying NVIDIA driver..."
nvidia-smi

echo "Killing apt/dpkg processes and waiting for locks..."
systemctl stop apt-daily.timer apt-daily-upgrade.timer unattended-upgrades 2>/dev/null || true
systemctl disable apt-daily.timer apt-daily-upgrade.timer unattended-upgrades 2>/dev/null || true
pkill -9 -f unattended-upgr 2>/dev/null || true
pkill -9 -f apt.systemd 2>/dev/null || true
sleep 2

# Wait and retry apt-get with exponential backoff
apt_install() {
    local max_attempts=10
    for i in $(seq 1 $max_attempts); do
        pkill -9 -f unattended-upgr 2>/dev/null || true
        rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/cache/apt/archives/lock 2>/dev/null || true
        dpkg --configure -a 2>/dev/null || true

        if apt-get update -qq && apt-get install -y -qq "$@"; then
            echo "apt-get succeeded"
            return 0
        fi
        echo "apt-get attempt $i failed, retrying in ${i}s..."
        sleep $i
    done
    echo "apt-get failed after $max_attempts attempts"
    return 1
}

echo "Installing system packages..."
apt_install python3-venv python3-pip awscli

echo "Creating Python virtual environment..."
python3 -m venv /root/venv
source /root/venv/bin/activate

echo "Installing Python packages..."
pip install -q --upgrade pip
pip install -q torch --index-url https://download.pytorch.org/whl/cu124
pip install -q sentence-transformers "duckdb>=1.3,<1.4" pyarrow "pyiceberg[sql-sqlite,s3fs]"

echo "Verifying GPU in PyTorch..."
python3 -c "import torch; print(f'GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"NOT FOUND\"}')"

echo "Setup complete!"
REMOTE_SCRIPT

    # Copy script and credentials
    log "Copying files to remote..."
    copy_to_remote "$ip" "$SCRIPT_DIR/vss-bulk-gpu.py" "/root/vss-bulk-gpu.py"

    # Create .env file on remote
    run_remote "$ip" "cat > /root/.env" << ENVFILE
export AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}'
export AWS_ENDPOINT_OVERRIDE='${AWS_ENDPOINT_OVERRIDE}'
ENVFILE
}

run_embedding_pipeline() {
    local ip="$1"
    local dry_run="$2"

    log "Running embedding pipeline..."

    local args="--all"
    if [[ "$dry_run" == "1" ]]; then
        args="$args --dry-run"
    fi

    run_remote "$ip" << REMOTE_SCRIPT
set -e
source /root/venv/bin/activate
source /root/.env
cd /root
export PYTHONUNBUFFERED=1
python3 -u vss-bulk-gpu.py $args
REMOTE_SCRIPT
}

run_vss_rebuild() {
    local ip="$1"

    log "Rebuilding VSS DuckDB cache with HNSW index..."

    run_remote "$ip" << 'REMOTE_SCRIPT'
set -e
source /root/venv/bin/activate
source /root/.env
cd /root
export PYTHONUNBUFFERED=1
python3 -u vss-bulk-gpu.py --rebuild-vss-only --upload
REMOTE_SCRIPT
}

#------------------------------------------------------------------------------
# Pre-flight Check - Count pending chunks before GPU provisioning
#------------------------------------------------------------------------------

check_pending_chunks() {
    log "Pre-flight check: Checking for chunks that need embeddings..."

    local s3_endpoint="${AWS_ENDPOINT_OVERRIDE#https://}"
    local iceberg_chunks="s3://govdata-parquet-v1/sec/vectorized_chunks"

    # Use python+duckdb to check pending chunks. The system duckdb CLI (1.4.x)
    # has a filter pushdown bug on list columns in iceberg_scan that silently
    # returns 0 rows. Python duckdb 1.3.x works correctly.
    local has_pending
    has_pending=$(python3 -c "
import duckdb, os, sys
try:
    conn = duckdb.connect()
    conn.execute('INSTALL iceberg; LOAD iceberg')
    conn.execute('INSTALL httpfs; LOAD httpfs')
    conn.execute(\"SET s3_region = 'us-east-1'\")
    conn.execute(\"SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}'\")
    conn.execute(\"SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}'\")
    conn.execute(\"SET s3_endpoint = '${s3_endpoint}'\")
    conn.execute('SET s3_use_ssl = true')
    conn.execute(\"SET s3_url_style = 'path'\")
    conn.execute('SET unsafe_enable_version_guessing = true')
    # Materialize to temp table to avoid filter pushdown bug on list columns
    conn.execute(\"CREATE TEMP TABLE _t AS SELECT embedding FROM iceberg_scan('${iceberg_chunks}') LIMIT 10000\")
    r = conn.execute('SELECT COUNT(*) FROM _t WHERE embedding IS NULL OR (array_length(embedding) = 1 AND embedding[1] = 0.0)').fetchone()[0]
    print(1 if r > 0 else 0)
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    print(0)
" 2>/dev/null || echo "0")

    has_pending=$(echo "$has_pending" | tr -d '[:space:]' | head -1)

    if [[ "$has_pending" == "1" ]]; then
        log "  Found chunks needing embeddings"
        echo "1"
    else
        log "  No chunks need embeddings"
        echo "0"
    fi
}

#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------

main() {
    echo "=============================================="
    echo "VSS GPU Runner"
    echo "=============================================="
    echo ""

    # Check required environment variables — missing credentials mean this machine is not
    # configured as a GPU runner; skip gracefully so pool exit code is not poisoned.
    _skip_missing() { warn "Skipping embeddings: $1"; exit 0; }
    [[ -z "$VULTR_API_KEY" ]] && _skip_missing "VULTR_API_KEY not set"
    [[ -z "$VULTR_SSH_KEY_ID" ]] && _skip_missing "VULTR_SSH_KEY_ID not set (run: ./vss-gpu-runner.sh list-ssh-keys)"
    [[ -z "$VULTR_NVIDIA_SCRIPT_ID" ]] && warn "VULTR_NVIDIA_SCRIPT_ID not set. NVIDIA driver will be installed manually (slower)."
    [[ -z "$AWS_ACCESS_KEY_ID" ]] && _skip_missing "AWS_ACCESS_KEY_ID not set"
    [[ -z "$AWS_SECRET_ACCESS_KEY" ]] && _skip_missing "AWS_SECRET_ACCESS_KEY not set"
    [[ -z "$AWS_ENDPOINT_OVERRIDE" ]] && _skip_missing "AWS_ENDPOINT_OVERRIDE not set"

    # Check SSH key
    [[ ! -f "$SSH_KEY_PATH" ]] && _skip_missing "SSH key not found: $SSH_KEY_PATH"

    log "Configuration:"
    log "  GPU Plan: $GPU_PLAN"
    log "  Region: $GPU_REGION"
    log "  Years: $YEARS"
    log "  Dry run: $DRY_RUN"
    echo ""

    # Redirect output to log file to survive parent JVM exit
    # (each ETL job runs a separate JVM; when it exits, the pipe closes
    # and set -e kills us on the next write)
    GPU_LOG="${GOVDATA_HOME}/runs/gpu-runner-$(date +%Y%m%d-%H%M%S).log"
    log "GPU runner output redirecting to: $GPU_LOG"
    exec >> "$GPU_LOG" 2>&1
    log "GPU runner started (PID: $$)"

    # Lock file to prevent concurrent GPU provisioning from multiple ETL jobs
    LOCK_FILE="${GOVDATA_HOME}/runs/.gpu-runner.lock"
    if [[ -f "$LOCK_FILE" ]]; then
        lock_pid=$(cat "$LOCK_FILE" 2>/dev/null)
        lock_age=0
        if [[ -f "$LOCK_FILE" ]]; then
            lock_age=$(( $(date +%s) - $(stat -c %Y "$LOCK_FILE" 2>/dev/null || echo 0) ))
        fi
        # If lock holder is still alive AND lock is < 30 min old, skip
        if kill -0 "$lock_pid" 2>/dev/null && [[ $lock_age -lt 1800 ]]; then
            log "Another GPU runner is active (PID $lock_pid, ${lock_age}s old) - skipping"
            echo ""
            echo "=============================================="
            echo "VSS GPU Runner Skipped (lock held by PID $lock_pid)"
            echo "=============================================="
            exit 0
        else
            warn "Stale lock file (PID $lock_pid dead or lock ${lock_age}s old) - removing"
            rm -f "$LOCK_FILE"
        fi
    fi
    echo $$ > "$LOCK_FILE"
    # Ensure lock is cleaned up on exit
    cleanup_lock() { rm -f "$LOCK_FILE"; }
    # Chain with existing cleanup trap
    original_cleanup=$(trap -p EXIT | sed "s/trap -- '//;s/' EXIT//")
    trap "cleanup_lock; $original_cleanup" EXIT

    # Pre-flight check: count pending chunks before creating GPU
    pending=$(check_pending_chunks)
    if [[ "$pending" -le 0 ]]; then
        log "No pending chunks to process - skipping GPU provisioning"
        echo ""
        echo "=============================================="
        echo "VSS GPU Runner Complete (no work needed)"
        echo "=============================================="
        exit 0
    fi

    log "Found $pending pending chunks - proceeding with GPU provisioning"
    echo ""

    # Check for existing GPU instance - reuse active ones, destroy truly orphaned
    existing_json=$(vultr_api GET "/instances" | jq -r '.instances[]? | select(.label | startswith("vss-embedding"))')
    if [[ -n "$existing_json" ]]; then
        existing_instance=$(echo "$existing_json" | jq -r '.id')
        created_time=$(echo "$existing_json" | jq -r '.date_created')

        if [[ -n "$existing_instance" && "$existing_instance" != "null" ]]; then
            created_epoch=$(date -d "$created_time" +%s 2>/dev/null || echo 0)
            now_epoch=$(date +%s)
            age_minutes=$(( (now_epoch - created_epoch) / 60 ))

            # Check if the instance is actively running a job or still setting up
            local ip
            ip=$(get_instance_ip "$existing_instance" 2>/dev/null) || ip=""
            local is_active=0
            if [[ -n "$ip" && "$ip" != "0.0.0.0" ]]; then
                # Check for embedding pipeline OR setup processes (apt, pip, nvidia, dpkg)
                if ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" \
                    "pgrep -f 'vss-bulk-gpu|apt-get|dpkg|pip|nvidia|install.sh'" 2>/dev/null; then
                    is_active=1
                fi
            fi

            # Instance is still setting up or running — skip
            if [[ $is_active -eq 1 ]]; then
                log "GPU instance $existing_instance (${age_minutes}m old) is active (running job or setup)"
                log "Skipping to let it finish"
                echo ""
                echo "=============================================="
                echo "VSS GPU Runner Skipped (instance active)"
                echo "=============================================="
                exit 0
            fi

            # Instance is young (< 25 min) but SSH isn't ready — still provisioning
            if [[ $age_minutes -lt 25 ]] && [[ -z "$ip" || "$ip" == "0.0.0.0" ]]; then
                log "GPU instance $existing_instance (${age_minutes}m old) is still provisioning (no IP)"
                log "Skipping to let it finish"
                echo ""
                echo "=============================================="
                echo "VSS GPU Runner Skipped (instance provisioning)"
                echo "=============================================="
                exit 0
            fi

            # Instance is young (< 25 min) but SSH failed — could be booting
            if [[ $age_minutes -lt 25 ]] && [[ $is_active -eq 0 ]] && [[ -n "$ip" && "$ip" != "0.0.0.0" ]]; then
                # SSH might not be ready yet — check if we can connect at all
                if ! ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "echo ok" 2>/dev/null; then
                    log "GPU instance $existing_instance (${age_minutes}m old) SSH not ready yet"
                    log "Skipping to let it boot"
                    echo ""
                    echo "=============================================="
                    echo "VSS GPU Runner Skipped (instance booting)"
                    echo "=============================================="
                    exit 0
                fi
            fi

            # If we get here: instance is old (>= 25 min) with no active processes, or
            # young with SSH working but no processes — truly orphaned
            warn "Found orphaned GPU instance $existing_instance (${age_minutes}m old, no active processes) - destroying"
            destroy_instance "$existing_instance" || true
            sleep 5
        fi
    fi

    # Create instance
    INSTANCE_ID=$(create_instance) || {
        error "Failed to create instance"
    }

    if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "null" ]]; then
        error "Failed to create instance - no instance ID returned"
    fi

    log "Instance created: $INSTANCE_ID"

    # Wait for instance to be ready
    INSTANCE_IP=$(wait_for_instance "$INSTANCE_ID")
    log "Instance IP: $INSTANCE_IP"

    # Wait for SSH
    wait_for_ssh "$INSTANCE_IP"

    # Setup instance
    setup_gpu_instance "$INSTANCE_IP"

    # Run embedding pipeline
    run_embedding_pipeline "$INSTANCE_IP" "$DRY_RUN"

    # Rebuild VSS DuckDB cache (HNSW index) from all embedded chunks
    if [[ "$DRY_RUN" != "1" ]]; then
        run_vss_rebuild "$INSTANCE_IP"
    fi

    # Destroy instance (cleanup trap will handle this)
    log "Pipeline complete, destroying instance..."
    destroy_instance "$INSTANCE_ID"
    INSTANCE_ID=""  # Clear so cleanup doesn't try again

    echo ""
    echo "=============================================="
    echo "VSS GPU Runner Complete"
    echo "=============================================="
}

# Handle commands
case "${1:-}" in
    list-plans)
        [[ -z "$VULTR_API_KEY" ]] && error "VULTR_API_KEY not set"
        list_gpu_plans
        ;;
    list-regions)
        [[ -z "$VULTR_API_KEY" ]] && error "VULTR_API_KEY not set"
        list_regions
        ;;
    list-ssh-keys)
        [[ -z "$VULTR_API_KEY" ]] && error "VULTR_API_KEY not set"
        list_ssh_keys
        ;;
    *)
        main
        ;;
esac
