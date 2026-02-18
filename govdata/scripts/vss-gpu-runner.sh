#!/bin/bash
#
# VSS GPU Runner - Automated GPU VM lifecycle for embedding generation
#
# Creates a Vultr GPU VM, runs embedding pipeline, destroys VM.
#
# Required environment variables:
#   VULTR_API_KEY          - Vultr API key
#   VULTR_SSH_KEY_ID       - Vultr SSH key UUID (get via: ./vss-gpu-runner.sh list-ssh-keys)
#   VULTR_NVIDIA_SCRIPT_ID - Vultr startup script for NVIDIA driver installation
#   AWS_ACCESS_KEY_ID      - S3/R2 access
#   AWS_SECRET_ACCESS_KEY  - S3/R2 secret
#   AWS_ENDPOINT_OVERRIDE  - R2 endpoint
#
# Optional:
#   VSS_GPU_PLAN           - Vultr GPU plan (default: vcg-a100-1c-6g-4vram)
#   VSS_GPU_REGION         - Vultr region (default: ewr)
#   VSS_GPU_OS             - OS ID (default: 2136 = Ubuntu 22.04)
#   VSS_YEARS              - Years to process (default: all)
#   VSS_DRY_RUN            - Set to 1 for dry run
#
# Usage:
#   source .env.prod
#   export VULTR_API_KEY="your-key"
#   ./vss-gpu-runner.sh
#
set -e

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
        # Retry destroy a few times in case of transient failures
        for i in 1 2 3; do
            if destroy_instance "$INSTANCE_ID" 2>/dev/null; then
                log "Instance destroyed successfully"
                break
            fi
            warn "Destroy attempt $i failed, retrying..."
            sleep 2
        done
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
    response=$(vultr_api POST "/instances" "{
        \"plan\": \"$GPU_PLAN\",
        \"region\": \"$GPU_REGION\",
        \"os_id\": $GPU_OS,
        \"label\": \"$GPU_LABEL\",
        \"hostname\": \"vss-gpu\",
        \"sshkey_id\": [\"$VULTR_SSH_KEY_ID\"],
        \"script_id\": \"$VULTR_NVIDIA_SCRIPT_ID\",
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
    local max_wait="${2:-300}"  # 5 minutes default
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
    local max_wait="${2:-120}"  # 2 minutes - startup scripts don't work on Vultr GPU VMs
    local interval=15
    local elapsed=0

    log "Checking for NVIDIA driver (startup script)..."

    while [[ $elapsed -lt $max_wait ]]; do
        # Check if nvidia-smi works (with timeout to prevent hang)
        if timeout 30 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "nvidia-smi > /dev/null 2>&1" 2>/dev/null; then
            log "NVIDIA driver is ready!"
            run_remote_short "$ip" "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader"
            return 0
        fi

        # Check startup script progress (with timeout)
        local status
        status=$(timeout 30 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "
            if [ -f /var/lib/vultr/states/.nvidia-ready ]; then
                echo 'ready'
            elif [ -f /var/log/nvidia-setup.log ]; then
                tail -1 /var/log/nvidia-setup.log 2>/dev/null || echo 'installing'
            else
                echo 'pending'
            fi
        " 2>/dev/null || echo "timeout")

        log "  NVIDIA status: $status"

        sleep $interval
        elapsed=$((elapsed + interval))
    done

    warn "NVIDIA driver installation timed out, will try manual install"
    return 1
}

destroy_instance() {
    local instance_id="$1"
    log "Destroying instance $instance_id..."
    vultr_api DELETE "/instances/$instance_id"
    log "Instance destroyed"
}

#------------------------------------------------------------------------------
# Remote Execution
#------------------------------------------------------------------------------

run_remote() {
    local ip="$1"
    shift
    # Use timeout to prevent hanging (30 min max for long operations like pip install)
    timeout 1800 ssh $SSH_OPTS -i "$SSH_KEY_PATH" "root@$ip" "$@"
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
pip install -q sentence-transformers duckdb pyarrow

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
    local years="$2"
    local dry_run="$3"

    log "Running embedding pipeline..."

    local args="--upload"
    if [[ "$years" == "all" ]]; then
        args="--all $args"
    else
        args="--years $years $args"
    fi

    if [[ "$dry_run" == "1" ]]; then
        args="$args --dry-run"
    fi

    run_remote "$ip" << REMOTE_SCRIPT
set -e
source /root/venv/bin/activate
source /root/.env
cd /root
python3 vss-bulk-gpu.py $args
REMOTE_SCRIPT
}

#------------------------------------------------------------------------------
# Pre-flight Check - Count pending chunks before GPU provisioning
#------------------------------------------------------------------------------

check_pending_chunks() {
    log "Pre-flight check: Checking for chunks that need embeddings..."

    # Get S3 endpoint without https://
    local s3_endpoint="${AWS_ENDPOINT_OVERRIDE#https://}"
    local iceberg_chunks="s3://govdata-parquet-v1/source=sec/SEC/vectorized_chunks"

    # Use DuckDB to check if ANY chunks need embeddings (fast LIMIT 1 check)
    local has_pending
    has_pending=$(duckdb -noheader -csv << EOF 2>/dev/null || echo "0"
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-east-1';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint = '${s3_endpoint}';
SET s3_use_ssl = true;
SET s3_url_style = 'path';
SET unsafe_enable_version_guessing = true;

-- Check if ANY chunks need embeddings (NULL or placeholder)
SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as has_pending
FROM (
    SELECT 1
    FROM iceberg_scan('${iceberg_chunks}')
    WHERE embedding IS NULL
       OR (array_length(embedding) = 1 AND embedding[1] = 0.0)
    LIMIT 1
);
EOF
)

    # Strip whitespace and validate
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

    # Check required environment variables
    [[ -z "$VULTR_API_KEY" ]] && error "VULTR_API_KEY not set"
    [[ -z "$VULTR_SSH_KEY_ID" ]] && error "VULTR_SSH_KEY_ID not set. Run: ./vss-gpu-runner.sh list-ssh-keys"
    [[ -z "$VULTR_NVIDIA_SCRIPT_ID" ]] && warn "VULTR_NVIDIA_SCRIPT_ID not set. NVIDIA driver will be installed manually (slower)."
    [[ -z "$AWS_ACCESS_KEY_ID" ]] && error "AWS_ACCESS_KEY_ID not set"
    [[ -z "$AWS_SECRET_ACCESS_KEY" ]] && error "AWS_SECRET_ACCESS_KEY not set"
    [[ -z "$AWS_ENDPOINT_OVERRIDE" ]] && error "AWS_ENDPOINT_OVERRIDE not set"

    # Check SSH key
    [[ ! -f "$SSH_KEY_PATH" ]] && error "SSH key not found: $SSH_KEY_PATH"

    log "Configuration:"
    log "  GPU Plan: $GPU_PLAN"
    log "  Region: $GPU_REGION"
    log "  Years: $YEARS"
    log "  Dry run: $DRY_RUN"
    echo ""

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

    # Check for existing GPU instance (prevent concurrent runs)
    # Also cleanup stale instances (running > 2 hours)
    existing_json=$(vultr_api GET "/instances" | jq -r '.instances[]? | select(.label | startswith("vss-embedding"))')
    if [[ -n "$existing_json" ]]; then
        existing_instance=$(echo "$existing_json" | jq -r '.id')
        created_time=$(echo "$existing_json" | jq -r '.date_created')

        if [[ -n "$existing_instance" && "$existing_instance" != "null" ]]; then
            # Check if instance is stale (> 2 hours old)
            created_epoch=$(date -d "$created_time" +%s 2>/dev/null || echo 0)
            now_epoch=$(date +%s)
            age_hours=$(( (now_epoch - created_epoch) / 3600 ))

            if [[ $age_hours -ge 2 ]]; then
                warn "Found stale GPU instance $existing_instance (${age_hours}h old) - destroying"
                destroy_instance "$existing_instance" || true
                sleep 5  # Wait for cleanup
            else
                log "GPU instance already running: $existing_instance (${age_hours}h old)"
                log "Another GPU job is in progress - skipping to avoid duplicate billing"
                echo ""
                echo "=============================================="
                echo "VSS GPU Runner Skipped (instance already running)"
                echo "=============================================="
                exit 0
            fi
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
    run_embedding_pipeline "$INSTANCE_IP" "$YEARS" "$DRY_RUN"

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
