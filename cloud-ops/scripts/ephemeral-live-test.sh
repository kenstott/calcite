#!/usr/bin/env bash
# Creates the cheapest short-lived resources that give the cloud-ops tables something to read,
# runs the live column audit against them, and deletes them again — also when the audit fails
# or the script is interrupted.
#
#   cloud-ops/scripts/ephemeral-live-test.sh            # create, audit, delete
#   cloud-ops/scripts/ephemeral-live-test.sh teardown   # only delete (after a crashed run)
#
# What it creates (minutes of the smallest size; a few cents in total):
#   Azure  resource group calcite-cloudops-test-rg: B1s VM (with VNet, NSG, NIC, public IP,
#          disk), storage account, Basic container registry, user-assigned managed identity
#   GCP    e2-micro VM that GCP deletes by itself after 15 minutes
#          (the Artifact Registry repository calcite-cloudops-test is permanent and free)
#   AWS    t3.micro instance, security group, empty ECR repository, empty DynamoDB table —
#          only if CLOUDOPS_AWS_ADMIN_ACCESS_KEY_ID / _SECRET_ACCESS_KEY are set, because
#          the adapter's own AWS key is read-only
#
# Azure and GCP are created with the signed-in az / gcloud CLIs, not with the adapter's
# read-only credentials. Everything carries the tag/label application=calcite-cloudops-test.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT/govdata/.env.prod"
NAME=calcite-cloudops-test
AZ_GROUP="$NAME-rg"
AZ_LOCATION=centralus  # a region where the B1s size is offered to this subscription
GCP_ZONE=us-central1-a
WORK="$(mktemp -d)"

env_value() {  # value of one variable in the env file, without sourcing the file
  python3 - "$ENV_FILE" "$1" <<'PY'
import sys
for raw in open(sys.argv[1]):
    line = raw.strip()
    if line.startswith("export "):
        line = line[7:].strip()
    if line.startswith(sys.argv[2] + "="):
        value = line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        print(value)
PY
}

AZ_SUBSCRIPTION="$(env_value CLOUDOPS_AZURE_SUBSCRIPTION_IDS | cut -d, -f1)"
GCP_PROJECT="$(env_value CLOUDOPS_GCP_PROJECT_IDS | cut -d, -f1)"
AWS_REGION_NAME="$(env_value CLOUDOPS_AWS_REGION)"
AWS_ADMIN_KEY="$(env_value CLOUDOPS_AWS_ADMIN_ACCESS_KEY_ID)"
AWS_ADMIN_SECRET="$(env_value CLOUDOPS_AWS_ADMIN_SECRET_ACCESS_KEY)"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

aws_admin() {  # aws_admin create|delete
  AWS_ADMIN_KEY="$AWS_ADMIN_KEY" AWS_ADMIN_SECRET="$AWS_ADMIN_SECRET" \
    python3 "$ROOT/cloud-ops/scripts/ephemeral_aws.py" "$1" "$AWS_REGION_NAME" "$NAME"
}

create_azure() {
  [ -n "$AZ_SUBSCRIPTION" ] || { log "azure: skipped (no subscription configured)"; return 0; }
  local tags=(application="$NAME" app="$NAME")
  log "azure: resource group $AZ_GROUP in $AZ_LOCATION"
  az group create --subscription "$AZ_SUBSCRIPTION" -n "$AZ_GROUP" -l "$AZ_LOCATION" \
    --tags "${tags[@]}" --only-show-errors -o none || return 1
  local identity
  identity="$(az identity create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "$NAME-id" \
    --tags "${tags[@]}" --only-show-errors --query id -o tsv)" || return 1
  # Storage account and registry names are global and alphanumeric: derive from the subscription
  local suffix; suffix="$(printf '%s' "$AZ_SUBSCRIPTION" | tr -d '-' | cut -c1-10)"
  log "azure: storage account, container registry"
  az storage account create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "calcitetest$suffix" \
    --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access false \
    --tags "${tags[@]}" --only-show-errors -o none || return 1
  az acr create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "calcitetest$suffix" \
    --sku Basic --tags "${tags[@]}" --only-show-errors -o none || return 1
  log "azure: B1s virtual machine"
  ssh-keygen -q -t rsa -b 2048 -N '' -f "$WORK/throwaway" || return 1
  az vm create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "$NAME" \
    --image Ubuntu2204 --size Standard_B1s --admin-username calcite \
    --ssh-key-values "$WORK/throwaway.pub" --assign-identity "$identity" \
    --nsg-rule NONE --public-ip-sku Standard --tags "${tags[@]}" \
    --only-show-errors -o none || return 1
}

delete_azure() {
  [ -n "$AZ_SUBSCRIPTION" ] || return 0
  if [ "$(az group exists --subscription "$AZ_SUBSCRIPTION" -n "$AZ_GROUP")" = true ]; then
    log "azure: deleting resource group $AZ_GROUP (everything in it)"
    az group delete --subscription "$AZ_SUBSCRIPTION" -n "$AZ_GROUP" --yes --only-show-errors
  fi
  log "azure: resource group exists: $(az group exists --subscription "$AZ_SUBSCRIPTION" -n "$AZ_GROUP")"
}

create_gcp() {
  [ -n "$GCP_PROJECT" ] || { log "gcp: skipped (no project configured)"; return 0; }
  log "gcp: e2-micro instance in $GCP_ZONE (auto-deletes after 15 minutes)"
  gcloud compute instances create "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
    --machine-type e2-micro --image-family debian-12 --image-project debian-cloud \
    --labels "application=$NAME,app=$NAME" --tags "$NAME" \
    --max-run-duration 900s --instance-termination-action DELETE \
    --quiet --format 'value(name,status)' || return 1
}

delete_gcp() {
  [ -n "$GCP_PROJECT" ] || return 0
  if gcloud compute instances describe "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
      --format 'value(name)' >/dev/null 2>&1; then
    log "gcp: deleting instance $NAME"
    gcloud compute instances delete "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" --quiet
  fi
  log "gcp: instances labelled $NAME left: $(gcloud compute instances list --project "$GCP_PROJECT" \
    --filter "labels.application=$NAME" --format 'value(name)' | wc -l | tr -d ' ')"
}

create_aws() {
  [ -n "$AWS_ADMIN_KEY" ] || { log "aws: skipped (CLOUDOPS_AWS_ADMIN_ACCESS_KEY_ID is blank)"; return 0; }
  log "aws: t3.micro instance, security group, ECR repository, DynamoDB table in $AWS_REGION_NAME"
  aws_admin create
}

delete_aws() {
  [ -n "$AWS_ADMIN_KEY" ] || return 0
  log "aws: deleting everything tagged $NAME"
  aws_admin delete
}

teardown() {
  trap - EXIT INT TERM
  log "teardown"
  delete_gcp
  delete_aws
  delete_azure
  rm -rf "$WORK"
}

if [ "${1:-}" = teardown ]; then
  teardown
  exit 0
fi

trap teardown EXIT
trap 'exit 130' INT TERM

status=0
create_gcp || status=1
create_aws || status=1
create_azure || status=1
if [ "$status" -ne 0 ]; then
  log "creating resources failed; nothing was audited"
  exit 1
fi

# Azure Resource Graph, which the adapter reads, indexes new resources with a delay
log "waiting for Azure Resource Graph to list the new virtual machine"
for _ in $(seq 1 36); do
  found="$(az rest --method post --only-show-errors \
    --url 'https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01' \
    --body "{\"subscriptions\":[\"$AZ_SUBSCRIPTION\"],\"query\":\"Resources | where type =~ 'microsoft.compute/virtualmachines' and name == '$NAME' | count\"}" \
    --query 'data[0].Count' -o tsv 2>/dev/null)"
  [ "${found:-0}" = 1 ] && break
  sleep 5
done
log "virtual machines listed: ${found:-0}"

log "running the live column audit"
python3 "$ROOT/cloud-ops/scripts/write-local-test-properties.py"
(cd "$ROOT" && ./gradlew :cloud-ops:integrationTest --tests '*CloudOpsLiveColumnAuditTest*' \
  --console=plain -q --rerun-tasks -x :core:compileJava -x :linq4j:compileJava) > "$WORK/audit.log" 2>&1
audit=$?
REPORT="$ROOT/cloud-ops/build/reports/cloudops-column-audit.txt"
if [ "$audit" -eq 0 ]; then
  cp "$REPORT" "$ROOT/cloud-ops/build/reports/cloudops-column-audit-ephemeral.txt"
  log "audit passed: cloud-ops/build/reports/cloudops-column-audit-ephemeral.txt"
else
  log "audit FAILED"
  grep -a -e 'Exception' -e 'FAILURE' "$WORK/audit.log" | head -20
fi
exit "$audit"
