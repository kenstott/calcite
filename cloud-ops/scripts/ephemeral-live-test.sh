#!/usr/bin/env bash
# Creates the cheapest short-lived resources that give the cloud-ops tables something to read,
# runs the live column audit against them, and deletes them again — also when the audit fails
# or the script is interrupted.
#
#   cloud-ops/scripts/ephemeral-live-test.sh            # create, audit, delete
#   cloud-ops/scripts/ephemeral-live-test.sh teardown   # only delete (after a crashed run)
#
# What it creates (the smallest size of each, for the length of one run — about an hour,
# most of it waiting for clusters and databases; well under a dollar in total):
#   Azure  resource group calcite-cloudops-test-rg: B1s VM (with VNet, NSG, NIC, public IP,
#          disk), storage account (versioning, soft delete and a lifecycle rule switched on),
#          Basic container registry, user-assigned managed identity, AKS cluster with one
#          node, SQL server with a Basic database, PostgreSQL and MySQL flexible servers,
#          serverless Cosmos DB account, smallest Azure Managed Redis cache
#   GCP    e2-micro VM that GCP deletes by itself after 15 minutes, zonal GKE cluster with
#          one node, db-f1-micro Cloud SQL instance
#          (the Artifact Registry repository calcite-cloudops-test is permanent and free)
#   AWS    t3.micro instance, security group, empty ECR repository, empty DynamoDB table,
#          db.t3.micro RDS instance, Aurora cluster without instances, cache.t4g.micro
#          ElastiCache cluster, EKS cluster with one t3.small node and its two IAM roles —
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
GCP_REGION=us-central1
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

all_of() {  # waits for the given background jobs; fails if any of them failed
  local failed=0 pid
  for pid in "$@"; do wait "$pid" || failed=1; done
  return "$failed"
}

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
  # Storage account, registry and database server names are global and alphanumeric. They are
  # derived from the subscription, and from the time of the run because a name deleted minutes
  # ago is not always free again (a SQL server re-created under its old name timed out)
  local suffix; suffix="$(printf '%s' "$AZ_SUBSCRIPTION" | tr -d '-' | cut -c1-6)$(date +%H%M)"
  local global="calcitetest$suffix" password pids=()
  password="A1-$(openssl rand -hex 16)"  # never printed; the databases live for minutes
  local common=(--subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP")

  # The slow ones start first and run side by side
  log "azure: starting AKS cluster, databases and cache"
  az aks create "${common[@]}" -n "$NAME" --node-count 1 --node-vm-size Standard_B2ms \
    --tier free --no-ssh-key --enable-managed-identity --tags "${tags[@]}" --only-show-errors -o none & pids+=($!)
  # Azure Cache for Redis no longer accepts new caches; Azure Managed Redis replaces it
  az redisenterprise create "${common[@]}" -n "$global" -l "$AZ_LOCATION" --sku Balanced_B0 \
    --public-network-access Enabled --tags "${tags[@]}" --only-show-errors -o none & pids+=($!)
  az postgres flexible-server create "${common[@]}" -n "${global}pg" -l "$AZ_LOCATION" \
    --tier Burstable --sku-name Standard_B1ms --storage-size 32 --version 16 \
    --admin-user calcite --admin-password "$password" --public-access None \
    --tags "${tags[@]}" --yes --only-show-errors -o none & pids+=($!)
  az mysql flexible-server create "${common[@]}" -n "${global}my" -l "$AZ_LOCATION" \
    --tier Burstable --sku-name Standard_B1ms --storage-size 32 \
    --admin-user calcite --admin-password "$password" --public-access None \
    --tags "${tags[@]}" --yes --only-show-errors -o none & pids+=($!)
  az cosmosdb create "${common[@]}" -n "$global" --capacity-mode Serverless \
    --locations regionName="$AZ_LOCATION" --tags "${tags[@]}" --only-show-errors -o none & pids+=($!)
  ( az sql server create "${common[@]}" -n "$global" -l "$AZ_LOCATION" \
      --admin-user calcite --admin-password "$password" --minimal-tls-version 1.2 --only-show-errors -o none \
    && az sql db create "${common[@]}" -s "$global" -n "$NAME" --service-objective Basic \
      --backup-storage-redundancy Local --tags "${tags[@]}" --only-show-errors -o none ) & pids+=($!)

  log "azure: storage account, container registry"
  az storage account create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "calcitetest$suffix" \
    --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access false \
    --tags "${tags[@]}" --only-show-errors -o none || return 1
  az storage account blob-service-properties update "${common[@]}" --account-name "$global" \
    --enable-versioning true --enable-delete-retention true --delete-retention-days 7 \
    --only-show-errors -o none || return 1
  az storage account management-policy create "${common[@]}" --account-name "$global" \
    --policy '{"rules":[{"enabled":true,"name":"expire","type":"Lifecycle","definition":{"actions":{"baseBlob":{"delete":{"daysAfterModificationGreaterThan":30}}},"filters":{"blobTypes":["blockBlob"]}}}]}' \
    --only-show-errors -o none || return 1
  az acr create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "calcitetest$suffix" \
    --sku Basic --tags "${tags[@]}" --only-show-errors -o none || return 1
  log "azure: B1s virtual machine"
  ssh-keygen -q -t rsa -b 2048 -N '' -f "$WORK/throwaway" || return 1
  az vm create --subscription "$AZ_SUBSCRIPTION" -g "$AZ_GROUP" -n "$NAME" \
    --image Ubuntu2204 --size Standard_B1s --admin-username calcite \
    --ssh-key-values "$WORK/throwaway.pub" --assign-identity "$identity" \
    --nsg-rule NONE --public-ip-sku Standard --tags "${tags[@]}" \
    --only-show-errors -o none || return 1
  log "azure: waiting for the AKS cluster, databases and cache"
  all_of "${pids[@]}" || return 1
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
  # A deleted Cloud SQL instance keeps its name reserved for a week: every run uses a new one
  local sql="$NAME-$(date +%m%d%H%M)" pids=()
  log "gcp: GKE cluster with one node, Cloud SQL instance $sql"
  gcloud container clusters create "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
    --num-nodes 1 --machine-type e2-small --labels "application=$NAME,app=$NAME" \
    --quiet --format 'value(name,status)' & pids+=($!)
  ( gcloud sql instances create "$sql" --project "$GCP_PROJECT" --region "$GCP_REGION" \
      --database-version POSTGRES_16 --edition enterprise --tier db-f1-micro \
      --storage-size 10 --storage-type HDD --no-backup --quiet --format 'value(name,state)' \
    && curl -sS -f -o /dev/null -X PATCH \
      -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      -H 'Content-Type: application/json' \
      -d "{\"settings\":{\"userLabels\":{\"application\":\"$NAME\",\"app\":\"$NAME\"}}}" \
      "https://sqladmin.googleapis.com/v1/projects/$GCP_PROJECT/instances/$sql" ) & pids+=($!)
  all_of "${pids[@]}" || return 1
}

delete_gcp() {
  [ -n "$GCP_PROJECT" ] || return 0
  if gcloud compute instances describe "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
      --format 'value(name)' >/dev/null 2>&1; then
    log "gcp: deleting instance $NAME"
    gcloud compute instances delete "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" --quiet
  fi
  local pids=() sql
  if gcloud container clusters describe "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
      --format 'value(name)' >/dev/null 2>&1; then
    log "gcp: deleting GKE cluster $NAME"
    gcloud container clusters delete "$NAME" --project "$GCP_PROJECT" --zone "$GCP_ZONE" \
      --quiet & pids+=($!)
  fi
  for sql in $(gcloud sql instances list --project "$GCP_PROJECT" \
      --filter "name~^$NAME-" --format 'value(name)'); do
    log "gcp: deleting Cloud SQL instance $sql"
    gcloud sql instances delete "$sql" --project "$GCP_PROJECT" --quiet & pids+=($!)
  done
  all_of ${pids[@]+"${pids[@]}"} || log "gcp: a deletion FAILED"
  log "gcp: GKE clusters left: $(gcloud container clusters list --project "$GCP_PROJECT" \
    --filter "name=$NAME" --format 'value(name)' | wc -l | tr -d ' '), Cloud SQL instances left: $(
    gcloud sql instances list --project "$GCP_PROJECT" --filter "name~^$NAME-" \
    --format 'value(name)' | wc -l | tr -d ' ')"
  log "gcp: instances labelled $NAME left: $(gcloud compute instances list --project "$GCP_PROJECT" \
    --filter "labels.application=$NAME" --format 'value(name)' | wc -l | tr -d ' ')"
}

create_aws() {
  [ -n "$AWS_ADMIN_KEY" ] || { log "aws: skipped (CLOUDOPS_AWS_ADMIN_ACCESS_KEY_ID is blank)"; return 0; }
  log "aws: instance, security group, ECR, DynamoDB, RDS, Aurora, ElastiCache, EKS in $AWS_REGION_NAME"
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
  local pids=()
  delete_gcp & pids+=($!)
  delete_aws & pids+=($!)
  delete_azure & pids+=($!)
  all_of "${pids[@]}" || log "teardown: a deletion FAILED — run '$0 teardown' again"
  rm -rf "$WORK"
}

if [ "${1:-}" = teardown ]; then
  teardown
  exit 0
fi

trap teardown EXIT
trap 'exit 130' INT TERM

creating=()
create_gcp & creating+=($!)
create_aws & creating+=($!)
create_azure & creating+=($!)
all_of "${creating[@]}"
status=$?
if [ "$status" -ne 0 ]; then
  # What was created is still worth auditing; the run fails at the end all the same
  log "creating some resources FAILED (see above); auditing what exists"
fi

# Azure Resource Graph, which the adapter reads, indexes new resources with a delay
# (one virtual machine, AKS cluster, SQL server and database, PostgreSQL and MySQL server,
# Cosmos DB account and Managed Redis cache: eight resources)
log "waiting for Azure Resource Graph to list the new resources"
for _ in $(seq 1 60); do
  found="$(az rest --method post --only-show-errors \
    --url 'https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01' \
    --body "{\"subscriptions\":[\"$AZ_SUBSCRIPTION\"],\"query\":\"Resources | where resourceGroup =~ '$AZ_GROUP' and type in~ ('microsoft.compute/virtualmachines','microsoft.containerservice/managedclusters','microsoft.sql/servers','microsoft.sql/servers/databases','microsoft.dbforpostgresql/flexibleservers','microsoft.dbformysql/flexibleservers','microsoft.documentdb/databaseaccounts','microsoft.cache/redisenterprise') and name != 'master' | count\"}" \
    --query 'data[0].Count' -o tsv 2>/dev/null)"
  [ "${found:-0}" = 8 ] && break
  sleep 5
done
log "resources listed: ${found:-0} of 8"

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
[ "$audit" -eq 0 ] && [ "$status" -eq 0 ]
exit $?
