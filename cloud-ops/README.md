# Cloud Ops JDBC Driver

Query Azure, AWS, and GCP resources — VMs, storage, Kubernetes clusters, databases, IAM — using standard SQL from any JDBC client.

## Download

Build the shadow JAR from this repo:

```bash
./gradlew :cloud-ops:shadowJar
# Output: cloud-ops/build/libs/sih-cloudops-*.jar
```

## Connect

**JDBC URL format:**
```
jdbc:cloudops:<provider>.<param>=<value>[;<provider>.<param>=<value>...]
```

**Driver class:** `org.apache.calcite.adapter.ops.CloudOpsDriver`

### Connection examples

```
# Azure only
jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1,sub2

# AWS only
jdbc:cloudops:aws.accessKeyId=xxx;aws.secretAccessKey=xxx;aws.region=us-east-1;aws.accountIds=123456789

# GCP only
jdbc:cloudops:gcp.credentialsPath=/path/to/service-account.json;gcp.projectIds=my-project

# All three clouds
jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1;aws.accessKeyId=xxx;aws.secretAccessKey=xxx;aws.region=us-east-1;aws.accountIds=123;gcp.credentialsPath=/path/sa.json;gcp.projectIds=proj1
```

## Connection parameters

### Azure
| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `azure.tenantId` | Azure AD tenant ID | `AZURE_TENANT_ID` |
| `azure.clientId` | Service principal client ID | `AZURE_CLIENT_ID` |
| `azure.clientSecret` | Service principal secret | `AZURE_CLIENT_SECRET` |
| `azure.subscriptionIds` | Comma-separated subscription IDs | `AZURE_SUBSCRIPTION_IDS` |

### AWS
| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `aws.accessKeyId` | IAM access key | `AWS_ACCESS_KEY_ID` |
| `aws.secretAccessKey` | IAM secret key | `AWS_SECRET_ACCESS_KEY` |
| `aws.region` | AWS region | `AWS_REGION` |
| `aws.accountIds` | Comma-separated account IDs | `AWS_ACCOUNT_IDS` |
| `aws.roleArn` | Role ARN to assume (optional) | `AWS_ROLE_ARN` |

### GCP
| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `gcp.credentialsPath` | Path to service account JSON | `GCP_CREDENTIALS_PATH` |
| `gcp.projectIds` | Comma-separated project IDs | `GCP_PROJECT_IDS` |

### Cache
| Parameter | Description | Default |
|-----------|-------------|---------|
| `cache.enabled` | Enable result caching | `true` |
| `cache.ttlMinutes` | Cache TTL in minutes | `5` |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1`
3. **Driver JAR:** add `sih-cloudops-*.jar`
4. **Driver class:** `org.apache.calcite.adapter.ops.CloudOpsDriver`

## Available tables

| Table | Clouds | Description |
|-------|--------|-------------|
| `cloud.compute_resources` | Azure, AWS, GCP | VMs and compute instances |
| `cloud.storage_resources` | Azure, AWS, GCP | Storage accounts and buckets |
| `cloud.kubernetes_clusters` | Azure, AWS, GCP | AKS, EKS, GKE clusters |
| `cloud.database_resources` | Azure, AWS, GCP | Managed database services |
| `cloud.network_resources` | Azure, AWS, GCP | VNets, VPCs, subnets |
| `cloud.iam_resources` | Azure, AWS, GCP | Users, roles, service accounts |
| `cloud.container_registries` | Azure, AWS, GCP | ACR, ECR, GCR |

## Sample queries

```sql
-- All VMs across all clouds
SELECT cloud_provider, region, name, status, instance_type
FROM cloud.compute_resources
ORDER BY cloud_provider, region;

-- Storage buckets over 1TB
SELECT cloud_provider, name, size_gb, region
FROM cloud.storage_resources
WHERE size_gb > 1024
ORDER BY size_gb DESC;

-- Kubernetes clusters and node counts
SELECT cloud_provider, name, region, node_count, kubernetes_version
FROM cloud.kubernetes_clusters;

-- Cross-cloud resource count
SELECT cloud_provider, COUNT(*) AS resources
FROM cloud.compute_resources
GROUP BY cloud_provider;
```
