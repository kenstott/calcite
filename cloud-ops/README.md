# Cloud Ops JDBC Driver

Query Azure, AWS, and GCP resources — VMs, storage, Kubernetes clusters, databases, IAM — using standard SQL from any JDBC client. The adapter is read-only: it lists resources through each cloud's inventory APIs and returns them as rows.

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

# AWS only, every region enabled for the account
jdbc:cloudops:aws.accessKeyId=xxx;aws.secretAccessKey=xxx;aws.accountIds=123456789012

# AWS only, two regions
jdbc:cloudops:aws.accessKeyId=xxx;aws.secretAccessKey=xxx;aws.accountIds=123456789012;aws.region=us-east-1,eu-west-1

# GCP only
jdbc:cloudops:gcp.credentialsPath=/path/to/service-account.json;gcp.projectIds=my-project

# All three clouds
jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1;aws.accessKeyId=xxx;aws.secretAccessKey=xxx;aws.accountIds=123456789012;gcp.credentialsPath=/path/sa.json;gcp.projectIds=proj1
```

## Connection parameters

A provider is switched on by one setting: `azure.tenantId`, `aws.accessKeyId` or
`gcp.credentialsPath`. Once it is on, its other required settings must be present too. A provider
that is partly configured fails the connection with a message naming what is missing, for example
`AWS is configured without aws.accountIds, aws.secretAccessKey`. At least one provider must be
configured.

Each setting falls back to the environment variable shown when the parameter is absent.

### Azure
| Parameter | Required | Description | Env variable |
|-----------|----------|-------------|-------------|
| `azure.tenantId` | switches Azure on | Azure AD tenant ID | `AZURE_TENANT_ID` |
| `azure.clientId` | yes | Service principal client ID | `AZURE_CLIENT_ID` |
| `azure.clientSecret` | yes | Service principal secret | `AZURE_CLIENT_SECRET` |
| `azure.subscriptionIds` | yes | Comma-separated subscription IDs | `AZURE_SUBSCRIPTION_IDS` |

### AWS
| Parameter | Required | Description | Env variable |
|-----------|----------|-------------|-------------|
| `aws.accessKeyId` | switches AWS on | IAM access key | `AWS_ACCESS_KEY_ID` |
| `aws.secretAccessKey` | yes | IAM secret key | `AWS_SECRET_ACCESS_KEY` |
| `aws.accountIds` | yes | Comma-separated account IDs | `AWS_ACCOUNT_IDS` |
| `aws.region` | no | One region, a comma-separated list, or `all`. Absent or `all` queries every region enabled for the account | `AWS_REGION` |
| `aws.roleArn` | no | Role ARN to assume; `{account-id}` is replaced with each account ID | `AWS_ROLE_ARN` |

When `aws.region` is absent or `all`, the adapter asks EC2 `DescribeRegions` which regions the
account can use and queries each of them. S3 buckets and IAM resources are global and are listed
once per account whatever the setting.

### GCP
| Parameter | Required | Description | Env variable |
|-----------|----------|-------------|-------------|
| `gcp.credentialsPath` | switches GCP on | Path to service account JSON | `GCP_CREDENTIALS_PATH` |
| `gcp.projectIds` | yes | Comma-separated project IDs | `GCP_PROJECT_IDS` |

### Other
| Parameter | Description | Default |
|-----------|-------------|---------|
| `schema` | Name the tables are registered under | `cloud` |
| `providers` | Comma-separated subset of configured providers to query | all configured |

The cache parameters `cache.enabled`, `cache.ttlMinutes` and `cache.debugMode` are accepted but
have no effect today; see [OPTIMIZATION.md](OPTIMIZATION.md#caching).

Permissions each cloud needs are listed in [aws-permissions-needed.md](aws-permissions-needed.md).
[CONFIGURATION.md](CONFIGURATION.md) covers model files and credential setup.

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:cloudops:azure.tenantId=xxx;azure.clientId=xxx;azure.clientSecret=xxx;azure.subscriptionIds=sub1`
3. **Driver JAR:** add `sih-cloudops-*.jar`
4. **Driver class:** `org.apache.calcite.adapter.ops.CloudOpsDriver`

## Available tables

| Table | Clouds | Description |
|-------|--------|-------------|
| `cloud.compute_resources` | Azure, AWS, GCP | VMs and compute instances |
| `cloud.storage_resources` | Azure, AWS, GCP | Storage accounts, buckets, and on Azure managed disks, SQL databases and Cosmos DB accounts |
| `cloud.kubernetes_clusters` | Azure, AWS, GCP | AKS, EKS, GKE clusters |
| `cloud.database_resources` | Azure, AWS, GCP | Managed database services |
| `cloud.network_resources` | Azure, AWS, GCP | VNets, VPCs, subnets, security groups, firewall rules |
| `cloud.iam_resources` | Azure, AWS, GCP | Users, roles, policies, managed identities, service accounts |
| `cloud.container_registries` | Azure, AWS, GCP | ACR, ECR, Artifact Registry |
| `cloud.compute_security_groups` | Azure, AWS | Which security group is attached to which instance |

The schema is named `cloud` unless the `schema` connection property says otherwise. Columns, types
and per-cloud notes are in [docs/SCHEMA.md](docs/SCHEMA.md).

A column is null where a cloud has no such concept (`resource_group` outside Azure, for one), or
where the value is not available from the inventory APIs the adapter calls. A cloud that cannot
be queried — expired credentials, a missing permission, an API that is not enabled — fails the
query with that cloud's error; it is never reported as "no resources".

## Identifier case

Table and column names are lower-case. Calcite's default lexical policy upper-cases unquoted
identifiers, so with a plain connection the names must be quoted:

```sql
SELECT "cloud_provider", "instance_name" FROM "cloud"."compute_resources";
```

To write them unquoted, add `unquotedCasing=TO_LOWER` to the URL. The driver forwards URL
parameters to Calcite. The queries below assume that setting.

## Sample queries

```sql
-- All VMs across all clouds
SELECT cloud_provider, region, instance_name, state, instance_type
FROM cloud.compute_resources
ORDER BY cloud_provider, region;

-- Storage resources with a known size over 1 TiB
SELECT cloud_provider, resource_name, size_bytes, region
FROM cloud.storage_resources
WHERE size_bytes > 1099511627776
ORDER BY size_bytes DESC;

-- Kubernetes clusters and node counts
SELECT cloud_provider, cluster_name, region, node_count, kubernetes_version
FROM cloud.kubernetes_clusters;

-- Cross-cloud resource count
SELECT cloud_provider, COUNT(*) AS resources
FROM cloud.compute_resources
GROUP BY cloud_provider;
```

What is and is not sent to the cloud APIs for a given query is described in
[OPTIMIZATION.md](OPTIMIZATION.md).
