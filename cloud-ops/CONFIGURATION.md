# Cloud Ops Adapter Configuration

How to give the adapter credentials for Azure, AWS and GCP, and what each setting does. The
settings are the same whether they arrive in a JDBC URL, a Calcite model file, or environment
variables; `CloudOpsSchemaFactory` reads all three.

## Where settings come from

For each setting the factory looks first at the schema operand (the model file or the
`jdbc:cloudops:` URL), then at an environment variable. A blank value counts as absent.

### JDBC URL

```
jdbc:cloudops:aws.accessKeyId=AKIA...;aws.secretAccessKey=...;aws.accountIds=111111111111
```

`CloudOpsDriver` splits the text after the prefix on `;`, URL-decodes each value, and builds an
inline model from the pairs. The pair `schema=<name>` names the schema; every other pair becomes
an operand.

### Model file

```json
{
  "version": "1.0",
  "defaultSchema": "cloud",
  "schemas": [
    {
      "name": "cloud",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.ops.CloudOpsSchemaFactory",
      "operand": {
        "azure.tenantId": "${AZURE_TENANT_ID}",
        "azure.clientId": "${AZURE_CLIENT_ID}",
        "azure.clientSecret": "${AZURE_CLIENT_SECRET}",
        "azure.subscriptionIds": "${AZURE_SUBSCRIPTION_IDS}",
        "gcp.credentialsPath": "${GCP_CREDENTIALS_PATH}",
        "gcp.projectIds": "${GCP_PROJECT_IDS}",
        "aws.accessKeyId": "${AWS_ACCESS_KEY_ID}",
        "aws.secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
        "aws.accountIds": "${AWS_ACCOUNT_IDS}"
      }
    }
  ]
}
```

Connect with `jdbc:calcite:model=/path/to/model.json`. Calcite substitutes `${NAME}` references
before it parses the file. Operand keys are flat strings with a dot in them, not nested objects.

### Environment variables only

Leave the operand empty and export the variables. This is how
[`pgwire-cloudops`](../pgwire-cloudops/README.md) is configured.

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_SUBSCRIPTION_IDS="sub1,sub2"

export GCP_CREDENTIALS_PATH="/path/to/key.json"
export GCP_PROJECT_IDS="project1,project2"

export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ACCOUNT_IDS="111111111111,222222222222"
```

### In Java

```java
CloudOpsConfig.AzureConfig azure = new CloudOpsConfig.AzureConfig(
    "tenant-id", "client-id", "client-secret", Arrays.asList("sub1", "sub2"));

// providers, azure, gcp, aws, cacheEnabled, cacheTtlMinutes, cacheDebugMode
CloudOpsConfig config = new CloudOpsConfig(null, azure, null, null, null, null, null);

SchemaPlus rootSchema = calciteConnection.getRootSchema();
rootSchema.add("cloud", new CloudOpsSchema(config, "cloud"));
```

This path skips the factory, so it also skips the factory's checks for missing settings.

## Which providers are on

A provider is switched on by one setting. With it present, the provider's other required settings
must be present as well.

| Provider | Switched on by | Also required |
|----------|----------------|---------------|
| Azure | `azure.tenantId` | `azure.clientId`, `azure.clientSecret`, `azure.subscriptionIds` |
| AWS | `aws.accessKeyId` | `aws.secretAccessKey`, `aws.accountIds` |
| GCP | `gcp.credentialsPath` | `gcp.projectIds` |

A provider that is partly configured fails schema creation. The message names the missing
settings:

```
AWS is configured without aws.accountIds, aws.secretAccessKey
```

It arrives as the cause of `Error creating Cloud Governance schema`. Earlier versions left such a
provider out without saying so.

The check keys on the switching setting. An `aws.secretAccessKey` with no `aws.accessKeyId` leaves
AWS off and raises nothing. With no provider on, schema creation fails with
`At least one cloud provider must be configured`.

Because settings fall back to the environment, a stray `AWS_ACCESS_KEY_ID` in the process
environment switches AWS on. If `AWS_ACCOUNT_IDS` is not set too, the connection fails.

## Settings

Write lists as comma-separated values without spaces. Only `aws.region` is trimmed.

### Azure

| Setting | Env variable | Description |
|---------|--------------|-------------|
| `azure.tenantId` | `AZURE_TENANT_ID` | Azure AD tenant ID |
| `azure.clientId` | `AZURE_CLIENT_ID` | App registration (client) ID |
| `azure.clientSecret` | `AZURE_CLIENT_SECRET` | App registration client secret |
| `azure.subscriptionIds` | `AZURE_SUBSCRIPTION_IDS` | Subscriptions to query |

The service principal needs the built-in **Reader** role on each subscription.

```bash
az ad app create --display-name "Cloud Ops Adapter"
az ad sp create --id <app-id>
az role assignment create --assignee <app-id> --role Reader \
  --scope /subscriptions/<subscription-id>
az ad app credential reset --id <app-id>
```

`scripts/New-CloudOpsAzureCredentials.ps1` does the same steps and writes the four values to an
env file.

The adapter reads Azure through Resource Graph (KQL), plus two Azure Resource Manager reads per
storage account for its blob-service settings and lifecycle policy.

### AWS

| Setting | Env variable | Description |
|---------|--------------|-------------|
| `aws.accessKeyId` | `AWS_ACCESS_KEY_ID` | Access key ID |
| `aws.secretAccessKey` | `AWS_SECRET_ACCESS_KEY` | Secret access key |
| `aws.accountIds` | `AWS_ACCOUNT_IDS` | Accounts to query |
| `aws.region` | `AWS_REGION` | Optional. Regions to query; see below |
| `aws.roleArn` | `AWS_ROLE_ARN` | Optional. Role to assume in each account |

**Regions.** `aws.region` accepts three forms:

| Value | Regions queried |
|-------|-----------------|
| `us-east-1` | That region |
| `us-east-1,eu-west-1` | Each listed region |
| absent, or `all` | Every region enabled for the account, found with EC2 `DescribeRegions` |

The setting is no longer required and no longer defaults to `us-east-1`. Querying every region
takes longer: each regional table makes its calls once per region, up to 16 regions at a time. An
empty name in a list (`us-east-1,,eu-west-1`) is rejected.

S3 buckets and IAM resources are global. They are listed once per account and do not depend on
`aws.region`.

**Accounts and roles.** Without `aws.roleArn` the access key is used for every listed account, and
each row is labelled with the account ID from the list. List only the account the key belongs to.

With `aws.roleArn`, the adapter assumes the role once per account, replacing `{account-id}` in the
ARN:

```json
{ "aws.roleArn": "arn:aws:iam::{account-id}:role/CloudOpsRole" }
```

The role in each target account must trust the principal that owns the access key:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::SOURCE-ACCOUNT:user/ops-user" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

A role that cannot be assumed fails the connection. The adapter does not fall back to the access
key's own account.

The IAM actions the adapter calls are listed in
[aws-permissions-needed.md](aws-permissions-needed.md).

### GCP

| Setting | Env variable | Description |
|---------|--------------|-------------|
| `gcp.credentialsPath` | `GCP_CREDENTIALS_PATH` | Path to a service-account JSON key |
| `gcp.projectIds` | `GCP_PROJECT_IDS` | Projects to query |

```bash
gcloud iam service-accounts create cloud-ops --display-name="Cloud Ops Service Account"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:cloud-ops@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/viewer"

gcloud iam service-accounts keys create key.json \
  --iam-account=cloud-ops@PROJECT_ID.iam.gserviceaccount.com
```

The APIs to enable in each project are listed in
[aws-permissions-needed.md](aws-permissions-needed.md#gcp).

### Other settings

| Setting | Env variable | Default | Description |
|---------|--------------|---------|-------------|
| `schema` | none | `cloud` | JDBC URL only. Name the tables are registered under |
| `providers` | `CLOUD_OPS_PROVIDERS` | `azure,gcp,aws` | Providers to query. A provider is queried only if it is also configured |
| `cache.enabled` | `CLOUD_OPS_CACHE_ENABLED` | `true` | Parsed, not used |
| `cache.ttlMinutes` | `CLOUD_OPS_CACHE_TTL_MINUTES` | `5` | Parsed, not used. A non-integer value fails schema creation |
| `cache.debugMode` | `CLOUD_OPS_CACHE_DEBUG_MODE` | `false` | Parsed, not used |

The cache settings do not reach the code that builds the cache. See
[OPTIMIZATION.md](OPTIMIZATION.md#caching).

## Errors

A failed cloud call fails the query. It does not return an empty table. The exception names the
cloud, the resource kind, and for AWS the account and region:

```
Querying AWS database resources failed: Querying database resources in AWS account 111111111111,
region eu-west-1 failed: User: arn:aws:iam::111111111111:user/ops is not authorized to perform:
rds:DescribeDBInstances
```

| Message starts with | Meaning |
|---------------------|---------|
| `Azure is configured without ...`, `AWS is configured without ...`, `GCP is configured without ...` | A provider is switched on but the named settings are missing |
| `At least one cloud provider must be configured` | None of the three switching settings was found |
| `Empty region name in AWS region setting` | `aws.region` has an empty entry |
| `Listing the regions enabled for AWS account ... failed` | `ec2:DescribeRegions` was refused or the key is invalid |
| `Assuming role ... for AWS account ... failed` | `sts:AssumeRole` was refused; check the role's trust policy |
| `Azure Resource Graph query failed` | Bad Azure credentials, or the principal lacks Reader on a subscription |
| `Azure Resource Manager answered 403 for ...` | The principal cannot read a storage account's blob service or management policy |
| `Failed to initialize GCP credentials` | The key file at `gcp.credentialsPath` cannot be read |
| `Querying ... in project ... failed`, `Listing ... of project ... failed` | A GCP API is not enabled or the service account lacks a role |

To check a configuration against live clouds, run the column audit described in
[TESTING.md](TESTING.md#live-column-audit).

## Credential handling

Keep secrets out of model files that are committed; reference environment variables. Give the
adapter read-only credentials. It never writes to a cloud, and the roles named above are enough.
