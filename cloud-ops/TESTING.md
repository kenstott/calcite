# Testing the Cloud Ops adapter

The module has unit tests that need no cloud, and live tests that read real Azure, AWS and GCP
accounts. One command runs both; which live tests run depends on whether credentials are on disk.

```bash
./gradlew :cloud-ops:test
```

## What `:cloud-ops:test` runs

Tests are JUnit 5 classes selected by `@Tag`. `cloud-ops/build.gradle.kts` configures the default task
as follows:

| Tag | Runs in `:cloud-ops:test` |
|-----|---------------------------|
| `unit` | always |
| `integration` | only when `src/test/resources/local-test.properties` exists and holds a complete set of credentials for at least one cloud |
| `performance` | never |
| no tag | always |

A complete set means: all four `azure.*` properties; or `gcp.credentialsPath` and
`gcp.projectIds`; or `aws.accessKeyId`, `aws.secretAccessKey` and `aws.accountIds`.

So on a machine with the properties file, a plain `:cloud-ops:test` calls the clouds. Remove or
rename the file to keep a run offline.

Eight test classes carry no tag. Three are offline. `CloudOpsDataDiscoveryTest`,
`CloudOpsComprehensiveCountTest`, `TestAWSIAM`, `SimpleCountTest` and
`SortPushdownIntegrationTest` read `local-test.properties`; the last two skip themselves when no
configuration loads.

### Other tasks

| Command | Selects |
|---------|---------|
| `./gradlew :cloud-ops:unitTest` | tag `unit` only |
| `./gradlew :cloud-ops:integrationTest` | tag `integration` only, whether or not credentials exist |
| `./gradlew :cloud-ops:performanceTest` | tag `performance` only |
| `./gradlew :cloud-ops:allTests` | everything |

Run one class with `--tests`:

```bash
./gradlew :cloud-ops:test --tests "*SortLimitPushdownTest*" --console=plain
```

HTML reports land in `cloud-ops/build/reports/tests/<task>/`: `unit`, `integration`,
`performance` and `all` for the tasks above, and Gradle's default `test` directory for
`:cloud-ops:test`.

## Credentials for live tests

Live tests read `cloud-ops/src/test/resources/local-test.properties`. The file is git-ignored.
Create it one of two ways.

**From the env file.** `scripts/write-local-test-properties.py` reads the `CLOUDOPS_*` variables
in `govdata/.env.prod` and writes the properties file with mode 600.

```bash
python3 cloud-ops/scripts/write-local-test-properties.py
# wrote cloud-ops/src/test/resources/local-test.properties for: azure, aws, gcp
```

| Cloud | Variables read |
|-------|----------------|
| Azure | `CLOUDOPS_AZURE_TENANT_ID`, `CLOUDOPS_AZURE_CLIENT_ID`, `CLOUDOPS_AZURE_CLIENT_SECRET`, `CLOUDOPS_AZURE_SUBSCRIPTION_IDS` |
| AWS | `CLOUDOPS_AWS_ACCESS_KEY_ID`, `CLOUDOPS_AWS_SECRET_ACCESS_KEY`, `CLOUDOPS_AWS_ACCOUNT_IDS`, optional `CLOUDOPS_AWS_ROLE_ARN` |
| GCP | `CLOUDOPS_GCP_CREDENTIALS_PATH`, `CLOUDOPS_GCP_PROJECT_IDS` |

A cloud with a blank required variable is left out and reported as skipped. The script exits with
an error if no cloud is complete. It does not write `aws.region`, so live tests query every region
enabled for the AWS account.

**By hand.** Copy the sample and fill in the clouds you want.

```bash
cp cloud-ops/src/test/resources/local-test.properties.sample \
   cloud-ops/src/test/resources/local-test.properties
```

The sample sets `aws.region=us-east-1`. Delete that line to cover all regions.

The permissions the credentials need are in
[aws-permissions-needed.md](aws-permissions-needed.md).

## Live column audit

`CloudOpsLiveColumnAuditTest` is the quickest check that a set of credentials can read everything
and that the columns are being filled. It connects through `CloudOpsDriver` with the properties
file, runs `SELECT *` on each of the eight tables, and writes
`cloud-ops/build/reports/cloudops-column-audit.txt`.

```bash
./gradlew :cloud-ops:integrationTest --tests '*CloudOpsLiveColumnAuditTest*' --console=plain
```

For each table and provider the report gives the row count, the columns that were null in every
row, and up to two sample rows. The figures below are illustrative:

```
== database_resources
   aws: 4 rows; always null: [resource_group, tls_version]
      e.g. cloud_provider=aws; account_id=111111111111; database_resource=...
```

The test fails only when a table cannot be read. A column that is always null does not fail it;
read the report and compare with the per-cloud notes in [docs/SCHEMA.md](docs/SCHEMA.md). A column
can also be always null because the account has no resource of the kind that fills it, which is
what the next script addresses.

## Audit against short-lived resources

An account with no databases or clusters cannot show whether those columns work.
`scripts/ephemeral-live-test.sh` creates the smallest resource of each kind, runs the column audit
against them, and deletes them again.

```bash
cloud-ops/scripts/ephemeral-live-test.sh            # create, audit, delete
cloud-ops/scripts/ephemeral-live-test.sh teardown   # only delete, after a crashed run
```

**It creates real, billable resources.** The script's own estimate is about an hour per run, most
of it waiting for clusters and databases, and well under a dollar in total.

What it creates, all tagged or labelled `application=calcite-cloudops-test`:

| Cloud | Resources |
|-------|-----------|
| Azure | Resource group `calcite-cloudops-test-rg` holding: a B1s VM with its VNet, NSG, NIC, public IP and disk; a storage account with versioning, soft delete and a lifecycle rule switched on; a Basic container registry; a user-assigned managed identity; an AKS cluster with one node; a SQL server with a Basic database; PostgreSQL and MySQL flexible servers; a serverless Cosmos DB account; an Azure Managed Redis cache (Azure refuses new Azure Cache for Redis instances) |
| GCP | An e2-micro VM that GCP deletes by itself after 15 minutes; a zonal GKE cluster with one node; a db-f1-micro Cloud SQL instance |
| AWS | A t3.micro instance; a security group; an empty ECR repository; an empty DynamoDB table; a db.t3.micro RDS instance; an Aurora cluster without instances; a cache.t4g.micro ElastiCache cluster; an EKS cluster with one t3.small node and its two IAM roles |

What it needs:

- `govdata/.env.prod` with `CLOUDOPS_AZURE_SUBSCRIPTION_IDS`, `CLOUDOPS_GCP_PROJECT_IDS` and
  `CLOUDOPS_AWS_REGION`. A cloud whose variable is blank is skipped.
- The `az` and `gcloud` CLIs, signed in as someone who may create these resources. Azure and GCP
  resources are created with the CLIs, not with the adapter's read-only credentials.
- For AWS, `CLOUDOPS_AWS_ADMIN_ACCESS_KEY_ID` and `CLOUDOPS_AWS_ADMIN_SECRET_ACCESS_KEY` in the
  env file, and Python with `boto3`. Without the admin key the AWS part is skipped, because the
  adapter's own key is read-only. `scripts/ephemeral_aws.py` does the AWS work.

What it does, in order:

1. Creates the resources of the three clouds in parallel. If any creation fails, nothing is
   audited and the script exits 1.
2. Waits up to five minutes for Azure Resource Graph to list the eight new Azure compute,
   cluster and database resources.
3. Runs `write-local-test-properties.py`, then the column audit through Gradle.
4. On success, copies the report to
   `cloud-ops/build/reports/cloudops-column-audit-ephemeral.txt`.
5. Deletes everything. The exit status is the audit's.

Deletion runs from a shell trap, so it also happens when the audit fails or the script is
interrupted. It removes the Azure resource group and everything in it, the GCP instance, GKE
cluster and every Cloud SQL instance whose name starts with `calcite-cloudops-test-`, and every
AWS resource carrying the test name. If the log shows `a deletion FAILED`, run the `teardown`
form again and check the consoles.

One thing is not deleted: the GCP Artifact Registry repository `calcite-cloudops-test`. The
script neither creates nor removes it; its header describes it as permanent and free.

### Role assumption

`AWSRoleAssumptionLiveTest` reads AWS through an assumed role with a key that may do nothing
but assume it, and checks that the same key without the role is refused.

```bash
export AWS_ADMIN_KEY=... AWS_ADMIN_SECRET=...
python3 cloud-ops/scripts/ephemeral_aws_role.py create
./gradlew :cloud-ops:test -PincludeTags=integration --tests "*AWSRoleAssumptionLiveTest"
python3 cloud-ops/scripts/ephemeral_aws_role.py delete
```

`create` makes the IAM user and role `calcite-cloudops-test-assume` and appends three
`aws.assumeRole.*` lines to `local-test.properties`; `delete` removes all of it. Without those
lines the test is skipped. The user and the role are in one account: the adapter makes the same
`sts:AssumeRole` call for a role in another account, but that has not been run.

Step 3 overwrites `local-test.properties`. Because the script starts Gradle, do not run it while
another Gradle build is using the same checkout.

## Tests worth knowing

| Class | Tag | What it covers |
|-------|-----|----------------|
| `SortLimitPushdownTest` | unit | The `CloudOpsSortScanRule` planner rule: when ORDER BY / LIMIT reach the table and when they do not |
| `CloudOpsSchemaFactoryTest` | unit | Schema creation from operand settings, including missing configuration |
| `AWSRegionSettingTest` | unit | Parsing of `aws.region` |
| `CloudOpsConstraintMetadataTest` | unit | Declared keys and foreign keys |
| `CloudOpsLiveColumnAuditTest` | integration | Every table against the live clouds |

## Troubleshooting

**Integration tests did not run.** `local-test.properties` is missing or no cloud in it is
complete. Run `write-local-test-properties.py` and read which clouds it reports as skipped.

**A live test fails with a cloud error.** The adapter fails a query when a cloud call fails, and
the message names the cloud, account and region. An `AccessDenied` or `AuthorizationFailed` means
a missing permission; see [aws-permissions-needed.md](aws-permissions-needed.md).

**More detail.** `src/test/resources/log4j2-test.xml` controls test logging. Set the logger
`org.apache.calcite.adapter.ops` to `DEBUG` to see what each scan received and fetched.
