# pgwire-cloudops

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes the **Cloud Ops** adapter
(`CloudOpsSchemaFactory`) over the PostgreSQL wire protocol — a unified SQL view of Azure, AWS, and
GCP resource inventory. Query it from `psql` or any PostgreSQL client.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-cloudops-<version>-<os>.tar.gz` (bundled CPython + JRE + the
`cloud-ops` module's runtime jars + this model).

## Configure

`model/model.json` lists the providers to query (`"providers": "azure,aws,gcp"`). Credentials are
read **from the environment** (the factory falls back to these when the operand isn't set), so no
secrets live in the model:

- **Azure:** `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_SUBSCRIPTION_IDS`
- **AWS:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ACCOUNT_IDS` (optional `AWS_REGION`, `AWS_ROLE_ARN`)
- **GCP:** `GCP_CREDENTIALS_PATH`, `GCP_PROJECT_IDS`

Configure only the providers you use, then narrow `providers` accordingly. (You can also set the
`azure.tenantId` / `aws.accessKeyId` / `gcp.credentialsPath` operands directly in the model instead.)

`AWS_REGION` takes one region, a comma-separated list, or `all`. Unset or `all`, the adapter
queries every region enabled for each account, which it finds with EC2 DescribeRegions. It no
longer defaults to `us-east-1`. S3 and IAM are global and do not depend on it.

A provider is switched on by `AZURE_TENANT_ID`, `AWS_ACCESS_KEY_ID` or `GCP_CREDENTIALS_PATH`. If
one of those is set and another variable of the same provider is missing, the schema fails to load
with a message such as `AWS is configured without aws.accountIds`. The provider is not left out.
Watch for an `AWS_ACCESS_KEY_ID` exported for some other tool.

A cloud call that fails at query time fails the query with that cloud's error. It does not return
zero rows.

Tables and columns are described in [cloud-ops/docs/SCHEMA.md](../cloud-ops/docs/SCHEMA.md), and
the permissions each cloud needs in
[cloud-ops/aws-permissions-needed.md](../cloud-ops/aws-permissions-needed.md).

## Run

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_ACCOUNT_IDS=...   # all enabled regions
tar -xzf pgwire-cloudops-<version>-<os>.tar.gz
cd pgwire-cloudops-<version>-<os>
./bin/pgwire-cloudops             # serves on 127.0.0.1:5433
./bin/pgwire-cloudops --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -c 'SELECT * FROM cloudops."<resource_table>" LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight
through to `pgwire-calcite`. There is no port env var — set it on the command line.

No Python or Java install required — the bundle is airgap-ready.
