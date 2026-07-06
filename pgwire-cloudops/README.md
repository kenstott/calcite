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
- **AWS:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ACCOUNT_IDS`, `AWS_REGION` (optional `AWS_ROLE_ARN`)
- **GCP:** `GCP_CREDENTIALS_PATH`, `GCP_PROJECT_IDS`

Configure only the providers you use, then narrow `providers` accordingly. (You can also set the
`azure.tenantId` / `aws.accessKeyId` / `gcp.credentialsPath` operands directly in the model instead.)

## Run

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_ACCOUNT_IDS=... AWS_REGION=us-east-1
tar -xzf pgwire-cloudops-<version>-<os>.tar.gz
cd pgwire-cloudops-<version>-<os>
./bin/pgwire-cloudops             # serves on 127.0.0.1:5433
psql -h 127.0.0.1 -p 5433 -c 'SELECT * FROM cloudops."<resource_table>" LIMIT 10'
```

No Python or Java install required — the bundle is airgap-ready.
