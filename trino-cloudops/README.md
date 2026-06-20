# trino-cloudops

A Trino connector that exposes the Calcite **Cloud Ops adapter** as a SQL catalog. One catalog
inventories cloud resources (compute, storage, databases, IAM, networking, Kubernetes, container
registries, …) across **Azure, AWS and GCP**, queryable as relational tables.

It mirrors [`trino-sharepoint`](../trino-sharepoint): a thin wrapper that reuses
[`trino-calcite-base`](../trino-calcite-base)'s `CalciteClient` for JDBC→Trino type mapping.
(Depending on the SPI-less base rather than the `trino-calcite` plugin keeps the generic `calcite`
connector out of this plugin's zip, so only `cloudops` is registered.) The friendly catalog
properties are assembled into a `jdbc:cloudops:` URL for `CloudOpsDriver`, which builds an inline
Calcite model targeting `CloudOpsSchemaFactory`. Tables are exposed under the `cloud` schema.

## Configuration

Configure only the providers you use; at least one is required. Each provider is enabled by its
gating property (`azure.tenant-id`, `aws.access-key-id`, `gcp.credentials-path`). A partially
configured provider **fails fast at startup** with a list of the missing properties.

```properties
connector.name=cloudops

# AWS
aws.access-key-id=AKIA...
aws.secret-access-key=...
aws.account-ids=111111111111,222222222222
aws.region=us-east-1

# Azure
azure.tenant-id=...
azure.client-id=...
azure.client-secret=...
azure.subscription-ids=sub-1,sub-2
```

| Property | Required | Description |
|----------|----------|-------------|
| `providers` | no | Comma-separated subset to query, e.g. `azure,aws` (default: all configured) |
| `azure.tenant-id` | enables Azure | Azure AD tenant ID |
| `azure.client-id` | with Azure | App registration client ID |
| `azure.client-secret` | with Azure | App registration client secret (sensitive) |
| `azure.subscription-ids` | with Azure | Comma-separated subscription IDs |
| `gcp.credentials-path` | enables GCP | Path to a service-account JSON key file |
| `gcp.project-ids` | with GCP | Comma-separated project IDs |
| `aws.access-key-id` | enables AWS | AWS access key ID |
| `aws.secret-access-key` | with AWS | AWS secret access key (sensitive) |
| `aws.account-ids` | with AWS | Comma-separated account IDs |
| `aws.region` | with AWS | e.g. `us-east-1` |
| `aws.role-arn` | no | IAM role ARN to assume for cross-account access |
| `cache.enabled` | no | Enable the adapter result cache (adapter default: `true`) |
| `cache.ttl-minutes` | no | Result-cache TTL in minutes (adapter default: `5`) |
| `cache.debug-mode` | no | Adapter cache debug logging (adapter default: `false`) |

The connector resolves credentials at the connector layer and passes them **explicitly** into the
inline model. Secrets and list values are URL-encoded into the `jdbc:cloudops:` URL and decoded by
the driver, so commas, `=` and `+` in values round-trip safely.

> The CloudOps adapter also honours provider environment variables (`AWS_ACCESS_KEY_ID`,
> `AZURE_TENANT_ID`, …) as a fallback when a property is absent. Prefer explicit catalog properties;
> the env fallback is the adapter's own behaviour, not the connector's.

## Build & test

The Trino connectors target **JDK 25** (Trino 481). Provide a JDK 25 toolchain:

```bash
./gradlew :trino-cloudops:test -Porg.gradle.java.installations.paths=/path/to/jdk-25
./gradlew :trino-cloudops:trinoPlugin   # builds the plugin-directory archive
```

The unit tests (`TestCloudOpsClientModule`) cover URL construction (encoding round-trips, list
values, omitted defaults) and the fail-fast validation. There is no in-process end-to-end test: it
would require live Azure/AWS/GCP credentials.
