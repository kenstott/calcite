# Trino SharePoint Connector

A [Trino](https://trino.io) connector that exposes SharePoint Lists as a SQL catalog. It is a thin
wrapper over the generic [trino-calcite](../trino-calcite) connector: it reuses `CalciteClient` for
type mapping and only swaps in the SharePoint JDBC driver
(`org.apache.calcite.adapter.sharepoint.SharePointListDriver`) plus friendly catalog properties, so
users never write a raw JDBC URL.

## Requirements

Like any Trino plugin, it must be built against the exact SPI version of the target server — the
version pinned by `trino.version` in the repo's `gradle.properties` (currently **481**, **Java 25**).

## Build & install

```bash
./gradlew :trino-sharepoint:trinoPlugin
unzip trino-sharepoint/build/distributions/trino-sharepoint-plugin-*.zip -d "$TRINO_HOME/plugin/"
```

## Configure a catalog

Create `etc/catalog/sharepoint.properties`:

```properties
connector.name=sharepoint
site-url=https://contoso.sharepoint.com/sites/mysite
auth-type=CLIENT_CREDENTIALS
client-id=<azure-ad-app-client-id>
client-secret=<client-secret>
tenant-id=<azure-ad-tenant-id>
# SharePoint list/column names are matched case-sensitively by Calcite; keep this on.
case-insensitive-name-matching=true
```

| Property | Description | Required |
|----------|-------------|----------|
| `site-url` | Full SharePoint site URL | Yes |
| `auth-type` | `CLIENT_CREDENTIALS`, `USERNAME_PASSWORD`, `DEVICE_CODE`, `MANAGED_IDENTITY`, `CERTIFICATE` | Yes |
| `client-id` | Azure AD app registration client ID | Yes |
| `tenant-id` | Azure AD tenant ID | Yes |
| `client-secret` | Client secret (`CLIENT_CREDENTIALS`) | Conditional |
| `user` / `password` | UPN + password (`USERNAME_PASSWORD`) | Conditional |

These map onto a `jdbc:sharepoint:…` URL for `SharePointListDriver`; see the
[sharepoint-list adapter](../sharepoint-list/README.md) for Azure AD app-registration details.

## Example queries

```sql
SHOW TABLES FROM sharepoint.sharepoint;
SELECT title, status, due_date FROM sharepoint.sharepoint.project_tasks WHERE status = 'In Progress';
```

SharePoint list and column names are normalised to lower-case `snake_case` by the adapter
(`SharePointNameConverter`).

## Behaviour and limitations

- **Pushdown.** Same as [trino-calcite](../trino-calcite) (it *is* that connector under the hood):
  Trino pushes projection and simple predicate filters down to the Calcite JDBC source. How those
  are executed below Calcite is internal to the SharePoint adapter, not the connector.
- Authentication and API selection (Microsoft Graph vs SharePoint REST) are handled by the
  underlying SharePoint adapter.

## Testing

A live SharePoint site (or recorded backend) is required for an end-to-end test; tag such tests as
integration-only. The generic Trino↔Calcite path is covered by
[trino-calcite](../trino-calcite)'s in-process test.
