# Trino SharePoint Connector

A [Trino](https://trino.io) connector that exposes SharePoint Lists, document libraries, calendars
and task lists as a SQL catalog. It is a thin wrapper over the generic
[trino-calcite](../trino-calcite) connector: it reuses `CalciteClient` for type mapping and swaps in
the SharePoint JDBC driver (`org.apache.calcite.adapter.sharepoint.SharePointListDriver`), exposing
friendly catalog properties so you never write a raw JDBC URL.

## Requirements

A Trino plugin must be built against the exact SPI version of the target server — the version pinned
by `trino.version` in the repo's `gradle.properties` (currently **481**, which requires **Java 25**).

## Download

Prebuilt plugin archives are attached to each `trino-v*` GitHub release. Grab
`trino-sharepoint-plugin-<version>.zip` from the
[Releases page](https://github.com/kenstott/calcite/releases), or with the GitHub CLI:

```bash
gh release download <trino-vX.Y.Z> --repo kenstott/calcite --pattern 'trino-sharepoint-plugin-*.zip'
```

The `Trino Connectors` workflow also publishes the archives as a `trino-connector-packages`
build artifact on each run.

## Build & install

Install the downloaded archive (or build from source) into the Trino server's `plugin/` directory,
then restart Trino:

```bash
# build from source (optional)
./gradlew :trino-sharepoint:trinoPlugin
# install
unzip trino-sharepoint/build/distributions/trino-sharepoint-plugin-*.zip -d "$TRINO_HOME/plugin/"
# yields $TRINO_HOME/plugin/trino-sharepoint/<jars>; restart Trino
```

## Configure a catalog

Create `etc/catalog/sharepoint.properties`. The required properties are always `connector.name`,
`site-url`, `auth-type`, and `case-insensitive-name-matching=true`; the remaining properties depend
on the auth type.

### Certificate auth (recommended for automation)

```properties
connector.name=sharepoint
site-url=https://contoso.sharepoint.com/sites/mysite
auth-type=CERTIFICATE
client-id=<azure-ad-app-client-id>
tenant-id=<azure-ad-tenant-id>
certificate-path=/etc/trino/sharepoint.pfx
certificate-password=<pfx-password>
case-insensitive-name-matching=true
```

### Client-secret auth

```properties
connector.name=sharepoint
site-url=https://contoso.sharepoint.com/sites/mysite
auth-type=CLIENT_CREDENTIALS
client-id=<azure-ad-app-client-id>
client-secret=<client-secret>
tenant-id=<azure-ad-tenant-id>
case-insensitive-name-matching=true
```

### All properties

| Property | Description | Required |
|----------|-------------|----------|
| `connector.name` | Always `sharepoint` | Yes |
| `site-url` | Full SharePoint site URL | Yes |
| `auth-type` | `CERTIFICATE`, `CLIENT_CREDENTIALS`, `USERNAME_PASSWORD`, `DEVICE_CODE`, `MANAGED_IDENTITY` | Yes |
| `case-insensitive-name-matching` | Must be `true` — see note below | Yes |
| `client-id` | Azure AD app registration client ID | Yes |
| `tenant-id` | Azure AD tenant ID | Yes |
| `certificate-path` | Path (inside the Trino server) to the PKCS#12 `.pfx` | `CERTIFICATE` |
| `certificate-password` | Password for the `.pfx` | `CERTIFICATE` |
| `thumbprint` | Cert SHA-1 thumbprint; omit to auto-compute from the `.pfx` | No |
| `client-secret` | Client secret | `CLIENT_CREDENTIALS` |
| `user` / `password` | UPN + password | `USERNAME_PASSWORD` |
| `schema` | Schema name for the lists (default `sharepoint`) | No |

> **`case-insensitive-name-matching=true` is required.** Calcite matches identifiers
> case-sensitively, but its Avatica metadata reports `storesUpperCaseIdentifiers()=true`; without
> this flag Trino upper-cases names and finds no tables.

The catalog name is the properties filename (`sharepoint.properties` → catalog `sharepoint`); the
schema defaults to `sharepoint`. So tables are addressed as `sharepoint.sharepoint.<list>`, or
`sharepoint.<schema>.<list>` if you set `schema`.

### Azure AD app registration

- API permission **`Sites.ReadWrite.All` (Application)** on Microsoft Graph, with **admin consent**.
- For `CERTIFICATE` auth, upload the **public** certificate to the app registration under
  **Certificates & secrets → Certificates**. See the
  [sharepoint-list adapter README](../sharepoint-list/README.md) for generating the `.pfx`.

## Example queries

```sql
SHOW SCHEMAS FROM sharepoint;
SHOW TABLES FROM sharepoint.sharepoint;
SELECT title, start_time, end_time FROM sharepoint.sharepoint.calendar;
SELECT title, status, due_date FROM sharepoint.sharepoint.project_tasks WHERE status = 'In Progress';
```

## Behaviour and limitations

- **Discovery is automatic.** Every non-hidden list, document library, calendar and task list in the
  site is exposed as a table — no list names to configure. Names are normalised to lower-case
  `snake_case`. Results are cached per site (default 5 minutes) so repeated queries don't re-crawl
  the site on every connection.
- **Column types.** SharePoint text/choice/lookup/user map to `VARCHAR`, numbers to `DOUBLE`,
  integers to `INTEGER`, booleans to `BOOLEAN`, datetimes to `TIMESTAMP`. Complex column values
  (recurrence, location, etc.) are returned as **JSON strings**.
- **Writes are gated by list type.** Generic lists, task lists and calendars accept
  `INSERT`/`UPDATE`/`DELETE`; document/picture libraries, surveys and discussion boards are
  **read-only** (their rows aren't plain list items).
- **Pushdown.** As with [trino-calcite](../trino-calcite), Trino pushes projection and simple
  predicate filters down to the Calcite JDBC source; what happens below Calcite is the adapter's
  concern, not the connector's.

## Quick local test with Docker

```bash
# Build the plugin and stage it
./gradlew :trino-sharepoint:trinoPlugin
unzip -o trino-sharepoint/build/distributions/trino-sharepoint-plugin-*.zip -d ./plugin
# Run Trino with the plugin, catalog and cert mounted (or bake them into an image)
docker run -d --name trino -p 8080:8080 \
  -v "$PWD/plugin/trino-sharepoint:/usr/lib/trino/plugin/sharepoint" \
  -v "$PWD/sharepoint.properties:/etc/trino/catalog/sharepoint.properties" \
  -v "$PWD/sharepoint.pfx:/etc/trino/sharepoint.pfx" \
  trinodb/trino:481
docker exec trino trino --execute "SHOW TABLES FROM sharepoint.sharepoint"
```

> On Windows, bind-mount path translation can be flaky; baking the files into a small image
> (`FROM trinodb/trino:481` + `COPY`) is more reliable.

This connector was verified end-to-end this way (certificate auth, full list discovery, calendar
reads). The generic Trino↔Calcite path also has an in-process test in
[trino-calcite](../trino-calcite).
