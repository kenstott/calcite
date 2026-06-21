# Trino Splunk Connector

A [Trino](https://trino.io) connector that exposes Splunk (its CIM data models, indexes and custom
tables) as a SQL catalog. It is a thin wrapper over the generic
[trino-calcite](../trino-calcite) connector: it reuses `CalciteClient` for type mapping and only
swaps in the Splunk JDBC driver (`org.apache.calcite.adapter.splunk.SplunkDriver`) plus friendly
catalog properties, so users never write a raw JDBC URL.

## Requirements

Like any Trino plugin, it must be built against the exact SPI version of the target server — the
version pinned by `trino.version` in the repo's `gradle.properties` (currently **481**, **Java 25**).

## Download

Prebuilt plugin archives are attached to each `trino-v*` GitHub release. Grab
`trino-splunk-plugin-<version>.zip` from the
[Releases page](https://github.com/kenstott/calcite/releases), or with the GitHub CLI:

```bash
gh release download <trino-vX.Y.Z> --repo kenstott/calcite --pattern 'trino-splunk-plugin-*.zip'
```

The `Trino Connectors` workflow also publishes the archives as a `trino-connector-packages`
build artifact on each run.

## Build & install

Install the downloaded archive (or build from source) into the Trino server's `plugin/` directory,
then restart Trino:

```bash
# build from source (optional)
./gradlew :trino-splunk:trinoPlugin
# install
unzip trino-splunk/build/distributions/trino-splunk-plugin-*.zip -d "$TRINO_HOME/plugin/"
# yields $TRINO_HOME/plugin/trino-splunk/<jars>; restart Trino
```

## Configure a catalog

Create `etc/catalog/splunk.properties`:

```properties
connector.name=splunk
url=https://localhost:8089
# Authenticate with a token (preferred) ...
token=eyJhbGciOiJIUzI1NiI...
# ... or with user + password:
# user=admin
# password=changeme
# Optional: app context + data-model discovery filter
app=Splunk_SA_CIM
datamodel-filter=Authentication
# Required: Splunk names are generated lower-case but Calcite reports upper-case storage;
# the connector fails fast at startup without this. Keep it on.
case-insensitive-name-matching=true
```

| Property | Description | Required |
|----------|-------------|----------|
| `url` | Splunk management URL, e.g. `https://localhost:8089` | Yes |
| `case-insensitive-name-matching` | Must be `true` — see note above | Yes |
| `token` | Splunk auth token (preferred) | Conditional |
| `user` / `password` | Username + password (when not using a token) | Conditional |
| `app` | App context for data-model discovery (e.g. `Splunk_SA_CIM`) | No |
| `datamodel-filter` | Data-model discovery filter (name, glob, or `/regex/`) | No |
| `disable-ssl-validation` | Disable TLS validation (test/self-signed only) | No |

These map onto a `jdbc:splunk:url=…;token=…` URL for `SplunkDriver`; see the
[splunk adapter](../splunk) for the full set of supported connection parameters (CIM models, custom
tables, cache control, environment-variable fallback, etc.).

## Example queries

```sql
SHOW TABLES FROM splunk.splunk;
SELECT _time, action, src_ip FROM splunk.splunk.authentication
WHERE _time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

## Behaviour and limitations

- **Pushdown.** Same as [trino-calcite](../trino-calcite) (it *is* that connector under the hood):
  Trino pushes projection and simple predicate filters down to the Calcite JDBC source. How those
  are executed below Calcite — including the Splunk adapter's own search pushdown — is internal to
  that adapter, not the connector.
- Authentication, data-model discovery and SSL handling are owned by the underlying Splunk adapter.

## Testing

A live Splunk instance (or recorded backend) is required for an end-to-end test; tag such tests as
integration-only. The generic Trino↔Calcite path is covered by
[trino-calcite](../trino-calcite)'s in-process test.
