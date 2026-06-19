# Trino Calcite Connector

A generic [Trino](https://trino.io) connector that exposes any Apache Calcite model — and therefore
any Calcite adapter (File, GovData, SharePoint, …) — to Trino as a SQL catalog. It is built on
Trino's `trino-base-jdbc` framework and wraps the generic Calcite JDBC driver
(`org.apache.calcite.jdbc.Driver`, `jdbc:calcite:`).

## Requirements

A Trino plugin must be built against the **exact** SPI version of the server it is deployed into.
This module targets the version pinned by `trino.version` in the repo's `gradle.properties`
(currently **481**, which requires **Java 25**). The module builds with its own Java 25 toolchain;
the Calcite driver it bundles is Java 8 bytecode and runs unchanged on the Trino JVM.

## Download

Prebuilt plugin archives are attached to each `trino-v*` GitHub release. Grab
`trino-calcite-plugin-<version>.zip` from the
[Releases page](https://github.com/kenstott/calcite/releases), or with the GitHub CLI:

```bash
gh release download <trino-vX.Y.Z> --repo kenstott/calcite --pattern 'trino-calcite-plugin-*.zip'
```

The `Trino Connectors` workflow also publishes the archives as a `trino-connector-packages`
build artifact on each run.

## Build

Or build the archive from source:

```bash
./gradlew :trino-calcite:trinoPlugin
# Output: trino-calcite/build/distributions/trino-calcite-plugin-*.zip
```

The archive is a Trino **plugin directory** (a directory of jars, not a fat jar): `trino-spi` and
the other artifacts Trino provides to plugins parent-first are excluded; the Calcite driver and
`trino-base-jdbc` are bundled.

## Install

Unzip the archive into the Trino server's `plugin/` directory:

```bash
unzip trino-calcite-plugin-*.zip -d "$TRINO_HOME/plugin/"
# yields $TRINO_HOME/plugin/trino-calcite/<jars>
```

## Configure a catalog

Create `etc/catalog/<name>.properties`:

```properties
connector.name=calcite
connection-url=jdbc:calcite:model=/etc/trino/models/example.json
# Required: Calcite matches identifiers case-sensitively, but its Avatica metadata reports
# storesUpperCaseIdentifiers()=true. Without this, Trino upper-cases names and finds nothing.
case-insensitive-name-matching=true
```

`connection-url` is any `jdbc:calcite:` URL. An inline model also works:
`connection-url=jdbc:calcite:model=inline:{...}`.

Name your Calcite schemas, tables and columns in **lower case** — Trino folds unquoted identifiers
to lower case.

## Example queries

```sql
SHOW SCHEMAS FROM calcite;
SHOW TABLES FROM calcite.test;
SELECT id, name, amount FROM calcite.test.events WHERE active = true;
```

## Behaviour and limitations

- **Pushdown.** Built on `trino-base-jdbc`: Trino pushes projection and simple predicate (domain)
  filters down to the Calcite JDBC source as SQL, so Trino receives only the rows it asked for.
  Aggregations and joins are executed by Trino. What Calcite does with that SQL — and how far it
  pushes toward its own backing sources — is internal to the model and not the connector's concern.
- **Type mapping** ([CalciteClient.java](src/main/java/org/apache/calcite/adapter/trino/CalciteClient.java))
  covers the standard `java.sql.Types` Calcite emits (boolean, integer family, real/double,
  decimal, char/varchar, varbinary, date, time, timestamp). Unmapped remote types are skipped; set
  `unsupported-type-handling=CONVERT_TO_VARCHAR` on the catalog to surface them as varchar instead.

## Testing

```bash
./gradlew :trino-calcite:test -PincludeTags=integration
```

[TestCalciteConnector](src/test/java/org/apache/calcite/adapter/trino/TestCalciteConnector.java)
spins up an in-process Trino cluster (`trino-testing`) over an in-memory Calcite model and asserts
on `SHOW TABLES`, projection, filtering, aggregation, and that the predicate + projection are pushed
down into the Calcite JDBC scan (no Trino-side `Filter` node).
