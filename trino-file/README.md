# trino-file

A Trino connector that exposes the Calcite **file adapter** as a SQL catalog. One catalog serves
files (CSV, JSON, Parquet, Excel, Markdown, …) discovered under a single mount — a local directory
or a remote glob.

It mirrors [`trino-sharepoint`](../trino-sharepoint): a thin wrapper that reuses
[`trino-calcite-base`](../trino-calcite-base)'s `CalciteClient` for JDBC→Trino type mapping.
(Depending on the SPI-less base rather than the `trino-calcite` plugin keeps the generic `calcite`
connector out of this plugin's zip, so only `file` is registered.) The friendly catalog properties
are assembled into an inline Calcite model (`jdbc:calcite:model=inline:{…}`) that instantiates one
`FileSchemaFactory` schema.

## Configuration

```properties
connector.name=file
glob=s3://my-bucket/data/**/*.parquet
```

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `glob` | yes | — | A local path or a remote URI glob (see Storage backends) |
| `schema-name` | no | `files` | Trino schema name exposed |
| `execution-engine` | no | `DUCKDB` | File-adapter engine: `DUCKDB`, `PARQUET`, `LINQ4J`, `ARROW`. DuckDB reads CSV/Parquet natively without Hadoop; PARQUET converts via Hadoop (incompatible with JDK 25 — see below) |
| `recursive` | no | `true` | Scan subdirectories |
| `refresh-interval` | no | — | How often the adapter re-reads data, e.g. `5 minutes` or `PT5M` |
| `catalog-refresh-interval` | no | = `refresh-interval` | How long Trino caches the table list before re-scanning for new files/sheets (maps to `metadata.cache-ttl`) |

One mount per catalog. For multiple mounts, configure multiple catalogs.

## Storage backends

The `storageType` is derived from the URI scheme; a bare path is local.

| Scheme | Credentials |
|--------|-------------|
| local path | — |
| `s3://` | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` (or `AWS_DEFAULT_REGION`) / `AWS_ENDPOINT_URL`, each overridable via `aws.access-key`, `aws.secret-key`, `aws.region`, `aws.endpoint`. Fails fast for an `s3://` glob with no resolvable access/secret key. |
| `hdfs://` | Ambient Hadoop configuration |
| `ftp://` / `ftps://` | In the URI: `ftp://user:pass@host/path` |
| `sftp://` | URI, or `sftp.username` / `sftp.password` / `sftp.private-key-path` / `sftp.strict-host-key-checking` |
| `http://` / `https://` | — |

S3 credentials are resolved at the connector layer and passed **explicitly** into the adapter's
`storageConfig`; the file adapter never reads the environment itself.

> AWS **session tokens** (`AWS_SESSION_TOKEN`) are not yet wired — `S3StorageProvider` uses
> `BasicAWSCredentials`. Temporary credentials would require a small file-adapter change.

## Refresh & catalog freshness

- **Data** is refreshed by the file adapter per `refresh-interval` (lazy on query for CSV/JSON; a
  background scheduler for DuckDB).
- **The table list** (new files appearing, or a new sheet in an `.xlsx`) is refreshed when Trino
  re-scans the mount. Each metadata lookup opens a fresh Calcite connection that re-parses the
  inline model and re-discovers tables; `catalog-refresh-interval` (i.e. `metadata.cache-ttl`)
  governs how often that happens. The default `0s` means the catalog is always live.

- **Multi-table source files** (`.xlsx`, `.md`, `.docx`, `.html`, `.xml`, `.pptx`) are converted to
  per-table JSON. When `refresh-interval` is set, `FileSchema` registers each local source file with
  the `ConversionFileWatcher`, which re-runs the conversion in the background at that interval when
  the file changes — so the single `refresh-interval` drives both data refresh and conversion
  re-runs. (Watching is local-file based; remote globs convert at scan time.)

## Build & test

The Trino connectors target **JDK 25** (Trino 481). Provide a JDK 25 toolchain:

```bash
./gradlew :trino-file:test -Porg.gradle.java.installations.paths=/path/to/jdk-25
./gradlew :trino-file:trinoPlugin   # builds the plugin-directory archive
```

Under JDK 25 the file adapter's optional `ParquetStatisticsExtractor` (stats / cache priming) still
touches Hadoop and logs `getSubject is not supported` — these are **non-fatal**: DuckDB serves the
query regardless. Only the CSV→Parquet *conversion* path is hard-blocked by this, which is why the
DuckDB-backed connector uses native Parquet/CSV reads instead.

The E2E test (`TestFileConnector`) uses the default `DUCKDB` engine over a Parquet fixture. The
`PARQUET` engine converts via Hadoop, whose `UserGroupInformation` is incompatible with JDK 25
(`getSubject is not supported`; the SecurityManager is removed, so `-Djava.security.manager=allow`
is rejected at VM init). The test JVM also adds `--add-modules=jdk.incubator.vector`, required by
Trino's block-encoding SIMD support.
