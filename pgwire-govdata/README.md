# pgwire-govdata

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes the **GovData adapter**
(`GovDataSchemaFactory`) over the PostgreSQL wire protocol. Query ~20 US government datasets — SEC
filings, census, economic indicators, crime, weather, patents, and more — from `psql` or any
PostgreSQL client. It is the same catalog as the [AskAmerica engine](../askamerica-engine), served
over pgwire instead of JDBC.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-govdata-<version>-<os>.tar.gz` (bundled CPython + JRE + the `govdata`
module's runtime jars + this model).

## Schemas

Each entry in [model.json](model.json) mounts one GovData source as a PostgreSQL schema. `sec` is the
default schema. The bundled set:

`sec`, `geo`, `econ`, `econ_reference`, `census`, `crime`, `weather`, `fec`, `fedregister`,
`cyber_vuln`, `cyber_threat`, `energy`, `health`, `edu`, `lands`, `patents`, `disasters`, `housing`,
`cftc`, `ag`, `ref`, `transport`, `environment`, `fiscal`.

All schemas share one DuckDB catalog (`database_filename`) so cross-schema joins/views resolve. To
serve a subset, delete the schema entries you don't need from `model/model.json` in the extracted
bundle.

## Configure

The data lives in an S3-compatible object store (the GovData parquet warehouse). Point the bundle at
it with environment variables — the `${VAR}` references in `model.json` are resolved at load
(env → system property → default):

| Variable | Purpose |
|----------|---------|
| `GOVDATA_PARQUET_DIR` | **Required.** Warehouse root, e.g. `s3://govdata/parquet/` |
| `AWS_ACCESS_KEY_ID` | Object-store access key |
| `AWS_SECRET_ACCESS_KEY` | Object-store secret key |
| `AWS_ENDPOINT_OVERRIDE` | S3 endpoint (e.g. an R2/MinIO URL); omit for AWS S3 |
| `AWS_REGION` | Region (default `auto`) |
| `GOVDATA_DUCKDB_CATALOG` | Shared DuckDB catalog path (default `govdata-catalog.duckdb`) |

`autoDownload` is `false`: this bundle **reads** the pre-built warehouse; it does not run ETL. Remote
parquet reads are cached to a local DuckDB httpfs cache that survives restarts.

## Run

```bash
tar -xzf pgwire-govdata-<version>-<os>.tar.gz
cd pgwire-govdata-<version>-<os>
export GOVDATA_PARQUET_DIR='s3://govdata/parquet/'
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_ENDPOINT_OVERRIDE=...
./bin/pgwire-govdata                 # serves on 127.0.0.1:5433
./bin/pgwire-govdata --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -d govdata -c 'SELECT cik, company_name FROM sec.filing_metadata LIMIT 5'
```

The database is named **`govdata`** (`current_database()`, the JDBC URL, and the DataGrip/DBeaver
database node); the `sec`, `econ`, `census`, … sources are schemas under it. The wrapper passes
`--database govdata` by default — override with `--database <name>` if you need a different label.

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight through to
`pgwire-calcite`. There is no port env var — set it on the command line.

No Python or Java install required — the bundle is airgap-ready.
