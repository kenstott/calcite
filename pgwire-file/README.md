# pgwire-file

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes the Calcite **file adapter**
(`FileSchemaFactory`) over the PostgreSQL wire protocol. Query CSV / JSON / Parquet files — local or
on S3 — from `psql` or any PostgreSQL client.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-file-<version>-<os>.tar.gz` (bundled CPython + JRE + the `file`
module's runtime jars + this model).

## Configure

Edit `model/model.json` in the extracted bundle:

- `directory` — the folder to expose as tables (default `data`, relative to the bundle). Put your
  files there, or point it at an absolute path.
- `executionEngine` — `duckdb` (default, fast, bundled), `parquet`, `arrow`, or `linq4j`.
- **S3 / remote:** set `"storageType": "s3"` and a `storageConfig` block; standard `AWS_*` env vars
  are honored. See the file adapter docs for the full operand set (`glob`, `recursive`, `refreshInterval`, …).

## Run

```bash
tar -xzf pgwire-file-<version>-<os>.tar.gz
cd pgwire-file-<version>-<os>
./bin/pgwire-file                 # serves on 127.0.0.1:5433
./bin/pgwire-file --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -c 'SELECT * FROM files."<table>" LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight
through to `pgwire-calcite`. There is no port env var — set it on the command line.

No Python or Java install required — the bundle is airgap-ready.
