# pgwire-splunk

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes **Splunk** data models
(`SplunkSchemaFactory`) over the PostgreSQL wire protocol. Query Splunk from `psql` or any
PostgreSQL client.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-splunk-<version>-<os>.tar.gz` (bundled CPython + JRE + the `splunk`
module's runtime jars + this model).

## Configure

Edit `model/model.json` in the extracted bundle:

- `url` — your Splunk management endpoint, e.g. `https://splunk.example.com:8089`
  (or supply `host` + `port` + `protocol` separately).
- Auth: `token` (preferred), or `username` + `password`.
- `disableSslValidation` — set `true` only for self-signed dev servers.
- Optional: `app` (app context), `datamodelFilter`, `datamodelCacheTtl`.

## Run

```bash
tar -xzf pgwire-splunk-<version>-<os>.tar.gz
cd pgwire-splunk-<version>-<os>
./bin/pgwire-splunk               # serves on 127.0.0.1:5433
./bin/pgwire-splunk --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -c 'SELECT * FROM splunk."<datamodel>" LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight
through to `pgwire-calcite`. There is no port env var — set it on the command line.

No Python or Java install required — the bundle is airgap-ready.
