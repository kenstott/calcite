# pgwire-sharepoint

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes **SharePoint Lists**
(`SharePointListSchemaFactory`) over the PostgreSQL wire protocol. Query SharePoint lists as SQL
tables from `psql` or any PostgreSQL client.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-sharepoint-<version>-<os>.tar.gz` (bundled CPython + JRE + the
`sharepoint-list` module's runtime jars + this model).

## Configure

Edit `model/model.json` in the extracted bundle:

- `siteUrl` — your SharePoint site, e.g. `https://contoso.sharepoint.com/sites/Team`.
- Azure AD **app-only** auth (client credentials): `tenantId`, `clientId`, `clientSecret`.
- The factory forwards the full operand map, so other supported auth patterns (certificate /
  thumbprint, device code, …) work by supplying their respective operands instead.

## Run

```bash
tar -xzf pgwire-sharepoint-<version>-<os>.tar.gz
cd pgwire-sharepoint-<version>-<os>
./bin/pgwire-sharepoint            # serves on 127.0.0.1:5433
./bin/pgwire-sharepoint --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -c 'SELECT * FROM sharepoint."<list>" LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight
through to `pgwire-calcite`. There is no port env var — set it on the command line.

## Writes

The bundle's launcher starts the server with `--allow-writes`, so `INSERT`, `UPDATE` and `DELETE`
against a list work for the columns the app registration may write. Each statement is sent to
SharePoint and committed when it runs. `BEGIN` / `COMMIT` are accepted, but a `ROLLBACK` after a
write is refused with an error, because the write cannot be undone. `RETURNING` is not supported.

No Python or Java install required — the bundle is airgap-ready.
