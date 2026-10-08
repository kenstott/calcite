# pgwire-servicenow

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes a **ServiceNow** instance's
tables (`ServiceNowSchemaFactory`) over the PostgreSQL wire protocol. Query ServiceNow from `psql`
or any PostgreSQL client. **Read-only.**

**Status: not tested against a ServiceNow instance.** The adapter's unit tests run against a local
server that plays the documented Table API; no live instance was available when this bundle was
written. See the [adapter README](../servicenow/README.md) for what is and is not verified. This
bundle is not yet in the release workflow's matrix.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-servicenow-<version>-<os>.tar.gz` (bundled CPython + JRE + the
`servicenow` module's runtime jars + this model).

## Configure

Edit `model/model.json` in the extracted bundle:

- `instanceUrl` — `https://<instance>.service-now.com`.
- `authType` — `basic`; `username` / `password` of a dedicated integration user. Every query runs
  with that user's roles and ACLs. Give it read access to the tables you expose and to the
  metadata tables `sys_db_object`, `sys_dictionary` and `sys_glide_object` (for example the
  `personalize_dictionary` role). Without that, startup fails and names the table it could not
  read; the adapter never guesses columns from sample rows. Do not use an administrator account.
- `tables` — comma-separated names of the tables to expose. Leave it out to expose every table in
  `sys_db_object`, which on a real instance is thousands and makes the first start slow.

Other operands (`pageSize`, `maxConcurrentRequests`, `maxRetries`, `maxRetryWaitSeconds`,
`catalogCacheDirectory`, `catalogCacheTtlMinutes`, `excludeColumnTypes`) are described in the
[adapter README](../servicenow/README.md).

## Startup time and caching

Before it listens, the server reads the instance's metadata tables once and keeps the result in
`catalogCacheDirectory` (default `~/.calcite/servicenow/catalog-cache`) for
`catalogCacheTtlMinutes` (default `1440`; `0` keeps nothing on disk). The pgwire catalog is also
cached next to the model as `model/catalog-cache-<hash>.pkl`; to pick up tables or fields added in
ServiceNow, delete that file and the cache directory, then restart.

Each table is one SQL table in the schema `servicenow`, named as in ServiceNow (lower case), with
inherited columns included. A reference field `caller_id` is two columns: `caller_id` (the
referenced record's sys_id) and `caller_id__display` (its display value, read only when selected).

## Run

```bash
tar -xzf pgwire-servicenow-<version>-<os>.tar.gz
cd pgwire-servicenow-<version>-<os>
./bin/pgwire-servicenow               # serves on 127.0.0.1:5433
psql -h 127.0.0.1 -p 5433 -c 'SELECT number, short_description FROM servicenow.incident LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight through
to `pgwire-calcite`.

## Filters are evaluated locally until a pushdown entry is verified

ServiceNow silently drops a query term it cannot parse and returns more rows than asked for, so a
`WHERE` clause is sent to ServiceNow only for the filter shapes a live harness run has verified
(see the [adapter README](../servicenow/README.md#filter-pushdown-off-until-verified)). The bundled
verification record is empty, so by default every filter is applied by Calcite over the rows
ServiceNow returns, and `SELECT ... WHERE` on a large table reads the whole table. To enable
pushdown, set `pushdownVerification` (a record written by the harness) or `trustPushdown` in
`model.json`. Selected columns are pushed down, and a `LIMIT` stops reading early. An empty value
is NULL, for strings too.

## Writes

There are none: `INSERT`, `UPDATE` and `DELETE` are not supported, and the launcher is started
without `--allow-writes`.
