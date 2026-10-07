# pgwire-salesforce

A pre-packaged [pgwire-calcite](../pgwire-calcite) server that exposes a **Salesforce** org's
sObjects (`SalesforceSchemaFactory`) over the PostgreSQL wire protocol. Query Salesforce from
`psql` or any PostgreSQL client.

This is a thin project: it contributes only the adapter [model.json](model.json); the server, the
airgap bundling, and the per-OS launcher come from `pgwire-calcite`. The release builds a
self-contained tarball `pgwire-salesforce-<version>-<os>.tar.gz` (bundled CPython + JRE + the
`salesforce` module's runtime jars + this model).

## Configure

Edit `model/model.json` in the extracted bundle. The default model uses the OAuth **client
credentials** flow:

- `loginUrl` — your org's My Domain URL, e.g. `https://acme.my.salesforce.com`
  (Setup → My Domain). `login.salesforce.com` does not work for this flow.
- `clientId` / `clientSecret` — the connected app's consumer key and secret. The app needs
  *Enable Client Credentials Flow* checked and a *Run As* user set under its OAuth policies;
  every query runs with that user's permissions.

Other auth options, replacing `clientId`/`clientSecret` in the operand:

- `username` + `password` (+ `securityToken`) with `clientId` + `clientSecret` — the OAuth
  username-password flow. Orgs created since Summer '23 block it by default.
- `accessToken` + `instanceUrl` — a pre-issued token.

Optional: `apiVersion` (default `v58.0`), `cacheMaxSize` (describe cache entries held in memory,
default 1000), `describeCacheDirectory` and `describeCacheTtlMinutes` (see below).

## Startup time and caching

Before it listens, the server reads the columns of every sObject, which is one Salesforce
describe call per sObject — about four minutes for an org with 1,200 of them. That is paid once:

- The finished catalog is written next to the model as `model/catalog-cache-<hash>.pkl` and
  loaded on every later start. The hash is of `model.json`, so editing the model rebuilds it.
- Each describe result is also kept in `describeCacheDirectory` (default
  `~/.calcite/salesforce/describe-cache`) for `describeCacheTtlMinutes` (default `1440`; `0` keeps
  nothing on disk), so a rebuild after a model edit reads them from disk instead of Salesforce.

To pick up fields or sObjects added in Salesforce, delete `model/catalog-cache-*.pkl` and the
describe cache directory, then restart.

Every queryable sObject is a table (`Account`, `Contact`, `Opportunity`, custom `*__c` objects);
columns and types come from the sObject's describe. Filters, projections, sorts and limits are
pushed down as SOQL; joins and aggregates run in Calcite.

## Run

```bash
tar -xzf pgwire-salesforce-<version>-<os>.tar.gz
cd pgwire-salesforce-<version>-<os>
./bin/pgwire-salesforce               # serves on 127.0.0.1:5433
./bin/pgwire-salesforce --host 0.0.0.0 --port 5455   # alternate bind/port
psql -h 127.0.0.1 -p 5433 -c 'SELECT "Name", "Industry" FROM salesforce."Account" LIMIT 10'
```

`--host` / `--port` (default `127.0.0.1:5433`) and any other launcher flags pass straight
through to `pgwire-calcite`. There is no port env var — set it on the command line.

## Writes

The bundle's launcher starts the server with `--allow-writes`, so `INSERT`, `UPDATE` and `DELETE`
work against any sObject field the connected app's *Run As* user may create or update:

```sql
INSERT INTO salesforce."Account" ("Name", "Industry") VALUES ('Acme', 'Energy');
UPDATE salesforce."Account" SET "Industry" = 'Banking' WHERE "Name" = 'Acme';
DELETE FROM salesforce."Account" WHERE "Name" = 'Acme';
```

Each statement is sent to Salesforce and committed when it runs. `BEGIN` / `COMMIT` are accepted,
but a `ROLLBACK` after a write is refused with an error, because the write cannot be undone.

`RETURNING` works on all three, which is how to get the Id Salesforce assigns:

```sql
INSERT INTO salesforce."Account" ("Name") VALUES ('Acme') RETURNING "Id", "Name";
```

No Python or Java install required — the bundle is airgap-ready.
