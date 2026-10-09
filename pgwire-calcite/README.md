# pgwire-calcite

A PostgreSQL wire-protocol server backed by Apache Calcite. It exposes Calcite's
planner and adapters over the PG wire protocol so DuckDB (`ATTACH … TYPE
postgres`), DBeaver/DataGrip, and any libpq/JDBC client can query Calcite data.

- **Requirements & design:** [docs/pgwire-calcite-server-requirements.md](../docs/pgwire-calcite-server-requirements.md)
- **Phased plan:** [docs/pgwire-calcite-implementation-plan.md](../docs/pgwire-calcite-implementation-plan.md)

> **License:** this directory is a BSL-1.1 subtree, distinct from the Apache-2.0
> Calcite repo around it. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Status: Phase 0 (fork, harness, corpus baseline)

The provisa pgwire server + vendored buenavista codec are copied verbatim
(headers/canaries preserved) and the backend seam is swapped from Trino to a
Phase-0 `StubBackend`. The catalog intercept (`catalog.py`) and COPY/DDL handlers
are copied but **not yet wired** to Calcite — they land in Phases 2 and 4.

| Module | Role | Phase |
|--------|------|-------|
| `vendor/buenavista/` | wire codec (verbatim) | reused |
| `src/pgwire_calcite/server.py` | wire server (rewired seams) | 0 |
| `src/pgwire_calcite/backend.py` | execution seam — `StubBackend` → `CalciteBackend` | 0 → 1 |
| `src/pgwire_calcite/state.py` | server state + schema registry | 0 |
| `src/pgwire_calcite/launcher.py` | minimal launcher (replaces FastAPI `state`) | 0 |
| `src/pgwire_calcite/catalog.py` | pg_catalog intercept (copied, unwired) | 2 |
| `src/pgwire_calcite/binary_copy.py` | `COPY ... TO STDOUT` (binary/csv/text) | 4 |
| `tests/corpus/` | client regression corpus + harness | 0 (ongoing) |

## Develop

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv -e ./vendor/buenavista -e . pytest
.venv/bin/python -m pytest tests/unit -v

# Run the server (Phase 0 stub backend):
.venv/bin/python -m pgwire_calcite.launcher --host 127.0.0.1 --port 5455
psql "host=127.0.0.1 port=5455 user=tester dbname=postgres" -c "SELECT 1;"
```

## Row counts in `pg_class.reltuples`

Clients that attach the server as a database (DuckDB, the PostgreSQL scanner, Trino's
PostgreSQL connector) read `pg_catalog` first and use `reltuples` to plan. `--row-counts`
selects where it comes from:

- `count` (default): one `COUNT(*)` per table when the catalog is first built. Exact, but the
  first `pg_catalog` query waits for every table to be resolved and counted.
- `recorded`: the counts the adapter already holds, fetched in one call that resolves no table.
  For the file adapter that is a table's count as last read from its Iceberg metadata, else the
  `observedCoverage.rowCount` of its declaration; a view is 0. A table with nothing recorded is
  named in the log and reports -1. The pgwire-govdata bundle starts in this mode.
- `off`: every table reports -1.

-1 is PostgreSQL's "never analyzed" value since version 14.

## Rejecting unfiltered scans of large tables

A SELECT that scans a very large table with no partition filter holds the shared engine
connection for minutes while every other statement queues. Set `--max-unfiltered-scan-rows N`
and `--table-coverage-file FILE` to reject such a statement up front (SQLSTATE `54000`), with
an error naming the table and the partition columns to filter on. A `LIMIT` with no ordering,
grouping or aggregation is admitted, as is any table missing from the file. Off by default.

Generate the file from the govdata schemas' measured row counts:

    python3 scripts/export_table_coverage.py coverage.json ../govdata/src/main/resources/*/*-schema.yaml

## Writes

The server is read-only unless started with `--allow-writes`; without it an `INSERT`, `UPDATE`
or `DELETE` is refused with SQLSTATE `25006`. With it, the statement is run through Calcite and
answered with PostgreSQL's command tag (`INSERT 0 n`, `UPDATE n`, `DELETE n`).

- The model decides per table. A table that is not modifiable — every table of the file,
  splunk, cloudops and govdata adapters — still rejects the write; the salesforce and
  sharepoint adapters accept it. Their release bundles pass `--allow-writes`; the others do not.
- A write is committed by the adapter when its statement runs. `BEGIN` and `COMMIT` are
  acknowledged, and a `ROLLBACK` in a transaction that wrote is refused (SQLSTATE `0A000`)
  rather than reported as done.
- When the server is given per-role grants (`serve(authz_grants=...)`), the grants that gate
  reads gate writes: a role may write only to relations granted to it.
- `RETURNING` is supported on `INSERT`, `UPDATE` and `DELETE` for tables whose adapter names
  a key column and reports the keys it creates (salesforce, sharepoint). Calcite has no
  `RETURNING`, so the server reads the rows back by key around the write; the reply carries
  the usual `INSERT 0 n` / `UPDATE n` / `DELETE n` tag. On any other table it is refused
  (SQLSTATE `0A000`) before the write runs. `UPDATE ... FROM`, `DELETE ... USING` and
  `INSERT ... ON CONFLICT` with `RETURNING` are not supported.
- DDL is not supported.
- Both the in-process `calcite` backend and the `bridge` backend route writes.

## Client timeouts and cancellation

`statement_timeout` (per session via `SET`, server default via launcher state) bounds a
statement's total time on the server, including any wait for the shared query engine. A
statement that cannot get the engine within `--max-queue-wait-ms` fails fast with a
`server busy` error instead of queueing indefinitely.

Cancelling a running statement (a timeout expiring, or a client `CancelRequest`) interrupts
the underlying DuckDB query, which does not stop instantly: observed cancel latency is 5-11
seconds. A client budgeting its own deadline around `statement_timeout` needs at least that
much slack on top — otherwise the client's socket deadline fires before the server has
finished cancelling and the connection is dropped with the statement still winding down.
