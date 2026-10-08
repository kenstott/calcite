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

A `CancelRequest` also reaches a statement that is still queued for the engine: its wait ends
with SQLSTATE `57014` instead of the statement running later for a client that gave up on it.

## Several clients, one engine

Statements from every connection run one at a time on the shared engine connection, and a
streamed result keeps it until the result is closed. A client that keeps the engine without
using it would starve the others, so the server drops that client instead:

- **Idle holder.** A session whose client stopped reading a result, or fetched part of a
  cursor (an `Execute` with a row limit) and went quiet, is disconnected once another
  statement has waited `--idle-holder-grace-ms` (default 30000; 0 = never) behind it. A
  client that is steadily reading a long result is using the engine and is never dropped.
  Closing a portal (`Close`) releases the engine at once.
- **Cancelled but not released.** When `statement_timeout` or a `CancelRequest` cancels a
  statement whose thread is outside the engine (waiting on its client), that client is
  disconnected after `--cancel-grace-ms`. Only a statement that does not come back from
  the engine itself is a wedge, and only that exits the server (status 3).

## Errors

Every error reaches the client as an ErrorResponse with a severity and a SQLSTATE; one the
engine raises without naming a state is `XX000`. In the extended protocol a failed message
is answered with the error and the messages after it are discarded until `Sync`; the
connection stays open. Binding an unknown prepared statement is `26000`, an unknown portal
`34000`, a malformed message `08P01`. Closing a statement or portal that does not exist is
not an error.

## Start-up and shutdown

The listening port is claimed before the backend is built, which can take minutes: an open
port is not a ready server. By default a connection made in that time waits and is served
once the server is up (it logs `listening on <host>:<port>` at that point). With
`--reject-while-starting`, such a connection is answered at once with `FATAL 57P03` ("the
database system is starting up"), as PostgreSQL does, so a client that polls for readiness
can tell a server that is starting from one that is stuck. A failure after the port is
claimed ends the process (status 1), so the port is never left held by a server that cannot
answer.

On `SIGTERM`/`SIGINT` the server stops accepting, cancels running statements, closes its
client connections and exits 0.

| Launcher option | Purpose |
|-----------------|---------|
| `--reject-while-starting` | answer connections made before the server is ready with `FATAL 57P03` |
| `--idle-shutdown-seconds N` | exit after N seconds with no client connected (also `PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS`) |
| `--pid-file PATH` | write the pid of the process holding the port; removed on a clean shutdown |
| `--jvm-arg ARG` | argument for the embedded JVM, repeatable (`--jvm-arg=-Xmx4g`) |
| `--owner-pid PID` | exit when that process is gone |

## Variants

`pgwire-file`, `pgwire-govdata`, `pgwire-salesforce`, `pgwire-sharepoint`, `pgwire-splunk` and
`pgwire-cloudops` are this server, unchanged, plus configuration: each directory holds a
`model.json` (the Calcite model) and a `launch-args.txt` (launcher arguments baked into the
bundle's entry script, e.g. `--allow-writes`). `.github/workflows/pgwire-adapters-release.yml`
builds them all the same way from those two files. `tests/unit/test_variant_drift.py` fails if
a variant carries anything else, or if the workflow or the AskAmerica connector passes the
launcher an argument it does not have.
