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
