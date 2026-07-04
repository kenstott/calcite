# Regression corpus (PGW-041)

The corpus is the **actual query/introspection SQL** the target clients (DuckDB,
DBeaver, DataGrip, psql) emit against a PostgreSQL endpoint. It is:

1. the acceptance oracle for Phases 2–4 (catalog, streaming, COPY), and
2. the drift-mitigation asset for decision **D1** — provisa and pgwire-calcite
   share this corpus (data), not the catalog/protocol code, so a probe fixed in
   one project is re-verifiable in the other.

Calcite version upgrades are gated on this corpus (adopted on our schedule,
verified against our paths — never blindly).

## Format

One JSON file per probe under `tests/corpus/<client>/<name>.json`. Schema and the
capture procedure are documented in [`harness.py`](harness.py).

- `captured: true`  — extracted from real client traffic.
- `captured: false` — hand-seeded from known probes; structure is exercised but
  the entry is a placeholder until real traffic is captured.
- `expect.min_phase` — the phase by which the entry must pass. The corpus test
  only runs entries whose `min_phase <= current phase`.

## Status

| Client   | Entries | Captured | Notes |
|----------|---------|----------|-------|
| psql     | 1       | yes      | Phase 0 simple-query gate |
| dbeaver  | 1       | **no**   | seed; live capture pending (needs DBeaver + reference PG) |
| datagrip | 0       | —        | pending |
| duckdb   | 0       | —        | pending |

**Pending your hardware:** capturing real DBeaver/DataGrip/DuckDB traffic needs
those clients pointed at a reference PostgreSQL with `log_statement = 'all'`.
That capture is a prerequisite for the Phase 2/4 exit gates and is called out in
the implementation plan as needing your machines.
