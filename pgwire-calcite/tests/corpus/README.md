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
| psql     | 1       | 1        | Phase 0 simple-query gate |
| dbeaver  | 5       | 1        | columns captured; column types / procedures / FK metadata seeded |
| datagrip | 4       | 1        | tables captured; extensions / dependencies / routines seeded |
| duckdb   | 0       | —        | pending |

The Phase-2 catalog-fidelity probes (`introspect_column_types`, `introspect_procedures`,
`introspect_foreign_keys`, `retrieve_extensions`, `retrieve_dependencies`,
`retrieve_routines`) exercise `format_type`, `pg_proc` /
`information_schema.routines` + `.parameters`, `information_schema.referential_constraints`,
`pg_available_extension_versions()` and the chained `::regclass::oid` cast in
`pg_depend.refclassid`. They replay in
`tests/unit/test_phase2_catalog.py::test_phase2_corpus_replays_against_catalog`.

**Pending your hardware:** capturing real DBeaver/DataGrip/DuckDB traffic needs
those clients pointed at a reference PostgreSQL with `log_statement = 'all'`.
That capture is a prerequisite for the Phase 2/4 exit gates and is called out in
the implementation plan as needing your machines.
