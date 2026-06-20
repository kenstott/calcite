# Table Updater — lazy view reconciliation on schema evolution

Status: proposed
Scope: govdata / askamerica JDBC driver (read-only R2 serving path)

## Problem

The askamerica driver serves govdata as Iceberg-on-R2: an embedded DuckDB reads
Iceberg tables directly from R2 (`autoDownload=false`, see
[GovDataDriver.java:226](../../src/main/java/org/apache/calcite/adapter/govdata/GovDataDriver.java#L226)),
backed by a **persistent** DuckDB catalog (`catalog.duckdb`) and the
`cache_httpfs` byte cache under the data dir (`~/.askamerica/.duckdb`, pinned in
[AskAmericaDriver.java:66-73](../../../askamerica-engine/src/main/java/org/apache/calcite/adapter/askamerica/AskAmericaDriver.java#L66)).

Row/snapshot freshness is automatic: Iceberg data/manifest files are immutable
and the mutable pointers (`version-hint.text`, `metadata.json`) are excluded
from `cache_httpfs`, so every query re-resolves the current snapshot live. The
one thing that does **not** self-heal is **column (schema) evolution**, and the
failure mode is severe — see below.

## Verified behavior

### DuckDB freezes `SELECT *` views and bind-fails when the schema widens

The DuckDB views are created as `SELECT * FROM iceberg_scan(...)`
([DuckDBJdbcSchemaFactory.java:1180](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java#L1180)).
DuckDB expands the `*` at `CREATE VIEW` time and records the column list. When
the underlying scan later returns more columns (a new Iceberg field), DuckDB
does **not** re-expand — it throws at bind time:

```
Binder Error: Contents of view were altered: types don't match!
Expected [INTEGER, INTEGER], but found [INTEGER, INTEGER, INTEGER] instead
```

Empirically confirmed on DuckDB 1.4.4 (base table `ALTER ADD COLUMN`, and a
persisted view over `read_parquet` read by a fresh process — same error both
ways, so it is the DuckDB *view* layer freezing, independent of the backing
function). Critically, the error is at **view-bind time**, so it breaks **every**
query against the table — even `SELECT <an existing column>` — not just `SELECT *`
or queries touching the new column. The table goes fully dark for that client.

Iceberg itself does not "expand" anything — it is a passive table format
declaring the current schema. `iceberg_scan` (a DuckDB table function) surfaces
that current schema at bind time; the DuckDB view is what pinned the old shape.

`CREATE OR REPLACE VIEW` fully fixes it — the recreated view binds against the
current schema and returns all columns.

### Nothing recreates the view today on a read-only client

- `catalog.duckdb` is static/persistent; views written into it survive restarts.
- The fast-start path skips recreation whenever the view already exists
  ([DuckDBJdbcSchemaFactory.java:1170-1175](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java#L1170)):
  *"check if view exists first for fast start (no S3 calls)... Schema updates
  are handled during ETL... not on startup."*
- The only existing recreate trigger is the in-process `_recreatedIcebergTables`
  operand list ([:348-351](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java#L348)),
  populated by the ETL process. A read-only R2 client never runs that ETL, so the
  list is always empty.

So when Iceberg gains a column, the persisted `SELECT *` view stays frozen and no
code path recreates it. The table is bricked for that client until something
issues `CREATE OR REPLACE`. There is no self-healing. This plan adds that trigger.

### Metadata is served from Calcite, planning uses the live row type

- `information_schema` and `pg_catalog` are Calcite-side schemas; columns are
  built by walking the Calcite schema tree and calling `Table.getRowType`
  ([InformationSchema.java:319-333](../../../file/src/main/java/org/apache/calcite/adapter/file/metadata/InformationSchema.java#L319),
  [PostgresMetadataSchema.java:62-70](../../../file/src/main/java/org/apache/calcite/adapter/file/metadata/PostgresMetadataSchema.java#L62)).
  DuckDB's native metadata is not the authority.
- For Iceberg tables the row type comes from `IcebergTable.getRowType`
  ([DuckDBJdbcSchema.java:371-376](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchema.java#L371)),
  reading the **live Iceberg schema** ([IcebergTable.java:151-161](../../../file/src/main/java/org/apache/calcite/adapter/file/iceberg/IcebergTable.java#L151)),
  rebuilt per connection (Calcite has no persistence). So planning already tracks
  the current schema; only the persisted DuckDB execution view lags. After a
  `CREATE OR REPLACE`, view == current Iceberg == row type — fully consistent.

## Design

Self-heal reactively, using the bind failure itself as the trigger. There is no
need to predict schema drift with hashes or versions — the condition that breaks
is directly observable, with zero false positives.

On the **first data access** to a table, probe its DuckDB view; if it bind-fails
with the "altered" error, `CREATE OR REPLACE` it, then proceed. The user query
never sees the error because the probe runs during planning, before execution.

### Hook point: `toRel`, gated by a verified-set

- Probe in the wrapper's `toRel` ([DuckDBJdbcSchema.java:361-368](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchema.java#L361)),
  **not** `getTable`/`getRowType`. `toRel` fires only when a table is pulled into
  a real query plan (a data scan). `getRowType` is also called by
  `information_schema` enumeration, so probing there would mass-probe all ~330
  tables on every metadata listing; `toRel` avoids that.
- `toRel` runs during planning, before execution, so recreating there fixes the
  view before the query binds against it — the user never encounters the failure.
- Maintain a thread-safe in-memory set of already-verified view names. "First
  access" needs no special detection: probe iff not in the set, then add it.
  Repeat queries skip; concurrent first-touches just do idempotent
  `CREATE OR REPLACE`.

### Probe + recreate

1. `toRel` for table T → if T already verified, delegate immediately.
2. Otherwise run a **bind-only** probe on the persistent DuckDB connection:
   `SELECT * FROM "schema"."T" LIMIT 0`. `LIMIT 0` triggers the bind-time check
   without fetching a data file (metadata-only GET).
3. On the specific `Contents of view were altered` error → call the existing
   `recreateIcebergView(tableName, tableLocation)`
   ([DuckDBJdbcSchema.java:158-176](../../../file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchema.java#L158)),
   which issues `CREATE OR REPLACE VIEW ... iceberg_scan(...)`.
4. Mark T verified; delegate to the real `toRel`.

### Failure handling (no silent fallbacks)

- Recreate **only** on the `Contents of view were altered` binder error. Any
  other probe failure (R2 unavailable, genuinely corrupt table) must propagate —
  recreating blindly would mask it, and the recreate would fail anyway.
- The recreate itself must fail loud if `iceberg_scan` cannot bind, rather than
  leaving a half-broken view.

## Why this replaces the hash/version design

An earlier draft precomputed a per-driver-version hash of each table's column
contract and diffed versions to decide which views to recreate. That predicted a
condition that is directly observable at far lower complexity. The probe needs no
driver version, no CSV history, no diff, and no upgrade/downgrade logic, and it
self-heals regardless of *why* a view is stale. It is strictly simpler and has no
false positives.

## Implementation steps

1. Add a thread-safe verified-set (e.g. `ConcurrentHashMap.newKeySet`) on the
   DuckDB schema instance, keyed by table name.
2. In the wrapper's `toRel`, before delegating: if the table is Iceberg-backed
   and not verified, run the `LIMIT 0` bind probe on the persistent connection.
3. Catch only the `Contents of view were altered` error; on it, call
   `recreateIcebergView`. Let other exceptions propagate.
4. Add the table to the verified-set after a successful probe or recreate.
5. Unit/integration test: build a view against an Iceberg table, add a column,
   then drive a query through Calcite and assert (a) no error reaches the client
   and (b) the new column is visible after the probe-triggered recreate.

## Constraints

- Java 8 source compatibility (no `var`, text blocks, switch expressions,
  records, etc.).
- No `System.getenv` in govdata/file — read any config from the model operand.
- No silent fallbacks: recreate only on the specific altered-view error; all
  other failures propagate.

## Confirmed end-to-end

`IcebergSelectStarWideningTest`
([file/src/test/.../iceberg/IcebergSelectStarWideningTest.java](../../../file/src/test/java/org/apache/calcite/adapter/file/iceberg/IcebergSelectStarWideningTest.java))
drives `SELECT *` through the full Calcite→DuckDB stack against a real Iceberg
table whose schema is widened (a column added) between connections. With the
model declaring **no** columns, `SELECT *` returns 2 columns before the change
and all 3 after — the Iceberg scan report shows
`projectedFieldNames=[order_id, customer_id, amount]`. This confirms the column
set tracks the **live Iceberg schema** (`IcebergTable.getRowType`), not any
narrower declared/static definition, so `SELECT *` is never clamped to a declared
subset.

The test covers the **fresh-view** path. It does not reproduce the stale-
persisted-view binder error (`Contents of view were altered`), which was
confirmed separately at the DuckDB process level; reproducing it faithfully needs
a persisted catalog read by a fresh process, and in-JVM catalog/row-type caching
makes it unreliable to assert in a single-JVM test.
