# `meta.table_coverage` — ETL-published coverage

## Why

`data_coverage` and `describe_table` currently answer "which years does this table actually
hold" by scanning the table. That is the wrong place to compute it. The answer changes only
when the ETL writes, so it should be computed once at write time and read back as a value.

Scanning costs three things that go away entirely once it is published:

- `data_coverage` holds the shared JDBC lock for the duration of its scan, up to 120s
- `describe_table`'s background probe runs `GROUP BY year, COUNT(*)` per table described,
  where it used to run `MIN/MAX`
- the `measuring` state exists at all — a caller can ask for coverage and be told to come back

## Shape

Additive. A new table; no existing table, column, or partition layout changes.

```
meta.table_coverage
  schema_name       VARCHAR   not null
  table_name        VARCHAR   not null
  partition_column  VARCHAR   not null   -- "year" for every current producer
  partition_value   VARCHAR   not null   -- the value as the partition holds it
  row_count         BIGINT    not null
  snapshot_id       BIGINT    not null   -- the Iceberg snapshot this describes
  computed_at       TIMESTAMP not null

  primaryKey: [schema_name, table_name, partition_column, partition_value]
```

Grain: one row per table per partition value. A table loaded for 2015-2017 and 2021-2023 has
six rows, and the gap is the absence of rows for 2018-2020 — which is exactly the fact
`data_coverage` reports and a scan is currently required to discover.

`partition_value` is VARCHAR rather than INT because that is how hive partition columns
surface on most of these tables. Readers parse it the way `IngestedYears` already does.

### Foreign keys

`(schema_name, table_name)` aligns to the catalog. Declared per described schema in the
existing `constraints:` block, the same way `health.fda_ndc_products` points at
`fda_drug_approvals` today:

```yaml
constraints:
  table_coverage:
    primaryKey: [schema_name, table_name, partition_column, partition_value]
    foreignKeys:
      - columns: [schema_name, table_name]
        targetSchema: "${META_SCHEMA_NAME:meta}"
        targetTable: table_registry
        targetColumns: [schema_name, table_name]
        comment: Coverage rows describe a catalogued table
```

Iceberg enforces none of this; the constraint is documentation and planner metadata. The
producer is responsible for the invariant.

## Producing it

**The ETL does not need to compute this.** Iceberg partition metadata already carries the
partition values and their record counts, in the manifests. The producer transcribes what is
already there — no data scan, no aggregation.

Hook point: alongside `IcebergMaterializationWriter.publishColumnStatistics()`, which is the
existing precedent for ETL-side metadata publication read back with no scan on the query
path. Coverage publication should follow the same rule that commit established: default off
for library callers, on for ETL runs.

Write semantics: delete-then-insert for the `(schema_name, table_name)` pair on each
materialization, so a table that loses a partition loses its coverage row rather than keeping
a stale one. Partial publication is worse than none — a half-written coverage table reports
gaps that do not exist.

## Consuming it

`data_coverage` becomes a single lookup keyed on `(schema_name, table_name)`.
`describe_table`'s observed block becomes free.

The scan path in `IngestedYears` stays, but as a **named state, not a fallback**:

| condition | reported |
|---|---|
| coverage row present, `snapshot_id` matches the table's current snapshot | `"basis": "published"`, with `computed_at` |
| coverage row present, `snapshot_id` differs | `"basis": "published_stale"`, both snapshot ids, and the reader says the table has been written since |
| no coverage row | `"basis": "scanned"`, measured live as today |

A caller can always tell which of the three they got. That is a reported difference, not a
silent substitution — a table whose coverage has never been published must not be
indistinguishable from one measured a moment ago.

## Open questions

1. **Where does `meta` live?** One shared schema across all 26 govdata schemas, or a
   `table_coverage` table per schema. Shared is one lookup and one FK target; per-schema
   avoids a cross-schema dependency for deployments that mount only some schemas.
2. **Does the MCP deployment see it?** The server reads from R2/S3 through the driver. If
   `meta` is not in `ASKAMERICA_SCHEMAS`, coverage is unreadable and every table reports
   `scanned`. Mounting it needs to be automatic rather than a per-deployment opt-in.
3. **Non-year partitions.** `partition_column` is in the key so `geography`, `type`, and
   `frequency` can be published later, but nothing consumes them yet. Publishing all
   partition columns from the start costs little and avoids a migration.
4. **Views.** A view has no partitions and no snapshot, so it gets no coverage row and
   correctly reports `scanned`. Confirm that is acceptable before shipping — views over
   partitioned tables are exactly where `describe_table` has no declared window today.
