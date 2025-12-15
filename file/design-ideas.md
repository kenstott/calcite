# File Adapter - Design Status

Most design ideas have been implemented. This document tracks remaining work.

**Design Documents:**
- `DESIGN-JDBC-ENGINES.md` - Pluggable JDBC query engines (implemented)
- `DESIGN-PARTITION-ALTERNATES.md` - Partition alternates with Iceberg (implemented)
- `DESIGN-ETL-CAPABILITIES.md` - ETL transformation capabilities

---

## Pending: Iceberg Time Travel via JDBC Engines

**Status**: Designed in DESIGN-JDBC-ENGINES.md, not yet implemented

The `createIcebergTimeRangeViewSql()` method generates UNION ALL views spanning multiple Iceberg snapshots, enabling time-travel queries via standard SQL with JDBC engines like DuckDB and ClickHouse.

**What's needed:**
- Wire `JdbcDialect.createIcebergTimeRangeViewSql()` into schema initialization
- Create views when `timeTravel.enabled: true` in partition config
- Test with DuckDB and ClickHouse
