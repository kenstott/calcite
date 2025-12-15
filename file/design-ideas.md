# File Adapter - Future Design Ideas

This document tracks potential future enhancements. Most original design ideas have been implemented.

**See Also:**
- `DESIGN-JDBC-ENGINES.md` - Pluggable JDBC query engines (implemented, time-travel pending)
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

---

## Future: Delta Lake Support

**Priority**: Medium | **Effort**: 4-6 weeks

Add support for Delta Lake table format alongside Iceberg.

**Key features:**
- Time travel queries
- ACID transactions
- Change Data Feed (CDC)
- Schema evolution

**Dependencies:** `io.delta:delta-standalone`

---

## Future: Additional File Formats

### ORC Support
**Priority**: Low | **Effort**: 2-3 weeks

Optimized Row Columnar format, common in Hive ecosystems.

### Avro Support
**Priority**: Low | **Effort**: 2-3 weeks

Schema-first format with strong schema evolution, common in streaming.

---

## Future: S3 Select Pushdown

**Priority**: Low | **Effort**: 3-4 weeks

Push filter predicates directly to S3 for server-side filtering, reducing data transfer for selective queries on large files.

---

## Future: gRPC Data Sources

**Priority**: Low | **Effort**: 4-6 weeks

Query gRPC microservices as SQL tables with automatic protobuf-to-SQL schema mapping.

---

## Future: Redis Distributed Caching

**Priority**: Low | **Effort**: 2-3 weeks

Enable distributed cache coordination for clustered deployments. Stub implementation exists (`RedisDistributedLock.java`) but methods are non-functional.

**What's needed:**
- Implement actual Redis integration for distributed locking
- Add Redis-backed statistics cache
- Test in multi-node scenarios

---

## Future: JSONL/NDJSON Support

**Priority**: Low | **Effort**: 1 week

Add support for newline-delimited JSON formats (`.jsonl`, `.ndjson`) common in streaming and log data.

**What's needed:**
- Add extensions to `TABLE_SOURCE_EXTENSIONS` in `FileSchema.java`
- Handle line-by-line parsing vs. array parsing
- Streaming support for large files

---

## Future: Office Document Hyperlink Crawling

**Priority**: Low | **Effort**: 2-3 weeks

Extract and follow hyperlinks from Word/Excel/PowerPoint documents, similar to HTML crawling.

**What's needed:**
- Parse `.rels` files for hyperlink relationships
- Extend `DocxTableScanner`, `ExcelToJsonConverter` with link extraction
- Integrate with existing `HtmlLinkCache` pattern

---

## Future: Pluggable Data Pipeline Framework

**Priority**: Low | **Effort**: 2-3 months

Transform file adapter into a data ingestion platform with plugin ecosystem for custom data sources (REST APIs, CDC streams, file watchers).

---

## Implemented Features (Reference)

The following features from original design documents have been implemented:

| Feature | Status | Location |
|---------|--------|----------|
| Environment variable substitution | Done | `FileSchemaFactory` |
| Duplicate schema detection | Done | `FileSchemaFactory` |
| Iceberg table format | Done | `iceberg/` package |
| JDBC dialect framework | Done | `jdbc/` package |
| DuckDB, Trino, Spark, ClickHouse dialects | Done | `jdbc/` package |
| Partition alternates | Done | `partition/` package |
| Similarity/vector functions | Done | `similarity/` package |
| HTML/Web table extraction | Done | `converters/` package |
| Materialized views | Done | `materialized/` package |
| Refresh framework | Done | `refresh/` package |
