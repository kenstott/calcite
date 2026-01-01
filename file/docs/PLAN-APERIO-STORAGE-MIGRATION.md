# Plan: Migrate .aperio to Storage Provider Compatible Formats

## Overview

Migrate `.aperio` metadata files from local-only formats (DuckDB database files, file locks) to formats that work with any storage provider (S3, file mount).

**Goal:** Enable stateless horizontal scale-out by storing all state in shared storage.

## Current State

```
.aperio/
├── .duckdb/
│   ├── shared.duckdb           # DuckDB catalog (views, schema metadata)
│   └── shared.duckdb.wal       # Write-ahead log
├── {schema}/
│   ├── .partition_status.duckdb    # Incremental tracking
│   ├── .partition_status.duckdb.wal
│   ├── .conversions.json           # File conversion lineage
│   ├── .conversions.json.lock      # File lock
│   └── .generated-model.json       # Generated schema config
```

**Problems for S3:**
| File | Issue |
|------|-------|
| `*.duckdb` | Single-file database, can't share across nodes |
| `*.duckdb.wal` | WAL requires local filesystem semantics |
| `*.json.lock` | File locking doesn't work in S3 |

## Target State

```
.aperio/
├── catalog/
│   └── (in-memory at runtime, views registered on connect)
├── {schema}/
│   ├── partition_status/
│   │   └── data.parquet           # Queryable by DuckDB
│   ├── conversions/
│   │   └── data.parquet           # Or keep as JSON
│   └── generated-model.json       # Read-only, no change
```

---

## Phase 1: Partition Status Migration

**Scope:** Convert `.partition_status.duckdb` to Parquet files.

### 1.1 Create ParquetPartitionStatusStore

- [ ] Create `ParquetPartitionStatusStore` implementing `IncrementalTracker`
- [ ] Same interface as `DuckDBPartitionStatusStore`
- [ ] Uses storage provider for read/write
- [ ] Query via DuckDB: `SELECT * FROM 's3://.../.aperio/{schema}/partition_status/*.parquet'`

### 1.2 Implement Write Strategy

- [ ] **Append-only writes:** New status → new Parquet file
- [ ] **Compaction:** Periodically merge small files into one
- [ ] **Conflict detection:**
  - Read current state before write
  - Use S3 conditional PUT (`If-None-Match`) for atomic create
  - Or: Accept last-write-wins for this use case (partition status is idempotent)

### 1.3 Migration Logic

- [ ] On startup, detect if `.partition_status.duckdb` exists
- [ ] If exists and no `partition_status/*.parquet`: migrate data
- [ ] After successful migration, optionally archive old `.duckdb` file

### 1.4 Configuration

```yaml
schema:
  stateStorage: shared   # "local" or "shared"
  # "local" = current behavior (.duckdb files)
  # "shared" = Parquet in storage provider
```

### 1.5 Files to Modify

| File | Change |
|------|--------|
| `DuckDBPartitionStatusStore.java` | Keep for backward compatibility |
| `ParquetPartitionStatusStore.java` | New implementation |
| `IncrementalTrackerFactory.java` | New factory to select implementation |
| `FileSchema.java` | Pass stateStorage config |

---

## Phase 2: Conversions Metadata Migration

**Scope:** Replace `.conversions.json` + `.conversions.json.lock` with Parquet or lock-free JSON.

### 2.1 Analyze Current Schema

```json
{
  "conversions": {
    "/path/to/source.xlsx": {
      "targetPath": "/path/to/converted.json",
      "sourceModified": 1704067200000,
      "convertedAt": 1704067300000
    }
  }
}
```

### 2.2 Option A: Keep JSON, Remove Lock

- [ ] Use atomic write pattern (write to temp, rename)
- [ ] S3: Use conditional PUT or accept eventual consistency
- [ ] Simpler, less change

### 2.3 Option B: Convert to Parquet

- [ ] Schema: `source_path, target_path, source_modified, converted_at`
- [ ] Query via DuckDB like partition status
- [ ] Consistent with partition status approach

### 2.4 Remove File Locking

- [ ] Remove `FileLock` usage in `ConversionMetadata.java`
- [ ] Replace with optimistic concurrency or last-write-wins
- [ ] For conversions, last-write-wins is acceptable (reconversion is idempotent)

### 2.5 Files to Modify

| File | Change |
|------|--------|
| `ConversionMetadata.java` | Remove FileLock, use atomic writes |
| `RawToParquetConverter.java` | Update metadata access |

---

## Phase 3: DuckDB Catalog Migration

**Scope:** Replace `shared.duckdb` catalog with runtime view registration.

### 3.1 Current Behavior

- `shared.duckdb` persists DuckDB views/tables
- Views point to Parquet files
- Allows cross-schema joins

### 3.2 New Behavior

- [ ] Use in-memory DuckDB database (`:memory:`)
- [ ] Register views at connection time from schema config
- [ ] Views still point to Parquet/Iceberg in storage
- [ ] No persistent catalog file needed

### 3.3 View Registration on Connect

```java
// On schema creation:
for (Table table : tables) {
  String viewSql = "CREATE VIEW " + table.name + " AS SELECT * FROM '" + table.path + "'";
  connection.execute(viewSql);
}
```

### 3.4 Cross-Schema Joins

- [ ] Multiple schemas attach to same in-memory database
- [ ] Or: Use DuckDB ATTACH for cross-database queries
- [ ] Evaluate performance implications

### 3.5 Files to Modify

| File | Change |
|------|--------|
| `DuckDBJdbcSchemaFactory.java` | Remove persistent catalog path |
| `DuckDBJdbcSchema.java` | Register views on init |
| `SharedDatabaseInfo` | Remove or refactor for in-memory |

---

## Phase 4: Storage Provider Integration

**Scope:** Wire everything through storage provider abstraction.

### 4.1 Unified Access

- [ ] All `.aperio` reads/writes go through `StorageProvider`
- [ ] `StorageProvider.getAperioPath(schema, filename)` helper

### 4.2 Configuration

```yaml
schema:
  baseDirectory: "s3://bucket/data"
  aperioDirectory: "s3://bucket/.aperio"   # Optional, defaults to baseDirectory/.aperio
  stateStorage: shared
```

### 4.3 DuckDB S3 Configuration

- [ ] Ensure DuckDB has S3 credentials configured
- [ ] `httpfs` extension loaded
- [ ] Credentials from environment or config

### 4.4 Files to Modify

| File | Change |
|------|--------|
| `FileSchema.java` | Add aperioDirectory config |
| `StorageProvider.java` | Add aperio helper methods |

---

## Phase 5: Testing & Migration

### 5.1 Unit Tests

- [ ] `ParquetPartitionStatusStoreTest` - CRUD operations
- [ ] `ConversionMetadataTest` - Lock-free operations
- [ ] Test with local file storage provider
- [ ] Test with S3 storage provider (integration)

### 5.2 Migration Testing

- [ ] Test upgrade path from old `.duckdb` format
- [ ] Test fresh install with new format
- [ ] Test mixed scenarios (some schemas migrated, some not)

### 5.3 Performance Testing

- [ ] Compare read performance: DuckDB file vs Parquet query
- [ ] Compare write performance: DuckDB insert vs Parquet append
- [ ] Measure impact of view registration on connect

### 5.4 Concurrency Testing

- [ ] Multiple JVMs writing to same partition status
- [ ] Verify no data loss or corruption
- [ ] Test conflict detection/resolution

---

## Rollout Strategy

### Stage 1: Opt-in (Default: local)
```yaml
stateStorage: local   # default, current behavior
```

### Stage 2: Opt-in (Default: shared for new schemas)
```yaml
stateStorage: shared  # new default
```

### Stage 3: Deprecate local
- Warn if using local mode with S3 storage
- Auto-migrate on startup

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Performance regression | Benchmark before/after, cache aggressively |
| Data loss during migration | Backup `.duckdb` before migration |
| S3 eventual consistency | Use strong consistency regions, accept for metadata |
| Complexity increase | Keep local mode for development simplicity |

---

## Success Criteria

- [ ] Multiple instances can share same `.aperio` state
- [ ] No file locking dependencies
- [ ] Works with both S3 and local file storage
- [ ] Backward compatible migration path
- [ ] No performance regression > 10%
