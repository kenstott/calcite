# Design: Partition Alternates for Partitioned Tables

## Status: Draft

## Problem Statement

The current alternate partition reorganization for large datasets causes OOM errors:

```
Source: type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/*.parquet
Target: type=regional_income_by_fips/geo_fips=*/year=*/*.parquet
Result: Exit code 137 (OOM killed) after 42 minutes
```

**Root cause**: Single-stage transformation attempts to process entire dataset at once, exceeding available memory even with batching.

**Scale context**:
- ~10,000+ source files across partition combinations
- Each file: ~100KB-10MB
- Total data: ~50GB+
- Memory limit: 8GB

## Solution Overview

**Key Insight**: If the root problem (too many small files) is solved at ingestion by consolidating partitions, then alternates can be created directly from JSON—no Parquet→Parquet reorganization needed.

**Architecture: DuckDB + Apache Iceberg**

```
JSON Source Files
       │
       ▼
┌─────────────────┐
│  DuckDB Engine  │  ← Transform JSON → Parquet (batched)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Iceberg │  ← Table management (catalog, snapshots, concurrency)
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
regional_income    _mv_a7b3c9d2...
(year/geo_fips)    (geo_fips/year)
 [user-facing]      [planner only]
```

**Why Iceberg?**

We were designing custom infrastructure that Iceberg already provides:

| What We Were Building | What Iceberg Provides |
|-----------------------|-----------------------|
| `alternate_registry` | Iceberg Catalog |
| `partition_alternate_status` | Snapshot metadata |
| `materialization_claims` | Optimistic concurrency |
| `status: BUILDING/COMPLETE` | Snapshot states |
| OVERWRITE_OR_IGNORE | Atomic snapshot commits |
| Manual orphan cleanup | `expireSnapshots()` |

**Iceberg gives us for free:**
- Atomic partition overwrites (exactly what we need for batch updates)
- Concurrent writer coordination (built-in optimistic locking)
- Snapshot isolation (readers see consistent state)
- Time travel (query old versions if needed)
- Industry standard format (Spark, Trino, Flink can read our tables)
- No custom metadata tables to maintain

**Hybrid approach:**
1. **DuckDB**: Fast JSON parsing and transformation (batched)
2. **Iceberg Java API**: Table management, atomic commits, concurrency

| Aspect | Source Table | Alternate |
|--------|--------------|-----------|
| Name | `regional_income` | `_mv_{random32}` |
| Format | Iceberg table | Iceberg table |
| Partition | `(year, geo_fips)` | `(geo_fips, year)` |
| Visibility | User-facing | Planner only (cryptic name) |
| Catalog | Iceberg HadoopCatalog or JdbcCatalog | Same catalog |

## Important Distinction: Partition Alternates vs Database Indexes

**Partition Alternates** are alternative partition layouts of the same data—not lookup structures like B-tree indexes:

| Aspect | Database Index | Partition Alternate (this design) |
|--------|----------------|----------------------------------|
| Purpose | Speed up row lookups | Speed up partition pruning |
| Storage | B-tree/hash structure | Full data copy with different partitioning |
| Query benefit | Avoid table scan | Avoid scanning many small files |
| Maintenance | Auto-updated on INSERT | Rebuilt from source on change |

**Not to be confused with Calcite Lattice** (precomputed aggregates with `GROUP BY`). This design preserves all rows.

**Terminology**: We use "Alternate" rather than "Index" to avoid confusion with traditional database indexes and to maintain consistency with existing code (`AlternatePartitionSelectionRule`, `AlternatePartitionMaterializer`).

## Schema Configuration

```yaml
tables:
  regional_income:
    # Source table: consolidated partitions from JSON
    source: "type=regional_income/year=*/geo_fips=*/*.parquet"
    partitions:
      style: hive
      keys:
        - name: year
          type: INTEGER
        - name: geo_fips
          type: VARCHAR
    batch_partition_columns: [year]       # Columns for batching (OOM prevention)
    incremental_keys: [year]              # Columns for incremental tracking

    # Partition alternates: same data, different partition order
    alternate_partitions:
      - name: by_fips                         # Description only (for logging), NOT a lookup key
        partition:
          style: hive
          keys:
            - name: geo_fips
              type: VARCHAR
            - name: year
              type: INTEGER
        batch_partition_columns: [year]       # Columns for batching (OOM prevention)
        incremental_keys: [year]              # Columns for incremental tracking
        # Physical Iceberg table: _mv_{random32} (auto-generated, managed by Iceberg catalog)
```

**Config field clarifications:**
- `name` - (alternates only) **Description only** for logging and documentation. Not a lookup key. The `_mv_xxx` Iceberg table ID is the actual identifier.
- `pattern` - **Removed**. Not needed with Iceberg - the catalog manages file paths internally. Partition structure comes from `keys`.
- `partitions.keys` - Defines partition key columns. Iceberg creates directory layout automatically (e.g., `geo_fips=01001/year=2020/`).
- `batch_partition_columns` - Columns for batching during materialization (OOM prevention). **Used by both primary table and alternates.**
- `incremental_keys` - Columns for incremental tracking (skip already-processed batches). **Used by both primary table and alternates.**
- `batch_partition_columns` and `incremental_keys` are often the same (e.g., both `[year]`) but conceptually distinct

### How It Works

1. **Check incremental tracker**: Skip batches already processed (`IncrementalTracker.isProcessed()`)
2. **DuckDB transforms JSON → Parquet**: Batched by `batch_partition_columns`, writes to staging
3. **Move files to Iceberg data location**: S3 server-side copy or local atomic rename
4. **Iceberg commits the files**: Atomic append/overwrite with final paths
5. **Mark batch processed**: Update tracker (`IncrementalTracker.markProcessed()`)
6. **Repeat for alternates**: Same process with different partition spec
7. **Calcite reads via Iceberg**: Tables visible in schema via Iceberg catalog

```java
// Simplified flow using existing IncrementalTracker
IncrementalTracker tracker = DuckDBPartitionStatusStore.getInstance(baseDirectory);

for (Map<String, String> batch : batches) {
    // 1. Check if already processed (incremental_keys)
    if (tracker.isProcessed(alternateName, sourceTable, batch)) {
        LOGGER.info("Skipping already processed batch: {}", batch);
        continue;
    }

    // 2. DuckDB transforms JSON to Parquet (staging location)
    Path stagingPath = createStagingPath();
    duckDb.execute("""
        COPY (SELECT * FROM read_json('%s', union_by_name=true))
        TO '%s' (FORMAT PARQUET, PARTITION_BY (%s))
        """, jsonPattern, stagingPath, partitionColumns);

    // 3. Move files from staging to Iceberg data location
    Path dataLocation = Paths.get(table.location(), "data");
    List<Path> movedFiles = moveFilesToDataLocation(stagingPath, dataLocation);

    // 4. Build DataFiles with final paths and commit atomically
    List<DataFile> dataFiles = buildDataFiles(movedFiles, table.spec());
    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal("year", batch.get("year")))
        .addFile(dataFiles)
        .commit();

    // 5. Mark batch as processed
    tracker.markProcessed(alternateName, sourceTable, batch, targetPattern);
}
```

### Iceberg Table Structure

```
warehouse/
  regional_income/                      # Iceberg table
    metadata/
      v1.metadata.json                  # Table metadata
      snap-xxx.avro                     # Snapshot manifests
    data/
      year=2020/geo_fips=01001/
        00000-0-xxx.parquet
      year=2020/geo_fips=01003/
        00000-0-xxx.parquet

  _mv_a7b3c9d2.../                      # Alternate (also Iceberg table)
    metadata/
      v1.metadata.json
    data/
      geo_fips=01001/year=2020/
        00000-0-xxx.parquet
```

### Configuration Validation Rules

1. `partition.keys` columns must exist in source data
2. `batch_partition_columns` should provide sufficient granularity to fit in memory
3. Partition alternate must use same columns as source (just reordered)

### Automatic Batch Key Embedding

When `batch_partition_columns` contains columns NOT in the partition's `keys`, batch keys are **automatically** embedded in filenames to prevent overwrites. No explicit configuration needed.

```yaml
alternate_partitions:
  - name: by_metric_trending
    partition:
      keys:
        - name: tablename
        - name: line_code                     # No year in partitions
    batch_partition_columns: [year]           # year ∉ partition → auto-embed in filename
```

**Implementation logic:**
```java
boolean needsFilenameEmbedding = !partitionColumns.containsAll(batchPartitionColumns);
if (needsFilenameEmbedding) {
    // Auto-generate FILENAME_PATTERN with batch key values
    filenamePattern = buildFilenamePattern(batchKey);  // e.g., "year_2020_{i}"
}
```

**Generated SQL (automatic):**
```sql
-- Batch 2020: year ∉ partition_by, so embed in filename
COPY (SELECT * FROM read_json('.../*_2020_*.json', ...))
TO '__alternates/_mv_xxx'
(FORMAT PARQUET,
 PARTITION_BY (tablename, line_code),
 FILENAME_PATTERN "year_2020_{i}",
 OVERWRITE_OR_IGNORE)
```

**Result structure:**
```
tablename=CAINC1/line_code=1/
  year_2020_0.parquet
  year_2021_0.parquet
  year_2022_0.parquet
```

**When to use each approach:**

| `batch_partition_columns` ⊆ partition? | Behavior | Use Case |
|------------------------------|----------|----------|
| Yes | Batch keys in directory structure | Point-in-time queries (can prune by batch key) |
| No | Batch keys in filename (automatic) | Trending queries (scan all batches together) |

**Invalidation with embedded filenames:**
```java
// Delete files for a specific batch
String pattern = String.format("year_%s_*.parquet", batchValue);
storageProvider.deleteByPattern(alternatePath, pattern);
```

## Optimal Parquet File Size & Partition Depth

### Recommended Settings

| Setting | Recommended | Rationale |
|---------|-------------|-----------|
| **File size** | 256MB - 1GB | < 128MB = metadata overhead; > 1GB = poor parallelism |
| **Row group size** | 512MB - 1GB | Larger = fewer I/O ops, but higher memory |
| **Minimum file size** | > 128MB | Match cloud storage block size |
| **Partition cardinality** | Low-moderate | High cardinality = small files problem |
| **Partition depth** | 2-4 levels | More depth = slower directory listing |
| **Compression** | Snappy (default) | Zstandard for storage-cost-sensitive workloads |

### The Small Files Problem

Query engines run **~100x slower** on many small files vs fewer large files:

| Scenario | Files | Query Performance |
|----------|-------|-------------------|
| Fragmented | 10,000 × 10MB | Slow (metadata overhead, listing) |
| Consolidated | 100 × 1GB | Fast |

**Root causes**:
- Metadata overhead per file (~150 bytes NameNode, plus Parquet footer)
- Directory listing operations (S3 `ListObjects` is expensive)
- Seek time per file open

**This is exactly why partition alternates exist** - consolidating `year/geo_fips_set/tablename/line_code/geo_fips` (5 levels, many small files) into `geo_fips/year` (2 levels, larger files).

### Partition Depth Guidelines

| Depth | Use Case | Example |
|-------|----------|---------|
| 1-2 | Most queries | `year/geo_fips` |
| 3 | Moderate filtering needs | `year/month/region` |
| 4+ | Avoid unless cardinality is low | High listing overhead |

**Rule of thumb**: Each partition level should have < 1000 distinct values. If a column has millions of values (like `geo_fips` with 3000+ counties), it should be a leaf partition, not intermediate.

### Target File Count per Alternate

When designing `batch_partition_columns`, aim for partition alternates to have:
- **< 10,000 total files** per table
- **> 100MB per file** on average
- **< 1000 files per partition value** at any level

### Source Consolidation at Ingestion

For tables like `regional_income` where data is downloaded at natural API granularity, consolidate **during initial Parquet creation** rather than post-hoc.

**Case Study: `regional_income`**

| Layout | Partition Columns | Est. Files | Status |
|--------|-------------------|------------|--------|
| Current | year/geo_fips_set/tablename/line_code/geo_fips | **700,000** | 70x over limit |
| Recommended | year/geo_fips | **~16,000** | Within guidelines |

The columns `geo_fips_set`, `tablename`, and `line_code` move from path segments into Parquet file content.

```yaml
tables:
  regional_income:
    # Consolidated layout: only partition by year and geo_fips
    source: "type=regional_income/year=*/geo_fips=*/*.parquet"
    partitions:
      style: hive
      keys:
        - name: year
          type: INTEGER
        - name: geo_fips
          type: VARCHAR
    # geo_fips_set, tablename, line_code are regular columns in the Parquet files
```

**Implementation with DuckDB** (during CSV→Parquet conversion):

```sql
-- Consolidate all data for a year into year/geo_fips partitions
COPY (
  SELECT
    *,
    'COUNTY' AS geo_fips_set,      -- Preserve as column
    'CAINC1' AS tablename,         -- Preserve as column
    line_code                       -- Already a column
  FROM read_csv('downloads/CAINC1_*.csv')
)
TO 'type=regional_income'
(FORMAT PARQUET,
 PARTITION_BY (year, geo_fips),    -- Only 2 partition levels
 OVERWRITE_OR_IGNORE)
```

**File size estimate**:
- 700,000 files ÷ 44 = ~16,000 files
- If original total ~70GB: 70GB ÷ 16,000 = ~4.4MB per file (still small, but acceptable)
- Could further consolidate to just `year` (~5 files × ~14GB each) if queries always filter by year first

**Benefits**:
- 44x fewer files immediately
- Queries on source table are fast without needing partition alternates
- Partition alternate only needed for `by_fips` layout (swap partition order)
- Dramatically reduced S3 listing overhead

**Trade-off**: Queries filtering by `tablename` or `line_code` must scan more data (no partition pruning). Acceptable if these filters are rare or combined with `year`/`geo_fips` filters.

**When to consolidate at ingestion**:
- File count exceeds 10,000
- Partition columns rarely filtered independently
- High cardinality intermediate partitions (tablename × line_code = ~1500 combinations)

This is complementary to partition alternates - consolidate aggressively at ingestion, use alternates only for different access patterns.

### Unified Materialization Framework

**Key insight**: Initial JSON→Parquet conversion and Parquet reorganization are the same operation with different source formats.

| Aspect | Initial Conversion | Reorganization |
|--------|-------------------|----------------|
| Source format | JSON | Parquet |
| DuckDB reader | `read_json()` | `read_parquet()` |
| Batch strategy | By `[year]` etc. | By `batch_partition_columns` config |
| Output | Partitioned Parquet | Partitioned Parquet |
| OOM prevention | Same batching | Same batching |

### Source File Scenarios

The materialization framework handles three source scenarios uniformly:

| Scenario | Source Origin | Source Format | DuckDB Reader |
|----------|---------------|---------------|---------------|
| API Download | Downloaded via API (govdata, etc.) | JSON | `read_json()` |
| Pre-existing JSON | Already on disk/S3 | JSON | `read_json()` |
| Pre-existing Parquet | Already on disk/S3 | Parquet | `read_parquet()` |

**Source format detection** (from file extension or explicit config):
```java
SourceFormat detectSourceFormat(String pattern) {
    if (pattern.endsWith(".json") || pattern.contains("*.json")) {
        return SourceFormat.JSON;
    } else if (pattern.endsWith(".parquet") || pattern.contains("*.parquet")) {
        return SourceFormat.PARQUET;
    }
    // Default based on table config or throw error
}
```

**Pre-existing files handling**:
- The `IncrementalTracker` is agnostic to how source files arrived
- It tracks `(alternate_name, incremental_key_values)` → processed status
- First run: All batches processed, marked as complete
- Subsequent runs: Skip already-processed batches
- If source files change: Invalidate affected batches via `tracker.invalidate()`

**Parquet-to-Parquet reorganization** (changing partition scheme):
```sql
-- Source: year/geo_fips partitioning
-- Target: geo_fips/year partitioning (alternate)
COPY (
  SELECT * FROM read_parquet(
    'type=regional_income/year=2020/**/*.parquet',
    hive_partitioning=true
  )
)
TO 'warehouse/.staging/{timestamp}/'
(FORMAT PARQUET, PARTITION_BY (geo_fips, year))
```

The flow is identical regardless of source format - only the DuckDB reader function changes.

**Proposed unified API:**

```java
public class MaterializationConfig {
    private final String sourcePattern;      // "downloads/regional_income/*.json" or "type=foo/**/*.parquet"
    private final SourceFormat sourceFormat; // JSON or PARQUET
    private final String targetBase;         // "type=regional_income"
    private final List<String> partitionBy;  // [year, geo_fips]
    private final List<String> batchAt;      // [year] - process one year at a time
    private final Map<String, String> columnMappings;
}

public enum SourceFormat {
    JSON,    // Use read_json() with union_by_name
    PARQUET  // Use read_parquet() with hive_partitioning
}
```

**SQL generation:**

```sql
-- For JSON source (initial conversion), batch by year:
COPY (
  SELECT * FROM read_json(
    'downloads/regional_income/year=2020/**/*.json',
    union_by_name=true
  )
)
TO 'type=regional_income'
(FORMAT PARQUET, PARTITION_BY (year, geo_fips), OVERWRITE_OR_IGNORE)

-- For Parquet source (reorganization), same structure:
COPY (
  SELECT * FROM read_parquet(
    'type=regional_income_old/year=2020/**/*.parquet',
    hive_partitioning=true
  )
)
TO 'type=regional_income'
(FORMAT PARQUET, PARTITION_BY (year, geo_fips), OVERWRITE_OR_IGNORE)
```

**Incremental update handling:**

When new source files arrive (new JSON downloads or new source Parquet files):

1. **Detect affected batches**: Which `batch_partition_columns` combinations have new/modified source files?
2. **Invalidate those batches**: Mark as needing reprocessing in tracking table
3. **Reprocess only affected batches**: Read source files for that batch, write to target

```java
// Incremental tracking - same for both JSON and Parquet sources
public void processIncrementally(MaterializationConfig config) {
    Set<Map<String, String>> affectedBatches = detectAffectedBatches(config);

    for (Map<String, String> batch : affectedBatches) {
        if (tracker.isProcessed(config.getName(), batch)) {
            continue;  // Already up to date
        }

        String batchSourcePattern = applyBatchFilter(config.getSourcePattern(), batch);
        materializeBatch(batchSourcePattern, config.getTargetBase(),
                        config.getPartitionBy(), config.getSourceFormat());

        tracker.markProcessed(config.getName(), batch);
    }
}
```

**Benefits of unification:**

1. Single code path for both JSON→Parquet and Parquet→Parquet
2. Consistent batching/OOM prevention
3. Consistent incremental tracking
4. Partition alternates use the same framework (same code, different `PARTITION_BY`)

**Implementation approach:**

Extend `ParquetReorganizer` to support JSON sources:

```java
// Current
public class ParquetReorganizer { ... }

// Extended
public class DataMaterializer {
    public enum SourceFormat { JSON, PARQUET }

    public void materialize(MaterializationConfig config) {
        // Same logic, just swap read_json/read_parquet based on format
    }
}
```

Or keep separate classes with shared base:

```
AbstractMaterializer
  ├── JsonToParquetMaterializer (initial conversion)
  └── ParquetReorganizer (reorganization, partition alternates)
```

## Memory Control via `batch_partition_columns`

The `batch_partition_columns` level determines memory usage per iteration:

| `batch_partition_columns` | Iterations | Memory per iteration |
|------------|------------|---------------------|
| `[year]` | ~5 | Large (all geo_fips_sets, tablenames) |
| `[year, geo_fips_set]` | ~15 | Medium |
| `[year, geo_fips_set, tablename]` | ~250 | Small (~3K files max) |

**Rule**: Choose `batch_partition_columns` so each iteration processes < 1000 files or < 1GB data.

## Directory Structure

```
downloads/                               # JSON source files (from API)
  regional_income/
    CAINC1_2020_COUNTY.json
    CAINC1_2020_STATE.json
    ...

type=regional_income/                    # Source table (user-facing)
  year=2020/geo_fips=01001/
    data.parquet                         # Consolidated: all tablenames, line_codes in file
  year=2020/geo_fips=01003/
    data.parquet

__alternates/                            # Partition alternates (planner only)
  _mv_a7b3c9d2e1f4g8h5i6j0k2l3m4n5o6p7/  # by_fips alternate
    geo_fips=01001/year=2020/
      data.parquet
    geo_fips=01001/year=2021/
      data.parquet
```

### Naming Strategy

Partition alternates use random names: `_mv_{random32}` (e.g., `_mv_a7b3c9d2e1f4g8h5i6j0k2l3m4n5o6p7`)

**Rationale**:
- Discourages direct queries to internal alternates
- Prevents naming collisions
- Iceberg catalog maps logical names to table identifiers
- Planner can still access via Iceberg catalog

**Name generation**:
```java
private String generateAlternateName() {
    String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder("_mv_");
    SecureRandom random = new SecureRandom();
    for (int i = 0; i < 32; i++) {
        sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
}
```

**Debugging**: Use Iceberg catalog to list tables and their properties.

## Execution Flow with Iceberg

### Staging Directory Strategy

Temp files are written to a staging area within the warehouse storage:

```
warehouse/
  .staging/                                    # Staging root
    20241215T103000Z_a7b3c9d2/                 # {ISO timestamp}_{random8}
      year=2020/geo_fips=01001/
        data.parquet
    20241215T104500Z_e1f4g8h5/                 # Another concurrent writer
      ...
  regional_income/                             # Final Iceberg table
    metadata/
    data/
```

**Design rationale:**
- **Same StorageProvider as warehouse** - Enables efficient moves (S3 server-side copy, local atomic rename)
- **Timestamped directories** - Easy orphan identification; supports cleanup lifecycle rules
- **Random suffix** - Isolates concurrent writers; prevents collision

**Cleanup policy:**
- **After successful commit**: Staging directory is empty (files moved to data location)
- **On failure**: Staging files remain as orphans
- **R2/S3 lifecycle rule**: Delete objects in `.staging/` prefix older than 24 hours (catches orphans from failures)
- **No explicit cleanup of orphans** - S3 deletions are slow (per-object API calls); let lifecycle handle failures

```java
// Generate staging path for this batch
private Path createStagingPath() {
    String timestamp = Instant.now().toString().replace(":", "").replace("-", "");
    String random = generateRandom8();
    return warehousePath.resolve(".staging").resolve(timestamp + "_" + random);
}
```

### Step 1: DuckDB Transforms JSON to Staging

```sql
-- For each year (batch_partition_columns: [year]):
-- Staging path: warehouse/.staging/20241215T103000Z_a7b3c9d2/
COPY (
  SELECT * FROM read_json(
    'downloads/regional_income/*_2020_*.json',
    union_by_name=true
  )
)
TO 'warehouse/.staging/20241215T103000Z_a7b3c9d2/'
(FORMAT PARQUET, PARTITION_BY (year, geo_fips))
```

### Step 2: Move Files and Commit to Iceberg

**Critical**: Files must be moved from staging to Iceberg's data location before commit.
If DataFiles reference staging paths and staging is cleaned up by lifecycle rule, table would have invalid references.

```java
Path stagingPath = createStagingPath();

// Load or create Iceberg table
Table table = catalog.loadTable(TableIdentifier.of("db", "regional_income"));

// Move files from staging to Iceberg data location
// S3: Uses server-side copy (efficient, no re-upload)
// Local: Uses atomic rename
Path dataLocation = Paths.get(table.location(), "data");
List<Path> movedFiles = moveFilesToDataLocation(stagingPath, dataLocation);

// Build DataFiles with FINAL paths (not staging paths)
List<DataFile> dataFiles = buildDataFiles(movedFiles, table.spec());

// Atomic overwrite of partition
table.newOverwrite()
    .overwriteByRowFilter(Expressions.equal("year", 2020))
    .addFile(dataFiles)
    .commit();

// Staging directory is now empty - lifecycle rule cleans up empty dirs
// Or delete explicitly since it's just one empty directory
```

**File move implementation:**
```java
List<Path> moveFilesToDataLocation(Path stagingPath, Path dataLocation) {
    List<Path> movedFiles = new ArrayList<>();

    // Walk staging directory, preserving partition structure
    Files.walk(stagingPath)
        .filter(Files::isRegularFile)
        .forEach(stagingFile -> {
            // Preserve partition path: staging/year=2020/geo_fips=01001/data.parquet
            //                       -> data/year=2020/geo_fips=01001/data.parquet
            Path relativePath = stagingPath.relativize(stagingFile);
            Path finalPath = dataLocation.resolve(relativePath);

            // S3: Use TransferManager for efficient server-side copy
            // Local: Use Files.move() for atomic rename
            storageProvider.move(stagingFile, finalPath);
            movedFiles.add(finalPath);
        });

    return movedFiles;
}
```

### Same for Alternates

```java
// Alternate table with different partition spec
Table alternate = catalog.loadTable(TableIdentifier.of("db", "_mv_a7b3c9d2..."));

// Same data, different partition order
table.newOverwrite()
    .overwriteByRowFilter(Expressions.equal("year", 2020))
    .addFile(dataFiles)  // Iceberg handles partition mapping
    .commit();
```

### Error Handling

**Strategy**: Log, retry (configurable attempts), then continue to next batch with non-fatal error.

```java
// Configurable retry settings
int maxRetries = config.getMaxRetries();  // default: 3
long retryDelayMs = config.getRetryDelayMs();  // default: 1000

for (Map<String, String> batch : batches) {
    int attempts = 0;
    boolean success = false;

    while (attempts < maxRetries && !success) {
        attempts++;
        try {
            processBatch(batch);  // DuckDB transform → move → Iceberg commit
            tracker.markProcessed(alternateName, sourceTable, batch, targetPattern);
            success = true;
        } catch (Exception e) {
            LOGGER.warn("Batch {} failed (attempt {}/{}): {}",
                batch, attempts, maxRetries, e.getMessage());

            if (attempts < maxRetries) {
                Thread.sleep(retryDelayMs * attempts);  // Exponential backoff
            }
        }
    }

    if (!success) {
        // Non-fatal: log error and continue to next batch
        LOGGER.error("Batch {} failed after {} attempts, skipping. "
            + "Will retry on next run.", batch, maxRetries);
        // Don't mark as processed - will retry on next run
        // Continue to next batch
    }
}

LOGGER.info("Materialization complete. {} batches succeeded, {} batches failed.",
    successCount, failedCount);
```

**Failure scenarios:**
| Failure Point | Behavior | Recovery |
|---------------|----------|----------|
| DuckDB transform | Retry, staging files orphaned | 24h lifecycle cleanup |
| File move | Retry, partial files in data location | `removeOrphanFiles()` cleanup |
| Iceberg commit | Retry with fresh DataFiles | Iceberg atomicity ensures no partial state |
| All retries exhausted | Log error, skip batch, continue | Batch retried on next run (not marked processed) |

## Incremental Processing with Iceberg

### Tracking Strategy: IncrementalTracker + Iceberg

The design uses **both** the existing `IncrementalTracker` and Iceberg, for complementary purposes:

| Component | Purpose | When Used |
|-----------|---------|-----------|
| `IncrementalTracker` | Fast batch-level lookup ("should I process this batch?") | Before each batch |
| Iceberg | Atomic commits, concurrency control, snapshot isolation | During commit |

**Why keep IncrementalTracker:**
- Already implemented and tested (`DuckDBPartitionStatusStore`)
- Fast local lookup without scanning Iceberg metadata
- Tracks processing status independent of Iceberg table state
- Aligned with existing codebase (`AlternatePartitionMaterializer`, `ParquetReorganizer`)

**Alternative: Pure Iceberg metadata** (shown below for reference, but not recommended):

```java
// Alternative approach: Query Iceberg metadata directly
// This works but is slower than IncrementalTracker for repeated lookups
Table table = catalog.loadTable(tableId);

// Get partition values that exist in Iceberg
Set<Integer> existingYears = table.newScan()
    .select("year")
    .planFiles()
    .stream()
    .map(f -> f.partition().get(0, Integer.class))
    .collect(toSet());

// Compare with source files to find gaps
Set<Integer> sourceYears = listSourceYears("downloads/regional_income/");
Set<Integer> missingYears = Sets.difference(sourceYears, existingYears);
```

**Recommended approach** (as shown in "How It Works" section): Use `IncrementalTracker.isProcessed()` for fast batch-level checks, Iceberg for atomic commits.

### Concurrency Handling (Built-in)

Iceberg uses optimistic concurrency control. When another writer has already committed the same partition, we assume it's the same update (idempotent) and skip to the next batch:

```java
// Multiple processes can try to commit simultaneously
// If conflict detected, assume another writer handled it - log and move on

try {
    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal("year", 2020))
        .addFile(dataFiles)
        .commit();
} catch (CommitFailedException e) {
    // Another process committed first - assume same update, skip to next batch
    LOGGER.warn("Batch year=2020 already committed by another writer, skipping");
    // Continue to next batch - don't retry
}
```

**No custom claim tables needed.** Iceberg handles:
- Conflict detection
- Snapshot isolation (readers see consistent state during writes)

**Log-and-skip rationale**: If two writers are processing the same batch, they're reading the same source data and producing the same output. The first commit wins; the second can safely skip.

### Resumability

Process crashes after 100/250 batches:

1. On restart, `IncrementalTracker` shows which batches completed
2. Loop skips already-processed batches (fast local lookup)
3. Processes only remaining batches
4. Each commit is atomic via Iceberg - no partial states

```java
// On restart, IncrementalTracker provides fast lookup
IncrementalTracker tracker = DuckDBPartitionStatusStore.getInstance(baseDirectory);

for (Integer year : sourceYears) {
    Map<String, String> batch = Map.of("year", String.valueOf(year));
    if (tracker.isProcessed(alternateName, sourceTable, batch)) {
        continue;  // Already done - skip
    }
    processYear(year);  // Atomic commit via Iceberg
    tracker.markProcessed(alternateName, sourceTable, batch, targetPattern);
}
```

## Invalidation Strategy with Iceberg

### Detecting Source Changes

Source file tracking uses the file adapter's existing infrastructure:

- **`IncrementalTracker` interface** - Generic tracking abstraction (`file/src/main/java/.../partition/IncrementalTracker.java`)
- **`DuckDBPartitionStatusStore`** - DuckDB-backed implementation at `{baseDirectory}/.partition_status.duckdb`
- **`partition_status` table** - Tracks by `(alternate_name, incremental_key_values)` with `processed_at` timestamp

When source JSON files change, we:

1. Detect affected partitions (e.g., year=2025)
2. Use Iceberg's partition overwrite to replace data

```java
// Delete and replace partition atomically
table.newOverwrite()
    .overwriteByRowFilter(Expressions.equal("year", 2025))
    .addFile(newDataFiles)
    .commit();
```

### Cascade to Alternates

When source table partition is updated, update corresponding alternates. Alternates are in the same namespace with `_mv_` prefix:

```java
void onSourcePartitionUpdated(String tableName, Map<String, Object> partitionValues) {
    // Find alternates for this table (same namespace, _mv_ prefix)
    List<TableIdentifier> alternates = catalog.listTables(namespace).stream()
        .filter(id -> id.name().startsWith("_mv_") && isAlternateOf(id, tableName))
        .collect(toList());

    // Update each alternate
    for (TableIdentifier altId : alternates) {
        Table alternate = catalog.loadTable(altId);
        reprocessPartition(alternate, partitionValues);
    }
}
```

### Time Travel for Debugging

Iceberg gives us free time travel:

```java
// Query previous version if something went wrong
table.newScan()
    .useSnapshot(previousSnapshotId)
    .planFiles();

// Or roll back
table.manageSnapshots()
    .rollbackTo(previousSnapshotId)
    .commit();
```

### Schema Evolution Handling

When source data schema changes (new columns, type changes), affected batches must be reprocessed.

**Detection mechanism**: Store schema fingerprint alongside batch status in IncrementalTracker.

```java
// Extended partition_status table (add column)
// schema_hash VARCHAR - hash of source schema at processing time

// On batch processing:
String currentSchemaHash = computeSchemaHash(sourceFiles);
String storedSchemaHash = tracker.getSchemaHash(alternateName, batch);

if (storedSchemaHash != null && !storedSchemaHash.equals(currentSchemaHash)) {
    LOGGER.info("Schema changed for batch {}, reprocessing", batch);
    tracker.invalidate(alternateName, batch);
}
```

**Schema hash computation**:
```java
String computeSchemaHash(String sourcePattern) {
    // Use DuckDB to infer schema from source files
    String sql = String.format(
        "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_json('%s', union_by_name=true))",
        sourcePattern);

    // Build deterministic string of column definitions
    StringBuilder sb = new StringBuilder();
    for (Row row : executeQuery(sql)) {
        sb.append(row.getString("column_name"))
          .append(":")
          .append(row.getString("column_type"))
          .append(";");
    }
    return DigestUtils.md5Hex(sb.toString());
}
```

**Behavior**:
| Scenario | Action |
|----------|--------|
| Schema unchanged | Skip batch (already processed) |
| New columns added | Invalidate batch, reprocess to include new columns |
| Column type changed | Invalidate batch, reprocess with new types |
| Columns removed | Invalidate batch, reprocess (Iceberg handles evolution) |

**Iceberg schema evolution**: Iceberg natively supports schema evolution. When reprocessing, the new schema is automatically reconciled with the existing table schema via Iceberg's `updateSchema()` API if needed.

## Query Optimization

Extends existing `AlternatePartitionSelectionRule` to consider partition alternates.

### Automatic Alternate Selection

User queries the source table; optimizer rewrites to best partition alternate:

```sql
-- User query
SELECT * FROM regional_income WHERE geo_fips = '01001'

-- Optimizer rewrites (by_fips alternate has geo_fips as leading partition)
SELECT * FROM _mv_a7b3c9d2... WHERE geo_fips = '01001'
```

### Selection Algorithm

```java
PartitionAlternate selectBestAlternate(Query query, List<PartitionAlternate> availableAlternates) {
    // Prefer alternates where:
    // 1. Leading partition columns match query predicates
    // 2. Fewest files need to be scanned
    // 3. Alternate is fully materialized (status = 'COMPLETE')

    return availableAlternates.stream()
        .filter(alt -> alt.getStatus() == Status.COMPLETE)  // Only use complete alternates
        .filter(alt -> alt.coversPredicates(query.getPredicates()))
        .min(Comparator.comparingInt(PartitionAlternate::estimatedFileScan))
        .orElse(sourceTable);
}
```

## Migration: Reconsolidating Existing Tables

When changing partition structure without re-downloading source data:

### Case Study: `regional_income` Migration

**Goal**: Convert from 700,000 files (5 partition levels) to ~16,000 files (2 partition levels)

```
Before: year/geo_fips_set/tablename/line_code/geo_fips/*.parquet (700K files)
After:  year/geo_fips/*.parquet (~16K files)
```

### Step 1: Update Schema Configuration

```yaml
# econ-schema.yaml - BEFORE
tables:
  regional_income:
    source: "type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/*.parquet"
    partitions:
      keys: [year, geo_fips_set, tablename, line_code, geo_fips]

# econ-schema.yaml - AFTER
tables:
  regional_income:
    source: "type=regional_income/year=*/geo_fips=*/*.parquet"
    partitions:
      keys:
        - name: year
          type: INTEGER
        - name: geo_fips
          type: VARCHAR
    # geo_fips_set, tablename, line_code become regular columns
```

### Step 2: Invalidate Cache (Don't Re-download)

```sql
-- Connect to cache_econ.duckdb
-- Reset parquet conversion status (keeps downloaded JSON intact)
UPDATE cache_entries
SET parquet_converted_at = 0,
    parquet_path = NULL
WHERE cache_key LIKE 'regional_income:%';

-- Verify: should show count of entries to reconvert
SELECT COUNT(*) as entries_to_reconvert
FROM cache_entries
WHERE cache_key LIKE 'regional_income:%'
  AND parquet_converted_at = 0;
```

### Step 3: Delete Old Parquet Files

```bash
# Delete old parquet structure (keeps JSON source files)
rm -rf $BASE_DIR/type=regional_income/

# For S3:
aws s3 rm s3://bucket/type=regional_income/ --recursive
```

### Step 4: Drop Iceberg Tables (if any)

```java
// Drop existing Iceberg tables for this source
Catalog catalog = getCatalog();
TableIdentifier sourceTable = TableIdentifier.of("db", "regional_income");

// Drop source table
if (catalog.tableExists(sourceTable)) {
    catalog.dropTable(sourceTable);
}

// Drop alternates (list tables starting with _mv_ that belong to this source)
for (TableIdentifier id : catalog.listTables(Namespace.of("db"))) {
    if (id.name().startsWith("_mv_") && isAlternateOf(id, "regional_income")) {
        catalog.dropTable(id);
    }
}
```

Or via CLI if using Iceberg REST catalog:
```bash
# Using spark-sql or any Iceberg-compatible tool
DROP TABLE IF EXISTS db.regional_income;
-- Alternates would need to be listed and dropped individually
```

### Step 5: Trigger Reconversion

Run the normal table initialization - it will:
1. See `parquet_converted_at = 0` for all entries
2. Read existing JSON source files
3. Write new Parquet files with consolidated partition structure

```java
// The conversion will now use the new partition schema:
// PARTITION_BY (year, geo_fips) instead of 5 columns
```

### Migration Script (Combined)

```bash
#!/bin/bash
# migrate_regional_income.sh

BASE_DIR="${1:-$HOME/.aperio/econ}"
CACHE_DB="$BASE_DIR/cache_econ.duckdb"
WAREHOUSE="$BASE_DIR/warehouse"

echo "=== Migrating regional_income to consolidated partitions ==="

# Step 1: Invalidate parquet conversion status
echo "Step 1: Invalidating parquet conversion status..."
duckdb "$CACHE_DB" <<EOF
UPDATE cache_entries
SET parquet_converted_at = 0, parquet_path = NULL
WHERE cache_key LIKE 'regional_income:%';

SELECT COUNT(*) || ' entries will be reconverted' as status
FROM cache_entries
WHERE cache_key LIKE 'regional_income:%';
EOF

# Step 2: Delete old parquet files and Iceberg metadata
echo "Step 2: Deleting old parquet files and Iceberg metadata..."
rm -rf "$BASE_DIR/type=regional_income/"
rm -rf "$WAREHOUSE/regional_income/"
# Delete any alternates for this table (identified by _mv_ prefix)
# Note: In production, use Iceberg catalog API to properly drop tables
echo "  Deleted type=regional_income/ and Iceberg table directories"

echo "=== Migration prepared. Run table initialization to reconvert. ==="
echo "Note: Iceberg tables will be recreated with new partition spec on first query."
```

### Verification After Migration

```sql
-- After reconversion completes, verify new structure via Iceberg:
-- Using DuckDB with Iceberg extension or Spark SQL

-- Option 1: Query Iceberg table directly
SELECT COUNT(*) as row_count FROM db.regional_income;

-- Option 2: Verify partition structure via Iceberg metadata
SELECT * FROM db.regional_income.partitions;

-- Option 3: DuckDB direct parquet read (for debugging)
SELECT
  COUNT(*) as file_count,
  COUNT(DISTINCT year) as years,
  COUNT(DISTINCT geo_fips) as fips_codes
FROM read_parquet('warehouse/regional_income/data/**/*.parquet', hive_partitioning=true);

-- Expected: ~16K files, ~5 years, ~3200 FIPS codes
```

## Implementation Phases

### Phase 1: Iceberg Infrastructure

**Scope**: Extend existing Iceberg infrastructure for write support

**Existing code to leverage**:
- `IcebergCatalogManager.java` - Already supports Hadoop/REST catalogs (extend for write operations)
- `IcebergStorageProvider.java` - Read-only currently (extend for write paths)
- DuckDB `iceberg_scan()` - Already used for reading Iceberg tables

**Dependencies** (already in build.gradle.kts):
```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
    <version>1.4.3</version>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-parquet</artifactId>
    <version>1.4.3</version>
</dependency>
```

**Files to create/modify**:
- Extend: `IcebergCatalogManager.java` - Add table creation and write methods
- New: `IcebergTableWriter.java` - Write DuckDB output to Iceberg tables
- `FileSchema.java` - Initialize Iceberg catalog for writes

**Deliverables**:
- [ ] Extend IcebergCatalogManager with createTable(), dropTable() methods
- [ ] Table creation with partition specs
- [ ] Basic write path: DuckDB → staging Parquet → Iceberg commit
- [ ] Integration test: create and query Iceberg table

### Phase 2: Batched Materialization with Iceberg

**Scope**: Implement batched JSON→Iceberg flow

**Files to modify**:
- New: `IcebergMaterializer.java` - Batched materialization to Iceberg
- `PartitionedTableConfig.java` - Add `ingestion` config section

**Deliverables**:
- [ ] `batch_partition_columns` support with DuckDB transformation
- [ ] Atomic partition overwrite via Iceberg
- [ ] Memory estimation warning
- [ ] Integration test: `regional_income` without OOM

### Phase 3: Partition Alternates

**Scope**: Create partition alternates as separate Iceberg tables

**Files to modify**:
- `PartitionedTableConfig.java` - Add `partition_alternates` config
- `IcebergMaterializer.java` - Support multiple tables per source

**Deliverables**:
- [ ] Config parsing for `partition_alternates`
- [ ] Random name generation (`_mv_xxx`)
- [ ] Create alternate Iceberg tables with different partition specs
- [ ] Same materializer creates source + alternates

### Phase 4: Calcite-Iceberg Integration

**Scope**: Expose Iceberg tables to Calcite planner

**Files to modify**:
- `FileSchema.java` - Load tables from Iceberg catalog, maintain alternate registry
- `AlternatePartitionSelectionRule.java` - Implement alternate registry lookup (existing TODO at line 179)

#### Alternate Registry Mechanism Design

The registry maps source tables to their alternates. It is populated from config at schema initialization time.

**Registry data structure** (in `FileSchema`):
```java
public class FileSchema extends AbstractSchema {
    // Map: source table name -> list of alternates
    private final Map<String, List<AlternateInfo>> alternateRegistry = new HashMap<>();

    // Populated during schema initialization from PartitionedTableConfig
    public void registerAlternate(String sourceTable, AlternateInfo alternate) {
        alternateRegistry.computeIfAbsent(sourceTable, k -> new ArrayList<>())
            .add(alternate);
    }

    // Called by AlternatePartitionSelectionRule
    public List<AlternateInfo> getAlternates(String sourceTable) {
        return alternateRegistry.getOrDefault(sourceTable, Collections.emptyList());
    }
}

// Alternate info needed by planner
public class AlternateInfo {
    final String icebergTableId;      // "_mv_a7b3c9d2" - Iceberg table identifier
    final List<String> partitionKeys; // ["geo_fips", "year"] - for pruning evaluation
    final String description;         // "by_fips" - human-readable, for logging only
}
```

**Registration flow** (during schema initialization):
```java
// In FileSchema initialization
for (PartitionedTableConfig tableConfig : configs) {
    if (tableConfig.getAlternatePartitions() != null) {
        for (AlternatePartitionConfig altConfig : tableConfig.getAlternatePartitions()) {
            // Generate or retrieve the Iceberg table ID
            String icebergTableId = generateOrRetrieveTableId(altConfig);

            AlternateInfo info = new AlternateInfo(
                icebergTableId,
                altConfig.getPartition().getColumns(),
                altConfig.getName()  // description only
            );

            registerAlternate(tableConfig.getName(), info);
        }
    }
}
```

**Rule lookup** (in `AlternatePartitionSelectionRule`):
```java
private List<AlternatePartitionInfo> getAlternatePartitions(RelOptTable table) {
    // Navigate: table -> unwrap to FileTable -> get schema -> get registry
    FileTable fileTable = table.unwrap(FileTable.class);
    if (fileTable == null) {
        return Collections.emptyList();  // Not a file adapter table
    }

    FileSchema fileSchema = fileTable.getSchema();
    String sourceTableName = table.getQualifiedName().get(table.getQualifiedName().size() - 1);

    return fileSchema.getAlternates(sourceTableName).stream()
        .map(alt -> new AlternatePartitionInfo(alt.icebergTableId, alt.partitionKeys))
        .collect(toList());
}
```

**Interface definition** (explicit contract between components):
```java
/**
 * Registry for alternate partition tables.
 * Implemented by FileSchema, accessed by AlternatePartitionSelectionRule.
 */
public interface AlternateRegistry {
    /**
     * Get all alternates for a source table.
     * @param sourceTable The user-facing table name (e.g., "regional_income")
     * @return List of alternates, empty if none configured
     */
    List<AlternateInfo> getAlternates(String sourceTable);

    /**
     * Register an alternate for a source table.
     * Called during schema initialization from config.
     */
    void registerAlternate(String sourceTable, AlternateInfo alternate);
}

// FileTable provides access to schema
public class FileTable extends AbstractTable {
    private final FileSchema schema;

    public FileSchema getSchema() {
        return schema;
    }
}

// FileSchema implements the registry
public class FileSchema extends AbstractSchema implements AlternateRegistry {
    private final Map<String, List<AlternateInfo>> alternateRegistry = new HashMap<>();

    @Override
    public void registerAlternate(String sourceTable, AlternateInfo alternate) {
        alternateRegistry.computeIfAbsent(sourceTable, k -> new ArrayList<>())
            .add(alternate);
    }

    @Override
    public List<AlternateInfo> getAlternates(String sourceTable) {
        return alternateRegistry.getOrDefault(sourceTable, Collections.emptyList());
    }
}
```

**Deliverables**:
- [ ] Add `alternateRegistry` to `FileSchema`
- [ ] Populate registry from config during schema initialization
- [ ] Implement `getAlternatePartitions()` to query registry
- [ ] Iceberg tables visible in Calcite schema
- [ ] Planner considers alternates for query rewriting
- [ ] DuckDB can query via `iceberg_scan()`

### Phase 5: Incremental Updates & Maintenance

**Scope**: Leverage Iceberg features for updates and cleanup

**Maintenance timing**: Run at end of ingestion, before connection is returned to caller.

```java
// At end of materialization (in FileSchema or IcebergMaterializer)
void runMaintenance(Table table) {
    // Expire old snapshots (keep last 7 days)
    table.expireSnapshots()
        .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
        .commit();

    // Remove orphan files from failed commits
    Actions.forTable(table)
        .removeOrphanFiles()
        .olderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
        .execute();

    LOGGER.info("Iceberg maintenance complete for {}", table.name());
}
```

**Deliverables**:
- [ ] Detect source changes and cascade to alternates
- [ ] Use Iceberg partition overwrite for updates
- [ ] Run expireSnapshots() at end of ingestion
- [ ] Run removeOrphanFiles() at end of ingestion
- [ ] Time travel queries for debugging

### Testing Strategy

**TODO**: Engage test-strategist to design comprehensive testing approach covering:

- [ ] Unit tests for individual components (IncrementalTracker, IcebergMaterializer, AlternateRegistry)
- [ ] Integration tests for end-to-end flows (JSON → Iceberg table → query)
- [ ] Failure scenario tests (OOM simulation, commit failures, concurrent writers)
- [ ] Performance benchmarks (batch processing time, memory usage)
- [ ] Regression tests for existing functionality

## Metrics & Monitoring

### Key Metrics to Track

| Metric | Description | Target |
|--------|-------------|--------|
| `partition_alternate.batch.duration_ms` | Time per batch | < 60s |
| `partition_alternate.batch.files_processed` | Files per batch | < 1000 |
| `partition_alternate.batch.bytes_processed` | Data per batch | < 1GB |
| `partition_alternate.total_duration_ms` | Full alternate time | Baseline + 20% |
| `partition_alternate.storage.amplification` | Total size / source size | < 3x |

### Logging

```
INFO  Materializing partition alternate 'by_fips' for table 'regional_income'
WARN  Batch {year=2020} estimated at 1.5 GB, threshold is 1 GB. Consider finer batch_partition_columns.
INFO    Batch 1/5: {year=2020} - 1.2s, 3200 files
INFO    Batch 2/5: {year=2021} - 1.1s, 3200 files
...
INFO  Partition alternate 'by_fips' complete: 5 batches in 6m 12s
INFO  Iceberg snapshot committed: snap-12345
```

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Storage costs increase | Medium | Document 2-3x overhead, use `expireSnapshots()` |
| Config errors cause data issues | High | Strict validation, dry-run mode |
| Iceberg catalog corruption | Low | Iceberg metadata is designed for durability; can recover from snapshots |
| Batch too large for memory | High | Memory estimation warning before processing |
| Orphan data files after failed commits | Low | Iceberg's `removeOrphanFiles()` maintenance action |
| Concurrent materialization conflicts | Low | Iceberg handles via optimistic concurrency |
| Incomplete alternates used by planner | Low | Only committed snapshots are visible (Iceberg guarantee) |

**Iceberg reduces risk**: Many concerns from custom implementation are handled by Iceberg's built-in features.

## Design Decisions (Resolved)

### 1. Table Management: Apache Iceberg

**Decision**: Use Apache Iceberg for table management instead of custom metadata tables.

**Rationale**:
- We were reinventing what Iceberg already provides (catalog, snapshots, concurrency)
- Iceberg is an industry standard with strong Java support
- Atomic commits, optimistic concurrency, time travel built-in
- No custom `alternate_registry` or `materialization_claims` tables needed

### 2. Hybrid Architecture: DuckDB + Iceberg

**Decision**: Use DuckDB for transformation, Iceberg for table management.

```
JSON → DuckDB (transform) → temp Parquet → Iceberg (commit)
```

**Rationale**:
- DuckDB excels at JSON parsing and transformation
- Iceberg excels at table management and ACID guarantees
- Best of both worlds

### 3. Batch Enumeration Strategy

**Decision**: Use glob-based directory listing to discover batch combinations from source JSON.

**Rationale**:
- Glob listing is fast (metadata only)
- Works for both local and S3 storage
- Compare with Iceberg table partitions to find gaps

### 4. Partial Failure Handling

**Decision**: Each batch commit is atomic via Iceberg. Failed batches don't affect others.

```java
try {
    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal("year", year))
        .addFile(dataFiles)
        .commit();
} catch (Exception e) {
    LOGGER.warn("Batch {} failed, will retry on next run", batch);
    // Iceberg guarantees no partial state - either committed or not
}
```

**Rationale**:
- Iceberg atomic commits prevent partial states
- Failed batches simply don't appear in table
- Next run will detect missing partitions and retry

### 5. Cleanup Policy

**Decision**: Use Iceberg's built-in maintenance operations.

```java
// Remove old snapshots (keeps last N days)
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
    .commit();

// Remove orphan files from failed commits
Actions.forTable(table)
    .removeOrphanFiles()
    .execute();
```

**Rationale**:
- Iceberg tracks all files in manifests
- `expireSnapshots()` cleans up old versions
- `removeOrphanFiles()` cleans up failed commit artifacts

### 6. Alternate Schema Visibility

**Decision**: Alternates are Iceberg tables in the catalog, visible to Calcite.

**Rationale**:
- Iceberg guarantees only committed data is visible
- No need for custom `status = 'COMPLETE'` check
- Catalog lists all tables; filter by `_mv_` prefix for internal alternates

```java
// Load alternates from Iceberg catalog
List<TableIdentifier> alternates = catalog.listTables(namespace).stream()
    .filter(id -> id.name().startsWith("_mv_"))
    .collect(toList());
```

### 7. Storage Budget

**Decision**: Up to 3x storage overhead is acceptable.

**Rationale**:
- Each partition alternate is a full copy of the data with different partitioning
- With 2 layouts (source + 1 alternate), expect ~2x storage
- With multiple alternates, could reach 3x
- Cloud storage is cheap; query performance improvement justifies cost
- Iceberg's `expireSnapshots()` helps manage storage over time

### 8. Compression

**Decision**: Use DuckDB default (Snappy). No configuration needed.

**Rationale**:
- Snappy is fast and well-supported
- Compression ratio differences (Snappy vs Zstandard) are minor for Parquet
- Not worth adding configuration complexity

### 9. Batch Key Embedding Strategy

**Decision**: Automatically embed batch keys in filenames when `batch_partition_columns` ⊄ partition columns. No explicit configuration needed.

**Rationale**:
- If `batch_partition_columns` aren't in partition columns, batches would overwrite each other
- Making it automatic prevents configuration errors
- User just specifies desired partition and `batch_partition_columns`; system handles the rest

**Behavior**:
```
batch_partition_columns ⊆ partition  →  Batch keys in directory structure (default)
batch_partition_columns ⊄ partition  →  Batch keys in filename (automatic)
```

**Trade-offs** (determined by partition_by choice, not explicit config):
| `partition_by` includes batch keys? | Directory structure | Query behavior |
|-------------------------------------|---------------------|----------------|
| Yes: `[tablename, line_code, year]` | `.../year=2020/` | Can prune by year |
| No: `[tablename, line_code]` | `year_2020_*.parquet` | Scans all years |

**Implementation**: Use DuckDB's `FILENAME_PATTERN` option with batch key values embedded when needed.

## Rejected Alternatives

### DuckDB Native Iceberg Write Support

**What it is**: DuckDB 1.4.0+ includes native Iceberg write support via the `iceberg` extension:

```sql
-- Attach to Iceberg catalog
ATTACH 'warehouse_name' AS iceberg_catalog (TYPE iceberg, ...);

-- Create table and insert
CREATE TABLE iceberg_catalog.default.my_table (col1 INTEGER, col2 VARCHAR);
INSERT INTO iceberg_catalog.default.my_table SELECT * FROM read_json('...');
```

**Why considered**: Single tool (DuckDB) for both transformation and Iceberg writes would simplify architecture.

**Why rejected**:

1. **Partitioned table limitations**: UPDATE/DELETE only work on non-partitioned tables (DuckDB 1.4.2). Partition alternates require different partition schemes.

2. **Catalog support**: DuckDB Iceberg writes require REST catalog (Polaris, Lakekeeper) or S3 Tables. Does not support HadoopCatalog which is used in existing codebase.

3. **Maturity**: Iceberg Java API is more mature and battle-tested for production write workloads.

4. **Existing infrastructure**: Codebase already has `IcebergCatalogManager` using Java API. Extending it is lower risk than switching to DuckDB-only approach.

**Sources**:
- [DuckDB Iceberg Writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview)

**Future consideration**: If DuckDB adds full partition support and HadoopCatalog compatibility, this could be revisited to eliminate the staging directory step.

## Design Clarifications

The following points clarify design aspects that may initially appear problematic but are intentional:

### Components Serve Distinct Purposes (Not Redundant State)

The design uses three components that might appear to duplicate state, but each serves a distinct purpose:

| Component | Purpose | What It Tracks |
|-----------|---------|----------------|
| **IncrementalTracker** | Fast batch-level lookup | "Have I processed batch X from source?" |
| **Iceberg catalog** | Table state management | "What files are committed in the table?" |
| **Staging directory** | Ephemeral workspace | Nothing persistent - just temp files during processing |

These are **not** redundant. IncrementalTracker is a processing checkpoint; Iceberg is the actual data store; staging is temporary workspace cleaned up by lifecycle rules.

**Synchronization**: IncrementalTracker is only marked AFTER Iceberg commit succeeds, ensuring they stay consistent.

### `source` Pattern vs Iceberg Data Location

The config `source` field describes **input** files (raw JSON or existing Parquet to be processed). The Iceberg `data/` directory is the **output** where Iceberg manages committed table files.

```
source: "downloads/*.json"     →  Input (raw files to process)
           ↓
      DuckDB transform
           ↓
      Staging directory
           ↓
      Iceberg data/            →  Output (managed by Iceberg catalog)
```

These are different stages in the pipeline, not incompatible models.

### Alternate `name` Field is Description Only

The `name` field in alternate config (e.g., `name: by_fips`) is **purely for human readability** in logs and documentation. It is not a lookup key and does not need to be persisted separately.

- The actual identifier is the Iceberg table ID (`_mv_xxx`)
- The registry maps `source_table` → `List<AlternateInfo>` using Iceberg table IDs
- The Iceberg catalog persists the `_mv_xxx` tables; on restart, alternates are re-registered from config

### No Migration Path Needed

There is no "adopt existing Parquet files into Iceberg" flow. The design philosophy is **rebuild from source**:

1. Delete old Parquet files
2. Drop old Iceberg tables (if any)
3. Re-materialize from source files (JSON or existing Parquet)

The Migration section documents this approach. This is simpler and avoids complex state reconciliation.

### Alternates Are Optimization Only

Partition alternates exist purely for query performance optimization. If an alternate fails to materialize:

- The source table remains authoritative and fully functional
- Queries still work correctly, just without the optimization benefit
- Failed batches are not marked processed → automatically retry on next run
- No reconciliation or manual intervention required

This is intentional: alternates should never block or corrupt the primary data path.

## References

### External Documentation
- Apache Iceberg Java API: https://iceberg.apache.org/docs/latest/java-api-quickstart/
- Iceberg Table Maintenance: https://iceberg.apache.org/docs/latest/maintenance/
- Iceberg Partition Spec: https://iceberg.apache.org/spec/#partitioning
- DuckDB partitioned writes: https://duckdb.org/docs/data/partitioning/partitioned_writes

### Existing File Adapter Code
- `IncrementalTracker.java` - Generic tracking interface
- `DuckDBPartitionStatusStore.java` - DuckDB-backed tracker implementation
- `create_partition_status.sql` - Schema for partition_status table
- `PartitionedTableConfig.java` - Config classes including `AlternatePartitionConfig`
- `AlternatePartitionMaterializer.java` - Current materializer (to be extended for Iceberg)
- `ParquetReorganizer.java` - Current reorganization logic
- `IcebergCatalogManager.java` - Existing Iceberg catalog support (to be extended)

## Appendix: Terminology

| Term | Definition |
|------|------------|
| **Partition Alternate** | An alternate partition layout of the same data for query optimization |
| **Source Table** | The primary, user-facing table with its default partition layout |
| **Physical Name** | The random `_mv_xxx` name used in Iceberg catalog |
| **Logical Name** | The human-readable name (`by_fips`) used in config and logging |
| **Batch** | A subset of data processed together to prevent OOM (e.g., one year) |
| **`batch_partition_columns`** | Config specifying which columns define batch boundaries for OOM prevention |
| **`incremental_keys`** | Config specifying which columns to use for incremental tracking (skip already-processed) |
| **`IncrementalTracker`** | Interface for tracking incremental processing (`file/.../partition/IncrementalTracker.java`) |
| **`DuckDBPartitionStatusStore`** | Implementation of IncrementalTracker using DuckDB at `{baseDirectory}/.partition_status.duckdb` |
| **Staging Directory** | Temp location in warehouse (`{warehouse}/.staging/{timestamp}_{random}/`) for DuckDB output before Iceberg commit; isolates concurrent writers; cleaned up after 24h |
| **Iceberg Catalog** | Central registry of Iceberg tables (HadoopCatalog or JdbcCatalog) |
| **Snapshot** | Iceberg's immutable point-in-time view of table data |
| **Partition Spec** | Iceberg's definition of how table data is partitioned |
| **DataFile** | Iceberg metadata representing a single Parquet file |
| **Manifest** | Iceberg file listing DataFiles belonging to a snapshot |
