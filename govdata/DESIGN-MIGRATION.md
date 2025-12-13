# Govdata Migration to File Adapter ETL - Design Document

## Overview

This document describes the migration of govdata from a monolithic ETL adapter to a thin schema-generation layer that delegates execution to the file adapter's new ETL capabilities.

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ govdata (monolithic)                                            │
│                                                                 │
│ ├── API Knowledge                                               │
│ │   ├── BEA API structure, table codes                         │
│ │   ├── BLS API structure, series IDs                          │
│ │   ├── Census API structure                                   │
│ │   └── FRED API structure                                     │
│ │                                                               │
│ ├── Schema Definitions (YAML)                                   │
│ │   ├── econ-schema.yaml                                       │
│ │   ├── geo-schema.yaml                                        │
│ │   └── Table/column definitions                               │
│ │                                                               │
│ ├── ETL Execution (Java)                                        │
│ │   ├── AbstractGovDataDownloader (2000+ lines)                │
│ │   ├── HTTP fetching with pagination                          │
│ │   ├── JSON → Parquet conversion                              │
│ │   ├── Dimension iteration                                    │
│ │   ├── Hive partitioning                                      │
│ │   └── Alternate partition reorganization                     │
│ │                                                               │
│ └── Output: Partitioned Parquet in S3                          │
└─────────────────────────────────────────────────────────────────┘
```

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ govdata (schema generator)                                      │
│                                                                 │
│ ├── API Knowledge (retained)                                    │
│ │   ├── BEA API structure, table codes                         │
│ │   ├── BLS API structure, series IDs                          │
│ │   ├── Census API structure                                   │
│ │   └── FRED API structure                                     │
│ │                                                               │
│ ├── Schema Generator                                            │
│ │   ├── Generates file-adapter-compatible YAML                 │
│ │   ├── Maps business concepts to API parameters               │
│ │   └── Could be LLM-assisted in future                        │
│ │                                                               │
│ └── Output: Schema YAML files                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ file adapter (ETL engine)                                       │
│                                                                 │
│ ├── HttpSource: API fetching                                   │
│ ├── DimensionIterator: Expand combinations                     │
│ ├── HiveParquetWriter: Materialize to parquet                  │
│ ├── ParquetReorganizer: Create alternate partitions            │
│ │                                                               │
│ └── Output: Partitioned Parquet in S3                          │
└─────────────────────────────────────────────────────────────────┘
```

## Migration Benefits

| Aspect | Before | After |
|--------|--------|-------|
| ETL Code Location | govdata (2000+ lines) | file adapter (reusable) |
| Schema Format | govdata-specific | Standard file adapter |
| Other Adapters | Duplicate ETL logic | Reuse file adapter |
| Testing | Complex integration tests | Unit test each component |
| Maintenance | Fix bugs in multiple places | Single ETL engine |

---

## Migration Phases

### Phase 0: Current State (Completed)
- ParquetReorganizer moved to file adapter ✓
- govdata delegates reorganization to file adapter ✓
- Threads configuration is schema-driven ✓

### Phase 1: Extract HiveParquetWriter

**Goal**: Move JSON → Parquet conversion from govdata to file adapter.

**Current govdata code** (AbstractGovDataDownloader):
```java
// ~200 lines of conversion logic
private void convertCachedJsonToParquetViaDuckDB(String tableName,
    List<TableColumn> columns, String missingValueIndicator,
    String jsonPath, String parquetPath, String dataPath) {
    // Build DuckDB COPY statement
    // Handle type coercion
    // Write partitioned output
}
```

**New file adapter component**:
```java
public class HiveParquetWriter {
    public void write(WriteConfig config) {
        // Reusable by any adapter
    }
}
```

**Migration steps**:
1. Create `HiveParquetWriter` in file adapter
2. Add schema support for `materialize` configuration
3. Update govdata to call `HiveParquetWriter`
4. Remove duplicate code from govdata

**Validation**:
- Existing tests pass
- Output parquet files identical
- Performance unchanged or improved

### Phase 2: Extract DimensionIterator

**Goal**: Move dimension expansion logic to file adapter.

**Current govdata code**:
```java
// Scattered across multiple methods
private List<String> getYearRange() { ... }
private List<String> getFrequencyValues() { ... }
private void iterateDimensions(Consumer<Map<String, String>> handler) { ... }
```

**New file adapter component**:
```java
public class DimensionIterator {
    public List<Map<String, String>> expand(DimensionsConfig config) {
        // Generic dimension expansion
    }
}
```

**Schema changes**:
```yaml
# Before (govdata-specific)
dimensions:
  year: {type: yearRange, minYear: 2020}

# After (standard file adapter)
dimensions:
  year:
    type: range
    start: "{env:GOVDATA_START_YEAR}"
    end: "{env:GOVDATA_END_YEAR}"
```

### Phase 3: Extract HttpSource

**Goal**: Move API fetching logic to file adapter.

**Current govdata code**:
```java
// AbstractGovDataDownloader
protected String fetchFromApi(String url, Map<String, String> params) {
    // HTTP client setup
    // Rate limiting
    // Retry logic
    // Response parsing
}
```

**New file adapter component**:
```java
public class HttpSource implements DataSource {
    public Stream<Map<String, Object>> fetch(Map<String, String> variables) {
        // Generic HTTP fetching
    }
}
```

**Schema changes**:
```yaml
# Before (govdata executes this)
download:
  url: "https://apps.bea.gov/api/data"
  parameters:
    TableName: CAINC1

# After (file adapter executes this)
source:
  type: http
  url: "https://apps.bea.gov/api/data"
  parameters:
    TableName: CAINC1
```

### Phase 4: Schema-Only Govdata

**Goal**: Govdata becomes pure schema generation.

**Final govdata structure**:
```
govdata/
├── src/main/resources/
│   ├── econ/
│   │   ├── bea-tables.yaml      # BEA table definitions
│   │   ├── bls-series.yaml      # BLS series definitions
│   │   └── fred-series.yaml     # FRED series definitions
│   └── geo/
│       ├── tiger-layers.yaml    # TIGER layer definitions
│       └── census-tables.yaml   # Census table definitions
│
├── src/main/java/
│   └── SchemaGenerator.java     # Generates file adapter schemas
│
└── generated/
    └── file-adapter-schema.yaml # Output for file adapter
```

**SchemaGenerator**:
```java
public class SchemaGenerator {

    public FileAdapterSchema generate(GovdataConfig config) {
        List<TableConfig> tables = new ArrayList<>();

        for (BeaTable table : config.getBeaTables()) {
            tables.add(TableConfig.builder()
                .name(table.getName())
                .source(HttpSourceConfig.builder()
                    .url("https://apps.bea.gov/api/data")
                    .parameters(Map.of(
                        "TableName", table.getTableCode(),
                        "Year", "{year}",
                        "GeoFips", "{geo}"
                    ))
                    .build())
                .dimensions(table.getDimensions())
                .materialize(MaterializeConfig.builder()
                    .pattern("type=" + table.getName() + "/year=*/geo=*/")
                    .partitionBy(List.of("year", "geo"))
                    .build())
                .build());
        }

        return new FileAdapterSchema(tables);
    }
}
```

---

## Detailed Code Migration

### AbstractGovDataDownloader Decomposition

| Method | Lines | Destination | Phase |
|--------|-------|-------------|-------|
| `convertCachedJsonToParquetViaDuckDB` | ~150 | HiveParquetWriter | 1 |
| `buildDuckDBCopySql` | ~80 | HiveParquetWriter | 1 |
| `getYearRange` / dimension methods | ~100 | DimensionIterator | 2 |
| `fetchFromApi` | ~120 | HttpSource | 3 |
| `handlePagination` | ~80 | HttpSource | 3 |
| `cacheResponse` | ~50 | HttpSource | 3 |
| `reorganizeAlternatePartitions` | ~200 | Already migrated ✓ | 0 |
| Schema parsing | ~300 | Keep in govdata | - |
| API-specific logic | ~400 | Keep in govdata | - |

### Schema Translation

**Current econ-schema.yaml** (govdata format):
```yaml
tables:
  - name: regional_income
    pattern: "type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/*.parquet"
    download:
      url: "https://apps.bea.gov/api/data"
      method: GetData
      parameters:
        datasetname: Regional
        TableName: "{tablename}"
        LineCode: "{line_code}"
        Year: "{year}"
        GeoFips: "{geo}"
      response:
        dataPath: "$.BEAAPI.Results.Data"
    dimensions:
      year: {type: yearRange, minYear: 1990}
      tablename: [CAINC1, CAINC4, CAINC5N]
      line_code: [1, 2, 3]
      geo: [STATE, COUNTY, MSA]
    columns:
      - {name: GeoFips, type: VARCHAR}
      - {name: GeoName, type: VARCHAR}
      - {name: TimePeriod, type: INTEGER}
      - {name: DataValue, type: VARCHAR}
    alternate_partitions:
      - name: regional_income_by_state
        pattern: "type=regional_income_by_state/geo_fips=*/*.parquet"
        partition:
          columnDefinitions:
            - {name: geo_fips, type: VARCHAR, sourceColumn: GeoFips}
        batch_partition_columns: [year, geo_fips_set]
        threads: 4
```

**Target file-adapter-schema.yaml** (standard format):
```yaml
tables:
  - name: regional_income

    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      parameters:
        method: GetData
        datasetname: Regional
        TableName: "{tablename}"
        LineCode: "{line_code}"
        Year: "{year}"
        GeoFips: "{geo}"
        UserID: "{env:BEA_API_KEY}"
      response:
        format: json
        dataPath: "$.BEAAPI.Results.Data"
      rateLimit:
        requestsPerSecond: 10

    dimensions:
      year:
        type: range
        start: "{env:GOVDATA_START_YEAR}"
        end: "{env:GOVDATA_END_YEAR}"
      tablename:
        type: list
        values: [CAINC1, CAINC4, CAINC5N]
      line_code:
        type: list
        values: [1, 2, 3]
      geo:
        type: list
        values: [STATE, COUNTY, MSA]

    columns:
      - {name: geo_fips, type: VARCHAR, source: GeoFips}
      - {name: geo_name, type: VARCHAR, source: GeoName}
      - {name: year, type: INTEGER, source: TimePeriod}
      - {name: value, type: VARCHAR, source: DataValue}
      - {name: geo_fips_set, type: VARCHAR, expression: "CASE WHEN LENGTH(geo_fips) = 2 THEN 'STATE' WHEN LENGTH(geo_fips) = 5 THEN 'COUNTY' ELSE 'OTHER' END"}

    materialize:
      enabled: true
      output:
        pattern: "type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/"
      partition:
        columns: [year, geo_fips_set, tablename, line_code]
        batchBy: [year, geo]
      options:
        threads: 2

    alternate_partitions:
      - name: regional_income_by_state
        pattern: "type=regional_income_by_state/geo_fips=*/*.parquet"
        partition:
          columns: [geo_fips]
        column_mappings:
          geo_fips: GeoFips
        batch_partition_columns: [year, geo_fips_set]
        threads: 4
```

---

## Testing Strategy

### Unit Tests (file adapter)
```java
class HiveParquetWriterTest {
    @Test void writesPartitionedParquet() { ... }
    @Test void handlesBatching() { ... }
    @Test void appliesColumnMappings() { ... }
}

class DimensionIteratorTest {
    @Test void expandsRangeType() { ... }
    @Test void expandsListType() { ... }
    @Test void buildsCartesianProduct() { ... }
}

class HttpSourceTest {
    @Test void fetchesWithPagination() { ... }
    @Test void handlesRateLimiting() { ... }
    @Test void cachesResponses() { ... }
}
```

### Integration Tests (govdata)
```java
class GovdataMigrationTest {
    @Test void generatedSchemaProducesSameOutput() {
        // Run old govdata ETL
        Set<String> oldFiles = runOldEtl();

        // Generate schema and run via file adapter
        Schema schema = generator.generate(config);
        Set<String> newFiles = runFileAdapterEtl(schema);

        // Compare outputs
        assertEquals(oldFiles, newFiles);
    }
}
```

### Validation Criteria
1. Output files are byte-identical (or semantically equivalent)
2. Query results unchanged
3. Performance within 10% of original
4. All existing tests pass

---

## Rollback Plan

Each phase is independently reversible:

### Phase 1 Rollback
```java
// Feature flag
if (useFileAdapterWriter) {
    fileAdapter.write(config);
} else {
    legacyConvertToParquet(config);  // Keep old code
}
```

### Phase 2 Rollback
```java
if (useFileAdapterDimensions) {
    fileAdapter.expandDimensions(config);
} else {
    legacyIterateDimensions(config);
}
```

### Full Rollback
- Keep old govdata code in deprecated package
- Feature flag to switch between old and new
- Remove after 2 release cycles

---

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: HiveParquetWriter | 2-3 days | None |
| Phase 2: DimensionIterator | 1-2 days | Phase 1 |
| Phase 3: HttpSource | 3-4 days | Phase 2 |
| Phase 4: Schema-Only Govdata | 2-3 days | Phase 3 |
| Testing & Validation | 2-3 days | All phases |
| **Total** | **10-15 days** | |

---

## Business Logic Hooks

### Why Hooks Are Needed

Generic ETL engines cannot anticipate all API quirks. Govdata APIs have specific behaviors:

| API | Quirk | Hook Solution |
|-----|-------|---------------|
| BEA | `(NA)` and `(D)` value encoding | `RowTransformer` to normalize |
| BEA | Nested `BEAAPI.Results.Data` response | `ResponseTransformer` to extract |
| BLS | Rate limit headers in response | `ResponseTransformer` to handle |
| Census | Variable name mapping | `DimensionResolver` for dynamic vars |
| FRED | Observation date formats | `RowTransformer` to standardize |

### Govdata Hook Implementations

After migration, govdata retains these hook implementations:

```
govdata/src/main/java/org/apache/calcite/adapter/govdata/
├── hooks/
│   ├── BeaResponseTransformer.java      # BEA-specific response handling
│   ├── BeaValueNormalizer.java          # Handle (NA), (D), etc.
│   ├── BlsResponseTransformer.java      # BLS rate limit handling
│   ├── CensusVariableResolver.java      # Dynamic census variable discovery
│   ├── FredDateNormalizer.java          # FRED date format standardization
│   └── GeoFipsValidator.java            # Validate geo FIPS codes
├── schema/
│   └── SchemaGenerator.java             # Generate file adapter schemas
└── resources/
    ├── bea-tables.yaml                  # BEA table definitions
    ├── bls-series.yaml                  # BLS series definitions
    └── census-tables.yaml               # Census table definitions
```

### Hook Registration

Govdata registers hooks via Java SPI:

```
# META-INF/services/org.apache.calcite.adapter.file.etl.ResponseTransformer
org.apache.calcite.adapter.govdata.hooks.BeaResponseTransformer
org.apache.calcite.adapter.govdata.hooks.BlsResponseTransformer

# META-INF/services/org.apache.calcite.adapter.file.etl.RowTransformer
org.apache.calcite.adapter.govdata.hooks.BeaValueNormalizer
org.apache.calcite.adapter.govdata.hooks.FredDateNormalizer

# META-INF/services/org.apache.calcite.adapter.file.etl.Validator
org.apache.calcite.adapter.govdata.hooks.GeoFipsValidator

# META-INF/services/org.apache.calcite.adapter.file.etl.DimensionResolver
org.apache.calcite.adapter.govdata.hooks.CensusVariableResolver
```

### Generated Schema with Hooks

SchemaGenerator produces schemas that reference hooks:

```yaml
tables:
  - name: bea_regional_income
    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      # ...

    hooks:
      responseTransformer: "org.apache.calcite.adapter.govdata.hooks.BeaResponseTransformer"
      rowTransformers:
        - type: class
          class: "org.apache.calcite.adapter.govdata.hooks.BeaValueNormalizer"
      validators:
        - type: class
          class: "org.apache.calcite.adapter.govdata.hooks.GeoFipsValidator"
```

### Separation of Concerns

| Component | Location | Responsibility |
|-----------|----------|----------------|
| ETL Engine | file adapter | Generic HTTP → Parquet pipeline |
| Hook Interfaces | file adapter | Extension points for customization |
| Hook Implementations | govdata | API-specific business logic |
| Schema Templates | govdata | API knowledge, table definitions |
| Schema Generator | govdata | Produce file adapter schemas |

This separation allows:
1. File adapter to remain generic and reusable
2. Govdata to encapsulate all government data knowledge
3. Other adapters to reuse file adapter with their own hooks
4. Testing hooks independently of ETL engine

---

## Open Questions

1. **Schema Compatibility**: Should file adapter schema be a superset of govdata schema, or a clean break?
   - Recommendation: Clean break with migration tool

2. **API-Specific Quirks**: Where do BEA/BLS/Census-specific workarounds live?
   - **Answer**: In govdata hook implementations (see Business Logic Hooks section)
   - Hooks are referenced in generated schemas, executed by file adapter

3. **Incremental Migration**: Can we migrate one API at a time?
   - Recommendation: Yes, start with simplest (FRED), end with complex (BEA)

4. **LLM Integration**: Should schema generation be LLM-assisted?
   - Recommendation: Future enhancement, not blocking migration

5. **Hook Testing**: How to test hooks in isolation?
   - Recommendation: Unit tests with mocked RequestContext/RowContext
   - Integration tests verify hook + ETL pipeline together
