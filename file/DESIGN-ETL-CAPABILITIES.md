# File Adapter ETL Capabilities - Design Document

## Overview

This document describes new capabilities for the file adapter that transform it from a file reader into a declarative ETL engine. These additions are fully backward compatible - existing functionality remains unchanged.

## Goals

1. **HttpSource** - Fetch data from HTTP/REST APIs
2. **HiveParquetWriter** - Materialize data to hive-partitioned parquet
3. **DimensionIterator** - Expand dimension combinations for batch processing
4. **Unified Pipeline** - Schema-driven ETL from API to queryable parquet

## Non-Goals

- Replacing existing file reading capabilities
- Breaking backward compatibility
- Domain-specific business logic (belongs in adapters like govdata)

---

## Component 1: HttpSource

### Purpose
Fetch data from HTTP/REST APIs as a table source, similar to how files are sources today.

### Schema Configuration

```yaml
tables:
  - name: api_data
    source:
      type: http
      url: "https://api.example.com/data"
      method: GET  # or POST
      parameters:
        apiKey: "{env:API_KEY}"
        year: "{year}"
        region: "{region}"
      headers:
        Accept: "application/json"
        Authorization: "Bearer {env:AUTH_TOKEN}"
      auth:
        type: apiKey  # or oauth2, basic
        location: header  # or query
        name: "X-Api-Key"
        value: "{env:API_KEY}"
      response:
        format: json  # or csv, xml
        dataPath: "$.results.data"  # JSONPath to data array
        pagination:
          type: offset  # or cursor, page
          limitParam: "limit"
          offsetParam: "offset"
          pageSize: 1000
      rateLimit:
        requestsPerSecond: 10
        retryOn: [429, 503]
        maxRetries: 3
      cache:
        enabled: true
        ttlSeconds: 86400
```

### Implementation

```java
public class HttpSource implements DataSource {
    private final HttpSourceConfig config;
    private final StorageProvider cacheProvider;

    public Stream<Map<String, Object>> fetch(Map<String, String> variables) {
        String url = substituteVariables(config.getUrl(), variables);
        // Handle pagination, rate limiting, caching
        return fetchWithPagination(url);
    }
}
```

### Key Features
- Variable substitution in URL, parameters, headers
- Environment variable references (`{env:VAR_NAME}`)
- Pagination support (offset, cursor, page-based)
- Rate limiting with backoff
- Response caching
- JSONPath data extraction

---

## Component 2: HiveParquetWriter

### Purpose
Materialize source data (JSON, CSV, API responses) to hive-partitioned parquet files.

### Schema Configuration

```yaml
tables:
  - name: sales_data
    source:
      type: http
      url: "https://api.example.com/sales"

    materialize:
      enabled: true
      trigger: auto  # or manual, onFirstQuery
      output:
        location: "s3://bucket/data/"
        pattern: "type=sales/year=*/region=*/"  # glob pattern describing output structure
        format: parquet
        compression: snappy
      partition:
        columns: [year, region]  # DuckDB PARTITION_BY columns
        batchBy: [year]  # process one year at a time
      options:
        threads: 4
        rowGroupSize: 100000
```

### Implementation

```java
public class HiveParquetWriter {
    private final StorageProvider storageProvider;
    private final String baseDirectory;

    public void materialize(MaterializeConfig config, DataSource source) {
        // For each batch combination
        for (Map<String, String> batch : buildBatchCombinations(config)) {
            Stream<Map<String, Object>> data = source.fetch(batch);
            writePartitionedParquet(data, config, batch);
        }
    }

    private void writePartitionedParquet(Stream<Map<String, Object>> data,
            MaterializeConfig config, Map<String, String> batch) {
        // Use DuckDB COPY with PARTITION_BY
        String sql = buildCopySql(data, config);
        executeDuckDB(sql);
    }
}
```

### Key Features
- Schema-driven column definitions with types
- Computed columns via expressions
- Batched processing for large datasets
- Configurable compression and row group size
- Incremental updates (only fetch new data)

---

## Component 3: DimensionIterator

### Purpose
Expand dimension definitions into concrete value combinations for batch processing.

### Schema Configuration

```yaml
tables:
  - name: economic_data
    dimensions:
      year:
        type: range
        start: 2020
        end: 2024
      frequency:
        type: list
        values: [A, M, Q]
      region:
        type: query
        sql: "SELECT DISTINCT region FROM regions WHERE active = true"
      geo:
        type: list
        values: [STATE, COUNTY, MSA]
```

### Implementation

```java
public class DimensionIterator {

    public List<Map<String, String>> expand(DimensionsConfig config) {
        List<List<DimensionValue>> allValues = new ArrayList<>();

        for (DimensionConfig dim : config.getDimensions()) {
            allValues.add(resolveDimension(dim));
        }

        return cartesianProduct(allValues);
    }

    private List<DimensionValue> resolveDimension(DimensionConfig dim) {
        switch (dim.getType()) {
            case RANGE:
                return IntStream.rangeClosed(dim.getStart(), dim.getEnd())
                    .mapToObj(String::valueOf)
                    .collect(toList());
            case LIST:
                return dim.getValues();
            case QUERY:
                return executeQuery(dim.getSql());
        }
    }
}
```

### Dimension Types
- `range` - numeric range (start, end, step)
- `list` - explicit value list
- `query` - SQL query to fetch values
- `yearRange` - special case for year ranges with current year support

---

## Component 4: Unified Pipeline

### Purpose
Orchestrate the full ETL pipeline from schema definition to queryable tables.

### Schema Configuration (Complete Example)

```yaml
tables:
  - name: sales_by_region

    # Source: HTTP API
    source:
      type: http
      url: "https://api.example.com/v1/sales"
      parameters:
        year: "{year}"
        region: "{region}"
        format: json
      auth:
        type: apiKey
        location: header
        name: X-API-Key
        value: "{env:SALES_API_KEY}"
      response:
        dataPath: "$.data.records"

    # Dimensions to iterate
    dimensions:
      year:
        type: range
        start: 2020
        end: 2024
      region:
        type: list
        values: [NORTH, SOUTH, EAST, WEST]

    # Column definitions
    columns:
      - {name: region_code, type: VARCHAR, source: regionCode}
      - {name: region_name, type: VARCHAR, source: regionName}
      - {name: year, type: INTEGER, source: fiscalYear}
      - {name: revenue, type: DECIMAL(15,2), source: totalRevenue}
      - {name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"}

    # Materialization to parquet
    materialize:
      enabled: true
      output:
        location: "{baseDirectory}"
        pattern: "type=sales/year=*/region=*/"
      partition:
        columns: [year, region]
        batchBy: [year, region]
      options:
        threads: 2

    # Alternate partition layouts
    alternate_partitions:
      - name: sales_by_quarter
        pattern: "type=sales_by_quarter/quarter=*/*.parquet"
        partition:
          columns: [quarter]
        batch_partition_columns: [year, region]
        threads: 4
```

### Pipeline Execution Flow

```
1. Schema Loading
   └── Parse YAML, validate configuration

2. Dimension Expansion
   └── year: [2020, 2021, 2022, 2023, 2024]
   └── region: [NORTH, SOUTH, EAST, WEST]
   └── combinations: 20 total

3. Data Fetching (per combination)
   └── HttpSource.fetch({year: 2020, region: NORTH})
   └── Cache response if configured

4. Materialization
   └── HiveParquetWriter.materialize()
   └── Batched by [year, region]
   └── Output: type=sales/year=2020/region=NORTH/data_0.parquet

5. Alternate Partition Creation
   └── ParquetReorganizer.reorganize()
   └── Output: type=sales_by_quarter/quarter=Q1/data_0.parquet

6. Table Registration
   └── PartitionedParquetTable ready for queries
```

---

## Integration with Existing Components

### StorageProvider
- HttpSource uses StorageProvider for caching
- HiveParquetWriter uses StorageProvider for output
- No changes needed to StorageProvider interface

### ParquetReorganizer
- Already supports batched reorganization
- HiveParquetWriter reuses same DuckDB patterns
- Alternate partitions work unchanged

### PartitionedTableConfig
- Extended with `source`, `dimensions`, `materialize` sections
- Backward compatible - existing configs work unchanged

### FileSchema
- Detects materialized tables automatically
- Triggers materialization on first query if `trigger: onFirstQuery`
- Registers resulting PartitionedParquetTable

---

## Backward Compatibility

### Existing Configs (unchanged)
```yaml
# This still works exactly as before
tables:
  - name: my_data
    pattern: "data/*.parquet"
    partition:
      columns: [year]
```

### Detection Logic
```java
if (config.getSource() == null) {
    // Existing behavior: file-based source
    return createFileBasedTable(config);
} else if (config.getSource().getType() == SourceType.HTTP) {
    // New behavior: HTTP source with materialization
    return createMaterializedTable(config);
}
```

---

## Implementation Phases

### Phase 1: HiveParquetWriter
- Extract JSON → Parquet logic from govdata
- Create HiveParquetWriter class
- Support file-based sources first
- Integrate with existing ParquetReorganizer

### Phase 2: DimensionIterator
- Implement dimension expansion
- Support range, list, query types
- Integrate with HiveParquetWriter batching

### Phase 3: HttpSource
- Implement HTTP fetching with pagination
- Add rate limiting and retry logic
- Add caching layer
- Integrate with DimensionIterator

### Phase 4: Pipeline Orchestration
- Unified pipeline executor
- Incremental update detection
- Progress reporting and error handling

---

## Configuration Reference

### Source Types
| Type | Description |
|------|-------------|
| `file` | Local or S3 files (default, existing behavior) |
| `http` | HTTP/REST API |
| `jdbc` | Database query (future) |

### Dimension Types
| Type | Description |
|------|-------------|
| `range` | Numeric range with start, end, step |
| `list` | Explicit value list |
| `query` | SQL query to fetch values |
| `yearRange` | Year range with current year support |

### Materialization Triggers
| Trigger | Description |
|---------|-------------|
| `auto` | Materialize when schema loads |
| `manual` | Only via explicit API call |
| `onFirstQuery` | Materialize on first table access |

---

## Component 5: Extensibility Hooks

### Purpose
Allow adapters (like govdata) to inject business-specific logic into the generic ETL pipeline without modifying the file adapter core.

### Hook Types

| Hook | When Called | Purpose |
|------|-------------|---------|
| `ResponseTransformer` | After HTTP fetch | Transform/normalize raw API response before parsing |
| `RowTransformer` | Per row during materialization | Complex logic that can't be expressed as SQL (see note) |
| `Validator` | Before writing parquet | Validate data quality, reject bad rows |
| `DimensionResolver` | During dimension expansion | Custom dimension value generation |
| `PostProcessor` | After materialization | Cleanup, notifications, aggregations |

**Note on RowTransformer vs column expressions:**

Most computed columns should use schema-driven `expression` (executed as SQL):
```yaml
columns:
  - {name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"}
  - {name: value_clean, type: VARCHAR, expression: "REPLACE(REPLACE(raw_value, '(NA)', ''), '(D)', '')"}
```

Use `RowTransformer` only when SQL expressions are insufficient:
- External lookups (enrichment from another data source)
- Stateful transformations (deduplication with memory)
- Complex parsing (nested JSON within a field)
- Non-SQL logic (custom algorithms, API-specific quirks)

### Schema Configuration

```yaml
tables:
  - name: bea_regional_income
    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      # ...

    hooks:
      # Java class implementing ResponseTransformer
      responseTransformer: "org.apache.calcite.adapter.govdata.BeaResponseTransformer"

      # Row-level transformations (can be expression or class)
      rowTransformers:
        - type: expression
          column: data_value
          expression: "REPLACE(REPLACE(data_value, '(NA)', ''), '(D)', '')"
        - type: class
          class: "org.apache.calcite.adapter.govdata.BeaValueNormalizer"

      # Validation before writing
      validators:
        - type: class
          class: "org.apache.calcite.adapter.govdata.BeaDataValidator"
        - type: expression
          condition: "geo_fips IS NOT NULL"
          action: "drop"  # or "fail", "warn"

      # Post-processing after materialization
      postProcessors:
        - class: "org.apache.calcite.adapter.govdata.BeaStatsGenerator"
```

### Java SPI Interfaces

```java
/**
 * Transforms raw API response before parsing.
 */
public interface ResponseTransformer {
    /**
     * Transform the raw response.
     * @param response Raw API response (JSON string)
     * @param context Request context (URL, parameters, dimension values)
     * @return Transformed response
     */
    String transform(String response, RequestContext context);
}

/**
 * Transforms individual rows during materialization.
 */
public interface RowTransformer {
    /**
     * Transform a single row.
     * @param row Map of column name to value
     * @param context Dimension values and table config
     * @return Transformed row (can return null to drop row)
     */
    Map<String, Object> transform(Map<String, Object> row, RowContext context);
}

/**
 * Validates rows before writing.
 */
public interface Validator {
    /**
     * Validate a row.
     * @param row Map of column name to value
     * @return ValidationResult (valid, invalid with action)
     */
    ValidationResult validate(Map<String, Object> row);
}

/**
 * Custom dimension value resolution.
 */
public interface DimensionResolver {
    /**
     * Resolve dimension values.
     * @param dimensionName Name of the dimension
     * @param config Dimension configuration from schema
     * @return List of dimension values to iterate
     */
    List<String> resolve(String dimensionName, DimensionConfig config);
}

/**
 * Post-processing after materialization completes.
 */
public interface PostProcessor {
    /**
     * Called after all data is materialized.
     * @param result Materialization result (file count, row count, etc.)
     * @param config Table configuration
     */
    void process(MaterializationResult result, TableConfig config);
}
```

### Hook Discovery

Hooks are discovered via:
1. **Schema reference**: Fully qualified class name in YAML
2. **Java SPI**: `META-INF/services/` registration
3. **Classpath scanning**: Annotated classes in adapter packages

```java
// SPI registration in govdata adapter
// META-INF/services/org.apache.calcite.adapter.file.etl.ResponseTransformer
org.apache.calcite.adapter.govdata.BeaResponseTransformer
org.apache.calcite.adapter.govdata.BlsResponseTransformer
org.apache.calcite.adapter.govdata.FredResponseTransformer
```

### Govdata Hook Examples

**BeaResponseTransformer**:
```java
public class BeaResponseTransformer implements ResponseTransformer {
    @Override
    public String transform(String response, RequestContext context) {
        // Handle BEA-specific response quirks
        // - Normalize error responses
        // - Extract nested data array
        // - Handle missing value indicators
        JsonNode root = parse(response);
        if (root.has("BEAAPI") && root.path("BEAAPI").has("Error")) {
            throw new ApiException(root.path("BEAAPI").path("Error").asText());
        }
        return root.path("BEAAPI").path("Results").path("Data").toString();
    }
}
```

**CensusGeoEnricher** (RowTransformer - requires external lookup):
```java
public class CensusGeoEnricher implements RowTransformer {
    private final Map<String, GeoMetadata> geoLookup;  // loaded from reference file

    @Override
    public Map<String, Object> transform(Map<String, Object> row, RowContext context) {
        // Enrich with data that can't come from the API response
        String geoFips = (String) row.get("geo_fips");
        GeoMetadata meta = geoLookup.get(geoFips);
        if (meta != null) {
            row.put("land_area_sq_mi", meta.getLandArea());
            row.put("census_region", meta.getRegion());
            row.put("timezone", meta.getTimezone());
        }
        return row;
    }
}
```

**Note**: Simple value normalization like BEA's `(NA)`/`(D)` markers should use column expressions:
```yaml
columns:
  - name: value
    type: DECIMAL(15,2)
    source: DataValue
    expression: "CASE WHEN DataValue IN ('(NA)', '(D)', '(NM)') THEN NULL ELSE CAST(DataValue AS DECIMAL(15,2)) END"
```

### Built-in Hooks

File adapter provides common built-in hooks:

| Hook | Purpose |
|------|---------|
| `JsonPathExtractor` | Extract data from nested JSON |
| `CsvNormalizer` | Handle CSV quirks (BOM, encoding) |
| `NullValueReplacer` | Replace placeholder strings with null |
| `TypeCoercer` | Coerce values to target types |
| `RowDeduplicator` | Remove duplicate rows |

Schema usage:
```yaml
hooks:
  rowTransformers:
    - type: builtin
      name: NullValueReplacer
      config:
        patterns: ["(NA)", "(D)", "N/A", "-"]
```

### Hook Ordering

Hooks execute in defined order:
1. `ResponseTransformer` - After fetch, before parsing
2. `RowTransformer` - Per row, in array order
3. `Validator` - After all transformers, in array order
4. `PostProcessor` - After all rows written, in array order

### Error Handling in Hooks

```java
public interface HookErrorHandler {
    enum Action { CONTINUE, SKIP_ROW, FAIL }

    /**
     * Handle hook execution error.
     * @param error The exception thrown
     * @param hook The hook that failed
     * @param context Current processing context
     * @return Action to take
     */
    Action handleError(Exception error, Object hook, ProcessingContext context);
}
```

Schema configuration:
```yaml
hooks:
  errorHandling:
    responseTransformer: fail      # fail on transformer error
    rowTransformer: skip_row       # skip row on transform error
    validator: continue            # log warning and continue
```

---

## Design Decisions

1. **Incremental Updates**: How to detect what's already fetched?

   **Decision**: Manifest-based tracking (generalized from govdata pattern).

   Govdata uses `CacheManifest` (DuckDB-backed) to track:
   - Fetched data: cache key → file path, timestamp
   - Converted parquet: cache key → parquet path
   - Unavailable data: cache key → retry TTL, reason
   - API errors: cache key → error message, retry TTL

   This pattern can be generalized to file adapter with optional hook for edge cases:
   ```yaml
   cache:
     manifest: true  # enable manifest tracking
     location: "{baseDirectory}/.manifest.duckdb"
   ```

   Hook for custom logic:
   ```java
   public interface CacheStrategy {
       boolean shouldFetch(CacheKey key, ManifestEntry existing);
       void onFetchComplete(CacheKey key, FetchResult result);
   }
   ```

2. **Error Handling**: What happens when one API call fails?

   **Decision**: Depends on failure type (generalized from govdata pattern).

   | Error Type | Action | Retry |
   |------------|--------|-------|
   | Transient (429, 503) | Exponential backoff | Immediate retries (3x) |
   | Not Found (404) | Mark unavailable | Retry after TTL (e.g., 7 days) |
   | API Error (200 + error body) | Mark API error | Retry after TTL (configurable) |
   | Auth Error (401, 403) | Fail materialization | No auto-retry |

   Schema configuration:
   ```yaml
   source:
     errorHandling:
       transientRetries: 3
       transientBackoffMs: 1000
       notFoundRetryDays: 7
       apiErrorRetryDays: 7
       authErrorAction: fail  # fail | skip | warn
   ```

3. **Schema Evolution**: What if API response schema changes?

   **Decision**: Fail on missing expected columns, ignore new columns.

   - **Missing expected column**: Fail with schema mismatch error. Requires human analysis and resolution.
   - **New unexpected column**: Silently ignore (no action needed).

   Rationale: Schema changes in external APIs often indicate breaking changes or data quality issues that need human review. Automatic handling risks data corruption.

   ```yaml
   columns:
     - {name: geo_fips, type: VARCHAR, source: GeoFips, required: true}   # fail if missing
     - {name: notes, type: VARCHAR, source: Notes, required: false}       # warn if missing
   ```
