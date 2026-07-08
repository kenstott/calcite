# Govdata Refactoring Guide: Migration to Declarative ETL

This guide provides step-by-step instructions for refactoring the govdata module to use the new declarative ETL capabilities defined in `/root/calcite/file/DESIGN-ETL-CAPABILITIES.md`.

## Executive Summary

The govdata module has sophisticated, battle-tested infrastructure for:
- HTTP API fetching with rate limiting, retry, and pagination
- Dimension iteration (year, frequency, geo, table combinations)
- Cache manifest tracking with DuckDB persistence
- JSON-to-Parquet conversion with type coercion

The new file adapter ETL design generalizes these patterns. This refactoring will:
1. **Extract** generic HTTP/caching/iteration logic to the file adapter
2. **Keep** govdata-specific API response handling as hooks
3. **Simplify** govdata to declarative schema + domain hooks

## Current Architecture Overview

### Key Classes and Their Responsibilities

| Class | Responsibility | Disposition |
|-------|---------------|-------------|
| `AbstractGovDataDownloader` | HTTP client, rate limiting, URL building, dimension iteration | **Extract to file adapter** |
| `AbstractEconDataDownloader` | ECON-specific caching, JSON-to-Parquet patterns | **Partially extract** |
| `BeaDataDownloader` | BEA API specifics, response parsing | **Keep as hooks** |
| `BlsDataDownloader` | BLS API specifics, series batching | **Keep as hooks** |
| `FredDataDownloader` | FRED API specifics, series catalog | **Keep as hooks** |
| `DuckDBCacheStore` | DuckDB-based manifest persistence | **Extract to file adapter** |
| `CacheManifest` | ECON cache tracking | **Replace with generic** |
| `CacheKey` | Cache key generation | **Extract to file adapter** |
| `ApiUrlBuilder` | URL construction | **Extract to file adapter** |

---

## Phase 1: HttpSource Integration

### Current Pattern (AbstractGovDataDownloader)

```java
// Current: Imperative HTTP fetching
protected final HttpResponse<String> executeWithRetry(HttpRequest request)
    throws IOException, InterruptedException {
    int maxRetries = getMaxRetries();
    long retryDelay = getRetryDelayMs();

    for (int attempt = 0; attempt < maxRetries; attempt++) {
        enforceRateLimit();
        HttpResponse<String> response = httpClient.send(request, ...);
        if (response.statusCode() == 429 || response.statusCode() >= 500) {
            // Exponential backoff
            Thread.sleep(retryDelay * (long) Math.pow(2, attempt));
            continue;
        }
        return response;
    }
}
```

### Target Pattern (Declarative Schema)

```yaml
# econ-schema.yaml - After refactoring
tables:
  - name: national_accounts
    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      method: GET
      parameters:
        UserID: "{env:BEA_API_KEY}"
        method: "GetData"
        DataSetName: "NIPA"
        TableName: "{tablename}"
        Frequency: "{frequency}"
        Year: "{year}"
      response:
        format: json
        dataPath: "BEAAPI.Results.Data"
        errorPath: "BEAAPI.Results.Error"
      rateLimit:
        requestsPerSecond: 1.5  # BEA: 100 req/min
        retryOn: [429, 503]
        maxRetries: 3
      cache:
        enabled: true
        ttlSeconds: 86400
```

### Refactoring Steps

#### Step 1.1: Create HttpSource Interface in File Adapter

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/HttpSource.java`

Extract from `AbstractGovDataDownloader`:
- `executeWithRetry()` -> `HttpSource.fetch()`
- `enforceRateLimit()` -> Rate limiter component
- `buildDownloadUrl()` -> URL builder with variable substitution

#### Step 1.2: Create ResponseTransformer Interface

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/ResponseTransformer.java`

```java
public interface ResponseTransformer {
    /**
     * Transform raw API response before parsing.
     * @param response Raw response body
     * @param context Request context (URL, parameters)
     * @return Transformed response suitable for parsing
     */
    String transform(String response, RequestContext context);
}
```

#### Step 1.3: Create Govdata Response Transformers

Keep API-specific logic in govdata as ResponseTransformer implementations:

**File**: `/root/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaResponseTransformer.java`

```java
public class BeaResponseTransformer implements ResponseTransformer {
    @Override
    public String transform(String response, RequestContext context) {
        JsonNode root = parse(response);

        // BEA-specific: Check for API error in response body
        if (root.has("BEAAPI") && root.path("BEAAPI").has("Error")) {
            throw new ApiException(root.path("BEAAPI").path("Error").asText());
        }

        // Extract data array from nested structure
        return root.path("BEAAPI").path("Results").path("Data").toString();
    }
}
```

Similar transformers for:
- `BlsResponseTransformer` - Handle BLS series response format
- `FredResponseTransformer` - Handle FRED observations format
- `TreasuryResponseTransformer` - Handle Treasury API format

#### Step 1.4: Map Current Methods to New Components

| Current Method | New Component |
|---------------|---------------|
| `buildDownloadUrl()` | `HttpSource.substituteVariables()` |
| `executeWithRetry()` | `HttpSource.fetchWithRetry()` |
| `enforceRateLimit()` | `RateLimiter` (built into HttpSource) |
| `resolveAuthValue()` | `HttpSource.resolveEnvironmentVariable()` |
| `downloadWithPagination()` | `HttpSource.fetchWithPagination()` |
| `navigateJsonPath()` | `JsonPathExtractor` (built-in hook) |

---

## Phase 2: DimensionIterator Integration

### Current Pattern (AbstractGovDataDownloader)

```java
// Current: Manual dimension iteration
public interface DimensionProvider {
    List<String> getValues(String dimensionName);
}

public static class IterationDimension {
    final String variableName;
    final List<String> values;
}

// In BeaDataDownloader:
private DimensionProvider createNationalAccountsDimensions(int startYear, int endYear,
    List<String> tableNames, List<String> frequencies) {
    return (dimensionName) -> {
        switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "tablename": return tableNames;
            case "frequency": return frequencies;
            default: return null;
        }
    };
}
```

### Target Pattern (Declarative Schema)

```yaml
# econ-schema.yaml - After refactoring
tables:
  - name: national_accounts
    dimensions:
      tablename:
        type: query
        sql: "SELECT DISTINCT table_name FROM reference_nipa_tables"
      frequency:
        type: list
        values: [A, Q]
      year:
        type: yearRange
        start: "{startYear}"
        end: "{endYear}"
```

### Refactoring Steps

#### Step 2.1: Create DimensionIterator in File Adapter

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java`

Extract from `AbstractGovDataDownloader`:
- `IterationDimension` class
- `DimensionProvider` interface
- `yearRange()` helper
- `computeDimensionSignature()` for cache invalidation

#### Step 2.2: Create DimensionResolver Interface

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionResolver.java`

```java
public interface DimensionResolver {
    /**
     * Resolve dimension values dynamically.
     * @param dimensionName Name of the dimension
     * @param config Dimension configuration from schema
     * @return List of values to iterate
     */
    List<String> resolve(String dimensionName, DimensionConfig config);
}
```

#### Step 2.3: Create Govdata Custom Dimension Resolvers

**File**: `/root/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDimensionResolver.java`

```java
public class BeaDimensionResolver implements DimensionResolver {
    @Override
    public List<String> resolve(String dimensionName, DimensionConfig config) {
        if ("bea_regional_geo".equals(dimensionName)) {
            // Query cached geo support to skip unsupported geos for this table
            String tableName = config.getContext().get("tableName");
            return queryAvailableGeos(tableName);
        }
        return null; // Fall back to default resolution
    }
}
```

#### Step 2.4: Map Current Methods to New Components

| Current Method | New Component |
|---------------|---------------|
| `extractDimensionValues()` | `DimensionIterator.expand()` |
| `yearRange()` | `DimensionIterator.yearRange()` (built-in) |
| `computeDimensionSignature()` | `DimensionIterator.signature()` |
| `DimensionProvider` | `DimensionResolver` (SPI) |

---

## Phase 3: HiveParquetWriter Integration

### Current Pattern (AbstractGovDataDownloader)

```java
// Current: Manual DuckDB SQL construction
public static String buildDuckDBConversionSql(
    List<TableColumn> columns,
    String missingValueIndicator,
    String jsonPath,
    String parquetPath,
    String dataPath) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n  SELECT\n");

    for (TableColumn column : columns) {
        if (column.hasExpression()) {
            sql.append("(").append(column.getExpression()).append(") AS ...")
        } else {
            // Type casting and null handling
        }
    }

    sql.append("  FROM read_json_auto('").append(jsonPath).append("'...");
    sql.append(") TO '").append(parquetPath).append("' (FORMAT PARQUET)");
}
```

### Target Pattern (Declarative Schema)

```yaml
# econ-schema.yaml - After refactoring
tables:
  - name: national_accounts
    columns:
      - {name: geo_fips, type: VARCHAR, source: GeoFips}
      - {name: data_value, type: DECIMAL(15,2), source: DataValue}
      - {name: quarter, type: VARCHAR, expression: "SUBSTR(TimePeriod, 1, 2)"}
      - {name: value_clean, type: DECIMAL, expression: "CASE WHEN DataValue IN ('(NA)', '(D)') THEN NULL ELSE CAST(DataValue AS DECIMAL) END"}

    materialize:
      enabled: true
      output:
        location: "{baseDirectory}"
        pattern: "type=national_accounts/year=*/frequency=*/tablename=*/"
      partition:
        columns: [year, frequency, tablename]
      options:
        compression: snappy
```

### Refactoring Steps

#### Step 3.1: Create HiveParquetWriter in File Adapter

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/HiveParquetWriter.java`

Extract from `AbstractGovDataDownloader`:
- `buildDuckDBConversionSql()` -> `HiveParquetWriter.buildQuery()`
- `convertJsonValueToType()` -> Type coercion utilities
- `convertJsonRecordToTypedMap()` -> Record transformation

#### Step 3.2: Use Column Expressions Instead of RowTransformers

Most govdata transformations should be **column expressions**, not RowTransformers:

**Before (Java code in BlsDataDownloader)**:
```java
// Remove BLS-specific null indicators
if ("(NA)".equals(value) || "(D)".equals(value)) {
    return null;
}
```

**After (Schema expression)**:
```yaml
columns:
  - name: value
    type: DECIMAL(15,2)
    source: value
    expression: "CASE WHEN value IN ('(NA)', '(D)', '(NM)') THEN NULL ELSE CAST(value AS DECIMAL(15,2)) END"
```

#### Step 3.3: Create RowTransformer Only for Non-SQL Logic

Reserve RowTransformer for cases that **cannot** be expressed in SQL:

**File**: `/root/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/geo/CensusGeoEnricher.java`

```java
public class CensusGeoEnricher implements RowTransformer {
    private final Map<String, GeoMetadata> geoLookup;  // External reference data

    @Override
    public Map<String, Object> transform(Map<String, Object> row, RowContext context) {
        // Enrichment from external reference file - cannot be expressed in SQL
        String geoFips = (String) row.get("geo_fips");
        GeoMetadata meta = geoLookup.get(geoFips);
        if (meta != null) {
            row.put("land_area_sq_mi", meta.getLandArea());
            row.put("census_region", meta.getRegion());
        }
        return row;
    }
}
```

---

## Phase 4: Cache Manifest Migration

### Current Pattern (DuckDBCacheStore)

```java
// Current: DuckDB-backed cache with schema-specific tables
public class DuckDBCacheStore implements AutoCloseable {
    public boolean isCached(String cacheKey) { ... }
    public void upsertEntry(String cacheKey, ...) { ... }
    public boolean isParquetConverted(String cacheKey) { ... }
    public void markApiError(String cacheKey, String errorMessage, int retryAfterDays) { ... }
}
```

### Target Pattern (Generic Manifest)

The `DuckDBCacheStore` is already well-designed and can be promoted to the file adapter with minimal changes:

```yaml
# Schema configuration
cache:
  manifest: true
  location: "{baseDirectory}/.manifest.duckdb"
  errorHandling:
    transientRetries: 3
    notFoundRetryDays: 7
    apiErrorRetryDays: 7
```

### Refactoring Steps

#### Step 4.1: Promote DuckDBCacheStore to File Adapter

**Move**: `govdata/.../DuckDBCacheStore.java` -> `file/.../etl/ManifestStore.java`

Changes needed:
- Remove govdata-specific table creation (`create_sec_filings.sql`)
- Make schema-specific tables extensible via hook
- Rename to `ManifestStore` for clarity

#### Step 4.2: Create CacheStrategy Interface

**File**: `/root/calcite/file/src/main/java/org/apache/calcite/adapter/file/etl/CacheStrategy.java`

```java
public interface CacheStrategy {
    /**
     * Determine if data should be fetched.
     * @param key Cache key
     * @param existing Existing manifest entry (null if none)
     * @return true if fetch should proceed
     */
    boolean shouldFetch(CacheKey key, ManifestEntry existing);

    /**
     * Called after successful fetch.
     */
    void onFetchComplete(CacheKey key, FetchResult result);
}
```

#### Step 4.3: Keep Govdata Cache Extensions

**File**: `/root/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/EconCacheExtension.java`

```java
public class EconCacheExtension implements ManifestExtension {
    @Override
    public void createTables(Connection conn) throws SQLException {
        // Create ECON-specific tables: catalog_series_cache, table_year_availability
        executeSqlResource(conn, "create_catalog_series_cache.sql");
        executeSqlResource(conn, "create_table_year_availability.sql");
    }
}
```

---

## Phase 5: Schema Migration

### Current Schema Format

```yaml
# Current econ-schema.yaml structure
schemaName: econ

partitionedTables:
  - name: national_accounts
    pattern: "type=national_accounts/frequency=*/year=*/tablename=*/*.parquet"
    partitions:
      style: hive
      columnDefinitions:
        - {name: type, type: VARCHAR}
        - {name: frequency, type: VARCHAR}
        - {name: year, type: INTEGER}
        - {name: tablename, type: VARCHAR}
    dimensions:
      type: [national_accounts]
      frequency: [A, Q]
      year:
        type: yearRange
    columns:
      - {name: date, type: string, nullable: true}
      - {name: value, type: double, nullable: true}
```

### Target Schema Format

```yaml
# Target econ-schema.yaml with ETL capabilities
schemaName: econ

tables:
  - name: national_accounts

    # HTTP Source (new)
    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      parameters:
        UserID: "{env:BEA_API_KEY}"
        method: "GetData"
        DataSetName: "NIPA"
        TableName: "{tablename}"
        Frequency: "{frequency}"
        Year: "{year}"
      response:
        format: json
        dataPath: "BEAAPI.Results.Data"
        errorPath: "BEAAPI.Results.Error"
      rateLimit:
        requestsPerSecond: 1.5
        retryOn: [429, 503]
        maxRetries: 3

    # Dimensions (enhanced)
    dimensions:
      tablename:
        type: query
        sql: "SELECT DISTINCT table_name FROM reference_nipa_tables"
      frequency:
        type: list
        values: [A, Q]
      year:
        type: yearRange
        start: "{startYear}"
        end: "{endYear}"

    # Columns with expressions
    columns:
      - {name: date, type: VARCHAR, source: TimePeriod}
      - {name: value, type: DECIMAL(15,2), source: DataValue,
         expression: "CASE WHEN DataValue IN ('(NA)','(D)') THEN NULL ELSE CAST(DataValue AS DECIMAL(15,2)) END"}
      - {name: geo_fips, type: VARCHAR, source: GeoFips}

    # Materialization (new)
    materialize:
      enabled: true
      trigger: auto
      output:
        location: "{baseDirectory}"
        pattern: "type=national_accounts/frequency=*/year=*/tablename=*/"
      partition:
        columns: [frequency, year, tablename]
      options:
        compression: snappy

    # Hooks (new)
    hooks:
      responseTransformer: "org.apache.calcite.adapter.govdata.econ.BeaResponseTransformer"
      validators:
        - type: expression
          condition: "geo_fips IS NOT NULL"
          action: warn
```

---

## Hook Registration

### SPI Registration

Create service provider files:

**File**: `govdata/src/main/resources/META-INF/services/org.apache.calcite.adapter.file.etl.ResponseTransformer`

```
org.apache.calcite.adapter.govdata.econ.BeaResponseTransformer
org.apache.calcite.adapter.govdata.econ.BlsResponseTransformer
org.apache.calcite.adapter.govdata.econ.FredResponseTransformer
org.apache.calcite.adapter.govdata.econ.TreasuryResponseTransformer
org.apache.calcite.adapter.govdata.geo.CensusResponseTransformer
```

**File**: `govdata/src/main/resources/META-INF/services/org.apache.calcite.adapter.file.etl.DimensionResolver`

```
org.apache.calcite.adapter.govdata.econ.BeaDimensionResolver
org.apache.calcite.adapter.govdata.geo.CensusGeoDimensionResolver
```

---

## Migration Checklist

### Phase 1: HttpSource (Week 1-2)
- [ ] Create `HttpSource` interface in file adapter
- [ ] Extract `executeWithRetry()` from AbstractGovDataDownloader
- [ ] Extract rate limiting infrastructure
- [ ] Create `ResponseTransformer` interface
- [ ] Implement `BeaResponseTransformer`
- [ ] Implement `BlsResponseTransformer`
- [ ] Implement `FredResponseTransformer`
- [ ] Add schema support for `source.type: http`
- [ ] Write unit tests for HttpSource

### Phase 2: DimensionIterator (Week 2-3)
- [ ] Create `DimensionIterator` in file adapter
- [ ] Extract dimension expansion logic
- [ ] Create `DimensionResolver` interface
- [ ] Support `type: query` dimensions
- [ ] Support `type: yearRange` dimensions
- [ ] Implement `BeaDimensionResolver` for geo filtering
- [ ] Add schema support for enhanced dimensions
- [ ] Write unit tests for DimensionIterator

### Phase 3: HiveParquetWriter (Week 3-4)
- [ ] Create `HiveParquetWriter` in file adapter
- [ ] Extract DuckDB SQL generation
- [ ] Support column expressions in schema
- [ ] Create `RowTransformer` interface
- [ ] Migrate BEA value cleaning to expressions
- [ ] Migrate BLS null indicators to expressions
- [ ] Implement `CensusGeoEnricher` as RowTransformer
- [ ] Write unit tests for HiveParquetWriter

### Phase 4: Cache Manifest (Week 4-5)
- [ ] Promote `DuckDBCacheStore` to file adapter
- [ ] Rename to `ManifestStore`
- [ ] Create `ManifestExtension` interface
- [ ] Implement `EconCacheExtension`
- [ ] Implement `GeoCacheExtension`
- [ ] Implement `SecCacheExtension`
- [ ] Write migration tests

### Phase 5: Schema Migration (Week 5-6)
- [ ] Update econ-schema.yaml with source sections
- [ ] Update econ-schema.yaml with materialize sections
- [ ] Update econ-schema.yaml with hooks sections
- [ ] Update geo-schema.yaml similarly
- [ ] Update FileSchema to detect materialized tables
- [ ] Test backward compatibility with existing patterns
- [ ] Write integration tests

### Phase 6: Cleanup (Week 6)
- [ ] Remove deprecated methods from AbstractGovDataDownloader
- [ ] Remove duplicate code from BeaDataDownloader
- [ ] Remove duplicate code from BlsDataDownloader
- [ ] Update documentation
- [ ] Run full regression tests

---

## Risks and Mitigations

### Risk 1: Breaking Existing Functionality
**Mitigation**:
- Keep all existing methods during transition
- Add `@Deprecated` annotations with migration notes
- Run full regression tests after each phase
- Maintain backward compatibility for existing schema files

### Risk 2: Performance Regression
**Mitigation**:
- Profile before and after each phase
- DimensionIterator should maintain current batching optimizations
- HiveParquetWriter should maintain DuckDB performance
- Cache manifest queries should remain efficient

### Risk 3: Complex Hook Interactions
**Mitigation**:
- Define clear hook ordering (ResponseTransformer -> RowTransformer -> Validator)
- Implement error handling per hook type
- Add extensive logging at hook boundaries
- Create test fixtures for hook combinations

### Risk 4: Schema Migration Errors
**Mitigation**:
- Schema validation before processing
- Detailed error messages for invalid configurations
- Support both old and new schema formats during transition
- Create schema migration tool/script

---

## Code Examples: Before and After

### Example 1: BEA National Accounts Download

**Before (BeaDataDownloader.java)**:
```java
public void downloadNationalAccountsMetadata(int startYear, int endYear,
    List<String> nipaTablesList, Map<String, Set<String>> tableFrequencies) {

    DimensionProvider dimensions = createNationalAccountsDimensions(
        startYear, endYear, tableNames, frequencies);

    iterateDownload("national_accounts", dimensions,
        (cacheKey, vars, jsonPath, parquetPath, helper) -> {
            String url = buildBeaUrl("GetData", "NIPA", vars);
            HttpResponse<String> response = executeWithRetry(request);

            // Parse BEA response
            JsonNode root = MAPPER.readTree(response.body());
            if (root.path("BEAAPI").path("Results").has("Error")) {
                throw new IOException("BEA API error: " + ...);
            }

            JsonNode data = root.path("BEAAPI").path("Results").path("Data");
            writeJsonToCache(jsonPath, data);
        });
}
```

**After (econ-schema.yaml + BeaResponseTransformer.java)**:
```yaml
# Schema-driven - no Java code needed for download logic
tables:
  - name: national_accounts
    source:
      type: http
      url: "https://apps.bea.gov/api/data"
      parameters:
        UserID: "{env:BEA_API_KEY}"
        method: "GetData"
        DataSetName: "NIPA"
        TableName: "{tablename}"
        Frequency: "{frequency}"
        Year: "{year}"
      response:
        dataPath: "BEAAPI.Results.Data"
    dimensions:
      tablename: {type: query, sql: "SELECT table_name FROM reference_nipa_tables"}
      frequency: {type: list, values: [A, Q]}
      year: {type: yearRange}
    hooks:
      responseTransformer: "org.apache.calcite.adapter.govdata.econ.BeaResponseTransformer"
```

```java
// Only API-specific error handling remains in Java
public class BeaResponseTransformer implements ResponseTransformer {
    @Override
    public String transform(String response, RequestContext context) {
        JsonNode root = parse(response);
        if (root.path("BEAAPI").path("Results").has("Error")) {
            String error = root.path("BEAAPI").path("Results").path("Error").asText();
            throw new ApiException("BEA API error: " + error);
        }
        return root.path("BEAAPI").path("Results").path("Data").toString();
    }
}
```

### Example 2: Value Cleaning

**Before (AbstractEconDataDownloader.java)**:
```java
protected Object convertJsonValueToType(JsonNode jsonValue, String columnName,
    String columnType, String missingValueIndicator) {

    if (jsonValue.isTextual()) {
        String textValue = jsonValue.asText();
        // BEA-specific null indicators
        if ("(NA)".equals(textValue) || "(D)".equals(textValue) || "(NM)".equals(textValue)) {
            return null;
        }
    }
    // ... type conversion
}
```

**After (econ-schema.yaml column expression)**:
```yaml
columns:
  - name: value
    type: DECIMAL(15,2)
    source: DataValue
    expression: >
      CASE
        WHEN DataValue IN ('(NA)', '(D)', '(NM)', 'null', '') THEN NULL
        ELSE CAST(DataValue AS DECIMAL(15,2))
      END
```

---

## Summary

This refactoring transforms govdata from an imperative, Java-heavy implementation to a declarative, schema-driven approach. The result:

1. **Reduced Code**: ~60% less Java code in govdata
2. **Better Separation**: Generic ETL in file adapter, domain logic in hooks
3. **Easier Maintenance**: Schema changes don't require code changes
4. **Reusable Infrastructure**: Other adapters can use the same ETL capabilities
5. **Improved Testing**: Declarative schemas are easier to test and validate

The govdata module becomes primarily a collection of:
- Schema YAML files defining tables and ETL pipelines
- Response transformers for API-specific parsing
- Dimension resolvers for dynamic value generation
- Row transformers for non-SQL enrichment logic
