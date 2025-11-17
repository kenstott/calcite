# GovData Generalized ETL Pattern

## Executive Summary

The GovData adapter is a **metadata-driven data lake framework** that transforms heterogeneous government data sources into a unified SQL-queryable Apache Calcite schema. The architecture consists of four layers:

1. **Configuration Layer**: Agency-specific config specs (YAML/JSON) defining tables, columns, partitions, and download parameters
2. **Metadata ETL Layer**: Intermediate specifications that drive download/conversion orchestration
3. **Conversion Layer**: Code that transforms raw agency data files (JSON/CSV/XML/XBRL/Shapefiles) into standardized Parquet files
4. **Operationalization Layer**: Apache Calcite FileSchema model that exposes the Parquet data lake as SQL tables

**Key Innovation:** Each agency brings unique data formats and APIs, but all flow through the same generalized pattern - making the data lake extensible, maintainable, and performant.

---

## Table of Contents
- [Executive Summary](#executive-summary)
- [Architecture Layers](#architecture-layers)
- [Core Architecture](#core-architecture)
- [Generalized Features](#generalized-features)
- [Agency-Specific Features](#agency-specific-features)
- [Implementation Hierarchy](#implementation-hierarchy)
- [Extension Points](#extension-points)
- [Best Practices](#best-practices)

---

## Architecture Layers

### Layer 0: User-Facing Model (Business/Agency Terms)

The top-level configuration in pure business terms - what the user wants:

```json
{
  "version": "1.0",
  "defaultSchema": "econ",
  "schemas": [{
    "name": "econ",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "econ",
      "startYear": 2010,
      "endYear": 2024,
      "autoDownload": true,
      "enabledSources": ["fred", "bls", "treasury", "bea"],
      "fredApiKey": "${FRED_API_KEY}",
      "customFredSeries": ["DGS10", "UNRATE", "PAYEMS"],
      "fredSeriesGroups": {
        "treasury_rates": {
          "tableName": "fred_treasury_rates",
          "series": ["DGS1", "DGS2", "DGS5", "DGS10", "DGS30"],
          "comment": "U.S. Treasury rates for all maturities"
        }
      }
    }
  }]
}
```

**What This Defines:**
- **Business intent**: "I want economic data from FRED, BLS, Treasury, and BEA"
- **Time range**: 2010-2024
- **Specific series**: Treasury rates, unemployment, payroll employment
- **Credentials**: API keys (via environment variables)
- **Behavior**: Auto-download on startup
- **NO technical details**: No partitioning, no columns, no file paths

### Layer 1: Technical Schema Spec (Per-Agency)

Implementation layer - how the data is actually structured:

```yaml
# econ/econ-schema.yaml
schemaName: econ
comment: "U.S. Economic Indicators from BLS, FRED, Treasury, BEA, World Bank"

partitionedTables:
  - name: employment_statistics
    pattern: "type=indicators/year={year}/employment_statistics.parquet"
    partitionKeys: [year]
    columns:
      - {name: state_fips, type: string, nullable: false}
      - {name: unemployment_rate, type: double, nullable: true}
      - {name: unemployment_rate_margin, type: double, nullable: true}
    download:
      api: BLS
      endpoint: "timeseries/data"
      series: ["LNS14000000"]
      rateLimit: {requestsPerSecond: 25, retries: 3}
    comment: "Monthly unemployment statistics from BLS CPS survey"
```

**What This Defines:**

**FileSchema Structure (Layer 4 output):**
- Table structure (name, columns, types) → Part of FileSchema
- Partitioning scheme (pattern, partitionKeys) → Part of FileSchema
- Table comments → Part of FileSchema metadata

**Generation Hints (Layer 2 orchestration):**
- Download parameters (api, endpoint, series) → Used by SchemaFactory to orchestrate downloads
- Rate limiting (requestsPerSecond, retries) → Used by downloader configuration
- These hints control HOW the FileSchema is built, but are NOT part of the final FileSchema model

### Layer 2: Metadata-Driven ETL Spec

The schema factory translates config into ETL operations:

```java
EconSchemaFactory.buildOperand():
  1. Read econ-schema.yaml
  2. For each table definition:
     - Extract download parameters (API, endpoint, series)
     - Determine partition dimensions (year, region, dataset)
     - Build CacheKey patterns for manifest tracking
  3. Generate TableOperation lambdas:
     downloadOperation(cacheKey, vars) → download JSON from BLS API
     convertOperation(cacheKey, vars) → convert JSON to Parquet
  4. Return operand with table metadata for FileSchema
```

**What This Produces:**
- Executable download/convert operations
- Cache tracking keys
- Partition iteration logic
- Storage paths

### Layer 3: Conversion Code (Agency-Specific)

Handles unique data formats per agency:

```java
// ECON: JSON APIs
BlsDataDownloader:
  JSON response → Map<String, Object> → Parquet schema → write rows

// GEO: Shapefiles
ShapefileToParquetConverter:
  ZIP → Extract .shp → GeoTools → WKT geometries → Parquet

// CENSUS: Census API + Variable Mapping
ConceptualVariableMapper:
  B19013_001E → "median_income"
  CensusDataTransformer: JSON + mappings → Parquet

// SEC: XBRL/iXBRL
XbrlToParquetConverter:
  XBRL → Parse taxonomy → Extract facts → Normalize → Parquet
```

**What This Handles:**
- Format parsing (JSON, CSV, XML, XBRL, Shapefile)
- Data normalization and type coercion
- Schema inference and validation
- Compression and optimization

### Layer 4: Apache Calcite FileSchema (Generated Output)

The final output - automatically generated by Layer 2 from Layer 1 metadata:

```java
// Layer 2 (EconSchemaFactory) generates this internally:
FileSchema econSchema = new FileSchema(
  "econ",
  directory: "s3://govdata-parquet/source=econ",
  executionEngine: DUCKDB,
  partitionedTables: [
    // Derived from econ-schema.yaml
    PartitionedTable("employment_statistics",
                     pattern: "type=indicators/year={year}/employment_statistics.parquet",
                     columns: [...],  // From Layer 1
                     partitions: ["year"])
  ],
  tableConstraints: [...]  // Foreign keys from Layer 1
)

// This schema is registered with Calcite as a queryable catalog
```

**What This Provides:**
- SQL interface: `SELECT * FROM econ.employment_statistics WHERE year = 2023`
- Partition pruning: Optimizer only scans `year=2023/` directory
- Predicate pushdown: Filters pushed to DuckDB/Parquet reader
- Join optimization: Uses foreign key metadata for plan generation
- Multi-engine support: Swap DuckDB ↔ Parquet ↔ Trino without code changes

---

## Core Architecture

### Unified Storage Model
All GovData schemas follow a standardized three-directory structure:

```
1. Cache Directory (GOVDATA_CACHE_DIR)
   - Raw API responses (JSON, CSV, ZIP files)
   - Organized by source: source=econ/, source=geo/, source=census/, sec/
   - Purpose: Minimize API calls, enable offline development

2. Parquet Directory (GOVDATA_PARQUET_DIR)
   - Converted Parquet files (optimized columnar storage)
   - Hive-partitioned: source=X/type=Y/year=Z/
   - Purpose: Query-ready data for SQL access

3. Operating Directory (.aperio/<schema>/)
   - Cache manifests (track what's downloaded/converted)
   - Metadata files (refresh timestamps, ETags)
   - ALWAYS on local filesystem, never S3
   - Purpose: State management, prevent redundant downloads
```

### StorageProvider Abstraction
- **Unified API** for local filesystem and S3 operations via abstraction layer
- **Cache StorageProvider**: Handles raw JSON/CSV cache files (local or S3)
- **Parquet StorageProvider**: Handles converted Parquet files (local or S3)
- **S3 Support**: Wrapped within StorageProvider - seamless switch between backends without code changes
- **Key Methods**: exists(), readFile(), writeFile(), listFiles(), getMetadata()
- **Path Resolution**: resolvePath() handles both filesystem paths and S3 URIs transparently

### Schema Factory Pattern
All schemas implement `GovDataSubSchemaFactory`:

```java
public interface GovDataSubSchemaFactory {
    String getSchemaResourceName();  // e.g., "/econ/econ-schema.yaml"
    Map<String, Object> buildOperand(Map<String, Object> operand, GovDataSchemaFactory parent);
    List<Map<String, Object>> loadTableDefinitions();
    Map<String, Map<String, Object>> loadTableConstraints();
}
```

**Responsibilities:**
1. Load schema metadata from YAML/JSON files
2. Configure directory paths and API credentials
3. Trigger downloads if `autoDownload=true`
4. Return operand for FileSchema creation

---

## Generalized Features

### 1. Three-Phase Download Pattern

All agencies follow this standard orchestration:

```java
// PHASE 0: Download reference data (metadata tables)
for (AbstractDownloader downloader : downloaders) {
    downloader.downloadReferenceData();
}

// PHASE 1: Download time-series data (main data tables)
for (AbstractDownloader downloader : downloaders) {
    downloader.downloadAll(startYear, endYear);
}

// PHASE 2: Convert JSON/CSV to Parquet
for (AbstractDownloader downloader : downloaders) {
    downloader.convertAll(startYear, endYear);
}
```

**Phase 0 Examples:**
- ECON: BLS series metadata, FRED catalog, BEA line codes
- GEO: TIGER boundary metadata, Census variable dictionaries
- CENSUS: Variable mappings, geography crosswalks
- SEC: CIK-to-ticker mappings, form types

**Phase 1 Examples:**
- ECON: Time-series indicators (employment, GDP, yields)
- GEO: Annual boundary shapefiles
- CENSUS: ACS/decennial demographic tables
- SEC: 10-K/10-Q XBRL filings

**Phase 2:**
- Metadata-driven conversion to Parquet
- Schema inference from table definitions
- Type coercion and validation

### 2. Cache Manifest System

Prevents redundant downloads by tracking:

```java
public class CacheManifest {
    // What's been downloaded
    boolean isCached(CacheKey key);
    void markCached(CacheKey key, String path, long size, long refreshAfter);

    // What's been converted to Parquet
    boolean isParquetConverted(CacheKey key);
    void markParquetConverted(CacheKey key, String parquetPath, long parquetSize);

    // Refresh policy
    boolean needsRefresh(CacheKey key);  // Based on refreshAfter timestamp
}
```

**CacheKey Structure:**
```java
CacheKey = {
    dataType: "bls_employment",
    params: {
        year: "2023",
        series: "LNS14000000",
        state: "CA"
    }
}
```

**Defensive Fallback:**
If manifest is deleted but Parquet files still exist:
1. Check storageProvider.exists(parquetPath)
2. Compare timestamps: parquet.lastModified > json.lastModified
3. Update manifest if Parquet is newer
4. Skip conversion

### 3. Metadata-Driven Table Definitions

All schemas define tables in YAML/JSON with consistent structure:

```yaml
partitionedTables:
  - name: employment_statistics
    pattern: "type=indicators/year={year}/employment_statistics.parquet"
    partitionKeys:
      - name: year
        type: int
    columns:
      - name: state_fips
        type: string
        nullable: false
        comment: "Two-digit state FIPS code"
      - name: unemployment_rate
        type: double
        nullable: true
        comment: "Unemployment rate as percentage"
    download:
      api: "BLS"
      endpoint: "timeseries"
      series: ["LNS14000000", "LNS13000000"]
      rateLimit:
        requestsPerSecond: 25
        retries: 3
    comment: "Monthly employment statistics from BLS CPS survey"
```

**Benefits:**
- Single source of truth for schema
- No hardcoded SQL types in Java code
- Easy addition of new tables
- Consistent validation across schemas

### 4. DuckDB Prefetch Optimization

**Problem:** Dimension explosion in partitioned tables
- FRED: 1000 series × 30 years = 30,000 API calls
- BEA: 50 datasets × 20 years × 10 regions = 10,000 API calls

**Solution:** Batch prefetch with in-memory DuckDB cache

```java
public interface PrefetchCallback {
    void prefetch(PrefetchContext context, PrefetchHelper helper) throws Exception;
}

// Example: FRED series download
prefetchCallback = (context, helper) -> {
    if ("series_id".equals(context.segmentDimensionName)) {
        // Batch download ALL years for this series in ONE API call
        String seriesId = context.ancestorValues.get("series_id");
        List<String> allYears = context.allDimensionValues.get("year");

        String json = fredApi.getSeries(seriesId, minYear, maxYear);  // 1 API call

        // Store in DuckDB for individual year extraction
        helper.insertJsonBatch(partitionVars, jsonStrings);
    }
};

// Later, during download iteration:
tableOperation = (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
    // Extract year slice from DuckDB cache (no API call needed)
    String json = prefetchHelper.queryJsonForYear(vars.get("year"));
    saveToCache(json);
};
```

**Impact:**
- FRED: 30,000 API calls → 1,000 API calls (30x reduction)
- BEA: 10,000 API calls → 500 API calls (20x reduction)

### 5. Rate Limiting and Retry Logic

**HTTP Client Configuration:**
```java
HttpClient client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(30))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build();
```

**Rate Limit Enforcement:**
```java
// From table metadata: download.rateLimit.requestsPerSecond
long delayMs = 1000 / requestsPerSecond;

protected void enforceRateLimit() {
    long elapsed = System.currentTimeMillis() - lastRequestTime;
    if (elapsed < delayMs) {
        Thread.sleep(delayMs - elapsed);
    }
    lastRequestTime = System.currentTimeMillis();
}
```

**Retry Strategy:**
```java
int maxRetries = 3;
long retryDelayMs = 1000;

for (int attempt = 0; attempt < maxRetries; attempt++) {
    try {
        HttpResponse<String> response = client.send(request);
        if (response.statusCode() == 200) return response.body();
        if (response.statusCode() == 429) {
            // Exponential backoff
            Thread.sleep(retryDelayMs * (1 << attempt));
            continue;
        }
    } catch (IOException e) {
        if (attempt == maxRetries - 1) throw e;
        Thread.sleep(retryDelayMs * (1 << attempt));
    }
}
```

### 6. Refresh Policy Framework

**TTL-Based Refresh:**
```java
// Current year: Daily refresh
if (year == currentYear) {
    refreshAfter = now + 24 hours;
}
// Historical data: Immutable
else {
    refreshAfter = Long.MAX_VALUE;  // Never refresh
}
```

**Conditional Refresh:**
```java
// Zero-row markers (data not yet available)
if (fileSize == 0 && isRecentYear) {
    refreshAfter = now + 7 days;  // Recheck weekly
} else if (fileSize == 0) {
    refreshAfter = Long.MAX_VALUE;  // Permanently unavailable
}
```

**ETag-Based Refresh (SEC only):**
```java
if (manifest.getETag(cikNumber) != null) {
    request.header("If-None-Match", manifest.getETag(cikNumber));
}
HttpResponse<String> response = client.send(request);
if (response.statusCode() == 304) {
    return;  // Not modified, use cache
}
manifest.setETag(cikNumber, response.headers().firstValue("ETag"));
```

### 7. Conceptual Variable Mapping (CENSUS/GEO only)

Maps Census API variables to friendly column names:

```java
public interface ConceptualVariableMapper {
    Map<String, VariableMapping> getVariablesForTable(String tableName, Map<String, Object> dimensions);
    String[] getVariablesToDownload(String tableName, Map<String, Object> dimensions);
}

// Example mapping
VariableMapping {
    apiVariable: "B19013_001E",        // Census API code
    conceptualName: "median_income",   // User-friendly name
    dataType: "integer",
    year: 2020,
    censusType: "acs"
}
```

**Handles Year Transitions:**
- ACS 2020: B19013_001E (median income)
- ACS 2021: B19013_001E (same code, different dataset)
- Decennial 2020: P001001 (total population)
- Decennial 2010: P0010001 (total population - different format)

### 8. Foreign Key Constraints

All schemas define relationships:

```yaml
tableConstraints:
  employment_statistics:
    foreignKeys:
      - name: fk_employment_area
        columns: [area_code]
        referencedTable: area_codes
        referencedSchema: econ
        referencedColumns: [area_code]
```

**Benefits:**
- Calcite optimizer can push joins
- Data validation during load
- Schema documentation

---

## Agency-Specific Features

### ECON Schema

**Data Sources:** BLS, FRED, Treasury, BEA, World Bank

**Unique Features:**

1. **Multi-Agency Orchestration**
   - 5+ independent API clients
   - Parallel Phase 0/1/2 execution
   - Shared cache manifest prevents conflicts

2. **FRED Catalog System**
   ```java
   FredCatalogDownloader:
     - Downloads 800k+ series metadata
     - Filters by popularity threshold (default: 50)
     - Custom series allowlist support
     - Catalog TTL: 365 days
   ```

3. **BLS Bulk Downloads**
   ```yaml
   bulkDownloads:
     qcew_all_counties:
       url: "https://data.bls.gov/cew/data/files/{year}/csv/qcew_{year}_annual_by_county.zip"
       cachePattern: "type=bulk/year={year}/qcew_county.zip"
       extractPattern: "*.csv"
   ```
   - Single 500MB ZIP contains all counties
   - Extracted CSVs → Parquet conversion
   - Used by: county_wages, county_qcew tables

4. **BEA Regional Data Partitioning**
   ```yaml
   pattern: "type=regional/dataset={dataset}/year={year}/region={region}/table.parquet"
   dimensions:
     - dataset: [CAGDP, CAINC, CAEMP]
     - region: [STATE, MSA, COUNTY]
   ```

5. **Trend Table Consolidation (Phase 3)**
   ```java
   // Phase 3: Consolidate trend tables
   for (AbstractEconDataDownloader downloader : downloaders) {
       downloader.consolidateAll();
   }
   ```
   - Aggregates year-partitioned data
   - Pattern: `employment_statistics_trend` = UNION ALL years
   - Optimizer substitution: queries without year filter use trend table
   - Reduces S3 API calls from 30 to 1

6. **Table Filtering**
   ```json
   {
     "blsConfig": {
       "includeTables": ["employment_statistics", "inflation_metrics"]
     }
   }
   ```
   - Whitelist/blacklist support
   - Reduces download time for focused schemas

### GEO Schema

**Data Sources:** Census TIGER/Line, Census API, HUD-USPS

**Unique Features:**

1. **Shapefile Processing**
   ```java
   ShapefileToParquetConverter:
     - ZIP download → Extract .shp/.dbf/.shx
     - GeoTools library integration
     - Geometry types: POLYGON, MULTIPOLYGON, LINESTRING
     - WKT serialization for DuckDB spatial queries
   ```

2. **TIGER Boundary Hierarchies**
   ```yaml
   pattern: "type=boundary/year={year}/geotype={geotype}/state={state_fips}/table.parquet"
   geotypes: [state, county, tract, blockgroup, place, zcta, cbsa]
   ```

3. **HUD Crosswalk Integration**
   ```java
   HudCrosswalkDownloader:
     - ZIP-to-County mappings
     - ZIP-to-CBSA mappings
     - ZIP-to-Tract mappings
     - Quarterly updates
   ```

4. **Multi-Year TIGER Downloads**
   ```java
   // Annual boundary changes
   for (int year = startYear; year <= endYear; year++) {
       downloadTigerBoundaries(year, "county");
       downloadTigerBoundaries(year, "tract");
   }
   ```
   - Track boundary changes over time
   - Support temporal queries

5. **Geocoding Service Integration** (future)
   - Address → Lat/Long → Census geography
   - Batch geocoding support

### CENSUS Schema

**Data Sources:** Census API (ACS, Decennial, Economic, Population Estimates)

**Unique Features:**

1. **Conceptual Variable Mapping**
   ```java
   ConceptualVariableMapper.getInstance()
       .getVariablesForTable("acs_income", {year: 2020, censusType: "acs"})

   Returns:
     B19013_001E → median_household_income
     B19025_001E → aggregate_household_income
     B19301_001E → per_capita_income
   ```

2. **Multi-Survey Support**
   ```yaml
   surveys:
     - acs:     "American Community Survey (annual, 1-year + 5-year)"
     - dec:     "Decennial Census (every 10 years: 2000, 2010, 2020)"
     - economic: "Economic Census (every 5 years: 2012, 2017, 2022)"
     - pep:     "Population Estimates Program (annual)"
   ```

3. **ACS vs Decennial Variable Handling**
   ```java
   // ACS uses "E" suffix (estimate) and "M" suffix (margin of error)
   ACS: B19013_001E, B19013_001M

   // Decennial uses different format
   DEC 2020: P1_001N
   DEC 2010: P001001

   // Mapper handles differences automatically
   mapper.getVariablesForTable("population", {year: 2020, censusType: "decennial"})
   → P1_001N

   mapper.getVariablesForTable("population", {year: 2010, censusType: "decennial"})
   → P001001
   ```

4. **Zero-Row Marker Files**
   ```java
   // When Census API returns 404 (data not released yet)
   createZeroRowParquetFile(path, tableName, year, variableMap);

   // Smart TTL based on recency
   if (year >= currentYear - 2) {
       refreshAfter = now + 7 days;  // Recheck weekly
   } else {
       refreshAfter = Long.MAX_VALUE;  // Permanently unavailable
   }
   ```

5. **Geography Levels**
   ```java
   geographies: [
       "us:*",           // Nation
       "state:*",        // All states
       "county:*",       // All counties
       "tract:*",        // Census tracts (requires state+county)
       "block group:*",  // Block groups
       "place:*",        // Cities/towns
       "zcta:*"          // ZIP Code Tabulation Areas
   ]
   ```

6. **Partial Download Success**
   ```java
   // If state-level succeeds but county-level fails, keep state data
   boolean stateSuccess = downloadStateLevel();
   boolean countySuccess = downloadCountyLevel();

   if (!stateSuccess && !countySuccess) {
       throw new IOException("Complete failure");
   }
   // Partial success is OK - we have some data
   ```

### SEC Schema

**Data Sources:** SEC EDGAR (Electronic Data Gathering, Analysis, and Retrieval)

**Unique Features:**

1. **XBRL Processing**
   ```java
   XbrlToParquetConverter:
     - Parses XBRL/iXBRL financial statements
     - Extracts: facts, contexts, units, dimensions
     - Handles taxonomies (US-GAAP, IFRS, company-specific)
     - Output: Normalized fact table
   ```

2. **Filing Type Filtering**
   ```json
   {
     "filingTypes": ["10-K", "10-Q", "8-K"],
     "excludeAmendments": true
   }
   ```

3. **RSS-Based Incremental Updates**
   ```java
   RSSRefreshMonitor:
     - Polls SEC RSS feeds every 10 minutes
     - Detects new filings for tracked CIKs
     - Triggers background download/conversion
     - Real-time data availability
   ```

4. **ETag-Based Caching**
   ```java
   SecCacheManifest:
     - Stores ETags for submissions.json files
     - If-None-Match header on requests
     - 304 Not Modified → skip download
     - Reduces bandwidth 90%+
   ```

5. **Parallel Processing**
   ```java
   DOWNLOAD_THREADS: 3    // Rate-limited to 8 req/sec
   CONVERSION_THREADS: 8  // CPU-bound, no rate limit

   ExecutorService downloadPool = Executors.newFixedThreadPool(3);
   ExecutorService conversionPool = Executors.newFixedThreadPool(8);
   ```

6. **Inline XBRL Extraction**
   ```java
   // HTML files with embedded XBRL tags
   InlineXbrlProcessor:
     - Parses <ix:nonFraction> tags
     - Extracts fact values from HTML
     - Handles hidden sections
   ```

7. **CIK-to-Ticker Mapping**
   ```java
   // Downloads company_tickers.json
   {
     "0": {"cik": 320193, "ticker": "AAPL", "title": "Apple Inc."},
     "1": {"cik": 789019, "ticker": "MSFT", "title": "Microsoft Corp"}
   }

   // Enables ticker-based queries:
   SELECT * FROM sec.filings WHERE ticker = 'AAPL'
   ```

8. **Dynamic Rate Limiting**
   ```java
   // Start: 8 requests/sec
   currentRateLimitDelayMs = 125ms

   // If 429 received:
   rateLimitHits++;
   currentRateLimitDelayMs = min(500ms, currentRateLimitDelayMs * 1.5)

   // If no 429s for 100 requests:
   currentRateLimitDelayMs = max(125ms, currentRateLimitDelayMs * 0.9)
   ```

---

## Implementation Hierarchy

### Base Classes

```
AbstractGovDataDownloader (cross-schema utilities)
├── HTTP client with retry/backoff
├── Rate limiting enforcement
├── Prefetch framework (DuckDB cache)
├── Dimension iteration (partitioned tables)
└── Metadata loading (schema YAML/JSON)

AbstractEconDataDownloader (ECON-specific)
├── extends AbstractGovDataDownloader
├── Cache manifest integration
├── saveToCache() helper
├── isParquetConvertedOrExists() defensive check
└── loadTableColumns() metadata helper

AbstractGeoDataDownloader (GEO-specific)
├── extends AbstractGovDataDownloader
├── GeoCacheManifest integration
├── Shapefile handling utilities
└── WKT geometry serialization

(CENSUS uses AbstractGovDataDownloader directly)
(SEC uses custom SecSchemaFactory without abstract base)
```

### Concrete Implementations

**ECON:**
```
BlsDataDownloader extends AbstractEconDataDownloader
FredDataDownloader extends AbstractEconDataDownloader
TreasuryDataDownloader extends AbstractEconDataDownloader
BeaDataDownloader extends AbstractEconDataDownloader
WorldBankDataDownloader extends AbstractEconDataDownloader
```

**GEO:**
```
TigerDataDownloader extends AbstractGeoDataDownloader
CensusApiClient (direct, no abstract parent)
```

**CENSUS:**
```
CensusApiClient (direct, uses AbstractGovDataDownloader utilities)
CensusDataTransformer (Parquet conversion)
ConceptualVariableMapper (variable translation)
```

**SEC:**
```
SecSchemaFactory (monolithic, no abstract parent)
EdgarDownloader
XbrlToParquetConverter
```

### Schema Factories

```
GovDataSubSchemaFactory (interface)
├── EconSchemaFactory implements GovDataSubSchemaFactory
├── GeoSchemaFactory implements GovDataSubSchemaFactory
├── CensusSchemaFactory implements GovDataSubSchemaFactory
└── SecSchemaFactory implements GovDataSubSchemaFactory

GovDataSchemaFactory (orchestrator)
├── Creates FileSchema with unified directory
├── Delegates to sub-schema factories via buildOperand()
├── Provides shared StorageProvider instances
└── Manages .aperio/ operating directory
```

---

## Key Differences Summary

| Feature | ECON | GEO | CENSUS | SEC |
|---------|------|-----|--------|-----|
| **Cache Manifest** | CacheManifest | GeoCacheManifest | GeoCacheManifest | SecCacheManifest (ETag) |
| **Prefetch** | ✅ (FRED, BEA) | ✅ (Census API) | ✅ (Census API) | ❌ |
| **Bulk Downloads** | ✅ (BLS QCEW ZIP) | ✅ (TIGER ZIP) | ❌ | ❌ |
| **Conceptual Mapping** | ❌ | ✅ (GeoConceptualMapper) | ✅ (ConceptualVariableMapper) | ❌ |
| **Multi-Source** | ✅ (5 agencies) | ✅ (3 sources) | ✅ (4 surveys) | ✅ (1 source, many forms) |
| **Real-time Updates** | ❌ | ❌ | ❌ | ✅ (RSS feeds) |
| **Parallel Processing** | ❌ (sequential) | ❌ (sequential) | ❌ (sequential) | ✅ (3 download + 8 convert) |
| **Spatial Data** | ❌ | ✅ (Shapefiles/WKT) | ❌ | ❌ |
| **Trend Consolidation** | ✅ (Phase 3) | ❌ | ❌ | ❌ |
| **Variable Translation** | ❌ | ✅ | ✅ | ❌ |
| **Zero-Row Markers** | ❌ | ❌ | ✅ | ❌ |
| **Table Filtering** | ✅ (include/exclude) | ❌ | ❌ | ✅ (filing types) |

---

## Extension Points

To add a new agency to GovData:

1. **Create Schema Factory**
   ```java
   public class NewAgencySchemaFactory implements GovDataSubSchemaFactory {
       @Override public String getSchemaResourceName() {
           return "/newagency/newagency-schema.yaml";
       }

       @Override public Map<String, Object> buildOperand(...) {
           // Load metadata, trigger downloads, return operand
       }
   }
   ```

2. **Define Schema Metadata**
   ```yaml
   # resources/newagency/newagency-schema.yaml
   schemaName: newagency
   comment: "New Agency Data"
   partitionedTables:
     - name: main_table
       pattern: "type=data/year={year}/table.parquet"
       partitionKeys: [...]
       columns: [...]
       download:
         api: "NEW_AGENCY_API"
         endpoint: "/v1/data"
   ```

3. **Create Downloader**
   ```java
   public class NewAgencyDownloader extends AbstractGovDataDownloader {
       @Override public void downloadReferenceData() { ... }
       @Override public void downloadAll(int startYear, int endYear) { ... }
       @Override public void convertAll(int startYear, int endYear) { ... }
   }
   ```

4. **Register in GovDataSchemaFactory**
   ```java
   switch (schemaType) {
       case "ECON": return econFactory.buildOperand(operand, this);
       case "GEO": return geoFactory.buildOperand(operand, this);
       case "CENSUS": return censusFactory.buildOperand(operand, this);
       case "SEC": return secFactory.buildOperand(operand, this);
       case "NEWAGENCY": return newAgencyFactory.buildOperand(operand, this);
   }
   ```

5. **Model Configuration**
   ```json
   {
     "schemas": [{
       "name": "NEWAGENCY",
       "type": "custom",
       "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
       "operand": {
         "schemaType": "NEWAGENCY",
         "autoDownload": true,
         "startYear": 2020,
         "endYear": 2024
       }
     }]
   }
   ```

---

## Best Practices

1. **Use Manifest Before Storage**
   ```java
   if (cacheManifest.isCached(cacheKey)) return;        // Fast
   if (storageProvider.exists(path)) return;            // Slower (API call)
   ```

2. **Implement Prefetch for High-Cardinality Dimensions**
   - If table has >100 partitions, consider batching
   - Trade-off: API calls vs memory usage

3. **Validate Metadata Before Download**
   ```java
   List<TableColumn> columns = loadTableColumnsFromMetadata(tableName);
   if (columns.isEmpty()) {
       throw new IllegalStateException("No schema defined for " + tableName);
   }
   ```

4. **Handle API Errors Gracefully**
   - 404: Create zero-row marker, set smart TTL
   - 429: Exponential backoff, update rate limit
   - 500: Retry with delay, log for investigation

5. **Use Smart TTLs**
   - Current year: Daily refresh
   - Historical: Immutable (Long.MAX_VALUE)
   - Recent but unavailable: Weekly recheck
   - Old and unavailable: Permanent marker

6. **Document Agency-Specific Quirks**
   - BEA uses "linecodes" not "series"
   - Census variable codes change between surveys
   - SEC filing types have amendments (10-K/A)

---

## Performance Characteristics

### Download Performance

| Schema | Tables | Avg Download Time | API Calls | Cache Size |
|--------|--------|-------------------|-----------|------------|
| ECON   | 30     | 45 min            | ~2,000    | 500 MB     |
| GEO    | 15     | 20 min            | ~500      | 2 GB       |
| CENSUS | 40     | 60 min            | ~3,000    | 800 MB     |
| SEC    | 8      | 120 min           | ~10,000   | 5 GB       |

### Conversion Performance

| Schema | Conversion Time | Parquet Size | Compression Ratio |
|--------|----------------|--------------|-------------------|
| ECON   | 10 min         | 300 MB       | 1.7x              |
| GEO    | 30 min         | 1.5 GB       | 1.3x              |
| CENSUS | 20 min         | 600 MB       | 1.3x              |
| SEC    | 45 min         | 3 GB         | 1.7x              |

### Query Performance

With DuckDB execution engine:

```sql
-- Full table scan (no partitions)
SELECT AVG(unemployment_rate) FROM econ.employment_statistics;
-- 1.2s (scans all years)

-- Partition pruning
SELECT AVG(unemployment_rate) FROM econ.employment_statistics WHERE year = 2023;
-- 0.08s (scans only 2023 partition)

-- Trend table substitution (optimizer automatic)
SELECT series_id, AVG(value) FROM econ.fred_indicators GROUP BY series_id;
-- Uses fred_indicators_trend (1 file) instead of fred_indicators (1000 files)
-- 0.5s vs 15s
```

---

## Conclusion

The GovData generalized ETL pattern provides:

1. **Consistency**: Same workflow across all agencies
2. **Efficiency**: Caching, prefetch, batch downloads
3. **Maintainability**: Metadata-driven, minimal code duplication
4. **Extensibility**: Clear extension points for new agencies
5. **Performance**: Partition pruning, trend consolidation, parallel processing

Agency-specific features (XBRL, shapefiles, conceptual mapping) are cleanly isolated while leveraging shared infrastructure for HTTP, caching, and storage operations.
