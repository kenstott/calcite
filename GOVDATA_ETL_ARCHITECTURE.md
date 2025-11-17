# GovData ETL Architecture & Domain Implementation Guide

## Table of Contents
1. [Overview](#overview)
2. [General ETL Capabilities](#general-etl-capabilities)
3. [Core Architecture](#core-architecture)
4. [Ingestion Model](#ingestion-model)
5. [Domain-Specific Implementations](#domain-specific-implementations)
6. [Common Patterns](#common-patterns)
7. [Performance Optimizations](#performance-optimizations)

---

## Overview

The **GovData Adapter** is a metadata-driven, schema-based ETL pipeline for ingesting government data from multiple federal sources into Apache Calcite. It implements a unified framework for downloading, caching, transforming, and querying data from economic, geographic, securities, and demographic APIs.

### Key Features
- **Metadata-Driven**: Schema definitions drive table structure, partitioning, and download behavior
- **Multi-Domain**: Economic (FRED, BEA, BLS), Geographic (TIGER/Line), Securities (SEC EDGAR), Census
- **Smart Caching**: ETag-based conditional GET, TTL policies, S3/local storage abstraction
- **Format Conversion**: Automatic JSON/CSV/XML/Shapefile → Parquet transformation
- **Performance**: DuckDB-based cache filtering, bulk downloads, prefetch optimization
- **Resilience**: Rate limiting, exponential backoff retry, error tracking

---

## General ETL Capabilities

### 1. **Data Acquisition**

#### Multi-Source Download
- **REST APIs**: FRED, BEA, BLS, Census, World Bank, Alpha Vantage
- **Bulk Files**: TIGER/Line shapefiles, QCEW quarterly ZIPs, SEC EDGAR filings
- **Multiple Formats**: JSON, CSV, XML, Shapefiles, XBRL

#### Smart Download Logic
- **Conditional GET**: HTTP ETag and Last-Modified headers
- **Rate Limiting**: Per-API configurable throttling (100ms-1000ms between requests)
- **Retry with Backoff**: Exponential backoff for 429/5xx errors (configurable retries)
- **Bulk Optimization**: Single download feeds multiple tables (e.g., QCEW ZIP → 4 tables)

#### Download Modes
```java
// Time-series download for year range
downloadAll(2010, 2023);

// Reference/catalog data (one-time)
downloadReferenceData();

// Force refresh with cache invalidation
downloadAll(2020, 2023, true /* forceRefresh */);
```

### 2. **Caching System**

#### Cache Manifest
Every downloaded file tracked in JSON manifest:
```json
{
  "cacheKey": "fred_series:series_id=GDP:year=2023",
  "filePath": "cache/fred/series/2023/GDP.json",
  "parquetPath": "parquet/fred_series/series_id=GDP/year=2023/data.parquet",
  "fileSize": 4567,
  "cachedAt": "2024-01-15T10:30:00Z",
  "refreshAfter": "2024-01-16T10:30:00Z",
  "refreshReason": "current_year_daily",
  "etag": "W/\"abc123\"",
  "downloadRetry": 0,
  "lastError": null,
  "errorCount": 0
}
```

#### Refresh Policies
- **`historical_immutable`**: Never refresh (e.g., 2010 GDP data)
- **`current_year_daily`**: Refresh daily for in-progress year
- **`etag_based`**: Refresh only if server ETag changes
- **`catalog_weekly`**: Reference tables refreshed weekly
- **API Error Handling**: 7-day retry cadence for API errors (configurable)

#### Storage Abstraction
```java
StorageProvider provider = // Local filesystem or S3
provider.exists(path);
provider.readJson(path, Class.class);
provider.writeParquet(table, path);
```

### 3. **Data Transformation**

#### Conversion Pipeline
```
Raw Data (JSON/CSV/XML) → Domain Converter → Apache Arrow Table → Parquet
```

#### Domain-Specific Converters
- **EconRawToParquetConverter**: Routes FRED/BEA/BLS/Treasury/World Bank
- **ShapefileToParquetConverter**: Geographic boundaries to Parquet
- **XbrlToParquetConverter**: SEC financial statements
- **SecToParquetConverter**: SEC filings and metadata

#### Transformation Features
- **Schema Mapping**: JSON field → Parquet column with type conversion
- **API Unwrapping**: Extract nested data from API response wrappers
- **Flattening**: Time-series arrays → row-per-observation
- **Enrichment**: Add derived columns (e.g., calculated metrics)
- **Filtering**: Skip invalid/incomplete records

### 4. **Query Integration**

#### Calcite Schema Registration
```json
{
  "schemas": [
    {
      "name": "econ",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.econ.EconSchemaFactory",
      "operand": {
        "cacheDir": "/data/cache",
        "parquetDir": "/data/parquet",
        "storageType": "s3"
      }
    }
  ]
}
```

#### SQL Access
```sql
-- Query FRED GDP time series
SELECT observation_date, value
FROM econ.fred_series
WHERE series_id = 'GDP' AND year >= 2020;

-- Join BEA regional income with TIGER states
SELECT s.state_name, r.personal_income
FROM econ.bea_regional_income r
JOIN geo.tiger_states s ON r.geoid = s.geoid
WHERE r.year = 2023;
```

---

## Core Architecture

### 1. **Abstract Base Layer**

#### AbstractGovDataDownloader
**Purpose**: Foundation for all domain downloaders

**Key Responsibilities**:
- HTTP client management with rate limiting
- Retry logic with exponential backoff
- Metadata-driven path resolution
- Bulk download coordination
- Cache manifest integration

**Core Methods**:
```java
// Download time-series data for year range
public void downloadAll(int startYear, int endYear);

// Convert raw data to Parquet
public void convertAll(int startYear, int endYear);

// Download reference/catalog tables
public void downloadReferenceData();

// Execute HTTP request with retry
protected <T> T executeWithRetry(String url, Class<T> responseClass);
```

**Configuration from Schema**:
```json
{
  "download": {
    "rateLimit": {
      "minIntervalMs": 1000,
      "maxRetries": 3,
      "retryDelayMs": 1000
    }
  }
}
```

#### AbstractCacheManifest
**Purpose**: Unified cache tracking across domains

**Shared Fields**:
- `filePath`: Raw file location
- `parquetPath`: Converted Parquet location (avoids S3 exists checks)
- `cachedAt`: Download timestamp
- `refreshAfter`: Explicit TTL timestamp
- `refreshReason`: Human-readable refresh policy
- `etag`: HTTP ETag for conditional GET
- `downloadRetry`: API error retry timestamp (0 = no restriction)
- `lastError`, `errorCount`: API error tracking

**Domain Extensions**:
- `CacheManifest` (ECON): Adds `catalogSeriesCache` for FRED catalog
- `GeoCacheManifest` (GEO): Optimized for S3 with no file existence checks
- `SecCacheManifest` (SEC): Tracks CIK resolution and filing metadata

#### CacheManifestQueryHelper
**Purpose**: High-performance cache filtering via DuckDB

**Problem**: Filtering 25,000 cached entries against 163,000 possible combinations takes 1-2 seconds with Java loops

**Solution**: DuckDB SQL filtering takes 50-100ms

```java
// Filter cache for specific year/state/supersector combinations
List<CacheKey> needed = CacheManifestQueryHelper.filterCacheManifest(
    cacheManifest,
    Arrays.asList(2020, 2021, 2022), // years
    Arrays.asList("06", "36", "48"), // state FIPS
    Arrays.asList("05", "10", "20")  // supersectors
);
```

**Strategies**:
- **Direct query** (<1000 entries): Avoids temp table overhead
- **Temp table** (large lists): Better hash join performance

#### CacheKey
**Purpose**: Immutable cache identifier

**Format**: `tableName:param1=value1:param2=value2` (sorted alphabetically)

**Example**: `fred_series:series_id=UNRATE:year=2023`

**Benefits**:
- Eliminates year as special concept (just another partition)
- Hashable for efficient lookups
- Human-readable for debugging

#### BulkDownloadConfig
**Purpose**: Multi-table downloads from single source

**Use Case**: QCEW quarterly ZIP feeds 4 tables (establishments, wages, employment, ownership)

**Structure**:
```json
{
  "bulkDownloads": {
    "qcew_quarterly": {
      "cachePattern": "source=bls/bulk/qcew_{year}_q{quarter}.zip",
      "url": "https://data.bls.gov/cew/data/files/{year}/csv/qcew_{year}_q{quarter}.zip",
      "variables": ["year", "quarter"],
      "comment": "Download once, convert to 4 tables"
    }
  }
}
```

### 2. **Schema Factory Pattern**

Each domain implements `SchemaFactory`:
```java
public class EconSchemaFactory implements SchemaFactory {
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        // 1. Load schema definition (econ-schema.json/yaml)
        // 2. Create StorageProvider (local or S3)
        // 3. Initialize downloaders (FRED, BEA, BLS, etc.)
        // 4. Register converters
        // 5. Return FileSchema with metadata
    }
}
```

**Factory Implementations**:
- `EconSchemaFactory`: Economic data (FRED, BEA, BLS, Treasury, World Bank)
- `GeoSchemaFactory`: Geographic data (TIGER/Line, HUD crosswalks)
- `SecSchemaFactory`: Securities data (SEC EDGAR, Alpha Vantage)
- `CensusSchemaFactory`: Census demographic data

---

## Ingestion Model

### 1. **Metadata-Driven Configuration**

All schemas defined in JSON/YAML in `src/main/resources/{schema}/`:

**Schema Structure**:
```json
{
  "version": "1.0",
  "partitionedTables": [
    {
      "name": "fred_series",
      "comment": "FRED economic time series observations",
      "pattern": "source=fred/type=series/series_id={series_id}/year={year}/data.json",
      "partitions": [
        {
          "name": "series_id",
          "type": "STRING",
          "required": true,
          "values": ["GDP", "UNRATE", "CPIAUCSL"]
        },
        {
          "name": "year",
          "type": "INTEGER",
          "required": true,
          "min": 1947,
          "max": 2024
        }
      ],
      "columns": [
        {"name": "series_id", "type": "STRING", "nullable": false},
        {"name": "observation_date", "type": "DATE", "nullable": false},
        {"name": "value", "type": "DOUBLE", "nullable": true},
        {"name": "realtime_start", "type": "DATE", "nullable": false},
        {"name": "realtime_end", "type": "DATE", "nullable": false}
      ],
      "download": {
        "rateLimit": {
          "minIntervalMs": 1000,
          "maxRetries": 3,
          "retryDelayMs": 1000
        },
        "listKey": ["GDP", "UNRATE", "CPIAUCSL"],
        "apiEndpoint": "https://api.stlouisfed.org/fred/series/observations"
      }
    }
  ],
  "referenceTables": [
    {
      "name": "reference_fred_series",
      "comment": "FRED series catalog with metadata",
      "pattern": "source=fred/type=catalog/reference_fred_series.json",
      "columns": [...],
      "download": {
        "apiEndpoint": "https://api.stlouisfed.org/fred/series/search"
      }
    }
  ],
  "bulkDownloads": {
    "qcew_quarterly": {
      "cachePattern": "source=bls/bulk/qcew_{year}_q{quarter}.zip",
      "url": "https://data.bls.gov/cew/data/files/{year}/csv/qcew_{year}_q{quarter}.zip",
      "variables": ["year", "quarter"]
    }
  }
}
```

### 2. **Download Workflow**

#### Standard Download Flow
```
1. User Query → SQL Parser → Table Scan
2. FileSchema checks parquet existence
3. If missing → RawToParquetConverter
4. Converter checks raw cache via CacheManifest
5. If expired/missing → Downloader.download()
6. Download with rate limit + retry
7. Update CacheManifest with ETag/TTL
8. Convert raw → Parquet
9. Return TableScan over Parquet
```

#### Bulk Download Flow
```
1. Identify bulk download pattern (e.g., QCEW ZIP)
2. Check if bulk file cached and fresh
3. Download once if needed
4. Convert multiple tables from same ZIP
5. Each table tracks own Parquet path
```

### 3. **Conversion Workflow**

#### Path-Based Routing
```java
public class EconRawToParquetConverter implements RawToParquetConverter {
    public void convert(String rawPath, String parquetPath) {
        if (rawPath.contains("/fred/")) {
            convertFredSeries(rawPath, parquetPath);
        } else if (rawPath.contains("/bea/")) {
            convertBeaData(rawPath, parquetPath);
        } else if (rawPath.contains("/bls/")) {
            convertBlsData(rawPath, parquetPath);
        }
        // ... other handlers
    }
}
```

#### Transformation Steps
```
1. Read raw JSON/CSV from StorageProvider
2. Parse and validate structure
3. Apply schema-specific transformations:
   - Unwrap API response (e.g., {"observations": [...]})
   - Flatten nested structures
   - Map field names (API → Parquet column)
   - Type conversion (string dates → DATE type)
   - Add partition columns if needed
4. Build Apache Arrow Table
5. Write Parquet with StorageProvider
6. Update CacheManifest.parquetPath
```

### 4. **Query Execution**

#### Table Scan Optimization
```sql
SELECT * FROM econ.fred_series
WHERE series_id = 'GDP' AND year = 2023;
```

**Execution**:
1. Calcite pushes filters to FileSchema
2. FileSchema identifies required partitions: `series_id=GDP/year=2023`
3. Checks parquet existence for that partition
4. DuckDB reads Parquet with filter pushdown
5. Returns result set

#### Cross-Domain Joins
```sql
SELECT s.state_name, b.personal_income
FROM econ.bea_regional_income b
JOIN geo.tiger_states s ON b.geoid = s.geoid
WHERE b.year = 2023 AND s.state_fips = '06';
```

**Execution**:
1. Scan `bea_regional_income` with year filter
2. Scan `tiger_states` with state filter
3. Hash join on geoid
4. Both scans use Parquet filter pushdown

---

## Domain-Specific Implementations

### 1. Economic Data (ECON)

**Package**: `org.apache.calcite.adapter.govdata.econ`

**Schema**: `econ-schema.json` / `econ-schema.yaml`

#### AbstractEconDataDownloader
**Purpose**: Template for all economic downloaders

**Pattern**: Enforces cache manifest usage

**Default Implementations**:
```java
// No-op defaults (subclasses override as needed)
protected void downloadReferenceData() { }
protected void downloadAll(int startYear, int endYear) { }
protected void convertAll(int startYear, int endYear) { }
```

---

#### FRED (Federal Reserve Economic Data)

**Downloader**: `FredDataDownloader`

**Data Source**: https://api.stlouisfed.org/fred/

**Coverage**: 841,000+ economic time series
- Interest rates (federal funds rate, treasury yields)
- GDP and components
- Monetary aggregates (M1, M2, M3)
- Inflation metrics (CPI, PCE, PPI)
- Employment statistics
- Housing market data

**Key Methods**:
```java
// Build list of series to download (catalog + custom)
private List<String> buildSeriesList();

// Download observations for all series in year range
public void downloadAll(int startYear, int endYear);

// Convert cached JSON to Parquet
public void convertCachedJsonToParquet(String seriesId, int year);
```

**Reference Tables**:
- `reference_fred_series`: Catalog of all series with metadata (via `FredCatalogDownloader`)

**API Structure**:
```
GET /fred/series/observations
  ?series_id=GDP
  &observation_start=2023-01-01
  &observation_end=2023-12-31
  &api_key=XXX

Response:
{
  "realtime_start": "2024-01-15",
  "realtime_end": "2024-01-15",
  "observation_start": "2023-01-01",
  "observation_end": "2023-12-31",
  "units": "lin",
  "output_type": 1,
  "file_type": "json",
  "order_by": "observation_date",
  "sort_order": "asc",
  "count": 4,
  "offset": 0,
  "limit": 100000,
  "observations": [
    {
      "realtime_start": "2024-01-15",
      "realtime_end": "2024-01-15",
      "date": "2023-01-01",
      "value": "26996.6"
    }
  ]
}
```

**Transformation**:
- Unwrap `observations` array
- Flatten to one row per observation
- Add `series_id` and `year` partition columns
- Parse `date` string → DATE type
- Parse `value` string → DOUBLE type

**Rate Limiting**: 120 requests/minute (configurable via schema metadata)

**Caching**:
- Current year: Daily refresh
- Historical years: Immutable (never refresh)
- ETag-based conditional GET

---

#### BEA (Bureau of Economic Analysis)

**Downloader**: `BeaDataDownloader`

**Data Source**: https://apps.bea.gov/api/

**Coverage**: GDP, personal income, trade, industry statistics
- National accounts (NIPA tables)
- GDP by industry
- Regional income and product accounts
- Trade balance and ITA data
- Fixed assets

**Key Tables**:
```
national_accounts      - NIPA tables (GDP components, frequency=A/Q)
industry_gdp          - GDP by industry
regional_income       - Personal income by state/county
trade_balance         - Export/import data
regional_linecodes    - Reference table for line item codes
```

**Multi-Frequency Support**:
```java
// Annual frequency
downloadNipaTable("T10101", 2010, 2023, "A");

// Quarterly frequency
downloadNipaTable("T10101", 2010, 2023, "Q");
```

**NIPA Tables**: Loads active table list from reference catalog
- Configured in schema: `nipaTableList` or loaded from API

**Key Industries**: From schema metadata
```json
{
  "download": {
    "listKey": ["11", "21", "22", "23", "31-33", "..."]
  }
}
```

**API Structure**:
```
GET /api/data
  ?UserID=XXX
  &method=GetData
  &datasetname=Regional
  &TableName=CAINC1
  &LineCode=1
  &Year=2023
  &GeoFips=STATE
  &ResultFormat=json
```

**CacheManifestQueryHelper Integration**:
- 16,000+ cached entries
- 163,000 possible combinations (years × line codes × frequencies)
- DuckDB filtering reduces overhead from 2 seconds → 50ms

**Caching**:
- Historical data: Immutable
- Current year: Weekly refresh
- Regional line codes: Monthly refresh

---

#### BLS (Bureau of Labor Statistics)

**Downloader**: `BlsDataDownloader`

**Data Source**: https://api.bls.gov/publicAPI/

**Coverage**: Employment, wages, inflation, productivity
- Consumer Price Index (CPI) - regional and metro
- Producer Price Index (PPI)
- Employment statistics by state/industry
- JOLTS (Job Openings and Labor Turnover Survey)
- QCEW (Quarterly Census of Employment and Wages)

**Key Tables**:
```
regional_cpi              - CPI by region
state_industry_employment - Employment by state/supersector
metro_employment          - Metro area employment
jolts_data                - Job openings/hires/separations
qcew_establishments       - Quarterly establishment counts
qcew_wages                - Quarterly wage data
qcew_employment           - Quarterly employment levels
```

**Series ID Generation**:
```java
// Regional CPI: CUUR{regionCode}SA0
// Example: CUURS49ASA0 = West region, all items CPI

// State industry employment: SMU{stateFips}{area}{supersector}{datatype}
// Example: SMU06000000500000001 = California (06), statewide (00000),
//          construction (05), all employees (00000001)
```

**Helper Classes**:
```java
public class BlsSeriesHelper {
    // Generate all state industry employment series (51 × 22 = 1,122)
    public static List<String> generateStateIndustrySeries() {
        List<String> series = new ArrayList<>();
        for (String stateFips : ALL_STATE_FIPS) {
            for (String supersector : SUPERSECTORS) {
                series.add("SMU" + stateFips + "00000" + supersector + "00000001");
            }
        }
        return series;
    }
}
```

**Reference Tables**:
- `jolts_industries`: JOLTS industry codes
- `jolts_states`: JOLTS state codes

**Bulk Downloads**:
- QCEW quarterly ZIPs (one download → 4 tables)

**API Structure**:
```
POST /publicAPI/v2/timeseries/data/
{
  "seriesid": ["CUURS49ASA0", "CUUR0000SA0"],
  "startyear": "2020",
  "endyear": "2023",
  "registrationkey": "XXX"
}

Response:
{
  "status": "REQUEST_SUCCEEDED",
  "Results": {
    "series": [
      {
        "seriesID": "CUURS49ASA0",
        "data": [
          {
            "year": "2023",
            "period": "M01",
            "periodName": "January",
            "value": "309.685",
            "footnotes": []
          }
        ]
      }
    ]
  }
}
```

**Error Handling**: BLS API returns HTTP 200 with error in JSON
```json
{
  "status": "REQUEST_NOT_PROCESSED",
  "message": ["series id INVALID not found"]
}
```

**Solution**: `CacheManifest` tracks `lastError` and `errorCount`, implements 7-day retry cadence

---

#### Treasury

**Downloader**: `TreasuryDataDownloader`

**Data Source**: https://api.fiscaldata.treasury.gov/

**Coverage**: Yield curves, federal debt, interest rates

**Key Tables**:
```
treasury_yields - Daily treasury yield curve
federal_debt    - Outstanding federal debt
interest_rates  - Average interest rates
```

**Format**: CSV-based time series

---

#### World Bank

**Downloader**: `WorldBankDataDownloader`

**Data Source**: https://api.worldbank.org/

**Coverage**: Global economic indicators by country

**Reference Tables**:
- `worldbank-indicators.json`: Indicator codes and names
- `worldbank-countries.json`: Country codes and names

**Indicators**: GDP, population, inflation, trade, development metrics

---

#### EconRawToParquetConverter

**Purpose**: Route and transform all ECON domain raw data

**Path-Based Routing**:
```java
public void convert(String rawPath, String parquetPath) {
    if (rawPath.contains("/fred/series/")) {
        handleFredSeries(rawPath, parquetPath);
    } else if (rawPath.contains("/bea/national/")) {
        handleBeaNational(rawPath, parquetPath);
    } else if (rawPath.contains("/bls/regional_cpi/")) {
        handleBlsRegionalCpi(rawPath, parquetPath);
    }
    // ... more handlers
}
```

**Handler Responsibilities**:
1. Read JSON from StorageProvider
2. Unwrap API response structure
3. Flatten nested arrays/objects
4. Map API fields → Parquet columns
5. Type conversion (strings → typed values)
6. Add partition columns
7. Build Apache Arrow Table
8. Write Parquet to StorageProvider

**Registration**:
```java
// In EconSchemaFactory.create()
EconRawToParquetConverter converter = new EconRawToParquetConverter(
    fredDownloader, beaDownloader, blsDownloader,
    treasuryDownloader, worldBankDownloader
);
fileSchema.registerConverter("econ", converter);
```

---

### 2. Geographic Data (GEO)

**Package**: `org.apache.calcite.adapter.govdata.geo`

**Schema**: `geo-schema.json`

#### AbstractGeoDataDownloader
**Purpose**: Template for geographic downloaders

**Default Rate Limit**: 500ms (slower than ECON for geographic services)

---

#### TIGER/Line (Census Geography)

**Downloader**: `TigerDataDownloader`

**Data Source**: https://www2.census.gov/geo/tiger/

**Coverage**: Free, no-registration geographic boundaries
- States and equivalent entities (50 states + DC, PR, territories)
- Counties and equivalent entities (~3,200)
- Places (cities, towns, CDPs) (~30,000)
- ZIP Code Tabulation Areas (ZCTAs) (~33,000)
- Congressional districts (435 + territories)
- Census tracts (~84,000)
- Block groups (~242,000)
- Urban areas, school districts, tribal areas

**Key Tables**:
```
tiger_states         - State boundaries
tiger_counties       - County boundaries
tiger_places         - Incorporated places and CDPs
tiger_zctas          - ZIP Code Tabulation Areas
tiger_cbsas          - Core-Based Statistical Areas
tiger_tracts         - Census tracts
tiger_block_groups   - Census block groups
tiger_districts      - Congressional districts
```

**URL Structure Variations**:
```
# 2010 special case
https://www2.census.gov/geo/tiger/TIGER2010/STATE/2010/tl_2010_us_state10.zip

# 2011-2013
https://www2.census.gov/geo/tiger/TIGER2012/STATE/tl_2012_us_state.zip

# 2014+
https://www2.census.gov/geo/tiger/TIGER2020/STATE/tl_2020_us_state.zip
```

**File Suffix Differences**:
- 2010: `state10`, `county10`, `zcta510`
- 2011+: `state`, `county`, `zcta5`

**Year-Aware Download**:
```java
private String buildTigerUrl(String geography, int year) {
    String suffix = (year == 2010) ? geography + "10" : geography;
    String baseUrl = (year == 2010)
        ? "https://www2.census.gov/geo/tiger/TIGER2010/"
        : "https://www2.census.gov/geo/tiger/TIGER" + year + "/";

    return baseUrl + geography.toUpperCase() + "/tl_" + year + "_us_" + suffix + ".zip";
}
```

**Storage Support**:
- Local filesystem: Direct file I/O
- S3: Via StorageProvider (no intermediate download)

**Shapefile Processing**:
1. Download ZIP from Census
2. Extract shapefile components (.shp, .shx, .dbf, .prj)
3. Parse shapefile with GeoTools/JTS
4. Convert geometries to WKT/WKB
5. Extract attribute table
6. Write Parquet with geometry column

**GeoCacheManifest Optimization**:
- S3-aware: Skips file existence checks (uses `parquetPath` in manifest)
- Refresh policies: TIGER boundaries immutable per year

---

#### HUD Crosswalks

**Downloader**: `HudCrosswalkFetcher`

**Data Source**: https://www.huduser.gov/portal/datasets/usps_crosswalk.html

**Coverage**: ZIP-to-geographic area crosswalks
- ZIP → CBSA (Core-Based Statistical Areas)
- ZIP → County
- ZIP → Census Tract
- ZIP → Congressional District

**Key Tables**:
```
hud_zip_cbsa         - ZIP to metro area mapping
hud_zip_county       - ZIP to county mapping
hud_zip_tract        - ZIP to census tract mapping
hud_zip_cd           - ZIP to congressional district mapping
```

**Use Cases**:
- Geographic aggregation: Sum ZIP-level data to metro areas
- Address geocoding: ZIP → county/tract for analysis
- Legislative analysis: ZIP → congressional district

---

#### GeoSchemaFactory

**Schema Creation**:
```java
public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    // 1. Load geo-schema.json
    // 2. Create StorageProvider
    // 3. Initialize TIGER downloader
    // 4. Initialize HUD downloader
    // 5. Register ShapefileToParquetConverter
    // 6. Return FileSchema
}
```

**Converter Registration**:
```java
ShapefileToParquetConverter converter = new ShapefileToParquetConverter();
fileSchema.registerConverter("geo", converter);
```

---

### 3. Securities Data (SEC)

**Package**: `org.apache.calcite.adapter.govdata.sec`

**Schema**: `sec-schema.json`

#### AbstractSecDataDownloader
**Purpose**: Base for SEC downloaders

**Rate Limit**: 100ms (10 requests/second, per SEC guidelines)

---

#### EDGAR Filings

**Downloader**: `EdgarDownloader`

**Data Source**:
- https://data.sec.gov (bulk feeds)
- https://www.sec.gov/Archives/edgar (individual filings)

**Coverage**: SEC corporate filings
- 10-K: Annual reports
- 10-Q: Quarterly reports
- 8-K: Current events
- DEF 14A: Proxy statements
- S-1: IPO registration
- 13F: Institutional holdings

**Key Features**:

**CIK Resolution**:
```java
// Lookup company CIK by ticker
String cik = edgarDownloader.resolveCik("AAPL");
// Returns: "0000320193"
```

**Filing Type Filtering**:
```java
// Download all 10-K filings for Apple since 2020
edgarDownloader.downloadFilings(
    "0000320193",  // CIK
    "10-K",        // Filing type
    LocalDate.of(2020, 1, 1),
    LocalDate.now()
);
```

**Conditional GET**:
- Supports ETag and Last-Modified
- Avoids re-downloading unchanged filings

**User-Agent Requirement**:
```
SEC requires User-Agent: Company Name AdminContact@company.com
```

**Tables**:
```
edgar_filings       - Filing metadata (date, type, accession)
edgar_10k           - 10-K annual reports
edgar_10q           - 10-Q quarterly reports
edgar_8k            - 8-K current reports
xbrl_facts          - XBRL financial statement data
```

---

#### Alpha Vantage

**Downloader**: `AlphaVantageDownloader`

**Data Source**: https://www.alphavantage.co/

**Coverage**: Stock prices, technical indicators, forex

**Tables**:
```
stock_daily         - Daily OHLCV data
stock_intraday      - Intraday tick data
technical_sma       - Simple moving averages
technical_rsi       - Relative strength index
forex_rates         - Foreign exchange rates
```

**API Key**: Required (free tier: 5 requests/minute)

---

#### XbrlToParquetConverter

**Purpose**: Extract financial data from XBRL filings

**XBRL Taxonomy**: GAAP/IFRS standardized tags
- `us-gaap:Assets`
- `us-gaap:Liabilities`
- `us-gaap:Revenues`
- `us-gaap:NetIncomeLoss`

**Extraction**:
1. Parse XBRL XML
2. Extract facts with context (period, entity, scenario)
3. Map taxonomy → table columns
4. Handle dimensions (segments, members)
5. Write Parquet with facts table

---

### 4. Census & Demographic Data

**Package**: `org.apache.calcite.adapter.govdata.census`

**Schema**: `census-schema.json`

#### Census API Client

**Downloader**: `CensusApiClient`

**Data Source**: https://api.census.gov/data/

**Coverage**: Decennial Census, ACS, economic surveys

**Key Tables**:
```
decennial_population    - 2020/2010/2000 Census population
acs_demographics        - American Community Survey 5-year estimates
acs_income              - Income and poverty statistics
acs_housing             - Housing characteristics
economic_census         - Business statistics
```

**Variable Mapping**:
```java
CensusVariableMapper mapper = new CensusVariableMapper();
String variableId = mapper.map("total_population");
// Returns: "B01001_001E"
```

**Geographic Levels**:
- Nation
- State
- County
- Census Tract
- Block Group
- ZIP Code Tabulation Area

---

## Common Patterns

### 1. **Template Method Pattern**

**Structure**:
```
AbstractGovDataDownloader (defines flow)
  ↓
AbstractEconDataDownloader (refines for ECON)
  ↓
FredDataDownloader (implements FRED-specific)
```

**Flow**:
```java
// AbstractGovDataDownloader defines:
public void downloadAll(int startYear, int endYear) {
    for (int year = startYear; year <= endYear; year++) {
        downloadYear(year); // Abstract method
    }
}

// FredDataDownloader implements:
protected void downloadYear(int year) {
    for (String seriesId : buildSeriesList()) {
        downloadSeries(seriesId, year);
    }
}
```

### 2. **Strategy Pattern**

**RecordTransformer**: Transform records during conversion
```java
@FunctionalInterface
public interface RecordTransformer {
    Map<String, Object> transform(Map<String, Object> record);
}

// Usage
converter.setRecordTransformer(record -> {
    record.put("calculated_field",
        (Double) record.get("field1") + (Double) record.get("field2"));
    return record;
});
```

**DimensionProvider**: Supply dimension values dynamically
```java
@FunctionalInterface
public interface DimensionProvider {
    List<String> getValues(String dimensionName);
}

// Usage
downloader.setDimensionProvider(dim -> {
    if ("state_fips".equals(dim)) {
        return loadStateFipsCodes();
    }
    return Collections.emptyList();
});
```

### 3. **Functional Composition**

**TableOperation**: Download or convert with resolved paths
```java
@FunctionalInterface
public interface TableOperation {
    void execute() throws Exception;
}

// Build list of operations
List<TableOperation> operations = new ArrayList<>();
for (String seriesId : seriesList) {
    for (int year = 2010; year <= 2023; year++) {
        String finalSeriesId = seriesId;
        int finalYear = year;
        operations.add(() -> downloadSeries(finalSeriesId, finalYear));
    }
}

// Execute with retry and rate limiting
for (TableOperation op : operations) {
    executeWithRetry(op);
}
```

### 4. **Prefetch Optimization**

**PrefetchHelper**: Batch API calls via in-memory DuckDB

**Problem**: Downloading 20 years of data for one series = 20 API calls

**Solution**: Prefetch callback batches requests
```java
public interface PrefetchCallback {
    void prefetch(String dimensionValue);
}

// Downloader implements
downloader.setPrefetchCallback(seriesId -> {
    // Fetch all years for this series in one API call
    List<Observation> obs = fredApi.getObservations(seriesId, 1947, 2024);

    // Cache in in-memory DuckDB
    prefetchHelper.cacheData(seriesId, obs);
});

// Later, individual TableOperations pull from cache
for (int year = 2010; year <= 2023; year++) {
    List<Observation> yearObs = prefetchHelper.getData(seriesId, year);
    convertToParquet(yearObs, year);
}
```

**Result**: 20 API calls → 1 API call per series

### 5. **Separation of Concerns**

**Download** (AbstractGovDataDownloader):
- HTTP requests with retry
- Rate limiting
- Error handling
- Cache manifest updates

**Cache** (AbstractCacheManifest):
- TTL tracking
- ETag management
- Refresh policy enforcement
- Error retry cadence

**Conversion** (RawToParquetConverter):
- Raw format parsing
- Schema transformation
- Parquet writing
- No download logic

**Schema** (SchemaFactory):
- Metadata loading
- Table registration
- Downloader initialization
- No data processing

**Storage** (StorageProvider):
- File I/O abstraction
- Local vs S3 handling
- Path resolution
- No business logic

---

## Performance Optimizations

### 1. **DuckDB Cache Filtering**

**Problem**: Java loops for 25,000 cache entries × 163,000 combinations = slow

**Solution**: Load cache into DuckDB, filter with SQL

**Before** (Java):
```java
// O(n*m) - 1-2 seconds
for (CacheEntry entry : cacheEntries) {
    for (Combination combo : combinations) {
        if (entry.matches(combo)) {
            needed.add(entry);
        }
    }
}
```

**After** (DuckDB):
```sql
-- O(n + m) with hash join - 50-100ms
SELECT c.cache_key
FROM cache_manifest c
INNER JOIN (VALUES
    (2020, '06', '05'),
    (2020, '36', '05'),
    (2021, '06', '05')
) AS v(year, state, supersector)
ON c.year = v.year
   AND c.state_fips = v.state
   AND c.supersector = v.supersector
```

**Implementation**:
```java
List<CacheKey> needed = CacheManifestQueryHelper.filterCacheManifest(
    cacheManifest,
    years,      // [2020, 2021, 2022]
    states,     // ["06", "36", "48"]
    supersectors // ["05", "10", "20"]
);
```

### 2. **Bulk Downloads**

**Pattern**: Download once, convert many

**Example**: QCEW Quarterly
```
Single download: qcew_2023_q1.zip (500 MB)
↓
Convert to 4 tables:
- qcew_establishments
- qcew_wages
- qcew_employment
- qcew_ownership
```

**Savings**: 1 download vs 4 downloads (4x bandwidth reduction)

### 3. **Prefetch Batching**

**Pattern**: Aggregate API calls across dimensions

**Example**: FRED Series Download
```
Without prefetch:
- Series GDP, year 2020: API call 1
- Series GDP, year 2021: API call 2
- ...
- Series GDP, year 2023: API call 4
= 4 calls per series × 100 series = 400 calls

With prefetch:
- Series GDP, all years: API call 1
= 1 call per series × 100 series = 100 calls
```

**Reduction**: 75% fewer API calls

### 4. **Parquet Path Caching**

**Problem**: S3 `exists()` calls are expensive (100-200ms each)

**Solution**: Store `parquetPath` in cache manifest
```json
{
  "cacheKey": "fred_series:series_id=GDP:year=2023",
  "parquetPath": "s3://bucket/parquet/fred_series/series_id=GDP/year=2023/data.parquet"
}
```

**Before**: Check S3 exists for every query
```java
if (s3Client.doesObjectExist(bucket, key)) {
    return readParquet(path);
}
```

**After**: Check manifest (in-memory)
```java
CacheEntry entry = manifest.get(cacheKey);
if (entry.parquetPath != null) {
    return readParquet(entry.parquetPath);
}
```

**Savings**: 100-200ms per partition scan eliminated

### 5. **ETag Conditional GET**

**Pattern**: Skip downloads for unchanged data

**Flow**:
```
1. Check cache manifest for ETag
2. Send HTTP request with If-None-Match: "abc123"
3. Server responds:
   - 304 Not Modified → Use cached file
   - 200 OK + new ETag → Download and update cache
```

**Savings**: No data transfer for unchanged resources

**Implementation**:
```java
protected <T> T downloadWithEtag(String url, String cachedEtag, Class<T> clazz) {
    HttpGet request = new HttpGet(url);
    if (cachedEtag != null) {
        request.addHeader("If-None-Match", cachedEtag);
    }

    HttpResponse response = httpClient.execute(request);
    if (response.getStatusLine().getStatusCode() == 304) {
        // Not modified, use cached file
        return readFromCache();
    }

    // New data, update cache with new ETag
    String newEtag = response.getFirstHeader("ETag").getValue();
    T data = parseResponse(response, clazz);
    updateCacheManifest(url, newEtag);
    return data;
}
```

---

## Key Architectural Decisions

### 1. **Why Parquet?**
- **Columnar**: Efficient for analytical queries (select specific columns)
- **Compression**: 5-10x smaller than JSON/CSV
- **Schema**: Enforced types, no parsing errors
- **Partitioning**: Directory structure enables filter pushdown
- **DuckDB Integration**: Native Parquet support with excellent performance

### 2. **Why Metadata-Driven?**
- **Maintainability**: Schema changes don't require code changes
- **Scalability**: Add new tables via JSON, not new Java classes
- **Validation**: Schema enforces data contracts
- **Documentation**: Self-documenting via schema definitions
- **Tooling**: Can generate DDL, docs, test data from schema

### 3. **Why StorageProvider Abstraction?**
- **Flexibility**: Switch local ↔ S3 without code changes
- **Testing**: Use local filesystem in tests, S3 in production
- **Performance**: Optimize for each storage type (local buffering vs S3 batch)
- **Cost**: Start with local filesystem, migrate to S3 as data grows

### 4. **Why Domain Separation?**
- **Independence**: ECON changes don't affect GEO
- **Specialization**: Domain-specific optimizations (rate limits, transformations)
- **Team Ownership**: Different teams can own different domains
- **Dependency Management**: Avoid monolithic downloader

### 5. **Why Cache Manifests?**
- **Transparency**: Clear visibility into what's cached and why
- **Control**: Explicit refresh policies vs implicit heuristics
- **Debugging**: Easily see why a download occurred or didn't
- **Optimization**: DuckDB filtering for large manifests

---

## File Locations Reference

**Core Infrastructure**:
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractGovDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractCacheManifest.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/CacheManifestQueryHelper.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/CacheKey.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/BulkDownloadConfig.java`

**ECON Domain**:
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/AbstractEconDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/FredDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BlsDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/EconRawToParquetConverter.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/EconSchemaFactory.java`

**GEO Domain**:
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/geo/AbstractGeoDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/geo/TigerDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/geo/GeoSchemaFactory.java`

**SEC Domain**:
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/sec/AbstractSecDataDownloader.java`
- `/Users/kennethstott/calcite/govdata/src/main/java/org/apache/calcite/adapter/govdata/sec/EdgarDownloader.java`

**Schemas**:
- `/Users/kennethstott/calcite/govdata/src/main/resources/econ/econ-schema.json`
- `/Users/kennethstott/calcite/govdata/src/main/resources/geo/geo-schema.json`
- `/Users/kennethstott/calcite/govdata/src/main/resources/sec/sec-schema.json`
- `/Users/kennethstott/calcite/govdata/src/main/resources/census/census-schema.json`

---

## Summary

The GovData ETL architecture provides a **scalable, maintainable framework** for ingesting federal government data into Apache Calcite. Key strengths:

1. **Unified Abstraction**: Common patterns for HTTP, caching, conversion across domains
2. **Performance**: DuckDB optimization, bulk downloads, prefetch batching, Parquet columnar storage
3. **Reliability**: Retry logic, rate limiting, ETag caching, error tracking
4. **Flexibility**: Metadata-driven schemas, storage abstraction (local/S3), pluggable converters
5. **Scalability**: Add new domains/tables via schema JSON, minimal code changes

**Total Coverage**:
- **ECON**: 841,000+ FRED series, BEA national/regional accounts, BLS employment/inflation, Treasury yields, World Bank indicators
- **GEO**: 3,200+ counties, 30,000+ places, 84,000+ tracts, TIGER/Line shapefiles, HUD crosswalks
- **SEC**: EDGAR 10-K/10-Q/8-K filings, XBRL financials, stock prices, institutional holdings
- **Census**: Decennial Census, ACS demographics/income/housing, economic surveys

The architecture enables **SQL queries across government data sources** with automatic download, caching, and format conversion—all driven by declarative schema definitions.
