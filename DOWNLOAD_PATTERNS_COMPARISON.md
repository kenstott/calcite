# Download Patterns Comparison

## Pattern Overview

| Aspect | executeDownload() | downloadAll() | downloadSeries() |
|--------|------------------|----------------|------------------|
| **Type** | Metadata-Driven | Custom Domain | Custom Domain |
| **Generic** | Yes | No | No |
| **Scope** | Any table | BLS/BEA/Treasury | FRED only |
| **API Key** | env var from metadata | Passed to constructor | Passed to constructor |
| **Iteration** | Via schema seriesList | None (downloads all) | Explicit per-series |
| **Pagination** | Yes (automatic) | No | No |
| **Caching** | Manifest-based | Implicit (path-based) | Per-series check |
| **Return Value** | JSON cache path | Nothing | Nothing |
| **Data Format** | JSON | JSON | JSON |
| **Currently Used** | NO | YES (BLS/BEA/Treasury) | YES (FRED) |

---

## Method Signatures

### 1. executeDownload() - Generic Infrastructure
```java
protected String executeDownload(String tableName, Map<String, String> variables)
    throws IOException, InterruptedException
```

**Input:**
- `tableName` - e.g., "fred_indicators", "employment_statistics"
- `variables` - e.g., {year: "2020", series: "DFF"}

**Output:**
- Relative path to cached JSON file (e.g., "type=fred_indicators/series=DFF/year=2020/fred_indicators.json")

**Process:**
1. Load table metadata from schema
2. Check if "download" is enabled
3. Iterate over seriesList (if configured)
4. For each iteration, handle pagination
5. Aggregate results
6. Write to cache
7. Return cache path

**Example Usage (Hypothetical - NOT ACTUALLY CALLED):**
```java
for (String seriesId : allSeriesIds) {
    for (int year = startYear; year <= endYear; year++) {
        variables.put("series", seriesId);
        variables.put("year", String.valueOf(year));

        // Could use generic metadata-driven download:
        String jsonPath = fredDownloader.executeDownload("fred_indicators", variables);
    }
}
```

---

### 2. downloadAll() - Custom Domain Pattern
```java
public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException
public void downloadAll(int startYear, int endYear, Set<String> enabledTables) throws IOException
```

**Input:**
- `startYear`, `endYear` - Year range
- `enabledTables` - (optional) Which tables to download

**Output:**
- Nothing (side effect: files written to cache)

**Cache Path Convention:**
- Caller must know path pattern (e.g., "type=employment_statistics/year=2020/...")
- Paths are hardcoded in download implementation

**Process:**
1. Download raw data for all tables
2. Save to cache (path determined by implementation)
3. Return control to caller
4. Caller must convert each table to Parquet

**Example Usage (BLS):**
```java
BlsDataDownloader blsDownloader = new BlsDataDownloader(cacheDir, ...);
blsDownloader.downloadAll(startYear, endYear, enabledTables);

// Then caller must convert each table:
for (int year = startYear; year <= endYear; year++) {
    blsDownloader.convertToParquet(employmentRawPath, employmentParquetPath);
    blsDownloader.convertToParquet(inflationRawPath, inflationParquetPath);
    // ... for each table ...
}
```

---

### 3. downloadSeries() - FRED-Specific Pattern
```java
public void downloadSeries(String seriesId, int startYear, int endYear)
    throws IOException, InterruptedException
```

**Input:**
- `seriesId` - FRED series identifier (e.g., "DFF", "GDP", "UNRATE")
- `startYear`, `endYear` - Year range

**Output:**
- Nothing (side effect: file written to series-specific cache path)

**Cache Path Convention:**
- `type=fred_indicators/series={seriesId}/year={startYear}/fred_indicators.json`
- Derived from seriesId and year parameters

**Process:**
1. Check if already cached (per-series check)
2. Fetch observations from FRED API for the year range
3. Filter out missing values (FRED uses "." for missing)
4. Extract just series_id, date, value
5. Save to series-specific cache file
6. Return control to caller

**Example Usage (FRED):**
```java
FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, fredApiKey, ...);

for (String seriesId : allSeriesIds) {
    // Download this series (per-series cache check)
    fredDownloader.downloadSeries(seriesId, startYear, endYear);

    // Then convert each (series, year) combination
    for (int year = startYear; year <= endYear; year++) {
        variables.put("series", seriesId);
        variables.put("year", String.valueOf(year));
        fredDownloader.convertCachedJsonToParquet("fred_indicators", variables);
    }
}
```

---

## Design Comparison

### Metadata-Driven (executeDownload)
**Pros:**
- Generic - works for any table with schema metadata
- Complete orchestration in one method
- Handles pagination transparently
- Returns clear path result
- Testable in isolation

**Cons:**
- Requires complete schema metadata
- Generic method less clear about what's happening
- No API-specific optimizations
- Currently unused (infrastructure burden)

### downloadAll() Pattern
**Pros:**
- Explicit API for each domain
- Can optimize for specific API quirks
- Handles multiple tables in one call
- Matches API semantics naturally

**Cons:**
- Caller must know cache paths
- Path coupling between download and conversion
- Conversion code scattered in orchestrator
- No clear abstraction for iteration

### downloadSeries() Pattern
**Pros:**
- Explicit per-series iteration API
- Per-series caching (can skip unchanged series)
- Clear semantics: "download this series for this year range"
- Optimizable per series
- Matches FRED API semantics (series-based)

**Cons:**
- FRED-specific (not reusable for other APIs)
- Caller must write nested loop
- Path convention must match schema pattern
- Additional layer of abstraction

---

## Data Flow Comparison

### executeDownload() Flow
```
Input: tableName + variables
   |
   +-> loadTableMetadata(tableName)
   |     |
   |     +-> Read econ-schema.json
   |     +-> Find table definition
   |     +-> Extract "download" section
   |
   +-> Check if download enabled
   +-> Check if seriesList defined
   +-> FOR each series (from seriesList):
   |     |
   |     +-> FOR each page (pagination):
   |           |
   |           +-> buildDownloadUrl(variables, series, offset)
   |           +-> executeWithRetry(HttpRequest)
   |           +-> Parse response
   |           +-> Extract data via dataPath
   |           +-> Add to aggregation
   |
   +-> Write aggregated data to JSON cache
   +-> Mark as cached in manifest
   |
Output: JSON cache path
```

### downloadAll() Flow
```
Input: startYear, endYear, [enabledTables]
   |
   +-> FOR each table:
   |     |
   |     +-> Download raw data
   |     +-> Save to cache (hardcoded path)
   |
Output: Nothing
   |
   +-> Caller must:
       +-> Know cache paths
       +-> Loop over years
       +-> Call convertToParquet() for each
```

### downloadSeries() Flow
```
Input: seriesId, startYear, endYear
   |
   +-> Check if cached (series + year key)
   +-> Fetch observations from FRED
   +-> Filter missing values (FRED ".")
   +-> Extract fields: series_id, date, value
   +-> Save to series-specific cache
   |     (path: type=fred_indicators/series={id}/year={year}/...)
   |
Output: Nothing
   |
   +-> Caller must:
       +-> Set variables: series={id}, year={year}
       +-> Call convertCachedJsonToParquet()
```

---

## When to Use Each Pattern

### Use executeDownload() IF:
1. Table has complete schema metadata "download" section
2. Need generic, reusable download mechanism
3. API matches pagination + seriesList iteration model
4. Want metadata to drive behavior

### Use downloadAll() IF:
1. Downloading multiple unrelated tables
2. Can't express API as simple iteration
3. Need domain-specific optimizations
4. API has multiple data sources per endpoint

### Use downloadSeries() IF:
1. Downloading series from FRED or similar
2. Need per-series caching optimization
3. Want explicit series iteration in code
4. Series are independent units

---

## Current State (As of Nov 2024)

### What's Implemented
- `executeDownload()` - Complete, tested, NOT USED
- `downloadAll()` - Used by BLS, BEA, Treasury, World Bank
- `downloadSeries()` - Used by FRED
- `convertCachedJsonToParquet()` - Used by ALL (fully metadata-driven)

### What's Missing
- Schema metadata for BLS/BEA/Treasury "download" sections
- Tests for executeDownload()
- Documentation of when to use which pattern

### What Could Be Improved
1. Add schema metadata for all tables (would enable executeDownload)
2. Unify orchestration code (currently scattered in EconSchemaFactory)
3. Better caching strategies per API
4. Standardize response formats for metadata-driven conversions

---

## Conclusion

All three patterns coexist by design:

1. **executeDownload()** provides infrastructure for metadata-driven downloads
2. **downloadAll()** is the practical pattern for complex APIs
3. **downloadSeries()** is FRED-specific optimization
4. **convertCachedJsonToParquet()** is the unified conversion layer

They are NOT redundant—each serves a specific level of abstraction and use case.
