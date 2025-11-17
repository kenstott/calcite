# FredDataDownloader Architecture & Refactoring Diagram

## Current Architecture (397 lines)

```
FredDataDownloader.downloadSeries()
│
├─ Hardcoded FRED API URL building (15 lines)
├─ Rate limiting enforcement (10 lines)
├─ Retry logic with exponential backoff (20 lines)
├─ fetchSeriesObservationsPaginated() (99 lines)
│  ├─ Manual URL construction with offset
│  ├─ Rate limiting (sleep between requests)
│  ├─ Retry loop with exponential backoff
│  ├─ Response parsing (extract "observations")
│  ├─ Pagination loop (offset += count)
│  └─ Missing value filtering ("." → skip)
│
├─ Cache file writing (10 lines)
├─ Manifest updates (5 lines)
└─ Return cache path
```

**Problem:** ALL of this duplicates AbstractGovDataDownloader.executeDownload()!

---

## Metadata-Driven Architecture (Parent Class)

```
AbstractGovDataDownloader.executeDownload()
│
├─ Load metadata from schema JSON (12 lines)
│  └─ Get download config, seriesList, auth, rates
│
├─ For each series in seriesList: (loop)
│  │
│  ├─ buildDownloadUrl() → "https://...?series_id=DFF&api_key=KEY&..."
│  │
│  ├─ downloadWithPagination() (loop)
│  │  ├─ enforceRateLimit() → sleep 500ms between requests
│  │  ├─ executeWithRetry() → HTTP request with backoff
│  │  ├─ Parse response["observations"]
│  │  ├─ Check pagination → offset += count
│  │  └─ Aggregate data
│  │
│  └─ (next series)
│
├─ Write aggregated JSON to cache (15 lines)
├─ Update manifest (10 lines)
└─ Return cache path
```

**Advantage:** Generic, metadata-driven, handles ANY API!

---

## Schema Configuration (econ-schema.json)

```
fred_indicators table
│
├─ Pattern: "type=fred_indicators/series=*/year=*/fred_indicators.parquet"
│
├─ Download config:
│  ├─ baseUrl: "https://api.stlouisfed.org/fred/series/observations"
│  │
│  ├─ authentication:
│  │  ├─ type: "query_param"
│  │  ├─ paramName: "api_key"
│  │  └─ envVar: "FRED_API_KEY"
│  │
│  ├─ queryParams:
│  │  ├─ series_id: {type: "iteration"}     ← LOOP over seriesList
│  │  ├─ api_key: {type: "auth"}            ← Get from env var
│  │  ├─ observation_start: {type: "expression", value: "startOfYear({year})"}
│  │  ├─ observation_end: {type: "expression", value: "endOfYear({year})"}
│  │  ├─ limit: {type: "constant", value: 100000}
│  │  └─ offset: {type: "pagination"}       ← Handle pagination
│  │
│  ├─ seriesList: ["DFF", "UNRATE", "GDP", "CPIAUCSL", ...]
│  │
│  ├─ rateLimit:
│  │  ├─ minIntervalMs: 500                 ← 500ms between requests
│  │  ├─ maxRetries: 5
│  │  └─ retryDelayMs: 5000
│  │
│  ├─ pagination:
│  │  ├─ enabled: true
│  │  └─ maxPerRequest: 100000
│  │
│  └─ response:
│     ├─ dataPath: "observations"           ← Extract from response["observations"]
│     └─ missingValueIndicator: "."         ← Filter missing values
```

**Everything is in the schema. No hardcoding in code!**

---

## Data Flow Comparison

### Current (FredDataDownloader.downloadSeries)
```
EconSchemaFactory
  ↓
for (String seriesId : allSeriesIds) {
  ↓
  FredDataDownloader.downloadSeries(seriesId, 2023, 2023)
    ↓
    fetchSeriesObservationsPaginated()
      ├─ Build URL manually
      ├─ Enforce rate limit manually
      ├─ Call FRED API
      ├─ Handle retries manually
      ├─ Extract observations
      ├─ Handle pagination manually
      └─ Filter missing values
    ↓
    Save to cache
    ↓
  FredDataDownloader.convertCachedJsonToParquet()
    └─ Use metadata-driven conversion
}
```

### Refactored (Using executeDownload)
```
EconSchemaFactory
  ↓
FredDataDownloader.executeDownload("fred_indicators", {year: 2023})
  ↓
  AbstractGovDataDownloader.executeDownload()
    ├─ Load schema metadata
    ├─ Get seriesList from schema
    ├─ For each series:
    │  ├─ Build URL from metadata (buildDownloadUrl)
    │  ├─ Call FRED API (executeWithRetry)
    │  │  ├─ enforceRateLimit() - 500ms between requests
    │  │  ├─ Retry with backoff on 429/5xx
    │  │  └─ executeWithRetry() returns response
    │  ├─ Parse response using dataPath
    │  ├─ Handle pagination
    │  └─ Aggregate data
    ├─ Write aggregated JSON to cache
    └─ Return cache path
  ↓
  For each series/year:
    FredDataDownloader.convertCachedJsonToParquet()
      └─ Metadata-driven conversion
```

**Benefits:**
- Single source of truth: schema JSON
- Reuses parent class infrastructure
- No duplicate code
- Easier to maintain
- More testable

---

## Specific Duplication Examples

### URL Building
**FredDataDownloader.downloadSeries (lines 195-202):**
```java
String url = FRED_API_BASE + "series/observations"
    + "?series_id=" + seriesId
    + "&api_key=" + apiKey
    + "&file_type=json"
    + "&observation_start=" + startDate
    + "&observation_end=" + endDate
    + "&limit=" + MAX_OBSERVATIONS_PER_REQUEST
    + "&offset=" + offset;
```

**AbstractGovDataDownloader.buildDownloadUrl() (lines 514-591):**
```java
// Generic implementation using schema metadata
// Handles: constant, expression, auth, iteration, pagination
// Already does URL encoding!
```

### Rate Limiting
**FredDataDownloader.fetchSeriesObservationsPaginated (lines 214-222):**
```java
synchronized (this) {
  long now = System.currentTimeMillis();
  long timeSinceLastRequest = now - lastRequestTime;
  if (timeSinceLastRequest < MIN_REQUEST_INTERVAL_MS) {
    long sleepTime = MIN_REQUEST_INTERVAL_MS - timeSinceLastRequest;
    Thread.sleep(sleepTime);
  }
  lastRequestTime = System.currentTimeMillis();
}
```

**AbstractGovDataDownloader.enforceRateLimit() (lines 102-117):**
```java
// Same logic, generic implementation
// Uses getMinRequestIntervalMs() from subclass
```

### Retry Logic
**FredDataDownloader.fetchSeriesObservationsPaginated (lines 210-245):**
```java
while (attempt < MAX_RETRIES) {
  // enforce rate limit
  response = httpClient.send(request, ...);
  if (response.statusCode() == 200) break;
  else if (response.statusCode() == 429) {
    // exponential backoff
    long backoffDelay = RETRY_DELAY_MS * (1L << (attempt - 1));
    Thread.sleep(backoffDelay);
  }
}
```

**AbstractGovDataDownloader.executeWithRetry() (lines 120-151):**
```java
// Same logic, generic implementation
// Used by executeDownload() internally
```

---

## What Gets Removed

### Direct Removal (Complete Functions)
```java
// Lines 64-158 (95 lines) - NOT USED IN PRODUCTION
public static class Series {
  public static final String FED_FUNDS_RATE = "DFF";
  public static final String UNEMPLOYMENT_RATE = "UNRATE";
  // ... 50+ more constants
}

// Lines 185-284 (99 lines) - DUPLICATE OF executeDownload()
private List<JsonNode> fetchSeriesObservationsPaginated(
    String seriesId, String startDate, String endDate)
    throws IOException, InterruptedException {
  // All this code is now in AbstractGovDataDownloader.downloadWithPagination()
  // ... manual URL building
  // ... manual rate limiting
  // ... manual retry logic
  // ... manual pagination
}
```

### Cleanup in downloadSeries() (lines 332-394)
- Remove custom URL building → use buildDownloadUrl()
- Remove rate limiting → use enforceRateLimit()
- Remove retry logic → use executeWithRetry()
- Remove pagination → use downloadWithPagination()
- Keep only wrapper logic

---

## File Size Impact

```
FredDataDownloader.java

BEFORE:
├─ Metadata loading: 0 lines (does it manually)
├─ HTTP execution: 0 lines (httpClient in parent)
├─ Rate limiting: ~10 lines (custom)
├─ Retry logic: ~20 lines (custom)
├─ URL building: ~15 lines (custom)
├─ Pagination: ~20 lines (custom)
├─ Series constants: 95 lines
├─ fetchSeriesObservationsPaginated: 99 lines
├─ downloadSeries: 62 lines (uses custom helpers)
├─ convertToParquet: 15 lines
├─ convertCachedJsonToParquet: 4 lines
└─ TOTAL: 397 lines

AFTER:
├─ Series constants: 0 lines (REMOVED - not used)
├─ fetchSeriesObservationsPaginated: 0 lines (REMOVED - use parent)
├─ downloadSeries: 15 lines (thin wrapper calling parent)
├─ convertToParquet: 15 lines (KEPT - used by factory)
├─ convertCachedJsonToParquet: 4 lines (KEPT - public API)
└─ TOTAL: ~160 lines

REDUCTION: 397 → 160 (60% fewer lines)
DUPLICATE CODE REMOVED: ~240 lines
```

---

## Integration Points

### EconSchemaFactory usage (line 488)
```java
FredDataDownloader fredDownloader = new FredDataDownloader(...);
for (String seriesId : allSeriesIds) {
  // Calls downloadSeries() - still works if kept as wrapper
  fredDownloader.downloadSeries(seriesId, startYear, endYear);

  // Calls parent method via wrapper
  fredDownloader.convertCachedJsonToParquet("fred_indicators", variables);
}
```

### Test usage
```java
// EconDataDownloadTest.java line 141
FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey, ...);
downloader.downloadSeries("DFF", 2023, 2023);  // Still works if wrapper kept

// EconSchemaIntegrationTest.java line 130
fredDownloader.downloadSeries("DFF", 2024, 2024);  // Still works
```

**All integration points continue to work with thin wrapper!**

---

## Migration Path

### Phase 1: Keep downloadSeries as Thin Wrapper
```
downloadSeries()
  → Still works (tests pass)
  → Internally uses executeDownload()
  → No breaking changes
  → Code reduction: 60%
```

### Phase 2: Update EconSchemaFactory (Optional)
```
Instead of:
  for (String seriesId : allSeriesIds) {
    fredDownloader.downloadSeries(seriesId, ...);
  }

Could use:
  String jsonPath = fredDownloader.executeDownload("fred_indicators", {year});
  // But need to handle dynamic seriesList
```

### Phase 3: Make Schema Dynamic (Future)
```
Allow EconSchemaFactory to inject:
  - Dynamic seriesList from catalog
  - Dynamic rate limits
  - Dynamic source parameters

Then fully eliminate downloadSeries()
```

---

## Summary

| Aspect | Current | Refactored | Improvement |
|--------|---------|-----------|-------------|
| Total lines | 397 | 160 | -60% |
| Duplicate code | ~240 | 0 | 100% removed |
| Rate limiting | Custom | Inherited | Consistent |
| Retry logic | Custom | Inherited | Consistent |
| Pagination | Custom | Inherited | Consistent |
| Test breakage | N/A | None | No changes |
| Risk | N/A | Low | Safe wrapper |
| Maintenance | High | Low | Single source |

**Conclusion:** The metadata-driven approach is production-ready and significantly reduces code duplication while maintaining full backward compatibility.
