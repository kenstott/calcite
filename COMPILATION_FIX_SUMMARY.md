# Compilation Error Fix Summary

## Overview
The refactoring changed method signatures from `(String dataType, int year, Map<String, String> params)` to use `CacheKey` objects. This document summarizes all fixes applied and remaining work.

## Completed Fixes

### 1. CacheKey Constructor
**File:** `govdata/src/main/java/org/apache/calcite/adapter/govdata/CacheKey.java`
- Changed constructor from `package-private` to `public`
- This allows downstream classes to create CacheKey instances

### 2. AbstractEconDataDownloader
**File:** `govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/AbstractEconDataDownloader.java`
- Added imports: `import org.apache.calcite.adapter.govdata.CacheKey;` and `import java.util.HashMap;`
- Methods `isCachedOrExists()` and `isParquetConvertedOrExists()` already correctly use CacheKey

### 3. BeaDataDownloader - Lambda Expressions
**File:** `govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BeaDataDownloader.java`
- Added import: `import org.apache.calcite.adapter.govdata.CacheKey;`
- Fixed ALL lambda expressions (8 total) from `(year, vars) ->` to `(cacheKey) ->` pattern

**Lambda Pattern Applied:**
```java
// OLD:
(year, vars) -> {
    // Use year and vars directly
    cacheManifest.markCached(tableName, year, vars, path, size);
}

// NEW:
(cacheKey) -> {
    // Extract from cacheKey
    Map<String, String> vars = cacheKey.getParameters();
    int year = Integer.parseInt(cacheKey.getParameter("year"));
    // Use extracted values
    cacheManifest.markCached(cacheKey, path, size, refreshAfter, reason);
}
```

## Remaining Fixes Needed

### Pattern 1: isCachedOrExists() calls
**Current (BROKEN):**
```java
if (isCachedOrExists(tableName, year, params)) {
```

**Fix Required:**
```java
Map<String, String> allParams = new HashMap<>(params != null ? params : new HashMap<>());
allParams.put("year", String.valueOf(year));
CacheKey cacheKey = new CacheKey(tableName, allParams);
if (isCachedOrExists(cacheKey)) {
```

### Pattern 2: markCached() calls
**Current (BROKEN):**
```java
cacheManifest.markCached(tableName, year, params, path, size);
```

**Fix Required:**
```java
Map<String, String> allParams = new HashMap<>(params != null ? params : new HashMap<>());
allParams.put("year", String.valueOf(year));
CacheKey cacheKey = new CacheKey(tableName, allParams);
int currentYear = java.time.LocalDate.now().getYear();
cacheManifest.markCached(cacheKey, path, size,
    year == currentYear ? System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24) : Long.MAX_VALUE,
    year == currentYear ? "current_year_daily" : "historical_immutable");
```

### Pattern 3: markParquetConverted() calls
**Current (BROKEN):**
```java
cacheManifest.markParquetConverted(tableName, year, params, parquetPath);
```

**Fix Required:**
```java
Map<String, String> allParams = new HashMap<>(params != null ? params : new HashMap<>());
allParams.put("year", String.valueOf(year));
CacheKey cacheKey = new CacheKey(tableName, allParams);
cacheManifest.markParquetConverted(cacheKey, parquetPath);
```

### Pattern 4: isParquetConverted() calls
**Current (BROKEN):**
```java
if (cacheManifest.isParquetConverted(tableName, year, params)) {
```

**Fix Required:**
```java
Map<String, String> allParams = new HashMap<>(params != null ? params : new HashMap<>());
allParams.put("year", String.valueOf(year));
CacheKey cacheKey = new CacheKey(tableName, allParams);
if (cacheManifest.isParquetConverted(cacheKey)) {
```

## Files Needing Fixes

### High Priority (Many Errors)
1. **BlsDataDownloader.java** - ~27 errors
   - Lambda expressions: 1 occurrence (line ~874)
   - isCachedOrExists calls: 11 occurrences
   - markParquetConverted calls: 6 occurrences
   - isParquetConverted calls: 4 occurrences
   - isCached calls: 5 occurrences

2. **BeaDataDownloader.java** - ~10 remaining errors
   - isCachedOrExists calls: 2 occurrences (lines 702, 1212)
   - markCached calls: 2 occurrences (lines 716, 1224)
   - isParquetConverted calls: 1 occurrence (line 1117)
   - markParquetConverted calls: 3 occurrences (lines 767, 1183, 1300)
   - isParquetConvertedOrExists calls: 2 occurrences (lines 760, 1279)

### Medium Priority
3. **FredDataDownloader.java** - 2 errors
   - Lambda expressions: 2 occurrences (lines ~152, ~203)

4. **TreasuryDataDownloader.java** - 2 errors
   - Lambda expressions: 2 occurrences (lines ~78, ~134)

5. **WorldBankDataDownloader.java** - 1 error
   - markParquetConverted call: 1 occurrence (line ~249)

### Low Priority (GEO package)
6. **AbstractGeoDataDownloader.java** - 1 error
   - markCached call: 1 occurrence (line ~170)

7. **HudCrosswalkFetcher.java** - 3 errors
   - isParquetConverted calls: 1 occurrence (line ~582)
   - markParquetConverted calls: 2 occurrences (lines ~595, ~619)

8. **CensusApiClient.java** - 3 errors
   - Lambda expression: 1 occurrence (line ~206)
   - isCached call: 1 occurrence (line ~602)
   - markCached call: 1 occurrence (line ~626)

## Import Requirements

All files that create CacheKey instances need:
```java
import org.apache.calcite.adapter.govdata.CacheKey;
import java.util.HashMap;
```

## Next Steps

1. Fix BlsDataDownloader (highest priority - most errors)
2. Fix remaining BeaDataDownloader method calls
3. Fix FredDataDownloader lambdas
4. Fix TreasuryDataDownloader lambdas
5. Fix World/Geo files
6. Test compilation: `./gradlew :govdata:compileJava`
7. Run tests to verify functionality

## Testing Commands

```bash
# Compile only
./gradlew :govdata:compileJava

# Full build
./gradlew :govdata:build

# Run specific test
./gradlew :govdata:test --tests "*BeaDataDownloaderTest*"
```
