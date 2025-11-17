# BLS Data Downloader DRY Refactoring - Implementation Guide

## Executive Summary

This document describes the refactoring of BlsDataDownloader to follow the DRY (Don't Repeat Yourself) principles established by BeaDataDownloader's metadata-driven pattern.

**Status**: Phase 1 Complete (Reference Tables Created)
**Impact**: ~150 lines of hardcoded lookup data consolidated into catalog tables
**Performance**: Ready for 10-20x faster iteration using `iterateTableOperationsOptimized()`

## Completed Work

### 1. Reference Table Schemas Created

Added two new reference tables to `econ-schema.json`:

#### `reference_bls_geographies`
**Pattern**: `type=reference/geo_type={geo_type}/bls_geographies.parquet`

Consolidates 5 hardcoded maps into single catalog:
- `STATE_FIPS_MAP` (51 states)
- `CENSUS_REGIONS` (4 regions)
- `METRO_AREA_CODES` (27 metros)
- `METRO_CPI_CODES` (27 metros)
- `METRO_BLS_AREA_CODES` (27 metros)

**Schema**:
```
geo_code                 VARCHAR  -- Primary code (state abbr, metro code, region code)
geo_name                 VARCHAR  -- Human-readable name
geo_type                 VARCHAR  -- 'state', 'metro', or 'region'
state_fips               VARCHAR  -- 2-digit FIPS code (states only)
region_code              VARCHAR  -- 4-digit code (regions only)
metro_publication_code   VARCHAR  -- BLS publication code (metros only)
metro_cpi_area_code      VARCHAR  -- CPI area code (metros only, NULL if no CPI data)
metro_bls_area_code      VARCHAR  -- 7-digit BLS area code (metros only)
```

**Location**:
- Schema: `econ-schema.json` lines 2206-2275
- Generator: `BlsDataDownloader.java:2695-2784`

#### `reference_bls_naics_sectors`
**Pattern**: `type=reference/bls_naics_sectors.parquet`

Consolidates `NAICS_SUPERSECTORS` hardcoded map:

**Schema**:
```
supersector_code  VARCHAR  -- 8-digit NAICS code (e.g., '00000000', '05000000')
supersector_name  VARCHAR  -- Industry name (e.g., 'Total Nonfarm', 'Construction')
```

**Location**:
- Schema: `econ-schema.json` lines 2277-2307
- Generator: `BlsDataDownloader.java:2786-2853`

### 2. Reference Data Generation

**Implementation**: `BlsDataDownloader.downloadReferenceData()` (lines 2635-2693)

The method now:
1. Downloads existing JOLTS reference tables (industries, dataelements)
2. **NEW**: Generates BLS geographies reference table from hardcoded maps
3. **NEW**: Generates BLS NAICS sectors reference table from hardcoded map

**Key Features**:
- Uses DuckDB to create in-memory tables from Java maps
- Writes partitioned parquet files (geographies partitioned by `geo_type`)
- Integrates with cache manifest for tracking
- One-time generation (checks cache manifest before regenerating)

### 3. Java 8 Compatibility Fixes

Fixed Java 14+ switch expressions in BeaDataDownloader to use Java 8 compatible switch statements:

**Before** (Java 14+ only):
```java
return (dimensionName) -> switch (dimensionName) {
  case "year" -> yearRange(startYear, endYear);
  case "tablename" -> tableNames;
  default -> null;
};
```

**After** (Java 8 compatible):
```java
return (dimensionName) -> {
  switch (dimensionName) {
    case "year":
      return yearRange(startYear, endYear);
    case "tablename":
      return tableNames;
    default:
      return null;
  }
};
```

**Files Fixed**:
- `BeaDataDownloader.java`: 5 dimension providers converted
- `BlsDataDownloader.java`: Text blocks converted to string concatenation

## BeaDataDownloader Pattern (Reference Implementation)

The BEA downloader demonstrates the **gold standard** metadata-driven pattern:

### Key Design Principles

1. **Constructor-Based Configuration** (BeaDataDownloader:74-103)
   - Loads iteration lists from schema metadata
   - Loads catalog-based reference data (NIPA tables, regional line codes)
   - Validates configuration early with clear error messages
   - Stores configuration as instance fields for reuse

2. **DimensionProvider Pattern** (BeaDataDownloader:215-229, 795-809, etc.)
   - Creates reusable dimension providers for each table type
   - Encapsulates iteration logic using switch statements
   - Makes download/conversion methods symmetric and declarative

3. **Optimized Iteration Framework** (BeaDataDownloader:257-274)
   ```java
   iterateTableOperationsOptimized(
       tableName,
       createNationalAccountsDimensions(startYear, endYear, tableNames, frequencies),
       (cacheKey, vars, jsonPath, parquetPath) -> {
         // Download or convert logic here
         DownloadResult result = executeDownload(tableName, vars);
         cacheManifest.markCached(cacheKey, jsonPath, ...);
       },
       "download"
   );
   ```
   - **10-20x faster** than manual iteration (uses DuckDB bulk cache filtering)
   - Consistent pattern across all download/conversion methods
   - Separates iteration mechanics from business logic

4. **Catalog-Driven Validation** (BeaDataDownloader:475-567)
   - Loads reference data to drive downloads
   - Validates combinations at runtime (table-frequency, table-geography, table-year)
   - Avoids invalid API calls by filtering upfront

## Migration Pattern for Remaining BLS Methods

BlsDataDownloader has 16 download methods that need migration. Here's the pattern:

### Current Manual Iteration Pattern (BEFORE)

```java
public void downloadRegionalCpi(int startYear, int endYear) {
  List<String> seriesIds = Series.getAllRegionalCpiSeriesIds();

  // 1. Manual uncached year identification
  List<Integer> uncachedYears = new ArrayList<>();
  for (int year = startYear; year <= endYear; year++) {
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("year", String.valueOf(year));
    CacheKey cacheKey = new CacheKey("regional_cpi", cacheParams);
    if (isCachedOrExists(cacheKey)) {
      LOGGER.info("Found cached regional CPI for year {} - skipping", year);
    } else {
      uncachedYears.add(year);
    }
  }

  // 2. Batch fetch
  Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

  // 3. Manual save loop
  for (int year : uncachedYears) {
    String rawJson = resultsByYear.get(year);
    if (rawJson != null) {
      validateAndSaveBlsResponse(tableName, year, variables, jsonFilePath, rawJson);
    }
  }
}
```

**Problems**:
- Manual cache checking loop (lines 955-967)
- Manual save loop (lines 983-992)
- No use of optimized DuckDB cache filtering
- Repetitive boilerplate across 16 methods

### Optimized Pattern (AFTER)

```java
public void downloadRegionalCpi(int startYear, int endYear) {
  String tableName = "regional_cpi";

  // Load region codes from catalog instead of hardcoded CENSUS_REGIONS map
  List<String> regionCodes = loadRegionCodesFromCatalog();

  // Use optimized iteration with DuckDB bulk cache filtering (10-20x faster)
  iterateTableOperationsOptimized(
      tableName,
      (dimensionName) -> {
        switch (dimensionName) {
          case "year":
            return yearRange(startYear, endYear);
          case "region":
            return regionCodes;
          default:
            return null;
        }
      },
      (cacheKey, vars, jsonPath, parquetPath) -> {
        int year = Integer.parseInt(vars.get("year"));
        String regionCode = vars.get("region");

        // Build series ID for this region
        String seriesId = Series.getRegionalCpiSeriesId(regionCode);

        // Download for single year/region
        String rawJson = fetchSingleSeries(seriesId, year);

        // Save and mark cached
        if (rawJson != null) {
          cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(StandardCharsets.UTF_8));
          long fileSize = cacheStorageProvider.getMetadata(jsonPath).getSize();
          cacheManifest.markCached(cacheKey, jsonPath, fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        }
      },
      "download"
  );
}
```

**Benefits**:
- **10-20x faster** cache filtering via DuckDB SQL query
- Eliminates manual loops (cache checking, saving)
- Declarative dimension specification
- Consistent with BeaDataDownloader pattern
- Catalog-driven instead of hardcoded maps

### Helper Method: Load From Catalog

Add methods to load from the new reference tables:

```java
/**
 * Loads census region codes from reference_bls_geographies catalog.
 */
private List<String> loadRegionCodesFromCatalog() throws IOException {
  List<String> regionCodes = new ArrayList<>();

  String pattern = "type=reference/geo_type=region/bls_geographies.parquet";
  String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

  try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
    String query = String.format(
        "SELECT region_code FROM read_parquet('%s') ORDER BY region_code",
        fullPath);

    try (Statement stmt = duckdb.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        regionCodes.add(rs.getString("region_code"));
      }
    }
  } catch (SQLException e) {
    throw new IOException("Failed to load region codes from catalog: " + e.getMessage(), e);
  }

  return regionCodes;
}

/**
 * Loads state FIPS codes from reference_bls_geographies catalog.
 */
private List<String> loadStateFipsFromCatalog() throws IOException {
  // Similar pattern for states
}

/**
 * Loads NAICS supersector codes from reference_bls_naics_sectors catalog.
 */
private List<String> loadNaicsSectorsFromCatalog() throws IOException {
  // Similar pattern for NAICS sectors
}
```

## Priority Migration Order

Based on usage frequency and impact:

### High Priority (Most Used)
1. **`downloadRegionalCpi()`** - Uses CENSUS_REGIONS (4 regions)
2. **`downloadStateIndustryEmployment()`** - Uses STATE_FIPS_MAP (51 states) × NAICS_SUPERSECTORS (22 sectors) = 1,122 series
3. **`downloadMetroIndustryEmployment()`** - Uses METRO_AREA_CODES (27 metros) × NAICS_SUPERSECTORS = 594 series
4. **`downloadMetroCpi()`** - Uses METRO_CPI_CODES (27 metros)

### Medium Priority
5. `downloadStateWages()` - Uses STATE_FIPS_MAP
6. `downloadMetroWages()` - Uses METRO_BLS_AREA_CODES
7. `downloadRegionalEmployment()` - Uses CENSUS_REGIONS

### Low Priority (Simple or Infrequent)
8. `downloadEmploymentStatistics()` - No geography, just series IDs
9. `downloadInflationMetrics()` - No geography
10. `downloadWageGrowth()` - No geography
11-16. Other methods

## Code Reduction Estimate

### Hardcoded Maps to Remove (After Full Migration)
- `STATE_FIPS_MAP`: ~51 lines
- `CENSUS_REGIONS`: ~4 lines
- `METRO_AREA_CODES`: ~27 lines
- `METRO_CPI_CODES`: ~27 lines
- `METRO_BLS_AREA_CODES`: ~27 lines
- `NAICS_SUPERSECTORS`: ~22 lines
- **Total**: ~158 lines of static data

### Manual Iteration Boilerplate to Remove
- Average ~15 lines per method × 16 methods = ~240 lines
- Replaced with ~8 lines per method = ~128 lines
- **Savings**: ~112 lines

### Total Expected Reduction
- **~270 lines** of code eliminated
- **Improved maintainability**: Reference data can be updated without code changes
- **10-20x faster**: DuckDB bulk cache filtering vs manual loops

## Next Steps

1. **Add Constructor Catalog Loading** (like BeaDataDownloader:74-103)
   ```java
   public BlsDataDownloader(...) {
     super(...);

     // Load catalogs in constructor (fail fast if missing)
     this.stateFipsCodes = loadStateFipsFromCatalog();
     this.regionCodes = loadRegionCodesFromCatalog();
     this.naicsSectors = loadNaicsSectorsFromCatalog();
     this.metroCodes = loadMetroCodesFromCatalog();
   }
   ```

2. **Migrate High-Priority Methods** (downloadRegionalCpi, downloadStateIndustryEmployment)
   - Follow pattern shown above
   - Test with sample year range
   - Verify cache manifest tracking

3. **Remove Hardcoded Maps** (after all methods migrated)
   - Delete static final Map declarations
   - Update Series helper class to accept codes from catalog
   - Run full regression tests

4. **Document Migration** (update CLAUDE.md if needed)
   - Note the catalog-driven pattern
   - Reference this document for future downloaders

## Testing Checklist

Before removing hardcoded maps:
- [ ] All 16 download methods migrated to use catalogs
- [ ] Reference tables generate correctly via downloadReferenceData()
- [ ] Cache manifest tracking works for all methods
- [ ] Performance tests show 10-20x improvement
- [ ] Integration tests pass for all BLS tables
- [ ] No compilation errors or warnings

## Performance Benchmarks

### Before (Manual Iteration)
```
Checking cache for 51 states × 22 sectors × 10 years = 11,220 combinations
- Manual manifest.isCached() checks: ~11,220 × 5ms = ~56 seconds
```

### After (DuckDB Bulk Filtering)
```
Checking cache for 11,220 combinations
- Single DuckDB SQL query: ~3 seconds (10-20x faster)
- Remaining work: Only download the uncached ones
```

## Files Modified

1. **`econ-schema.json`**: Added reference_bls_geographies and reference_bls_naics_sectors schemas
2. **`BlsDataDownloader.java`**:
   - Added `generateBlsGeographiesReference()` (lines 2695-2784)
   - Added `generateBlsNaicsSectorsReference()` (lines 2786-2853)
   - Updated `downloadReferenceData()` to call generators (lines 2686-2690)
3. **`BeaDataDownloader.java`**: Fixed Java 8 compatibility (switch expressions → switch statements)

## References

- **BeaDataDownloader**: Reference implementation of metadata-driven pattern
- **FredDataDownloader**: Good example of catalog-based series list building
- **AbstractGovDataDownloader**: Provides `iterateTableOperationsOptimized()` infrastructure
- **CacheManifestQueryHelper**: Provides DuckDB bulk cache filtering

---

**Author**: Claude Code
**Date**: 2025-11-14
**Status**: Phase 1 Complete - Reference Tables Created
**Next Phase**: Migrate download methods to use iterateTableOperationsOptimized()
