# Bulk Download Pattern

## Overview

The `bulkDownload` operand in econ-schema.json enables a "download once, convert many" pattern where a single large source file feeds multiple parquet tables.

## Example: QCEW (Quarterly Census of Employment and Wages)

### Problem
QCEW data is published as a single ~80MB ZIP file containing CSV data for all geographic levels:
- State-level wages (state_wages table)
- County-level wages (county_wages table)
- County-level detailed QCEW (county_qcew table)
- Metro-level wages (metro_wages table)

Previously, each table independently specified the same download URL, leading to:
- Redundant download attempts (4x for the same file)
- Path inconsistencies (`type=qcew/` vs `type=qcew_bulk/`)
- No machine-readable dependency information

### Solution: bulkDownload Operand

#### 1. Define Bulk Download in Schema

```json
{
  "bulkDownloads": {
    "qcew_annual_bulk": {
      "cachePattern": "type=qcew_bulk/year={year}/frequency={frequency}/qcew.zip",
      "url": "https://data.bls.gov/cew/data/files/{year}/csv/{year}_{frequency}_singlefile.zip",
      "variables": ["year", "frequency"],
      "comment": "QCEW bulk CSV download shared by multiple tables"
    }
  }
}
```

#### 2. Tables Reference the Bulk Download

```json
{
  "name": "state_wages",
  "download": {
    "bulkDownload": "qcew_annual_bulk",
    "csvConversion": {
      "csvPath": "{year}_{frequency}_singlefile.csv",
      "filterConditions": [
        "agglvl_code = '50'",
        "own_code = '0'",
        "industry_code = '10'"
      ]
    }
  }
}
```

#### 3. Tables Without Download Operand

Tables that don't need downloading (e.g., derived views) simply omit the `download` operand:

```json
{
  "name": "state_employment_view",
  "type": "view",
  "sql": "SELECT state_fips, total_employment FROM state_wages"
}
```

## Benefits

1. **Single Source of Truth**: Download URL and cache path defined once
2. **Explicit Dependencies**: Machine-readable relationship between bulk file and tables
3. **Download Optimization**: Check cache once for bulk file, not per-table
4. **Path Consistency**: All dependent tables use same cache pattern
5. **Clearer Semantics**:
   - Tables WITH `download.bulkDownload` → depends on bulk file download
   - Tables WITH `download.url` → independent download
   - Tables WITHOUT `download` → no download needed (views, derived tables)

## Implementation Status

### ✅ Completed (Phase 1)
- [x] Added `bulkDownloads` section to econ-schema.json
- [x] Updated all 4 QCEW tables to reference `qcew_annual_bulk`
- [x] Created `BulkDownloadConfig` class
- [x] Added `loadBulkDownloads()` method to EconSchemaFactory
- [x] Fixed path inconsistencies (all use `type=qcew_bulk/`)

### 🔄 Future Work (Phase 2)
- [ ] Update BlsDataDownloader to read bulkDownload config from schema
- [ ] Implement bulk download orchestration in EconSchemaFactory:
  - Group tables by bulkDownload reference
  - Download each bulk file once before converting dependent tables
  - Skip download check for tables without `download` operand
- [ ] Add cache checking optimization:
  - Check bulk file timestamp, not individual table timestamps
  - Mark all dependent tables as "convertible" when bulk file exists
- [ ] Add bulk download progress tracking in CacheManifest

## Usage Pattern

### For Downloader Implementers

When implementing a new bulk download source:

1. **Define the bulk download** in the schema's `bulkDownloads` section
2. **Reference it** in table `download` operands
3. **Implement conversion** logic that reads from the bulk file

```java
// Load bulk download configuration
Map<String, BulkDownloadConfig> bulkDownloads = loadBulkDownloads();
BulkDownloadConfig qcewBulk = bulkDownloads.get("qcew_annual_bulk");

// Resolve paths for a given year
Map<String, String> variables = new HashMap<>();
variables.put("year", "2023");
variables.put("frequency", "annual");

String cachePath = qcewBulk.resolveCachePath(variables);
String downloadUrl = qcewBulk.resolveUrl(variables);

// Check if bulk file is cached
if (!cacheManifest.isBulkFileCached(cachePath)) {
  downloadFile(downloadUrl, cachePath);
  cacheManifest.markBulkFileCached(cachePath);
}

// Convert from bulk file to individual table parquets
for (Table table : dependentTables) {
  convertToParquet(cachePath, table);
}
```

### For Schema Designers

Use `bulkDownload` when:
- ✅ Multiple tables share the same large source file
- ✅ Source file contains superset of data needed by individual tables
- ✅ Tables use different filters/transformations on the same source

Use direct `download.url` when:
- ✅ Table has its own unique API endpoint
- ✅ No other tables share the source data

Omit `download` entirely when:
- ✅ Table is a view (SQL-only definition)
- ✅ Table is derived from other tables
- ✅ Table doesn't require external data download

## Related Files

- **Schema**: `govdata/src/main/resources/econ-schema.json`
- **Config Class**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BulkDownloadConfig.java`
- **Factory**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/EconSchemaFactory.java`
- **QCEW Downloader**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BlsDataDownloader.java`

## See Also

- `METADATA_DRIVEN_PATHS.md` - General metadata-driven path resolution
- `PHASE4-REFACTORING-PATTERN.md` - Downloader refactoring patterns
