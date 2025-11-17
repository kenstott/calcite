# Phase 4: Metadata-Driven Schema Refactoring Pattern

## Overview

This document describes the established pattern for eliminating hardcoded schema definitions across all economic data downloaders. The pattern has been implemented and verified in FredDataDownloader.java.

## The Simplified Pattern

### Before (Hardcoded - ~50 lines per method):
```java
private void writeXxxParquet(List<Map<String, Object>> data, String targetPath) throws IOException {
  // 1. Build schema manually with hardcoded types and comments
  Schema schema = SchemaBuilder.record("RecordName")
      .namespace("org.apache.calcite.adapter.govdata.econ")
      .fields()
      .name("field1").doc("Hardcoded comment 1").type().stringType().noDefault()
      .name("field2").doc("Hardcoded comment 2").type().doubleType().noDefault()
      .name("field3").doc("Hardcoded comment 3").type().nullable().stringType().noDefault()
      // ... 10-20 more fields ...
      .endRecord();

  // 2. Convert data to GenericRecords
  List<GenericRecord> records = new ArrayList<>();
  for (Map<String, Object> item : data) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("field1", item.get("field1"));
    record.put("field2", item.get("field2"));
    record.put("field3", item.get("field3"));
    // ... 10-20 more puts ...
    records.add(record);
  }

  // 3. Write to Parquet
  storageProvider.writeAvroParquet(targetPath, schema, records, "RecordName");
}
```

### After (Metadata-Driven - ~5 lines per method):
```java
private void writeXxxParquet(List<Map<String, Object>> data, String targetPath) throws IOException {
  // 1. Load column metadata from econ-schema.json
  List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
      loadTableColumns("table_name");  // Table name from JSON

  // 2. Write to Parquet - StorageProvider handles schema building and GenericRecord conversion
  storageProvider.writeAvroParquet(targetPath, columns, data, "RecordType", "RecordName");
}
```

## Benefits

1. **Code Reduction**: ~90% reduction in code (50 lines → 5 lines per method)
2. **Single Source of Truth**: All schema metadata in `econ-schema.json`
3. **Maintainability**: Schema changes only require JSON updates
4. **Consistency**: Same pattern across all downloaders
5. **No Manual Schema Building**: StorageProvider handles all Avro details

## Infrastructure Components (Already Implemented)

### 1. TableColumn Class
Location: `PartitionedTableConfig.java` lines 177-205

```java
public static class TableColumn {
  private final String name;
  private final String type;       // "string", "int", "long", "double", "boolean"
  private final boolean nullable;
  private final String comment;

  // getters...
}
```

### 2. StorageProvider.writeAvroParquet() Overload
Location: `StorageProvider.java` lines 209-303

```java
default void writeAvroParquet(String path,
                             List<TableColumn> columns,
                             List<Map<String, Object>> dataRecords,
                             String recordType,
                             String recordName) throws IOException
```

This method:
- Builds Avro schema from TableColumn metadata
- Converts Map data to GenericRecords
- Writes to Parquet file
- All Avro/Parquet details handled internally

### 3. AbstractEconDataDownloader.loadTableColumns()
Location: `AbstractEconDataDownloader.java` lines 501-557

```java
protected static List<TableColumn> loadTableColumns(String tableName)
```

This method:
- Loads `econ-schema.json` from resources
- Finds the specified table
- Extracts and returns column metadata

## Refactoring Checklist

### Files to Refactor

- [x] **FredDataDownloader.java** - ✅ COMPLETED
  - `writeFredIndicatorsParquet()` - 1 method, 6 .doc() calls

- [ ] **BlsDataDownloader.java** - 19 methods, 129 .doc() calls
  - `writeEmploymentStatisticsParquet()` - table: employment_statistics
  - `writeInflationMetricsParquet()` - table: inflation_metrics
  - `writeWageGrowthParquet()` - table: wage_growth
  - `writeRegionalEmploymentParquet()` - table: regional_employment
  - `writeRegionalCpiParquet()` - table: regional_cpi
  - `writeMetroCpiParquet()` - table: metro_cpi
  - `writeStateIndustryParquet()` - table: state_industry
  - `writeStateWagesParquet()` - table: state_wages
  - `writeStateWagesQcewParquet()` - table: state_wages
  - `writeMetroIndustryParquet()` - table: metro_industry
  - `writeMetroWagesParquet()` - table: metro_wages
  - `writeMetroWagesQcewParquet()` - table: metro_wages
  - `writeJoltsRegionalParquet()` - table: jolts_regional
  - `writeCountyWagesParquet()` - table: county_wages
  - `writeJoltsStateParquet()` - table: jolts_state
  - `writeJoltsIndustriesParquet()` - table: reference_jolts_industries
  - `writeJoltsDataelementsParquet()` - table: reference_jolts_dataelements
  - `convertAndSaveRegionalEmployment()` - table: regional_employment
  - `parseAndConvertQcewToParquet()` - table: county_qcew
  - Method at line ~5392 - table: regional_cpi

- [ ] **BeaDataDownloader.java** - ~10 methods, 90 .doc() calls
  - Methods to be identified via grep

- [ ] **FredCatalogDownloader.java** - ~3 methods, 19 .doc() calls
  - Methods to be identified via grep

- [ ] **WorldBankDataDownloader.java** - ~3 methods, 15 .doc() calls
  - Methods to be identified via grep

- [ ] **TreasuryDataDownloader.java** - ~3 methods, 11 .doc() calls
  - Methods to be identified via grep

## Table Name Mappings

Schema record names → JSON table names (for loadTableColumns parameter):

| Record Name | JSON Table Name |
|------------|----------------|
| employment_statistics | employment_statistics |
| inflation_metrics | inflation_metrics |
| wage_growth | wage_growth |
| regional_employment | regional_employment |
| regional_cpi | regional_cpi |
| metro_cpi | metro_cpi |
| state_industry | state_industry |
| state_wages | state_wages |
| metro_industry | metro_industry |
| metro_wages | metro_wages |
| jolts_regional | jolts_regional |
| county_wages | county_wages |
| jolts_state | jolts_state |
| **jolts_industries** | **reference_jolts_industries** ⚠️ |
| **jolts_dataelements** | **reference_jolts_dataelements** ⚠️ |
| CountyQcew | county_qcew |
| ConsumerPriceIndex | regional_cpi |

⚠️ Note: Two tables have different names in code vs JSON

## Step-by-Step Refactoring Process

For each `write*Parquet()` method:

### 1. Identify the method
Find methods with SchemaBuilder.record() calls:
```bash
grep -n "SchemaBuilder.record" FileName.java
```

### 2. Determine table name
- Look at the record name in SchemaBuilder.record("RecordName")
- Map to JSON table name using table above
- Special cases: jolts_industries → reference_jolts_industries, jolts_dataelements → reference_jolts_dataelements

### 3. Replace the method body
```java
// OLD: Delete everything from SchemaBuilder to GenericRecord creation
Schema schema = SchemaBuilder.record(...)...endRecord();
List<GenericRecord> records = ...;
for (...) { record.put(...); }

// NEW: Replace with 2-line pattern
List<TableColumn> columns = loadTableColumns("table_name_from_json");
storageProvider.writeAvroParquet(targetPath, columns, existingDataList, "RecordType", "RecordName");
```

### 4. Verify data format
Ensure the data is already in `List<Map<String, Object>>` format. If not, minimal conversion may be needed.

### 5. Compile and test
```bash
./gradlew :govdata:compileJava
```

## Example: FredDataDownloader (Completed)

### Before:
```java
@SuppressWarnings("deprecation")
private void writeFredIndicatorsParquet(List<Map<String, Object>> observations, String targetPath)
    throws IOException {
  Schema schema = SchemaBuilder.record("FredIndicator")
      .namespace("org.apache.calcite.adapter.govdata.econ")
      .fields()
      .name("series_id").doc("FRED series identifier").type().stringType().noDefault()
      .name("series_name").doc("Descriptive name of the economic data series").type().stringType().noDefault()
      .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
      .name("value").doc("Observed value for this date").type().doubleType().noDefault()
      .name("units").doc("Units of measurement (e.g., 'Percent', 'Billions of Dollars')").type().stringType().noDefault()
      .name("frequency").doc("Data frequency (e.g., 'Daily', 'Monthly', 'Quarterly', 'Annual')").type().stringType().noDefault()
      .endRecord();

  List<GenericRecord> records = new ArrayList<>();
  for (Map<String, Object> obs : observations) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("series_id", obs.get("series_id"));
    record.put("series_name", obs.get("series_name"));
    record.put("date", obs.get("date"));
    record.put("value", obs.get("value"));
    record.put("units", obs.get("units"));
    record.put("frequency", obs.get("frequency"));
    records.add(record);
  }

  storageProvider.writeAvroParquet(targetPath, schema, records, "FredIndicator");
}
```

### After:
```java
@SuppressWarnings("deprecation")
private void writeFredIndicatorsParquet(List<Map<String, Object>> observations, String targetPath)
    throws IOException {
  // Load column metadata from econ-schema.json
  java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
      loadTableColumns("fred_indicators");

  // Write parquet using StorageProvider with metadata-driven schema
  // The StorageProvider handles all Avro schema building and GenericRecord conversion
  storageProvider.writeAvroParquet(targetPath, columns, observations, "FredIndicator", "FredIndicator");
}
```

**Result**: 30 lines → 7 lines (including comments), BUILD SUCCESSFUL

## Verification Status

- ✅ Infrastructure complete (StorageProvider, TableColumn, loadTableColumns)
- ✅ JSON schemas transformed (all 3 files: econ, geo, sec)
- ✅ Pattern verified with FredDataDownloader
- ✅ Compilation verified: BUILD SUCCESSFUL

## Next Steps

1. Apply pattern to BlsDataDownloader (19 methods) - MECHANICAL
2. Apply pattern to BeaDataDownloader (~10 methods) - MECHANICAL
3. Apply pattern to FredCatalogDownloader (~3 methods) - MECHANICAL
4. Apply pattern to WorldBankDataDownloader (~3 methods) - MECHANICAL
5. Apply pattern to TreasuryDataDownloader (~3 methods) - MECHANICAL
6. Proceed to Phase 5: Create AbstractGeoDataDownloader and AbstractSecDataDownloader
7. Proceed to Phase 6: Testing and verification

## Summary

The pattern is established, verified, and dramatically simplifies schema management. All remaining refactoring is mechanical application of the 5-line pattern to ~38 remaining methods across 5 files.

**Total Impact**:
- Code reduction: ~271 .doc() calls eliminated
- ~1,500+ lines of hardcoded schema code → ~190 lines of metadata-driven code
- ~87% code reduction in schema management
- Single source of truth: JSON schema files
