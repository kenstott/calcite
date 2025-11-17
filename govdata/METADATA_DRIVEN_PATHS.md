# Metadata-Driven Path Resolution

## Overview

The govdata adapter now provides infrastructure for deriving file paths from schema patterns using variable substitution. This eliminates hardcoded path construction and makes the schema file (econ-schema.json) the single source of truth for both table schemas and file paths.

## New Methods in AbstractEconDataDownloader

### 1. `loadTableMetadata(String tableName)`

Loads full table metadata from econ-schema.json including pattern, columns, and partitions.

```java
Map<String, Object> metadata = AbstractEconDataDownloader.loadTableMetadata("fred_indicators");

String pattern = (String) metadata.get("pattern");
// Returns: "type=fred_indicators/year=*/fred_indicators.parquet"

JsonNode columns = (JsonNode) metadata.get("columns");
JsonNode partitions = (JsonNode) metadata.get("partitions");
```

### 2. `resolveJsonPath(String pattern, Map<String, String> variables)`

Derives JSON cache file path by replacing wildcards and changing extension to .json.

```java
String pattern = "type=fred_indicators/year=*/fred_indicators.parquet";
Map<String, String> vars = new HashMap<>();
vars.put("year", "2020");

String jsonPath = AbstractEconDataDownloader.resolveJsonPath(pattern, vars);
// Returns: "type=fred_indicators/year=2020/fred_indicators.json"
```

### 3. `resolveParquetPath(String pattern, Map<String, String> variables)`

Derives Parquet output path by replacing wildcards.

```java
String pattern = "type=fred_indicators/year=*/fred_indicators.parquet";
Map<String, String> vars = new HashMap<>();
vars.put("year", "2020");

String parquetPath = AbstractEconDataDownloader.resolveParquetPath(pattern, vars);
// Returns: "type=fred_indicators/year=2020/fred_indicators.parquet"
```

## Usage Example: fred_indicators

### Current Approach (Hardcoded)

```java
// ❌ Hardcoded path construction
String cachePath = "type=indicators/year=" + year;
String jsonFile = cacheStorageProvider.resolvePath(cachePath, "fred_indicators.json");
String parquetPath = storageProvider.resolvePath(parquetDir,
    "type=indicators/year=" + year + "/fred_indicators.parquet");
```

### New Approach (Metadata-Driven)

```java
// ✅ Schema-driven path construction
Map<String, Object> metadata = loadTableMetadata("fred_indicators");
String pattern = (String) metadata.get("pattern");

Map<String, String> vars = new HashMap<>();
vars.put("year", String.valueOf(year));

String jsonPath = resolveJsonPath(pattern, vars);
// Result: type=fred_indicators/year=2020/fred_indicators.json

String parquetPath = resolveParquetPath(pattern, vars);
// Result: type=fred_indicators/year=2020/fred_indicators.parquet

// Use with storage providers
String fullJsonPath = cacheStorageProvider.resolvePath(cacheDir, jsonPath);
String fullParquetPath = storageProvider.resolvePath(parquetDir, parquetPath);
```

## Multi-Partition Example: employment_statistics

For tables with multiple partition dimensions (frequency + year):

```java
Map<String, Object> metadata = loadTableMetadata("employment_statistics");
String pattern = (String) metadata.get("pattern");
// Pattern: "type=employment_statistics/frequency=*/year=*/employment_statistics.parquet"

Map<String, String> vars = new HashMap<>();
vars.put("frequency", "monthly");
vars.put("year", "2023");

String jsonPath = resolveJsonPath(pattern, vars);
// Returns: "type=employment_statistics/frequency=monthly/year=2023/employment_statistics.json"

String parquetPath = resolveParquetPath(pattern, vars);
// Returns: "type=employment_statistics/frequency=monthly/year=2023/employment_statistics.parquet"
```

## Constant Partition Example: treasury_yields

For tables with constant partition values (no wildcards):

```java
Map<String, Object> metadata = loadTableMetadata("treasury_yields");
String pattern = (String) metadata.get("pattern");
// Pattern: "type=timeseries/frequency=daily/year=*/treasury_yields.parquet"

Map<String, String> vars = new HashMap<>();
vars.put("year", "2022");
// Note: frequency=daily is a constant, no variable needed but caller must provide it
vars.put("frequency", "daily");  // ❌ NOT NEEDED - constant in pattern

String jsonPath = resolveJsonPath(pattern, vars);
// Returns: "type=timeseries/frequency=daily/year=2022/treasury_yields.json"
```

**Important:** Caller only needs to provide variables for wildcards (*). Constants like `frequency=daily` are preserved from the pattern automatically.

## Error Handling

### Missing Variables

```java
String pattern = "type=fred/year=*/frequency=*/fred.parquet";
Map<String, String> vars = new HashMap<>();
vars.put("year", "2020");
// Missing: frequency

try {
    resolveJsonPath(pattern, vars);
} catch (IllegalArgumentException e) {
    // Exception message: "Missing required variables for pattern '...': [frequency]"
}
```

### Non-Existent Table

```java
try {
    loadTableMetadata("non_existent_table");
} catch (IllegalArgumentException e) {
    // Exception message: "Table 'non_existent_table' not found in econ-schema.json"
}
```

## Benefits

1. **Single Source of Truth**: Pattern in econ-schema.json drives both schema and file paths
2. **Eliminates Hardcoding**: No more manual string concatenation for paths
3. **Self-Documenting**: Variable map makes partition values explicit
4. **Validation**: Ensures all required variables are provided before constructing paths
5. **Refactor-Friendly**: Changing partition scheme only requires schema update
6. **Type Safety**: Compile-time errors if variable map is malformed

## Testing

See `MetadataPathResolverTest.java` for comprehensive unit tests covering:
- Single wildcard substitution (year only)
- Multiple wildcard substitution (frequency + year)
- Constant partition handling
- Missing variable error handling
- End-to-end metadata loading and path resolution

Run tests:
```bash
./gradlew :govdata:test --tests "org.apache.calcite.adapter.govdata.econ.MetadataPathResolverTest"
```

## Future Work

This infrastructure enables:
1. **Automated Download/Conversion**: Generic methods that work for any table based on metadata
2. **Dynamic Table Discovery**: Iterate through econ-schema.json to process all tables
3. **Path Validation**: Verify actual file existence matches schema patterns
4. **Schema Migration**: Tools to update patterns without changing code
5. **Cross-Schema Reuse**: Adapt for geo-schema.json, sec-schema.json, etc.
