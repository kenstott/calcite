# CSV Type Inference Documentation

## Overview

The File Adapter supports automatic type inference for CSV files. Instead of treating all columns as VARCHAR, the system can analyze CSV data and automatically detect appropriate types like INTEGER, DOUBLE, DATE, TIME, TIMESTAMP, and BOOLEAN.

## Features

- **Automatic Type Detection**: Analyzes CSV data to identify column types
- **Sampling Strategy**: Configurable sampling rate to balance accuracy vs performance
- **Safe Defaults**: All inferred types are nullable by default
- **Temporal Type Support**: Detects dates, times, and timestamps including RFC-formatted strings
- **Confidence Threshold**: A minority of non-conforming values doesn't force the whole
  column to VARCHAR — the column is promoted and those values read as null

## Configuration

### Schema-Level Configuration (model.json)

```json
{
  "version": "1.0",
  "defaultSchema": "CSV",
  "schemas": [
    {
      "name": "CSV",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/path/to/csv/files",

        // CSV Type Inference Configuration
        "csvTypeInference": {
          "enabled": true,                    // Enable type inference (default: true)
          "samplingRate": 1.0,                // Fraction of rows to sample (default: 1.0)
          "maxSampleRows": 1000,              // Max rows to sample (default: 1000)
          "confidenceThreshold": 0.95,        // Min conforming fraction to type a column
          "makeAllNullable": true,            // Make all types nullable (default: true)
          "nullableThreshold": 0.01,          // If makeAllNullable=false, null ratio threshold
          "inferDates": true,                 // Infer DATE types (default: true)
          "inferTimes": true,                 // Infer TIME types (default: true)
          "inferTimestamps": true             // Infer TIMESTAMP types (default: true)
        }
      }
    }
  ]
}
```

### Table-Level Configuration

You can override type inference settings for specific tables:

```json
{
  "tables": [
    {
      "name": "SALES",
      "type": "csv",
      "path": "sales.csv",
      "csvTypeInference": {
        "enabled": true,
        "samplingRate": 0.5,     // Sample 50% for this critical table
        "maxSampleRows": 5000     // Sample more rows for better accuracy
      }
    },
    {
      "name": "LARGE_LOG",
      "type": "csv",
      "path": "large_log.csv",
      "csvTypeInference": {
        "enabled": true,
        "samplingRate": 0.01,    // Only sample 1% of this large file
        "maxSampleRows": 10000    // But sample up to 10k rows
      }
    },
    {
      "name": "LEGACY_DATA",
      "type": "csv",
      "path": "legacy.csv",
      "csvTypeInference": {
        "enabled": false          // Disable inference for this table
      }
    }
  ]
}
```

### JDBC URL Configuration

Type inference can also be configured via JDBC URL parameters:

```
jdbc:calcite:model=inline:{
  "schemas": [{
    "name": "CSV",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data",
      "csv_type_inference_enabled": true,
      "csv_type_inference_sampling_rate": 0.2,
      "csv_type_inference_confidence": 0.9
    }
  }]
}
```

## Type Detection Rules

### Numeric Types
- **INTEGER**: Whole numbers that fit in 32-bit range
- **BIGINT**: Whole numbers outside INTEGER range
- **DOUBLE**: Numbers with decimal points or scientific notation

### Boolean Type
- **BOOLEAN**: Matches true/false, TRUE/FALSE, True/False, 0/1

### Temporal Types
- **DATE**: Common date formats (yyyy-MM-dd, MM/dd/yyyy, dd/MM/yyyy, etc.)
- **TIME**: Time formats (HH:mm:ss, h:mm a, etc.)
- **TIMESTAMP**: Date-time combinations
- **TIMESTAMP WITH TIME ZONE**: RFC-formatted timestamps with timezone info

### String Type
- **VARCHAR**: Default fallback for unrecognized patterns

## Confidence Threshold

`confidenceThreshold` (default 0.95) is the fraction of a column's non-null sampled values
that must parse as a recognized type for the column to take that type. A minority below
`1 − threshold` doesn't force the whole column to VARCHAR: the column is promoted, and the
non-conforming values become **null**, each logged at WARN with the column and the value.

So a column of 1000 integers with 5 stray strings infers INTEGER (0.995 ≥ 0.95) and those 5
rows read as null. Raise the threshold toward 1.0 to demand a cleaner column before
promoting; lower it to tolerate messier data.

Nulling rather than raising is a deliberate, documented exception to this codebase's
no-silent-fallbacks rule (see `docs/testing/contradictions.md`, C-08 and the C-16
amendment). The short version: the mismatch is expected by construction — you asked for a
threshold that tolerates a minority — and raising is not usable here, because the Parquet
engine converts the whole file when the table is created, so one bad value would drop the
entire table from the schema rather than failing a single row.

The same null-and-warn applies to a value that contradicts a type **declared** in the header
(`amount:int` with a cell reading `n/a`): the author asserted the type, so the value is bad
data, not a bad type.

## Null Handling

The type inferrer recognizes various null representations:
- Empty strings
- NULL, null, Null
- NA, N/A
- NONE, None
- NIL, nil

By default, all inferred types are nullable for safety. This can be configured:

```json
{
  "csvTypeInference": {
    "makeAllNullable": false,      // Don't force nullable
    "nullableThreshold": 0.05      // Make nullable if >5% nulls
  }
}
```

## Performance Considerations

1. **Sampling Rate**: Defaults to 1.0 (every row, up to `maxSampleRows`). Lower rates are
   faster on very large files but come with a real caveat — see the warning below.
2. **Max Sample Rows**: Limits processing time for large files
3. **Caching**: Inferred types are cached with the table schema
4. **First Query Impact**: Type inference happens on first table access

> **Warning — a fractional `samplingRate` makes inference non-deterministic.** Rows are kept
> via `Math.random()`, so the same file can infer *different* column types on different runs,
> and a file with fewer than roughly `1/samplingRate` rows will often draw zero rows and
> silently fall back to all-VARCHAR. Prefer lowering `maxSampleRows` (deterministic: reads the
> first N rows) over lowering `samplingRate`. Only set a fractional rate when you specifically
> want a random spread across a large file and can accept run-to-run variation in column types.

## Example Use Cases

### Financial Data
```json
{
  "csvTypeInference": {
    "enabled": true,
    "samplingRate": 0.5,          // Higher sampling for accuracy
    "makeAllNullable": false,     // Some columns may be required
    "nullableThreshold": 0.001    // Very low null tolerance
  }
}
```

### Log Files
```json
{
  "csvTypeInference": {
    "enabled": true,
    "samplingRate": 0.001,        // Very low sampling for huge files
    "maxSampleRows": 10000,       // But sample reasonable amount
    "inferTimestamps": true       // Important for log analysis
  }
}
```

### Mixed Format Data
Lower `confidenceThreshold` to tolerate a larger minority of non-conforming values (they
read as null), and raise `maxSampleRows` to see more of the file before deciding.
```json
{
  "csvTypeInference": {
    "enabled": true,
    "confidenceThreshold": 0.8,   // Tolerate up to 20% non-conforming (they read as null)
    "maxSampleRows": 5000,
    "makeAllNullable": true       // Always nullable for safety
  }
}
```

## Troubleshooting

### Types Not Being Inferred
1. Check if type inference is enabled
2. Verify sampling rate is not too low
3. Increase max sample rows so more of the file is seen
4. Lower `confidenceThreshold` if a small minority of messy values is holding the column back
5. Review logs for inference details - look for which sampled values fell through to VARCHAR

### Wrong Types Detected
1. Increase sampling rate for better coverage
2. Increase max sample rows
3. Consider disabling specific type inference (dates, times, etc.)

### Performance Issues
1. Reduce sampling rate
2. Reduce max sample rows
3. Consider disabling for very large files
4. Use table-specific configuration

## Migration from Legacy Behavior

Type inference is **enabled by default** as of this version. A schema with no
`csvTypeInference` block, or one that omits `enabled`, now gets the same settings as
`defaultConfig()`: enabled, 10% sampling (up to 1000 rows), 95% confidence threshold, all
temporal types inferred, all inferred types nullable. Previously the default was disabled
(every CSV column stayed VARCHAR unless a schema explicitly opted in); that guarantee no
longer holds for unconfigured schemas.

To keep the old all-VARCHAR behavior for a schema, set it explicitly:

```json
{
  "csvTypeInference": {
    "enabled": false
  }
}
```

For a schema adopting inference for the first time:

1. Test with a small sampling rate first
2. Adjust configuration based on results
3. Consider table-specific overrides for special cases
4. Clear any existing `.aperio/{schema}/*.parquet` cache so previously-VARCHAR-typed
   columns reconvert with the newly inferred types on the next query

## Programmatic Usage

```java
// Create configuration
CsvTypeInferrer.TypeInferenceConfig config =
    CsvTypeInferrer.TypeInferenceConfig.builder()
        .enabled(true)
        .samplingRate(0.1)
        .maxSampleRows(1000)
        .makeAllNullable(true)
        .build();

// Infer types
List<CsvTypeInferrer.ColumnTypeInfo> types =
    CsvTypeInferrer.inferTypes(source, config, "UNCHANGED");

// Use inferred types
for (ColumnTypeInfo info : types) {
    System.out.println(info.columnName + ": " + info.inferredType +
                      " (nullable=" + info.nullable + ")");
}
```
