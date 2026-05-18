# Aperio JDBC Driver

Query CSV, JSON, Parquet, Excel, Arrow, and 20+ other file formats using standard SQL. Connect to local directories, S3 buckets, or HTTP endpoints from any JDBC client.

## Download

Build the shadow JAR from this repo:

```bash
./gradlew :file:shadowJar
# Output: file/build/libs/calcite-file-*-all.jar
```

## Connect

**JDBC URL format:**
```
jdbc:aperio:<path>[?param=value&param=value...]
```

**Driver class:** `org.apache.calcite.adapter.file.AperioDriver`

### Quick examples

```
# Local directory
jdbc:aperio:/data/csv-files

# S3 bucket
jdbc:aperio:/?storageType=s3&s3Bucket=my-bucket&s3Region=us-east-1

# With DuckDB engine and recursive scanning
jdbc:aperio:/data?executionEngine=duckdb&recursive=true

# Pass a Calcite model file directly
jdbc:aperio:model=/path/to/model.json
```

## Connection parameters

| Parameter | Description | Default | Env variable |
|-----------|-------------|---------|-------------|
| `executionEngine` | `duckdb` or `parquet` | `duckdb` | `APERIO_EXECUTION_ENGINE` |
| `schema` | Schema name visible in SQL | `files` | |
| `recursive` | Scan subdirectories | `false` | |
| `filePattern` | Glob filter (e.g. `*.csv`) | (all) | `APERIO_FILE_PATTERN` |
| `excludePattern` | Glob exclusion pattern | | `APERIO_EXCLUDE_PATTERN` |
| `storageType` | `s3`, `http`, or `https` | (local) | `APERIO_STORAGE_TYPE` |

### S3 parameters

| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `s3Bucket` | Bucket name | `S3_BUCKET` |
| `s3Region` | AWS region | `AWS_REGION` |
| `s3AccessKey` | Access key ID | `AWS_ACCESS_KEY_ID` |
| `s3SecretKey` | Secret access key | `AWS_SECRET_ACCESS_KEY` |

### DuckDB tuning

| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `duckdbMemoryLimit` | Max memory (e.g. `4GB`) | `DUCKDB_MEMORY_LIMIT` |
| `duckdbThreads` | Thread count | `DUCKDB_THREADS` |

### CSV type inference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `csvInferTypes` | Enable type inference | `false` |
| `csvSamplingRate` | Fraction of rows to sample | `0.1` |
| `csvMaxSampleRows` | Max rows to sample | `1000` |
| `csvInferDates` | Detect date columns | `false` |

### Iceberg

| Parameter | Description | Env variable |
|-----------|-------------|-------------|
| `icebergEnabled` | Enable Iceberg catalog | `APERIO_ICEBERG_ENABLED` |
| `icebergCatalogType` | Catalog type (e.g. `rest`, `hive`) | `ICEBERG_CATALOG_TYPE` |
| `icebergWarehouse` | Warehouse path or URI | `ICEBERG_WAREHOUSE` |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:aperio:/path/to/your/data`
3. **Driver JAR:** add `calcite-file-*-all.jar`
4. **Driver class:** `org.apache.calcite.adapter.file.AperioDriver`

## Sample queries

```sql
-- List all files discovered as tables
SELECT table_name FROM information_schema.tables WHERE table_schema = 'files';

-- Query a CSV file (filename becomes table name)
SELECT * FROM files."sales_2024.csv" LIMIT 10;

-- Aggregate across parquet files in a directory
SELECT region, SUM(revenue) AS total
FROM files."quarterly_results.parquet"
GROUP BY region
ORDER BY total DESC;

-- Join a JSON file with a CSV file
SELECT o.order_id, c.name, o.amount
FROM files."orders.json" o
JOIN files."customers.csv" c ON o.customer_id = c.id;
```

## Supported file formats

| Format | Extension | Notes |
|--------|-----------|-------|
| CSV | `.csv` | Type inference optional |
| JSON | `.json`, `.jsonl` | Nested objects flattened |
| Parquet | `.parquet` | Native DuckDB support |
| Excel | `.xlsx`, `.xls` | First sheet by default |
| Arrow | `.arrow`, `.feather` | |
| ORC | `.orc` | |
| Avro | `.avro` | |
| TSV | `.tsv` | Tab-separated |
