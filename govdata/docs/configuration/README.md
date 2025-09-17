# GovData Configuration Guide

## Overview

The GovData adapter supports multiple configuration methods to connect to government data sources. This guide covers all configuration options and best practices.

## Configuration Methods

### 1. Model-Based Configuration (Recommended)

Create a JSON model file defining your schemas and data sources:

```json
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [
    {
      "name": "sec",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "sec",
        "ciks": ["AAPL", "MSFT", "GOOGL"],
        "startYear": 2020,
        "endYear": 2024
      }
    }
  ]
}
```

Connect using:
```java
String url = "jdbc:calcite:model=/path/to/model.json";
Connection conn = DriverManager.getConnection(url);
```

### 2. JDBC URL Configuration

For simple use cases, configure directly in the JDBC URL:

```java
// SEC data with specific companies
String url = "jdbc:govdata:dataSource=sec&ciks=AAPL,MSFT&startYear=2020&endYear=2024";

// Economic data with API key
String url = "jdbc:govdata:dataSource=econ&blsApiKey=YOUR_KEY";

// Geographic data with auto-download
String url = "jdbc:govdata:dataSource=geo&autoDownload=true";
```

### 3. Properties Configuration

Pass configuration via Properties object:

```java
Properties props = new Properties();
props.setProperty("dataSource", "sec");
props.setProperty("ciks", "AAPL,MSFT");
props.setProperty("startYear", "2020");
props.setProperty("endYear", "2024");

Connection conn = DriverManager.getConnection("jdbc:govdata:", props);
```

## Schema-Specific Configuration

### SEC Schema Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `dataSource` | String | Required | Must be "sec" |
| `ciks` | String[] | Required | Company CIKs or tickers |
| `startYear` | Integer | Current-5 | Start year for data |
| `endYear` | Integer | Current | End year for data |
| `filingTypes` | String[] | ["10-K","10-Q","8-K"] | Filing types to download |
| `directory` | String | System temp | Cache directory |
| `downloadMissing` | Boolean | true | Auto-download missing data |
| `cacheExpiry` | String | "30d" | Cache expiration period |
| `enableVectorization` | Boolean | false | Enable text embeddings |
| `includeInsiderTransactions` | Boolean | true | Download Forms 3,4,5 |
| `includeStockPrices` | Boolean | true | Download stock prices |

### ECON Schema Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `dataSource` | String | Required | Must be "econ" |
| `blsApiKey` | String | Env: BLS_API_KEY | BLS API key |
| `fredApiKey` | String | Env: FRED_API_KEY | FRED API key |
| `beaApiKey` | String | Env: BEA_API_KEY | BEA API key |
| `enabledSources` | String[] | All | ["bls","fred","treasury","bea","worldbank"] |
| `updateFrequency` | String | "daily" | Update frequency |
| `cacheDirectory` | String | ${GOVDATA_CACHE_DIR} | Cache directory |
| `parquetDirectory` | String | ${GOVDATA_PARQUET_DIR} | Parquet storage |
| `historicalDepth` | String | "5 years" | Historical data depth |

### GEO Schema Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `dataSource` | String | Required | Must be "geo" |
| `autoDownload` | Boolean | true | Auto-download boundary files |
| `cacheDirectory` | String | System temp | Cache directory |
| `tigerYear` | Integer | Current | TIGER/Line year |
| `hudQuarter` | String | Latest | HUD crosswalk quarter |
| `includeDemographics` | Boolean | false | Include demographic data |
| `spatialIndexing` | Boolean | true | Enable spatial indexes |
| `simplificationTolerance` | Double | 0.0001 | Boundary simplification |

## Environment Variables

### Required for ECON Schema
```bash
# API Keys
export BLS_API_KEY=your_bls_api_key
export FRED_API_KEY=your_fred_api_key
export BEA_API_KEY=your_bea_api_key

# Optional: World Bank API (no key required)
export WORLD_BANK_API_URL=https://api.worldbank.org/v2
```

### Global Configuration
```bash
# Data directories
export GOVDATA_CACHE_DIR=/path/to/cache      # Raw data cache
export GOVDATA_PARQUET_DIR=/path/to/parquet  # Parquet storage

# Date ranges (applies to all schemas)
export GOVDATA_START_YEAR=2020
export GOVDATA_END_YEAR=2024

# Schema-specific overrides
export SEC_START_YEAR=2018
export SEC_END_YEAR=2024
export ECON_START_YEAR=2015
export ECON_END_YEAR=2024
```

### Storage Configuration
```bash
# S3 Storage
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export GOVDATA_S3_BUCKET=s3://your-bucket/govdata

# HDFS Storage
export HDFS_NAMENODE=hdfs://namenode:9000
export HDFS_USER=hadoop
```

## Multi-Schema Configuration

### Combined Model Example
```json
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [
    {
      "name": "sec",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "sec",
        "ciks": ["AAPL", "MSFT"],
        "startYear": 2020,
        "endYear": 2024
      }
    },
    {
      "name": "econ",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "econ",
        "enabledSources": ["bls", "fred"],
        "updateFrequency": "daily"
      }
    },
    {
      "name": "geo",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "geo",
        "autoDownload": true,
        "tigerYear": 2024
      }
    }
  ]
}
```

This enables cross-domain queries:
```sql
SELECT 
    s.state_name,
    COUNT(DISTINCT f.cik) as companies,
    AVG(e.unemployment_rate) as unemployment
FROM sec.filing_metadata f
JOIN geo.tiger_states s ON f.state_of_incorporation = s.state_code
JOIN econ.regional_employment e ON e.state_code = s.state_code
GROUP BY s.state_name;
```

## Storage Provider Configuration

### Local Storage (Default)
```json
{
  "operand": {
    "directory": "/path/to/local/cache"
  }
}
```

### S3 Storage
```json
{
  "operand": {
    "directory": "s3://bucket/path",
    "storageConfig": {
      "region": "us-east-1",
      "endpoint": "https://s3.amazonaws.com"
    }
  }
}
```

### HDFS Storage
```json
{
  "operand": {
    "directory": "hdfs://namenode:9000/path",
    "storageConfig": {
      "user": "hadoop",
      "replication": 3
    }
  }
}
```

## Performance Tuning

### Connection Pool Settings
```java
Properties props = new Properties();
props.setProperty("calcite.connection.pool.enabled", "true");
props.setProperty("calcite.connection.pool.maxSize", "10");
props.setProperty("calcite.connection.pool.minSize", "2");
```

### Query Optimization
```java
// Enable partition pruning
props.setProperty("calcite.partition.pruning", "true");

// Set fetch size for large result sets
props.setProperty("calcite.fetch.size", "1000");

// Enable parallel execution
props.setProperty("calcite.parallel.enabled", "true");
props.setProperty("calcite.parallel.threads", "4");
```

### Cache Configuration
```json
{
  "operand": {
    "cacheExpiry": "7d",        // Keep data for 7 days
    "cacheSize": "10GB",        // Max cache size
    "compressionEnabled": true,  // Enable compression
    "partitionPruning": true    // Enable partition pruning
  }
}
```

## Troubleshooting

### Common Issues

1. **Missing API Keys**
   ```
   Error: BLS_API_KEY environment variable not set
   ```
   Solution: Set required environment variables or pass in operand

2. **Connection Timeout**
   ```
   Error: Connection timeout downloading data
   ```
   Solution: Increase timeout settings:
   ```json
   {
     "operand": {
       "connectionTimeout": "60s",
       "readTimeout": "120s"
     }
   }
   ```

3. **Cache Directory Permissions**
   ```
   Error: Cannot write to cache directory
   ```
   Solution: Ensure write permissions or change directory:
   ```bash
   chmod 755 /path/to/cache
   # OR
   export GOVDATA_CACHE_DIR=/tmp/govdata-cache
   ```

### Debug Logging

Enable debug logging to troubleshoot issues:

```java
// In code
Logger.getLogger("org.apache.calcite.adapter.govdata").setLevel(Level.DEBUG);

// Via system property
System.setProperty("calcite.debug", "true");

// Via log4j.properties
log4j.logger.org.apache.calcite.adapter.govdata=DEBUG
```

## Best Practices

1. **Use Environment Variables for Sensitive Data**
   - Store API keys in environment variables
   - Never commit API keys to version control

2. **Configure Appropriate Cache Directories**
   - Use fast local storage for cache
   - Ensure adequate disk space (10GB+ recommended)

3. **Set Reasonable Date Ranges**
   - Limit date ranges to needed data
   - Use partitioning for efficient queries

4. **Enable Only Required Data Sources**
   - Reduce startup time by enabling only needed sources
   - Minimize API calls and storage usage

5. **Monitor API Rate Limits**
   - BLS: 500 requests/day (with key)
   - FRED: 120 requests/minute
   - Configure appropriate delays between requests