# GovData Architecture Overview

## Architectural Pattern: Declarative Data Pipeline

The GovData adapter is a **declarative data pipeline orchestrator** that:
1. **Declares** what government data should be available as SQL tables
2. **Orchestrates** data acquisition and transformation to Parquet format
3. **Delegates** actual query execution to the FileSchema adapter
4. **Enhances** metadata with table comments, foreign keys, and constraints

This separation of concerns allows GovData to focus on data pipeline management while leveraging FileSchema's proven query execution capabilities.

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    SQL Queries                          │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│                   Apache Calcite                        │
│         (Query Planning, Optimization, Execution)       │
└─────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────┐
│              GovData Schema Factory                     │
│  (Declarative Pipeline & Metadata Enhancement Layer)    │
│                                                         │
│  • Declares data requirements via operands              │
│  • Ensures data pipeline is current                     │
│  • Adds table comments and constraints                  │
│  • Manages cross-domain foreign keys                    │
└─────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼────────┐ ┌───────▼────────┐ ┌───────▼────────┐
│  SEC Pipeline  │ │  ECON Pipeline │ │  GEO Pipeline  │
│  (Downloads &  │ │  (Downloads &  │ │  (Downloads &  │
│   Transforms)  │ │   Transforms)  │ │   Transforms)  │
└────────────────┘ └────────────────┘ └────────────────┘
        │                   │                   │
        └───────────────────┴───────────────────┘
                            │
                    Produces Parquet Files
                            │
┌─────────────────────────────────────────────────────────┐
│               FileSchema Adapter                        │
│         (Actual Query Execution Engine)                 │
│                                                         │
│  Delegates to engine based on executionEngine operand:  │
│  • DUCKDB - In-memory analytical database               │
│  • PARQUET - Direct Parquet file reading                │
│  • LINQ4J - Java-based query processing                 │
│  • ARROW - Apache Arrow columnar processing             │
└─────────────────────────────────────────────────────────┘
                            │
                ┌───────────┼───────────┐
                │           │           │
            ┌───▼───┐   ┌───▼───┐   ┌──▼────┐
            │ Local │   │  S3   │   │ HDFS  │
            │Storage│   │Storage│   │Storage│
            └───────┘   └───────┘   └───────┘
```

## Core Components

### 1. GovDataSchemaFactory (Pipeline Orchestrator)
**Purpose**: Declarative data pipeline manager and metadata enhancer

**Primary Responsibilities**:
- **Pipeline Declaration**: Interprets operands to declare data requirements
- **Pipeline Execution**: Ensures data pipeline has run and files are current
- **Metadata Enhancement**: Adds table comments, constraints, and foreign keys
- **Schema Delegation**: Creates FileSchema with enhanced metadata for query execution

**Key Behavior**:
```java
// On each connection, GovData:
1. Reads declarative configuration from operands
2. Triggers data pipeline if needed (download + transform)
3. Enhances metadata with comments and constraints
4. Delegates to FileSchema for actual query execution
```

### 2. Data Pipeline Components

#### SEC Pipeline
- **Declares**: Which companies (CIKs) and filing types needed
- **Downloads**: SEC EDGAR filings and XBRL data
- **Transforms**: Converts XML/XBRL to Parquet tables
- **Enriches**: Adds stock prices from Yahoo Finance

#### ECON Pipeline
- **Declares**: Which economic indicators and time ranges needed
- **Downloads**: Data from BLS, FRED, Treasury, BEA APIs
- **Transforms**: Normalizes time series data to Parquet
- **Manages**: API key authentication and rate limits

#### GEO Pipeline
- **Declares**: Which geographic boundaries and years needed
- **Downloads**: Census TIGER/Line and HUD crosswalk files
- **Transforms**: Converts shapefiles to Parquet with geometries
- **Maps**: Provides state code/FIPS translations

### 3. Data Downloaders

**Common Interface**: `DataDownloader<T>`

```java
public interface DataDownloader<T> {
    void download(DownloadConfig config);
    T parseData(File rawData);
    void convertToParquet(T data, File output);
    boolean needsUpdate(File existing);
}
```

**Implementations**:
- `XbrlDownloader` - SEC EDGAR filings
- `BlsDataDownloader` - Bureau of Labor Statistics
- `FredDataDownloader` - Federal Reserve Economic Data
- `TreasuryDataDownloader` - Treasury Direct
- `BeaDataDownloader` - Bureau of Economic Analysis
- `TigerDataDownloader` - Census boundaries
- `HudCrosswalkDownloader` - HUD ZIP mappings

### 4. Storage Provider Layer

**Purpose**: Abstraction for different storage backends

**Interface**: `StorageProvider`
```java
public interface StorageProvider {
    boolean exists(String path);
    InputStream openInputStream(String path);
    void writeFile(String path, byte[] content);
    void writeFile(String path, InputStream content);
    void createDirectories(String path);
    boolean delete(String path);
    List<String> listFiles(String path);
}
```

**Implementations**:
- `LocalFileStorageProvider` - Local filesystem
- `S3StorageProvider` - Amazon S3
- `HDFSStorageProvider` - Hadoop HDFS

### 5. Table Implementations

**Base Class**: `AbstractTable`

**Key Methods**:
- `getRowType()` - Define schema
- `scan()` - Return data enumerable
- `getStatistic()` - Provide optimizer hints

**Table Types**:
- **Physical Tables**: Direct mapping to Parquet files
- **Virtual Tables**: Computed/aggregated views
- **Partitioned Tables**: Year/CIK/Type partitioning

## Data Flow: The Magic of Declarative Pipelines

### What Makes It Magical
As you noted, the beauty of GovData is that you "explain the data of interest in its native language - get full formed data lake with all metadata including comments and constraints - for zero effort." This works through:

1. **Native Language Declaration**: Specify data needs using familiar terms (company tickers, economic indicators, geographic regions)
2. **Automatic Pipeline Execution**: GovData handles all the complexity of APIs, downloads, transformations
3. **Rich Metadata**: Automatically adds table comments, foreign keys, and constraints
4. **Flexible Storage**: Works with local storage for personal use or shared S3 for team collaboration

### 1. Connection & Declaration
```
User declares needs in model.json:
  "ciks": ["AAPL", "MSFT"]     → Native business language
  "fredIndicators": ["GDP"]     → Familiar economic terms
  "tigerYear": 2024            → Simple year selection
                ↓
GovDataSchemaFactory interprets declarations
                ↓
Triggers appropriate data pipelines
```

### 2. Pipeline Execution (Automatic)
```
For each declared data source:
  → Check if data exists and is current
  → If missing/stale: Download from APIs
  → Transform to Parquet format
  → Store in configured location (local/S3/HDFS)
  → Add metadata enhancements
```

### 3. Schema Delegation to FileSchema
```
GovData creates FileSchema with:
  → Directory pointing to Parquet files
  → Table definitions with comments
  → Foreign key constraints
  → Execution engine choice (DuckDB/Parquet/etc.)
                ↓
FileSchema handles all query execution
```

### 4. Query Execution (Transparent)
```
SQL Query → Calcite → FileSchema → Engine → Results
         Users just write SQL - all complexity hidden
```

## Partitioning Strategy

### SEC Tables
```
/sec-parquet/
  /cik=0000320193/           # Apple
    /filing_type=10-K/
      /year=2023/
        financial_line_items.parquet
        filing_metadata.parquet
    /filing_type=10-Q/
      /year=2023/
```

### ECON Tables
```
/econ-parquet/
  /source=bls/
    /type=employment/
      /year=2023/
        employment_statistics.parquet
  /source=fred/
    /type=indicators/
      /year=2023/
        fred_indicators.parquet
```

### GEO Tables
```
/geo-parquet/
  /source=census/
    /type=boundaries/
      /year=2024/
        tiger_states.parquet
        tiger_counties.parquet
```

## Foreign Key Management

### Cross-Domain FK Flow
```
GovDataSchemaFactory.create()
  → Detect available schemas
  → If SEC + GEO: Add SEC→GEO constraints
  → If ECON + GEO: Add ECON→GEO constraints
  → Apply constraints to table metadata
```

### FK Metadata Structure
```java
Map<String, Object> foreignKey = Map.of(
    "columns", List.of("state_of_incorporation"),
    "targetTable", List.of("geo", "tiger_states"),
    "targetColumns", List.of("state_code")
);
```

## Performance Optimizations

### 1. Partition Pruning
- Filter pushdown to storage layer
- Skip irrelevant partitions
- Reduce I/O significantly

### 2. Columnar Storage (Parquet)
- Column-wise compression
- Predicate pushdown
- Statistics for skip scanning

### 3. Caching Strategy
- LRU cache for frequently accessed data
- Configurable TTL per data source
- Background refresh for stale data

### 4. Parallel Processing
- Concurrent API downloads
- Parallel Parquet conversion
- Multi-threaded query execution

## Error Handling

### Retry Strategy
```java
@Retryable(
    maxAttempts = 3,
    backoff = @Backoff(delay = 1000, multiplier = 2)
)
public void downloadData() {
    // Download logic
}
```

### Circuit Breaker
- Prevents cascading failures
- Fast fail when service unavailable
- Automatic recovery detection

### Graceful Degradation
- Use cached data when API fails
- Partial results for multi-source queries
- Clear error messages to users

## Security Considerations

### API Key Management
- Environment variables for keys
- Never logged or exposed
- Encrypted storage option

### Data Privacy
- No PII in logs
- Configurable data retention
- Audit trail for data access

### Network Security
- HTTPS for all API calls
- Certificate validation
- Proxy support for enterprise

## Extensibility Points

### Adding New Data Sources
1. Implement `DataDownloader` interface
2. Create `SchemaFactory` subclass
3. Register in `GovDataSchemaFactory`
4. Add configuration options

### Custom Table Types
1. Extend `AbstractTable`
2. Implement `getRowType()` and `scan()`
3. Add to schema factory
4. Define foreign keys if applicable

### Storage Providers
1. Implement `StorageProvider` interface
2. Register in `StorageProviderFactory`
3. Add configuration parsing
4. Handle authentication

## Monitoring and Observability

### Metrics
- API call counts and latency
- Cache hit rates
- Query execution times
- Data freshness indicators

### Logging
- Structured logging with context
- Configurable log levels
- Separate logs for downloads

### Health Checks
- API connectivity tests
- Storage accessibility
- Data freshness validation
- Schema consistency checks
