# GovData Design Decisions

## Overview

This document captures key architectural decisions made in the GovData adapter design, including the rationale, trade-offs, and alternatives considered.

## Decision 1: Declarative Pipeline Pattern

### Decision
GovData acts as a declarative data pipeline orchestrator that delegates query execution to FileSchema.

### Rationale
As noted by the project lead: "It's a bit of a strange adapter in that effectively govdata and its operands are a declarative data pipeline, so every connection just makes sure that the pipeline is updated, then it delegates to the file schema adapter."

### Why This Architecture?

1. **Separation of Concerns**
   - GovData focuses on data acquisition and metadata
   - FileSchema handles query execution
   - Each component does what it does best

2. **Metadata Enhancement**
   - GovData adds value through table comments and constraints
   - These enhancements make the data more useful than raw FileSchema access
   - Users get rich documentation without extra effort

3. **Flexibility**
   - Can swap execution engines (DuckDB, Parquet, Arrow) without changing pipeline
   - Storage can be local, S3, or HDFS without code changes
   - Easy to add new data sources without affecting existing ones

### Alternatives Considered

1. **Direct Implementation**: Build query execution directly in GovData
   - ❌ Would duplicate FileSchema's proven functionality
   - ❌ More code to maintain
   - ❌ Less flexibility in execution engines

2. **Pure FileSchema**: Just use FileSchema directly
   - ❌ No automatic data download
   - ❌ No metadata enhancement
   - ❌ No cross-domain foreign keys

3. **Separate Services**: Pipeline as separate service from adapter
   - ❌ More complex deployment
   - ❌ Synchronization challenges
   - ✅ Better separation (but overkill for current needs)

### Trade-offs

**Benefits:**
- Leverages proven FileSchema implementation
- Clear separation of concerns
- Easier testing and maintenance
- Flexible execution options

**Costs:**
- Additional abstraction layer
- Slight performance overhead for delegation
- More complex architecture to understand

## Decision 2: Parquet as Universal Format

### Decision
All government data is converted to Parquet format for storage and querying.

### Rationale
- **Columnar Format**: Optimal for analytical queries
- **Compression**: Reduces storage costs significantly
- **Schema Evolution**: Handles schema changes gracefully
- **Tool Support**: Works with many analytical tools
- **Self-Describing**: Contains schema metadata

### Alternatives Considered

1. **Native Formats**: Keep data in original format (XML, JSON, CSV)
   - ❌ Poor query performance
   - ❌ No consistent schema
   - ❌ Difficult cross-domain joins

2. **Database Storage**: Load into PostgreSQL/MySQL
   - ❌ Requires database infrastructure
   - ❌ Less portable
   - ❌ Higher operational overhead

3. **Multiple Formats**: Support various formats
   - ❌ Complex query planning
   - ❌ Maintenance burden
   - ❌ Inconsistent performance

## Decision 3: Foreign Keys as Metadata Only

### Decision
Foreign key constraints are metadata hints for the optimizer, not enforced at runtime.

### Rationale
Following Calcite's philosophy: "Constraints are purely metadata - and not enforced."

### Benefits
- **Performance**: No runtime FK checking overhead
- **Flexibility**: Can have partial foreign keys (valid for subset)
- **Optimization**: Query planner uses FKs for better plans
- **Documentation**: FKs serve as schema documentation

### Implementation
```java
// FKs added as metadata
Map<String, Object> foreignKey = Map.of(
    "columns", List.of("state_of_incorporation"),
    "targetTable", List.of("geo", "tiger_states"),
    "targetColumns", List.of("state_code")
);
```

## Decision 4: Business-Friendly Configuration

### Decision
Users configure using familiar business terms (company tickers, indicator names) rather than technical identifiers.

### Rationale
"Explain the data of interest in its native language - get full formed data lake with all metadata including comments and constraints - for zero effort."

### Examples
```json
// User writes:
"ciks": ["AAPL", "MSFT"]  // Not "0000320193", "0000789019"

// User writes:
"fredIndicators": ["GDP", "UNEMPLOYMENT"]  // Not series IDs

// User writes:
"tigerYear": 2024  // Not complex version strings
```

### Benefits
- Lower barrier to entry
- Fewer configuration errors
- More intuitive for analysts
- Self-documenting configuration

## Decision 5: Lazy Data Loading

### Decision
Data is downloaded only when needed, not eagerly on connection.

### Rationale
- **Efficiency**: Don't download unused data
- **Speed**: Fast connection establishment
- **Cost**: Minimize API calls and storage
- **Freshness**: Check for updates on access

### Implementation
```java
if (!parquetFileExists() || isStale()) {
    triggerDataPipeline();
}
```

## Decision 6: Multi-Level Storage Abstraction

### Decision
Abstract storage behind a `StorageProvider` interface supporting local, S3, and HDFS.

### Rationale
"You can work with colleagues and all point to a common S3 bucket or you can work independently using your own data storage, including local storage."

### Benefits
- **Collaboration**: Teams share S3/HDFS storage
- **Development**: Local storage for testing
- **Flexibility**: Change storage without code changes
- **Cost**: Choose storage based on needs

## Decision 7: Schema-Specific Factories

### Decision
Each data source (SEC, ECON, GEO) has its own factory, all coordinated by GovDataSchemaFactory.

### Rationale
- **Specialization**: Each factory handles unique data source requirements
- **Modularity**: Can develop/test/deploy independently
- **Extensibility**: Easy to add new data sources
- **Cross-Domain**: Central coordinator manages relationships

### Structure
```
GovDataSchemaFactory (Coordinator)
    ├── SecSchemaFactory (SEC specialist)
    ├── EconSchemaFactory (ECON specialist)
    └── GeoSchemaFactory (GEO specialist)
```

## Decision 8: Table Comments and Documentation

### Decision
Automatically generate comprehensive table and column comments from metadata.

### Rationale
- **Self-Documenting**: Schema explains itself
- **Discovery**: Users can understand tables without external docs
- **Consistency**: Comments always match actual schema
- **Tools**: BI tools can display comments to users

### Example
```sql
-- Users can discover schema through SQL:
SHOW TABLES;
DESCRIBE sec.filing_metadata;
-- Each table/column has helpful comments
```

## Decision 9: Partition Strategy

### Decision
Use hierarchical directory partitioning (year/type/entity).

### Rationale
- **Performance**: Partition pruning for faster queries
- **Organization**: Logical data organization
- **Incremental**: Easy incremental updates
- **Standard**: Follows Parquet/Hive conventions

### Structure
```
/sec-parquet/
  /cik=0000320193/
    /filing_type=10-K/
      /year=2023/
        data.parquet
```

## Decision 10: Error Handling Philosophy

### Decision
Gracefully degrade with cached data rather than fail completely.

### Rationale
- **Availability**: Queries work even if APIs are down
- **User Experience**: Partial results better than errors
- **Resilience**: System continues functioning
- **Transparency**: Warn about stale data

### Implementation
```java
try {
    downloadFreshData();
} catch (ApiException e) {
    log.warn("Using cached data due to: " + e);
    return cachedData();
}
```

## Future Considerations

### Potential Evolution

1. **Real-Time Updates**: WebSocket/streaming for live data
2. **Distributed Processing**: Spark/Flink integration
3. **ML Integration**: Built-in feature engineering
4. **GraphQL API**: Alternative query interface
5. **Data Lineage**: Track data provenance

### Lessons Learned

1. **Delegation Works**: Reusing FileSchema was the right choice
2. **Metadata Matters**: Comments and FKs add significant value
3. **Simple Config**: Business terms reduce friction
4. **Storage Flexibility**: Critical for adoption
5. **Pipeline Pattern**: Declarative approach simplifies usage

## Summary

The GovData architecture reflects a pragmatic balance between:
- **Simplicity** for users vs **Flexibility** for developers
- **Performance** optimization vs **Maintenance** burden
- **Feature richness** vs **Complexity** management
- **Innovation** vs **Proven patterns**

The declarative pipeline pattern with FileSchema delegation has proven to be an elegant solution that provides powerful capabilities while maintaining architectural simplicity.