# Plan: Add Trino Execution Engine to FileSchema

## Overview
Add a `TRINO` execution engine to FileSchema as an alternative to DuckDB and Parquet, using the same JDBC wrapping pattern as DuckDB. Trino will enable interactive SQL analytics on parquet files with 5-15 second query latency for dashboard use cases.

## Architecture Pattern
- **JDBC Wrapping**: Use Calcite's JdbcSchema to delegate execution to Trino
- **Catalog Management**: Active mode - FileSchema discovers parquet files and creates external tables in Trino
- **Remote Connection**: Unlike DuckDB (in-process), Trino runs as a separate server
- **Configuration**: Support both standalone Trino and managed services (AWS Athena)

---

## Phase 1: Core Engine Configuration

### 1.1 Add TRINO to ExecutionEngineType Enum
**File**: `file/src/main/java/org/apache/calcite/adapter/file/execution/ExecutionEngineConfig.java`

**Changes**:
- Add `TRINO` enum value to `ExecutionEngineType` (line 196, after DUCKDB)
- Add to `getAvailableEngineTypes()` array (line 158)
- Update documentation comments explaining Trino's use case

**Trino Description**:
```java
/**
 * Trino-based distributed SQL engine with remote execution.
 * Best for: Interactive analytics, cloud-native architectures, federated queries.
 * Queries parquet files on S3/disk with 5-15 second latency.
 * Requires Trino server and JDBC driver dependency.
 */
TRINO
```

### 1.2 Create TrinoConfig Class
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/execution/trino/TrinoConfig.java`

**Pattern**: Mirror `DuckDBConfig.java`

**Configuration Properties**:
```java
- jdbcUrl: String (e.g., "jdbc:trino://localhost:8080/catalog")
- jdbcDriver: String (default: "io.trino.jdbc.TrinoDriver")
- jdbcUser: String (default: current user)
- jdbcPassword: String (optional)
- catalogName: String (default: "hive")
- schemaName: String (Trino schema to use)
- threads: int (query parallelism)
- memoryPerNode: String (e.g., "4GB")
- s3Config: Map<String, Object> (AWS credentials, endpoint)
```

### 1.3 Create TrinoExecutionEngine Utility Class
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/execution/trino/TrinoExecutionEngine.java`

**Pattern**: Mirror `DuckDBExecutionEngine.java`

**Methods**:
```java
- isAvailable(): Check for "io.trino.jdbc.TrinoDriver" on classpath
- getEngineType(): Return "TRINO"
```

---

## Phase 2: JDBC Schema Integration

### 2.1 Create TrinoJdbcSchemaFactory
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/trino/TrinoJdbcSchemaFactory.java`

**Pattern**: Adapt from `DuckDBJdbcSchemaFactory.java` with key differences:

**Key Methods**:
1. `create(SchemaPlus parentSchema, String schemaName, String directoryPath, boolean recursive, FileSchema fileSchema, Map<String, Object> operand)`:
   - Establish JDBC connection to remote Trino server
   - Configure Trino session properties
   - Create Trino schema if needed
   - Register parquet files as external tables

2. `registerFilesAsExternalTables(Connection conn, String directoryPath, boolean recursive, String trinoSchema, String calciteSchemaName, FileSchema fileSchema)`:
   - Iterate through FileSchema's conversion registry
   - Create external tables using Trino DDL

3. `createExternalTable(Connection conn, String schemaName, String tableName, String parquetPath)`:
   - Execute Trino-specific external table DDL
   - Support both single files and partitioned datasets

**Trino DDL Syntax** (differs from DuckDB):
```sql
-- Single file
CREATE TABLE IF NOT EXISTS hive.schema_name.table_name (
  -- Columns inferred or explicit
) WITH (
  format = 'PARQUET',
  external_location = 's3://bucket/path/to/file.parquet'
)

-- Partitioned dataset
CREATE TABLE IF NOT EXISTS hive.schema_name.table_name (
  -- Columns
) WITH (
  format = 'PARQUET',
  external_location = 's3://bucket/path/to/base/',
  partitioned_by = ARRAY['year', 'month']
)
```

### 2.2 Create TrinoJdbcSchema Class
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/trino/TrinoJdbcSchema.java`

**Pattern**: Extend Calcite's `JdbcSchema`

**Purpose**: Custom schema class to maintain Trino connection and handle refresh operations

**Key Methods**:
- Constructor: Store FileSchema reference for metadata access
- `refresh()`: Re-register tables after FileSchema refresh
- `getConnection()`: Provide connection to Trino server

### 2.3 Create TrinoConvention
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/trino/TrinoConvention.java`

**Pattern**: Mirror `DuckDBConvention.java`

**Purpose**: Define Calcite relational convention for Trino query planning

---

## Phase 3: SQL Dialect Support

### 3.1 Verify/Enhance TrinoSqlDialect
**File**: `core/src/main/java/org/apache/calcite/sql/dialect/TrinoSqlDialect.java`

**Check**: Verify existing Trino dialect in Calcite core

**Enhancements Needed**:
- External table DDL support
- Parquet-specific functions
- S3 path handling
- Partition spec generation

### 3.2 Create TrinoFunctionMapping (if needed)
**New File**: `file/src/main/java/org/apache/calcite/adapter/file/trino/TrinoFunctionMapping.java`

**Pattern**: Similar to `DuckDBFunctionMapping.java`

**Purpose**: Map Calcite functions to Trino equivalents

---

## Phase 4: Configuration and Wiring

### 4.1 Update ExecutionEngineConfig Constructor
**File**: `file/src/main/java/org/apache/calcite/adapter/file/execution/ExecutionEngineConfig.java`

**Changes**:
- Add `TrinoConfig trinoConfig` parameter (similar to duckdbConfig)
- Store trinoConfig field
- Provide getter method

### 4.2 Update FileSchemaFactory
**File**: `file/src/main/java/org/apache/calcite/adapter/file/FileSchemaFactory.java`

**Changes**:
- Detect `executionEngine = "TRINO"` in operand
- Parse Trino configuration from operand
- Create TrinoJdbcSchema instead of FileSchema tables
- Wire FileSchema to TrinoJdbcSchema for metadata

**Example Configuration** (model.json):
```json
{
  "schemas": [{
    "name": "analytics",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data/parquet",
      "executionEngine": "TRINO",
      "trinoConfig": {
        "jdbcUrl": "jdbc:trino://localhost:8080/hive",
        "jdbcUser": "admin",
        "catalogName": "hive",
        "schemaName": "default",
        "s3Config": {
          "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
          "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
          "awsRegion": "us-east-1"
        }
      }
    }
  }]
}
```

---

## Phase 5: Testing

### 5.1 Unit Tests
**New File**: `file/src/test/java/org/apache/calcite/adapter/file/trino/TrinoExecutionEngineTest.java`

**Tests**:
- Driver availability detection
- Configuration parsing
- External table DDL generation
- S3 path handling

### 5.2 Integration Tests
**New File**: `file/src/test/java/org/apache/calcite/adapter/file/trino/TrinoIntegrationTest.java`

**Requirements**:
- Trino server running (Docker container for CI)
- Test external table creation
- Test query execution
- Test partitioned datasets
- Test S3 integration (with MinIO)

**Test Pattern**:
```java
@Test
@Tag("integration")
void testTrinoQueryExecution() throws SQLException {
  // Setup: Create FileSchema with TRINO engine
  // Exercise: Execute SQL query
  // Verify: Results match expected
  // Verify: Query delegated to Trino (check logs)
}
```

### 5.3 Cross-Engine Comparison Tests
**New File**: `file/src/test/java/org/apache/calcite/adapter/file/TrinoVsDuckDBTest.java`

**Purpose**: Verify Trino and DuckDB produce same results for same queries

---

## Phase 6: Documentation

### 6.1 Code Documentation
- Javadoc for all public classes and methods
- Inline comments explaining Trino-specific logic
- Document differences from DuckDB pattern

### 6.2 User Documentation
**New File**: `file/docs/trino-engine.md`

**Contents**:
- Installation requirements (Trino server, JDBC driver)
- Configuration examples
- Performance characteristics vs other engines
- When to use Trino (interactive analytics, cloud environments)
- AWS Athena integration guide
- Troubleshooting guide

### 6.3 Update README
**File**: `file/README.md` or main README

**Changes**:
- Add TRINO to supported execution engines
- Link to Trino engine documentation
- Performance comparison matrix

---

## Phase 7: Build and Dependencies

### 7.1 Update build.gradle
**File**: `file/build.gradle`

**Changes**:
```gradle
dependencies {
  // Trino JDBC driver (optional, compile-time only)
  compileOnly 'io.trino:trino-jdbc:428'  // Latest version

  // Test dependencies
  testImplementation 'io.trino:trino-jdbc:428'
  testImplementation 'org.testcontainers:trino:1.19.1'  // For integration tests
}
```

### 7.2 Update Gradle Testcontainers Configuration
**Purpose**: Support Trino container for integration tests

---

## Implementation Decisions

### Catalog Management: Active Mode
**Decision**: FileSchema creates external tables in Trino (not passive)

**Rationale**:
- Consistent with DuckDB pattern
- FileSchema knows parquet file locations
- Enables automatic schema discovery
- Simplifies user configuration

### Remote vs Embedded
**Decision**: Support remote Trino server only (not embedded)

**Rationale**:
- Trino is designed as distributed server
- No embedded mode exists
- Simpler architecture
- Matches production use case

### AWS Athena Support
**Decision**: Support Athena via JDBC URL configuration

**Rationale**:
- Athena is Presto/Trino under the hood
- Uses Trino JDBC driver
- Just different JDBC URL pattern
- No additional code needed

**Athena Example**:
```json
"trinoConfig": {
  "jdbcUrl": "jdbc:trino://athena.us-east-1.amazonaws.com:443/AwsDataCatalog",
  "jdbcUser": "AKIA...",
  "jdbcPassword": "...",
  "catalogName": "AwsDataCatalog",
  "schemaName": "default"
}
```

---

## Success Criteria

### Functional
- [ ] Trino engine type recognized in configuration
- [ ] JDBC connection established to Trino server
- [ ] External tables created for all parquet files
- [ ] Queries execute on Trino and return correct results
- [ ] S3 paths work (both direct S3 and local files)
- [ ] Partitioned datasets handled correctly
- [ ] Hive-style partitioning supported

### Performance
- [ ] Query latency 5-15 seconds for typical analytical queries
- [ ] No significant overhead vs direct Trino queries
- [ ] Proper pushdown of filters, projections, aggregations

### Quality
- [ ] All unit tests pass
- [ ] Integration tests pass with Trino container
- [ ] No regression in existing engine tests
- [ ] Code follows FileSchema conventions
- [ ] Comprehensive documentation

---

## Estimated Effort
- **Phase 1-2**: Core implementation (8-10 hours)
- **Phase 3-4**: Integration and wiring (4-6 hours)
- **Phase 5**: Testing (6-8 hours)
- **Phase 6-7**: Documentation and polish (2-3 hours)

**Total**: 20-27 hours of development time

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Trino DDL differences | Research Trino external table syntax thoroughly before implementation |
| Remote connection overhead | Document expected latency, provide connection pooling configuration |
| S3 authentication complexity | Test with multiple S3 providers (AWS, MinIO, localstack) |
| Partition handling differences | Study Trino's partition discovery vs DuckDB's approach |
| Integration test complexity | Use Testcontainers for consistent Trino environment |

---

## Next Steps After Approval

1. Set up local Trino development environment (Docker)
2. Create feature branch: `feature/trino-execution-engine`
3. Implement Phase 1 (core configuration)
4. Implement Phase 2 (JDBC integration) - most critical
5. Add basic integration test to validate approach
6. Continue through remaining phases
7. Create pull request with comprehensive testing evidence

---

## Appendix: Key Architectural Insights from Discussion

### Why Trino vs Other Options?

**Trino is more appropriate than:**
- **Hive Thrift Server**: Trino is more modern, faster (5-15s vs minutes), and better for interactive use
- **ClickHouse**: Trino queries in-place without data duplication; ClickHouse requires loading data

**Use Trino when:**
- Need interactive analytics (5-15 second queries)
- Have cloud/S3 storage
- Want to query parquet in-place without data movement
- Need federated queries across multiple sources

**Don't use Trino when:**
- Need sub-second latency (use ClickHouse with ingestion)
- Need high concurrency (1000s of users)
- Running batch ETL jobs (use Hive or Spark)

### Active vs Passive Catalog Management

**Active (chosen approach)**:
- FileSchema discovers parquet files
- FileSchema creates external tables in Trino
- DDL is engine-specific (not generic JDBC)
- Consistent with DuckDB pattern

**Passive (not chosen)**:
- Tables already exist in Trino
- Created by admins or other processes
- FileSchema just connects
- Would be truly generic JDBC

**Why Active**: Consistent with FileSchema's value proposition of automatic discovery and configuration.
