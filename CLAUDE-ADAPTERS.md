# Claude Development Guidelines - Adapter-Specific Knowledge

## 🗂️ FILE ADAPTER

### Engine Architecture
```
DuckDB Engine = Special case of Parquet Engine
   ↓
Converts everything to Parquet internally
   ↓
Creates DuckDB catalog over Parquet files
   ↓
Always uses 1 catalog: "memory"
```

### Common DuckDB Error Patterns
```
Error: "catalog 'X' does not exist"
→ DuckDB tried to find 'X' as: table → schema → catalog
→ Exhausted all lookups in "memory" catalog
→ Usually means table/schema name is wrong

Error: "table 'Y' does not exist"
→ Check table configuration in model JSON
→ Verify parquet files exist in expected directory structure
```

### Engine-Specific Test Commands
```bash
# Test all file engines systematically
CALCITE_FILE_ENGINE_TYPE=PARQUET gtimeout 1800 ./gradlew :file:test --continue --console=plain
CALCITE_FILE_ENGINE_TYPE=DUCKDB gtimeout 1800 ./gradlew :file:test --continue --console=plain
CALCITE_FILE_ENGINE_TYPE=LINQ4J gtimeout 1800 ./gradlew :file:test --continue --console=plain
CALCITE_FILE_ENGINE_TYPE=ARROW gtimeout 1800 ./gradlew :file:test --continue --console=plain
```

### File Adapter Debugging
```bash
# Check what DuckDB sees
duckdb -c "DESCRIBE SELECT * FROM read_parquet('/path/to/file.parquet')"

# Verify parquet file structure
duckdb -c "SELECT COUNT(*) FROM read_parquet('/path/to/file.parquet')"

# Test partitioned table access
duckdb -c "SELECT * FROM read_parquet('/path/cik=*/filing_type=*/year=*/*.parquet') LIMIT 5"
```

## 🏢 GOVDATA ADAPTER

### Required Environment Variables
```bash
# Economic data sources
FRED_API_KEY=your_fred_key                    # Federal Reserve Economic Data
BLS_API_KEY=your_bls_key                      # Bureau of Labor Statistics
BEA_API_KEY=your_bea_key                      # Bureau of Economic Analysis

# Geographic data (HUD requires credentials)
HUD_USERNAME=your_hud_username
HUD_PASSWORD=your_hud_password

# Cache directories
GOVDATA_CACHE_DIR=/path/to/cache              # Raw data storage
GOVDATA_PARQUET_DIR=/path/to/parquet          # Converted parquet files
```

### Schema Structure
```
govdata/
├── source=econ/
│   ├── type=indicators/year=2024/
│   ├── type=timeseries/year=2024/
│   └── type=regional/year=2024/
├── source=geo/
│   └── type=boundary/year=2024/
└── source=sec/
    └── cik=*/filing_type=*/year=*/
```

### Test Commands by Data Source
```bash
# Test economic data integration
BLS_API_KEY=xxx FRED_API_KEY=yyy BEA_API_KEY=zzz \
./gradlew :govdata:test -PincludeTags=integration --tests "*EconDataValidationTest*"

# Test geographic data
GOVDATA_CACHE_DIR=/tmp/test-cache \
./gradlew :govdata:test -PincludeTags=integration --tests "*GeoDataValidationTest*"

# Test SEC data integration
./gradlew :govdata:test -PincludeTags=integration --tests "*SecSchemaValidationTest*"
```

### 🔍 MANDATORY DEBUG LOGGING REQUIREMENTS

All govdata adapter methods **MUST** implement comprehensive debug logging using CalciteTrace infrastructure.

#### Required Tracer Setup
```java
private static final Logger GOVDATA_TRACER =
    LoggerFactory.getLogger("org.apache.calcite.adapter.govdata." + ClassName.class.getSimpleName());
```

#### Four Required Debug Statements Per Method

**1. Entry Logging**
```java
public ReturnType methodName(ParamType param) {
    if (GOVDATA_TRACER.isDebugEnabled()) {
        GOVDATA_TRACER.debug("Entering {} with params: {}", "methodName", param);
    }
    // method implementation
}
```

**2. Success Exit Logging**
```java
    ReturnType result = computeResult();
    if (GOVDATA_TRACER.isDebugEnabled()) {
        GOVDATA_TRACER.debug("Successfully completed {}, returning: {}", "methodName", result);
    }
    return result;
```

**3. Error Logging**
```java
    try {
        // risky operation
    } catch (Exception e) {
        GOVDATA_TRACER.debug("Error in {}: {}", "methodName", e.getMessage(), e);
        throw e; // or handle appropriately
    }
```

**4. Fail Exit Logging** (for validation failures that don't throw exceptions)
```java
    if (!isValid(input)) {
        if (GOVDATA_TRACER.isDebugEnabled()) {
            GOVDATA_TRACER.debug("{} failed validation, returning: {}", "methodName", "invalid input");
        }
        return null; // or appropriate failure response
    }
```

#### Performance Guidelines
- **ALWAYS** use `isDebugEnabled()` guards for expensive string operations
- Use `{}` placeholders for parameter substitution (SLF4J pattern)
- Keep debug messages concise but informative
- Include relevant context (method name, key parameters, results)

#### Integration with CalciteTrace
- Follow existing CalciteTrace patterns for logger naming
- Use consistent log levels: DEBUG for method tracing, ERROR for actual problems
- Leverage CalciteTrace's component-specific tracers where applicable

## 📊 SPLUNK ADAPTER

### Query Pushdown Limitations
```java
// ✅ Can push down simple field references
SELECT field1, field2 FROM splunk_table

// ❌ Cannot push down complex expressions - Calcite handles these
SELECT CAST(field1 AS INTEGER), field1 + field2 FROM splunk_table
```

### RexNode Type Checking (Critical)
```java
// ❌ WRONG - Never assume all projections are RexInputRef
RexInputRef inputRef = (RexInputRef) project;

// ✅ CORRECT - Always check type first
if (project instanceof RexInputRef) {
    RexInputRef inputRef = (RexInputRef) project;
    // Handle simple field reference
} else {
    // Let Calcite handle complex expressions
}
```

### Test Environment Setup
```bash
# Enable Splunk integration tests
CALCITE_TEST_SPLUNK=true gtimeout 1800 ./gradlew :splunk:test --continue --console=plain
```

## 📋 SHAREPOINT ADAPTER

### Integration Test Configuration
```bash
# Enable SharePoint integration tests
SHAREPOINT_INTEGRATION_TESTS=true gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain

# Specific SharePoint test examples
SHAREPOINT_INTEGRATION_TESTS=true ./gradlew :sharepoint-list:test \
  --tests "*.SharePointListIntegrationTest.testListDiscovery"

SHAREPOINT_INTEGRATION_TESTS=true ./gradlew :sharepoint-list:test \
  --tests "*.SharePointSQL2003ComplianceTest.testBasicSelect"
```

### Common SharePoint Issues
- List discovery requires proper permissions
- SQL 2003 compliance testing needs specific SharePoint setup
- Direct connection tests may require network access

## 🎯 ADAPTER-SPECIFIC DECISION TREES

### File Adapter Issues
```
File adapter test failing?
├─ Check CALCITE_FILE_ENGINE_TYPE environment variable
├─ DuckDB "catalog not found"?
│  ├─ Verify table name in model JSON matches expected
│  ├─ Check parquet file directory structure
│  └─ Confirm file permissions and accessibility
├─ Parquet engine issues?
│  ├─ Check parquet file format validity
│  └─ Verify Arrow/Parquet library compatibility
└─ LINQ4J issues?
    └─ Usually indicates Java classpath or reflection problems
```

### Govdata Adapter Issues
```
Govdata test failing?
├─ Missing API keys?
│  ├─ Check required environment variables above
│  └─ Verify API key validity and rate limits
├─ Cache directory issues?
│  ├─ Check GOVDATA_CACHE_DIR permissions
│  ├─ Verify GOVDATA_PARQUET_DIR writeable
│  └─ Look for disk space issues
└─ Data conversion problems?
    ├─ Check source data format changes
    ├─ Verify parquet conversion logic
    └─ Test individual data downloader components
```

## 🔧 CROSS-ADAPTER PATTERNS

### JDBC Adapter Reference
- Follow JDBC adapter patterns as the reference implementation
- Consistent metadata handling across adapters
- Standard connection property patterns
- Common error handling approaches

### Schema Factory Patterns
```java
// Standard schema factory pattern
public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    // 1. Extract configuration from operand map
    // 2. Validate required parameters
    // 3. Create and return schema instance
    // 4. Handle errors gracefully with descriptive messages
}
```

### Testing Patterns Across Adapters
```bash
# Standard test timeout for integration tests
timeout 300 ./gradlew :adapter:test -PincludeTags=integration

# Standard console output for debugging
--console=plain

# Standard continuation on failure
--continue
```
