# Claude Development Guidelines - Troubleshooting & Debug Workflows

## 🚨 EMERGENCY DEBUGGING PROTOCOL

### When Completely Stuck
1. **STOP** - Don't create more files or tests
2. **READ** the full error message and stack trace  
3. **TRACE** execution path with strategic debug output
4. **ISOLATE** the minimal failing case
5. **DOCUMENT** findings before attempting fixes

## 🔍 SYSTEMATIC DEBUGGING WORKFLOWS

### Workflow 1: Test Failure Investigation
```bash
# Step 1: Get clean error output
./gradlew :module:test -PincludeTags=integration --tests "*FailingTest*" --console=plain

# Step 2: Check prerequisites
echo "Environment variables:"
env | grep -E "(API_KEY|CACHE_DIR|ENGINE_TYPE)"

# Step 3: Verify test data
ls -la /path/to/test/data/
duckdb -c "SELECT COUNT(*) FROM read_parquet('/path/to/test.parquet')"

# Step 4: Add strategic debug output
# See "Debug Output Patterns" section below
```

### Workflow 2: Adapter Connection Issues  
```java
// Step 1: Trace schema creation
System.err.println("DEBUG: Creating schema with operand: " + operand);

// Step 2: Trace table discovery
System.err.println("DEBUG: Found tables: " + tableNames);

// Step 3: Trace SQL generation
System.err.println("DEBUG: Generated SQL: " + sql);

// Step 4: Trace data retrieval
System.err.println("DEBUG: Result set metadata: " + resultSet.getMetaData());
```

### Workflow 3: Calcite Plan Analysis
```bash
# Enable Calcite debug logging
export CALCITE_DEBUG=true

# Get execution plan output
./gradlew :module:test --tests "*FailingTest*" --debug

# Look for RelNode tree in output
grep -A 20 "RelNode" build/test-results/
```

## 🎯 COMMON ERROR PATTERNS & SOLUTIONS

### Pattern: "0 tests executed"
```
ROOT CAUSE: Test has @Tag("integration") but command missing -PincludeTags=integration

DIAGNOSTIC:
./gradlew :module:test --tests "*TestName*"
> Task :module:test
> BUILD SUCCESSFUL
> 0 tests executed

SOLUTION:
./gradlew :module:test -PincludeTags=integration --tests "*TestName*"
```

### Pattern: DuckDB "catalog not found"
```
ROOT CAUSE: Table/schema name doesn't match DuckDB expectations

DIAGNOSTIC COMMANDS:
duckdb -c "SHOW SCHEMAS"
duckdb -c "SHOW TABLES FROM schema_name"  
duckdb -c "DESCRIBE table_name"

INVESTIGATION STEPS:
1. Check model JSON table definitions
2. Verify parquet file directory structure
3. Test direct parquet file access
4. Confirm case sensitivity (DuckDB uses lowercase)
```

### Pattern: Integration test timeout
```
ROOT CAUSE: Network/API dependency or large dataset processing

DIAGNOSTIC:
timeout 30 ./gradlew :module:test -PincludeTags=integration --tests "*TestName*"

SOLUTIONS:
1. Increase timeout: timeout 300 ...
2. Check API rate limits
3. Verify network connectivity  
4. Use smaller test datasets
```

### Pattern: Parquet schema mismatch
```
ROOT CAUSE: Parquet file schema doesn't match table expectations

DIAGNOSTIC COMMANDS:
duckdb -c "DESCRIBE SELECT * FROM read_parquet('/path/to/file.parquet')"
duckdb -c "SELECT * FROM read_parquet('/path/to/file.parquet') LIMIT 3"

INVESTIGATION:
1. Compare expected vs actual column names/types
2. Check for partition columns in schema
3. Verify data conversion process
4. Test with minimal parquet file
```

## 🔧 DEBUG OUTPUT PATTERNS

### Strategic System.err Placement
```java
// At method entry points
System.err.println("DEBUG: " + getClass().getSimpleName() + "." + methodName + " called");

// Before SQL execution  
System.err.println("DEBUG: Executing SQL: " + sql);

// After data retrieval
System.err.println("DEBUG: Retrieved " + rowCount + " rows");

// At error boundaries
System.err.println("DEBUG: Exception in " + methodName + ": " + e.getMessage());
```

### Stack Trace Generation
```java
// For complex execution flow analysis
Thread.dumpStack();

// For specific method call chains
new Exception("DEBUG: Stack trace at checkpoint").printStackTrace();

// For understanding call context
System.err.println("DEBUG: Called from: " + Thread.currentThread().getStackTrace()[2]);
```

### Data Structure Inspection
```java
// For ResultSet analysis
ResultSetMetaData meta = rs.getMetaData();
for (int i = 1; i <= meta.getColumnCount(); i++) {
    System.err.printf("Column %d: %s (%s)%n", 
        i, meta.getColumnName(i), meta.getColumnTypeName(i));
}

// For Map/operand debugging
operand.forEach((k, v) -> System.err.println("DEBUG: " + k + " = " + v));

// For List/collection debugging  
System.err.println("DEBUG: Collection size=" + list.size() + ", contents=" + list);
```

## 🎯 ERROR-SPECIFIC DECISION TREES

### DuckDB Issues Decision Tree
```
DuckDB error?
├─ "catalog does not exist"?
│  ├─ Check table name casing (DuckDB uses lowercase)
│  ├─ Verify model JSON configuration
│  └─ Test with: duckdb -c "SHOW SCHEMAS"
├─ "table does not exist"?  
│  ├─ Check parquet file exists at expected path
│  ├─ Test with: duckdb -c "SELECT * FROM read_parquet('/path')"
│  └─ Verify directory structure matches partition pattern
├─ "column does not exist"?
│  ├─ Check parquet schema: DESCRIBE SELECT * FROM read_parquet(...)
│  ├─ Verify column name casing
│  └─ Check for partition columns in schema
└─ Connection issues?
    ├─ Check file permissions
    ├─ Verify DuckDB library version compatibility
    └─ Test with minimal example
```

### Test Execution Issues Decision Tree
```
Test not running as expected?
├─ "0 tests executed"?
│  ├─ Check @Tag annotation on test
│  ├─ Add -PincludeTags=[tag] to command
│  └─ Verify test name pattern matching
├─ Test found but skipped?
│  ├─ Check for @Disabled annotation
│  ├─ Look for assumeTrue() conditions
│  └─ Verify environment variable requirements
├─ Test runs but fails immediately?
│  ├─ Check test setup/prerequisites
│  ├─ Verify required data files exist
│  └─ Check for missing dependencies
└─ Test hangs/times out?
    ├─ Add timeout to gradle command
    ├─ Check for infinite loops in test logic
    └─ Verify network connectivity for integration tests
```

## 🧹 CLEANUP PROTOCOLS

### Debug Code Cleanup Checklist
```bash
# 1. Find debug output
grep -r "System.out\|System.err" src/

# 2. Find temp/debug tests
find . -name "*Test*.java" -exec grep -l "@Tag(\"temp\)\|@Tag(\"debug\")" {} \;

# 3. Find dead code
# Manual review - look for unused methods, unreachable code

# 4. Find temp files
find . -name "*.md" -path "./build/*" -o -name "Test*.java" -path "./"
```

### Before Commit Verification
```bash
# 1. Build succeeds
./gradlew build

# 2. Tests pass  
./gradlew :module:test -PincludeTags=unit
./gradlew :module:test -PincludeTags=integration

# 3. No debug artifacts
grep -r "System.out\|System.err\|TODO.*DEBUG\|FIXME" src/ && echo "Debug artifacts found!"

# 4. No temp files in wrong locations
find . -maxdepth 1 -name "*.java" -o -name "*.class" && echo "Files in wrong location!"
```

## 🚫 ANTI-PATTERNS TO AVOID

### Debugging Anti-Patterns
```java
// ❌ Guessing without evidence
// "Maybe it's a connection issue" → Add debug output to verify

// ❌ Surface-level fixes
// Changing assertion without understanding why it failed

// ❌ Creating workarounds instead of fixes
// Adding try-catch to hide exceptions instead of addressing root cause

// ❌ Abandoning difficult debugging  
// "This is too complex" → Break down systematically

// ❌ Not cleaning up debug output
// Leaving System.err.println() statements in committed code
```

### Problem Resolution Anti-Patterns
```bash
# ❌ Moving to new approach without resolving current issue
# "Let me try a different adapter" instead of fixing current one

# ❌ Creating duplicate tests instead of fixing failing ones
# TestFoo2.java because TestFoo.java is hard to debug

# ❌ Commenting out failing assertions
# // assertEquals(expected, actual); // TODO: fix later

# ❌ Claiming completion without verification
# "This should work now" without running the test
```

## 🎓 LEARNING FROM FAILURES

### Failure Analysis Template
```
FAILURE: [Brief description]
ROOT CAUSE: [Technical cause traced through debugging]
INVESTIGATION METHOD: [How the root cause was discovered]
SOLUTION: [What fixed it]
PREVENTION: [How to avoid this pattern in future]
VERIFICATION: [Command/test that proves fix works]
```

### Pattern Recognition
- **Recurring Issues**: Document patterns that appear multiple times
- **Environmental Dependencies**: Note tests that require specific setup
- **Timing Issues**: Identify tests sensitive to timeouts/race conditions  
- **Data Dependencies**: Track tests that need specific data formats/content