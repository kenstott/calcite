# Claude Development Guidelines - Testing

## 🚨 CRITICAL TEST EXECUTION RULES

### Tag-Based Test System
```kotlin
// In build.gradle.kts - this is WHY tests don't run by default
useJUnitPlatform {
    includeTags("unit")  // Only unit tests by default
}
```

### The #1 Testing Mistake (DO NOT MAKE)
```bash
# ❌ WRONG - Will find 0 tests if @Tag("integration")
./gradlew :module:test --tests "*IntegrationTest*"

# ✅ CORRECT - Check tag first, then use proper command
./gradlew :module:test -PincludeTags=integration --tests "*IntegrationTest*"
```

## 📋 COMMAND REFERENCE BY SCENARIO

### Common Test Execution Patterns
```bash
# Default behavior (unit tests only)
./gradlew :govdata:test

# Integration tests for specific module  
./gradlew :govdata:test -PincludeTags=integration

# Specific integration test
./gradlew :govdata:test -PincludeTags=integration --tests "*AppleMicrosoftTest*"

# All tests (override default filtering)
./gradlew :govdata:test -PrunAllTests

# Multiple tag types
./gradlew :govdata:test -PincludeTags=unit,integration
```

### Adapter-Specific Test Commands
```bash
# File adapter - test all engines
CALCITE_FILE_ENGINE_TYPE=DUCKDB ./gradlew :file:test -PincludeTags=integration
CALCITE_FILE_ENGINE_TYPE=PARQUET ./gradlew :file:test -PincludeTags=integration  
CALCITE_FILE_ENGINE_TYPE=ARROW ./gradlew :file:test -PincludeTags=integration
CALCITE_FILE_ENGINE_TYPE=LINQ4J ./gradlew :file:test -PincludeTags=integration

# Govdata adapter - with API keys
BLS_API_KEY=xxx FRED_API_KEY=yyy ./gradlew :govdata:test -PincludeTags=integration

# SharePoint adapter
SHAREPOINT_INTEGRATION_TESTS=true ./gradlew :sharepoint-list:test -PincludeTags=integration

# Splunk adapter  
CALCITE_TEST_SPLUNK=true ./gradlew :splunk:test -PincludeTags=integration
```

## 🔍 TEST DEBUGGING WORKFLOW

### Step 1: Understand the Failure
```bash
# Get full error output with context
./gradlew :module:test -PincludeTags=integration --tests "*FailingTest*" --console=plain

# Check what the test is actually trying to do
# Look at @Tag annotations, test setup, assertions
```

### Step 2: Systematic Investigation  
1. **Read full stack trace** - don't just look at assertion failure
2. **Check test prerequisites** - environment variables, test data, network access
3. **Verify test expectations** - are the assertions correct for current behavior?
4. **Add debug logging** at key points to trace execution

### Step 3: Root Cause Analysis
```java
// Add strategic debug output
System.err.println("DEBUG: Expected=" + expected + ", Actual=" + actual);
System.err.println("DEBUG: Table schema=" + table.getSchema());

// Generate stack traces for complex flows
Thread.dumpStack();
```

### Step 4: Fix, Don't Replace
- **ALWAYS** fix the original failing test
- **NEVER** create `TestFoo2.java` because `TestFoo.java` fails  
- **NEVER** add `testMethodNew()` instead of fixing `testMethod()`
- **Only** create isolation tests for complex debugging, then delete them

## 📝 TEST QUALITY STANDARDS

### Required Test Tags
```java
@Tag("unit")        // Runs by default, no external dependencies
@Tag("integration") // Requires -PincludeTags=integration  
@Tag("performance") // Manual execution only
@Tag("temp")        // Temporary debugging tests - MUST be deleted
```

### Test Data Best Practices
- **Use numeric values** for time/date/timestamp expectations
- **Store timestamp with no TZ as UTC**, read/adjust to local TZ
- **Never use formatted values** in assertions
- **Test with production-like data** for integration tests

### Test Organization Rules
- **PROHIBITED**: Test files in module root directories
- **PROHIBITED**: Compiled `.class` files in repository
- **REQUIRED**: Proper package structure in `src/test/java/`
- **REQUIRED**: Delete temporary/debug tests after use

## ⚠️ COMMON ANTI-PATTERNS

### Test Execution Anti-Patterns
```bash
# ❌ Forgetting -P prefix
./gradlew :module:test includeTags=integration

# ❌ Assuming tests run without tag specification
./gradlew :module:test --tests "*IntegrationTest*"

# ❌ Not checking console output for "0 tests executed"
```

### Test Development Anti-Patterns
```java
// ❌ Creating duplicates instead of fixing
public class TestFoo2 { ... }        // Wrong
public void testMethodNew() { ... }  // Wrong

// ❌ Relaxing tests instead of fixing root cause  
// assertEquals(expected, actual);   // Original
assertEquals(actual, actual);        // Wrong "fix"

// ❌ Leaving debug artifacts
System.out.println("Debug info");   // Remove before commit
```

## 🎯 DECISION TREE: Test Not Running

```
Test not executing?
├─ "0 tests executed" message?
│  ├─ Check @Tag annotation on test class/method
│  ├─ @Tag("integration")? → Add -PincludeTags=integration
│  ├─ @Tag("unit")? → Should run by default, check test name pattern
│  └─ No @Tag? → Add @Tag("unit") and re-run
├─ Test found but failing?
│  ├─ Check environment variables (API keys, cache dirs)
│  ├─ Check test data prerequisites  
│  └─ Follow debugging workflow above
└─ Test skipped?
    ├─ Check for @Disabled annotation
    ├─ Check assumeTrue() conditions
    └─ Check environment assumptions
```

## 🔧 TIMEOUT AND PERFORMANCE

### Extended Timeout Commands
```bash
# File adapter full regression (all engines)
CALCITE_FILE_ENGINE_TYPE=DUCKDB gtimeout 1800 ./gradlew :file:test --continue --console=plain

# Long-running integration tests
timeout 300 ./gradlew :govdata:test -PincludeTags=integration --console=plain

# Continue on failure to see all results
./gradlew :module:test --continue -PincludeTags=integration
```

### Performance Considerations
- File adapter tests require extended timeouts
- Integration tests may need network access
- Some tests require specific environment variables
- Use `--continue` to see all failures, not just first one