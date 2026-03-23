---
description: JUnit 5 testing patterns, fixtures, assertions, and tag-based execution for this project
---

# Java Testing Conventions

Apply these testing patterns when working on $ARGUMENTS.

## Framework: JUnit 5 (Jupiter 5.9.1)

```java
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
```

No JUnit 4 — the build auto-converts.

## Tag System — CRITICAL

```java
@Tag("unit")        // Runs by default
@Tag("integration") // Requires -PincludeTags=integration
@Tag("performance") // Manual only
```

```bash
./gradlew :module:test                                  # unit only
./gradlew :module:test -PincludeTags=integration        # integration only
./gradlew :module:test -PrunAllTests                    # all tags
```

**The #1 mistake**: Running `--tests "*IntegrationTest*"` without `-PincludeTags=integration` → 0 tests executed.

## Fixture Pattern (Project Standard)

This project uses immutable fixture objects with fluent `with*` builders — NOT `@ParameterizedTest`.

```java
// Available fixtures (in testkit module):
Fixtures.forParser()     → SqlParserFixture
Fixtures.forValidator()  → SqlValidatorFixture
Fixtures.forSqlToRel()   → SqlToRelFixture
Fixtures.forRules()      → RelOptFixture
Fixtures.forOperators()  → SqlOperatorFixture
Fixtures.forMetadata()   → RelMetadataFixture

// Usage:
@Test void testFilterPushdown() {
  Fixtures.forRules()
      .withPlanner(p -> p.addRule(MyRule.INSTANCE))
      .sql("SELECT * FROM t WHERE x > 5")
      .check();
}

// Customize with with*:
fixture.withFactory(factory -> factory.withValidator(...))
```

## Assertions

```java
// Primary: Hamcrest matchers
assertThat(result, is(42));
assertThat(list, hasSize(3));
assertThat(plan, containsString("LogicalFilter"));
assertThat(value, notNullValue());

// JUnit 5 for exceptions and conditions
assertThrows(CalciteException.class, () -> parse(badSql));
assertDoesNotThrow(() -> validate(goodSql));
assertTrue(condition, "message");

// DiffRepository for plan comparison (regression-style)
DiffRepository diffRepos = DiffRepository.lookup(MyTest.class);
```

## Test Structure

```java
@Tag("integration")
class MyAdapterIntegrationTest {

  @TempDir
  Path tempDir;  // JUnit 5 temp directory injection

  @BeforeAll
  static void setUp() {
    // One-time setup (static)
  }

  @BeforeEach
  void init() {
    // Per-test setup
  }

  @Test void testBasicQuery() {
    // Arrange
    // Act
    // Assert
  }
}
```

## Environment-Dependent Tests

```java
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Test void testWithApiKey() {
  assumeTrue(System.getenv("FRED_API_KEY") != null,
      "FRED_API_KEY required");
  // Test body
}
```

## Conditional Execution

```java
// Custom ExecutionCondition (see ArrowExtension for example)
@ExtendWith(ArrowExtension.class)
class ArrowTest { ... }
```

## Source environment before running tests

```bash
cd /root/calcite/govdata && source .env.sh
```

Required for API keys (BLS, FRED, BEA), S3 credentials, cache directories.
