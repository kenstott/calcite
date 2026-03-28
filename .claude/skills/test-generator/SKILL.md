---
description: "Systematic test generation with gap analysis, compile-fix loops, and checkpoint tracking"
model: sonnet
effort: high
---

# Test Generator

Systematically generate tests for a Calcite adapter: $ARGUMENTS

## Argument Parsing (Step 0)

`$ARGUMENTS` can be either **structured** or **natural language**.

### Structured Format

`<adapter-name> [flags...]`

- First token = adapter name
- Remaining tokens = flags:
  - `--jacoco` — run JaCoCo coverage report after generation
  - `--unit-only` — skip integration test generation
  - `--resume` — resume from existing checkpoint file
  - `--dry-run` — perform gap analysis only, do not generate tests

### Natural Language Format

If `$ARGUMENTS` does not start with a known adapter name or flag, treat the entire string as a natural language prompt. Extract:

1. **Adapter name** — look for any known adapter name mentioned anywhere in the text (e.g., "the govdata adapter" → `govdata`, "file module" → `file`)
2. **Intent** — map the user's intent to flags:
   - "verify", "check", "analyze", "audit", "assess" → `--dry-run` (gap analysis only, unless the text also asks to create/generate)
   - "create", "generate", "add", "write", "build" tests → full generation (no `--dry-run`)
   - "verify ... and if needed, create" or "check ... and fix gaps" → full generation (the intent is to act on findings)
   - "resume", "continue", "pick up where" → `--resume`
   - "unit tests only", "no integration" → `--unit-only`
   - "coverage", "jacoco" → `--jacoco`
3. **Scope narrowing** — if the user mentions specific classes, packages, or patterns (e.g., "focus on the downloader classes"), filter the gap list to match after Step 3

If no adapter name can be identified, report an error and list the known adapters.

### Examples

| Input | Parsed As |
|---|---|
| `govdata` | adapter=govdata |
| `file --dry-run --jacoco` | adapter=file, flags=--dry-run --jacoco |
| `verify that the govdata adapter meets all test criteria and if needed, create additional tests` | adapter=govdata, full generation |
| `check test coverage for the file adapter` | adapter=file, --dry-run |
| `generate unit tests for splunk, skip integration` | adapter=splunk, --unit-only |
| `resume test generation for file adapter with jacoco` | adapter=file, --resume --jacoco |
| `audit the sharepoint-list adapter's test gaps and create tests for any missing classes` | adapter=sharepoint-list, full generation |
| `focus on the downloader and loader classes in govdata` | adapter=govdata, full generation, scope filter=*Downloader*, *Loader* |

## Adapter Resolution (Step 1)

Map the adapter name to its Gradle module, source root, test root, and packages.

### Known Adapter Lookup Table

| Adapter | Gradle Module | Source Package Root | Notes |
|---|---|---|---|
| `file` | `:file` | `org.apache.calcite.adapter.file` | Multiple sub-packages (execution, format, partition, etc.) |
| `govdata` | `:govdata` | `org.apache.calcite.adapter.govdata` | Sub-packages (econ, sec, etc.) |
| `splunk` | `:splunk` | `org.apache.calcite.adapter.splunk` | Tests use `org.apache.calcite.test` package |
| `sharepoint-list` | `:sharepoint-list` | `org.apache.calcite.adapter.sharepoint` | — |
| `spark` | `:spark` | `org.apache.calcite.adapter.spark` | — |
| `arrow` | `:arrow` | `org.apache.calcite.adapter.arrow` | — |
| `elasticsearch` | `:elasticsearch` | `org.apache.calcite.adapter.elasticsearch` | — |
| `cassandra` | `:cassandra` | `org.apache.calcite.adapter.cassandra` | — |
| `mongodb` | `:mongodb` | `org.apache.calcite.adapter.mongodb` | — |
| `redis` | `:redis` | `org.apache.calcite.adapter.redis` | — |
| `kafka` | `:kafka` | `org.apache.calcite.adapter.kafka` | — |
| `geode` | `:geode` | `org.apache.calcite.adapter.geode` | — |
| `druid` | `:druid` | `org.apache.calcite.adapter.druid` | — |
| `innodb` | `:innodb` | `org.apache.calcite.adapter.innodb` | — |
| `graphql` | `:graphql` | `org.apache.calcite.adapter.graphql` | — |
| `openapi` | `:openapi` | `org.apache.calcite.adapter.openapi` | — |
| `salesforce` | `:salesforce` | `org.apache.calcite.adapter.salesforce` | — |
| `cloud-ops` | `:cloud-ops` | `org.apache.calcite.adapter.cloudops` | — |
| `csv-nextgen` | `:csv-nextgen` | `org.apache.calcite.adapter.csv` | — |

### Fallback Convention (Unknown Adapters)

If the adapter name is not in the table:
- Gradle module: `:<adapter-name>`
- Source root: `<adapter-name>/src/main/java`
- Test root: `<adapter-name>/src/test/java`
- Package: `org.apache.calcite.adapter.<adapter-name>` (with hyphens removed)

Verify the module directory exists before proceeding. If it does not exist, report an error.

### Source and Test Root Paths

For any adapter:
- **Source root**: `<adapter-name>/src/main/java/` (then navigate the package tree)
- **Test root**: `<adapter-name>/src/test/java/` (then navigate the package tree)

Use `Glob` to find all `.java` files under each root.

## Checkpoint Management (Step 2)

### Checkpoint File Location

`<module-dir>/TEST_PROGRESS.md`

Example: `file/TEST_PROGRESS.md`

### If `--resume` Flag is Set

1. Read the existing `TEST_PROGRESS.md`
2. Parse the tables to identify:
   - **Completed** classes (skip these)
   - **Failed** classes (retry these)
   - **Remaining** classes (process these)
3. Resume from the first unprocessed class

### If Starting Fresh (No `--resume` or No Checkpoint)

Create initial checkpoint:

```markdown
# Test Generation Progress: <Adapter Name>

**Started**: <date>
**Adapter**: <adapter-name>
**Module**: <gradle-module>
**Flags**: <flags>

## Summary

| Metric | Count |
|---|---|
| Source Classes | <N> |
| Already Covered | <N> |
| Gaps Found | <N> |
| Tests Generated | 0 |
| Tests Passing | 0 |
| Tests Failed | 0 |

## Completed

| Source Class | Test Class | Status | Notes |
|---|---|---|---|

## Failed

| Source Class | Test Class | Attempts | Error |
|---|---|---|---|

## Remaining

| Source Class | Priority | Reason |
|---|---|---|
```

## Gap Analysis (Step 3)

### Pass 1: Collect Source Classes

Use `Glob` to find all `.java` files under the source root. Exclude:
- `package-info.java`
- Annotation interface files (files containing only `@interface` declarations)

Record each class with its fully-qualified name.

### Pass 2: Collect Existing Test Classes

Use `Glob` to find all `*Test.java` and `*Test*.java` files under the test root.

For each test file, strip known suffixes to determine which source class it covers:
- `Test` → e.g., `FooTest` covers `Foo`
- `IntegrationTest` → e.g., `FooIntegrationTest` covers `Foo`
- `DeepCoverageTest` + optional digit → e.g., `FooDeepCoverageTest3` covers `Foo`
- `CoverageTest` → e.g., `FooCoverageTest` covers `Foo`
- `UnitTest` → e.g., `FooUnitTest` covers `Foo`

Build a set of covered class names.

### Pass 3: Identify Gaps and Prioritize

Gap = source classes NOT in the covered set.

Rank gaps by priority (highest first):

| Priority | Pattern | Rationale |
|---|---|---|
| 1 | `*SchemaFactory` | Entry point — must work |
| 2 | `*Schema` | Core schema discovery |
| 3 | `*Table`, `*TableFactory` | Query targets |
| 4 | `*Rule`, `*Converter` | Query planning correctness |
| 5 | `*Driver` | JDBC connectivity |
| 6 | `*Wrapper`, `*Handler` | Middleware |
| 7 | `*Util`, `*Utils`, `*Helper` | Shared utilities |
| 8 | `*Transformer`, `*Downloader`, `*Loader` | Data pipeline |
| 9 | Everything else | Remaining |

### Report to User

**Before generating any tests**, print the gap analysis:

```
=== Gap Analysis: <adapter> ===

Source classes:     <N>
Already covered:   <N>
Gaps found:        <N>

Priority 1 (SchemaFactory):
  - FileSchemaFactory

Priority 2 (Schema):
  - FileSchema

...

Proceed with test generation? (Y/n)
```

If `--dry-run` is set, stop here after printing the gap analysis.

## Detect Adapter Test Pattern (Step 4)

Read 2-3 existing test files in the adapter to determine conventions. Check for:

### Pattern Detection Rules

| Feature | Check | Example |
|---|---|---|
| **Mockito usage** | `import static org.mockito` or `mock(` | file adapter uses Mockito; govdata does not |
| **Class visibility** | `public class` vs `class` | file adapter uses `public class`; some use package-private |
| **TempDir usage** | `@TempDir` annotation | file adapter uses `@TempDir` |
| **Section banners** | `// === Section ===` comments | file adapter uses section banners |
| **Logger field** | `private static final Logger` | govdata uses Logger |
| **TestEnvironmentLoader** | `TestEnvironmentLoader` | govdata integration tests |
| **Test package** | Does test package match source package? | splunk tests use `org.apache.calcite.test` |
| **Assert style** | Hamcrest (`assertThat`) vs JUnit (`assertEquals`) | Both used, prefer Hamcrest |

Store detected patterns for use in generation.

### Adapter-Specific Pattern Defaults

- **file**: Mockito, `public class`, `@TempDir`, section banners, Hamcrest assertions
- **govdata**: No Mockito, real instances, Logger field, `TestEnvironmentLoader` for integration
- **splunk**: No Mockito, test package = `org.apache.calcite.test`
- **sharepoint-list**: No Mockito, `assertThrows(RuntimeException.class, ...)` for connection tests
- **Other**: Inspect existing tests; if none exist, default to govdata-style (no Mockito, real instances)

## Generate Test Classes (Step 5)

For each gap class (in priority order):

### 5a. Read Source Class

Read the source `.java` file to identify:
- Public methods (candidates for test cases)
- Constructors and their parameters
- Dependencies (fields, constructor parameters)
- Static methods and factory methods
- Exception paths (throws declarations, explicit throw statements)
- Enums and inner classes

### 5b. Generate Test File

Create `{ClassName}Test.java` in the appropriate test package directory.

#### Template Structure

```java
<license-header>
package <test-package>;

<imports>

/**
 * Unit tests for {@link <ClassName>}.
 */
@Tag("unit")
<class-visibility> class <ClassName>Test {
  <fields>

  <setup-methods>

  <test-methods>
}
```

#### License Header

Read an existing test file in the same module and copy its license header verbatim. The header ends at the first line that is not a comment (i.e., does not start with `/*`, ` *`, ` */`, or `//`).

**Important**: govdata main source uses BSL 1.1 but its tests use ASF 2.0. Always copy from a test file, not a source file.

#### Import Rules

- JUnit 5: `org.junit.jupiter.api.Test`, `Tag`, `BeforeEach`, etc.
- Hamcrest: `org.hamcrest.CoreMatchers.*`, `org.hamcrest.MatcherAssert.assertThat`
- Mockito (only if adapter uses it): `org.mockito.Mockito.*`, `org.mockito.Mock`, etc.
- Source class import
- **Java 8 only**: No `java.util.List.of()`, `Map.of()`, `var`, text blocks, records, sealed classes, switch expressions, pattern matching

#### Test Method Guidelines

- One `@Test` method per public method on the source class (at minimum)
- Additional methods for edge cases, null inputs, exception paths
- Method names: `test<MethodName>_<scenario>` (e.g., `testGetSchema_nullName`)
- Each test follows Arrange-Act-Assert pattern
- Use `assertThat` with Hamcrest matchers as primary assertion style
- Use `assertThrows` for expected exceptions

#### If `--unit-only` is NOT Set

Also generate `{ClassName}IntegrationTest.java` when the source class:
- Makes external connections (HTTP, DB, file system)
- Requires environment variables or config files
- Has complex initialization that needs real dependencies

Integration tests should:
- Use `@Tag("integration")`
- Include `assumeTrue` guards for required environment variables
- Test real interactions (not mocked)

### 5c. Java 8 Compliance Checklist

Before writing any generated test, verify NONE of these appear:
- `var` keyword
- Text blocks (`"""`)
- `List.of()`, `Map.of()`, `Set.of()` — use `Arrays.asList()`, `Collections.singletonMap()`, etc.
- `Stream.toList()` — use `.collect(Collectors.toList())`
- Records, sealed classes
- Switch expressions
- Pattern matching in `instanceof`
- `String.isBlank()`, `String.strip()` — use `String.trim()`, manual checks

### 5d. Write the File

Use the `Write` tool to create the test file at the correct path under the test root.

## Compile and Fix Loop (Step 6)

After writing each test file:

### 6a. Compile Check

```bash
./gradlew <gradle-module>:compileTestJava --console=plain 2>&1
```

### 6b. If Compilation Fails

Parse the error output to identify:
- Missing imports
- Wrong method signatures
- Incorrect types
- Inaccessible members (private/protected)

Fix the test file using `Edit` tool. Common fixes:
- Add missing imports
- Change method calls to match actual API
- Replace inaccessible member access with reflection or alternative approach
- Fix generic type parameters

### 6c. Run the Test

```bash
./gradlew <gradle-module>:test --tests "*<ClassName>Test*" --console=plain 2>&1
```

For integration tests, add `-PincludeTags=integration`.

### 6d. If Test Fails

Read the failure output and fix assertions or setup. Common issues:
- Wrong expected values
- Missing mock setup
- Null pointer from uninitialized dependencies
- Environment-dependent behavior

### 6e. Retry Limit

Up to **3 fix iterations** per test class (compile + run = 1 iteration).

If still failing after 3 attempts:
1. Mark as **FAILED** in checkpoint
2. Log the error summary
3. Move to the next gap class

## Update Checkpoint (Step 7)

After processing each class, update `TEST_PROGRESS.md`:

### On Success

Move from Remaining to Completed:
```markdown
| SourceClass | SourceClassTest | PASS | 5 tests, 0 failures |
```

### On Failure

Move from Remaining to Failed:
```markdown
| SourceClass | SourceClassTest | 3 | CompileError: cannot find symbol ... |
```

### Update Summary Counts

Recalculate all counters in the Summary table.

## Post-Generation Summary (Step 8)

Print a final summary:

```
=== Test Generation Complete: <adapter> ===

Classes processed:  <N>
Tests passing:      <N>
Tests failed:       <N>
New test files:     <N>

New files created:
  - <path/to/NewTest1.java>
  - <path/to/NewTest2.java>

Run all generated tests:
  ./gradlew <module>:test --console=plain

Run integration tests:
  ./gradlew <module>:test -PincludeTags=integration --console=plain

Checkpoint saved: <module>/TEST_PROGRESS.md
```

## Optional JaCoCo Report (Step 9)

If `--jacoco` flag was provided:

```bash
./gradlew <gradle-module>:test -PenableJacoco --console=plain 2>&1
```

After execution, check for the JaCoCo report:
- HTML: `<module>/build/reports/jacoco/test/html/index.html`
- XML: `<module>/build/reports/jacoco/test/jacocoTestReport.xml`

Report a summary of coverage percentages if available.

## Example Invocations

```
/test-generator file
/test-generator govdata --unit-only
/test-generator splunk --dry-run
/test-generator file --resume
/test-generator sharepoint-list --jacoco
/test-generator file --resume --jacoco
```

## Critical Rules

1. **Java 8 strict compliance** — ZERO TOLERANCE for Java 9+ features
2. **No Mockito injection into adapters that don't use it** — never add Mockito to `build.gradle.kts`
3. **License headers from test files** — never copy from source files (BSL vs ASF mismatch)
4. **Report gaps before generating** — always show the user the gap list first
5. **Checkpoint after every class** — progress must survive interruption
6. **3-attempt limit** — do not loop forever on a failing test
7. **Do not remove existing tests** — only add new test files
8. **Use adapter conventions** — match the style of existing tests in the module