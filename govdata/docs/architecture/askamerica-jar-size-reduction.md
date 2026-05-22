# AskAmerica Engine JAR Size Reduction Plan

**Current size:** ~719 MB (`askamerica-engine-*.jar`)
**Target:** ~150–200 MB
**Status:** Deferred — no code changes yet

## Background

Two shadow JARs are defined:

| JAR | Module | Main class | Purpose |
|---|---|---|---|
| `sih-govdata-*.jar` | `govdata` | `EtlRunner` | Full ETL + read; used by workers |
| `askamerica-engine-*.jar` | `askamerica-engine` | `McpServerLauncher` | Read-only MCP server; distributed to end users |

`askamerica-engine` depends on `project(":govdata")` which pulls in all ETL
dependencies. The `shadowJar` block in `askamerica-engine/build.gradle.kts`
applies excludes to strip ETL classes, but those excludes are incomplete.

## Root Cause

`govdata` has no query/ETL module split. ETL dependencies (Hadoop, AWS SDK v1,
POI, ONNX) live in the same Gradle module as the query-time schema factories.
Until that split happens, the `askamerica-engine` shadow JAR requires
increasingly fragile class-level excludes.

---

## Phase 1 — Complete the Excludes (Quick Win, ~250 MB savings)

**File:** `askamerica-engine/build.gradle.kts` → `tasks.shadowJar { ... }`

Add the following excludes (no code changes required):

```kotlin
// AWS SDK v1 — DuckDB handles S3 natively; v1 SDK is ETL-only
exclude("com/amazonaws/**")
exclude("com/amazon/**")

// Hadoop — complete the partial excludes already present
exclude("org/apache/hadoop/**")

// POI schemas leaking through existing poi exclude
exclude("org/openxmlformats/**")
exclude("org/apache/xmlbeans/**")

// Byte-buddy — runtime code generation pulled by Hadoop/AWS
exclude("net/bytebuddy/**")

// Redis client — transitive, not used at query time
// (verify source: ./gradlew :askamerica-engine:dependencies | grep redisson)
exclude("org/redisson/**")

// RxJava — transitive of AWS SDK v1 / Hadoop; gone with those
exclude("io/reactivex/**")

// Arrow — extend existing partial exclude; write path only
exclude("org/apache/arrow/**")
```

**Verification after applying:**
```bash
./gradlew :askamerica-engine:shadowJar
jar tf askamerica-engine/build/libs/askamerica-engine-*.jar | grep -E "^(com/amazonaws|org/apache/hadoop|org/openxmlformats|net/bytebuddy|org/redisson|io/reactivex)" | wc -l
# Should return 0
ls -lh askamerica-engine/build/libs/askamerica-engine-*.jar
```

---

## Phase 2 — AWS SDK v1 → v2 Migration (~130 MB savings, overlaps Phase 1)

Replace `com.amazonaws:aws-java-sdk-s3:1.12.x` with AWS SDK v2 modular jars.
Only S3 is needed at query time.

**In `govdata/build.gradle.kts`:**
```kotlin
// Remove:
implementation("com.amazonaws:aws-java-sdk-s3:1.12.565")
implementation("com.amazonaws:aws-java-sdk-sts:1.12.565")

// Add:
implementation("software.amazon.awssdk:s3:2.25.x")
implementation("software.amazon.awssdk:url-connection-client:2.25.x")
```

Call-site changes: any class using `AmazonS3Client` / `AmazonS3Builder` needs
migration to `S3Client` / `S3ClientBuilder`. Grep first:
```bash
grep -r "AmazonS3\|com\.amazonaws\.services\.s3" govdata/src/main/java/ --include="*.java" -l
```

---

## Phase 3 — govdata Module Split (Structural Fix)

Split `govdata` into:

| Module | Dependencies | Used by |
|---|---|---|
| `govdata-query` | `duckdb_jdbc`, `iceberg-core`, `jackson-databind`, `httpclient` | `askamerica-engine` |
| `govdata-etl` | everything current | `sih-govdata` JAR, workers |

`askamerica-engine` would depend on `project(":govdata-query")` only, making
the shadow JAR naturally small without any exclude maintenance.

**Prerequisite:** Phases 1 and 2 first (to understand the actual query-time
dependency surface before splitting).

---

## Testing Requirements

Each phase must pass before moving to the next.

| Phase | Tests required |
|---|---|
| 1 | Run `askamerica-engine` integration tests against live R2 + local parquet. Verify all schemas load (`list_schemas`, `list_tables`). Verify a query per schema returns rows. |
| 2 | Same as Phase 1 plus: verify S3 presigned URL generation, multipart reads, and STS credential refresh still work. Run `govdata` integration tests for any schema using R2 storage. |
| 3 | Full regression — `CALCITE_FILE_ENGINE_TYPE=DUCKDB ./gradlew :file:test` plus all govdata schema smoke queries. ETL worker dry-runs against staging. |

Phase 1 is low-risk (excludes only, no code changes) and can be done without
an integration environment. Phases 2 and 3 require live R2 credentials.

---

## Expected Outcomes

| Phase | Action | Est. Savings | JAR Size After |
|---|---|---|---|
| 1 | Complete excludes | ~250 MB | ~470 MB |
| 1+2 | Complete excludes + AWS v2 | ~380 MB | ~340 MB |
| 1+2+3 | Module split | ~520 MB | ~200 MB |
