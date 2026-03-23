# File Adapter Testing Updates Plan

Analysis date: 2026-03-23

## Overview

The file adapter has 115 test files but significant gaps in coverage and quality. The most dangerous gaps are in planner rules (11% coverage), execution engines (0%), and cache (0%). Additionally, 11 test files contain zero assertions, giving false confidence.

This plan is organized into phases. Each phase is self-contained and can be executed independently. Phases are ordered by risk — fixing silent correctness issues first, then filling coverage gaps, then consolidating duplication.

---

## Phase 1: Fix Assertion-Free Tests

**Why first**: These tests pass regardless of behavior. Any rule or materialization regression is invisible.

### 1.1 Rule tests (Critical)

**Files**: `rules/AllOptimizationRulesTest.java`, `rules/DuckDBOptimizationRulesTest.java`

Both files share identical `createTestData()` and `createHLLSketches()` methods and print emoji instead of asserting. Action:

- [ ] Delete both files
- [ ] Create `rules/OptimizationRulesTest.java` — single parameterized test class
- [ ] Add real assertions: verify plan shape after optimization, verify row counts, verify filter elimination
- [ ] Use HepPlanner for deterministic plan output (not VolcanoPlanner)
- [ ] Tag as `@Tag("integration")` since it needs Parquet test data

### 1.2 Materialization tests

**Files**: `materialization/MaterializedViewParquetTest.java`, `MaterializedViewQueryTest.java`, `ProofMaterializedViewsWorkWithParquet.java`

All three have zero assertions. Action:

- [ ] Delete `ProofMaterializedViewsWorkWithParquet.java` (172 lines, proof-of-concept artifact)
- [ ] Delete `MaterializedViewQueryTest.java` (174 lines, duplicates MaterializedViewParquetQueryTest)
- [ ] Add assertions to `MaterializedViewParquetTest.java`: verify materialized view is created, verify query results match non-materialized query, verify COUNT(*) uses materialized path

### 1.3 Other assertion-free tests

| File | Action |
|------|--------|
| `table/SingleTableTest.java` | Add assertions for row count, column names, first row values |
| `temporal/BeforeAfterComparisonTest.java` | Add assertions comparing timestamp before/after values |
| `temporal/DateSelectTest.java` | Add assertions for date column values |
| `excel/SimpleExcelTest.java` | Delete — `ExcelComprehensiveTest` covers this |
| `temporal/TimestampAnalysisTest.java` | Delete — tagged `@Tag("temp")`, pure debug output |
| `util/TestSchemaIntrospection.java` | Delete — no tag, no assertions, pure debug output |

### 1.4 Debug artifacts

- [ ] Delete `DebugTableListTest.java` — in default package, has `main()`, not a test
- [ ] Review `CsvTypeInferenceTest.java` — remove `@Tag("temp")`, keep `@Tag("unit")`
- [ ] Review `BasicRefreshTest.java` — remove `@Tag("temp")`, keep `@Tag("unit")`

---

## Phase 2: Test Planner Rules

**Why**: 8 of 9 rules have zero test coverage. These rules silently transform query plans — wrong behavior produces wrong results without errors.

### 2.1 Create dedicated test per rule

Each test should verify:
1. Rule **matches** on appropriate RelNode patterns
2. Rule **does not match** on inappropriate patterns
3. Transformed plan is **semantically correct** (same results, better cost)
4. Edge cases: empty tables, single-row tables, all-null columns

Use HepPlanner for deterministic output. Use `RelOptUtil.toString()` to assert plan shape.

| Rule | Test File to Create | Key Scenarios |
|------|-------------------|---------------|
| `SimpleFileFilterPushdownRule` | `SimpleFileFilterPushdownRuleTest.java` | Filter eliminates all rows via stats; filter cannot be pushed (complex expression); filter on non-stats column passes through |
| `SimpleFileJoinReorderRule` | `SimpleFileJoinReorderRuleTest.java` | Small table moved to build side; equal-size tables unchanged; three-way join reorder |
| `CountStarStatisticsRule` | `CountStarStatisticsRuleTest.java` | COUNT(*) replaced with table metadata; COUNT(*) with WHERE not replaced; table without stats falls back |
| `HLLCountDistinctRule` | `HLLCountDistinctRuleTest.java` | COUNT(DISTINCT x) rewritten to HLL; non-distinct COUNT not matched; multiple distinct columns |
| `SimpleHLLCountDistinctRule` | `SimpleHLLCountDistinctRuleTest.java` | Same as HLL but for simple (non-DuckDB) path |
| `SimpleFileColumnPruningRule` | `SimpleFileColumnPruningRuleTest.java` | Unused columns removed from scan; all columns used — no change; SELECT * preserves all |
| `AlternatePartitionSelectionRule` | `AlternatePartitionSelectionRuleTest.java` | Alternate partition selected when better stats; no alternate available — no change |
| `FileStatisticsRules` | `FileStatisticsRulesTest.java` | Rules registered correctly; statistics propagated through plan |

### 2.2 DuckDB-specific rules

| Rule | Test File to Create | Key Scenarios |
|------|-------------------|---------------|
| `DuckDBCountDistinctInterceptRule` | `DuckDBCountDistinctInterceptRuleTest.java` | Intercepts COUNT(DISTINCT) for DuckDB-native execution |
| `DuckDBHLLCountDistinctRule` | `DuckDBHLLCountDistinctRuleTest.java` | Rewrites to DuckDB HLL function |
| `DuckDBIcebergCountStarRule` | `DuckDBIcebergCountStarRuleTest.java` | Rewrites COUNT(*) using Iceberg metadata |

---

## Phase 3: Test Execution Engines and Tables

**Why**: 0% coverage on the code that actually runs queries.

### 3.1 Execution engines

- [ ] Create `execution/DuckDBExecutionEngineTest.java`
  - Test query execution through DuckDB path
  - Test memory limit configuration
  - Test error handling on malformed Parquet

- [ ] Create `execution/ParquetExecutionEngineTest.java`
  - Test Parquet file reading via engine
  - Test column pruning at engine level
  - Test predicate pushdown at engine level

- [ ] Create `execution/VectorizedArrowExecutionEngineTest.java`
  - Test vectorized batch processing
  - Test batch size configuration
  - Test fallback when Arrow not available

### 3.2 Table implementations

Priority targets (most used, zero tests):

- [ ] Create `table/ParquetScannableTableTest.java`
  - Test `scan()` returns correct rows
  - Test `FilterableTable` interface — filter pushdown
  - Test with null values, empty files, large files

- [ ] Create `table/GlobParquetTableTest.java`
  - Test multi-file pattern matching
  - Test partition discovery from directory structure
  - Test schema merge across files with different columns

- [ ] Create `table/HLLAcceleratedTableTest.java`
  - Test HLL sketch integration
  - Test approximate vs exact count comparison
  - Test sketch cache behavior

---

## Phase 4: Test ETL Write Path and Cache

**Why**: The materialization writers are the critical output path. Cache manages concurrent access.

### 4.1 Materialization writers

- [ ] Create `etl/ParquetMaterializationWriterTest.java`
  - Test writing single partition
  - Test writing multiple partitions
  - Test schema preservation
  - Test overwrite vs append behavior

- [ ] Create `etl/IcebergMaterializationWriterTest.java`
  - Test Iceberg commit
  - Test partition spec handling
  - Test schema evolution (add column)
  - Test concurrent writes (if applicable)

- [ ] Create `etl/MaterializationWriterFactoryTest.java`
  - Test factory selects correct writer for Parquet vs Iceberg
  - Test configuration propagation

### 4.2 Cache subsystem

- [ ] Create `cache/ConcurrentSpilloverManagerTest.java`
  - Test concurrent get/put from multiple threads
  - Test spillover to disk when memory threshold exceeded
  - Test eviction policy

- [ ] Create `cache/ConcurrentParquetCacheTest.java`
  - Test cache hit/miss behavior
  - Test concurrent access safety
  - Test cache invalidation

---

## Phase 5: Consolidate Duplicative Tests

**Why**: Reduces maintenance burden and makes test intent clearer.

### 5.1 Recursive directory tests

- [ ] Keep `feature/RecursiveDirectoryComprehensiveTest.java` (superset)
- [ ] Delete `table/RecursiveDirectoryTest.java`
- [ ] Delete `table/RecursiveGlobTest.java`

### 5.2 Materialization tests

After Phase 1 deletions, remaining files:
- [ ] Merge `MaterializedViewParquetTest.java` and `MaterializedViewParquetQueryTest.java` into single `MaterializedViewTest.java`
- [ ] Keep `MaterializationTest.java` (different scope — tests materialization lifecycle)

### 5.3 Excel tests

- [ ] Delete `excel/SimpleExcelTest.java` (covered by `ExcelComprehensiveTest`)
- [ ] Verify `ExcelFileTest` and `MultiTableExcelTest` test scenarios not in `ExcelComprehensiveTest`; merge if redundant

### 5.4 Casing tests

- [ ] Merge `config/CasingConfigurationTest.java` and `table/TableNameCasingTest.java` into single `CasingTest.java`

---

## Phase 6: Housekeeping

### 6.1 Add missing @Tag annotations

| File | Suggested Tag |
|------|--------------|
| `DocumentConverterIntegrationTest.java` | `@Tag("integration")` |
| `DuckDBExcelIntegrationTest.java` | `@Tag("integration")` |
| `IcebergNonEmptyTableTest.java` | `@Tag("integration")` |
| `JsonPathExtractionTest.java` | `@Tag("unit")` |
| `TableColumnTest.java` | `@Tag("unit")` |

### 6.2 Fix system property pollution

In these files, add `@AfterEach` to restore original system property values:
- `rules/AllOptimizationRulesTest.java` (or its replacement from Phase 1)
- `rules/DuckDBOptimizationRulesTest.java` (or its replacement from Phase 1)
- `rules/PartitionDistinctRuleTest.java`

### 6.3 Reduce System.out.println noise

Files with 40+ println calls — convert to logger or remove:
- `storage/SharePointLiveIntegrationTest.java` (133)
- `refresh/BasicRefreshTest.java` (84)
- `storage/MicrosoftGraphCredentialTest.java` (55)
- `temporal/DualTimestampTypeTest.java` (50)
- `refresh/RefreshableTableTest.java` (47)

### 6.4 Re-evaluate disabled tests

10 disabled tests in `HtmlCrawlerIntegrationTest` and `HtmlCrawlerTest`:
- [ ] Replace `@Disabled` with `assumeTrue(networkAvailable())` guard
- [ ] Or delete if HTML crawler is no longer maintained

---

## Coverage Targets

| Source Area | Current | Target After Plan |
|------------|---------|-------------------|
| Rules | 11% (1/9) | 100% (9/9) |
| DuckDB rules | 0% (0/3) | 100% (3/3) |
| Execution engines | 0% (0/16) | 19% (3/16 — the 3 main engines) |
| Table implementations | 38% (6/16) | 56% (9/16 — add 3 priority tables) |
| ETL write path | 0% (0/4) | 75% (3/4) |
| Cache | 0% (0/3) | 67% (2/3) |
| Materialization tests | 5 files, 3 assertion-free | 2 files, all with assertions |

## Execution Notes

- Each phase can be a separate PR
- Phase 1 is purely cleanup — no new test logic, low risk
- Phase 2 is the highest-value work — planner rules are where silent bugs live
- Phases 3-4 require test data fixtures (Parquet files, Iceberg catalogs)
- Phase 5 is safe deletion — verify with `./gradlew :file:test -PrunAllTests` before and after
- Phase 6 is mechanical and can be done alongside any other phase
