# File Adapter — Contradictions & Resolutions

Conflicts surfaced while reverse-engineering requirements from **docs + code + tests**, with the
owner's resolution for each. Verdict legend: `DOC-WRONG` (fix doc), `CODE-WRONG` (fix code — a bug),
`BOTH` (reconcile), `ACCEPT` (intended; pin behavior). Ledger:
[../requirements/file.yaml](../requirements/file.yaml).

Status: **25 of 26 resolved; C-14 deferred to a deeper analysis.** These resolutions are decisions,
not yet implemented — each becomes a code/doc change or a filed issue + a golden test.

---

## A. Doc ↔ Code

### C-01 — `recursive` default  → **CODE-WRONG**
Docs *true*, code *false* (and string `"true"` ignored).
**Resolution:** Change code so `recursive` defaults to **true** (documented behavior is intended);
fix the parser so a string `"true"` also enables it. (FILE-110)

### C-02 — Default execution engine  → **CODE-WRONG**
Javadoc *DuckDB-if-available*, code *always parquet*.
**Resolution:** Implement DuckDB-if-available detection (default to DuckDB when its driver is on the
classpath, else parquet). (FILE-119)

### C-03 — HLL std error at precision 14  → **DOC/CODE reconcile (publish conservative)**
Docs *~2%*, code/tests *~0.8%*.
**Resolution:** Keep **~2%** as the published/guaranteed bound (real accuracy is better); align the
code comment to ~2%. Tests keep the <2% ceiling. (FILE-120, FILE-152)

### C-04 — Optimizations on by default  → **CODE-WRONG**
Docs *active*, code *rules gated off* (separate from sketch-gen which is on).
**Resolution:** Enable the optimization rules **on by default** (they're result-preserving); keep a
property to disable. Document the two layers (sketch-gen vs rule-activation). NOTE: per C-18, the HLL
rule still fires only for `APPROX_COUNT_DISTINCT`. (FILE-121)

### C-05 — Raw cache TTL  → **DOC-WRONG**
Docs imply *TTL*, code *no TTL*.
**Resolution:** No-TTL is intended; fix docs to state the IncrementalTracker is the sole staleness
authority and the raw cache is immutable until invalidated. (FILE-143)

### C-06 — `s3://` URL auto-routing  → **CODE-WRONG**
Docs *auto-route*, code *throws (needs creds)*.
**Resolution:** Make `s3://` URLs route, falling back to the **AWS default credential chain** (env /
IAM role / profile) when no explicit creds are given. (FILE-112)

### C-07 — `listFiles` missing dir  → **CODE-WRONG (standardize)**
Local/S3 *throw*, HDFS *empty list*.
**Resolution:** **Standardize on throw** — make HDFS throw too (a silent empty list hides config
mistakes). (FILE-113, FILE-117)
**IMPLEMENTED 2026-06-30:** `HDFSStorageProvider.listFiles` now throws `IOException "Directory does
not exist"` on a missing path (FILE-117 golden green).

### C-08 — `confidenceThreshold` dead  → **CODE-WRONG**
Documented but inert.
**Resolution:** Implement it — promote a column to numeric/temporal when the non-conforming string
ratio is below `1 − threshold`, instead of any single string forcing VARCHAR. (FILE-096)

### C-09 — Numeric/boolean tokens  → **CODE-WRONG**
`1e5` (no dot) → VARCHAR; booleans accept yes/y/no/n.
**Resolution:** Make inference recognize dot-less scientific notation (`1e5`) as DOUBLE; update docs
to also list the yes/y/no/n boolean tokens. (FILE-097, FILE-100)

### C-10 — Markdown naming  → **CODE-WRONG**
Markdown PascalCase (base name + on-disk filenames) vs lowercase elsewhere.
**Resolution:** Make Markdown match the other converters — **lowercase snake_case** base name and
on-disk filenames. (FILE-105, FILE-171)

### C-11 — `FileReaderException` SQLStates  → **CODE-WRONG**
Docs list SQLStates; the exception carries none.
**Resolution:** Make `FileReaderException` a `SQLException` carrying the documented SQLStates
(42S02/22018/08001/HY000/42000), mapping each failure mode to its code. (FILE-151)

---

## B. Code ↔ Code

### C-12 — Two JDBC drivers, different defaults  → **CONSOLIDATE**
**Resolution:** Consolidate on **AperioDriver** (`jdbc:aperio:`); deprecate `FileJdbcDriver`; align
defaults to DuckDB-if-available (per C-02). (FILE-146, FILE-147, FILE-168)

### C-13 — `batch_size` invalid handling  → **CODE-WRONG (standardize)**
URL path defaults 2048; Properties path skips the schema.
**Resolution:** **Fail-fast with a clear error** on an invalid `batch_size` (no silent default, no
silent schema-drop). (FILE-168)

### C-14 — `${VAR}` expansion inconsistency  → **CODE-WRONG (reuse resolver)**
AperioDriver lacks `${VAR:default}` and leaves unresolved `${VAR}` literal.
**Analysis (done):** There is NO 2-level pass-through. Two *independent* entry points each resolve
their own operand once — govdata uses the file module's `VariableResolver`
(`GovDataSchemaFactory.resolveEnvVar`, `ModelOperand.getString`, `EtlRunner`); `AperioDriver` uses its
own `expandVarString`. Both already leave an undefined `${VAR}` literal (shared, intentional,
unit-tested at `VariableResolverTest:113-118`). No code keys off a leftover `${...}` as a pass-through
signal (the `contains("${")` checks are just regex-skip guards). The only real gap: `expandVarString`
doesn't support `${VAR:default}`/`${VAR:-default}` and is a weaker duplicate.
**Resolution:** Delete `AperioDriver.expandVarString` and route `buildOperand` output through the
shared `VariableResolver` (same module). Aligns with C-12 (AperioDriver canonical). Low priority /
latent (the govdata production path never hits AperioDriver). (FILE-147)

### C-15 — `shouldAutoDownload` default  → **CODE-WRONG (⚠ confirm)**
Test *true*, code *false*.
**Resolution:** Change code so `autoDownload` defaults to **true**. ⚠ This **reverses** a prior
documented decision ("autoDownload default false — reads must never trigger ETL"); confirm against
that safety rationale before implementing.

---

## C. Likely bugs

### C-16 — Silent fallbacks vs rule #6  → **CODE-WRONG**
**Resolution:** **Fix all to raise** a clear error on parse failure; remove the fabricated synthetic
HLL seeds. (FILE-101, FILE-126)

### C-17 — Unparseable DATE throws NPE  → **CODE-WRONG**
**Resolution:** Replace the NPE with the same clear, raised parse error as C-16 — consistent across
DATE/TIME/TIMESTAMP. (refines FILE-101)

### C-18 — `APPROX_ONLY_INSTANCE` rewrites all COUNT(DISTINCT)  → **CODE-WRONG**
**Resolution:** Plain `COUNT(DISTINCT)` always returns an **exact** count; HLL rewrite only for
`APPROX_COUNT_DISTINCT` (or an explicit opt-in). Fix the instance to honor `approxOnly`. (FILE-124)

### C-19 — `/tmp` debug-file side effects  → **CODE-WRONG**
**Resolution:** **Gate behind a debug flag** — never write hard-coded `/tmp` paths by default; drop
the WARN logs to debug level. (FILE-125)

### C-20 — `RefreshablePartitionedParquetTable.scan()` NPE  → **CODE-WRONG**
**Resolution:** Add the null/empty guard `getRowType()` uses (empty file list → empty result), plus a
regression test. (FILE-135)

### C-21 — Materialized view stuck after failure  → **CODE-WRONG**
**Resolution:** Set `materialized=true` only after a successful write; on failure reset the flag and
delete the partial parquet so the next access retries. (FILE-137)

### C-22 — Time-travel statistics use current snapshot  → **CODE-WRONG**
**Resolution:** Make `getStatistic` honor `snapshotId`/`asOfTimestamp` so stats match the scanned
snapshot; add a test. (FILE-145)

---

## D. Inactive / weak

### C-23 — Dead HLL rule  → **CONSOLIDATE**
**Resolution:** Re-enable the intended capability and merge the two HLL rules into one.

### C-24 — FK matched by position  → **CODE-WRONG**
**Resolution:** Match FK target columns **by name** (standard, least-surprising); add a test where
source/target names/orders differ. (FILE-149)

### C-25 — SmartCasing all-upper passthrough  → **ACCEPT**
**Resolution:** `DEPTS→depts` is intended; lock it with a test (plus the known-acronym dictionary
cases). (FILE-148)

### C-26 — Weak tests  → **FOLD INTO GOLDEN REWRITE**
**Resolution:** Leave for now; replace with proper golden/invariant tests during the strangler
rewrite (track as gaps).

## C-28 — Uppercase file extension `.JSON` not recognized (FILE-061)
- **Found by:** RECODE wave 5 (NamingUnionTrackerRequirementsTest).
- **Discrepancy:** supported-formats.md documents `PRODUCTS.JSON -> products`, but `FileSchema` bulk-conversion discovery gates on `source.trimOrNull(".json")` (FileSchema.java:1803), which is case-sensitive; `isJsonFile` lowercases but is not the gate. An uppercase `.JSON` extension registers no table.
- **Status:** staged `@Disabled("C-28")` target in NamingUnionTrackerRequirementsTest#uppercaseJsonExtensionIsRecognized. Resolution pending: make extension match case-insensitive, or correct the doc.

## C-29 — CSV multi-file union is RICHEST_FILE, not superset+null-fill (FILE-034)
- **Found by:** RECODE wave 5 (NamingUnionTrackerRequirementsTest).
- **Discrepancy:** FILE-034 states the unified table exposes the column SUPERSET with NULLs for absent columns. For CSV, `resolveCsvSchema` is hard-wired to RICHEST_FILE (single file's columns by max count) — never a superset, never NULL-filled. The superset+NULL behavior is the **Parquet** `UNION_ALL_COLUMNS` path (`resolveParquetUnionAllColumns`), which needs real parquet files (not hermetic).
- **Status:** FILE-034 kept **in-progress**. CSV richest-file subset asserted now; the parquet-superset path needs an integration parquet fixture (follow-up).

## C-30 — jsonSearchPaths has no auto child-table / parent-id FK promotion (FILE-056)
- **Found by:** RECODE wave 5 (TableExtractionRequirementsTest).
- **Discrepancy:** FILE-056 states nested arrays become CHILD tables with a synthesized parent-id FK column. `JsonMultiTableFactory.createTables` maps each JSONPath to exactly one `JsonScannableTable`; there is no nested-array→child promotion and no FK injection anywhere in `format/json/*`. A nested array becomes its own table ONLY via an explicit JSONPath aimed at it.
- **Status:** FILE-056 description amended to match reality (one table per path; nested array via explicit path; no auto FK). Resolution: doc was overstated.

## C-31 — file-adapter ClickHouse engine views not enumerated by Calcite JdbcSchema (FILE-044)
- **Found by:** live-service wave (ClickHouseEngineConsistencyIntegrationTest).
- **Discrepancy:** executionEngine=clickhouse (ClickHouseJdbcSchemaFactory) creates `CREATE OR REPLACE VIEW s.<table> AS SELECT * FROM file('<parquet>','Parquet')` on the server (confirmed in factory logs), but `JdbcSchema.getTableNames()` returns [] for the ClickHouse database, so a query through jdbc:calcite fails validation "Object '<table>' not found". The engine cannot be driven end-to-end through Calcite. (Also: clickhouse-jdbc 0.7.1 returns a bare "Query failed" for statements that succeed over the ClickHouse HTTP interface.)
- **Status:** FILE-044 kept **in-progress**. The integration test proves the clickhouse model creates the views and that ClickHouse-server-side COUNT/filter/GROUP BY + uniq() match the parquet path; the through-Calcite consistency is blocked pending a fix to ClickHouseJdbcSchema view enumeration.

## C-32 — csv `nullableThreshold` is parsed but unwired (FILE-055)
- **Found by:** RECODE backlog (CsvInferenceRequirementsTest, 2026-06-29).
- **Discrepancy:** FILE-055 states "with makeAllNullable=false a column becomes nullable when its null ratio exceeds nullableThreshold." `CsvTypeInferrer` parses, clamps (`Math.max(0,min(1,...))`) and exposes `nullableThreshold` via `getNullableThreshold()` (default 0.0) but NO code consumes it: every `determineType` branch sets `nullable = nullValues > 0 || config.makeAllNullable` — a binary rule, not a ratio-vs-threshold comparison. `ColumnTypeInfo.nullRatio` is computed but never compared. So any single null token makes the column nullable regardless of ratio.
- **Resolution (2026-06-29):** RESOLVED as **DOC-WRONG / advisory** — nullability is advisory metadata, never enforced (parquet/duckdb treat all columns nullable; the flag only informs humans + the optimizer), so a ratio threshold buys nothing. FILE-055 reworded to the binary rule and marked **complete**. `nullableThreshold` is **dead code slated for removal** (low priority): drop the field, its clamp, and `getNullableThreshold()` from `CsvTypeInferrer`. The `@Disabled("C-32")` `columnNullableOnlyWhenNullRatioExceedsThreshold_target` stays as documentation of the removed knob until the field is deleted.

## C-33 — env-var substitution order: system property OVERRIDES env (FILE-049)
- **Found by:** RECODE backlog (EnvVarSubstitutionRequirementsTest, 2026-06-30).
- **Discrepancy:** FILE-049 stated the resolution order "env → system property → default → error". The production `EnvironmentVariableSubstitutor.substitute(String)`/`substituteInJson(String)` build `new HashMap<>(System.getenv())` and then overlay `System.getProperties()` ON TOP (EnvironmentVariableSubstitutor.java:65-66, 137-138), so a system property of the same name **overrides** the env var — the opposite precedence. The source comment ("System properties take precedence") shows this is intentional.
- **Resolution:** DOC-WRONG. FILE-049 reworded to "system property → env → default → error" and marked **complete**; the golden asserts the actual override (set sysprop PATH → substitute("${PATH}") returns the sysprop value). No code change — the precedence is deliberate.

## C-34 — ETL validators stage declared but never invoked (FILE-085)
- **Found by:** RECODE backlog (EtlMetadataRequirementsTest, 2026-06-30).
- **Discrepancy:** FILE-085 documents the per-row order responseTransformer → rowTransformers → **validators** (drop|warn|fail). The validator machinery exists — `Validator.validate(Map)`/`ValidationResult` (VALID/DROP/WARN/FAIL), `HooksConfig.getValidators()`, and `getValidatorAction()` default CONTINUE — but a grep over `file/src/main/java` finds **zero call sites**: `getValidators()`/`Validator.validate(...)` are never invoked. So the validators stage of the hook order is dead code, and the responseTransformer→rowTransformers→validators chain cannot be proven end-to-end. (Also: `HttpSource.transformResponse` ignores `getResponseTransformerAction()` and always rethrows — the response error action is unconfigurable.)
- **Status:** FILE-085 kept **in-progress**. The error-handling defaults (fail/skip_row/continue) are asserted now; the full-order/validators clause is staged `@Disabled("C-34")` in `EtlMetadataRequirementsTest#fullHookOrderThroughValidators_target`. Pagination→Iceberg streaming is an integration follow-up. Resolution pending: wire the validators stage into the pipeline (or drop the validator config + doc).
