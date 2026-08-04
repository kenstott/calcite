# Numbers/Booleans/Temporals Normalization — Audit Findings & Remediation Plan

## Context

We claim to "find and normalize all numbers, booleans, and temporal values into typed
data." This plan follows an audit of that claim across the `file/` adapter's format
converters (CSV, JSON, Excel, HTML, XML, DOCX, PPTX, Markdown, YAML) and the `govdata/`
schemas built on top of them. The audit ran three independent research passes (CSV
execution path, non-CSV formats, govdata-specific normalization) and read the actual
converter/enumerator code rather than the docs describing it — several docs (e.g.
`file/docs/csv-type-inference.md`) describe the intended behavior accurately; the gap is
between that intended behavior and what the code actually does at runtime.

**Bottom line: the claim is true in outline, false in specifics.** Real typed-value
conversion does happen for CSV and for native JSON numbers/booleans, and HTML/XML/DOCX
scraping does real number/boolean detection. But there are three genuine correctness bugs
(not just missing coverage) where the code produces silently wrong values, plus several
formats and value shapes with no normalization at all.

This plan is design/scoping only. Implementation is a distinct next step requiring
explicit go-ahead per area, since the fixes touch `file/` (used well beyond govdata) and
`govdata/` schema YAML that 26 live ETL pools depend on.

## Bugs (produce wrong data today, not just missing coverage)

### B1. CSV date parsing silently ignores the formatter that inference proved correct

`CsvTypeInferrer.determineType` picks the specific `DateTimeFormatter` that successfully
parsed a column's sampled values (e.g. `dd/MM/yyyy` vs `MM/dd/yyyy` — genuinely ambiguous
without knowing which locale wrote the file) and stores it on
`ColumnTypeInfo.dateTimeFormatter`
([CsvTypeInferrer.java:249](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeInferrer.java#L249)).

That formatter is never threaded through. All three call sites that build a
`CsvTypeConverter` pass an empty or null formatters map instead:
- [CsvEnumerator.java:180](file/src/main/java/org/apache/calcite/adapter/file/execution/linq4j/CsvEnumerator.java#L180)
- [CsvEnumerator.java:205](file/src/main/java/org/apache/calcite/adapter/file/execution/linq4j/CsvEnumerator.java#L205)
- [ParquetConversionUtil.java:994](file/src/main/java/org/apache/calcite/adapter/file/format/parquet/ParquetConversionUtil.java#L994)

At runtime, `CsvTypeConverter` falls back to its own hardcoded formatter list
([CsvTypeConverter.java:43-65](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeConverter.java#L43-L65)),
tried in a fixed order that differs from `CsvTypeInferrer`'s list and always tries
`MM/dd/yyyy` before `dd/MM/yyyy`. **A column inference correctly identified as
day-first can be silently reparsed month-first at query time, swapping day and month.**

No existing test catches this: `CsvTypeConverterTest` calls the converter directly with a
fresh formatters map (never through inference), and `CsvEnumeratorDeepCoverageTest`'s
`testEnumeratorWithMixedTypes` only asserts row length, not values.

**Fix:** thread `ColumnTypeInfo.dateTimeFormatter` from `CsvTable.getRowType` through to
the `CsvTypeConverter` construction at all three sites. Add a regression test with a
`dd/MM/yyyy`-only sample (e.g. `25/12/2024`, unambiguous day-first) asserting the actual
`LocalDate` value returned by a real query, not just the declared column type.

### B2. Excel dates are silently destroyed on the way to typed data

Apache POI hands back a real `java.util.Date` for date-formatted cells, and
`ExcelToJsonConverter.getCellValueAsObject`
([ExcelToJsonConverter.java:186-215](file/src/main/java/org/apache/calcite/adapter/file/converters/ExcelToJsonConverter.java#L186-L215))
preserves it correctly as a `Date` object into the intermediate JSON via `putPOJO`.

But that intermediate JSON is written with a bare Jackson `ObjectMapper` — no
`JavaTimeModule`, no date-format configuration — so Jackson's default
`WRITE_DATES_AS_TIMESTAMPS` serializes the `Date` as a raw epoch-millis **number**
(confirmed empirically: `{"d":1785787132716}`). `JsonEnumerator` then reads that number
back and types the column BIGINT. The value that POI already had correctly typed as a
date becomes an opaque large integer with no trace it was ever temporal.

This is worse than "not normalized" — it's normalized *wrong*, destroying information that
was already correct upstream.

**Fix:** configure the Excel→JSON intermediate `ObjectMapper` to serialize `Date`/POI
temporal cell values as ISO-8601 strings (register `JavaTimeModule`, disable
`WRITE_DATES_AS_TIMESTAMPS`), and extend the date-string-to-DATE/TIMESTAMP promotion from
B4/P3 below so those ISO strings become typed columns rather than permanent VARCHAR.

### B3. CSV type inference and the DuckDB execution engine disagree with each other

For `executionEngine: duckdb`, `DuckDBJdbcSchemaFactory` generates
`CREATE VIEW ... AS SELECT * FROM read_csv_auto('<path>')` with no parameters at all
([DuckDBJdbcSchemaFactory.java:1792-1815](file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java#L1792-L1815),
comment at line 1795-1798 confirms this is intentional). This completely bypasses
`CsvTypeInferrer`, `CsvTypeConverter`, and `NullEquivalents` — DuckDB's own sniffer decides
types, null tokens, and date formats independently, using its own rules.

`EngineInvarianceTest`'s javadoc already documents this as "an accepted exception" and the
test fixture deliberately avoids temporal columns to dodge comparing them
([EngineInvarianceTest.java:30-39](file/src/test/java/org/apache/calcite/adapter/file/EngineInvarianceTest.java#L30-L39)),
and even where it does compare, it stringifies (`rs.getObject(i).toString()`) rather than
checking type identity — so a same-string/different-Java-type divergence would pass
unnoticed.

**Decision needed, not just a fix:** either (a) pass the resolved `csvTypeInference`
settings (null tokens, date formats) into `read_csv_auto`'s parameters so both engines
agree, or (b) explicitly scope the type-inference claim to the Enumerable engine and
document that `executionEngine: duckdb` uses DuckDB-native inference instead. Recommend
(a) where DuckDB's parameters allow it (nullstr, dateformat), falling back to (b) with
explicit documentation for whatever DuckDB can't be told to match.

## Coverage gaps (no bug, but "all" is not true)

| # | Gap | Where | Evidence |
|---|---|---|---|
| G1 | PPTX and Markdown table scanners do zero type detection | `converters/PptxTableScanner.java:339`, `converters/MarkdownTableScanner.java:241` | plain `row.put(header, stringValue)`, no call to the shared inference helper HTML/XML/DOCX already use |
| G2 | Date-like strings never promoted to DATE/TIMESTAMP outside CSV and SEC/XBRL | JSON (`JsonEnumerator`), HTML/XML/DOCX (`ConverterUtils.tryParseDateTimeToISO`, [ConverterUtils.java:341-419](file/src/main/java/org/apache/calcite/adapter/file/converters/ConverterUtils.java#L341-L419)) | HTML/XML/DOCX canonicalize to an ISO *string* via `node.put`, still typed VARCHAR downstream; JSON has no date detection at all |
| G3 | Boolean detection is narrow everywhere | `CsvTypeInferrer.BOOLEAN_PATTERN` ([CsvTypeInferrer.java:93](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeInferrer.java#L93)); `ConverterUtils.setJsonValueWithTypeInference` | only `true/false/TRUE/FALSE/True/False/0/1`; no `Y/N`, `yes/no`, `T/F` anywhere |
| G4 | No thousands separators, currency symbols, percent signs, parenthetical negatives, or locale decimal commas in the general CSV inferrer | `CsvTypeInferrer.INTEGER_PATTERN`/`FLOAT_PATTERN` ([CsvTypeInferrer.java:91-92](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeInferrer.java#L91-L92)) | `"1,234"`, `"$1,234.56"`, `"12%"`, `"(123)"` all fail every pattern and force the whole column to VARCHAR; this parsing exists only narrowly inside the SEC/XBRL-specific converter |
| G5 | One dirty value collapses an entire CSV column to VARCHAR | `ColumnTypeTracker.determineType` case 3 ([CsvTypeInferrer.java:522-532](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeInferrer.java#L522-L532)) | any single unparseable value in the sample makes the whole column VARCHAR, even if 999/1000 rows are clean integers |
| G6 | CSV type inference is opt-in, off by default | `TypeInferenceConfig.fromMap` ([CsvTypeInferrer.java:167-184](file/src/main/java/org/apache/calcite/adapter/file/format/csv/CsvTypeInferrer.java#L167-L184)) | nothing is normalized unless a schema explicitly sets `csvTypeInference.enabled: true` |
| G7 | govdata never turns on CSV type inference, anywhere | all 26 `govdata/src/main/resources/**/*.yaml` | `grep -rni csvtypeinference` returns zero hits; govdata instead relies entirely on hand-authored per-column `type:`/`TRY_CAST` expressions, with coverage ranging 1–21 typed columns per schema |
| G8 | Known boolean-shaped flag left as a raw string | `govdata/src/main/resources/cftc/cftc-schema.yaml:847,880,885,894,899` | `cleared` (Y/N) is never cast to BOOLEAN; views compare it as `cleared = 'Y'` string literal |
| G9 | XBRL scale multiplier applied only in a derived view, not the base table | `govdata/src/main/resources/sec/sec-schema.yaml:1738` (`financial_facts` view) vs. `XbrlToParquetConverter.java:1138-1151` (base `financial_line_items` stores `value_numeric` unscaled) | querying `financial_line_items` directly gives unscaled numbers; only `financial_facts` is correct |
| G10 | No general fiscal-period-label → DATE parser | `govdata/src/main/java/.../econ/ItaDataTransformer.java:223` | reformats `"2022-Q1"` → `"2022Q1"` as a string; does not produce a DATE. Only SEC/XBRL gets real period-end date resolution |

## How existing data gets remediated — two independent persistence layers

Every fix below changes what a *parser/converter* does, not just documentation. Anything
already parsed under the old (buggy or incomplete) logic is sitting in one of two caches
that won't notice a code change on its own:

- **Plain `file/` adapter**: converted CSV/JSON/Excel data is cached as a persistent
  `.parquet` file under `.aperio/{schema}/` (`FileSchema.baseDirectory`,
  [file/CLAUDE.md:69](file/CLAUDE.md#L69)). `ParquetConversionUtil.needsConversion`
  reconverts only when the source file's mtime is newer than the cached parquet's
  ([ParquetConversionUtil.java:180-195](file/src/main/java/org/apache/calcite/adapter/file/format/parquet/ParquetConversionUtil.java#L180-L195)) —
  a code fix alone changes nothing for an existing table until that cache is deleted (or
  the source file is touched) to force reconversion. **Remediation = delete the affected
  `.aperio/{schema}/*.parquet` cache entries (or the whole cache directory for a schema)
  after the code fix ships**, no re-fetch of source data needed since the original
  CSV/JSON/Excel file is untouched on disk.
- **govdata**: the raw fetched bytes live in a separate, partition-keyed raw cache that a
  transform/parser code change does **not** invalidate
  (`.claude/skills/data-purge/SKILL.md` — "the raw cache is keyed by partition dims, not
  the URL, so a URL/transformer change does NOT invalidate it"). The `/data-fix` skill
  (`govdata/scripts/parallel/data-fix.sh`) purges only the Iceberg table + tracker
  completion marker and reprocesses from that still-cached raw data by default
  (`--raw false`) — **no re-fetch from the origin API needed for any of the bugs in this
  plan**, since all of them are transform/parse-side, not "the source bytes were wrong."
  `--raw true` (deleting the raw cache too, forcing a re-fetch) is reserved for cases
  where the source data itself changed or the cache is corrupt — none of that applies
  here. There is **no lighter-weight in-place column-type-change tooling** in this
  codebase (no CTAS/ALTER pattern exists for Iceberg tables here) — if a fix changes a
  base-table column's type, `/data-fix` (full table rewrite from raw cache) is the only
  mechanism, unless the fix can instead live in a downstream SQL view (cheaper: no
  purge/reprocess at all, just edit the view).

So for every item below, the choice is: **(a) in-place, no data movement** (the fix only
changes live query-time behavior, e.g. a view expression or an engine parameter — nothing
was ever materialized wrong), or **(b) cache-clear / `/data-fix` reprocess** (the fix
changes what gets written to a persisted cache or Iceberg table, so existing
already-processed rows are still wrong until reprocessed).

## Plan of work

Grouped by blast radius and urgency. Each item below should get its own explicit
go-ahead before implementation, per `file/CLAUDE.md`'s "analyze and present a plan →
request approval → implement" requirement — this document is the "present a plan" step
for the whole area, but the actual diffs still get proposed and confirmed one at a time.

### Phase 1 — Correctness bugs (silently wrong data today)

1. **B1** — thread the inferred `DateTimeFormatter` into `CsvTypeConverter` at all three
   construction sites; add a day-first-only regression test asserting the actual
   `LocalDate` value.
   - **Data remediation: cache-clear, file/ adapter only.** Confirmed not applicable to
     govdata (govdata never enables `csvTypeInference` — see G7/G9 table). Any plain
     `file/`-adapter schema with `csvTypeInference` enabled and an ambiguous date column
     needs its `.aperio/{schema}/*.parquet` cache entry for that table deleted after the
     fix ships, so the next query reconverts with the corrected formatter threading.
     Source CSVs are untouched, so this is a local file delete, not a re-fetch.

2. **B2** — fix Excel→JSON date serialization (register a time-aware Jackson module,
   disable `WRITE_DATES_AS_TIMESTAMPS`); add a test round-tripping an Excel date cell to
   a typed SQL DATE/TIMESTAMP column.
   - **Data remediation: cache-clear, file/ adapter only — confirmed zero govdata
     impact.** Verified no govdata schema routes through `ExcelToJsonConverter`/
     `SafeExcelToJsonConverter`/`MultiTableExcelToJsonConverter`; govdata's `.xlsx`
     sources (energy, housing, transport, research, fiscal) each go through their own
     schema-specific `responseTransformer` classes that call Apache POI directly, bypassing
     the buggy converter entirely. So this bug has **no govdata remediation step at all**
     — only direct `file/`-adapter users pointing at `.xlsx` files need their `.aperio`
     Excel-derived parquet cache cleared after the fix.

3. **B3** — decide DuckDB-engine CSV inference parity (pass inference settings into
   `read_csv_auto`, or explicitly document the divergence).
   - **Data remediation: none — in-place only.** Nothing is materialized by either
     engine path; both read the live CSV per query. Once the decision lands (parameter
     parity or documented divergence), the fix takes effect on the very next query with
     no cache to clear and no reingest.

### Phase 2 — Coverage gaps in `file/` converters

4. **G1** ✅ *(done)* — extended `ConverterUtils.setJsonValueWithTypeInference` to
   `PptxTableScanner` and `MarkdownTableScanner`. Verified zero govdata impact (neither
   class is referenced anywhere under `govdata/`). Regression tests added to both
   `PptxTableScannerTest` and `MarkdownTableTest` proving numeric/boolean values are now
   typed instead of always VARCHAR.
   - **Data remediation: cache-clear.** Same `.aperio` parquet-cache mechanism as B1/B2 —
     clear cached parquet for any PPTX/Markdown-backed table after the fix ships. Not a
     govdata concern (govdata does not ingest via these scanners).

5. **G3** ✅ *(done)* — broadened boolean token recognition (`Y/N`, `yes/no`, `T/F`,
   case-insensitive) in `CsvTypeInferrer.BOOLEAN_PATTERN`, `CsvTypeConverter.parseBoolean`
   (added `t`/`f`; `y`/`n`/`yes`/`no` already existed there), and
   `ConverterUtils.setJsonValueWithTypeInference`. Regression tests added to
   `CsvTypeConverterTest`, `CsvTypeInferrerTest`, and `ConverterUtilsTest`.
   - **Data remediation: cache-clear for file/ adapter tables using affected tokens.**
     For govdata, this Java-level change has no effect on its own since govdata doesn't
     use `csvTypeInference` or `ConverterUtils`-based scraping for its typed columns
     (see G8 below for the govdata-specific instance of this same gap, handled
     separately since it's a hand-authored YAML column, not this shared inference path).

6. **G4** ✅ *(done)* — added a shared `NumericFormats.stripFormatting` helper
   (new file, `format/csv/NumericFormats.java`) used by both `CsvTypeInferrer`
   (inference-time detection) and `CsvTypeConverter` (runtime parsing), so the two
   stay consistent. Strips thousands separators, a `$`/`€`/`£`/`¥` currency symbol,
   and a trailing `%` (stripped, not converted to a fraction — `"12%"` → `12`, not
   `0.12`); converts an accounting-style `"(123)"` into `-123`. Confirmed zero
   govdata impact (govdata never enables `csvTypeInference` and references neither
   `CsvTypeInferrer`/`CsvTypeConverter` nor the new class). Regression tests added
   to `CsvTypeConverterTest` and `CsvTypeInferrerTest`.
   - **Data remediation: cache-clear**, same mechanism as above. No govdata impact
     directly (govdata's own SEC/XBRL-specific parenthetical-negative handling in
     `XbrlToParquetConverter` is untouched by this change — that code path doesn't call
     `CsvTypeInferrer`).

7. **G2** ✅ *(done)* — `JsonEnumerator.deduceRowType` now detects columns whose sampled
   String values are all the same ISO 8601 kind produced by
   `ConverterUtils.setJsonValueWithTypeInference` (plain date, local datetime, or offset
   datetime) or a plain ISO local time, and promotes the column to a real
   `DATE`/`TIMESTAMP`/`TIMESTAMP_WITH_LOCAL_TIME_ZONE`/`TIME` SQL type instead of VARCHAR —
   converting every row's value (not just the sampled ones) to Calcite's internal
   representation (epoch day / epoch millis / millis-of-day), matching
   `CsvTypeConverter`'s conventions exactly. A column is only promoted if *every* sampled
   value matches the *same* ISO kind; any non-string or mismatched-kind value disqualifies
   it back to VARCHAR, and any post-sample row that fails to parse becomes `null` with a
   logged warning (the codebase's existing TRY_CAST-style convention) rather than
   corrupting or crashing. This is the single shared code path used by hand-authored JSON
   files and by every converter that funnels through the intermediate-JSON pipeline
   (HTML/XML/DOCX/PPTX/Markdown), so no per-converter changes were needed.
   - **Data remediation: cache-clear, file/ adapter only — confirmed zero govdata
     impact.** Re-checked the flag from the original audit: govdata's `format: json`
     entries in schema YAML describe the *HTTP response body format* consumed by
     schema-specific `responseTransformer` Java classes (which write explicitly typed
     columns straight to Iceberg/Parquet), not a `file/`-adapter model.json table —
     govdata never constructs a `FileSchemaFactory`-backed JSON table and never
     references `JsonScannableTable`/`JsonTable`/`JsonEnumerator` from `file/`. Same
     "own Java converter, bypasses the shared pipeline" pattern already confirmed for
     B2/G1/G4. Verified via `grep -rln "JsonScannableTable\|JsonEnumerator" govdata/src/main/java`
     (no hits) and by reading a sample `format: json` table definition (cyber-vuln) to
     confirm it's ETL config, not a file-adapter table declaration.
   - Regression tests: `JsonTableDateInferenceTest` (row-type + data-list layer: DATE,
     TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME promotion; mixed-kind and ordinary
     string columns correctly stay VARCHAR) and `JsonDateInferenceJdbcTest` (full
     JDBC round-trip through the Parquet-cache layer, confirming `getDate`/`getTimestamp`
     return correct values end-to-end, not just at the row-type layer).

8. **G5 / C-08** ✅ *(done — supersedes an earlier, wrong resolution)* — `confidenceThreshold`
   was documented and configurable but never consulted: a single unparseable value in the
   sample forced the whole column to VARCHAR regardless of the threshold. It now works as
   documented — the inferrer widens across the non-VARCHAR types present and promotes the
   column when the conforming fraction meets `confidenceThreshold`.
   - **Correction to the record.** I first closed G5 as "no rule-6-compliant way to do this"
     and wrote that into the plan and `csv-type-inference.md`. That was wrong: I had found
     `docs/testing/contradictions.md` C-16 (raise on parse failure) but missed **C-08**,
     which already resolved this exact gap as CODE-WRONG with the instruction to implement
     promotion, and even had a staged `@Disabled` target test waiting
     (`CsvInferenceRequirementsTest.confidenceThresholdPromotesMajority`, now enabled). The
     earlier commit is superseded by this one; the docs claiming promotion is impossible are
     removed.
   - **Parse-failure behavior: null + WARN, and C-16 amended to say so.** Implementing
     promotion forces the question of what a scan does when it reaches one of the tolerated
     non-conforming values. Raising (C-16 as originally written) turned out to be unusable:
     the PARQUET engine materializes the whole file when the table is created, so one bad
     value fails that conversion and `FileSchema` drops the table from the schema entirely —
     every query then fails, including ones that never reference the column (observed:
     `Object 'mostly_integers' not found within 'csv_infer'`). That is strictly worse than
     the nulls it was meant to prevent. C-16 now carries an explicit amendment carving out
     CSV value conversion, with the reasoning: the mismatch is expected by construction —
     either the header declared the type (so the *data* is bad, not the type) or the column
     was promoted under a threshold that tolerates a minority by definition — and the WARN
     names the column and value, so nothing is silent.
   - Fixes C-17 as a side effect: an unparseable DATE used to NPE inside `convert()` (the
     debug log called `getClass()` on a null result). All temporal parsers now raise a clean
     internal `CsvParseException`, which `convert()` turns into the same null + WARN.
   - Regression tests: `CsvInferenceRequirementsTest.confidenceThresholdPromotesMajority`
     (the previously staged C-08 target), `CsvTypeInferenceTest`
     `testMinorityUnparseableValuePromotesColumn` (end-to-end JDBC: 19 clean integers + 1
     garbage value at `confidenceThreshold` 0.9 → INTEGER column, table materializes),
     `ResolvedBugTargetsTest.badNumericValueBecomesNull` and `badDateIsNullNotNpe` (replacing
     the raise/NPE pins, so a future change can't make one bad row remove a table).
   - **Data remediation: cache-clear, file/ adapter only — zero govdata impact** (govdata
     never routes CSV through `CsvTypeInferrer`; see G6). Columns that previously fell back
     to VARCHAR because of a small minority of bad values will now type properly, so affected
     tables need their `.aperio/{schema}/*.parquet` cache entry cleared.

9. **G6** ✅ *(done)* — `csvTypeInference` now defaults to **enabled**. A schema with no
   `csvTypeInference` block (or one omitting `enabled`) gets `defaultConfig()`'s settings;
   `"enabled": false` remains an honored opt-out. Note there were two disagreeing
   "defaults" before this: `defaultConfig()` (javadoc'd as "enabled", but dead code — zero
   production call sites) and `fromMap()`'s absent-config branch (disabled), which is what
   every real schema actually got. `fromMap(null)` now returns `defaultConfig()`, so the
   two can't drift again.
   - **Uncovered and fixed — declared header types were being overridden.** The flip
     initially broke 31 tests. Root cause was a real precedence bug, not stale tests:
     `CsvTable.getRowType` built the row type from the header (honoring Calcite's
     `name:type` convention, e.g. `JOINEDAT:date`) and then *replaced it wholesale* with
     sampled inference. Harmless while inference was opt-in and off; with it on by default
     every explicitly declared type silently became whatever sampling guessed (declared
     `:date` columns came back VARCHAR, so `getDate()` threw "cannot convert to Date").
     Fixed by adding `CsvEnumerator.explicitlyTypedColumns(source)` and skipping inference
     for those columns — a `name:type` header is the author stating the type outright, so
     sampling must never override it (a column declared `:string` to preserve leading zeros
     in zip codes would otherwise be re-inferred numeric). This fixed 29 of the 31.
   - **Uncovered and fixed — a fractional `samplingRate` made schemas non-deterministic.**
     The old `defaultConfig()` sampled at 0.1 via `Math.random()`, fine as an opt-in tuning
     knob for large files but not as a default: the same file could infer *different* column
     types on different runs, and any file with fewer than ~`1/samplingRate` rows routinely
     drew zero rows and silently fell back to all-VARCHAR (this is why the 5-row
     `nulldata.csv` was left untyped). A table's schema must not be a coin flip, so the
     default sampling rate is now 1.0 — every row up to the existing 1000-row
     `maxSampleRows` cap, which stays the (deterministic) way to bound cost on large files.
     Documented the remaining hazard for anyone who still sets a fractional rate explicitly.
   - Docs: `csv-type-inference.md`'s "Migration from Legacy Behavior" now states the new
     default, shows the `"enabled": false` opt-out, and notes the `.aperio` cache-clear
     needed for a table to pick up newly inferred types.
   - Regression tests: `CsvTypeInferenceTest.testInferenceEnabledByDefaultWithNoConfig`
     (no config block at all → INTEGER, using a 300-row fixture so the result can't hinge
     on sampling luck) and `testExplicitlyDisabledInferenceStaysVarchar` (opt-out still
     works); `CsvInferenceRequirementsTest`/`CsvTypeInferrerTest` updated for the new
     defaults; `SqlIntegrationCoverageTest`'s three `nulldata` tests updated because
     `amount` is now genuinely numeric (2 SQL NULLs / 3 non-null) rather than five
     non-null empty strings.
   - **Data remediation: cache-clear, file/ adapter only — confirmed zero govdata impact.**
     Every `file/`-adapter CSV table with no explicit `csvTypeInference` setting will type
     differently on next conversion, so its `.aperio/{schema}/*.parquet` cache entry needs
     clearing to pick the new types up. govdata is unaffected: its only two
     `FileSchemaFactory` usages are a Parquet directory (`SecEmbeddingSchemaFactory`) and
     HTML table scraping (`SecDataFetcher`) — never CSV — and every govdata `.csv` source
     is parsed by its own transformer classes, bypassing `FileSchema`/`CsvTypeInferrer`
     entirely (same pattern already confirmed for B2/G1/G2/G4/G5).

### Phase 3 — govdata-specific fixes

10. **G9** ✅ *(code done; base-table reprocess still pending)* — `value_numeric` now carries
    the value the filer *reported*, with the iXBRL `scale` factor applied at extraction
    (`XbrlToParquetConverter.applyScaleFactor`), instead of the value as *displayed* in the
    document.
    - **Why this was a base-data bug, not a view convenience.** `ix:nonFraction/@scale` says
      the document text is the value divided by 10^scale — a statement "in thousands" tags
      1,234 with `scale="3"` to report 1,234,000. `value_numeric` stored the 1,234. Since
      inline XBRL is mandatory for modern filings, that meant the base column silently mixed
      two different things: the true value (traditional XBRL, no scale) and a display value
      understated by 10^scale (iXBRL). Only `financial_facts.value_dollars` compensated.
    - **A second consumer was already wrong**, which the original audit missed: the
      `revenue_trends` view selects `fli.value_numeric AS revenue` with no scale applied, so
      it under-reported revenue for every iXBRL filer. `edu.md`'s cross-schema example does
      the same. Both become correct with the base-table fix and needed no edit — which is
      exactly the argument for fixing the base column rather than teaching each new view to
      remember the multiplication.
    - Reconciled so scale is applied exactly once: `financial_facts.value_dollars` is now a
      plain alias of `value_numeric` (kept so existing callers keep working) rather than
      re-applying `POWER(10, scale)`. The `scale` column is retained for provenance, and the
      as-displayed text is still in `value`, so nothing is lost.
    - Scaling shifts the decimal exponent via `BigDecimal` rather than multiplying by
      `Math.pow(10, scale)`, so a scaled dollar figure is exact (`1.1` at scale 3 is 1100.0,
      not 1100.0000000000001). Pinned by `XbrlScaleFactorTest`.
    - Updated `docs/requirements/govdata.yaml` GOV-008, whose scenario previously *required*
      callers to compute `value_numeric * POWER(10, scale)` themselves; it now states the
      opposite and warns against double-applying.
    - **Data remediation: still required — the fix only changes newly written data.**
      Existing `financial_line_items` parquet holds unscaled values, so it needs
      `/data-fix --schema sec --table financial_line_items --raw false` (full rewrite from the
      preserved raw XBRL cache, no re-fetch from SEC). Until that runs, the base table and any
      view over it stay understated for iXBRL filings. **Not yet run — needs sign-off**, since
      it rewrites a live table.

11. **G8** ❌ *(rejected — the premise was wrong; no change made)* — the audit recorded
    `cftc.cleared` as a "Y/N flag left as VARCHAR" and proposed casting it to BOOLEAN. It
    isn't a two-state flag. The schema documents three states — `Y=cleared, N=not cleared,
    I=not applicable` ([cftc-schema.yaml:187](govdata/src/main/resources/cftc/cftc-schema.yaml#L187))
    — and the live table confirms `I` is not a rounding error:

    | `cleared` | rows | share |
    |---|---|---|
    | `N` | 182,466,218 | 94.4% |
    | `I` | 10,255,811 | **5.3%** |
    | NULL | 1,903,472 | 1.0% |
    | `Y` | 50,787 | 0.03% |

    `I` covers 10.3M rows — 200x more common than `Y`. There is no lossless BOOLEAN
    mapping: `I` would have to become `false` (asserting the trade was *not cleared*, a
    factual claim the source explicitly declines to make) or `NULL` (conflating "not
    applicable" with the 1.9M genuinely missing values). Either choice destroys a
    distinction the source deliberately encodes, and it would silently corrupt the
    `swap_activity` view, whose `uncleared_count` counts `cleared = 'N'` and would begin
    absorbing those 10.3M rows.

    The column is not mistyped — it is a genuine three-state enum, and VARCHAR represents
    it correctly. **Data remediation: none.** The generalization in the original item
    ("audit other schemas for the same Y/N-as-VARCHAR pattern") is likewise retired as
    stated: a Y/N-looking column is only a boolean if its domain is actually two-valued,
    which has to be checked per column against the data rather than assumed from the name.

12. **G7** — evaluate enabling `csvTypeInference` schema-by-schema for govdata tables
    with no explicit `type:`/`TRY_CAST` override.
    - **Data remediation: `/data-fix` reprocess per affected schema, sequenced after
      Phase 1/2.** Any table that flips this on gets different (better) typing than it
      has today, which is exactly the "already-materialized data is now stale" case —
      each affected table needs `/data-fix --schema X --table Y --raw false` (raw cache
      is XBRL/CSV bytes, untouched by this change, so no re-fetch). Explicitly sequence
      this **after** Phase 1/2 land, since enabling inference before B1/G4/G5 are fixed
      would bake those same bugs into 26 schemas' worth of freshly reprocessed data.

13. **G10** — scope a general fiscal-period-label parser for schemas beyond SEC, if
    warranted by actual query needs.
    - **Data remediation: `/data-fix` reprocess, only if/when built.** Not committing to
      this yet (see decision below) — if built, the same `--raw false` reprocess pattern
      applies per affected schema; flagging now only so the remediation shape is decided
      alongside the "should we build this" decision, not as a separate afterthought.

## Open decisions requiring explicit go-ahead

- **B3 direction**: match DuckDB's inference to the Java inferrer's settings, or document
  and accept the divergence as engine-specific behavior.
- **G6 default flip**: resolved — flipped to enabled-by-default; see the G6 entry above for
  the two real bugs the flip exposed (declared header types being overridden, and
  non-deterministic fractional sampling) and the cache-clear remediation.
- **G5 policy**: resolved — `confidenceThreshold` promotion is implemented per C-08, and
  C-16 is amended so a non-conforming CSV value becomes null + WARN rather than raising (see
  the G5/C-08 entry above). This supersedes my earlier "no rule-6-compliant way" conclusion,
  which was reached without having found C-08.
- **Phase 3 sequencing**: whether to hold G7 (turning on inference in govdata) until
  Phase 1/2 land, as recommended above, or treat them independently.
- **G9/G8 scope**: resolved — remediate the base table via `/data-fix` for both.
  Views are for enrichment (joins, derived labels, cross-references), not for
  simplifying remediation of a base-table correctness bug; a view that only exists to
  paper over an untyped/unscaled base column leaves the underlying defect in place
  indefinitely. `financial_facts` and any `cftc` views may still add real enrichment on
  top of the corrected base columns.
- **G2 govdata exposure**: resolved — confirmed zero govdata usage, same as B2/G1/G4. See
  the G2 entry above for how this was verified.
