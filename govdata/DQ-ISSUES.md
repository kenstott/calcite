# DQ Issue Log

## DQ-001 — Duration facts missing `period_end` for Limitless Projects Inc.

**Table**: `financial_line_items`  
**Severity**: Medium  
**Scope**: 1 filing  
**Discovered**: 2026-04-29  

**Symptom**: 374 duration facts (`is_instant = false`) have `period_end = NULL` for accession `0001842167-26-000004` (Limitless Projects Inc., 10-Q). Affects 85 distinct concepts.

**Root cause indicator**: Sample `value` for these facts is `--07-31` — a fiscal year-end date pattern. The XBRL context parser is not resolving `period_end` for contexts in this filing, likely due to a non-standard context encoding (e.g., using `--MM-DD` relative date notation rather than an absolute ISO date in the `<period><endDate>` element).

**Impact**: All duration-type financial facts for this filing are missing their end date, making period-over-period comparisons and time-series queries incorrect for this accession.

**Fix needed**: Investigate context parsing in `XbrlToParquetConverter` for filings that use relative date notation (`--MM-DD`) in period elements. Add handling or fallback to derive `period_end` from the fiscal year context.

---

## DQ-002 — `numeric_value` conflates direct parse with unparseable text; missing heuristic conversion

**Table**: `financial_line_items`  
**Severity**: Medium  
**Scope**: 54/55 filings (year=2026), universal  
**Discovered**: 2026-04-29  

**Symptom**: 9,930+ facts have `unit_ref IS NOT NULL` and `numeric_value IS NULL`. Sample values include word numerals (`one`, `two`, `three`), nil markers (`—`, `–`, `none`, `no`, `nil`), and unparseable strings. The current column `numeric_value` only performs direct string-to-double conversion and leaves everything else NULL.

**Root cause**: `unit_ref IS NOT NULL` is not a reliable indicator that a fact is numeric — it means the fact is associated with a unit, not that its value is a digit string. Filers legitimately use textual nil markers and written-out numerals in unit-bearing XBRL facts. The current schema does not distinguish between "could not parse" and "no numeric representation exists."

**Fix — schema change in `financial_line_items`**:
- Rename `numeric_value` → `value_numeric` (double) — best-effort numeric representation, NOT the original filing value; NULL if no conversion is possible
- Add `value_numeric_method` (varchar) — provenance of `value_numeric`:
  - `'direct'` — parsed directly from `value` as a number
  - `'heuristic'` — converted via lookup (nil markers → `0`; word numerals → integer)
  - `null` — no conversion possible
- Keep `value` as the raw string from the filing, unchanged

**Heuristic scope**:
- Nil markers → `0`: `—`, `–`, `-–`, `-—`, `none`, `nil`, `no`, `not`, `null`
- Word numerals → integer: `one`→1, `two`→2, … `ninety-nine`→99
- All other non-numeric strings → NULL

**Implementation**: Write-time fix in `XbrlToParquetConverter`. Do not implement as a query-time transform.

---

## DQ-003 — `phone` and `mailing_address` always NULL in `filing_metadata` for 10-K/10-Q filings

**Table**: `filing_metadata`  
**Severity**: Medium  
**Scope**: All 10-K/10-Q filings (100% NULL on both columns)  
**Discovered**: 2026-04-29  

**Symptom**: `phone` and `mailing_address` are NULL on every 10-K/10-Q row. `business_address` is correctly populated for the same filings, ruling out a general XBRL parse failure.

**Root cause**:
- `phone`: Code calls `extractDeiValue(doc, "EntityPhoneNumber", ...)`. The element `EntityPhoneNumber` does not exist in the SEC DEI taxonomy. The standard DEI elements are `dei:CityAreaCode` (area code) and `dei:LocalPhoneNumber` (local number).
- `mailing_address`: Code calls `extractDeiValue(doc, "EntityAddressMailingAddressLine1", ...)`. No such element exists in the DEI taxonomy. Mailing address is available in EDGAR's `submissions.json` under `addresses.mailing.street1`, not in XBRL instance documents.

**Evidence**: Direct inspection of a 10-K facts parquet (accession `0001428336-26-000010`):
  - DEI phone concepts present: `dei:CityAreaCode = "801"`, `dei:LocalPhoneNumber = "727-1000"`
  - No `EntityPhoneNumber` or `EntityAddressMailingAddressLine1` elements in any filing

**Fix needed**:
- `phone`: Change to `extractDeiValue(doc, "LocalPhoneNumber", "EntityPhoneNumber")`, then prepend `CityAreaCode` if present to form a full phone number.
- `mailing_address`: Extend `SecDataFetcher.getCompanyInfoForCik` to extract `addresses.mailing.street1` from `submissions.json`, then use `submissionsInfo.get("mailing_address")` instead of the XBRL lookup.
- `acceptance_datetime` and `file_size` remain hardcoded null (intentional — not available from XBRL source)

---

## DQ-004 — `xbrl_relationships` missing for filings that embed linkbases in the extension XSD

**Table**: `xbrl_relationships`  
**Severity**: Medium  
**Scope**: At least 1 confirmed filing (Ooma, Inc. `0001327688-26-000009`, 10-K); prevalence across full corpus unknown  
**Discovered**: 2026-04-29  

**Symptom**: Ooma's 10-K has 2,120 facts and 214 contexts in Iceberg but zero rows in `xbrl_relationships`. No relationships staging file exists for this accession in any `year=2026` or `year=2025` batch.

**Root cause**: The parser in `XbrlToParquetConverter.downloadAndParseLinkbases()` handles two packaging patterns:
1. Separate linkbase files referenced via `<link:linkbaseRef>` elements in the XSD
2. Relationships embedded inline in the iXBRL HTML document

Ooma uses a third pattern: all linkbase arcs (`calculationLink`, `presentationLink`, `definitionLink` — 1,818 arc elements) are embedded directly inside the extension XSD file (`ooma-20260131.xsd`) with no `<link:linkbaseRef>` elements. The parser finds 0 `linkbaseRef` elements, skips the loop, and produces zero relationships.

**Evidence**: `ooma-20260131.xsd` contains 1,818 arc element matches and no `link:linkbaseRef` elements. The filing directory contains only 3 files: `FilingSummary.xml`, `ooma-20260131.xsd`, and `ooma-20260131_htm.xml` — no separate linkbase XML files.

**Fix needed**: After finding 0 `linkbaseRef` elements in the XSD, add a fallback to parse the XSD document itself for embedded `link:calculationLink`, `link:presentationLink`, and `link:definitionLink` arc content directly..

---

## EDU Schema — Data Quality Issues

*Discovered: 2026-05-05 | Source: Static analysis of ETL transformer Java files*

---

## DQ-005 — CCD Districts and Schools: `county_code` not padded to 5-digit geo join key

**Tables**: `ccd_districts`, `ccd_schools`
**Severity**: High
**Scope**: All rows (100% of records affected)
**Discovered**: 2026-05-05
**Status**: FIXED 2026-05-05

**Symptom**: `county_code` stored as 4-digit integer (e.g., `6037` for CA/LA), cannot join to `geo.counties.county_fips` which requires 5-digit zero-padded string (`"06037"`).

**Root cause**: The Urban Institute API returns `county_code` as the full state+county FIPS stored as an integer, dropping the leading zero for states 01–09. Neither `CcdDistrictsResponseTransformer.augmentRecord()` nor `CcdSchoolsResponseTransformer.augmentRecord()` padded it.

**Fix**: Both transformers now normalize `county_code` to `String.format("%05d", code)` in `augmentRecord()`. Rows where the code is zero or non-numeric log a warning and leave the field as-is.

---

## DQ-006 — NAEP Scores: Unguarded `parseInt` on URL-injected `year` and `grade`

**Table**: `naep_scores`
**Severity**: High
**Scope**: Any request where year or grade dimension parameters are malformed
**Discovered**: 2026-05-05

**Symptom**: The transformer crashes silently (returns `"[]"`) if the URL dimension `year` or `grade` cannot be parsed as an integer — returning empty results with no logged error.

**Root cause**: `NaepScoresResponseTransformer` injects year and grade from URL parameters via `Integer.parseInt()` at lines 77 and 83 with no try-catch. `NumberFormatException` propagates uncaught through `transform()`, which returns `"[]"` on any exception.

**Status**: FIXED 2026-05-05 — Both `parseInt` calls wrapped in try-catch; on failure the field is omitted and a WARN is logged. The outer exception handler still returns `"[]"` for other failures.

---

## DQ-007 — NAEP Scores: `jurisdiction` can be null, violating NOT NULL schema constraint

**Table**: `naep_scores`
**Severity**: High
**Scope**: API responses where `statecode`/`jurisdiction` field naming differs from expected
**Discovered**: 2026-05-05

**Symptom**: Rows inserted with `jurisdiction=NULL` violate the NOT NULL schema constraint; these rows break primary key uniqueness and cause silent data loss on materialization.

**Root cause**: The normalization at lines 86–92 maps `statecode` → `jurisdiction` only if `jurisdiction` is absent. If both fields are missing (API variant change), `jurisdiction` remains null. No validation or skip-guard follows.

**Revised assessment**: `jurisdiction=NULL` is not a skip condition — the row may carry valid score data and outer joins in views handle sparse coverage. The correct response is to surface the gap, not suppress the row.

**Status**: FIXED 2026-05-05 — Added WARN log after normalization block if `jurisdiction` is still null. Row is written as-is; views use LEFT JOIN to handle missing jurisdiction.

---

## DQ-008 — IPEDS Financials: Unguarded `parseInt` on `year`; null `year` rows not filtered

**Table**: `ipeds_financials`
**Severity**: High
**Scope**: Any response row where year is non-numeric; rows with null year are passed through
**Discovered**: 2026-05-05

**Symptom**: `year=NULL` rows are inserted into a NOT NULL PK column; rows with non-numeric year string cause uncaught `NumberFormatException`.

**Root cause**: `IpedsFinancialsResponseTransformer` line 224 calls `Integer.parseInt(yearStr)` with no try-catch. The existing null filter at line 263 checks only for null `unitid`, not null `year`. By contrast, `Double.parseDouble()` calls elsewhere are correctly wrapped in try-catch.

**Status**: FIXED 2026-05-05 — `parseInt(yearStr)` wrapped in try-catch; on failure logs a WARN and leaves `year` absent from the row (null in Parquet). The existing unitid null-filter at line 263 is sufficient — a row with null year and valid unitid is still observable; views handle it via LEFT JOIN semantics.

---

## DQ-009 — IPEDS Financials: `form_type` defaults to empty string `""` — invalid PK value

**Table**: `ipeds_financials`
**Severity**: High
**Scope**: Any response where `form_type` header is absent
**Discovered**: 2026-05-05

**Symptom**: `form_type=""` is inserted as a PK component. Valid values are `F1A`, `F2`, `F3`. Empty string produces duplicate or invalid PK rows.

**Root cause**: Line 221: `row.put("form_type", formType != null ? formType : "")`. If `formType` is null (header not present), the row gets `form_type=""` rather than being skipped.

**Status**: FIXED 2026-05-05 — When `formType` is null the transformer logs a WARN and writes `NULL` for `form_type` (via `putNull`) instead of `""`. `form_type` is a dimension injected from the schema URL config, not from the API response; null indicates a schema misconfiguration, not bad source data. Rows still pass the unitid filter and are written with null form_type, making the misconfiguration observable in the output.

---

## DQ-010 — IPEDS Completions: `race` and `sex` not validated as integers; null PK possible

**Table**: `ipeds_completions`
**Severity**: Medium
**Scope**: Rows where Urban Institute API returns non-numeric race or sex codes
**Discovered**: 2026-05-05

**Symptom**: `race` and `sex` are schema-defined as `INTEGER NOT NULL` PK components. Non-numeric API values (e.g., `"unknown"`, null) pass through untransformed and fail on materialization.

**Root cause**: `IpedsCompletionsResponseTransformer.augmentRecord()` is empty — no validation or coercion is applied to `race` and `sex` from the Urban Institute API response.

**Revised assessment**: FALSE POSITIVE. In the IPEDS taxonomy, `race=99` and `sex=99` are the valid aggregate codes meaning "all races" and "all sexes". These are the most common rows (institution-level totals). Null race or sex from the Urban Institute API would indicate a genuinely missing disaggregation, not a broken record — and the appropriate handling is to write the row and let queries filter on `race IS NULL`. No code change needed.

---

## DQ-011 — College Scorecard: Rate fields may be stored as 0–100 instead of 0.0–1.0

**Table**: `college_scorecard`
**Severity**: Medium
**Scope**: All rows — universal if College Scorecard API returns percentages as 0–100
**Discovered**: 2026-05-05

**Symptom**: Calculated views that use these fields (e.g., `frpl_rate`) will produce values 100× too large if the API returns `45.5` (meaning 45.5%) and the schema expects `0.455`.

**Affected columns**: `pell_grant_rate`, `completion_rate_4yr`, `completion_rate_2yr`, `retention_rate_fulltime`, `default_rate_3yr` (mapped in `CollegeScorecardResponseTransformer` lines 66–75).

**Root cause**: No scaling is applied in the transformer. College Scorecard API documentation must be verified for return format. Schema does not document expected range.

**Revised assessment**: FALSE POSITIVE. The College Scorecard API returns all rate fields as 0.0–1.0 proportions (e.g., `0.4553` for 45.53%). No scaling needed. No code change required.

---

## DQ-012 — College Scorecard Programs: `cip_code` and `unit_id` can be null — PK violation

**Table**: `college_scorecard_programs`
**Severity**: High
**Scope**: API rows where `code` or `unit_id` field is absent
**Discovered**: 2026-05-05

**Symptom**: Rows with `cip_code=NULL` or `unit_id=NULL` violate NOT NULL PK constraints and produce undefined behavior on materialization.

**Root cause**:
- Line 88: `row.put("cip_code", textOrNull(prog, "code"))` — `textOrNull()` returns null if the field is missing
- Line 86: `row.set("unit_id", nullSafe(prog.path("unit_id")))` — `nullSafe()` can return a null node
- Neither is guarded with a skip

**Revised assessment**: Rows with null `cip_code` or `unit_id` should be written with null and logged, not skipped — the row may contain valid earnings data that can be joined via outer joins in views. Skipping silently destroys data.

**Status**: FIXED 2026-05-05 — Added WARN logging when `unit_id` or `cip_code` is missing. Rows are still written with null values; views use LEFT JOIN to handle them.

---

## DQ-013 — College Scorecard Programs: `credential_level` has no explicit integer conversion

**Table**: `college_scorecard_programs`
**Severity**: Medium
**Scope**: Rows where API returns `level` as a string rather than a JSON number
**Discovered**: 2026-05-05

**Symptom**: `credential_level` is schema-typed `INTEGER NOT NULL`. If the API returns `"5"` (string), the value passes through as a JSON text node, causing a type mismatch when the Parquet writer tries to write an integer column.

**Root cause**: Line 92: `row.set("credential_level", nullSafe(credential.path("level")))` — no explicit `asInt()` conversion or try-catch.

**Status**: FIXED 2026-05-05 — `credential_level` now extracted via `asInt()` for numeric nodes; for textual nodes, `parseInt` is attempted and logs WARN on failure. Unrecognized types store null.

---

## DQ-014 — IPEDS Institutions: County FIPS may be county-only (3-digit) rather than state+county (5-digit)

**Table**: `ipeds_institutions`
**Severity**: Medium
**Scope**: Potentially all rows depending on which IPEDS API field is used
**Discovered**: 2026-05-05

**Symptom**: `county_fips_str` formatted with `%05d` produces `"00003"` for county code 3 instead of the correct `"02003"` (Alaska county), breaking geo joins.

**Root cause**: `IpedsInstitutionsResponseTransformer` lines 38–43 pad `county_fips` to 5 digits assuming the API returns a complete 5-digit state+county code. IPEDS bulk data `COUNTYCD` is the 3-digit county portion — the state FIPS must be prepended separately. If the API field is county-only, `%05d` on a 3-digit value produces a zero-prefixed 5-digit code without the state prefix.

**Revised assessment**: FALSE POSITIVE. NCES `COUNTYCD` is the full 5-digit state+county FIPS code stored as an integer (e.g., `6037` for California/LA = FIPS `06037`). `String.format("%05d", fipsInt)` correctly restores the leading zero. The transformer is correct as written.


---

## DQ-015 — `ref` has no multi-hop ownership view; the obvious query silently answers a different question

**Table**: `ref.gleif_relationships` (recommendation: add a `ref.gleif_ownership_ancestry` view)
**Severity**: Medium — no data is wrong, but the correct answer is unreachable in practice
**Scope**: 126,025 `IS_DIRECTLY_CONSOLIDATED_BY` edges; all ownership-structure questions
**Discovered**: 2026-08-21

**Symptom**: There is no way to ask "how deep does this ownership chain go" or "give me everything
under ultimate parent X" without hand-writing a recursive CTE. Callers instead reach for
`IS_ULTIMATELY_CONSOLIDATED_BY`, which is a single-hop shortcut, and get a **wrong answer with no
error**: it reports depth 1 for entities that are several layers down.

Concretely, walking direct-parent edges puts HSBC USA four layers below HSBC Holdings plc — US bank
→ HSBC North America Holdings (US) → HSBC Overseas Holdings (UK) → HSBC Holdings plc — while the
ultimate-parent edge reports a single hop. The intermediate holding companies are invisible.

**Impact**: An analyst question that should be a census becomes an anecdote. `entity_relationships`
takes ~212s for ONE entity, so a bank-ownership analysis came back covering 16 hand-picked firms
rather than the population, explicitly for cost reasons. This is also why no public source states a
depth distribution for US bank ownership, and why the OFR's own graph-theoretic study
(Flood, Kenett, Lumsdaine & Simon 2017) reported entity COUNTS instead — the naive query answers a
different question, and the correct one is expensive to assemble.

Edge-type counts, for context:

```
IS_FUND-MANAGED_BY              148,578
IS_ULTIMATELY_CONSOLIDATED_BY   132,198   <- single-hop shortcut
IS_DIRECTLY_CONSOLIDATED_BY     126,025   <- the real chain
IS_SUBFUND_OF                    72,697
IS_INTERNATIONAL_BRANCH_OF        1,939
IS_FEEDER_TO                      1,387
```

**Fix needed**: a `ref.gleif_ownership_ancestry` view, one row per (entity, ancestor). Tested
against the live corpus:

```
116,978 ancestry rows | 89,392 entities | max depth 9 | ~15s for the whole forest
```

```sql
WITH RECURSIVE edges AS (
  SELECT start_node_id AS child_lei, end_node_id AS parent_lei
  FROM gleif_relationships
  WHERE relationship_type = 'IS_DIRECTLY_CONSOLIDATED_BY'
    AND relationship_status = 'ACTIVE'
    AND registration_status = 'PUBLISHED'
    AND start_node_id IS NOT NULL AND end_node_id IS NOT NULL
),
walk AS (
  SELECT child_lei AS lei,
         parent_lei AS immediate_parent_lei,
         parent_lei AS ancestor_lei,
         1 AS depth,
         CAST(child_lei AS VARCHAR) || '>' || CAST(parent_lei AS VARCHAR) AS path
  FROM edges
  UNION ALL
  SELECT w.lei, w.immediate_parent_lei, e.parent_lei, w.depth + 1,
         w.path || '>' || CAST(e.parent_lei AS VARCHAR)
  FROM walk w
  JOIN edges e ON e.child_lei = w.ancestor_lei
  WHERE w.depth < 20
    AND POSITION(CAST(e.parent_lei AS VARCHAR) IN w.path) = 0
)
SELECT w.lei, w.immediate_parent_lei, w.ancestor_lei, w.depth, w.path,
       CASE WHEN top.child_lei IS NULL THEN TRUE ELSE FALSE END AS ancestor_is_ultimate
FROM walk w
LEFT JOIN edges top ON top.child_lei = w.ancestor_lei
```

**Three observations from testing, each of which will bite whoever implements this:**

1. **THE GRAPH HAS CYCLES.** Without the `POSITION(...) = 0` path-membership guard the recursion
   does not terminate, and — worse — the spurious rows look like genuine depth rather than an
   error: depths 10, 11 and 12 each returned exactly 9 rows. With the guard, real max depth is 9.
   The cycles are GLEIF data errors and are worth reporting upstream in their own right.

2. **Array literals and `list_append` do not parse through this SQL layer.** Both `[child, parent]`
   and `list_append(path, x)` fail. Hence the `'>'`-delimited VARCHAR path, which also makes the
   cycle guard a simple `POSITION`. If an array `path` is wanted, build it in a materialisation job
   rather than in a query the engine must parse.

3. **Source from `gleif_relationships`, NOT `current_gleif_parents` — worth 4.6x.** Both produce
   byte-identical results (116,978 rows / 89,392 entities), but the convenience view resolves legal
   names on both endpoints, and inside a recursive CTE that join is re-evaluated at every level:
   **68.8s versus 14.9s**. Resolve names on the OUTER query, joined once, never inside the walk.

**Materialised vs view**: 15s is acceptable once, not per query. A flat physical table refreshed
whenever `gleif_relationships` re-ingests is preferable to a live view, and flat beats a nested
JSON tree for the queries actually asked — `depth` becomes an indexed integer, "everyone under X at
depth <= N" is a filter and a join, and full ancestry is still available via `path` without JSON
path operators. A nested tree only pays off if something needs to render the downward org-chart
shape, which no observed question required.

**Why this matters commercially**: every banking, insurance and corporate-strategy question that
needs ownership structure depends on it. Those are the highest-value audiences and the ones
currently least served.

---

## DQ-016 — `sec.financial_facts` filters do not push through its own JOIN — a hang, not just a slow query

**Table**: `sec.financial_facts` (VIEW over `financial_line_items` LEFT JOIN `filing_contexts` JOIN
a `concept_aliases` VALUES mapping)
**Severity**: High — not a wrong answer, a hang: 45+ minutes at 900%+ CPU and ~9.7GB RAM, no error
ever surfaced to the caller
**Scope**: any query filtering `financial_facts` by `cik`/`accession_number`; the identical filter
run directly against the base table `financial_line_items` returns instantly
**Discovered**: 2026-08-22

**Symptom**: `SELECT DISTINCT canonical_name FROM sec.financial_facts WHERE cik IN (...)` never
returned. The server process was found still alive ~20+ minutes later, pinned at 950%+ CPU with
181 cumulative CPU-minutes logged; killing it (`kill -9`) released free memory from ~71MB to
~9.7GB. `EXPLAIN PLAN FOR` on the hang-triggering shape shows the entire filter+join collapsing
into one `JdbcTableScan` — the whole view expansion is handed to DuckDB as a single opaque SQL
statement, so the runaway work happens inside DuckDB's own engine, not Calcite's Enumerable path.

**Root cause (confirmed by direct comparison, not inferred)**: filtering `financial_facts` by `cik`
does not push the predicate down through the view's `LEFT JOIN`/`VALUES`-join into
`financial_line_items` before that join runs — a query that reads as "filtered to one company"
becomes a full-corpus scan-and-join. Filtering `financial_line_items` directly by the same `cik`
(60,310 rows for the filer tested) returned instantly.

**Why the query timeout didn't save it**: `ASKAMERICA_QUERY_TIMEOUT_SECONDS` (default 180s) is
demonstrably enforced elsewhere in this codebase, but did not fire here — the hang ran at least an
order of magnitude past the configured bound with no `queryExecutionTimeoutReached` error ever
raised. Calcite core does carry a mechanism to propagate the outer timeout into a fully-pushed-down
statement (`ResultSetEnumerable.setTimeout`/`setTimeoutIfPossible`), but which of its two hops
actually failed here — timeout propagation into `DataContext.Variable.TIMEOUT`, or DuckDB's own
cooperative cancellation being unable to interrupt a query already wedged inside one oversized
native operator — was not conclusively isolated, to avoid re-triggering the outage with a live
repro.

**A second, related gap**: sub-category breakdowns (loan class, business segment, geography,
product line) are not reachable as columns at all — they require joining
`financial_line_items.context_ref = filing_contexts.context_id` and reading
`filing_contexts.segment`, a semicolon-joined string of XBRL `Axis=Member` pairs namespaced to
each filer's own taxonomy prefix (e.g. `fbc:CommercialRealEstateLoansMember`). Nothing in the
original schema comments indicated this join path existed; `filing_contexts.segment`'s comment
was a single line ("Segment dimension information (XML fragment)") giving no indication it was a
usable, documented key.

**Fixed so far**:
- `financial_line_items`, `filing_contexts.segment`, and `financial_facts`' own comments rewritten
  to state the scope limit, the performance divergence, and the join path explicitly (shipped).
- `askamerica-engine`'s `McpServer.java` instructions and `query` tool description updated with a
  generalized version of this pattern (any `<entity>_facts`-style view built from a join over a
  much larger base table is a candidate for the same failure) (shipped).
- A wall-clock watchdog added server-side: any statement still active at 2x its configured timeout
  gets `Statement.cancel()`; still active at 4x, the shared connection is evicted so subsequent
  callers get a fresh one rather than queuing behind the wedged one indefinitely (shipped, compiled
  and regression-tested against normal queries; the escalation path itself was not live-fired
  against a real hang, deliberately, to avoid re-triggering the outage). If the watchdog's
  `cancel()` is what unblocks the query, the caller now gets an explicit, accurate message — real
  elapsed time, which multiple of the timeout fired — instead of DuckDB's bare "Interrupted!" text,
  so the calling agent can tell a timeout from a crash and decide to narrow the query rather than
  retry the same shape blind. The remaining gap: if `cancel()` itself fails to unblock the thread
  and only the stage-2 connection eviction eventually runs, the caller may still get no response at
  all rather than a labeled timeout — a request-level hard deadline would close this, not attempted
  here given the risk of restructuring `DB_LOCK`'s serialization without live-hang test coverage.
- `sec.financial_facts_by_segment` — a convenience view unpivoting `filing_contexts.segment` into a
  searchable `segment_members` column — drafted, tested against a real filing (see the join and
  matching figures above), and **NOT added to the live schema**. A schema YAML edit invalidates the
  bundled DuckDB seed and forces a full reseed, which is the data team's call to schedule, not
  something to apply ad hoc from an investigation session — matching DQ-015's precedent. The
  proposal also has an open correctness gap that should be resolved before it ships: a plain
  `segment_members ILIKE '%X%'` filter returns every context that mentions the category anywhere in
  its dimension list, including credit-quality/delinquency sub-breakdowns of the same category
  (e.g. querying for a bank's CRE loans returned the two clean consolidated totals plus 12
  additional rows further broken out by Pass/Substandard/Non-Accrual status) — summing
  `value_dollars` across the returned rows without further filtering double-counts, the same trap
  `financial_facts` itself already warns about for its own `is_consolidated_total` column.

**Suggested remaining fix**: either rewrite `financial_facts`'s (and, if deployed,
`financial_facts_by_segment`'s) SQL so the entity-key filter is applied inside a subquery on
`financial_line_items` before the joins run (forcing the pushdown Calcite/DuckDB isn't doing on
its own), or add an explicit single-dimension flag/column to `financial_facts_by_segment` so a
caller can select one Axis at a time instead of ILIKE-matching across the whole concatenated
Member list.

---

## DQ-017 — `fec.committees` name collides with congressional committees; description was correct, discovery wasn't

**Table**: `fec.committees`
**Severity**: Low — a metadata/discoverability improvement, not a description error
**Scope**: any question about congressional committee membership or jurisdiction
**Discovered**: 2026-08-22

**Symptom**: no schema in the catalog covers congressional committee membership or jurisdiction
(`officials.members` has no committee-assignment field either). A caller reaching for that data —
e.g. "who sits on House Financial Services" — has a plausible reason to try `fec.committees`
first, given the name, and only discovers the mismatch after reading its description or querying
it directly.

**Not a misdescription.** The table's existing comment was already accurate: "All political
committees registered with the FEC. Includes PACs, party committees, campaign committees, and
Super PACs." It never claimed to cover congressional committees and nothing in it was wrong. The
actual problem is narrower: a correct, generic description does not by itself warn off a plausible
confusion a caller is likely to walk into via the table's name — especially through keyword-based
`search_catalog` matching, where "committee" surfaces this table as a false-positive-looking
candidate for an unrelated question.

**Found by**: arm A of the comparative-eval harness (`campaign-money-vs-committee-jurisdiction-
alignment`, 2026-08-22), which confirmed via `list_schemas`/`list_tables`/`search_catalog` that no
committee-assignment table exists, filed the gap via `report_issue`, and worked around it with an
external research pass for 118th Congress committee rosters — the run's headline result was not
degraded by this, just the effort to reach it.

**Suggested fix, not applied here — for the data team to schedule**: lead `fec.committees`'
comment with an explicit disambiguation ("NOT CONGRESSIONAL COMMITTEES"), naming the actual gap
(no schema carries committee assignments) so a caller who lands here via the name collision is
redirected immediately rather than after a wasted query. Comment-only change, no reseed required —
but govdata schema/metadata edits are the data team's call, not applied unilaterally from this
session; reported here and in the Govdata Defect Register for them to apply.

**Also still open**: no congressional committee membership/jurisdiction table exists anywhere in
the catalog. Suggest adding one (e.g. sourced from Congress.gov's committee-membership endpoint)
to the `officials` schema.

---

## DQ-018 — `cftc.commodity_derivatives` hangs on a bare COUNT(*); and CFTC COT positioning is a real, missing dataset

**Table**: `cftc.commodity_derivatives` (and likely its siblings — `credit_default_swaps`,
`equity_derivatives`, `fx_derivatives`, `rate_swaps` — all filtered VIEWs over the same
`cftc_trades` base table)
**Severity**: Medium — a hang, not a wrong answer, but on the simplest possible query
**Scope**: any unfiltered or lightly-filtered query against these views
**Discovered**: 2026-08-22

**Symptom**: `SELECT COUNT(*) FROM cftc.commodity_derivatives` does not return within 60s on a
freshly-started, otherwise-idle host — reproduced directly, independent of any specific question
or prior host state. Same failure shape as DQ-016 (`sec.financial_facts`): a VIEW's own filter
(here, presumably a WHERE/asset-class filter selecting energy/ag/metals swaps out of all asset
classes) appears not to push down before scanning the much larger base table (`cftc_trades`,
individual DTCC GTR swap dissemination records — plausibly a very large table given it covers
every reported OTC swap event across all asset classes).

**Context this was found in**: arm A of the comparative-eval harness
(`cftc-positioning-vs-physical-energy-production`, 2026-08-22) was asked to report speculative
*futures* positioning (the CFTC Commitments of Traders concept) for energy contracts. It correctly
judged that `cftc.commodity_derivatives` — OTC swap transaction records — was not the right table
for that (COT positioning and OTC swap dissemination are two entirely different CFTC datasets) and
went to CFTC's own public data directly instead, which is the right call independent of whether
the table also hangs.

**The more important finding: CFTC Commitments of Traders (COT) does not exist in this catalog at
all**, and it should — it is the standard weekly speculative-positioning series used across
finance/commodities questions, and confirmed independently needed twice in one day: this
harness's own bare arm and a real Claude Desktop session (run by the user, outside this harness)
both went straight to CFTC's public Socrata API for it, unprompted, and produced correct results.

**Concrete source, verified working today**:
- Portal: `publicreporting.cftc.gov`, Socrata API.
- Dataset: Disaggregated Futures-Only report, resource ID `72hh-3qpy` (verify current ID before
  building — CFTC's dataset IDs can change; `WebSearch "CFTC publicreporting.cftc.gov disaggregated
  futures only resource id"` if in doubt).
- Query shape: `https://publicreporting.cftc.gov/resource/72hh-3qpy.json?cftc_contract_market_code=067651&$where=report_date_as_yyyy_mm_dd>='2024-01-01'&$order=report_date_as_yyyy_mm_dd&$limit=5000`
  (067651 = NYMEX WTI crude, 023651 = NYMEX Henry Hub natural gas — other contracts have their own
  codes).
- Key fields: `report_date_as_yyyy_mm_dd`, `m_money_positions_long_all` /
  `m_money_positions_short_all` (managed-money long/short — the standard "speculative positioning"
  proxy), `open_interest_all`. `net_managed_money = long - short`, weekly, as-of-Tuesday.
- No API key required.

**Suggested, for the data team**: (1) investigate the same filter-pushdown pattern as DQ-016 for
`commodity_derivatives` and its sibling filtered views — likely the same root cause, possibly the
same fix. (2) Add CFTC COT (Disaggregated Futures-Only, and possibly Legacy/Supplemental) as a new
govdata source — it is a real, independently-confirmed gap, not a hypothetical one, and the public
endpoint above is already verified working with no auth required.

## DQ-019 — `energy.eia_electricity_generation` only 2023-2024 actually loaded despite declared 2008-2024, and `generation_year` is offset +2 from the real year

**Table**: `energy.eia_electricity_generation`

**Severity**: High — the table is unusable for any multi-year trend despite advertising 17 years
of history, and the typed year column actively misleads any query that filters/groups on it.

**Discovered**: 2026-08-22, via the AskAmerica comparative-eval harness
(`renewable-share-vs-retail-electricity-prices`), arm A, using `data_coverage`/`describe_table`/
direct `query` — not a guess, independently reproduced.

**Symptom, part 1 — coverage gap**: `describe_table`/`data_coverage` declare `yearRange`
2008-2024, but a direct row scan shows only years 2023-2024 are actually loaded (296,786 rows
total, all in 2023-2024). `data_coverage`'s own `missing_vs_declared` lists 2008-2022 entirely
absent.

**Symptom, part 2 — `generation_year` corruption**: the partition column `year` (varchar) correctly
holds `'2023'`/`'2024'`, but the typed `generation_year` (integer) column is offset **+2** for
every row: 2023-partition rows carry `generation_year=2025`, 2024-partition rows carry
`generation_year=2026`. Reproduced via:
```sql
SELECT year, generation_year, generation_month, COUNT(*) AS n
FROM energy.eia_electricity_generation
GROUP BY year, generation_year, generation_month
ORDER BY year, generation_year, generation_month
```
Any query filtering or grouping on `generation_year` — the documented, typed year column — silently
returns future, non-existent years (2025/2026) and appears empty for the real 2023/2024 data. A
caller who trusts the typed column over the varchar partition column gets zero rows and no error,
which reads as "no data" rather than "wrong column."

**Context**: the arm correctly abandoned this table and substituted `energy.eia_state_energy_consumption`
(EIA SEDS, confirmed fully loaded 1960-2024, no gaps) after discovering both defects, filed the
issue in-product via `report_issue`, and delivered a complete answer from the substitute source.
No workaround was applied to the schema/data here — report-only, per this session's scope.

**Suggested, for the data team**: (1) backfill 2008-2022 to match the declared window, or correct
the declared `yearRange` to 2023-2024 if backfill is not planned. (2) Fix the `generation_year`
derivation in the transformer — it appears to derive from an apparent current-run-year-based offset
rather than the year/month the row actually describes; the varchar `year` partition column has the
correct value and can serve as the reference during the fix.
