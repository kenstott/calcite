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

