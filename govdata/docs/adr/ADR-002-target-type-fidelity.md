# ADR-002: Target Type Fidelity

**Status:** Accepted  
**Date:** 2026-05-21

## Context

Government data sources routinely publish numeric and temporal values as strings — pipe-delimited FEC bulk files, CMS CSV downloads, Medicaid JSON APIs. When ETL ingests these values without casting, Iceberg stores them as VARCHAR. Downstream consequences:

- Aggregates (`SUM`, `AVG`) silently serialize as JSON strings (`"3.1906808E8"`) instead of numbers, breaking LLM numeric reasoning.
- `COUNT(*)` returns `"1100"` instead of `1100`.
- `WHERE year = 2024` fails; the user must write `WHERE "year" = '2024'`.
- Analytic functions (`ORDER BY total_amount DESC`) sort lexicographically, not numerically.

The root cause was traced to two places: (1) schema YAML column types left as `string`/`VARCHAR` when the source value is numeric, and (2) `McpServer.query()` calling `val.toString()` for every column regardless of JDBC type (fixed in `numeric-types-v9`).

## Decision

**All target model column types must use the most specific type the data supports:**

| Category | Preferred type in YAML | Iceberg physical type |
|---|---|---|
| Whole counts, IDs that fit 32-bit | `integer` | INT |
| Large IDs, row counts | `long` | LONG |
| Dollar amounts, rates, ratios | `double` | DOUBLE |
| High-precision financials (>15 sig digits) | `decimal(p,s)` | DECIMAL |
| Calendar date (no time) | `date` | DATE |
| Timestamp with time zone | `timestamp_tz` | TIMESTAMPTZ |
| Timestamp without time zone | `timestamp` | TIMESTAMP |
| Everything else | `string` | STRING |

**ETL must cast on write, not on read:**

- Numeric columns: parse and cast to the target numeric type during ETL row transformation; never write a string that happens to contain a number.
- Date columns: parse source format (MMDDYYYY, MM/DD/YYYY, YYYYMMDD, ISO-8601, epoch millis) to `java.time.LocalDate` / `java.time.Instant` before writing to Parquet/Iceberg.
- Reject ambiguous source values with a logged warning and a null (or `errorPath` record), not a silent string passthrough.

**Views may not use `TRY_CAST` as a substitute for correct source typing.** `TRY_CAST` in a view masks an upstream ETL defect. Fix the type at the source.

## Known Violations by Schema

> **Note:** This analysis was produced by static YAML inspection on 2026-05-21. Each entry needs a live data sample to confirm the source format and the correct target type before fixing. Priority is ordered by query impact (aggregates broken > filters broken > display only).

### `fec` — HIGH priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `year` | all transaction tables | VARCHAR (partition) | INTEGER | Source is the URL year parameter; ETL writes path component as string. Breaks `WHERE year = 2024`. |
| `transaction_date` | individual_contributions, committee_contributions | string | DATE | Source is MMDDYYYY with no separators. operating_expenditures uses MM/DD/YYYY. Needs format-aware parse per table. |
| `disbursement_date`, `communication_date` | independent_expenditures | string | DATE | Same MMDDYYYY format. Needs research to confirm. |
| `coverage_end_date` | candidate_summary, committee_summary | string | DATE | FEC summary files; format TBD. Needs research. |

### `health` — HIGH priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `total_amount_reimbursed` | medicaid_drug_utilization | string | DOUBLE | CMS Medicaid API returns as string decimal. Confirmed. |
| `medicaid_amount_reimbursed` | medicaid_drug_utilization | string | DOUBLE | Same. |
| `non_medicaid_amount_reimbursed` | medicaid_drug_utilization | string | DOUBLE | Same. |
| `number_of_prescriptions` | medicaid_drug_utilization | string | INTEGER or LONG | May contain suppressed values ("DS" for data suppression). Needs research. |
| `units_reimbursed` | medicaid_drug_utilization | string | DOUBLE | May also have suppressed values. Needs research. |
| `total_amount` | cms_open_payments_general | string | DOUBLE | CMS Open Payments CSV publishes as string decimal. Confirmed. |
| `number_of_payments` | cms_open_payments_general | string | INTEGER | Should be whole number. Needs live sample confirmation. |
| `payment_date` | cms_open_payments_general | string | DATE | Format TBD. Needs research. |
| `year` | medicaid_drug_utilization, cdc_wonder_mortality, cdc_behavioral_risk | string | INTEGER | Partition or data column. |
| `week_ending_date` | cdc_wonder_mortality | string | DATE | CDC format TBD. Needs research. |
| `age_adjusted_rate` | cdc_wonder_mortality | string | DOUBLE | Rate field. |
| `pct` | cdc_behavioral_risk | string | DOUBLE | Percentage (0–100). Needs sample to confirm no non-numeric values. |
| `sample_size` | cdc_behavioral_risk | string | INTEGER | Count field. |
| `dose1_pct_us`, `series_complete_pct`, `booster_pct`, `bivalent_booster_pct` | covid_vaccinations | string | DOUBLE | All percentage fields. |
| `series_complete_count`, `booster_count`, `bivalent_booster_count` | covid_vaccinations | string | LONG | Count fields; national totals can exceed 2^31. |
| `receive_year` | fda_adverse_events | string | INTEGER | Year extracted from receive_date. |
| `patient_age` | fda_adverse_events | string | DOUBLE | FDA allows decimal ages (e.g. 0.5 years). |
| `product_quantity` | fda_recalls, fda_medical_device_recalls | string | string | May contain non-numeric descriptions ("ALL LOTS"). Keep as string; needs research. |
| Various `*_date` columns | clinical_trials, fda_* | string | DATE | ISO-8601 partial dates ("2023-09", "2023") are common in ClinicalTrials.gov. Keep as string unless fully parseable. |

### `econ` — MEDIUM priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `year` | bls_unemployment, bls_cpi, gdp_by_industry, others | string/VARCHAR | INTEGER | Partition column. Same issue as fec. |
| `value` | bls_unemployment, bls_cpi, gdp_by_industry, world_bank_indicators | string | DOUBLE | Core economic indicator value. Needs research to confirm no suppressed/non-numeric values in BLS data. |
| `date` | treasury_yield_curve, treasury_debt, world_bank_indicators | string | DATE | ISO-8601 in most BEA/BLS sources. |
| `record_date` | treasury_debt | string | DATE | Treasury API format TBD. |
| `time_period` | imf_* | string | string | IMF uses period codes like "2023Q3", "2023M09" — not parseable as DATE without custom logic. Keep string; add note. |

### `crime` — MEDIUM priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `year` | ncvs_* | string | INTEGER | Partition column. |
| `yearq` | ncvs_* | string | string | Quarter-year composite (e.g. "20231"). Not a standard type; keep string. |
| `age` | ncvs_victimization | string | string | FBI/BJS age codes include ranges ("12-14"). Keep string; needs research. |
| `income` | ncvs_* | string | string | Income bracket codes, not raw numbers. Keep string. |
| `hhsize` | ncvs_household | string | string | Household size codes may include "5 or more". Needs research. |
| `nibrs_start_date` | agencies | string | DATE | Date when agency started NIBRS reporting. Needs research. |

### `cyber_vuln` — MEDIUM priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `date_added`, `due_date` | kev (CISA KEV) | string | DATE | ISO-8601 confirmed in CISA API. |
| `severity_score` | nvd_cve_scores | string | DOUBLE | CVSS score 0.0–10.0. |
| `published_date` | osv_advisories | string | DATE | OSV uses ISO-8601. Needs confirmation for partial dates. |

### `weather` — LOW priority (display impact only)

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `date` | ghcnd_daily, air_quality_daily, others | string | DATE | NOAA uses YYYYMMDD or ISO-8601 depending on endpoint. |
| `min_date`, `max_date` | ghcnd_stations | string | DATE | Station coverage dates. |
| `week_date` | county_weekly_weather | string | DATE | Week start date. |

### `econ_reference` — LOW priority

| Column | Table(s) | Current type | Correct type | Notes |
|---|---|---|---|---|
| `date` | column template | string | DATE | Used as template; affects multiple derived tables. |
| `last_updated` | various | string | DATE or TIMESTAMP | Format TBD. Needs research. |

### Schemas with no known violations
`geo`, `ref`, `fedregister`, `patents`, `lands`, `sec` (filing_date is already DATE in template; individual column review pending).

## Consequences

- All schemas with known VARCHAR-stored numerics require a re-ingest pass. Known violations at time of writing: `fec` (`year` partition column, `transaction_date`), `health` (`total_amount`, `total_amount_reimbursed`, `medicaid_amount_reimbursed`, `non_medicaid_amount_reimbursed`, `units_reimbursed`, `number_of_prescriptions`, `number_of_payments`).
- Schema YAML review checklist: any column whose `comment` contains "dollars", "amount", "count", "rate", "year", or "date" must have a non-string type unless the source genuinely contains mixed or unparseable values.
- `McpServer.query()` now uses JDBC `getInt`/`getLong`/`getDouble`/`getBoolean` per column type, so correctly-typed Iceberg columns will serialize as JSON numbers automatically.
- Partition columns (e.g. `year`) must match their declared `type` in the hive partition `columnDefinitions`; ETL must write the partition value as the declared type, not as a string path component that DuckDB later reads as VARCHAR.
