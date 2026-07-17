# Patents Schema Data Plan

## Implementation Status (2026-07-16)

**Partially delivered.** All 7 planned tables are materialized (each backed by its own
transformer under `govdata/patents/`), but several were reshaped relative to this plan and two
substantive column-level features remain unbuilt. The most consequential deviation is that
`patent_grants` was delivered as a *faithful* `g_patent`-only table; the plan's proposed
5-file-join extras were **source-split** into standalone extension tables rather than folded in.

| Planned table | Delivered as | Notes |
|---|---|---|
| `patent_grants` | `patent_grants` (`PatentGrantsTransformer`) | Faithful `g_patent` only. Abstract/filing/figure/location extras source-split into `patent_abstracts`, `patent_applications`, `patent_figures`, `patent_locations` (each a transformer). Citation-count columns **NOT BUILT**. |
| `patent_assignees` | `patent_assignees` (`PatentAssigneesTransformer`) | Inline geo columns delivered only via join to `patent_locations` on `location_id`, not inline. |
| `patent_inventors` | `patent_inventors` (`PatentInventorsTransformer`) | Inline geo columns delivered only via join to `patent_locations` on `location_id`, not inline. |
| `patent_cpc_classes` | `patent_cpc_classes` (`PatentCpcClassesTransformer`) | As planned. |
| `patent_claims` | `patent_claims` (`PatentClaimsTransformer`) | `embedding` column **NOT BUILT**; `dependent` delivered as string, not INTEGER (documented type change). |
| `patent_summaries` | `patent_summaries` (`PatentSummariesTransformer`) | `embedding` column **NOT BUILT**. |
| `trademark_applications` | VIEW over 5 faithful source tables | Delivered as a VIEW over `trademark_case_file`, `trademark_owner`, `trademark_classification`, `trademark_intl_class`, `trademark_statement` (each a transformer), not a single materialized join table. |

**Not yet built (substantive remaining gaps):**
- `patent_grants.num_us_citations` / `patent_grants.num_foreign_citations` — absent from every
  patents table; no citation-aggregation source (`g_us_patent_citation` / `g_foreign_citation`)
  is ingested anywhere. Real unbuilt feature.
- `patent_claims.embedding FLOAT[]` and `patent_summaries.embedding FLOAT[]` — absent from the
  YAML column lists (the schema header comment claims embeddings are populated, but no embedding
  column is defined). Real unbuilt feature.

**Delivered with deviation (not gaps):**
- `patent_grants` extension tables (`patent_abstracts`, `patent_applications`, `patent_figures`,
  `patent_locations`) — source-split from the proposed 5-file join.
- Assignee/inventor geo columns — via join to `patent_locations` (`location_id`), not inline.
- `trademark_applications` — a VIEW over 5 faithful source tables.
- `patent_claims.dependent` — deliberately delivered as string (verbatim `"claim N"`), not INTEGER.

---

## Strategic Context

USPTO patent and trademark data is one of the most underused public datasets for financial
analysis. The primary cross-schema value is **innovation and IP asset tracking**: joining
patent grants to SEC company filings (via assignee entity extraction), BLS R&D employment,
and Census ACS educational attainment creates a complete picture of corporate R&D productivity,
technology licensing revenue risk, and regional innovation concentration. Trademark data
complements SEC brand valuation disclosures and M&A activity.

PatentsView (the USPTO's analytics portal) provides bulk TSV downloads with pre-disambiguated
inventor and assignee identities — no raw XML parsing required. All files are HTTP downloads
from `https://s3.amazonaws.com/data.patentsview.org/` (PatentsView has migrated to the USPTO
Open Data Portal at `data.uspto.gov`; all `patentsview.org` URLs redirect there).

---

## Data Sources

| Source | Publisher | Format | Access | Cadence |
|---|---|---|---|---|
| Patent grants | USPTO / PatentsView | Bulk TSV (`g_patent.tsv` + joins) | Free, no key | Quarterly |
| Patent inventors | USPTO / PatentsView | Bulk TSV (`g_inventor_disambiguated.tsv`) | Free, no key | Quarterly |
| Patent assignees | USPTO / PatentsView | Bulk TSV (`g_assignee_disambiguated.tsv`) | Free, no key | Quarterly |
| Location reference | USPTO / PatentsView | Bulk TSV (`g_location_disambiguated.tsv`) | Free, no key | Quarterly |
| CPC classifications | USPTO / PatentsView | Bulk TSV (`g_cpc_current.tsv`) | Free, no key | Quarterly |
| Patent claims | USPTO / PatentsView | Bulk TSV (`g_claims_{year}.tsv.zip`, per-year) | Free, no key | Quarterly |
| Patent summaries | USPTO / PatentsView | Bulk TSV (`g_brf_sum_text_{year}.tsv.zip`, per-year) | Free, no key | Quarterly |
| Trademark applications | USPTO Economic Research | Bulk CSV/DTA (`case_file` + 4 joins) | Free, no key | Quarterly |

---

## Quarterly Blackout Model

All tables in this schema refresh on a **quarterly cadence** — PatentsView publishes updated
bulk files approximately 6 weeks after the end of each calendar quarter. New data is available
only in months **3, 6, 9, 12**. The daily ETL run must fail fast in all other months.

```
Month  1  — BLACKOUT (no new data)
Month  2  — BLACKOUT
Month  3  — ACTIVE (Q4 prior-year data released)
Month  4  — BLACKOUT
Month  5  — BLACKOUT
Month  6  — ACTIVE (Q1 data released)
Month  7  — BLACKOUT
Month  8  — BLACKOUT
Month  9  — ACTIVE (Q2 data released)
Month 10  — BLACKOUT
Month 11  — BLACKOUT
Month 12  — ACTIVE (Q3 data released)
```

**Implementation:** Worker 81 (`daily`) calls the standard govdata release-window gate at
startup:

```bash
within_release_window "patent" "3,6,9,12" || exit 0
```

This exits cleanly with a skip log in the 9 blackout months. Worker 80 (`initial`) never
calls this gate — historical backfill always runs in full regardless of current month.

---

## Proposed Tables

### `patent_grants`

One row per granted utility patent. Core dimension table — all other patent tables
join here on `patent_id`.

**Sources (requires ETL join of 5 files):** — **DELIVERED VIA SOURCE-SPLIT:** `patent_grants` is faithful `g_patent` only; the extras below became standalone extension tables (`patent_applications`, `patent_abstracts`, `patent_figures`, `patent_locations`), not an in-table join.
- `g_patent.tsv` — core patent attributes (`patent_id`, `patent_type`, `patent_date`, `wipo_kind`, `num_claims`, `withdrawn`)
- `g_application.tsv` — filing date (`filing_date`, `patent_application_type`, `series_code`) — delivered via `patent_applications`
- `g_patent_abstract.tsv` — abstract text (`patent_abstract`) — delivered via `patent_abstracts`
- `g_figures.tsv` — figure counts (`num_figures`, `num_sheets`) — delivered via `patent_figures`
- Aggregated counts from `g_us_patent_citation.tsv` and `g_foreign_citation.tsv` — **NOT BUILT** (no citation source ingested anywhere)

**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `g_patent.patent_id` | USPTO patent number (e.g., `US10123456B2`) |
| grant_year | INTEGER | parsed from `patent_date` | Partition key |
| patent_date | DATE | `g_patent.patent_date` | Date of patent grant |
| filing_date | DATE | `g_application.filing_date` | Original application filing date — delivered via `patent_applications` (not inline) |
| patent_type | VARCHAR | `g_patent.patent_type` | utility / design / plant |
| patent_title | VARCHAR | `g_patent.patent_title` | Patent title |
| patent_abstract | VARCHAR | `g_patent_abstract.patent_abstract` | Patent abstract (truncated to 2000 chars) — delivered via `patent_abstracts` (not inline) |
| num_claims | INTEGER | `g_patent.num_claims` | Number of claims |
| num_figures | INTEGER | `g_figures.num_figures` | Number of figures — delivered via `patent_figures` (not inline) |
| num_sheets | INTEGER | `g_figures.num_sheets` | Number of drawing sheets — delivered via `patent_figures` (not inline) |
| num_us_citations | INTEGER | COUNT over `g_us_patent_citation` | Forward US patent citations (innovation signal) — **NOT BUILT** |
| num_foreign_citations | INTEGER | COUNT over `g_foreign_citation` | Forward foreign patent citations — **NOT BUILT** |
| wipo_kind | VARCHAR | `g_patent.wipo_kind` | WIPO kind code (B1, B2, etc.) |
| withdrawn | BOOLEAN | `g_patent.withdrawn` | Whether patent was withdrawn; filter to `false` for active grants |
| patent_application_type | VARCHAR | `g_application.patent_application_type` | Application type (e.g., utility, continuation) — delivered via `patent_applications` (not inline) |

---

### `patent_assignees`

Organizations (companies, universities, governments) that own patents at grant time.
One row per patent-assignee combination. Assignee-to-SEC entity matching is handled
outside this schema via entity extraction.

**Sources (requires ETL join of 2 files):** — **DELIVERED WITH DEVIATION:** the `g_location` fields are NOT inline on `patent_assignees`; they live in the standalone `patent_locations` table and are reached by joining on `location_id`.
- `g_assignee_disambiguated.tsv` — assignee identity and type
- `g_location_disambiguated.tsv` — geographic fields via `location_id` — delivered via `patent_locations` (join on `location_id`)

**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `assignee.patent_id` | FK to patent_grants |
| grant_year | INTEGER | | Partition key |
| assignee_sequence | INTEGER | `assignee.assignee_sequence` | Order when patent has multiple assignees |
| assignee_id | VARCHAR | `assignee.assignee_id` | PatentsView disambiguated assignee ID |
| assignee_organization | VARCHAR | `assignee.disambig_assignee_organization` | Organization name; null for individual assignees |
| assignee_name_first | VARCHAR | `assignee.disambig_assignee_individual_name_first` | First name; null for org assignees |
| assignee_name_last | VARCHAR | `assignee.disambig_assignee_individual_name_last` | Last name; null for org assignees |
| assignee_type | VARCHAR | `assignee.assignee_type` | US company / foreign company / US government / individual |
| state_fips | VARCHAR | `location.state_fips` | FK to geo.states (assignee location at grant) — delivered via `patent_locations` join (`location_id`), not inline |
| county_fips | VARCHAR | `location.county_fips` | FK to geo.counties — delivered via `patent_locations` join (`location_id`), not inline |
| country_code | VARCHAR | `location.disambig_country` | 2-letter ISO country code — delivered via `patent_locations` join (`location_id`), not inline |
| latitude | DOUBLE | `location.latitude` | Assignee location latitude — delivered via `patent_locations` join (`location_id`), not inline |
| longitude | DOUBLE | `location.longitude` | Assignee location longitude — delivered via `patent_locations` join (`location_id`), not inline |

---

### `patent_inventors`

Individual inventors listed on patent grants. One row per patent-inventor combination.
Joins to `geo.counties` for regional innovation analysis; joins to census ACS STEM
education fields for workforce pipeline research.

**Sources (requires ETL join of 2 files):** — **DELIVERED WITH DEVIATION:** the `g_location` fields are NOT inline on `patent_inventors`; they live in the standalone `patent_locations` table and are reached by joining on `location_id`.
- `g_inventor_disambiguated.tsv` — inventor identity (~8 GB uncompressed)
- `g_location_disambiguated.tsv` — geographic fields via `location_id` — delivered via `patent_locations` (join on `location_id`)

**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `inventor.patent_id` | FK to patent_grants |
| grant_year | INTEGER | | Partition key |
| inventor_id | VARCHAR | `inventor.inventor_id` | PatentsView disambiguated inventor ID |
| inventor_sequence | INTEGER | `inventor.inventor_sequence` | Inventor order on patent (0 = first/primary) |
| name_first | VARCHAR | `inventor.disambig_inventor_name_first` | First name |
| name_last | VARCHAR | `inventor.disambig_inventor_name_last` | Last name |
| gender_code | VARCHAR | `inventor.gender_code` | M / F (inferred by PatentsView disambiguation); useful for diversity analysis |
| state_fips | VARCHAR | `location.state_fips` | FK to geo.states (inventor location at grant) — delivered via `patent_locations` join (`location_id`), not inline |
| county_fips | VARCHAR | `location.county_fips` | FK to geo.counties — delivered via `patent_locations` join (`location_id`), not inline |
| country_code | VARCHAR | `location.disambig_country` | 2-letter ISO country code — delivered via `patent_locations` join (`location_id`), not inline |
| latitude | DOUBLE | `location.latitude` | Inventor location latitude — delivered via `patent_locations` join (`location_id`), not inline |
| longitude | DOUBLE | `location.longitude` | Inventor location longitude — delivered via `patent_locations` join (`location_id`), not inline |

---

### `patent_cpc_classes`

Cooperative Patent Classification codes assigned to each patent. One row per
patent-CPC combination. CPC is the primary technology taxonomy for patent analysis.

**Source:** `g_cpc_current.tsv`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `cpc.patent_id` | FK to patent_grants |
| grant_year | INTEGER | | Partition key |
| cpc_sequence | INTEGER | `cpc.cpc_sequence` | CPC code sequence on patent |
| cpc_section | VARCHAR | `cpc.cpc_section` | CPC section (A–H, Y) |
| cpc_class | VARCHAR | `cpc.cpc_class` | CPC class (2-char, e.g., `H04`) |
| cpc_subclass | VARCHAR | `cpc.cpc_subclass` | CPC subclass (1-char, e.g., `L`) |
| cpc_group | VARCHAR | `cpc.cpc_group` | Full CPC classification string (e.g., `H04L0009000`) |
| cpc_type | VARCHAR | `cpc.cpc_type` | `i` (inventive) or `a` (additional) |

**Note:** `g_cpc_at_issue.tsv` is also available with CPC classification as it appeared at
grant time (vs. current reclassification), plus `cpc_version_indicator` and `cpc_action_date`.
Use `g_cpc_current.tsv` for analysis; `g_cpc_at_issue.tsv` for historical accuracy.

---

### `patent_claims`

Individual patent claims with vectorized text for semantic search. One row per claim.
Claims define the legal scope of the patent — the most analytically useful text component.
(Note: the `embedding` / vectorized-text feature is **NOT BUILT** — see column table below.)

**Source:** `g_claims_{year}.tsv.zip` — one file per year from 1976–present,
at `https://s3.amazonaws.com/data.patentsview.org/claims/`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `claims.patent_id` | FK to patent_grants |
| grant_year | INTEGER | | Partition key |
| claim_sequence | INTEGER | `claims.claim_sequence` | Processing order of claim (1-based) |
| claim_number | INTEGER | `claims.claim_number` | Claim number as printed in the patent |
| claim_text | VARCHAR | `claims.claim_text` | Full claim text |
| dependent | INTEGER | `claims.dependent` | 0 = independent claim; positive integer = sequence number of parent claim — delivered as string (verbatim `"claim N"`), NOT INTEGER (deliberate, documented type change) |
| exemplary | BOOLEAN | `claims.exemplary` | Claim designated as exemplary for the patent; useful for ML classification |
| embedding | FLOAT[] | generated at ingestion | Vector embedding of `claim_text` — **NOT BUILT** (no embedding column defined in the YAML) |

**Vectorization note:** For dependent claims, consider prepending the parent independent
claim text before embedding to capture full legal scope rather than just the delta language.

---

### `patent_summaries`

Patent brief summary section text with vectorized embeddings. One row per patent.
More concise than the full description; covers the invention's key aspects and
distinguishing features over prior art.
(Note: the `embedding` / vectorized-embeddings feature is **NOT BUILT** — see column table below.)

**Source:** `g_brf_sum_text_{year}.tsv.zip` — one file per year from 1976–present,
at `https://s3.amazonaws.com/data.patentsview.org/brief-summary-text/`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| patent_id | VARCHAR | `summary.patent_id` | FK to patent_grants |
| grant_year | INTEGER | | Partition key |
| summary_text | VARCHAR | `summary.summary_text` | Full brief summary section text |
| embedding | FLOAT[] | generated at ingestion | Vector embedding of `summary_text` — **NOT BUILT** (no embedding column defined in the YAML) |

**Not in scope:** `g_detail_desc_text_{year}.tsv` (full specification, ~100+ GB uncompressed)
and `g_draw_desc_text_{year}.tsv` (drawing descriptions).

---

### `trademark_applications`

USPTO trademark applications and registrations. One row per application.
Joins to `sec.filing_metadata` via applicant name for brand valuation and M&A research.

**Sources (requires ETL join of 5 files on `serial_no`):** — **DELIVERED AS A VIEW:** `trademark_applications` is a VIEW over 5 faithful, separately-materialized source tables (`trademark_case_file`, `trademark_owner`, `trademark_classification`, `trademark_intl_class`, `trademark_statement`), each with its own transformer — not a single materialized join table.
- `case_file.csv` — core application attributes (79 columns) — delivered via `trademark_case_file`
- `owner.csv` — applicant/owner name and address — delivered via `trademark_owner`
- `classification.csv` — US class codes — delivered via `trademark_classification`
- `intl_class.csv` — international class codes — delivered via `trademark_intl_class`
- `statement.csv` — goods and services descriptions — delivered via `trademark_statement`

**Partition:** `application_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Description |
|---|---|---|---|
| serial_no | VARCHAR | `case_file.serial_no` | USPTO trademark serial number (primary key) |
| registration_no | VARCHAR | `case_file.registration_no` | Registration number (null if pending) |
| application_year | INTEGER | parsed from `filing_dt` | Partition key |
| filing_dt | DATE | `case_file.filing_dt` | Application filing date |
| registration_dt | DATE | `case_file.registration_dt` | Registration date (null if not registered) |
| abandon_dt | DATE | `case_file.abandon_dt` | Abandonment date; null for live marks |
| mark_id_char | VARCHAR | `case_file.mark_id_char` | Trademark text (null for design marks) |
| mark_draw_cd | VARCHAR | `case_file.mark_draw_cd` | Drawing code: 1=typed, 2=design, 3=stylized, 4=standard char, 5=color |
| cfh_status_cd | VARCHAR | `case_file.cfh_status_cd` | Status code (live/dead/registered/abandoned/cancelled) |
| cfh_status_dt | DATE | `case_file.cfh_status_dt` | Date of current status |
| publication_dt | DATE | `case_file.publication_dt` | Date published for opposition |
| renewal_dt | DATE | `case_file.renewal_dt` | Most recent renewal date |
| std_char_claim_in | BOOLEAN | `case_file.std_char_claim_in` | Standard character mark (plain text) flag |
| applicant_name | VARCHAR | `owner.own_name` | Owner/applicant organization or individual name |
| applicant_type | VARCHAR | `owner.own_type_cd` | Owner type code |
| applicant_state | VARCHAR | `owner.own_addr_state_cd` | 2-letter state code (not FIPS; join to geo.states) |
| applicant_country | VARCHAR | `owner.own_addr_country_cd` | 2-letter ISO country code |
| goods_services_class | VARCHAR | `intl_class` aggregated | International class(es) (comma-separated, 1–45) |
| goods_services_description | VARCHAR | `statement` aggregated | Description of goods/services (truncated to 2000 chars) |

---

## Join Architecture

```
patent_grants (patent_id, grant_year)
    ├── patent_assignees (patent_id, grant_year)
    │       ├── [entity extraction] → sec.filing_metadata (R&D productivity)
    │       └── geo.counties (county_fips → regional innovation clusters)
    ├── patent_inventors (patent_id, grant_year)
    │       └── geo.counties (county_fips → inventor geography)
    │               └── census.acs_education (county_fips → STEM pipeline)
    ├── patent_cpc_classes (patent_id, grant_year)
    │       → technology sector classification
    ├── patent_claims (patent_id, grant_year)
    │       → semantic search via embedding
    └── patent_summaries (patent_id, grant_year)
            → semantic search via embedding

trademark_applications (serial_no, application_year)
    ├── [entity extraction] → sec.filing_metadata (brand asset tracking)
    └── geo.states (applicant_state via state code lookup)
```

**R&D input ↔ patent output (cross-schema).** The NSF/NCSES R&D tables live in `econ`
(`econ.nsf_herd_by_institution`, etc. — see `research.md`). Aggregating `econ` R&D spend by
`state_fips, year` against `patent_assignees` grant counts by `state_fips, grant_year` gives a
patents-per-R&D-dollar ratio — a cross-schema join, not an in-schema one.

**Key analytical joins:**
- `patent_assignees` / `trademark_applications` join to `sec.filing_metadata` via entity extraction (handled outside this schema)
- `patent_cpc_classes.cpc_section = 'G'` (Physics/Computing) concentrations by county join
  to `econ.bls_employment` for tech cluster analysis
- `patent_grants.grant_year` trends join to `econ.fred_indicators` R&D spending series

---

## Worker Assignment

| Worker | Mode | Year range | Tables | Heap | Schedule |
|---|---|---|---|---|---|
| 80 | `initial` (historical) | `GOVDATA_START_YEAR` → `GOVDATA_INCREMENTAL_START_YEAR - 1` | All 7 tables | 4 GB / 6 GB | Once |
| 81 | `daily` | `GOVDATA_INCREMENTAL_START_YEAR` → current year | All 7 tables | 2 GB / 3 GB | Daily (release-window gated) |

**Run modes follow the standard govdata pattern:**

- **Worker 80 (`initial`)**: `GOVDATA_RUN_MODE=historical`. Processes all grant years from
  `GOVDATA_START_YEAR` (default 2010) through `GOVDATA_INCREMENTAL_START_YEAR - 1` (i.e., last
  full year). Release-window checks are skipped — initial always runs in full. For per-year
  files (`g_claims_{year}.tsv.zip`, `g_brf_sum_text_{year}.tsv.zip`), only year files within
  the requested range are downloaded. For bulk single-file sources (`g_patent.tsv`,
  `g_assignee_disambiguated.tsv`, `g_inventor_disambiguated.tsv`, `g_cpc_current.tsv`,
  `g_location_disambiguated.tsv`), the full snapshot is downloaded once, cached, and filtered
  to the target year range when writing Parquet partitions.

- **Worker 81 (`daily`)**: `GOVDATA_RUN_MODE=daily`. Processes only `GOVDATA_INCREMENTAL_START_YEAR`
  (the current year). Must call `within_release_window "patent" "3,6,9,12"` at startup — exits
  immediately with a skip log in the 9 blackout months. In active months, downloads only the
  current-year per-year files and refreshes the current-year Parquet partition for bulk-file
  tables. Previously written historical partitions are never re-processed.

**Notes:**
- `patent_claims` and `patent_summaries` require vectorization at ingestion time — process
  them last in worker 80 and allow the full 8-hour timeout (`-t 480`).
- Batch embedding calls to avoid per-row API overhead.
- Worker 80 only needs to run once; worker 81 runs daily but silently skips 9 of 12 months.

---

## Release Windows

| Table | Active months | Blackout months | Rationale |
|---|---|---|---|
| `patent_grants` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | PatentsView quarterly bulk refresh |
| `patent_assignees` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | Same quarterly refresh cycle |
| `patent_inventors` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | Same quarterly refresh cycle |
| `patent_cpc_classes` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | Same quarterly refresh cycle |
| `patent_claims` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | Same quarterly refresh cycle |
| `patent_summaries` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | Same quarterly refresh cycle |
| `trademark_applications` | 3, 6, 9, 12 | 1, 2, 4, 5, 7, 8, 10, 11 | USPTO quarterly bulk data |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GOVDATA_PARQUET_DIR` | Required | Root Parquet directory; patents data lands in `${GOVDATA_PARQUET_DIR}/patents/` |
| `GOVDATA_CACHE_DIR` | Required | Root cache directory; raw downloads cached in `${GOVDATA_CACHE_DIR}/patents/` |
| `GOVDATA_RUN_MODE` | Required | `historical` (worker 80, backfill) or `daily` (worker 81, incremental) |
| `GOVDATA_START_YEAR` | Optional | Historical backfill start year (default: 2010); used only in `historical` mode |
| `GOVDATA_INCREMENTAL_START_YEAR` | Optional | First year owned by daily workers (default: current year); historical cap is this value minus 1 |
| `PATENTS_INCLUDE_DESIGN` | Optional | Set to `true` to include design patents in `patent_grants` (adds ~1M rows/year; default: excluded) |

---

## Implementation Notes

- **run-pool.sh integration.** Add `patents` to the schema_workers map: `patents` → `80 81`.
  Add worker 80 to the `historical` alias set; add worker 81 to the `daily` alias set.
- **S3 base URL.** All PatentsView bulk files are at `https://s3.amazonaws.com/data.patentsview.org/`.
  Per-year files (claims, summaries) follow pattern `{dataset}/{filename}_{year}.tsv.zip`.
- **`patent_grants` requires 5-file join.** — **NOT AS DELIVERED:** `patent_grants` ships as faithful
  `g_patent` only; the extras were source-split into `patent_applications` (filing date), `patent_abstracts`
  (abstract), `patent_figures` (figure counts), and `patent_locations`, and the citation aggregates from
  `g_us_patent_citation.tsv` / `g_foreign_citation.tsv` are **NOT BUILT** (source not ingested).
  `g_patent.tsv` alone has only 8 columns. Filing date
  comes from `g_application.tsv`, abstract from `g_patent_abstract.tsv`, figure counts from
  `g_figures.tsv`, and citation aggregates from `g_us_patent_citation.tsv` / `g_foreign_citation.tsv`.
- **Location join is required for assignees and inventors.** — **DELIVERED VIA `patent_locations`:** geo fields
  are not inline on `patent_assignees` / `patent_inventors`; they are reached by joining `location_id` to the
  standalone `patent_locations` table. Neither `g_assignee_disambiguated.tsv`
  nor `g_inventor_disambiguated.tsv` contains geographic columns directly — they carry a `location_id`
  that joins to `g_location_disambiguated.tsv` for `state_fips`, `county_fips`, `latitude`, `longitude`.
- **`g_inventor_disambiguated.tsv` is ~8 GB uncompressed.** Initial load worker (80) needs
  `-t 480` (8-hour timeout) and should process tables sequentially to avoid memory pressure.
- **Trademark data is a normalized relational dataset** with 13 tables linked by `serial_no`.
  The flat `trademark_applications` table requires joining `case_file` + `owner` + `classification`
  + `intl_class` + `statement`. — **DELIVERED AS A VIEW** over the 5 faithful source tables
  (`trademark_case_file`, `trademark_owner`, `trademark_classification`, `trademark_intl_class`,
  `trademark_statement`), not a materialized flat table. `applicant_state` is a 2-letter code, not FIPS — join to `geo.states`.
- **`patent_claims` dependent column is an integer, not a string.** — **NOT AS DELIVERED:** shipped as a
  string (verbatim `"claim N"`); the integer form below describes the plan, not the built table. `0` = independent claim;
  positive integer = `claim_sequence` of the parent claim. For embedding, consider prepending
  the parent independent claim text to dependent claims before vectorizing.
- **Vectorization must use batched embedding calls.** — **NOT BUILT:** no `embedding` column is defined on
  `patent_claims` or `patent_summaries` in the delivered YAML, so ingestion-time vectorization is not yet
  implemented. Per-row API calls are prohibitively slow
  at patent scale. Use the same embedding model used elsewhere in the project for cross-schema
  semantic search compatibility.
- **Assignee-to-SEC entity matching** is handled outside this schema via entity extraction —
  no fuzzy join logic or crosswalk table belongs here.
- **Design patents** are excluded from `patent_grants` by default; set
  `PATENTS_INCLUDE_DESIGN=true` to include them (adds ~1M rows/year).
- **`withdrawn` flag.** Filter `g_patent.withdrawn = 0` for active grants. Withdrawn patents
  are rare but should be excluded from all analytical joins.
