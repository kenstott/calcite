# Patents Schema Data Plan

## Strategic Context

USPTO patent and trademark data is one of the most underused public datasets for financial
analysis. The primary cross-schema value is **innovation and IP asset tracking**: joining
patent grants to SEC company filings (via assignee CIK or name), BLS R&D employment, and
Census ACS educational attainment creates a complete picture of corporate R&D productivity,
technology licensing revenue risk, and regional innovation concentration. Trademark data
complements SEC brand valuation disclosures and M&A activity.

PatentsView (the USPTO's analytics portal) provides a clean REST API and bulk downloads
with pre-disambiguated inventor and assignee identities — no raw XML parsing required.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Patent grants (utility) | USPTO / PatentsView | REST API + bulk CSV | Free, no key | Quarterly |
| Patent inventors | USPTO / PatentsView | REST API + bulk CSV | Free, no key | Quarterly |
| Patent assignees | USPTO / PatentsView | REST API + bulk CSV | Free, no key | Quarterly |
| CPC classifications | USPTO / PatentsView | REST API + bulk CSV | Free, no key | Quarterly |
| Patent citations | USPTO / PatentsView | Bulk CSV | Free, no key | Annual |
| Trademark applications | USPTO TSDR / PatentsView | Bulk CSV | Free, no key | Quarterly |

All PatentsView endpoints: `https://search.patentsview.org/api/v1/`
Bulk downloads: `https://patentsview.org/download/data-download-tables`

---

## Proposed Tables

### `patent_grants`

One row per granted utility patent. Core dimension table — all other patent tables
join here on `patent_id`.

**Source:** PatentsView `/patent` endpoint or `g_patent.tsv` bulk download
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly (PatentsView refreshes ~6 weeks after each USPTO weekly issue)
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| patent_id | VARCHAR | USPTO patent number (e.g., `US10123456B2`) |
| grant_year | INTEGER | Partition key |
| grant_date | DATE | Date of patent grant |
| filing_date | DATE | Original application filing date |
| patent_type | VARCHAR | utility / design / plant |
| title | VARCHAR | Patent title |
| abstract | VARCHAR | Patent abstract (truncated to 2000 chars) |
| claims_count | INTEGER | Number of claims |
| figures_count | INTEGER | Number of figures |
| num_us_citations | INTEGER | Forward US patent citations (innovation signal) |
| num_foreign_citations | INTEGER | Forward foreign patent citations |
| num_us_references | INTEGER | Prior art US references cited |
| wipo_kind | VARCHAR | WIPO kind code (B1, B2, etc.) |

---

### `patent_assignees`

Organizations (companies, universities, governments) that own patents at grant time.
One row per patent-assignee combination. Join to `sec.filing_metadata` via `assignee_id`
or name-matching against company name.

**Source:** PatentsView `g_assignee_disambiguated.tsv`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| patent_id | VARCHAR | FK to patent_grants |
| grant_year | INTEGER | Partition key |
| assignee_id | VARCHAR | PatentsView disambiguated assignee ID |
| assignee_name | VARCHAR | Organization name (disambiguated) |
| assignee_type | VARCHAR | US company / foreign company / US government / individual |
| state_fips | VARCHAR | FK to geo.states (assignee location at grant) |
| county_fips | VARCHAR | FK to geo.counties |
| country_code | VARCHAR | 2-letter ISO country code |
| latitude | DOUBLE | Assignee location latitude |
| longitude | DOUBLE | Assignee location longitude |

---

### `patent_inventors`

Individual inventors listed on patent grants. One row per patent-inventor combination.
Joins to `geo.counties` for regional innovation analysis; joins to census ACS STEM
education fields for workforce pipeline research.

**Source:** PatentsView `g_inventor_disambiguated.tsv`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| patent_id | VARCHAR | FK to patent_grants |
| grant_year | INTEGER | Partition key |
| inventor_id | VARCHAR | PatentsView disambiguated inventor ID |
| inventor_sequence | INTEGER | Inventor order on patent (0 = first/primary) |
| first_name | VARCHAR | First name |
| last_name | VARCHAR | Last name |
| state_fips | VARCHAR | FK to geo.states (inventor location at grant) |
| county_fips | VARCHAR | FK to geo.counties |
| country_code | VARCHAR | 2-letter ISO country code |
| latitude | DOUBLE | Inventor location latitude |
| longitude | DOUBLE | Inventor location longitude |

---

### `patent_cpc_classes`

Cooperative Patent Classification codes assigned to each patent. One row per
patent-CPC combination. CPC is the primary technology taxonomy for patent analysis.

**Source:** PatentsView `g_cpc_current.tsv`
**Partition:** `grant_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| patent_id | VARCHAR | FK to patent_grants |
| grant_year | INTEGER | Partition key |
| cpc_sequence | INTEGER | CPC code sequence on patent |
| section | VARCHAR | CPC section (A–H, Y) |
| subsection | VARCHAR | CPC class (2-char, e.g., `H01`) |
| group_code | VARCHAR | CPC group (e.g., `H01L`) |
| subgroup | VARCHAR | Full CPC symbol (e.g., `H01L21/02`) |
| category | VARCHAR | i (inventive) or a (additional) |

---

### `trademark_applications`

USPTO trademark applications and registrations. One row per application.
Joins to `sec.filing_metadata` via applicant name for brand valuation and M&A research.

**Source:** USPTO TSDR bulk data / PatentsView trademark bulk downloads
**Partition:** `application_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| serial_number | VARCHAR | USPTO trademark serial number |
| registration_number | VARCHAR | Registration number (null if pending) |
| application_year | INTEGER | Partition key |
| filing_date | DATE | Application filing date |
| registration_date | DATE | Registration date (null if not registered) |
| mark_identification | VARCHAR | Trademark text (null for design marks) |
| mark_type | VARCHAR | Standard character / Stylized / Design / Sound |
| status | VARCHAR | Live / Dead — Registered / Abandoned / Cancelled |
| applicant_name | VARCHAR | Owner/applicant organization name |
| applicant_state_fips | VARCHAR | FK to geo.states |
| applicant_country | VARCHAR | 2-letter ISO country code |
| goods_services_class | VARCHAR | International class(es) (comma-separated, 1–45) |
| goods_services_description | VARCHAR | Description of goods/services (truncated) |

---

## Join Architecture

```
patent_grants (patent_id, grant_year)
    ├── patent_assignees (patent_id, grant_year)
    │       ├── sec.filing_metadata (company_name ~ assignee_name → R&D productivity)
    │       └── geo.counties (county_fips → regional innovation clusters)
    ├── patent_inventors (patent_id, grant_year)
    │       └── geo.counties (county_fips → inventor geography)
    │               └── census.acs_education (county_fips → STEM pipeline)
    └── patent_cpc_classes (patent_id, grant_year)
            → technology sector classification

trademark_applications (serial_number, application_year)
    ├── sec.filing_metadata (applicant_name ~ company_name → brand asset tracking)
    └── geo.states (applicant_state_fips)
```

**Key analytical joins:**
- `patent_assignees.assignee_name` fuzzy-matched to `sec.filing_metadata.company_name`
  identifies publicly traded companies' patent portfolios for IP asset valuation
- `patent_cpc_classes.section = 'G'` (Physics/Computing) concentrations by county join
  to `econ.bls_employment` for tech cluster analysis
- `patent_grants.grant_year` trends join to `econ.fred_indicators` R&D spending series

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 80 | initial | All 5 tables, full history (1976+) | 4 GB / 6 GB | Once |
| 81 | quarterly | All 5 tables, incremental | 2 GB / 3 GB | Quarterly |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `patent_grants` | Months 3, 6, 9, 12 | PatentsView quarterly bulk refresh |
| `patent_assignees` | Months 3, 6, 9, 12 | Same quarterly refresh cycle |
| `patent_inventors` | Months 3, 6, 9, 12 | Same quarterly refresh cycle |
| `patent_cpc_classes` | Months 3, 6, 9, 12 | Same quarterly refresh cycle |
| `trademark_applications` | Months 3, 6, 9, 12 | USPTO quarterly bulk data |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `PATENTS_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=patents`) |
| `PATENTS_CACHE_DIR` | Recommended | Raw download cache |
| `PATENTS_SINCE_YEAR` | Optional | Incremental start year (default: full history from 1976) |

---

## Implementation Notes

- PatentsView bulk TSV files are large: `g_inventor_disambiguated.tsv` is ~8 GB uncompressed.
  The initial load worker (80) needs `-t 480` (8-hour timeout) and should process tables
  sequentially rather than concurrently to avoid memory pressure.
- Assignee-to-CIK matching requires a fuzzy join step — PatentsView does not include CIK
  numbers. A pre-computed crosswalk table (generated once via name similarity against
  `sec.company_facts`) should be materialized as a view or separate reference table.
- Design patents (mark_type) are excluded from `patent_grants` by default; set
  `PATENTS_INCLUDE_DESIGN=true` to include them (adds ~1M rows/year).
