# Disasters Schema Data Plan

## Implementation Status (2026-07-16)

**Status: PARTIALLY DELIVERED.** All 7 planned tables are built (with transformers), plus
extension roll-up views. Several planned columns were renamed or not ingested; most importantly
`wildfire_perimeters.forest_id` — the bridge FK to `lands.national_forests` — is NOT built,
which blocks the planned wildfire→lands cross-schema join.

Verified against `disasters-schema.yaml` and `.../govdata/disasters/`.

| Planned table | Delivered as (transformer) | Notes |
|---|---|---|
| `disaster_declarations` | `disaster_declarations` (FemaDisasterDeclarationsTransformer) | Built |
| `public_assistance_projects` | `public_assistance_projects` (FemaPublicAssistanceTransformer) | Column renames |
| `hazard_mitigation_projects` | `hazard_mitigation_projects` (FemaHazardMitigationTransformer) | Built |
| `nfip_policies` | `nfip_policies` (FemaNfipPoliciesTransformer) | Per-transaction grain (not pre-aggregated) |
| `nfip_claims` | `nfip_claims` (FemaNfipClaimsTransformer) | Per-claim grain (not pre-aggregated) |
| `wildfire_perimeters` | `wildfire_perimeters` (WfigsPerimeterStreamingTransformer) | `forest_id` NOT built; static snapshot |
| `storm_events` | `storm_events` (StormEventsStreamingTransformer) | `narrative` NOT ingested |

**Extension views delivered beyond plan:** `disasters_declarations_by_state_year`,
`disasters_pa_obligated_by_county`, `disasters_nfip_claims_by_county`,
`disasters_storm_impact_by_county`.

**Missing columns / not yet built:**
- `wildfire_perimeters`: `forest_id` ABSENT — the bridge FK to `lands.national_forests` is NOT built, blocking the wildfire→lands cross-schema join; `agency` (lead agency FS/BLM/NPS) ABSENT — YAML has `poo_state`/`fire_cause`/`incident_type_category` instead; static snapshot, NOT `fire_year`-partitioned; renamed `cause`→`fire_cause`, `fire_type`→`incident_type_category`, `acres_burned`→`gis_acres`/`final_acres`.
- `storm_events`: `narrative` ABSENT (2000-char event narrative not ingested).
- `public_assistance_projects`: `project_number`→`pw_number`; `applicant_name`→`applicant_id` (name not stored); `category`→`damage_category_code`/`damage_category`.
- `nfip_policies`/`nfip_claims`: delivered at per-transaction/per-claim grain (plan described pre-aggregation by zip/county; aggregation deferred to the views); `zip_code`→`reported_zip_code`.

---

## Strategic Context

The `disasters` schema consolidates **discrete emergency event data** from three authoritative
federal sources: FEMA OpenFEMA (declarations, assistance, flood insurance), NIFC/WFIGS (wildfire
perimeters), and NOAA NCEI (storm events). The unifying theme is bounded incidents — each row
represents an event with a start date, geographic footprint, and measurable damage — as distinct
from the continuous meteorological measurements in the `weather` schema.

**Primary cross-schema value:**
- `disaster_declarations` + `sec.filing_metadata` — 10-K filings where the company's state
  experienced a major disaster during the fiscal year (risk disclosure research)
- `wildfire_perimeters` + `lands.national_forests` — burned area on National Forest land
  (timber revenue risk, USDA cost exposure)
- `storm_events` + `econ.bls_employment` — employment impact in disaster-affected counties
- `nfip_claims` + `wildfire_perimeters` — post-fire flood claims downstream of burn scars
- `disaster_declarations` + `fec.contributions` — fundraising spikes after major disasters

**Schema boundary:**
- In scope: discrete events (declarations, perimeters, storm episodes, paid claims)
- Out of scope: continuous conditions — drought indices, climate normals, daily observations
  (those live in the `weather` schema)

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Disaster Declarations Summaries | FEMA OpenFEMA | REST JSON | Free, no key | Daily |
| Public Assistance Funded Projects | FEMA OpenFEMA | REST JSON | Free, no key | Continuous |
| Hazard Mitigation Grant Projects | FEMA OpenFEMA | REST JSON | Free, no key | Monthly |
| NFIP Flood Insurance Policies | FEMA OpenFEMA | REST JSON | Free, no key | Monthly |
| NFIP Flood Insurance Claims | FEMA OpenFEMA | REST JSON | Free, no key | Monthly |
| Wildfire perimeters (current) | NIFC WFIGS | ArcGIS REST | Free, no key | Daily (fire season) |
| Wildfire perimeters (historical) | NIFC / USDA FS | ArcGIS REST | Free, no key | Annual |
| Storm events | NOAA NCEI | CSV bulk download | Free, no key | Monthly |

FEMA API base: `https://www.fema.gov/api/open/v2/`
WFIGS current: `https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_YTD/FeatureServer/0`
NOAA NCEI storm events: `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`

---

## Proposed Tables

### `disaster_declarations`

The canonical FEMA record. One row per disaster-declaration-county-program combination.

**Source:** `disasterDeclarationsSummaries`
**Partition:** `declaration_year`
**Auth:** None
**Cadence:** Daily (new declarations within 24h of presidential signature)
**Release window:** Every day (continuous feed)

| Column | Type | Description |
|---|---|---|
| disaster_number | VARCHAR | FEMA disaster number (e.g., DR-4611) |
| declaration_type | VARCHAR | DR (major), EM (emergency), FM (fire management), FS (fire suppression) |
| declaration_date | DATE | Date of presidential declaration |
| declaration_year | INTEGER | Partition key |
| incident_type | VARCHAR | Hurricane, Flood, Tornado, Wildfire, Earthquake, etc. |
| incident_begin_date | DATE | Start of incident period |
| incident_end_date | DATE | End of incident period (null if ongoing) |
| state_fips | VARCHAR | 2-digit state FIPS — FK to geo.states |
| county_fips | VARCHAR | 5-digit county FIPS — FK to geo.counties (null for statewide) |
| designated_area | VARCHAR | Human-readable area name |
| ih_program_declared | BOOLEAN | Individual and Households Program declared |
| ia_program_declared | BOOLEAN | Individual Assistance declared |
| pa_program_declared | BOOLEAN | Public Assistance declared |
| hm_program_declared | BOOLEAN | Hazard Mitigation declared |

---

### `public_assistance_projects`

FEMA Public Assistance grants to state/local governments and nonprofits for disaster recovery.
Largest dollar-value dataset — billions per major disaster.

**Source:** `publicAssistanceFundedProjectsDetails`
**Partition:** `declaration_year`
**Auth:** None
**Cadence:** Weekly (project awards finalized over months post-declaration)
**Release window:** Every day (continuous updates; weekly run sufficient)

| Column | Type | Description |
|---|---|---|
| project_number | VARCHAR | PA project identifier — delivered as `pw_number` |
| disaster_number | VARCHAR | FK to disaster_declarations |
| declaration_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| applicant_name | VARCHAR | Recipient organization name — delivered as `applicant_id` (name **NOT** stored) |
| category | VARCHAR | FEMA category (A=debris, B=protective measures, C-G=infrastructure) — delivered as `damage_category_code`/`damage_category` |
| project_amount | DOUBLE | Approved project cost (USD) |
| federal_share_obligated | DOUBLE | Federal obligation to date (USD) |
| total_obligated | DOUBLE | Total obligated including non-federal share (USD) |
| project_size | VARCHAR | Large (>$131k) or Small |
| damage_category | VARCHAR | Infrastructure type damaged |

---

### `hazard_mitigation_projects`

Pre- and post-disaster mitigation grants (BRIC, HMGP, FMA programs). Captures long-term
investment in resilience — useful for insurance risk, infrastructure, and climate adaptation analysis.

**Source:** `hazardMitigationGrantProgramProjectDetails`
**Partition:** `declaration_year`
**Auth:** None
**Cadence:** Monthly
**Release window:** Months 1–12 (continuous; monthly run sufficient)

| Column | Type | Description |
|---|---|---|
| project_identifier | VARCHAR | HMGP project ID |
| disaster_number | VARCHAR | FK to disaster_declarations (null for BRIC/FMA pre-disaster) |
| declaration_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| program_area | VARCHAR | HMGP, BRIC, FMA |
| project_type | VARCHAR | Acquisition, elevation, floodproofing, generators, etc. |
| project_title | VARCHAR | Project description |
| federal_share_obligated | DOUBLE | Federal obligation (USD) |
| project_status | VARCHAR | Open, Closed, Withdrawn |

---

### `nfip_policies`

National Flood Insurance Program in-force policy counts and coverage by ZIP/county.
Aggregated — not individual policies. Joins to geo.zctas on zip_code. — **delivered at per-transaction grain** (not pre-aggregated; aggregation deferred to the views)

**Source:** `fimaNfipPolicies`
**Partition:** `policy_year`
**Auth:** None
**Cadence:** Monthly
**Release window:** Months 1–12 (continuous)

| Column | Type | Description |
|---|---|---|
| policy_year | INTEGER | Partition key |
| policy_count | INTEGER | Number of in-force policies |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| zip_code | VARCHAR | ZIP code — FK to geo.zctas — delivered as `reported_zip_code` |
| coverage_amount | DOUBLE | Total building + contents coverage (USD) |
| total_written_premium | DOUBLE | Annual premium (USD) |
| occupancy_type | VARCHAR | Single family, 2-4 family, other residential, non-residential |
| flood_zone | VARCHAR | FEMA flood zone (AE, AH, AO, VE, X, etc.) |

---

### `nfip_claims`

Paid flood insurance claims by county and year. Key signal for disaster severity and
uninsured loss estimation. Joins to disaster_declarations on county/date overlap. — **delivered at per-claim grain** (not pre-aggregated; aggregation deferred to the views)

**Source:** `fimaNfipClaims`
**Partition:** `year_of_loss`
**Auth:** None
**Cadence:** Monthly
**Release window:** Months 1–12 (continuous)

| Column | Type | Description |
|---|---|---|
| year_of_loss | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| zip_code | VARCHAR | FK to geo.zctas — delivered as `reported_zip_code` |
| flood_zone | VARCHAR | FEMA flood zone at time of loss |
| number_of_claims | INTEGER | Claim count (aggregated) |
| total_building_payment | DOUBLE | Total building claims paid (USD) |
| total_contents_payment | DOUBLE | Total contents claims paid (USD) |
| total_payment | DOUBLE | Total paid (USD) |
| occupancy_type | VARCHAR | Residential / non-residential |

---

### `wildfire_perimeters`

Current and historical wildfire perimeters from WFIGS (Wildland Fire Interagency Geospatial
Services). One row per fire incident. Joins to `disaster_declarations` on county/date for
FEMA fire management declarations; joins to `lands.national_forests` on `forest_id` for
timber revenue impact; joins to `nfip_claims` for post-fire flood exposure estimation.
**Delivered as a static snapshot, NOT `fire_year`-partitioned; `forest_id` NOT built, so the
`lands.national_forests` bridge join is unavailable.**

**Source:** NIFC WFIGS — `wfigs_perimeters_current_year` (daily) + historical archive (annual)
**Partition:** `fire_year`
**Auth:** None
**Cadence:** Daily during fire season (May–November); annual archive otherwise
**Release window:** Months 5–11 for daily updates; month 1 for prior-year archive

| Column | Type | Description |
|---|---|---|
| incident_id | VARCHAR | IRWIN/WFIGS incident identifier |
| incident_name | VARCHAR | Fire name |
| fire_year | INTEGER | Partition key |
| discovery_date | DATE | Date fire was reported |
| containment_date | DATE | Date fire was 100% contained (null if active) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (primary county of origin) |
| cause | VARCHAR | Human / Lightning / Undetermined — delivered as `fire_cause` |
| fire_type | VARCHAR | Wildfire / Prescribed / Unknown — delivered as `incident_type_category` |
| acres_burned | DOUBLE | Final (or current) fire perimeter area in acres — delivered as `gis_acres`/`final_acres` |
| forest_id | VARCHAR | FK to lands.national_forests (null if not on NF land) — **NOT BUILT** (blocks wildfire→lands cross-schema join) |
| agency | VARCHAR | Lead agency (FS, BLM, NPS, BIA, State, Local) — **NOT BUILT** (YAML has `poo_state` instead) |
| geometry_wkt | VARCHAR | Fire perimeter polygon WKT (simplified) |

---

### `storm_events`

NOAA NCEI Storm Events Database — one row per storm episode detail record. Covers all
significant weather events (hurricanes, tornadoes, floods, hail, winter storms, etc.)
from 1950 onward. Joins to `disaster_declarations` on county/date overlap to link storm
episodes to FEMA declarations; joins to `econ.bls_employment` for economic impact analysis.

**Source:** NOAA NCEI bulk CSV — `StormEvents_details-ftp_v1.0_dYEAR_cDATE.csv.gz`
**Partition:** `event_year`
**Auth:** None
**Cadence:** Monthly (NCEI updates current-year file monthly; prior years finalized ~3 months after year-end)
**Release window:** Months 1–12 (continuous; monthly run refreshes current-year estimates)

| Column | Type | Description |
|---|---|---|
| event_id | VARCHAR | NCEI storm event ID |
| episode_id | VARCHAR | Episode ID (groups related events) |
| event_year | INTEGER | Partition key |
| begin_date | DATE | Event start date |
| end_date | DATE | Event end date |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null for marine/offshore) |
| event_type | VARCHAR | Tornado, Flash Flood, Hurricane, Blizzard, Wildfire, etc. |
| magnitude | DOUBLE | Event magnitude (wind speed, hail size, etc.; type-dependent) |
| magnitude_type | VARCHAR | EG (estimated gust), ES (sustained), MS (measured), etc. |
| injuries_direct | INTEGER | Direct injuries |
| injuries_indirect | INTEGER | Indirect injuries |
| deaths_direct | INTEGER | Direct fatalities |
| deaths_indirect | INTEGER | Indirect fatalities |
| damage_property | DOUBLE | Property damage (USD, converted from K/M/B notation) |
| damage_crops | DOUBLE | Crop damage (USD) |
| narrative | VARCHAR | Event narrative (truncated to 2000 chars) — **NOT BUILT** (not ingested) |
| cz_type | VARCHAR | C (county), Z (NWS forecast zone), M (marine) |

---

## Join Architecture

```
geo.counties (county_fips)
    ├── disaster_declarations (county_fips, declaration_year)
    │       ├── public_assistance_projects (disaster_number, county_fips)
    │       ├── hazard_mitigation_projects (disaster_number, county_fips)
    │       └── [date overlap] sec.filing_metadata (state_of_incorporation)
    │                          econ.bls_employment (county_fips, year)
    │                          edu.ipeds_institutions (county_fips)
    ├── wildfire_perimeters (county_fips, fire_year)
    │       ├── lands.national_forests (forest_id → NF land impact)   # NOT BUILT (forest_id absent)
    │       └── nfip_claims (county_fips + year → post-fire flood claims)
    └── storm_events (county_fips, event_year)
            └── [date overlap] disaster_declarations (incident_type match)

geo.zctas (zip_code)
    ├── nfip_policies (zip_code, policy_year)
    └── nfip_claims (zip_code, year_of_loss)
```

**Schema boundary notes:**
- `weather` schema: drought monitor, GHCN-Daily observations, climate normals — continuous
  measurements. Join to `disasters` on county/year for disaster context.
- `forests` schema: national forest boundaries, timber sales, forest inventory — land management
  data. `wildfire_perimeters.forest_id` bridges the two schemas. — **NOT BUILT** (`forest_id` absent; bridge unavailable)

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 74 | initial | All 7 tables, full history | 4 GB / 6 GB | Once |
| 75 | daily | `disaster_declarations`, `wildfire_perimeters` | 2 GB / 3 GB | Daily |
| 76 | monthly | `public_assistance_projects`, `hazard_mitigation_projects`, `nfip_policies`, `nfip_claims`, `storm_events` | 2 GB / 3 GB | Monthly |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `disaster_declarations` | Every day | Declarations issued continuously |
| `public_assistance_projects` | Every day | Projects awarded over months post-declaration |
| `wildfire_perimeters` | Months 5–11 (daily); month 1 (annual archive) | Active fire season + prior-year archive |
| `storm_events` | Months 1–12 | NCEI monthly refresh of current-year estimates |
| `hazard_mitigation_projects` | Months 1–12 | Continuous; monthly run captures all updates |
| `nfip_policies` | Months 1–12 | FEMA publishes monthly snapshots |
| `nfip_claims` | Months 1–12 | FEMA publishes monthly snapshots |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DISASTERS_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=disasters`) |
| `DISASTERS_CACHE_DIR` | Recommended | Raw download cache |
| `DISASTERS_SINCE_YEAR` | Optional | Incremental load start year for historical backfill |
| `DISASTERS_FIRE_SEASON_ONLY` | Optional | If `true`, wildfire_perimeters worker skips outside May–Nov |
