# Transport Schema Data Plan

## Status: DELIVERED (2026-07-16)

All planned tables are **implemented and merged to main** (`059efbbb0`). They ship in [transport-schema.yaml](../../../src/main/resources/transport/transport-schema.yaml) with transformers under [org.apache.calcite.adapter.govdata.transport](../../../src/main/java/org/apache/calcite/adapter/govdata/transport/).

| Planned table | Delivered as | Java |
|---|---|---|
| `bts_airline_performance` | `airline_ontime` + `t100_segments` (split by BTS source) | `BtsT100DataProvider` (t100); airline_ontime = CSV passthrough |
| `faa_aircraft_registry` | tables `faa_aircraft_master` + `faa_aircraft_reference` + `faa_engine_reference` + VIEW `faa_aircraft_registry` | CSV passthrough over `ReleasableAircraft.zip`; view does the join/decodes |
| `ntsb_aviation_accidents` | `ntsb_aviation_accidents` | `NtsbAviationTransformer` (Jackcess, avall.mdb) |
| `fmcsa_carriers` | `fmcsa_carriers` | Socrata bulk-CSV passthrough |
| `bts_freight_flows` | table `cfs_shipments` (CFS PUF microdata) + VIEW `bts_freight_flows` | CSV passthrough; view = weighted O-D aggregation |

**Delivery deviations:**
- One-table-per-source rule split several planned tables (airline perf → 2, FAA registry → 3 faithful tables + a view, CFS → microdata table + view).
- FMCSA safety-event columns (`crashes_fatal/injury/tow`, `out_of_service_rate`, `total_inspections`) are **not** in `fmcsa_carriers` — those live in separate SMS datasets, deferred to future companion tables + a rollup view.
- NTSB grain is event×aircraft with no `county_fips` (the MDB lacks it) — state_fips only.
- Six tables beyond plan shipped: `vehicle_recalls`, `safety_complaints`, `fatal_crashes`, `airports`, `transit_ridership`, `vehicle_registrations`.

The proposal below is preserved for historical context.

## Strategic Context

The U.S. Department of Transportation and its sub-agencies publish comprehensive
operational data for all major transport modes. The `transport` schema's primary
cross-schema value is **transportation company financial analysis and infrastructure risk**:
BTS airline statistics join directly to SEC 10-K filings for carriers; FMCSA safety ratings
identify regulated trucking companies; FAA accident data joins to NTSB investigations for
aviation safety research; freight flows join to ECON county wages for supply chain analysis.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Airline on-time statistics (Form 41) | BTS TranStats | CSV bulk download | Free, no key | Monthly |
| FAA aircraft registry | FAA | CSV bulk download | Free, no key | Monthly |
| NTSB aviation accidents | NTSB | CSV bulk download | Free, no key | Continuous |
| FMCSA carrier safety (SAFER) | FMCSA | CSV bulk download + API | Free, no key | Continuous |
| Commodity Flow Survey (CFS) | BTS / Census | CSV bulk download | Free, no key | Quinquennial |

BTS TranStats: `https://www.transtats.bts.gov/`
FAA Registry: `https://registry.faa.gov/database/ReleasableAircraft.zip`
NTSB: `https://www.ntsb.gov/investigations/AccidentReports/`
FMCSA SAFER: `https://ai.fmcsa.dot.gov/SMS/`

---

## Proposed Tables

### `bts_airline_performance`

Monthly airline on-time performance, passenger traffic, and operating statistics by
carrier and origin-destination market. Joins to `sec.filing_metadata` for publicly
traded airlines; joins to `geo.counties` via airport for regional traffic analysis.

**Source:** BTS TranStats — On-Time Performance (Marketing Carrier) + T-100 Traffic Data
**Partition:** `report_year`
**Auth:** None
**Cadence:** Monthly (2-month lag)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| report_year | INTEGER | Partition key |
| report_month | INTEGER | Month (1–12) |
| carrier_code | VARCHAR | IATA carrier code |
| carrier_name | VARCHAR | Airline name (fuzzy-join to sec.filing_metadata) |
| origin_airport | VARCHAR | IATA origin airport code |
| dest_airport | VARCHAR | IATA destination airport code |
| origin_state_fips | VARCHAR | FK to geo.states |
| dest_state_fips | VARCHAR | FK to geo.states |
| flights_scheduled | INTEGER | Scheduled departures |
| flights_operated | INTEGER | Completed departures |
| passengers | INTEGER | Revenue passengers enplaned |
| seats | INTEGER | Available seats |
| load_factor | DOUBLE | Passengers / seats ratio |
| avg_delay_minutes | DOUBLE | Average departure delay (minutes) |
| cancellations | INTEGER | Cancelled flights |
| diversions | INTEGER | Diverted flights |
| operating_revenue | DOUBLE | Operating revenue (USD) |
| operating_expense | DOUBLE | Operating expense (USD) |

---

### `faa_aircraft_registry`

Monthly snapshot of the FAA civil aircraft registry. Reference table for aviation
sector analysis — joins to NTSB accidents on N-number; joins to `sec.filing_metadata`
for fleet ownership by publicly traded leasing companies and airlines.

**Source:** FAA Releasable Aircraft Registry — monthly ZIP download
**Partition:** None (latest snapshot; append-only historical via snapshot_month)
**Auth:** None
**Cadence:** Monthly
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| n_number | VARCHAR | FAA N-number (tail number) |
| snapshot_month | DATE | Month of snapshot |
| serial_number | VARCHAR | Manufacturer serial number |
| manufacturer | VARCHAR | Manufacturer name |
| model | VARCHAR | Aircraft model |
| aircraft_category | VARCHAR | Land / Sea / Amphibian |
| aircraft_type | VARCHAR | Fixed Wing Single Engine / Rotorcraft / Balloon / etc. |
| engine_type | VARCHAR | Reciprocating / Turbo-prop / Turbo-jet / Electric / etc. |
| year_manufactured | INTEGER | Year of manufacture |
| registrant_name | VARCHAR | Owner/registrant name |
| registrant_type | VARCHAR | Individual / Partnership / Corporation / Government / etc. |
| state_fips | VARCHAR | Registrant state — FK to geo.states |
| county_fips | VARCHAR | Registrant county — FK to geo.counties |
| status | VARCHAR | Valid / Administrative / Triennial / Expired / etc. |
| airworthiness_date | DATE | Date of original airworthiness certificate |

---

### `ntsb_aviation_accidents`

NTSB aviation accident and incident database. One row per investigation. Joins to
`faa_aircraft_registry` on N-number; joins to `geo.counties` for geographic risk
analysis; joins to `bts_airline_performance` on carrier for safety-vs-operations research.

**Source:** NTSB Aviation Accident Database — `https://data.ntsb.gov/carol-main-public/`
**Partition:** `event_year`
**Auth:** None
**Cadence:** Continuous (initial reports within 24–72 hours; final reports 12–18 months)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| event_id | VARCHAR | NTSB event ID |
| event_year | INTEGER | Partition key |
| event_date | DATE | Date of accident/incident |
| aircraft_n_number | VARCHAR | FK to faa_aircraft_registry |
| carrier_name | VARCHAR | Operator/carrier name |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| airport_id | VARCHAR | FAA airport identifier (null for en-route) |
| event_type | VARCHAR | Accident / Incident |
| investigation_type | VARCHAR | NTSB / FAA |
| injury_severity | VARCHAR | Fatal / Serious / Minor / None |
| fatal_injuries | INTEGER | Fatalities |
| serious_injuries | INTEGER | Serious injuries |
| aircraft_damage | VARCHAR | Destroyed / Substantial / Minor / None |
| phase_of_flight | VARCHAR | Takeoff / Cruise / Landing / etc. |
| primary_cause | VARCHAR | NTSB probable cause summary |

---

### `fmcsa_carriers`

FMCSA motor carrier safety data — operating authority, safety ratings, inspection
history, and crash records for interstate trucking companies. Joins to `sec.filing_metadata`
for publicly traded trucking companies (XPO, J.B. Hunt, Werner, etc.); joins to `geo.states`
for interstate operations mapping.

**Source:** FMCSA SAFER System — `https://ai.fmcsa.dot.gov/SMS/` bulk download
**Partition:** None (current snapshot; `snapshot_date` for history)
**Auth:** None
**Cadence:** Monthly
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| dot_number | VARCHAR | USDOT number (primary key) |
| snapshot_date | DATE | Date of data snapshot |
| carrier_name | VARCHAR | Legal carrier name (fuzzy-join to sec.filing_metadata) |
| dba_name | VARCHAR | DBA name |
| state_fips | VARCHAR | Principal office state — FK to geo.states |
| carrier_type | VARCHAR | Carrier / Broker / Freight Forwarder |
| operation_type | VARCHAR | Interstate / Intrastate Hazmat / Intrastate Non-Hazmat |
| safety_rating | VARCHAR | Satisfactory / Conditional / Unsatisfactory / Not Rated |
| safety_rating_date | DATE | Date of most recent rating |
| total_drivers | INTEGER | Number of CDL drivers |
| total_power_units | INTEGER | Trucks and tractors |
| total_inspections | INTEGER | Vehicle inspections (rolling 24 months) |
| out_of_service_rate | DOUBLE | OOS violations / inspections |
| crashes_fatal | INTEGER | Fatal crashes (rolling 24 months) |
| crashes_injury | INTEGER | Injury crashes |
| crashes_tow | INTEGER | Tow-away crashes |
| hazmat_flag | BOOLEAN | Authorized to transport hazardous materials |

---

### `bts_freight_flows`

Bureau of Transportation Statistics Commodity Flow Survey — freight shipments by
commodity type, origin state, destination state, and transport mode. Published
quinquennially (2017, 2022, etc.). Joins to `econ.bls_employment` for logistics
employment by mode; joins to `agr.nass_crop_production` for agricultural freight
volume estimation.

**Source:** BTS / Census CFS public use microdata — bulk CSV
**Partition:** `survey_year`
**Auth:** None
**Cadence:** Quinquennial (every 5 years)
**Release window:** Year+2 (2022 survey → 2024 publication)

| Column | Type | Description |
|---|---|---|
| survey_year | INTEGER | Partition key |
| origin_state_fips | VARCHAR | FK to geo.states (origin state) |
| dest_state_fips | VARCHAR | FK to geo.states (destination state) |
| naics_sector | VARCHAR | Shipper NAICS sector |
| commodity_sctg | VARCHAR | SCTG commodity code |
| commodity_description | VARCHAR | Commodity description |
| mode | VARCHAR | Truck / Rail / Air / Water / Pipeline / Multiple modes |
| shipments_thousands | DOUBLE | Estimated annual shipments (thousands) |
| value_million | DOUBLE | Estimated value of shipments (USD millions) |
| tons_thousands | DOUBLE | Estimated tonnage (thousands of short tons) |
| ton_miles_million | DOUBLE | Estimated ton-miles (millions) |

---

## Join Architecture

```
sec.filing_metadata (company_name)
    ├── bts_airline_performance (carrier_name ~ company_name)
    └── fmcsa_carriers (carrier_name ~ company_name)

geo.counties (county_fips)
    ├── ntsb_aviation_accidents (county_fips, event_year)
    └── faa_aircraft_registry (county_fips → registrant location)
            └── ntsb_aviation_accidents (n_number → accident history)

geo.states (state_fips)
    ├── bts_airline_performance (origin_state_fips / dest_state_fips)
    └── bts_freight_flows (origin_state_fips, survey_year)
            └── econ.bls_employment (state_fips → logistics employment)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 88 | initial | All 5 tables, full history | 4 GB / 6 GB | Once |
| 89 | monthly | `bts_airline_performance`, `faa_aircraft_registry`, `ntsb_aviation_accidents`, `fmcsa_carriers` | 2 GB / 3 GB | Monthly |

*`bts_freight_flows` updates only on survey years (every 5 years); include in initial + manual refresh.*

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `bts_airline_performance` | Months 1–12 | Monthly BTS release with 2-month lag |
| `faa_aircraft_registry` | Months 1–12 | Monthly FAA snapshot |
| `ntsb_aviation_accidents` | Months 1–12 | Continuous NTSB updates |
| `fmcsa_carriers` | Months 1–12 | Monthly SAFER snapshot |
| `bts_freight_flows` | Survey year +2 | Quinquennial CFS publication |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TRANSPORT_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=transport`) |
| `TRANSPORT_CACHE_DIR` | Recommended | Raw download cache |
| `TRANSPORT_SINCE_YEAR` | Optional | Incremental start year (default: 2000) |
