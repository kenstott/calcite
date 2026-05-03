# Public Lands Schema Data Plan

## Strategic Context

The `lands` schema covers U.S. federal public land units and their associated economic,
ecological, and visitation data across three agencies: USDA Forest Service (USFS),
National Park Service (NPS), and Bureau of Land Management (BLM). Together these agencies
manage ~640 million acres — roughly 28% of U.S. land.

**Primary cross-schema value:**
- `national_forests` + `disasters.wildfire_perimeters` — burned NF acreage driving USFS
  suppression cost and SEC timber/insurance risk disclosures
- `timber_sales` + `sec.filing_metadata` — Forest Service contract volume as a leading
  indicator for publicly traded timber/paper companies
- `nps_visitation` + `econ.bls_employment` — tourism employment in gateway counties
- `nps_units` + `fec.contributions` — donor geography relative to park proximity
- `blm_field_offices` + `econ.fred_indicators` — extractive lease revenue by region

**Schema boundary:**
- In scope: land management unit boundaries, economic activity on public land (timber sales,
  concessions, leases), biological inventories, and visitation
- Out of scope: wildfire perimeters (discrete events → `disasters` schema); weather
  observations on public land (→ `weather` schema)

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| National Forest boundaries | USDA FS ArcGIS | REST feature service | Free, no key | Annual |
| Timber sale program (FACTS) | USDA FS | CSV bulk download | Free, no key | Quarterly |
| Forest inventory (FIA) | USDA FS FIA DataMart | REST API | Free, no key | Annual |
| NPS unit boundaries | NPS ArcGIS | REST feature service | Free, no key | Annual |
| NPS visitation statistics | NPS IRMA | REST API | Free, no key | Monthly |
| BLM field office boundaries | BLM ArcGIS | REST feature service | Free, no key | Annual |

USDA FS ArcGIS: `https://apps.fs.usda.gov/arcx/rest/services/EDW/`
NPS ArcGIS: `https://mapservices.nps.gov/arcgis/rest/services/`
NPS IRMA stats: `https://irma.nps.gov/Stats/api/v1/`
BLM ArcGIS: `https://gis.blm.gov/arcgis/rest/services/`

---

## Proposed Tables

### `national_forests`

National Forest and Grassland unit boundaries with acreage and region.
Reference/dimension table — joins to counties via spatial overlap (county_fips pre-assigned).
FK target for `disasters.wildfire_perimeters.forest_id`.

**Source:** USDA FS ArcGIS — `EDW_ForestSystemBoundaries_01`
**Partition:** None (static reference, ~175 units)
**Auth:** None
**Cadence:** Annual
**Release window:** Month 10 (October — post-fire-season administrative updates)

| Column | Type | Description |
|---|---|---|
| forest_id | VARCHAR | USDA FS unit ID (e.g., `0501`) |
| forest_name | VARCHAR | Unit name (e.g., "Angeles National Forest") |
| region | VARCHAR | USDA FS region number (01–10) |
| state_fips | VARCHAR | Primary state FIPS — FK to geo.states |
| gross_acres | DOUBLE | Total unit area in acres |
| proclaimed_acres | DOUBLE | Proclaimed National Forest area |
| geometry_wkt | VARCHAR | Simplified boundary WKT (for display) |

---

### `timber_sales`

USDA Forest Service timber sale program — contracts awarded by National Forest and year.
Joins to `national_forests` on `forest_id` and to SEC timber/paper company filings via state.

**Source:** USDA FS timber sale program CSV bulk downloads (FACTS data)
**Partition:** `sale_year`
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| contract_id | VARCHAR | FS timber sale contract number |
| sale_name | VARCHAR | Sale name |
| sale_year | INTEGER | Partition key (year of contract award) |
| forest_id | VARCHAR | FK to national_forests |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| sale_type | VARCHAR | Lump sum / Scale / Stewardship / Other |
| volume_mbf | DOUBLE | Volume sold (thousand board feet) |
| appraised_value | DOUBLE | Appraised stumpage value (USD) |
| sale_value | DOUBLE | Awarded sale value (USD) |
| species_group | VARCHAR | Softwood / Hardwood / Mixed |
| advertised_date | DATE | Date of advertisement |
| award_date | DATE | Date of contract award |

---

### `forest_inventory`

USDA Forest Service Forest Inventory and Analysis (FIA) — state-level forest area,
volume, and carbon stock estimates. Joins to `geo.states` and `econ.bls_employment`.

**Source:** USDA FS FIA DataMart REST API (`https://apps.fs.usda.gov/fia/datamart/`)
**Partition:** `inventory_year`
**Auth:** None
**Cadence:** Annual
**Release window:** Months 6–9 (FIA annual estimates typically released summer)

| Column | Type | Description |
|---|---|---|
| inventory_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| forest_type_group | VARCHAR | Forest type (e.g., Oak-Hickory, Douglas-fir, Ponderosa pine) |
| ownership_class | VARCHAR | National Forest / Other Federal / State / Private |
| land_area_acres | DOUBLE | Forested land area in acres |
| live_volume_cuft | DOUBLE | Live tree volume (cubic feet) |
| carbon_stock_tons | DOUBLE | Above-ground carbon stock (short tons) |
| trees_per_acre | DOUBLE | Average tree density |
| basal_area_sqft | DOUBLE | Basal area per acre (tree diameter proxy) |

---

### `nps_units`

National Park Service unit boundaries — parks, monuments, recreation areas, seashores,
parkways, and all other NPS-designated units. Reference/dimension table; joins to counties
via spatial overlap. FK target for `nps_visitation.unit_code`.

**Source:** NPS ArcGIS — `NPS_Land_Resources/MapServer/2` (NPS boundary polygons)
**Partition:** None (static reference, ~430 units)
**Auth:** None
**Cadence:** Annual
**Release window:** Month 4 (April — post-annual-report administrative updates)

| Column | Type | Description |
|---|---|---|
| unit_code | VARCHAR | NPS 4-letter unit code (e.g., `YOSE`, `GRCA`) |
| unit_name | VARCHAR | Full unit name |
| unit_type | VARCHAR | National Park / Monument / Recreation Area / Seashore / etc. |
| state_fips | VARCHAR | Primary state FIPS — FK to geo.states |
| county_fips | VARCHAR | Primary county FIPS — FK to geo.counties |
| gross_acres | DOUBLE | Total unit area in acres |
| established_date | DATE | Date of establishment |
| region | VARCHAR | NPS region (e.g., "Pacific West", "Northeast") |
| geometry_wkt | VARCHAR | Simplified boundary WKT (for display) |

---

### `nps_visitation`

NPS annual and monthly visitation statistics by unit. Covers recreational visits,
overnight stays, backcountry use, and concession revenue. Joins to `geo.counties`
for gateway-community economic impact analysis; joins to `econ.bls_employment`
for tourism employment correlation.

**Source:** NPS IRMA Statistics API — `https://irma.nps.gov/Stats/api/v1/`
**Partition:** `visit_year`
**Auth:** None
**Cadence:** Monthly (current year updated monthly; prior years finalized ~March)
**Release window:** Months 1–12 (continuous; February–April strongest for prior-year finals)

| Column | Type | Description |
|---|---|---|
| unit_code | VARCHAR | FK to nps_units |
| visit_year | INTEGER | Partition key |
| visit_month | INTEGER | Month (1–12; null for annual totals) |
| recreation_visits | INTEGER | Recreational visitor count |
| recreation_hours | DOUBLE | Total recreational visitor hours |
| tent_campers | INTEGER | Tent/trailer camper nights |
| rv_campers | INTEGER | RV camper nights |
| backcountry_campers | INTEGER | Backcountry camper nights |
| non_recreation_visits | INTEGER | Non-recreational visits (commuters, government, etc.) |
| concession_lodging_nights | INTEGER | Concessioner lodging guest nights |

---

### `blm_field_offices`

BLM administrative unit boundaries — field offices and district offices that manage
federal subsurface mineral rights, grazing allotments, and surface use permits.
Primarily useful as a geographic reference for joining extractive industry activity
to SEC filings and FEC oil/gas sector donor geography.

**Source:** BLM ArcGIS — `BLM_National_Administrative_Units/FeatureServer`
**Partition:** None (static reference, ~150 field offices)
**Auth:** None
**Cadence:** Annual
**Release window:** Month 6 (annual administrative boundary updates)

| Column | Type | Description |
|---|---|---|
| office_code | VARCHAR | BLM field office code |
| office_name | VARCHAR | Office name (e.g., "Moab Field Office") |
| office_type | VARCHAR | Field Office / District Office / State Office |
| state_fips | VARCHAR | FK to geo.states |
| adm_acres | DOUBLE | Administered surface acreage |
| geometry_wkt | VARCHAR | Simplified boundary WKT (for display) |

---

### `onrr_revenues`

Office of Natural Resources Revenue (ONRR) production volumes and royalty payments for
oil, gas, coal, and other minerals extracted from federal and tribal lands. One row per
commodity-revenue-type-period combination by state and county. This is the primary
financial link between BLM-managed subsurface rights and SEC 10-K disclosures for E&P
and mining companies.

**Source:** ONRR Natural Resources Revenue Data — `https://revenuedata.doi.gov/downloads/`
Bulk CSV exports updated monthly. Free, no key.
**Partition:** `revenue_year`
**Auth:** None
**Cadence:** Monthly (prior-month production data published ~3 months in arrears)
**Release window:** Months 1–12 (continuous; lag means month N data arrives month N+3)

| Column | Type | Description |
|---|---|---|
| revenue_year | INTEGER | Partition key |
| revenue_month | INTEGER | Month (1–12) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null for offshore/tribal) |
| land_class | VARCHAR | Federal / Native American / Federal Offshore |
| land_category | VARCHAR | Onshore / Offshore / Gulf of Mexico / Pacific |
| commodity | VARCHAR | Oil / Gas / Coal / Geothermal / Renewables / Other |
| revenue_type | VARCHAR | Royalties / Bonus / Rents / Other Revenues |
| product | VARCHAR | Specific product (e.g., "Coalbed Methane", "Crude Oil") |
| volume | DOUBLE | Production volume (units vary by commodity) |
| volume_unit | VARCHAR | BBL / MCF / TON / KWH / ACRE |
| revenue | DOUBLE | Revenue collected (USD) |
| company_name | VARCHAR | Operating company name (fuzzy-join to sec.filing_metadata) |

**Cross-schema joins:**
- `company_name` fuzzy-matched to `sec.filing_metadata.company_name` identifies federal
  royalty exposure for publicly traded E&P and mining companies
- `county_fips` + `revenue_year` joins to `econ.bls_employment` for energy sector employment
  correlation with production volumes

---

## Join Architecture

```
geo.counties (county_fips)
    ├── timber_sales (county_fips, sale_year)
    │       └── sec.filing_metadata (state → timber/paper company 10-K disclosures)
    └── nps_units (county_fips → gateway county)
            └── nps_visitation (unit_code, visit_year)
                    └── econ.bls_employment (county_fips → tourism employment)

geo.states (state_fips)
    └── forest_inventory (state_fips, inventory_year)
            └── econ.bls_employment (state_fips → forestry/logging employment)

national_forests (forest_id)
    ├── disasters.wildfire_perimeters (forest_id → burned NF acreage)
    └── timber_sales (forest_id)

blm_field_offices (office_code)
    └── [spatial] geo.counties (county_fips → oil/gas lease county attribution)

geo.counties (county_fips)
    └── onrr_revenues (county_fips, revenue_year)
            └── sec.filing_metadata (company_name ~ operator → E&P royalty exposure)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 77 | initial | All 7 tables, full history | 4 GB / 6 GB | Once |
| 78 | annual | `national_forests`, `timber_sales`, `forest_inventory`, `nps_units`, `blm_field_offices` | 2 GB / 3 GB | Annual |
| 79 | monthly | `nps_visitation`, `onrr_revenues` | 2 GB / 3 GB | Monthly |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `national_forests` | Month 10 | Post-fire-season boundary updates |
| `timber_sales` | Months 3, 6, 9, 12 | Quarterly FACTS data releases |
| `forest_inventory` | Months 6–9 | FIA annual estimates released summer |
| `nps_units` | Month 4 | Post-annual-report boundary updates |
| `nps_visitation` | Months 1–12 | NPS publishes monthly stats; Feb–Apr for prior-year finals |
| `blm_field_offices` | Month 6 | Annual administrative boundary updates |
| `onrr_revenues` | Months 1–12 | ONRR monthly; ~3-month publication lag |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `LANDS_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=lands`) |
| `LANDS_CACHE_DIR` | Recommended | Raw download cache |
| `LANDS_SINCE_YEAR` | Optional | Incremental start year for timber sales and NPS visitation |

---

## Implementation Notes

- NPS IRMA API returns monthly stats per unit via `/report/` endpoint with `UnitCode`,
  `Year`, and `Month` parameters. For initial load, iterate all unit codes × years 1904–present.
  Most units have data from 1979+; the 1904 records are for a handful of flagship parks.
- BLM field office boundaries are useful primarily as a spatial reference; BLM does not
  publish standardized extractive revenue by field office via open API. Join to SEC 10-K
  filings via operator state as a proxy until a royalty data source is added.
- NPS unit boundaries occasionally have overlapping polygons (e.g., national recreation areas
  within national forests). The `county_fips` column stores the primary county of the unit
  centroid; multi-county units may need a separate crosswalk for full county attribution.
