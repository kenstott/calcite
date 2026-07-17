# Public Lands Schema Data Plan

## Status: DELIVERED

All eight planned tables are **implemented and merged to main**. They ship in [lands-schema.yaml](../../../src/main/resources/lands/lands-schema.yaml) with transformers under [org.apache.calcite.adapter.govdata.lands](../../../src/main/java/org/apache/calcite/adapter/govdata/lands/).

| Planned table | Delivered as | Java |
|---|---|---|
| `national_forests` | `national_forests` | `UsfsForestBoundaryTransformer` |
| `timber_sales` | `timber_sales` | `UsfsFactsTransformer` (+ `UsfsTimberLayerResolver`) |
| `forest_inventory` | `forest_inventory` | `FiaDatamartTransformer` |
| `forest_metrics` | `forest_metrics` | `FiaMetricsTransformer` |
| `nps_units` | `nps_units` | `NpsUnitBoundaryTransformer` |
| `nps_visitation` | `nps_visitation` | `NpsIrmaTransformer` |
| `blm_field_offices` | `blm_field_offices` | `BlmFieldOfficeTransformer` |
| `onrr_revenues` | `onrr_revenues` | `OnrrRevenueTransformer` |

**Delivery deviations:**
- `forest_metrics` remains a CONDPROP-weighted **approximation**; proper population-level FIA estimation (EXPNS / POP_EVAL expansion factors) is still deferred — even though the `fia_pop_evaluations` inputs now land as an extension table.
- `national_forests.gis_acres` delivered as `gross_acres`.
- Six extension FIA tables beyond plan: `fia_plots`, `fia_tree_grm`, `fia_seedlings`, `fia_down_woody_debris`, `fia_invasives`, `fia_pop_evaluations`.

The proposal below is preserved for historical context.

## Strategic Context

The `lands` schema covers U.S. federal public land units and their associated economic,
ecological, and visitation data across three agencies: USDA Forest Service (USFS),
National Park Service (NPS), and Bureau of Land Management (BLM). Together these agencies
manage ~640 million acres — roughly 28% of U.S. land.

**Primary cross-schema value:**
- `national_forests` + `disasters.wildfire_perimeters` — burned NF acreage driving USFS
  suppression cost and SEC timber/insurance risk disclosures
- `timber_sales` + `sec.filing_metadata` — harvest activity by state as a leading
  indicator for publicly traded timber/paper companies
- `nps_visitation` + `econ.bls_employment` — tourism employment in gateway counties
- `nps_units` + `fec.contributions` — donor geography relative to park proximity
- `blm_field_offices` + `econ.fred_indicators` — extractive lease revenue by region
- `onrr_revenues` + `sec.filing_metadata` — federal royalty exposure for publicly traded
  E&P and mining companies (join on state or commodity)

**Schema boundary:**
- In scope: land management unit boundaries, economic activity on public land (timber
  harvest, leases), biological inventories, and visitation
- Out of scope: wildfire perimeters (discrete events → `disasters` schema); weather
  observations on public land (→ `weather` schema)

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| National Forest boundaries | USDA FS ArcGIS | MapServer REST | Free, no key | Annual |
| Timber harvest activities (FACTS) | USDA FS ArcGIS | MapServer REST | Free, no key | Quarterly |
| Forest inventory (FIA) | USDA FS FIA DataMart | REST API | Free, no key | Annual |
| NPS unit boundaries | NPS / Esri ArcGIS Online | FeatureServer REST | Free, no key | Annual |
| NPS visitation statistics | NPS IRMA | REST API (XML) | Free, no key | Monthly |
| BLM field office boundaries | BLM ArcGIS | FeatureServer REST | Free, no key | Annual |
| ONRR mineral revenues | ONRR | Bulk CSV | Free, no key | Monthly |

**Actual service endpoints (as of 2026):**

| Table | URL base |
|---|---|
| `national_forests` | `https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_ForestSystemBoundaries_01/MapServer/0` |
| `timber_sales` | `https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_TimberHarvest_01/MapServer/0` |
| `forest_inventory` | `https://apps.fs.usda.gov/fiadb-api/fullreport` |
| `nps_units` | `https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/FeatureServer/2/query` |
| `nps_visitation` | `https://irmaservices.nps.gov/Stats/v1/visitation` |
| `blm_field_offices` | `https://gis.blm.gov/arcgis/rest/services/admin_boundaries/BLM_Natl_AdminUnit_Generalized/FeatureServer/0/query` |
| `onrr_revenues` | `https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv` |

---

## Tables

### `national_forests`

National Forest and Grassland unit boundaries with acreage and region.
Reference/dimension table (~175 units). Static; no year dimension.

**Source:** USDA FS ArcGIS — `EDW_ForestSystemBoundaries_01/MapServer/0`
**Partition:** None
**Auth:** None
**Cadence:** Annual
**Release window:** Month 10 (October — post-fire-season administrative updates)

| Column | Type | Source field | Notes |
|---|---|---|---|
| forest_id | VARCHAR | `forestnumber` | USDA FS unit ID (e.g., `0501`) |
| forest_name | VARCHAR | `forestname` | Unit name |
| region | VARCHAR | `region` | USDA FS region (01–10) |
| gis_acres | DOUBLE | `gis_acres` | GIS-calculated acreage |

> **Note:** MapServer/0 returns lowercase field names. The `state_fips`, `proclaimed_acres`,
> and `geometry_wkt` columns in the original plan are not available from this endpoint.

---

### `timber_sales`

USDA Forest Service timber harvest activities by National Forest, district, and fiscal year.
Sourced from FACTS (Forest Activity Tracking System) via `EDW_TimberHarvest_01`. Note: this
table covers completed harvest *activities* rather than sale contracts; it supersedes the
original `EDW_TimberSaleProgram_01` endpoint (which no longer exists).

**Source:** USDA FS ArcGIS — `EDW_TimberHarvest_01/MapServer/0`
**Partition:** `fy_completed` (fiscal year completed)
**Auth:** None
**Cadence:** Quarterly
**Release window:** Months 3, 6, 9, 12

| Column | Type | Source field | Notes |
|---|---|---|---|
| facts_id | VARCHAR | `FACTS_ID` | FACTS activity record ID |
| sale_name | VARCHAR | `SALE_NAME` | Sale/activity name |
| fy_completed | INTEGER | `FY_COMPLETED` | Fiscal year completed (partition key) |
| fy_awarded | INTEGER | `FY_AWARDED` | Fiscal year awarded |
| forest_code | VARCHAR | `FOREST_CODE` | USFS forest code |
| forest_name | VARCHAR | `FOREST_NAME` | National Forest name |
| district_code | VARCHAR | `DISTRICT_CODE` | Ranger district code |
| district_name | VARCHAR | `DISTRICT_NAME` | Ranger district name |
| state_abbr | VARCHAR | `STATE_ABBR` | 2-char state abbreviation |
| gis_acres | DOUBLE | `GIS_ACRES` | Activity area in acres |
| activity_code | VARCHAR | `ACTIVITY_CODE` | FACTS activity code |
| activity_name | VARCHAR | `ACTIVITY_NAME` | Activity description |
| treatment_type | VARCHAR | `TREATMENT_TYPE` | Treatment category |
| units_accomplished | DOUBLE | `UNITS_ACCOMPLISHED` | Quantity (see `uom`) |
| uom | VARCHAR | `UOM` | Unit of measure (e.g., MBF, Acres) |
| cost_per_uom | DOUBLE | `COST_PER_UOM` | Cost per unit |

> **Cross-schema note:** Join to `sec.filing_metadata` via `state_abbr` → state of
> incorporation for timber/paper company 10-K disclosures.

---

### `forest_inventory`

USDA Forest Service Forest Inventory and Analysis (FIA) — state-level forest area,
volume, and carbon stock estimates.

**Source:** USDA FS FIA DataMart — `https://apps.fs.usda.gov/fiadb-api/fullreport`
**Partition:** `inventory_year`
**Auth:** None
**Cadence:** Annual
**Release window:** Months 6–9 (FIA annual estimates released summer)

| Column | Type | Notes |
|---|---|---|
| inventory_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| forest_type_group | VARCHAR | Forest type (Oak-Hickory, Douglas-fir, etc.) |
| ownership_class | VARCHAR | National Forest / Other Federal / State / Private |
| basal_area_sqft | DOUBLE | Basal area per acre (tree diameter proxy) |

> **Resolved (2026):** The FIA DataMart `fullreport` query API (which required a
> `wc={statecd}{year}` evaluation-group parameter the ETL did not build) has been
> dropped. `FiaDatamartTransformer` now downloads the per-state bulk archive
> `https://apps.fs.usda.gov/fia/datamart/CSV/{state}_CSV.zip` and parses `{state}_COND.csv`
> directly, with per-state fan-out driven by the `state` dimension in the schema YAML.
> DQ T1/T2 run at normal severity (the `warn` downgrade was removed); Lands DQ PASSes
> with ≈30,255 rows.

---

### `forest_metrics`

USDA FIA tree-level metrics per state × forest type group × ownership class × inventory year.
Joins to `forest_inventory` on the composite key `(state_fips, inventory_year, forest_type_group, ownership_class)`.
All columns are CONDPROP_UNADJ-weighted averages across live trees (STATUSCD=1) in accessible
forest conditions (COND_STATUS_CD=1).

**Source:** USDA FIA DataMart — `{stateAbbr}_TREE.csv` (primary) + `{stateAbbr}_COND.csv` (secondary join)
**Partition:** `stateAbbr` (one file per state covers all inventory years)
**Auth:** None
**Cadence:** Annual
**Release window:** Months 6–9 (same as forest_inventory)
**Transformer:** `FiaMetricsTransformer` (implements `StreamingResponseTransformer` — TREE CSV is streamed
line-by-line to avoid OOM on large state files; COND CSV is loaded into a HashMap for the join)

| Column | Type | Notes |
|---|---|---|
| inventory_year | INTEGER | FIA inventory year (INVYR); composite PK with state_fips, type groups |
| state_fips | VARCHAR | 2-digit FIPS (FK to forest_inventory, geo.states) |
| forest_type_group | VARCHAR | Resolved from FORTYPCD via type-group lookup table |
| ownership_class | VARCHAR | National Forest / Other Federal / State / Local / Private |
| trees_per_acre | DOUBLE | CONDPROP-weighted avg of TPA_UNADJ (live trees per acre) |
| live_volume_cuft | DOUBLE | CONDPROP-weighted avg of TPA_UNADJ × VOLCFNET (cu ft per acre) |
| carbon_stock_tons | DOUBLE | CONDPROP-weighted avg of TPA_UNADJ × CARBON_AG / 2000 (tons/acre) |

> **Methodology note:** `TPA_UNADJ` is FIA's per-tree expansion factor representing trees per
> acre. Multiplying by `CONDPROP_UNADJ` weights by the condition's fraction of plot area.
> Dividing the weighted sums by `condprop_sum` gives a per-acre estimate aggregated to the
> state × type group × ownership × year level. This is a CONDPROP approximation; proper
> population-level FIA estimates require `EXPNS` or `POP_EVAL` tables (deferred).

---

### `nps_units`

National Park Service unit boundaries — parks, monuments, recreation areas, seashores,
parkways, and all other NPS-designated units. Reference/dimension table (~430 units).

**Source:** NPS / Esri ArcGIS Online — `NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/FeatureServer/2`
**Partition:** None
**Auth:** None
**Cadence:** Annual
**Release window:** Month 4 (April — post-annual-report administrative updates)

| Column | Type | Source field | Notes |
|---|---|---|---|
| unit_code | VARCHAR | `UNIT_CODE` | NPS 4-letter unit code (e.g., `YOSE`) |
| unit_name | VARCHAR | `UNIT_NAME` | Full unit name |
| unit_type | VARCHAR | `UNIT_TYPE` | National Park / Monument / Seashore / etc. |
| state_abbr | VARCHAR | `STATE` | 2-char state abbreviation |
| region | VARCHAR | `REGION` | NPS region name |
| gross_acres | DOUBLE | `Shape__Area` / 4046.856 | Total area in acres (derived from m²) |

> **Note:** The original `NPS_Land_Resources/MapServer/2` endpoint (mapservices.nps.gov)
> no longer exists. The current endpoint is on ArcGIS Online (services1.arcgis.com).
> The `county_fips`, `established_date`, and `geometry_wkt` columns from the original
> plan are not available from this endpoint.

---

### `nps_visitation`

NPS monthly visitation statistics by unit from the IRMA Statistics API. Covers recreational
and non-recreational visits. Joins to `geo.counties` for gateway-community economic impact
analysis; joins to `econ.bls_employment` for tourism employment correlation.

**Source:** NPS IRMA — `https://irmaservices.nps.gov/Stats/v1/visitation`
**Response format:** XML (`<ArrayOfVisitationData>`)
**Partition:** `visit_year`
**Auth:** None
**Cadence:** Monthly (current year updated monthly; prior years finalized ~March)
**Release window:** Months 1–12 (continuous)

| Column | Type | XML element | Notes |
|---|---|---|---|
| unit_code | VARCHAR | `<UnitCode>` | FK to nps_units |
| visit_year | INTEGER | `<Year>` | Partition key |
| visit_month | INTEGER | `<Month>` | Month (1–12) |
| recreation_visits | INTEGER | `<RecreationVisitors>` | Recreational visitor count |
| non_recreation_visits | INTEGER | `<NonRecreationVisitors>` | Non-recreational visits |

> **Note:** The legacy `irma.nps.gov/Stats/api/v1/` endpoint returns HTTP 504 (gateway
> timeout). The current endpoint is `irmaservices.nps.gov/Stats/v1/visitation` and returns
> XML. The transformer (`NpsIrmaTransformer`) handles both XML and JSON for forwards
> compatibility. Camping, lodging, and visitor-hours columns from the original plan are not
> available from the new endpoint.

---

### `blm_field_offices`

BLM administrative unit boundaries — field offices and district offices that manage
federal subsurface mineral rights, grazing allotments, and surface use permits.

**Source:** BLM ArcGIS — `admin_boundaries/BLM_Natl_AdminUnit_Generalized/FeatureServer/0`
**Partition:** None (~150 field offices)
**Auth:** None
**Cadence:** Annual
**Release window:** Month 6 (annual administrative boundary updates)

| Column | Type | Source field | Notes |
|---|---|---|---|
| office_code | VARCHAR | `ADM_UNIT_CD` | BLM field office code |
| office_name | VARCHAR | `ADMU_NAME` | Office name (e.g., "Moab Field Office") |
| office_type | VARCHAR | `BLM_ORG_TYPE` | Field Office / District Office / State Office |
| state_abbr | VARCHAR | `ADMIN_ST` | 2-char state abbreviation |

> **Note:** The original `BLM_National_Administrative_Units/FeatureServer/1` endpoint does
> not exist on gis.blm.gov. The current service is `BLM_Natl_AdminUnit_Generalized/FeatureServer/0`.
> The `state_fips`, `adm_acres`, and `geometry_wkt` columns from the original plan are not
> available from this endpoint.

---

### `onrr_revenues`

Office of Natural Resources Revenue (ONRR) royalty and revenue data for oil, gas, coal,
geothermal, and other minerals extracted from federal and tribal lands. One row per
fiscal year / land class / commodity / county combination. This is the primary financial
link between BLM-managed subsurface rights and SEC 10-K disclosures for E&P companies.

**Source:** ONRR — `https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv`
Single bulk CSV covering all fiscal years (FY 2004–present).
**Partition:** Static (no year dimension — full dataset on each run)
**Auth:** None
**Cadence:** Monthly (ONRR updates the bulk CSV ~3 months in arrears)
**Release window:** Months 1–12 (continuous)

| Column | Type | Source column | Notes |
|---|---|---|---|
| fiscal_year | INTEGER | `Fiscal Year` | |
| land_class | VARCHAR | `Land Class` | Federal / Native American |
| land_category | VARCHAR | `Land Category` | Onshore / Offshore / Gulf of Mexico |
| state_name | VARCHAR | `State` | Full state name (null for offshore) |
| county_name | VARCHAR | `County` | County name (null for offshore/tribal) |
| county_fips | VARCHAR | `FIPS Code` | 5-digit FIPS (null for offshore/tribal) |
| offshore_region | VARCHAR | `Offshore Region` | e.g., "Gulf of Mexico" (null for onshore) |
| revenue_type | VARCHAR | `Revenue Type` | Royalties / Bonus / Rents / Other Revenues |
| mineral_lease_type | VARCHAR | `Mineral Lease Type` | Oil & Gas / Coal / Geothermal / etc. |
| commodity | VARCHAR | `Commodity` | Oil / Gas / Coal / Geothermal / Renewables |
| product | VARCHAR | `Product` | Specific product (e.g., Crude Oil, Coalbed Methane) |
| revenue | DOUBLE | `Revenue` | Revenue collected (USD) |

> **Domain change (2024):** The original source `revenuedata.doi.gov` no longer redirects
> correctly. The current domain is `revenuedata.onrr.gov`. The per-year URL pattern
> (`/downloads/{year}/*.csv`) also no longer exists — the only available format is the
> single bulk all-years CSV. `company_name`, `volume`, `volume_unit`, `revenue_month`, and
> `state_fips` columns from the original plan are not available in the bulk CSV.

---

## Join Architecture

```
national_forests (forest_id / forest_code)
    └── timber_sales (forest_code, fy_completed)
            └── sec.filing_metadata (state_abbr ~ state_of_incorporation → timber/paper 10-Ks)

nps_units (unit_code)
    └── nps_visitation (unit_code, visit_year)
            └── econ.bls_employment (state_abbr → gateway-county tourism employment)

blm_field_offices (office_code)
    └── [state] onrr_revenues (state_name → mineral revenue by BLM state)
            └── sec.filing_metadata (commodity/company → E&P royalty exposure)

geo.counties (county_fips)
    └── onrr_revenues (county_fips, fiscal_year)

geo.states (abbr)
    ├── national_forests (state_abbr — no direct FK; spatial join)
    ├── timber_sales (state_abbr)
    └── blm_field_offices (state_abbr)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 82 (`worker-82.sh`) | historical | All 7 tables, full history | 4 GB / 6 GB | Once (first setup) |
| 83 (`worker-83.sh`) | daily | All 7 tables (gated by release window) | 2 GB / 3 GB | Daily |

Workers 82 and 83 delegate to `worker-lands.sh historical` and `worker-lands.sh daily` respectively.
See `docs/operations/lands-maintenance.md` for operational details.

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

All lands tables use the shared `GOVDATA_*` globals — no schema-specific env vars are required.

| Variable | Required | Description |
|---|---|---|
| `GOVDATA_PARQUET_DIR` | Yes | S3/local root for Iceberg tables; lands data under `lands/` |
| `GOVDATA_CACHE_DIR` | Yes | Raw download cache root |
| `GOVDATA_START_YEAR` | Optional | Historical start year (default 2010) |
| `GOVDATA_INCREMENTAL_START_YEAR` | Optional | First year of daily/incremental window (default 2026) |

---

## Open Issues

| Issue | Table | Status |
|---|---|---|
| FIA API required `wc={statecd}{year}` — ETL passed only `wc={year}` (0 rows) | `forest_inventory` | Resolved — switched to per-state bulk `{state}_CSV.zip` + `COND.csv`; DQ PASS at normal severity |
| `timber_sales` now sources harvest *activities*, not sale contracts; some analytical uses need contract-level data | `timber_sales` | Accepted — no public FACTS contract endpoint available |
| `nps_units` lacks `county_fips` — gateway-county joins require a separate crosswalk | `nps_units` | Accepted — no county column in ArcGIS Online NPS boundary service |
