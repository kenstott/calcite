# Energy Schema Data Plan

## Strategic Context

The U.S. Energy Information Administration (EIA) publishes the most comprehensive open
energy statistics in the world. The `energy` schema's primary cross-schema value is
**energy company financial analysis**: joining EIA generation and production data to SEC
10-K filings for utilities, E&P companies, and coal producers creates operational ground
truth for financial disclosures. Weekly storage and stock data provides leading indicators
for commodity price forecasts that directly affect earnings.

---

## Data Sources

| Source | Form | Publisher | API / Format | Key Required | Cadence |
|---|---|---|---|---|---|
| Electricity generation | EIA-923 | EIA | REST API v2 | **Yes — required** (free at eia.gov/opendata) | Monthly |
| Utility annual report | EIA-861 | EIA | Bulk XLS ZIP | None | Annual |
| Retail electricity prices | EIA-861/826 | EIA | REST API v2 | **Yes — required** | Annual/Monthly |
| Power plant inventory | EIA-860 | EIA | Bulk XLS ZIP | None | Annual |
| Capacity changes (monthly) | EIA-860M | EIA | Bulk XLS | None | Monthly |
| Oil & gas production | EIA-914/SEDS | EIA | REST API v2 | **Yes — required** | Monthly |
| State energy consumption | SEDS | EIA | REST API v2 | **Yes — required** | Annual |
| Natural gas storage | EIA-912 | EIA | REST API v2 | **Yes — required** | Weekly |
| Petroleum stocks | EIA-PSW | EIA | REST API v2 | **Yes — required** | Weekly |
| Crude oil imports (company) | EIA-814 | EIA | Bulk XLSX | None | Monthly |
| Refinery operations | EIA-820/817 | EIA | REST API v2 | **Yes — required** | Monthly |
| Coal mine production | MSHA open data | MSHA | Bulk CSV/ZIP | None | Annual |

**EIA API v2 base:** `https://api.eia.gov/v2/`
**EIA API key (free, instant):** `https://www.eia.gov/opendata/register.php`
**EIA bulk downloads:** `https://www.eia.gov/electricity/data/`
**EIA-814 imports:** `https://www.eia.gov/petroleum/imports/companylevel/`
**MSHA open data:** `https://arlweb.msha.gov/OpenGovernmentData/DataSets/`

> **Note on API key:** The EIA v2 API returns `API_KEY_MISSING` for every endpoint without
> a key. The plan previously stated "key recommended" — it is actually **required**. The key
> is free and issued instantly.

> **Note on EIA v2 format:** All EIA v2 `/data` endpoints return a **tall/narrow format** —
> one row per (period × facets) with a single `value` column. Wide denormalized tables
> require pivoting multiple series in the ETL. Period is always a string (`"YYYY"`,
> `"YYYY-MM"`, or `"YYYY-MM-DD"`); no separate year/month fields exist in the API.

> **Note on state identifiers:** EIA never returns FIPS codes. It uses 2-letter postal
> abbreviations (`stateid`, `location`) or proprietary area codes (`duoarea`, e.g., `"STX"`
> for Texas). All `state_fips` columns require a lookup join to `geo.states` in the ETL.

---

## Tables

### `eia_electricity_generation`

Monthly net electricity generation by state, energy source, and sector.

**Source:** EIA API v2 — `electricity/electric-power-operational-data`
**Partition:** `generation_year`
**Cadence:** Monthly (2-month publication lag)
**Release window:** Months 1–12

| Column | Type | Source Field | Description |
|---|---|---|---|
| generation_year | INTEGER | parsed from `period` | Partition key |
| generation_month | INTEGER | parsed from `period` | Month (1–12); null if annual frequency |
| state_abbr | VARCHAR | `location` | 2-letter postal abbreviation; also includes EIA region codes (e.g., `"PAD"`, `"NEW"`) |
| state_fips | VARCHAR | derived via lookup | FK to geo.states; not in API — join from state_abbr |
| state_description | VARCHAR | `stateDescription` | Full state/region name |
| energy_source_code | VARCHAR | `fueltypeid` | EIA fuel code: `NG`, `COL`, `NUC`, `HYC`, `WND`, `SUN`, `PET`, `ALL`, `BIO`, `GEO`, etc. |
| energy_source | VARCHAR | `fuelTypeDescription` | Fuel label (API returns lowercase: `"natural gas"`, `"coal, excluding waste coal"`) |
| sector_code | VARCHAR | `sectorid` | EIA sector code as string; values: `"1"`–`"8"`, `"90"`, `"94"`–`"99"` (wider than the 1–6 in earlier plan) |
| sector | VARCHAR | `sectorDescription` | Sector label: `"Electric Utility"`, `"IPP Non-CHP"`, `"All Sectors"`, etc. (15 distinct sectors) |
| generation_thousand_mwh | DOUBLE | `generation` | Net generation in **thousand MWh** (API unit = `"thousand megawatthours"`; multiply ×1000 for MWh) |
| fuel_consumed_total_mmbtu | DOUBLE | `total-consumption-btu` | Total fuel consumed in million MMBtu; null for non-thermal sources |
| fuel_consumed_for_eg_mmbtu | DOUBLE | `consumption-for-eg-btu` | Fuel consumed for electricity generation only (excludes useful thermal output) |
| fuel_cost_per_mmbtu | DOUBLE | `cost-per-btu` | Average fuel cost per MMBtu (USD) |
| sulfur_content | DOUBLE | `sulfur-content` | Sulfur content of fuel (%; relevant for coal/petroleum) |
| ash_content | DOUBLE | `ash-content` | Ash content of coal (%) |

> **Capacity columns removed:** `net_summer_capacity_mw`, `net_winter_capacity_mw`,
> `capacity_factor`, and `heat_rate_btu_kwh` are **not in this endpoint**. Capacity lives
> in `electricity/operating-generator-capacity` and should be sourced from `eia_power_plants`
> (EIA-860). Heat rate must be computed externally from fuel consumed and generation.

---

### `eia_electricity_prices`

Annual (or monthly) average retail electricity prices by state and customer sector.

**Source:** EIA API v2 — `electricity/retail-sales`
**Partition:** `price_year`
**Cadence:** Annual prior-year published ~October; monthly updates available
**Release window:** Month 10 (annual); months 1–12 (monthly)

| Column | Type | Source Field | Description |
|---|---|---|---|
| price_year | INTEGER | parsed from `period` | Partition key |
| price_month | INTEGER | parsed from `period` | Month (1–12) for monthly data; null for annual |
| state_abbr | VARCHAR | `stateid` | 2-letter postal abbreviation (API field name is `stateid`) |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| state_description | VARCHAR | `stateDescription` | Full state name |
| sector_code | VARCHAR | `sectorid` | Sector code: `"RES"`, `"COM"`, `"IND"`, `"TRA"`, `"OTH"`, `"ALL"` |
| sector | VARCHAR | `sectorName` | Sector label (API returns lowercase: `"residential"`, `"commercial"`, `"industrial"`, `"transportation"`, `"other"`, `"all sectors"`) |
| avg_price_cents_kwh | DOUBLE | `price` | Average retail price (cents per kWh; confirmed unit) |
| revenue_million_dollars | DOUBLE | `revenue` | Total retail revenue (million dollars; API unit = `"million dollars"`) |
| sales_million_kwh | DOUBLE | `sales` | Total electricity sold (**million kWh** — API unit is `"million kilowatt hours"`, not MWh) |
| customers | INTEGER | `customers` | Number of customer accounts |
| price_rank | INTEGER | computed | State rank by avg_price_cents_kwh within sector (1 = cheapest); window function over dataset |
| yoy_price_change_pct | DOUBLE | computed | Year-over-year price change; requires prior-year join |

---

### `eia_utility_annual`

Annual electric utility company report — company-level customers, sales, revenue, and
operations. Primary link to SEC filings for utility holding companies.

**Source:** EIA Form 861 bulk XLS ZIP — `https://www.eia.gov/electricity/data/eia861/`
Multi-file structure: requires joining `Utility_Data`, `Sales_Ult_Cust`, `Operational_Data`,
`Demand_Response`, `Net_Metering` files on `Utility Number`.
**Partition:** `report_year`
**Cadence:** Annual (prior year published ~October)
**Release window:** Month 10

| Column | Type | Source Field | Description |
|---|---|---|---|
| utility_id | INTEGER | `Utility Number` | EIA utility number — stable identifier across years |
| report_year | INTEGER | `Data Year` | Partition key |
| utility_name | VARCHAR | `Utility Name` | Legal utility name (fuzzy-join to sec.filing_metadata) |
| entity_type | VARCHAR | `Ownership Type` (Utility_Data) | `"Investor Owned"`, `"Municipal"`, `"Cooperative"`, `"State"`, `"Federal"`, `"Other"` |
| state_abbr | VARCHAR | `State` | 2-letter state abbreviation (API field; FIPS derived separately) |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| ba_code | VARCHAR | `BA Code` | Balancing authority code (e.g., `"TVA"`, `"SOCO"`, `"PJM"`) |
| nerc_region | VARCHAR | `NERC Region` | NERC reliability region (e.g., `"SERC"`, `"RFC"`, `"WECC"`) |
| service_type | VARCHAR | `Service Type` (Sales_Ult_Cust) | `"Bundled"` or `"Delivery"` — important filter for deregulated markets |
| data_type | VARCHAR | `Data Type` | `"O"` = Observed, `"I"` = Imputed |
| activity_generation | BOOLEAN | `Generation` flag | Utility owns/operates generating units |
| activity_transmission | BOOLEAN | `Transmission` flag | Utility owns/operates transmission |
| activity_distribution | BOOLEAN | `Distribution` flag | Utility owns/operates distribution |
| customers_residential | INTEGER | `RESIDENTIAL Customers (Count)` | Residential accounts |
| customers_commercial | INTEGER | `COMMERCIAL Customers (Count)` | Commercial accounts |
| customers_industrial | INTEGER | `INDUSTRIAL Customers (Count)` | Industrial accounts |
| customers_transportation | INTEGER | `TRANSPORTATION Customers (Count)` | Transportation (EV charging, etc.) |
| customers_total | INTEGER | `TOTAL Customers (Count)` | All sectors combined |
| sales_residential_mwh | DOUBLE | `RESIDENTIAL Sales (Megawatthours)` | Retail sales to residential (MWh) |
| sales_commercial_mwh | DOUBLE | `COMMERCIAL Sales (Megawatthours)` | Retail sales to commercial (MWh) |
| sales_industrial_mwh | DOUBLE | `INDUSTRIAL Sales (Megawatthours)` | Retail sales to industrial (MWh) |
| sales_transportation_mwh | DOUBLE | `TRANSPORTATION Sales (Megawatthours)` | Retail sales to transportation sector |
| sales_total_mwh | DOUBLE | `TOTAL Sales (Megawatthours)` | Total retail electricity sales (MWh) |
| revenue_residential_thousand | DOUBLE | `RESIDENTIAL Revenues (Thousand Dollars)` | Revenue from residential (**thousand dollars**, not millions) |
| revenue_commercial_thousand | DOUBLE | `COMMERCIAL Revenues (Thousand Dollars)` | Revenue from commercial (thousand dollars) |
| revenue_industrial_thousand | DOUBLE | `INDUSTRIAL Revenues (Thousand Dollars)` | Revenue from industrial (thousand dollars) |
| revenue_total_thousand | DOUBLE | `TOTAL Revenues (Thousand Dollars)` | Total retail revenue (thousand dollars) |
| avg_retail_price_cents_kwh | DOUBLE | computed | `(revenue_total_thousand × 1e7) / (sales_total_mwh × 1e3)`; cents per kWh |
| purchased_power_mwh | DOUBLE | `Wholesale Power Purchases` (Operational_Data) | Power purchased from wholesale market |
| wholesale_sales_mwh | DOUBLE | `Sales for Resale` (Operational_Data) | Electricity sold at wholesale |
| summer_peak_demand_mw | DOUBLE | `Summer Peak Demand` (Operational_Data) | Peak demand in summer (MW) |
| winter_peak_demand_mw | DOUBLE | `Winter Peak Demand` (Operational_Data) | Peak demand in winter (MW) |
| net_generation_mwh | DOUBLE | `Net Generation` (Operational_Data) | Net generation by utility-owned plants |
| demand_response_actual_mw | DOUBLE | sum of `Actual Peak Demand Savings (MW)` | Actual peak demand reduction from DR programs |
| net_metering_installations | INTEGER | `All Technologies Total Installations` (Net_Metering) | Net metering installations (labeled "Installations" not "Customers") |
| net_metering_capacity_mw | DOUBLE | `All Technologies Total Capacity MW` (Net_Metering) | Total net metering installed capacity |
| net_metering_energy_sold_back_mwh | DOUBLE | `All Technologies Total Energy Sold Back MWh` | Energy credited back to grid from net metering |

> **Removed columns:** `service_area_population` (not in EIA-861; Service Territory file has
> county names only — population requires Census ACS join), `regulated` boolean (not in EIA-861;
> EIA-860 has `Regulatory Status` per plant), `ownership_type` as single string (EIA-861 uses
> per-activity Y/N flags, not a single `Vertically integrated` label — see `activity_*` columns).

---

### `eia_power_plants`

Annual snapshot of every U.S. electric generating plant and its generators.

**Source:** EIA Form 860 bulk XLS ZIP — `https://www.eia.gov/electricity/data/eia860/`
Files joined: `2___Plant_Y{YYYY}.xlsx` (plant attributes) + `3_1_Generator_Y{YYYY}.xlsx`
(generator capacity/fuel/status) on `Plant Code`.

> **Design note:** One row per **generator** (not plant). A single plant has multiple
> generators with different fuels, capacities, and statuses. Plant-level attributes are
> denormalized onto each generator row.

**Partition:** `report_year`
**Cadence:** Annual (prior year published ~August)
**Release window:** Months 8–10

| Column | Type | Source Field | Description |
|---|---|---|---|
| plant_id | INTEGER | `Plant Code` | EIA 5-digit plant code (stable) |
| generator_id | VARCHAR | `Generator ID` | Generator ID within plant (e.g., `"1"`, `"GT11"`) |
| report_year | INTEGER | derived from filename | Partition key (not in file; constant from ZIP filename `Y{YYYY}`) |
| plant_name | VARCHAR | `Plant Name` | Plant name |
| utility_id | INTEGER | `Utility ID` | FK to eia_utility_annual |
| utility_name | VARCHAR | `Utility Name` | Owning utility name |
| state_abbr | VARCHAR | `State` | 2-letter state abbreviation |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| county_name | VARCHAR | `County` | County name (string, not FIPS — e.g., `"Tuscaloosa"`) |
| county_fips | VARCHAR | derived via lookup | FK to geo.counties; requires Census county-name-to-FIPS mapping |
| city | VARCHAR | `City` | City name |
| latitude | DOUBLE | `Latitude` | Plant latitude (WGS84) |
| longitude | DOUBLE | `Longitude` | Plant longitude (WGS84) |
| nerc_region | VARCHAR | `NERC Region` | NERC reliability region (e.g., `"SERC"`, `"WECC"`) |
| balancing_authority_code | VARCHAR | `Balancing Authority Code` | BA code (e.g., `"SOCO"`, `"CAISO"`) |
| balancing_authority_name | VARCHAR | `Balancing Authority Name` | Full BA name |
| primary_purpose_naics | INTEGER | `Primary Purpose (NAICS Code)` | NAICS sector (22 = Utilities) |
| regulatory_status | VARCHAR | `Regulatory Status` | `"RE"` = Regulated, `"NR"` = Non-regulated |
| sector_code | INTEGER | `Sector` | Numeric sector code |
| sector | VARCHAR | `Sector Name` | Sector description (e.g., `"Electric Utility"`, `"IPP Non-CHP"`) |
| technology | VARCHAR | `Technology` (Generator) | Technology type (e.g., `"Natural Gas Fired Combined Cycle"`, `"Onshore Wind Turbine"`) |
| prime_mover | VARCHAR | `Prime Mover` | Code: `"GT"`, `"ST"`, `"IC"`, `"PV"`, `"WT"`, `"ES"`, etc. |
| energy_source_1 | VARCHAR | `Energy Source 1` | Primary fuel code (e.g., `"NG"`, `"DFO"`, `"SUN"`) |
| energy_source_2 | VARCHAR | `Energy Source 2` | Secondary fuel code (null if single-fuel) |
| energy_source_3 | VARCHAR | `Energy Source 3` | Tertiary fuel code |
| nameplate_capacity_mw | DOUBLE | `Nameplate Capacity (MW)` | Gross nameplate capacity |
| net_summer_capacity_mw | DOUBLE | `Summer Capacity (MW)` | Net summer generating capacity |
| net_winter_capacity_mw | DOUBLE | `Winter Capacity (MW)` | Net winter generating capacity |
| minimum_load_mw | DOUBLE | `Minimum Load (MW)` | Minimum stable operating load |
| ownership_code | VARCHAR | `Ownership` | `"S"` = Sole owner, `"J"` = Joint, `"W"` = Wholly owned subsidiary |
| operating_status | VARCHAR | `Status` | Code: `"OP"` = Operating, `"SB"` = Standby, `"RE"` = Retired, `"CN"` = Canceled, `"TS"` = Construction complete not operating, etc. |
| operating_month | INTEGER | `Operating Month` | Month commercial operation began |
| operating_year | INTEGER | `Operating Year` | Year commercial operation began |
| planned_retirement_month | INTEGER | `Planned Retirement Month` | Month of planned retirement (null if no plan) |
| planned_retirement_year | INTEGER | `Planned Retirement Year` | Year of planned retirement |
| water_source | VARCHAR | `Name of Water Source` | Cooling water source (e.g., `"Black Warrior River"`) |
| energy_storage_flag | BOOLEAN | `Energy Storage` | `"Y"`/`"N"` flag; detailed storage capacity in `3_4_Energy_Storage_Y{YYYY}.xlsx` |

> **Removed columns:** `operator_id` / `operator_name` (not in Plant or Generator files;
> requires Schedule 4 Owner file), `commercial_operation_date` / `planned_retirement_date` as
> DATE (source has separate month + year integers — kept as `operating_month`/`operating_year`).

---

### `eia_capacity_changes`

Monthly additions, retirements, and planned changes from EIA-860M snapshot.

**Source:** EIA Form 860M monthly XLS — `https://www.eia.gov/electricity/data/eia860m/`
**Partition:** `change_year`
**Cadence:** Monthly
**Release window:** Months 1–12

| Column | Type | Source Field | Description |
|---|---|---|---|
| plant_id | INTEGER | `Plant ID` | FK to eia_power_plants |
| generator_id | VARCHAR | `Generator ID` | Generator ID within plant |
| snapshot_year | INTEGER | derived from filename | Year of the 860M snapshot file |
| snapshot_month | INTEGER | derived from filename | Month of the 860M snapshot file |
| change_type | VARCHAR | derived from sheet tab | `"Addition"` (Operating sheet), `"Retirement"` (Retired sheet), `"Planned Addition"` (Planned sheet), `"Cancellation"` (Canceled or Postponed sheet) |
| change_year | INTEGER | Partition key | `Operating Year` / `Planned Operation Year` / `Retirement Year` depending on `change_type` |
| change_month | INTEGER | corresponding month field | Month of the change event |
| plant_name | VARCHAR | `Plant Name` | Plant name |
| entity_id | INTEGER | `Entity ID` | EIA entity/operator ID (may differ from EIA-861 `Utility Number`) |
| entity_name | VARCHAR | `Entity Name` | Operator/owner name |
| state_abbr | VARCHAR | `Plant State` | 2-letter state abbreviation |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| county_name | VARCHAR | `County` | County name (string; FIPS requires lookup) |
| county_fips | VARCHAR | derived via lookup | FK to geo.counties |
| balancing_authority_code | VARCHAR | `Balancing Authority Code` | BA code |
| sector | VARCHAR | `Sector` | Sector description |
| technology | VARCHAR | `Technology` | Full technology description (e.g., `"Solar Photovoltaic"`, `"Natural Gas Fired Combustion Turbine"`) |
| energy_source_code | VARCHAR | `Energy Source Code` | EIA fuel code (e.g., `"NG"`, `"SUN"`, `"WND"`) |
| prime_mover_code | VARCHAR | `Prime Mover Code` | Prime mover code |
| nameplate_capacity_mw | DOUBLE | `Nameplate Capacity (MW)` | Gross nameplate capacity |
| net_summer_capacity_mw | DOUBLE | `Net Summer Capacity (MW)` | Net summer capacity |
| net_winter_capacity_mw | DOUBLE | `Net Winter Capacity (MW)` | Net winter capacity |
| nameplate_energy_capacity_mwh | DOUBLE | `Nameplate Energy Capacity (MWh)` | For storage: energy capacity in MWh |
| dc_net_capacity_mw | DOUBLE | `DC Net Capacity (MW)` | DC-side capacity for solar PV |
| commercial_operation_month | INTEGER | `Operating Month` | Month commercial operation began (additions) |
| commercial_operation_year | INTEGER | `Operating Year` | Year commercial operation began |
| retirement_month | INTEGER | `Retirement Month` | Month of retirement (retirements only) |
| retirement_year | INTEGER | `Retirement Year` | Year of retirement |
| latitude | DOUBLE | `Latitude` | Plant latitude |
| longitude | DOUBLE | `Longitude` | Plant longitude |

---

### `eia_fossil_fuel_production`

Monthly state-level crude oil, natural gas, and coal production. Sourced from three
separate EIA endpoints that must be unioned.

**Sources:**
- Crude oil: EIA API v2 — `petroleum/crd/crpdn` *(corrected from `petroleum/sum/sndrc` which is invalid)*
- Natural gas: EIA API v2 — `natural-gas/prod/sum`
- Coal: EIA Form 7A bulk XLS (no reliable API v2 route)

**Partition:** `production_year`
**Cadence:** Monthly oil/gas (2-month lag); annual coal
**Release window:** Months 1–12 (oil/gas); Month 11 (coal)

| Column | Type | Source Field | Description |
|---|---|---|---|
| production_year | INTEGER | parsed from `period` | Partition key |
| production_month | INTEGER | parsed from `period` | Month (1–12); null for annual coal |
| eia_area_code | VARCHAR | `duoarea` | EIA area code (e.g., `"STX"` = Texas, `"SNM"` = New Mexico); NOT a FIPS code |
| state_abbr | VARCHAR | derived from duoarea | 2-letter postal abbreviation; derived via EIA area code lookup |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| fuel_type | VARCHAR | derived | `"Crude Oil"` / `"Natural Gas"` / `"Dry Natural Gas"` / `"Coal"` / `"NGLs"`; derived from product + process |
| process_code | VARCHAR | `process` | EIA process code (e.g., `"VGM"` = marketed production, `"VPR"` = gross withdrawals) |
| process_name | VARCHAR | `process-name` | Process description |
| production_volume | DOUBLE | `value` (cast from string) | Production volume |
| production_unit | VARCHAR | `units` | Exact API unit string: `"Thousand Barrels"` / `"MMcf"` / `"Short Tons"` |
| series_id | VARCHAR | `series` | EIA legacy series ID (for backward compatibility lookups) |
| series_description | VARCHAR | `series-description` | Full series description including units |
| marketed_production | DOUBLE | `value` where `process`=`"VGM"` | Gross minus reinjected/vented/flared (gas only; MMcf) |

> **Removed columns:** `producing_wells` and `avg_well_productivity` (separate EIA endpoint
> required — not in production sum routes), `wellhead_price` and `production_value` (price
> data is in `petroleum/pri/` and `natural-gas/pri/` — separate API calls), `flared_vented`
> (not available at state level in production sum routes).

---

### `eia_state_energy_consumption`

Annual state energy consumption by sector and fuel type (SEDS).

**Source:** EIA API v2 — `seds/data`
**Partition:** `consumption_year`
**Cadence:** Annual (prior year published ~June)
**Release window:** Months 6–8

> **SEDS structure:** The API returns one row per MSN series code — a single `value` column
> serves consumption, expenditure, AND price, differentiated by the MSN code. The ETL must
> fetch all relevant MSN codes and pivot/classify by type. `sector` and `fuel_type` are
> **derived** from the MSN code via the EIA MSN lookup table — they are not separate API fields.

| Column | Type | Source Field | Description |
|---|---|---|---|
| consumption_year | INTEGER | parsed from `period` | Partition key (`period` = `"YYYY"` string) |
| state_abbr | VARCHAR | `stateid` | 2-letter postal abbreviation (API field: `stateid`) |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| state_name | VARCHAR | `stateDescription` | Full state name |
| msn | VARCHAR | `msn` | EIA SEDS series code (e.g., `"NGRCB"` = residential natural gas consumption in BTU) |
| sector | VARCHAR | derived from msn | Derived from MSN positions 2–3 via EIA MSN lookup: `"Residential"` / `"Commercial"` / `"Industrial"` / `"Transportation"` / `"Electric Power"` / `"Total"` |
| fuel_type | VARCHAR | derived from msn | Derived from MSN positions 0–1 via EIA MSN lookup: `"Natural Gas"` / `"Petroleum"` / `"Coal"` / `"Nuclear"` / `"Renewables"` / `"Total"` |
| value | DOUBLE | `value` (cast from string) | Raw SEDS value; interpretation depends on `units` |
| units | VARCHAR | `units` | `"Billion Btu"` / `"Million Dollars"` / `"Dollars per Million Btu"` |
| consumption_bbtu | DOUBLE | `value` where units=`"Billion Btu"` | Energy consumed (**Billion BTU** — API unit; earlier plan incorrectly said Trillion BTU) |
| expenditure_million | DOUBLE | `value` where units=`"Million Dollars"` | Energy expenditure (USD millions) |
| price_per_mmbtu | DOUBLE | `value` where units=`"Dollars per Million Btu"` | Average end-use price (USD/MMBtu) |
| series_description | VARCHAR | `series-description` | Full EIA series description (data lineage) |
| consumption_per_capita_bbtu | DOUBLE | computed | `consumption_bbtu / population`; requires Census ACS population join |
| expenditure_per_capita | DOUBLE | computed | `(expenditure_million × 1e6) / population`; energy burden proxy |

---

### `eia_natural_gas_storage`

Weekly natural gas working storage by region. One of the most closely watched short-term
energy indicators.

**Source:** EIA API v2 — `natural-gas/stor/sum`
**Partition:** `storage_year`
**Cadence:** Weekly (Thursday release; data covers Wednesday)
**Release window:** Months 1–12

| Column | Type | Source Field | Description |
|---|---|---|---|
| report_date | DATE | `period` cast to DATE | Wednesday of the storage report week (API format: `"YYYY-MM-DD"`) |
| storage_year | INTEGER | parsed from `period` | Partition key |
| storage_week | INTEGER | computed from `period` | ISO week number; derived via date function |
| eia_region_code | VARCHAR | `duoarea` | EIA region code: `"R10"` (East), `"R20"` (Midwest), `"R30"` (Mountain), `"R40"` (Pacific), `"R50"` (South Central), `"NUS"` (Lower 48 Total) |
| region | VARCHAR | `area-name` | Region label: `"East Region"`, `"Midwest Region"`, `"Mountain Region"`, `"Pacific Region"`, `"South Central Region"`, `"Lower 48 States"` *(API includes "Region" suffix — different from earlier plan)* |
| storage_type_code | VARCHAR | `process` | EIA storage type code: `"SAB"` = Base Gas, `"SAW"` = Working Gas, `"SBS"` = Total Stocks, `"SLNG"` = LNG |
| storage_type | VARCHAR | `process-name` | Storage type label: `"Base Gas"`, `"Working Gas"`, `"Total Stocks"`, `"LNG"` *(API says "Total Stocks" not "Total" as in earlier plan)* |
| volume_bcf | DOUBLE | `value` | Storage volume (Bcf; units confirmed as `"Bcf"`) |
| units | VARCHAR | `units` | Should always be `"Bcf"`; carry for validation |
| series_id | VARCHAR | `series` | EIA legacy series ID |
| change_bcf | DOUBLE | computed | Week-over-week change; LAG window function over ordered `report_date` |
| yoy_change_bcf | DOUBLE | computed | Same ISO week prior year; requires self-join on `storage_week` + `storage_year-1` |
| five_year_avg_bcf | DOUBLE | uncertain | May be a separate `process` code in the API (e.g., `"5YA"`); verify during implementation. If not available, compute from 5 prior years of same ISO week |
| surplus_deficit_bcf | DOUBLE | computed | `volume_bcf − five_year_avg_bcf` |

> **Removed:** `pct_full` — storage total capacity denominator is not available in this
> endpoint; the EIA does not publish a standardized working gas capacity by region.

---

### `eia_petroleum_stocks`

Weekly U.S. and PADD petroleum inventory levels.

**Source:** EIA API v2 — `petroleum/stoc/wstk`
**Partition:** `stock_year`
**Cadence:** Weekly (Wednesday release; data covers prior Friday)
**Release window:** Months 1–12

| Column | Type | Source Field | Description |
|---|---|---|---|
| report_date | DATE | `period` cast to DATE | Friday of the stocks reporting week (API format: `"YYYY-MM-DD"`) |
| stock_year | INTEGER | parsed from `period` | Partition key |
| stock_week | INTEGER | computed from `period` | ISO week number |
| eia_area_code | VARCHAR | `duoarea` | EIA area code: `"NUS"` = Total US, `"R10"` = PADD 1, `"R50"` = PADD 5 |
| padd | VARCHAR | `area-name` | Area label: `"U.S."`, `"East Coast"`, `"Midwest"`, `"Gulf Coast"`, `"Rocky Mountain"`, `"West Coast"` |
| product_code | VARCHAR | `product` | EIA product code (e.g., `"EPC0"` = Crude Oil, `"EPM0"` = Motor Gasoline) |
| product | VARCHAR | `product-name` | Product label: `"Crude Oil"`, `"Motor Gasoline"`, `"Distillate Fuel Oil"`, etc. |
| process_code | VARCHAR | `process` | Distinguishes `"SAX"` = Ending Stocks Excl. SPR vs. `"SAE"` = Ending Stocks (incl. SPR) |
| process_name | VARCHAR | `process-name` | `"Ending Stocks Excluding SPR"`, `"Ending Stocks"`, etc. |
| stocks_kbbl | DOUBLE | `value` | Inventory (thousand barrels; API unit `"MBBL"` = same as thousand barrels) |
| series_id | VARCHAR | `series` | EIA legacy series ID |
| series_description | VARCHAR | `series-description` | Full description (e.g., `"U.S. Ending Stocks excluding SPR of Crude Oil (Thousand Barrels)"`) |
| change_kbbl | DOUBLE | computed | Week-over-week inventory change |
| yoy_change_kbbl | DOUBLE | computed | Year-over-year change (same ISO week) |
| five_year_avg_kbbl | DOUBLE | computed | 5-year rolling average for same ISO week |
| surplus_deficit_kbbl | DOUBLE | computed | `stocks_kbbl − five_year_avg_kbbl` |
| days_supply | DOUBLE | computed | `stocks_kbbl / avg_daily_consumption`; requires 4-week rolling average demand |

---

### `eia_crude_oil_imports`

Monthly U.S. crude oil imports by company, origin country, and receiving refinery.

**Source:** EIA Form EIA-814 company-level imports bulk XLSX —
`https://www.eia.gov/petroleum/imports/companylevel/`
*(Corrected from `petroleum/move/imp` API route, which returns PADD-aggregate movements
with no API gravity, sulfur, or company/refinery detail)*

**Partition:** `import_year`
**Cadence:** Monthly (2-month lag)
**Release window:** Months 1–12

| Column | Type | Source Field | Description |
|---|---|---|---|
| rpt_period | DATE | `RPT_PERIOD` | Last day of reporting month (e.g., `2024-01-31`) |
| import_year | INTEGER | parsed from `RPT_PERIOD` | Partition key |
| import_month | INTEGER | parsed from `RPT_PERIOD` | Month (1–12) |
| importer_name | VARCHAR | `R_S_NAME` | Importing/trading company name (useful for SEC fuzzy-join) |
| origin_country | VARCHAR | `CNTRY_NAME` | Origin country name (all-caps: `"CANADA"`, `"SAUDI ARABIA"`) |
| origin_country_code | INTEGER | `GCTRY_CODE` | EIA numeric country code (e.g., 260 = Canada, 309 = Saudi Arabia) |
| origin_country_iso3 | VARCHAR | derived via lookup | ISO 3-letter code; derived from `GCTRY_CODE` via reference table |
| entry_port_code | VARCHAR | `PORT_CODE` | EIA port code |
| entry_port_city | VARCHAR | `PORT_CITY` | Port city name (e.g., `"BUFF-NIAG FL, NY"`) |
| entry_padd | INTEGER | `PORT_PADD` | PADD of entry port (1–5) |
| dest_padd | INTEGER | `PCOMP_PADD` | PADD of destination refinery (1–5; distinct from entry PADD) |
| receiving_company | VARCHAR | `PCOMP_RNAM` | Receiving company/refinery owner name |
| refinery_site_id | INTEGER | `PCOMP_SITEID` | EIA refinery site ID |
| refinery_site_name | VARCHAR | `PCOMP_SNAM` | Refinery site name (e.g., `"BRADFORD"`, `"TULSA TERMINAL"`) |
| refinery_state_abbr | VARCHAR | `PCOMP_STAT` | Refinery state abbreviation |
| refinery_state_fips | VARCHAR | derived via lookup | FK to geo.states |
| api_gravity | DOUBLE | `APIGRAVITY` | API gravity of crude (degrees; higher = lighter) |
| sulfur_content_pct | DOUBLE | `SULFUR` | Sulfur content (weight percent; higher = more sour) |
| volume_kbbl | INTEGER | `QUANTITY` | Import volume (thousand barrels) |
| crude_grade | VARCHAR | derived | Derived from api_gravity + sulfur: `"Light Sweet"` (API≥38, S<0.42%) / `"Light Sour"` / `"Medium"` / `"Heavy Sweet"` / `"Heavy Sour"` (API<22, S≥2%) |

> **Removed:** `landed_cost_per_bbl` — not reported in EIA-814. The API aggregate route
> (`petroleum/move/imp`) does have cost data but lacks the per-shipment detail; if landed
> cost is needed, it requires a separate EIA cost series join.

---

### `eia_refinery_operations`

Monthly U.S. refinery throughput and capacity utilization by PADD region.

**Source:** EIA API v2 — `petroleum/pnp/refp`
**Partition:** `report_year`
**Cadence:** Monthly (2-month lag)
**Release window:** Months 1–12

> **Format note:** This endpoint returns a tall/narrow format — one row per series per PADD
> per period. Building a wide denormalized row requires pivoting across series codes in the ETL.
> PADD-level only — no state breakdown is available from this route.

| Column | Type | Source Field | Description |
|---|---|---|---|
| report_year | INTEGER | parsed from `period` | Partition key |
| report_month | INTEGER | parsed from `period` | Month (1–12; `period` = `"YYYY-MM"`) |
| eia_area_code | VARCHAR | `duoarea` | `"NUS"` (Total US), `"R10"`–`"R50"` (PADDs 1–5) |
| padd | VARCHAR | `area-name` | `"U.S."`, `"East Coast"`, `"Midwest"`, `"Gulf Coast"`, `"Rocky Mountain"`, `"West Coast"` |
| series_id | VARCHAR | `series` | EIA series ID |
| process_code | VARCHAR | `process` | Distinguishes crude runs vs. gross input vs. capacity series |
| metric_name | VARCHAR | `series-description` | Full series description |
| units | VARCHAR | `units` | `"MBBL/D"` (thousand barrels per day) or `"PCT"` (utilization rate) |
| crude_input_kbbl_per_day | DOUBLE | pivoted from `value` | Crude oil runs to stills (MBBL/D) |
| total_gross_input_kbbl_per_day | DOUBLE | pivoted from `value` | All inputs including unfinished oils (MBBL/D) |
| operable_capacity_kbbl_per_day | DOUBLE | pivoted from `value` | Operable atmospheric distillation capacity (MBBL/D) |
| utilization_rate_pct | DOUBLE | pivoted from `value` | Gross input / operable capacity (%; API returns as `"PCT"`) |
| gasoline_output_kbbl_per_day | DOUBLE | pivoted from `value` | Motor gasoline net production (MBBL/D) |
| distillate_output_kbbl_per_day | DOUBLE | pivoted from `value` | Distillate fuel oil net production (MBBL/D) |
| jet_fuel_output_kbbl_per_day | DOUBLE | pivoted from `value` | Kerosene-type jet fuel (MBBL/D) |
| residual_fuel_output_kbbl_per_day | DOUBLE | pivoted from `value` | Residual fuel oil (MBBL/D) |
| propane_output_kbbl_per_day | DOUBLE | pivoted from `value` | Propane/propylene production (MBBL/D) |
| total_products_output_kbbl_per_day | DOUBLE | computed | Sum of individual product outputs; not a direct API series |
| processing_gain_kbbl_per_day | DOUBLE | computed | `total_products − total_gross_input`; volume gain from processing |

> **Removed:** `state_fips` — this route is PADD-level only; no state-level refinery data
> is available from `petroleum/pnp/refp`.

---

### `eia_coal_mines`

Annual coal mine production, employment, and productivity by individual mine.

**Source:** MSHA open data (not EIA — EIA API v2 has no mine-level coal data):
- Mine attributes: `https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip`
- Annual production/employment: `https://arlweb.msha.gov/OpenGovernmentData/DataSets/MinesProdYearly.zip`

Joined on `MINE_ID`. Filter `COAL_METAL_IND = 'C'` for coal mines only.

**Partition:** `report_year`
**Cadence:** Annual (prior year published ~November)
**Release window:** Month 11

| Column | Type | Source Field | Description |
|---|---|---|---|
| mine_id | VARCHAR | `MINE_ID` | MSHA mine ID (7-character; same as MSHA mine identifier) |
| report_year | INTEGER | `CAL_YR` | Partition key (calendar year of production) |
| mine_name | VARCHAR | `MINE_NAME` / `CURRENT_MINE_NAME` | Mine name (production file vs. mines file) |
| controller_name | VARCHAR | `CURRENT_CONTROLLER_NAME` | Parent/controlling company name (best field for SEC fuzzy-join — represents the entity that would appear in a 10-K) |
| operator_name | VARCHAR | `CURRENT_OPERATOR_NAME` | Operating company name (may differ from controller for contract mines) |
| state_abbr | VARCHAR | `STATE` | 2-letter state abbreviation |
| state_fips | VARCHAR | derived via lookup | FK to geo.states |
| county_fips | VARCHAR | `BOM_STATE_CD` + `FIPS_CNTY_CD` | 5-digit county FIPS (concatenate 2-digit BOM state + 3-digit county code) |
| county_name | VARCHAR | `FIPS_CNTY_NM` | County name |
| latitude | DOUBLE | `LATITUDE` | Mine latitude |
| longitude | DOUBLE | `LONGITUDE` | Mine longitude |
| mine_type | VARCHAR | `CURRENT_MINE_TYPE` | `"Surface"`, `"Underground"`, `"Facility"` |
| subunit_code | VARCHAR | `SUBUNIT_CD` | `"01"` = Underground, `"03"` = Strip/Quarry, `"04"` = Auger, `"05"` = Culm bank/refuse pile, `"06"` = Dredge |
| subunit | VARCHAR | `SUBUNIT` | Subunit description (important: a mine can have multiple subunit rows in production file) |
| mine_status | VARCHAR | `CURRENT_MINE_STATUS` | `"Active"`, `"Temporarily Idled"`, `"NonProducing"`, `"Intermittent"`, `"New Mine"`, `"Abandoned"`, `"Abandoned and Sealed"` |
| coal_type | VARCHAR | `PRIMARY_CANVASS` | `"Coal(Bituminous)"` or `"Coal(Anthracite)"` *(MSHA does not separately classify Subbituminous or Lignite in this field)* |
| production_short_tons | DOUBLE | `ANNUAL_COAL_PRODUCTION` | Short tons of coal produced |
| avg_employee_count | DOUBLE | `AVG_EMPLOYEE_CNT` | Average quarterly employee count |
| annual_hours | DOUBLE | `ANNUAL_HOURS` | Total employee-hours worked during the year |
| labor_productivity | DOUBLE | computed | `ANNUAL_COAL_PRODUCTION / ANNUAL_HOURS` (short tons per hour) |
| avg_mine_height_inches | DOUBLE | `AVG_MINE_HEIGHT` | Average mining height in **inches** *(earlier plan said feet — actual unit is inches)* |

> **Removed:** `mine_openings` (not in MSHA data), `seam_thickness_ft` (replaced by
> `avg_mine_height_inches` with correct unit from source).

---

## Primary Keys

| Table | PK Columns | Notes |
|---|---|---|
| `eia_electricity_generation` | `(state_abbr, energy_source_code, sector_code, generation_year, generation_month)` | `generation_month` NULL for annual-frequency rows; treat NULL as 0 in composite key |
| `eia_electricity_prices` | `(state_abbr, sector_code, price_year, price_month)` | `price_month` NULL for annual-only rows |
| `eia_utility_annual` | `(utility_id, report_year)` | `utility_id` is stable across years (EIA-861 Utility Number) |
| `eia_power_plants` | `(plant_id, generator_id, report_year)` | One row per generator per annual snapshot |
| `eia_capacity_changes` | `(plant_id, generator_id, snapshot_year, snapshot_month, change_type)` | Composite covers the same generator appearing on multiple sheets in one snapshot |
| `eia_fossil_fuel_production` | `(eia_area_code, fuel_type, process_code, production_year, production_month)` | `production_month` NULL for annual coal rows |
| `eia_state_energy_consumption` | `(state_abbr, msn, consumption_year)` | MSN encodes fuel × sector × metric — one row per MSN per state per year |
| `eia_natural_gas_storage` | `(eia_region_code, storage_type_code, report_date)` | `report_date` is the Wednesday of the report week |
| `eia_petroleum_stocks` | `(eia_area_code, product_code, process_code, report_date)` | `process_code` distinguishes SPR-inclusive vs. exclusive series for same product |
| `eia_crude_oil_imports` | `(importer_name, origin_country_code, refinery_site_id, rpt_period)` | No surrogate key in source; this composite is the best natural key |
| `eia_refinery_operations` | `(eia_area_code, series_id, report_year, report_month)` | Tall format — one row per series per PADD per month |
| `eia_coal_mines` | `(mine_id, subunit_code, report_year)` | MSHA MinesProdYearly has one row per mine × subunit × year |

---

## Foreign Keys

### Within-Schema

| Child Table | Child Column(s) | Parent Table | Parent Column(s) | Strength |
|---|---|---|---|---|
| `eia_power_plants` | `utility_id` | `eia_utility_annual` | `utility_id` | Hard — EIA-860 and EIA-861 share the same utility number namespace |
| `eia_capacity_changes` | `plant_id` | `eia_power_plants` | `plant_id` | Hard — EIA-860M plant IDs match EIA-860 |
| `eia_capacity_changes` | `entity_id` | `eia_utility_annual` | `utility_id` | Soft — `entity_id` in EIA-860M is the operator; may differ from the owning utility in EIA-861 for merchant plants |

### To `geo` Schema

| Child Table | Child Column | Parent Table | Parent Column | Note |
|---|---|---|---|---|
| `eia_electricity_generation` | `state_fips` | `geo.states` | `state_fips` | Derived via state_abbr lookup |
| `eia_electricity_prices` | `state_fips` | `geo.states` | `state_fips` | |
| `eia_utility_annual` | `state_fips` | `geo.states` | `state_fips` | HQ state; utility may serve multiple states |
| `eia_power_plants` | `state_fips` | `geo.states` | `state_fips` | |
| `eia_power_plants` | `county_fips` | `geo.counties` | `county_fips` | Derived via county-name-to-FIPS mapping; ~2% of plants lack county |
| `eia_capacity_changes` | `state_fips` | `geo.states` | `state_fips` | |
| `eia_capacity_changes` | `county_fips` | `geo.counties` | `county_fips` | Same derivation as eia_power_plants |
| `eia_fossil_fuel_production` | `state_fips` | `geo.states` | `state_fips` | duoarea → state_abbr → state_fips |
| `eia_state_energy_consumption` | `state_fips` | `geo.states` | `state_fips` | |
| `eia_crude_oil_imports` | `refinery_state_fips` | `geo.states` | `state_fips` | |
| `eia_coal_mines` | `state_fips` | `geo.states` | `state_fips` | |
| `eia_coal_mines` | `county_fips` | `geo.counties` | `county_fips` | 5-digit FIPS from MSHA BOM_STATE_CD + FIPS_CNTY_CD |

### To `sec` Schema (fuzzy — no surrogate key bridge)

| Child Table | Child Column | Parent Table | Match Column | Join Strategy |
|---|---|---|---|---|
| `eia_utility_annual` | `utility_name` | `sec.filing_metadata` | `company_name` | Normalized uppercase, strip "Inc"/"Corp"/"LLC"; match on longest common token sequence |
| `eia_power_plants` | `utility_name` | `sec.filing_metadata` | `company_name` | Same normalization |
| `eia_crude_oil_imports` | `receiving_company` | `sec.filing_metadata` | `company_name` | Refinery owner name — higher noise than utility names |
| `eia_crude_oil_imports` | `importer_name` | `sec.filing_metadata` | `company_name` | Trading company — many are non-public; match rate lower |
| `eia_coal_mines` | `controller_name` | `sec.filing_metadata` | `company_name` | Best coal company link; controller = ultimate parent entity |

### To `econ` Schema

| Child Table | Child Column | Parent Table | Match | Note |
|---|---|---|---|---|
| `eia_natural_gas_storage` | `report_date` | `econ.fred_indicators` | `date` WHERE `series = 'DHHNGSP'` | Henry Hub spot price; same Wednesday date |
| `eia_petroleum_stocks` | `report_date` | `econ.fred_indicators` | `date` WHERE `series = 'DCOILWTICO'` | WTI spot price; stocks published Friday, WTI is daily |
| `eia_fossil_fuel_production` | `state_fips` + `production_year` | `econ.bls_employment` | `state_fips` + `year` WHERE `naics = '211'` (oil/gas extraction) | Production volume vs. sector employment |
| `eia_state_energy_consumption` | `state_fips` + `consumption_year` | `econ.bls_employment` | `state_fips` + `year` WHERE `naics = '22'` (utilities) | Energy sector employment |

### To Other Planned Schemas

| Child Table | Child Column | Future Schema | Match | Note |
|---|---|---|---|---|
| `eia_fossil_fuel_production` | `state_fips` + `production_year` | `lands.onrr_revenues` | `state_fips` + `revenue_year` | Federal royalties vs. total state production |
| `eia_state_energy_consumption` | `state_fips` + `consumption_year` | `env.epa_ghg_emissions` | `state_fips` + `report_year` | Implied GHG from SEDS consumption vs. EPA reported emissions |
| `eia_coal_mines` | `county_fips` + `report_year` | `health.cdc_mortality` | `county_fips` + `year` | Coal dust / respiratory mortality correlation |
| `eia_power_plants` | `county_fips` + `report_year` | `env.epa_air_quality` | `county_fips` + `year` | Power plant density vs. local air quality index |

---

## Convenience Views

Pre-built analytical views for common energy research patterns. These mirror the ECON schema's `interest_rate_spreads`, `housing_indicators`, etc.

### `state_energy_mix`

State-level annual electricity generation breakdown by fuel category as a percentage share. Ready-to-query renewable transition dashboard; joins to `geo.states` for map visualization.

**Sources:** `eia_electricity_generation` aggregated to `(state_abbr, generation_year)` filtered to `sector_code = '99'` (All Sectors) and `generation_month IS NULL OR generation_month = 0` for annual totals.

| Column | Expression | Description |
|---|---|---|
| state_abbr | | 2-letter state abbreviation |
| state_fips | | FK to geo.states |
| generation_year | | Year |
| total_generation_thousand_mwh | `SUM(generation_thousand_mwh)` | Total net generation |
| coal_thousand_mwh | `SUM(...) WHERE energy_source_code = 'COL'` | Coal generation |
| gas_thousand_mwh | `SUM(...) WHERE energy_source_code = 'NG'` | Natural gas generation |
| nuclear_thousand_mwh | `SUM(...) WHERE energy_source_code = 'NUC'` | Nuclear generation |
| hydro_thousand_mwh | `SUM(...) WHERE energy_source_code = 'HYC'` | Conventional hydro |
| wind_thousand_mwh | `SUM(...) WHERE energy_source_code = 'WND'` | Wind generation |
| solar_thousand_mwh | `SUM(...) WHERE energy_source_code = 'SUN'` | Solar (utility + small-scale) |
| other_renewables_thousand_mwh | `SUM(...) WHERE energy_source_code IN ('GEO','BIO','OTH')` | Geothermal, biomass, other |
| fossil_pct | `(coal + gas) / total × 100` | Fossil fuel share |
| renewable_pct | `(hydro + wind + solar + other) / total × 100` | Renewable share |
| clean_pct | `(nuclear + renewable) / total × 100` | Carbon-free share |
| yoy_renewable_pct_change | `renewable_pct − LAG(renewable_pct, 1) OVER (PARTITION BY state_abbr ORDER BY generation_year)` | Year-over-year renewable share change (pp) |

---

### `utility_scorecard`

Annual electric utility company performance dashboard — customers, sales, revenue, and capacity. Primary view for joining to SEC 10-K filings for utility holding companies.

**Sources:** `eia_utility_annual` LEFT JOIN `eia_power_plants` aggregated by `(utility_id, report_year)` on `operating_status = 'OP'`.

| Column | Expression | Description |
|---|---|---|
| utility_id | | EIA utility number |
| report_year | | Year |
| utility_name | | Legal utility name |
| entity_type | | Investor Owned / Municipal / Cooperative / etc. |
| state_abbr | | HQ state |
| ba_code | | Balancing authority |
| nerc_region | | NERC region |
| customers_total | | Total customer accounts |
| sales_total_mwh | | Total retail electricity sales (MWh) |
| revenue_total_thousand | | Total retail revenue (thousand USD) |
| avg_retail_price_cents_kwh | | Computed average retail price |
| net_metering_installations | | Net metering installations count |
| net_metering_capacity_mw | | Total net metering capacity (MW) |
| operating_capacity_mw | `SUM(net_summer_capacity_mw) FROM eia_power_plants WHERE operating_status='OP'` | Total operating summer capacity (MW) |
| fossil_capacity_mw | `SUM(...) WHERE energy_source_1 IN ('NG','DFO','COL','RFO','PC','NG')` | Fossil fuel operating capacity |
| renewable_capacity_mw | `SUM(...) WHERE energy_source_1 IN ('SUN','WND','HYC','GEO','LFG','WDS')` | Renewable operating capacity |
| storage_capacity_mwh | `SUM(nameplate_energy_capacity_mwh) FROM eia_power_plants WHERE energy_storage_flag=true` | Battery/storage capacity |
| renewable_capacity_pct | `renewable_capacity_mw / operating_capacity_mw × 100` | Share of operating fleet that is renewable |
| sales_per_customer_mwh | `sales_total_mwh / customers_total` | Average consumption per customer (MWh) |

---

### `capacity_pipeline`

Planned generating capacity additions and retirements over the next 36 months. Leading indicator for utility capex and grid mix trajectory.

**Sources:** `eia_capacity_changes` filtered to `change_type IN ('Planned Addition', 'Planned Retirement', 'Cancellation')` with `change_year >= CURRENT_YEAR`.

| Column | Expression | Description |
|---|---|---|
| state_abbr | | State of plant |
| state_fips | | FK to geo.states |
| change_year | | Year of planned change |
| change_month | | Month of planned change |
| change_type | | Planned Addition / Planned Retirement / Cancellation |
| technology | | Full technology description |
| fuel_category | `CASE WHEN energy_source_code IN ('SUN','WND','HYC','GEO') THEN 'Renewable' WHEN energy_source_code IN ('NG','DFO','COL') THEN 'Fossil' WHEN energy_source_code = 'NUC' THEN 'Nuclear' WHEN prime_mover_code = 'ES' THEN 'Storage' ELSE 'Other' END` | Simplified fuel category |
| additions_mw | `SUM(net_summer_capacity_mw) WHERE change_type='Planned Addition'` | Total planned additions (MW) |
| retirements_mw | `SUM(net_summer_capacity_mw) WHERE change_type='Planned Retirement'` | Total planned retirements (MW) |
| net_change_mw | `additions_mw − retirements_mw` | Net capacity change (MW) |
| storage_additions_mwh | `SUM(nameplate_energy_capacity_mwh) WHERE change_type='Planned Addition' AND prime_mover_code='ES'` | Storage energy capacity additions (MWh) |
| project_count | `COUNT(DISTINCT plant_id)` | Number of distinct plants with changes |

---

### `weekly_gas_storage_signal`

Weekly natural gas storage surplus/deficit vs. 5-year historical average with week-over-week and year-over-year changes. Directly mirrors the EIA's weekly storage report market-moving format.

**Sources:** `eia_natural_gas_storage` filtered to `storage_type_code = 'SAW'` (Working Gas) with computed rolling averages.

| Column | Expression | Description |
|---|---|---|
| report_date | | Wednesday of the report week |
| storage_week | | ISO week number |
| storage_year | | Year |
| region | | EIA region label |
| eia_region_code | | EIA region code |
| working_gas_bcf | `volume_bcf` | Working gas in storage (Bcf) |
| change_bcf | `volume_bcf − LAG(volume_bcf,1) OVER (PARTITION BY eia_region_code ORDER BY report_date)` | Week-over-week change |
| yoy_change_bcf | | Same ISO week prior year |
| five_year_avg_bcf | `AVG(volume_bcf) OVER (PARTITION BY eia_region_code, storage_week ... 5 prior years)` | 5-year rolling same-week average |
| surplus_deficit_bcf | `working_gas_bcf − five_year_avg_bcf` | Surplus (+) or deficit (−) vs. 5-year average |
| surplus_deficit_pct | `surplus_deficit_bcf / five_year_avg_bcf × 100` | Percent above/below 5-year average |
| henry_hub_price | `econ.fred_indicators.value WHERE series='DHHNGSP' AND date = report_date` | Henry Hub spot price (USD/MMBtu) at time of report |

---

### `state_energy_burden`

Annual energy affordability analysis: retail electricity price × residential consumption relative to median household income. Policy-relevant view for low-income energy assistance programs.

**Sources:** `eia_electricity_prices` (sector='RES', annual) JOIN `eia_state_energy_consumption` (MSN like 'ES%C%' residential electric) JOIN `census.acs_income` on `(state_fips, year)`.

| Column | Expression | Description |
|---|---|---|
| state_abbr | | State abbreviation |
| state_fips | | FK to geo.states and census.acs_income |
| price_year | | Year |
| avg_price_cents_kwh | | Average residential retail price (cents/kWh) |
| residential_consumption_bbtu | | Residential energy consumption (Billion BTU) |
| residential_electricity_sales_million_kwh | `sales_million_kwh FROM eia_electricity_prices WHERE sector='RES'` | Residential retail electricity (million kWh) |
| median_household_income | `census.acs_income.median_household_income` | Median HH income (USD) |
| avg_annual_electricity_bill | `(avg_price_cents_kwh / 100) × (residential_electricity_sales_million_kwh × 1e6 / customers_residential)` | Estimated average annual residential electricity bill (USD) |
| electricity_burden_pct | `avg_annual_electricity_bill / median_household_income × 100` | Electricity cost as % of median income |
| energy_burden_pct | `(expenditure_million × 1e6 / population) / median_household_income × 100` | Total energy expenditure burden (all fuels) |
| burden_rank | `RANK() OVER (PARTITION BY price_year ORDER BY electricity_burden_pct DESC)` | State rank (1 = highest burden) |

---

### `fossil_production_price_correlation`

Monthly state-level fossil fuel production alongside corresponding commodity spot prices. Primary view for oil/gas E&P company revenue modeling and SEC 10-K cross-validation.

**Sources:** `eia_fossil_fuel_production` JOIN `econ.fred_indicators` on `(date = first day of period month)` for WTI and Henry Hub series.

| Column | Expression | Description |
|---|---|---|
| state_abbr | | State abbreviation |
| state_fips | | FK to geo.states |
| production_year | | Year |
| production_month | | Month (1–12) |
| crude_production_kbbl | `production_volume WHERE fuel_type='Crude Oil' AND process_code='VGM'` | Marketed crude oil production (thousand barrels) |
| gas_production_mmcf | `production_volume WHERE fuel_type='Natural Gas' AND process_code='VGM'` | Marketed natural gas production (MMcf) |
| wti_price_per_bbl | `econ.fred_indicators.value WHERE series='DCOILWTICO'` | WTI spot price (USD/bbl; monthly average) |
| henry_hub_price_per_mmbtu | `econ.fred_indicators.value WHERE series='DHHNGSP'` | Henry Hub spot price (USD/MMBtu; monthly average) |
| crude_revenue_implied_million | `(crude_production_kbbl × 1000 × wti_price_per_bbl) / 1e6` | Implied crude revenue at spot price (USD millions) |
| gas_revenue_implied_million | `(gas_production_mmcf × 1000 × henry_hub_price_per_mmbtu) / 1e6` | Implied gas revenue at spot price (USD millions) |
| combined_revenue_implied_million | `crude_revenue_implied_million + gas_revenue_implied_million` | Combined implied revenue (USD millions) |

---

### `refinery_utilization_summary`

Monthly U.S. and PADD refinery throughput, capacity, and utilization rates with product yield breakdown.

**Sources:** `eia_refinery_operations` pivoted from tall to wide format.

| Column | Expression | Description |
|---|---|---|
| eia_area_code | | PADD or NUS |
| padd | | Area label |
| report_year | | Year |
| report_month | | Month |
| crude_input_kbbl_per_day | | Crude runs to stills (MBBL/D) |
| operable_capacity_kbbl_per_day | | Operable distillation capacity (MBBL/D) |
| utilization_rate_pct | | Gross input / capacity (%) |
| gasoline_yield_pct | `gasoline_output / crude_input × 100` | Gasoline as % of crude input |
| distillate_yield_pct | `distillate_output / crude_input × 100` | Distillate as % of crude input |
| jet_fuel_yield_pct | `jet_fuel_output / crude_input × 100` | Jet fuel yield (%) |
| processing_gain_kbbl_per_day | | Volume gain from refining |
| vs_prior_month_pct | `utilization_rate_pct − LAG(utilization_rate_pct,1) OVER (PARTITION BY eia_area_code ORDER BY report_year, report_month)` | Month-over-month utilization change (pp) |
| vs_prior_year_pct | `utilization_rate_pct − LAG(utilization_rate_pct,12) OVER (...)` | Year-over-year utilization change (pp) |

---

## Table Summary

| # | Table | Source | Grain | Cadence | Key Cross-Schema Join |
|---|---|---|---|---|---|
| 1 | `eia_electricity_generation` | EIA-923 API | State × source × sector × month | Monthly | SEC utilities via state |
| 2 | `eia_utility_annual` | EIA-861 XLS | Utility company × year | Annual | SEC via utility_name |
| 3 | `eia_electricity_prices` | EIA-861/826 API | State × sector × month/year | Annual/monthly | CENSUS income (energy burden) |
| 4 | `eia_power_plants` | EIA-860 XLS | Plant × generator × year | Annual | SEC via utility_id / utility_name |
| 5 | `eia_capacity_changes` | EIA-860M XLS | Generator × change event | Monthly | SEC capex pipeline |
| 6 | `eia_fossil_fuel_production` | EIA-914/SEDS API | State × fuel × month | Monthly | LANDS ONRR royalties |
| 7 | `eia_state_energy_consumption` | SEDS API | State × MSN × year | Annual | CENSUS income, ENV GHG |
| 8 | `eia_natural_gas_storage` | EIA-912 API | Region × week | Weekly | ECON FRED Henry Hub price |
| 9 | `eia_petroleum_stocks` | EIA-PSW API | PADD × product × week | Weekly | ECON FRED WTI price |
| 10 | `eia_crude_oil_imports` | EIA-814 XLSX | Company × origin × refinery × month | Monthly | SEC refiners via company name |
| 11 | `eia_refinery_operations` | EIA-820/817 API | PADD × month | Monthly | SEC refiners via state/PADD |
| 12 | `eia_coal_mines` | MSHA open data | Mine × subunit × year | Annual | SEC coal producers via controller_name |

---

## Join Architecture

```
sec.filing_metadata (company_name)
    ├── eia_utility_annual (utility_name ~ company_name → revenue/sales context)
    ├── eia_power_plants (utility_name ~ company_name → asset inventory)
    ├── eia_crude_oil_imports (receiving_company / importer_name ~ company_name)
    └── eia_coal_mines (controller_name ~ company_name → coal output vs. 10-K)

geo.states (state_fips)
    ├── eia_electricity_generation (state_fips, generation_year)
    ├── eia_electricity_prices (state_fips, price_year)
    │       └── census.acs_income (state_fips → energy burden: price × income)
    ├── eia_fossil_fuel_production (state_fips, production_year)
    │       ├── lands.onrr_revenues (state_fips → federal royalty vs. total production)
    │       └── econ.fred_indicators (WTI/Henry Hub series → price-volume correlation)
    └── eia_state_energy_consumption (state_fips, consumption_year)
            ├── econ.bls_employment (state_fips → energy sector employment)
            └── env.epa_ghg_emissions (state_fips + year → implied vs. reported GHG)

geo.counties (county_fips)
    ├── eia_power_plants (county_fips, report_year)
    └── eia_coal_mines (county_fips, report_year)

eia_utility_annual (utility_id)
    ├── eia_power_plants (utility_id → generator inventory for that utility)
    └── eia_capacity_changes (entity_id → capex pipeline)

[weekly signals — region/PADD, no county FIPS]
eia_natural_gas_storage (eia_region_code, report_date)
    └── econ.fred_indicators (Henry Hub spot price → storage-price correlation)

eia_petroleum_stocks (eia_area_code, report_date)
    └── econ.fred_indicators (WTI spot price → inventory-price correlation)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 82 | initial | All 12 tables, full history (1990+) | 4 GB / 6 GB | Once |
| 83 | weekly | `eia_natural_gas_storage`, `eia_petroleum_stocks` | 1 GB / 2 GB | Weekly |
| 84* | monthly | `eia_electricity_generation`, `eia_fossil_fuel_production`, `eia_capacity_changes`, `eia_crude_oil_imports`, `eia_refinery_operations` | 2 GB / 3 GB | Monthly |
| 85* | annual | `eia_utility_annual`, `eia_electricity_prices`, `eia_power_plants`, `eia_state_energy_consumption`, `eia_coal_mines` | 2 GB / 3 GB | Annual |

*Workers 84–85 conflict with AGR schema in current plan — resolve when master worker map is finalized.*

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `eia_electricity_generation` | Months 1–12 | Monthly EIA-923; 2-month lag |
| `eia_utility_annual` | Month 10 | Annual EIA-861 |
| `eia_electricity_prices` | Month 10 (annual); 1–12 (monthly) | EIA-861/826 |
| `eia_power_plants` | Months 8–10 | Annual EIA-860 |
| `eia_capacity_changes` | Months 1–12 | Monthly EIA-860M |
| `eia_fossil_fuel_production` | Months 1–12 (oil/gas); month 11 (coal) | EIA-914/7A |
| `eia_state_energy_consumption` | Months 6–8 | Annual SEDS |
| `eia_natural_gas_storage` | Months 1–12 | Weekly EIA-912 |
| `eia_petroleum_stocks` | Months 1–12 | Weekly EIA-PSW |
| `eia_crude_oil_imports` | Months 1–12 | Monthly EIA-814; 2-month lag |
| `eia_refinery_operations` | Months 1–12 | Monthly Petroleum Supply Monthly |
| `eia_coal_mines` | Month 11 | Annual MSHA release |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENERGY_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=energy`) |
| `ENERGY_CACHE_DIR` | Recommended | Raw download cache |
| `ENERGY_EIA_API_KEY` | **Required** | EIA API key — free at eia.gov/opendata; all v2 endpoints reject requests without key |
| `GOVDATA_START_YEAR` | Optional | Historical start year for all modes (default: 2010) |
| `ENERGY_WEEKLY_LOOKBACK_WEEKS` | Optional | Weeks back to refresh on weekly runs (default: 4) |

---

## Implementation Notes

- **EIA v2 tall format.** All API endpoints return one row per `(period × facets)` with a
  single `value` field. Wide tables (e.g., `eia_refinery_operations`) require pivoting multiple
  series in the ETL. Use the `series` field as the pivot key.

- **State identifier mapping.** EIA uses two identifier systems: `stateid`/`location` (2-letter
  postal abbreviation) and `duoarea` (EIA area codes like `"STX"` for Texas). Neither maps
  directly to FIPS. Maintain a lookup table: `state_abbr → state_fips` (from `geo.states`)
  and `duoarea → state_abbr` (from EIA reference data at `api.eia.gov/v2/petroleum/sum/?api_key=`).

- **Period parsing.** Weekly data: `"YYYY-MM-DD"` → DATE. Monthly: `"YYYY-MM"` → parse year +
  month integers. Annual: `"YYYY"` → parse year integer. No endpoint provides a pre-split year
  or month field.

- **EIA-860 plant vs. generator grain.** The plan stores one row per generator in
  `eia_power_plants`. Plant-level attributes (`latitude`, `longitude`, `balancing_authority_code`,
  etc.) from `2___Plant_Y{YYYY}.xlsx` are denormalized onto each generator row. Total plant
  capacity is the sum of `net_summer_capacity_mw` across all operating generators for that plant.

- **EIA-861 multi-file join.** `eia_utility_annual` requires joining up to 6 separate XLS files
  within the annual ZIP on `Utility Number`. The `Sales_Ult_Cust` file has a 3-row composite
  header; rows 1–3 must be concatenated to derive column names before parsing data rows.

- **EIA-814 crude import file structure.** Monthly XLSX files named `impa{YY}{M}.xlsx` (e.g.,
  `impa24a.xlsx` for Jan 2024). Filter `PROD_CODE = '025'` for crude oil only. GCTRY_CODE to
  ISO3 mapping requires EIA's country code reference table.

- **MSHA data for coal mines.** MSHA Mines ZIP contains pipe-delimited files. Filter
  `COAL_METAL_IND = 'C'`. The MinesProdYearly file has multiple rows per mine (one per
  `SUBUNIT_CD`); sum `ANNUAL_COAL_PRODUCTION` and `ANNUAL_HOURS` across subunits for
  mine-level totals, or keep subunit-level rows for underground vs. surface split.

- **SEDS MSN lookup.** SEDS MSN codes are 5-character strings encoding fuel (pos 0–1),
  sector (pos 2–3), and metric type (pos 4: `C`=consumption, `D`=expenditure, `P`=price).
  The EIA publishes the full MSN lookup table at `eia.gov/opendata/documentation.php`.

- **`petroleum/sum/sndrc` is invalid.** Confirmed during validation — this path returns
  HTTP 400. Use `petroleum/crd/crpdn` for state-level crude production.

- **PADD region codes.** `petroleum/` and some `natural-gas/` endpoints use `duoarea` region
  codes: `"R10"` = PADD 1 (East Coast), `"R20"` = PADD 2 (Midwest), `"R30"` = PADD 3
  (Gulf Coast), `"R40"` = PADD 4 (Rocky Mountain), `"R50"` = PADD 5 (West Coast),
  `"NUS"` = Total Lower 48.
