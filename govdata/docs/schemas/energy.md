# Energy Schema Documentation

## Overview

The `energy` schema provides access to U.S. energy data from the Energy Information Administration
(EIA) and Mine Safety and Health Administration (MSHA). It covers electricity generation, retail
prices, utility operations, power plant inventories, generator capacity changes, fossil fuel
production, state energy consumption, natural gas storage, petroleum stocks, crude oil imports,
refinery operations, and coal mine production.

The schema is served by `EnergySchemaFactory` via `GovDataSchemaFactory` and is driven by
`energy-schema.yaml` with configurable download automation.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `eia_electricity_generation` | Monthly electricity generation (MWh), fuel consumption, and cost by state, source, and sector | EIA API v2 `/v2/electricity/electric-power-operational-data` | Monthly |
| `eia_electricity_prices` | Monthly/annual retail electricity price (¢/kWh), revenue, sales, and customer counts by state and sector | EIA API v2 `/v2/electricity/retail-sales` | Monthly |
| `eia_utility_annual` | Annual utility survey: entity type, NERC region, service type, customer counts, sales, revenue, peak demand, and net generation | EIA Form 861 ZIP/XLSX | Annual |
| `eia_power_plants` | Annual generator inventory: plant/generator IDs, location, technology, prime mover, energy sources, capacity, ownership, and operating status | EIA Form 860 ZIP/XLSX | Annual |
| `eia_capacity_changes` | Monthly December snapshot of planned generator additions and retirements with nameplate/summer/winter capacity | EIA Form 860M XLSX | Monthly |
| `eia_fossil_fuel_production` | Monthly crude oil production by state (thousand barrels per day) | EIA API v2 `/v2/petroleum/crd/crpdn` | Monthly |
| `eia_state_energy_consumption` | Annual state energy consumption by sector and fuel type (trillion BTU) from SEDS | EIA API v2 `/v2/seds` | Annual |
| `eia_natural_gas_storage` | Weekly underground natural gas storage by EIA region (working gas, base gas, injections, withdrawals) | EIA API v2 `/v2/natural-gas/stor/wkly` | Weekly |
| `eia_petroleum_stocks` | Weekly petroleum product stocks by PADD district (crude, gasoline, distillate, jet fuel, etc.) | EIA API v2 `/v2/petroleum/stoc/wstk` | Weekly |
| `eia_crude_oil_imports` | Monthly crude oil imports: importer, origin country, entry port, PADD, refinery site, API gravity, sulfur content, volume | EIA Form 814 monthly XLSX archive | Monthly |
| `eia_refinery_operations` | Monthly refinery process data in tall format: crude inputs, distillation capacity, utilization, and secondary processes by PADD | EIA API v2 `/v2/petroleum/pnp/wiup` | Monthly |
| `eia_coal_mines` | Annual coal mine production and demographics by state, county, and mine type | MSHA MinesProdYearly bulk download | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `state_energy_mix` | Annual share of generation by fuel type per state (% of total MWh) | `eia_electricity_generation` |
| `utility_scorecard` | Per-utility annual scorecard: residential rate, commercial rate, total customers, net generation, efficiency proxy | `eia_utility_annual`, `eia_electricity_prices` |
| `capacity_pipeline` | Net planned capacity additions vs. retirements by state and technology | `eia_capacity_changes` |
| `weekly_gas_storage_signal` | Year-over-year weekly working gas storage comparison (national total, current vs prior year) | `eia_natural_gas_storage` |
| `state_energy_burden` | Average retail electricity price (¢/kWh) and MWh per customer by state (residential sector) | `eia_electricity_prices` |
| `refinery_utilization_summary` | Wide-format refinery utilization by PADD: crude inputs vs. capacity and utilization % | `eia_refinery_operations` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory; energy data lands in `${GOVDATA_PARQUET_DIR}/source=energy` |
| `GOVDATA_CACHE_DIR` | Root cache directory; energy cache lands in `${GOVDATA_CACHE_DIR}/energy` |

### Optional — credentials

| Variable | Description | Affected tables |
|---|---|---|
| `ENERGY_EIA_API_KEY` | Free EIA API key (register at eia.gov/opendata); raises rate limit from ~1 req/s to 5 req/s | All EIA API v2 tables |

### Optional — behavior

| Variable | Description |
|---|---|
| `GOVDATA_START_YEAR` | Historical start year for all modes (initial, weekly, monthly, annual). Default `2010`. |

---

## Maintenance: worker-energy.sh

All energy data maintenance is handled by a single parameterized script:

```
scripts/parallel/worker-energy.sh <mode>
```

See [Energy Data Maintenance](../operations/energy-maintenance.md) for the full runbook.

---

## Sample Queries

```sql
-- Annual renewable share of electricity generation by state (latest year)
SELECT state_abbr, state_name,
       ROUND(SUM(CASE WHEN energy_source_code IN ('SUN','WND','HYC','GEO','WAS','WWS')
                      THEN generation_thousand_mwh ELSE 0 END)
             / NULLIF(SUM(generation_thousand_mwh), 0) * 100, 1) AS renewable_pct
FROM energy.eia_electricity_generation
WHERE generation_year = (SELECT MAX(generation_year) FROM energy.eia_electricity_generation)
  AND sector_code = '99'
GROUP BY state_abbr, state_name
ORDER BY renewable_pct DESC;

-- Weekly working gas storage vs. prior year
SELECT report_date, value_bcf, prior_year_bcf,
       ROUND(value_bcf - prior_year_bcf, 1) AS yoy_diff_bcf
FROM energy.weekly_gas_storage_signal
ORDER BY report_date DESC
LIMIT 10;

-- Top 10 states by residential electricity price
SELECT state_name, ROUND(avg_price_cents_kwh, 2) AS price_cents_kwh
FROM energy.state_energy_burden
ORDER BY avg_price_cents_kwh DESC
LIMIT 10;

-- Refinery utilization by PADD district (latest month)
SELECT padd, report_year, report_month,
       crude_inputs_kbbl, operable_capacity_kbbl, utilization_pct
FROM energy.refinery_utilization_summary
WHERE report_year = (SELECT MAX(report_year) FROM energy.refinery_utilization_summary)
ORDER BY padd;
```
