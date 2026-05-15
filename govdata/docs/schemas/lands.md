# Lands Schema Documentation

## Overview

The `lands` schema provides access to U.S. federal public lands data. It covers USFS national
forest boundaries and timber harvest activities, FIA forest inventory and live-tree metrics,
NPS park unit boundaries and visitation statistics, BLM field office boundaries, and ONRR
federal mineral royalty revenues.

The schema is served by `LandsSchemaFactory` via `GovDataSchemaFactory` and is driven by
`lands-schema.yaml`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `national_forests` | USFS national forest and grassland boundary polygons with area and administrative region | USFS ArcGIS EDW `/EDW/EDW_AdminForest` feature service | Annual |
| `timber_sales` | USFS timber harvest activities: forest, activity type, fiscal year, GIS acres, units accomplished, state | USFS FACTS ArcGIS `/EDW/EDW_Activity` feature service | Annual |
| `forest_inventory` | FIA forest inventory estimates from COND CSV: one row per state × forest type group × ownership class × inventory year with basal area, net volume, and condition proportion | FIA Datamart `{state}_COND.csv` bulk downloads | Annual (FIA cycle) |
| `forest_metrics` | FIA live-tree metrics from TREE CSV joined to COND: one row per state × forest type group × ownership class × inventory year with trees-per-acre, live cubic-foot volume, and above-ground carbon stock | FIA Datamart `{state}_TREE.csv` + `{state}_COND.csv` | Annual (FIA cycle) |
| `nps_units` | NPS park unit metadata: name, type, state, area, designation year, lat/lon | NPS IRMA ArcGIS feature service | Annual |
| `nps_visitation` | Monthly NPS park visitation counts by park unit and year | NPS IRMA visitation API | Annual |
| `blm_field_offices` | BLM field office boundaries with area and state | BLM ArcGIS feature service | Annual |
| `onrr_revenues` | ONRR federal mineral royalty revenues: state, county, commodity, revenue type, fiscal year, amount | ONRR bulk download | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `lands_forest_condition_metrics` | Joins `forest_inventory` and `forest_metrics` on state × forest type group × ownership class × inventory year — five FIA metrics in a single row | `forest_inventory`, `forest_metrics` |
| `lands_timber_by_forest_state` | Annual USFS timber harvest area and volume by state; joins to `national_forests` via forest_code | `timber_sales`, `national_forests` |
| `lands_nps_gateway_impact` | Annual NPS visitation totals per park unit with unit metadata; ready to join `econ.bls_employment` on county | `nps_visitation`, `nps_units` |
| `lands_onrr_by_state_commodity` | ONRR federal mineral revenues aggregated by state, commodity, and fiscal year | `onrr_revenues` |
| `lands_nps_gateway_employment` | NPS annual visitation correlated with county-level employment and wages (`econ.county_wages` BLS QCEW) | `nps_visitation`, `nps_units`, `econ.county_wages` |
| `lands_onrr_energy_revenues` | ONRR revenues for energy commodities (Oil, Gas, Coal) aggregated by state and fiscal year | `onrr_revenues` |
| `lands_timber_public_companies` | Correlates USFS timber harvest activity with SEC-registered timber/paper companies (SIC 0800-0899, 2400-2499, 2600-2699) | `timber_sales`, `sec.filing_metadata` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |

---

## FIA Data Notes

- `forest_inventory` is derived from `{state}_COND.csv` — available for all 51 FIA-reporting states/territories.
- `forest_metrics` is derived from `{state}_TREE.csv` joined to `{state}_COND.csv` — large states (LA, GA, ME, MI) have multi-million-row TREE files; expect 2–70 minutes per state during initial ETL.
- Both tables partition by `stateAbbr` and `type`. Join on `(state_fips, inventory_year, forest_type_group, ownership_class)`.
- `forest_metrics.incrementalTtl=365d` with release window months 6–9 (Jun–Sep); TTL refresh is suppressed outside that window.

---

## Sample Queries

```sql
-- Carbon stock by ownership class (latest FIA inventory)
SELECT ownership_class, state_fips,
       SUM(carbon_stock_tons) AS total_carbon_tons_per_acre
FROM lands.forest_metrics
GROUP BY ownership_class, state_fips
ORDER BY total_carbon_tons_per_acre DESC;

-- Top NPS parks by annual visitation
SELECT unit_name, unit_type, state_code,
       SUM(recreation_visitors) AS annual_visitors
FROM lands.nps_visitation
WHERE year = (SELECT MAX(year) FROM lands.nps_visitation)
GROUP BY unit_name, unit_type, state_code
ORDER BY annual_visitors DESC
LIMIT 20;

-- Federal mineral royalty revenues by commodity (latest year)
SELECT commodity, SUM(revenue) AS total_revenue
FROM lands.onrr_revenues
WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM lands.onrr_revenues)
GROUP BY commodity
ORDER BY total_revenue DESC;

-- Combined forest condition metrics view
SELECT state_fips, forest_type_group, ownership_class, inventory_year,
       basal_area_sq_ft_per_acre, net_volume_cuft_per_acre,
       live_volume_cuft, carbon_stock_tons
FROM lands.lands_forest_condition_metrics
WHERE ownership_class = 'National Forest'
ORDER BY inventory_year DESC, carbon_stock_tons DESC;
```
