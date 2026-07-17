# Ag Schema — International (UN) Data Plan

## Status: DELIVERED

The proposed FAOSTAT table is **implemented and merged to main**. It ships in [ag-schema.yaml](../../../src/main/resources/ag/ag-schema.yaml) with its transformer under [org.apache.calcite.adapter.govdata.ag](../../../src/main/java/org/apache/calcite/adapter/govdata/ag/).

| Planned table | Delivered as | Java |
|---|---|---|
| `faostat_production` | `faostat_production` (partition `[type, domain, year]`) + view `faostat_production_enriched` | `FaostatTransformer` |

**Delivery deviations:**
- Country key kept as raw M49 `country_code` FK'd to `ref.countries.iso_numeric`, rather than mapping M49 → ISO alpha-3 as proposed — functionally equivalent; aggregate rows flagged via `Area Code >= 5000`.

The proposal below is preserved for historical context.

The `ag` schema is U.S.-domestic today (USDA: `nass_crop_production`,
`nass_livestock_inventory`, `rma_crop_insurance`, `ers_farm_income`,
`fsa_commodity_payments`) — all geo-FK'd to `geo.states` / `geo.counties`. This plan adds
the **agriculture-related UN feed** — country-level production, land use, and food-balance
statistics — so U.S. USDA data is comparable against the rest of the world in one schema.

## Strategic Context

Cross-country agriculture belongs in `ag` (its analog schema), not `econ`. The UN feed is
**FAOSTAT** (UN FAO). Cross-schema value: joins to `ref.countries` for the geographic spine,
and reconciles semantically against `nass_crop_production` / `nass_livestock_inventory` by
commodity for U.S.-vs-world production comparison. Unlike the existing USDA tables (keyed on
FIPS via `geo`), FAOSTAT is the schema's first **country-level** table and keys on
`ref.countries.iso_alpha3`.

**Design** (project rule): one ETL table = one upstream source, **tall** (one row per
area × item × element × year). Wide/pivot reshaping is SQL views, never Java.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| FAOSTAT | UN FAO | Bulk CSV (normalized zips) + REST | Free, no key | Annual |

- FAOSTAT bulk: `https://bulks-faostat.fao.org/production/{domain}_All_Data_(Normalized).zip`
- FAOSTAT REST (incremental): `https://faostatservices.fao.org/api/v1/`

---

## Proposed Table — `faostat_production`

UN FAO FAOSTAT — agricultural production, land use, and food-balance statistics per area
(country) × item × element × year. Tall mirror of the U.S. USDA tables at country grain.
Joins to `ref.countries` on `country_code`; reconciles semantically against
`nass_crop_production` / `nass_livestock_inventory` by commodity name.

**Source:** FAOSTAT bulk — `https://bulks-faostat.fao.org/production/{domain}_All_Data_(Normalized).zip` (streamed per domain)
**Partition:** `[type, year]` — fan-out dimension is `domain` (QCL crops+livestock, RL land, FBS food balance, …)
**Auth:** None
**Cadence:** Annual (FAO refreshes domains through the year — freshness-gate the re-fetch)
**Release window:** Months 6–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Data year — partition column |
| domain | VARCHAR | FAOSTAT domain code (QCL, RL, FBS, TCL, …) |
| country_code | VARCHAR | ISO alpha-3 (FK to ref.countries.iso_alpha3; mapped from FAO area M49) |
| area_name | VARCHAR | FAO area name |
| is_aggregate | BOOLEAN | True for FAO regional/economic-group areas (World, Africa, EU, …) — exclude from country FK |
| item_code | VARCHAR | FAO item code (commodity / land class / balance line) |
| item_name | VARCHAR | Item name (Wheat, Cattle, Arable land, …) |
| element_code | VARCHAR | FAO element code (statistic type) |
| element_name | VARCHAR | Element (Production, Area harvested, Yield, Import quantity, …) |
| value | DOUBLE | Numeric value (null when FAO suppressed / not available) |
| unit | VARCHAR | Unit (tonnes, ha, hg/ha, 1000 head, …) |
| flag | VARCHAR | FAOSTAT observation flag (E=estimate, I=imputed, M=missing, …) |

**Materialize:** `format: iceberg`, partition `[type, year]`, `overwritePartitions: true`
(FAO republishes full history per domain, so replace year partitions wholesale — same pattern
as `econ.world_indicators` and the fix that kept `ers_farm_income` from duplicating to
billions of rows).

**Constraints:**

```
faostat_production:
  primaryKey: [type, year, domain, country_code, item_code, element_code]
  foreignKeys:
    - columns: [country_code] → ref.countries.iso_alpha3   # country areas only; aggregates flagged, not FK'd
```

**Transformer:** new `FaostatTransformer` (Java) to (1) stream the normalized bulk zip per
domain (as `ErsFarmIncomeProvider` / `RmaSummaryOfBusinessTransformer` do for USDA bulk
files); (2) map the FAO **area M49 code** (present in the normalized files) to ISO alpha-3
via `ref.countries.iso_numeric → iso_alpha3` — **no new crosswalk column needed** (M49 == ISO
3166-1 numeric for assigned countries); **fail loudly** on an unmapped real country (rule #6);
(3) flag FAO regional/economic-group areas as `is_aggregate`.

**Curated domain list.** Fan out over an explicit `domain` list in the schema YAML
`dimension_values`, seeded with the domains that concord to the USDA tables (QCL crops &
livestock, RL land use, FBS food balances). Do **not** ingest all ~80 FAOSTAT domains;
`log()` the list so coverage is never silently truncated.

---

## Join Architecture

```
ref.countries (iso_alpha3)
    └── faostat_production (country_code, year)   [country areas; FAO region/group areas flagged]
            └── ag.nass_crop_production / nass_livestock_inventory   semantic (commodity name), US-vs-world
```

The existing USDA tables stay FIPS-keyed to `geo`; `faostat_production` is the only `ag` table
that keys to `ref.countries`. Links to the USDA tables are semantic (commodity name), not a
shared key — the only declared FK is `country_code → ref.countries.iso_alpha3`.

---

## Environment Variables

`ag` uses `SCHEMA_NAME` (default `ag`) + global `GOVDATA_*`. FAOSTAT needs **no new key**.

| Variable | Required | Description |
|---|---|---|
| `SCHEMA_NAME` | (existing) | Schema name (default `ag`) |
| `GOVDATA_PARQUET_DIR` | (global) | Output path — already used by ag |
| `GOVDATA_START_YEAR` / `GOVDATA_END_YEAR` | (global) | Year window |

---

## Implementation Notes

- **Bulk over API.** FAOSTAT's normalized bulk zips carry full history far more cheaply than
  paging the REST API; stream the zip in a `dataProvider`/transformer hook and use the REST
  API only for incremental current-year deltas — the same pattern the existing ERS/RMA
  providers use.
- **Country codes.** The normalized files include an **Area Code (M49)** column → join
  through the existing `ref.countries.iso_numeric` (M49 == ISO 3166-1 numeric); no new
  crosswalk. FAO regional/economic-group areas (World, Africa, EU, Least Developed Countries,
  …) are kept but flagged `is_aggregate` so country-level joins exclude them without dropping
  useful denominators.
- **Overwrite semantics.** FAO republishes full per-domain history each release →
  `overwritePartitions: true`, matching `econ.world_indicators` and `ag.ers_farm_income`.
- **Verification gate.** Not "done" until an ETL/DQ run + `verify-tables` passes end-to-end
  through the Calcite→DuckDB read path.
