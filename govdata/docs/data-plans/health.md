# Health Schema — International (UN) Data Plan

The `health` schema is U.S.-domestic today (openFDA: `fda_ndc_products`, `fda_drug_approvals`,
`fda_drug_recalls`, `fda_adverse_events`). This plan adds the **health-related UN feeds** —
country-level health statistics that mirror the U.S. clinical/drug data with international
public-health indicators, giving U.S.-vs-world comparability inside the same schema.

## Strategic Context

Cross-country health data belongs in `health` (its analog schema), not `econ`. The primary
UN feed is the **WHO Global Health Observatory (GHO)** — mortality, disease burden, risk
factors, and service coverage per country × indicator × year. Cross-schema value: joins to
`ref.countries` for the geographic spine, and reconciles semantically against
`econ.world_indicators` health codes (e.g. `SH.DYN.MORT`, `SH.STA.MMRT`) — WHO is the
authoritative source, World Bank the harmonized redistribution.

**Design** (project rule): each ETL table mirrors exactly one upstream source, **tall** (one
row per country × indicator × dimensions × year). Wide/pivot reshaping is SQL views, never Java.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Global Health Observatory (GHO) | WHO | OData (JSON) | Free, no key | Annual (irregular per indicator) |
| *(follow-on)* Child mortality (IGME) | UNICEF | CSV / API | Free, no key | Annual |
| *(follow-on)* HIV/AIDS estimates | UNAIDS | CSV | Free, no key | Annual |

- WHO GHO OData: `https://ghoapi.azureedge.net/api/{IndicatorCode}` (indicator catalog at `.../api/Indicator`)

---

## Proposed Table — `who_gho_indicators`

WHO Global Health Observatory — health indicators per country × indicator × dimensions ×
year. International mirror of the U.S. clinical data in `health`. Joins to `ref.countries` on
`country_code` (GHO `SpatialDim` is already ISO alpha-3 for COUNTRY rows); reconciles
semantically against `econ.world_indicators` health codes.

**Source:** WHO GHO OData — `https://ghoapi.azureedge.net/api/{indicator}`
**Partition:** `[type, year]` — fan-out dimension is `indicator_code` (curated catalog, not the full GHO index)
**Auth:** None
**Cadence:** Annual (irregular; indicators update on their own WHO schedules — freshness-gate the re-fetch)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Data year (`TimeDim`) — partition column |
| indicator_code | VARCHAR | GHO indicator code (e.g. `WHOSIS_000001` life expectancy) |
| indicator_name | VARCHAR | Indicator label |
| country_code | VARCHAR | ISO alpha-3 (`SpatialDim`; FK to ref.countries.iso_alpha3 for COUNTRY rows) |
| spatial_type | VARCHAR | `SpatialDimType` — COUNTRY / REGION / WORLDBANKINCOMEGROUP |
| is_aggregate | BOOLEAN | True when spatial_type != COUNTRY (region/income-group roll-up — exclude from country FK) |
| sex | VARCHAR | Sex dimension (BTSX / MLE / FMLE) when present |
| dimension | VARCHAR | Additional GHO dimension value (age group, cause, …) when present |
| value_numeric | DOUBLE | Numeric value (`NumericValue`; null when only a display string exists) |
| value_display | VARCHAR | Display value string (e.g. "72.6 [70.1–75.0]") |
| unit | VARCHAR | Unit when GHO supplies one |

**Materialize:** `format: iceberg`, partition `[type, year]`, `overwritePartitions: true`
(WHO republishes full history per indicator, so replace year partitions wholesale — same
pattern as `econ.world_indicators`; avoids the append-duplication trap).

**Constraints:**

```
who_gho_indicators:
  primaryKey: [type, year, indicator_code, country_code, spatial_type, sex, dimension]
  foreignKeys:
    - columns: [country_code] → ref.countries.iso_alpha3   # COUNTRY rows only; aggregates flagged, not FK'd
```

**Transformer:** new `WhoGhoTransformer` (Java) to (1) extract the OData `value` array per
indicator; (2) pass through `SpatialDim` (already ISO alpha-3) and flag non-COUNTRY
`SpatialDimType` as `is_aggregate` — a COUNTRY row whose code fails to resolve against
`ref.countries` **fails loudly**, no silent drop (rule #6); (3) normalize the sex/dimension
codes.

**Curated indicator catalog.** Fan out over an explicit `indicator_code` list in the schema
YAML `dimension_values` (seed it in a `who/who-gho-indicators.json` resource, mirroring the
`worldbank/worldbank-indicators.json` pattern). Do **not** ingest the full GHO index (~2,000
indicators); `log()` the list size so coverage is never silently truncated. Seed set:

| Theme | GHO codes |
|---|---|
| Life expectancy / mortality | `WHOSIS_000001` (life expectancy at birth), `WHOSIS_000002` (HALE), `MORT_MATERNALNUM`, `MDG_0000000001` (under-5 mortality rate) |
| Disease burden | `MDG_0000000020` (TB incidence), `MALARIA_EST_INCIDENCE`, `HIV_0000000001` (HIV prevalence) |
| Risk factors | `NCD_BMI_30A` (obesity), `SA_0000001688` (alcohol per capita), `M_Est_tob_curr_std` (tobacco use) |
| Health system | `WHS4_100` (measles immunization), `UHC_INDEX_REPORTED` (UHC service coverage), `HWF_0001` (physicians per 10k) |

---

## Join Architecture

```
ref.countries (iso_alpha3)
    └── who_gho_indicators (country_code, year)   [COUNTRY rows; region/income aggregates flagged]
            └── econ.world_indicators (health WDI codes)   semantic reconciliation, US-vs-world
```

All links to the U.S. openFDA `health` tables are semantic (no shared clinical key); the only
declared FK is `country_code → ref.countries.iso_alpha3`.

---

## Environment Variables

`health` uses `HEALTH_SCHEMA_NAME` + global `GOVDATA_*`. WHO GHO needs **no new key** (open API).

| Variable | Required | Description |
|---|---|---|
| `HEALTH_SCHEMA_NAME` | (existing) | Schema name (default `health`) |
| `GOVDATA_PARQUET_DIR` | (global) | Output path — already used by health |
| `GOVDATA_START_YEAR` / `GOVDATA_END_YEAR` | (global) | Year window |

---

## Implementation Notes

- **No API key, but rate-limit-friendly.** GHO is a static CDN (`ghoazureedge.net`); still
  throttle the per-indicator fan-out and enable `rawCache` so re-runs don't re-download.
- **Country codes already align.** GHO COUNTRY rows use ISO alpha-3 → join straight to
  `ref.countries.iso_alpha3`; no crosswalk needed (unlike Comtrade's M49). Region and
  income-group rows (`spatial_type != COUNTRY`) are kept but flagged `is_aggregate`.
- **Numeric vs. display values.** Many GHO indicators publish a confidence-interval string
  in `Value` with the point estimate in `NumericValue`. Keep both columns; never parse the
  point estimate out of the display string.
- **Overwrite semantics.** WHO republishes full per-indicator history each release →
  `overwritePartitions: true`, matching `econ.world_indicators`.
- **Verification gate.** Not "done" until an ETL/DQ run + `verify-tables` passes end-to-end
  through the Calcite→DuckDB read path.

## Deferred follow-ons

- **UNICEF IGME** (child/neonatal mortality) and **UNAIDS** (HIV/AIDS estimates) are the
  next UN health feeds; both are country-level, ISO-alpha-3-joinable, and would follow the
  same tall + `ref.countries` FK pattern as `who_gho_indicators`.
