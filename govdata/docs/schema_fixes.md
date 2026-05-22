# Schema Cadence Fixes

Assessment of all govdata schema YAMLs against updated cadence guidance. Three categories
of work: code-level changes, universal schema cleanup, and per-schema dimension fixes.

---

## Part 1: Code-Level Changes (Apply First)

### 1a. Default `overwritePartitions` to `true`

`overwritePartitions: true` is correct for every table. The append default is never safer.

**File:** `file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java`

Change the `IcebergConfigBuilder` field default:
```java
// line ~763
private boolean overwritePartitions = true;  // was false
```

Once defaulted, all explicit `overwritePartitions: true` entries in YAML become redundant
and can be removed as cleanup. Do not remove them yet — wait until after the default is
confirmed in a build.

### 1b. Add `GOVDATA_CURRENT_YEAR` and `GOVDATA_CURRENT_QUARTER`

Two concerns must be kept separate:

| Concern | Mechanism |
|---|---|
| Cache freshness — force a new fetch on schedule | Dynamic dimension (`GOVDATA_CURRENT_YEAR`, `GOVDATA_CURRENT_MONTH`, `GOVDATA_CURRENT_QUARTER`) |
| Availability gating — don't request data before it exists | `dataLag` + `releaseMonth` on `yearRange` |

**Files to modify:**

`govdata/scripts/parallel/common.sh` — add alongside `_CURRENT_MONTH`:
```bash
_CURRENT_YEAR=$(date +%Y)
_CURRENT_QUARTER=$(( ($(date +%-m) - 1) / 3 + 1 ))
```

`govdata/src/main/java/org/apache/calcite/adapter/govdata/GovDataSchemaFactory.java` —
inject alongside the existing `currentMonth` wiring:
```java
Object currentYearObj = operand.get("currentYear");
if (currentYearObj != null) {
  System.setProperty("GOVDATA_CURRENT_YEAR", String.valueOf(currentYearObj));
}
Object currentQuarterObj = operand.get("currentQuarter");
if (currentQuarterObj != null) {
  System.setProperty("GOVDATA_CURRENT_QUARTER", String.valueOf(currentQuarterObj));
}
```

**Patterns:**

Annual source, known release month (August):
```yaml
dimensions:
  year: ["${GOVDATA_CURRENT_YEAR}"]
  # scheduler runs after August; dataLag/releaseMonth on yearRange gates history
```

Annual source, estimated release window (Q3):
```yaml
dimensions:
  year: ["${GOVDATA_CURRENT_YEAR}"]
  month: ["07","08","09"]   # 3 fetches during window; pre-publication entries harmless
```

Annual source, published after the effective year (September of Y+1):
```yaml
dimensions:
  year:
    type: yearRange
    start: "${GOVDATA_START_YEAR:2010}"
    end: current
    dataLag: 1
    releaseMonth: 9
```

Quarterly source:
```yaml
dimensions:
  year: ["${GOVDATA_CURRENT_YEAR}"]
  quarter: ["${GOVDATA_CURRENT_QUARTER}"]
```

Historical runs: `GOVDATA_CURRENT_YEAR` unset → fallback `${GOVDATA_CURRENT_YEAR:FULL}`
signals the historical worker to iterate a `yearRange`. Exact fallback semantics TBD —
simplest implementation sets `GOVDATA_CURRENT_YEAR` to the effective end year for the run.

**Tables to migrate once these variables are available:** see Part 3 — tables with
`incrementalTtlDays` + `releaseWindow` (fec, fedregister, geo, econ-reference) should
be re-evaluated for `GOVDATA_CURRENT_YEAR` or `GOVDATA_CURRENT_QUARTER` dimensions.

### 1c. Remove `incrementalTtlDays` and `releaseWindow` Features

**Files to modify:**
- `file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java` — remove field, builder method, getter, YAML parser branch
- `file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java` — remove `incrementalTtlMs`, `isInReleaseWindow`, `filterUnprocessedWithSuccessTtl` branch, `ReleaseWindowConfig` gating
- Remove `ReleaseWindowConfig` inner class from `MaterializeConfig`

**Schema YAML lines to remove after code change:**

| Schema | Lines |
|---|---|
| `fec/fec-schema.yaml` | 358, 492, 574, 728, 890, 1052, 1233, 1402, 1542, 1686, 1882, 2094 |
| `fedregister/fedregister-schema.yaml` | 153 |
| `geo/geo-schema.yaml` | 207 |
| `lands/lands-schema.yaml` | 362, 449 |
| `ref/ref-schema.yaml` | 217, 289, 365, 474 |
| `econ/econ-reference-schema.yaml` | 181, 246, 323, 379, 482, 717, 857 |

### 1d. Remove `ttl_days` Feature (Cyber Schemas)

Remove the TTL map, `getTtlDays()`, `hasTtl()`, and `loadTtlConfig()` from
`AbstractCyberCacheManifest`.

**Schema YAML lines to remove:**

| Schema | Lines |
|---|---|
| `cyber/cyber-threat-schema.yaml` | 138, 245, 374, 473, 1044 (and header comments 8–9) |
| `cyber/cyber-vuln-schema.yaml` | 300, 400 (and header comment line 10) |
| `cyber/cyber-vuln-iceberg-smoke.yaml` | 89, 321 |

### 1e. Remove `current_year_ttl_days` (Econ)

**Schema YAML lines to remove:**
- `econ/econ-schema.yaml` lines 3734, 3752, 3772

---

## Part 2: Missing `overwritePartitions: true` (Universal Cleanup)

After Part 1a defaults it in code, this becomes cosmetic. Listed here for completeness
and for any interim period before the code change lands.

Every table below has an `iceberg:` block but no `overwritePartitions: true`:

| Schema | Tables |
|---|---|
| `census` | acs_population, acs_income, acs_housing, acs_education, acs_employment, acs_poverty, decennial_population, acs_race_ethnicity, acs_age, acs_commuting, acs_health_insurance, acs_language, acs_disability, acs_veterans, acs_migration, acs_occupation, acs_industry, acs_internet, acs_nativity, acs_marital_status, acs_household_type, acs_housing_tenure, acs_income_distribution, decennial_housing, pep_population, cbp_establishments, acs1_population, acs1_income, economic_census, saipe_poverty, sahie_insurance, bds_dynamics, abs_characteristics, nonemployer_statistics, building_permits, lodes_workplace |
| `crime` | cde_offenses, cde_police_employment, cde_hate_crimes, cde_use_of_force, cde_crime_agency, cde_arrests, cde_shr, cde_leoka, cde_supplemental, bjs_ncvs_* |
| `cyber_threat` | All tables |
| `cyber_vuln` | All tables |
| `econ` | All tables |
| `econ_reference` | All tables |
| `edu` | All tables |
| `energy` | All tables |
| `geo` | states, counties, places, zctas, census_tracts, block_groups, cbsa, congressional_districts, school_districts, state_legislative_lower, state_legislative_upper, county_subdivisions, tribal_areas, urban_areas, pumas, voting_districts, all crosswalk tables, rural_urban_continuum, ruca_codes, gazetteer_* |
| `health` | All tables |
| `lands` | timber_sales, nps_visitation, onrr_revenues |
| `patents` | All tables |
| `sec` | All tables |
| `weather` | cdo_monthly_summaries, cdo_annual_summaries, epa_annual_aqi, epa_daily_aqi, drought_monitor_weekly, hms_smoke_daily, hms_smoke_polygons |

---

## Part 3: Missing Time-Varying Dimensions (Critical — Data Correctness)

Tables with `rawCache: enabled: true` but no time-varying dimension have data permanently
frozen at the first fetch. Dimension pattern to use:

- **Monthly source or monthly cache bust:** `month: ["${GOVDATA_CURRENT_MONTH:12}"]`
- **Annual, known release month:** `year: ["${GOVDATA_CURRENT_YEAR}"]` + scheduler calendar gate
- **Annual, estimated release window:** `year: ["${GOVDATA_CURRENT_YEAR}"]` + `month: ["MM","MM","MM"]`
- **Annual, published after effective year:** `yearRange` with `dataLag` + `releaseMonth`
- **Truly static:** `rawCache: false` or keep `rawCache: true` with comment noting expected release

### crime

| Table | Fix |
|---|---|
| `cde_agencies` | Add `year: *crime_year_range` — agency registry updates annually |
| `cde_trends` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` — source updates monthly |
| `bjs_nibrs_estimates` | Add `year: *crime_year_range` — annual publication |

### econ

| Table | Fix |
|---|---|
| `bea_sainc_bulk`, `bea_sagdp_bulk`, and other BEA bulk ZIPs (`dimensions: {}`) | Add `year: *econ_year_range` — BEA publishes updated bulk ZIPs annually |

### geo

| Table | Fix |
|---|---|
| `nws_stations` | Use `rawCache: false` — station metadata changes; no clear annual cadence |
| `watersheds_huc2/4/8/12` | Static National GDB; rarely changes. Keep `rawCache: true`, add comment with next expected release year. Do not add a spurious year dimension. |
| `rural_urban_continuum`, `ruca_codes` | USDA updates decennially. Add comment with next expected release year (2033). |
| `congressional_districts` (`start: "2023", end: "2023"`) | Update `end` to `current` with `dataLag: 2` — 119th Congress data now exists. |

### health (systemic — nearly every table affected)

| Table | Fix |
|---|---|
| `fda_ndc_products` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` — FDA updates NDC codes monthly |
| `fda_drug_approvals` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` |
| `fda_drug_recalls` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` |
| `fda_device_recalls` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` |
| `fda_adverse_events` | Remove hardcoded date range from URL; parameterize with `{year}` and `{month}`; add `year: *health_year_range` + `month: ["${GOVDATA_CURRENT_MONTH:12}"]` |
| `cms_hospital_quality` | Add `year: ["${GOVDATA_CURRENT_YEAR}"]` + `month: ["7","8","9"]` — CMS releases annually, timing estimated Q3; month is pure cache-buster, not in URL |
| `cms_open_payments` | Remove hardcoded `PGYR2023` from URL path; parameterize with `{year}`; add `year: *health_year_range` with `dataLag: 1, releaseMonth: 6` |
| `cdc_mortality` | Add `year: *health_year_range` — both annual and weekly series are year-partitioned |
| `cdc_brfss` | Add `year: *health_year_range` with `dataLag: 1` — annual survey, prior-year data |
| `rxnorm_drugs` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` — RxNorm publishes monthly updates |
| `medicaid_drug_utilization` | Add `year: *health_year_range` — per-year dataset IDs |
| `clinical_trials` | `sinceDate`/`untilDate` params do not bust rawCache. Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` or use `rawCache: false` |
| `cdc_covid_vaccinations` | No fix needed — CDC froze this dataset 2023-05-12 |

### weather

| Table | Fix |
|---|---|
| `nws_alerts` | Use `rawCache: false` — real-time feed; no historical value; caching is wrong |

### cyber_threat

| Table | Fix |
|---|---|
| `attack_techniques` | Add `year: ["${GOVDATA_CURRENT_YEAR}"]` + `month: ["4","10"]` — MITRE releases ~2×/year (April and October); month is pure cache-buster, not in URL |
| `cwe_catalog` | Add `year: ["${GOVDATA_CURRENT_YEAR}"]` + `quarter: ["${GOVDATA_CURRENT_QUARTER}"]` — MITRE updates quarterly (requires Part 1b) |

### cyber_vuln

| Table | Fix |
|---|---|
| `kev_catalog`, `kev_cwes` | Use `rawCache: false` — daily-updated CISA feed; no historical value from caching |
| `osv_vulnerabilities`, `vuln_cross_refs`, `advisories` | Add `year: *cyber_year_range` or use `rawCache: false` |

### ref

| Table | Fix |
|---|---|
| `gleif_entities`, `gleif_cik_mapping` | Add `year: *ref_year_range` — GLEIF publishes daily golden copies; annual dimension gives one fresh fetch per year |
| `sec_company_tickers` | Add `month: ["${GOVDATA_CURRENT_MONTH:12}"]` — tickers change frequently |
| `figi_instruments` | Add `year: *ref_year_range` |

### lands

| Table | Fix |
|---|---|
| `national_forests`, `nps_units`, `blm_field_offices` | Add `year: *lands_year_range` — boundaries update annually |
| `onrr_revenues` | Use `rawCache: false` — bulk historical CSV (FY 2004–present) in one file; or add `year: *lands_year_range` if URL can be parameterized |
| `forest_inventory`, `forest_metrics` | Add `year: *lands_year_range` — FIA publishes per-inventory-year data |

### econ_reference

| Table | Fix |
|---|---|
| `jolts_industries`, `jolts_dataelements`, `bls_geographies`, `naics_sectors` | Add `year: *econ_ref_year_range` — reference tables that rarely but do change; annual dimension forces one refresh per year |
| `nipa_tables`, `regional_linecodes` | Add `year: *econ_ref_year_range` |

### patents

| Table | Fix |
|---|---|
| `trademark_applications` | `end: 2022` is stale. Update `end` to `current` with `dataLag: 1` — USPTO has released subsequent snapshots |

---

## Part 4: No Fixed Years Rule Violations

Hardcoded calendar years in URLs or YAML config (outside of legitimate `start:` floors):

| Schema | Table | Violation | Fix |
|---|---|---|---|
| `health` | `fda_adverse_events` | `20230101 TO 20251231` hardcoded in URL | Parameterize with `{year}` and `{month}` dimensions — see Part 3 |
| `health` | `cms_open_payments` | `PGYR2023` hardcoded in URL path | Parameterize path segment with `{year}` — see Part 3 |
| `geo` | `congressional_districts` | `start: "2023", end: "2023"` — fixed single year | Set `end: current` with `dataLag: 2` — see Part 3 |
| `patents` | `trademark_applications` | `end: 2022` — stale fixed end | Set `end: current` with `dataLag: 1` — see Part 3 |

---

## Priority Order

1. **Part 1a** — default `overwritePartitions: true` in code ✓ Done
2. **Part 1b** — add `GOVDATA_CURRENT_YEAR` and `GOVDATA_CURRENT_QUARTER` to `common.sh` and `GovDataSchemaFactory`
3. **Parts 1c/1d/1e** — remove TTL features from code and all YAMLs
4. **Part 3, health** — systemic frozen data across most actively-used tables
5. **Part 3, cyber** — `attack_techniques`, `cwe_catalog`, `kev_*` (some require Part 1b)
6. **Part 3, crime/ref/lands/econ** — remaining dimension fixes
7. **Part 4** — hardcoded year cleanup in URLs (several overlap with Part 3)
8. **Part 2** — `overwritePartitions: true` YAML cleanup (cosmetic after Part 1a defaults it)
6. **Part 2** — cleanup after Part 1a default lands
7. **Part 4** — hardcoded year fixes in URLs
