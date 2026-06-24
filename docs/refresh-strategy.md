# GovData Refresh / Freshness Strategy

How every GovData ETL table decides whether to re-fetch and re-materialize on an
`--etl-resume` run. The goal is idempotence: a second run over unchanged sources
should do **no** downloads and **no** Iceberg writes.

This document is generated from a survey of the schema YAMLs
(`govdata/src/main/resources/**/*-schema.yaml`). Regenerate it when schemas change.

## The three tiers

Every fetch table is gated by exactly one of these standard mechanisms — there is
no separate "cadence" abstraction, only the existing dimension + freshness machinery.

| Tier | Mechanism | What it skips | When to use |
|---|---|---|---|
| **T1 — pre-download probe** | `freshness:` = `etag` / `last_modified` / `size` / `count` / `version` / `graphql`. A HEAD or cheap probe is compared to the last stored token. | The **fetch** (and the write). Cheapest. | Any source that exposes a validator (ETag, Last-Modified, a total-count endpoint, a version field). **Preferred wherever the source supports it.** |
| **T2 — period + completion** | A period dimension (`year`/`quarter`/`month`/`week`/`day`/`day_of_week`). Per-period completion markers persist across runs; a completed period is dropped before any fetch. | The **attempt** (fetch + write) for periods already done. | Genuinely periodic data, and as the cadence gate for "latest" sources (see below). Proven by `bjs_ncvs`, `sec.*`, `census.acs_*`. |
| **T3 — post-download hash** | `freshness:` = `hash`. The payload is downloaded, hashed, and the write is skipped if the hash matches the last run. | Only the **write**. Still downloads every run. | Last resort — only where neither a probe nor a natural period fits. The hash is computed by **streaming** the rows through an order-independent multiset digest (O(1) memory — it no longer materialises the whole fetch, so large tables are safe), but it still **re-downloads every run**. Prefer a period/probe when one exists. |

### The one rule

**Never put a period dimension on a `dataset_type: snapshot` table.** `snapshot`
means "the open snapshot always re-runs" — it deliberately ignores per-period
completion. Pairing the two makes every resume run see the period as unprocessed,
trip self-heal re-registration, and **duplicate rows** (verified on `cde_trends`:
run 3 went 10 → 20 rows). A latest-snapshot table is either `snapshot` + a probe/hash
(no period), **or** a plain period dimension (no `snapshot`) — never both.

## Cadence for periodless "latest" tables

A handful of T3 hash tables have **no natural period** (a single "LATEST" blob):
they re-download on every resume run just to compute the hash. To bound that to the
source's real update frequency, give them a standard period dimension matched to that
frequency — `year/month` for monthly data, `year/month/day` for daily. This is the
same Tier-2 machinery, not a new concept; the period also becomes a partition, so the
table keeps a per-period history of the snapshot.

`day_of_year` / `workday-of-year` (business-day, Mon–Fri) are proposed additions to
the standard period-slot list for daily/business-day cadence; until they exist,
`year/month/day` covers daily cadence with existing slots.

## Inventory by schema and table

Fetch tables only (derived/view tables are summarized at the end). `Tier` per the
table above; `—` = none.

### census

| Table | Period | Freshness | Tier |
|---|---|---|---|
| acs_population | year | — | T2 period |
| acs_income | year | — | T2 period |
| acs_housing | year | — | T2 period |
| acs_education | year | — | T2 period |
| acs_employment | year | — | T2 period |
| acs_poverty | year | — | T2 period |
| decennial_population | year | — | T2 period |
| acs_race_ethnicity | year | — | T2 period |
| acs_age | year | — | T2 period |
| acs_commuting | year | — | T2 period |
| acs_health_insurance | year | — | T2 period |
| acs_language | year | — | T2 period |
| acs_disability | year | — | T2 period |
| acs_veterans | year | — | T2 period |
| acs_migration | year | — | T2 period |
| acs_occupation | year | — | T2 period |
| acs_industry | year | — | T2 period |
| acs_internet | year | — | T2 period |
| acs_nativity | year | — | T2 period |
| acs_marital_status | year | — | T2 period |
| acs_household_type | year | — | T2 period |
| acs_housing_tenure | year | — | T2 period |
| acs_income_distribution | year | — | T2 period |
| decennial_housing | year | — | T2 period |
| pep_population | year | — | T2 period |
| cbp_establishments | year | — | T2 period |
| acs1_population | year | — | T2 period |
| acs1_income | year | — | T2 period |
| economic_census | year | — | T2 period |
| saipe_poverty | year | — | T2 period |
| sahie_insurance | year | — | T2 period |
| bds_dynamics | year | — | T2 period |
| abs_characteristics | year | — | T2 period |
| nonemployer_statistics | year | — | T2 period |
| building_permits | year | — | T2 period |
| qwi_employment | time | hash | T3 hash |
| lodes_workplace | year | — | T2 period |
| trade_exports | time | hash | T3 hash |
| trade_imports | time | hash | T3 hash |

### econ

| Table | Period | Freshness | Tier |
|---|---|---|---|
| employment_statistics | year | — | T2 period |
| inflation_metrics | year | — | T2 period |
| regional_cpi | year | — | T2 period |
| metro_cpi | year | — | T2 period |
| state_industry | year | — | T2 period |
| state_wages | year | — | T2 period |
| metro_industry | year | — | T2 period |
| metro_wages | year | — | T2 period |
| county_qcew | year | — | T2 period |
| county_wages | year | — | T2 period |
| jolts_regional | year | — | T2 period |
| jolts_industry | year | — | T2 period |
| jolts_state | year | — | T2 period |
| wage_growth | year | — | T2 period |
| regional_employment | year | — | T2 period |
| treasury_yields | year | — | T2 period |
| federal_debt | year | — | T2 period |
| world_indicators | year | last_modified | T1 pre-probe |
| fred_indicators | year | — | T2 period |
| national_accounts | year | — | T2 period |
| state_personal_income | year | last_modified | T1 pre-probe |
| state_gdp | year | last_modified | T1 pre-probe |
| state_quarterly_income | year | last_modified | T1 pre-probe |
| state_quarterly_gdp | year | last_modified | T1 pre-probe |
| state_consumption | year | last_modified | T1 pre-probe |
| regional_income | year | — | T2 period |
| ita_data | year | — | T2 period |
| gdp_statistics | year | — | T2 period |
| industry_gdp | year | — | T2 period |

### econ-reference

| Table | Period | Freshness | Tier |
|---|---|---|---|
| jolts_industries | year | — | T2 period |
| jolts_dataelements | year | — | T2 period |
| nipa_tables | year | — | T2 period |
| regional_linecodes | year | — | T2 period |
| fred_series | — | hash | T3 hash |

### fec

| Table | Period | Freshness | Tier |
|---|---|---|---|
| candidates | year | — | T2 period |
| committees | year | — | T2 period |
| candidate_committee_linkages | year | — | T2 period |
| individual_contributions | year | — | T2 period |
| committee_contributions | year | — | T2 period |
| intercommittee_transactions | year | — | T2 period |
| operating_expenditures | year | — | T2 period |
| independent_expenditures | year | — | T2 period |
| electioneering_communications | year | — | T2 period |
| communication_costs | year | — | T2 period |
| candidate_summaries | year | — | T2 period |
| committee_summaries | year | — | T2 period |

### crime

| Table | Period | Freshness | Tier |
|---|---|---|---|
| cde_agencies | year | — | T2 period |
| cde_offenses | year | — | T2 period |
| cde_police_employment | year | — | T2 period |
| cde_hate_crimes | year,month | — | T2 period |
| cde_use_of_force | year | — | T2 period |
| cde_reta | year | — | T2 period |
| cde_arrests | year,month | — | T2 period |
| cde_shr | year,month | — | T2 period |
| cde_leoka | year | — | T2 period |
| cde_trends | year | hash | T3 hash |
| cde_supplemental | year,month | — | T2 period |
| bjs_nibrs_estimates | year | — | T2 period |
| bjs_ncvs_personal | year | — | T2 period |
| bjs_ncvs_personal_pop | year | — | T2 period |
| bjs_ncvs_household | year | — | T2 period |
| bjs_ncvs_household_pop | year | — | T2 period |

### cyber-threat

| Table | Period | Freshness | Tier |
|---|---|---|---|
| attack_techniques | — | etag | T1 pre-probe |
| ioc_urls | — | etag | T1 pre-probe |
| ioc_hashes | — | etag | T1 pre-probe |
| ioc_ips | — | etag | T1 pre-probe |
| ioc_mixed | — | hash | T3 hash |
| nist_controls | — | etag | T1 pre-probe |
| nist_csf_functions | — | etag | T1 pre-probe |
| cis_controls | — | etag | T1 pre-probe |
| owasp_top10 | — | etag | T1 pre-probe |
| attack_to_nist_mappings | — | etag | T1 pre-probe |
| threat_pulses | first_seen | version + watermark | T1 pre-probe (watermark delta) |

### cyber-vuln

| Table | Period | Freshness | Tier |
|---|---|---|---|
| cwe_catalog | — | last_modified | T1 pre-probe |
| vulnerabilities | year,quarter | count | T1 pre-probe |
| vulnerability_cwes | year,quarter | count | T1 pre-probe |
| kev_catalog | — | etag | T1 pre-probe |
| kev_cwes | — | etag | T1 pre-probe |
| osv_vulnerabilities | — | etag | T1 pre-probe |
| vuln_cross_refs | — | graphql | T1 pre-probe |
| advisories | year | — | T2 period |

### edu

| Table | Period | Freshness | Tier |
|---|---|---|---|
| ccd_districts | year | — | T2 period |
| ccd_schools | year | hash | T3 hash |
| naep_scores | year | — | T2 period |
| naep_achievement_levels | year | — | T2 period |
| crdc_schools | year | — | T2 period |
| ipeds_institutions | year | hash | T3 hash |
| ipeds_completions | year | — | T2 period |
| ipeds_financials | year | — | T2 period |
| ipeds_tuition | year | — | T2 period |
| college_scorecard | year | — | T2 period |
| college_scorecard_programs | year | — | T2 period |

### health

| Table | Period | Freshness | Tier |
|---|---|---|---|
| fda_ndc_products | — | version | T1 pre-probe |
| fda_drug_approvals | — | version | T1 pre-probe |
| fda_drug_recalls | — | version | T1 pre-probe |
| fda_adverse_events | — | version | T1 pre-probe |
| fda_device_recalls | — | version | T1 pre-probe |
| clinical_trials | month | — | T2 period |
| clinical_trial_conditions | month | — | T2 period |
| clinical_trial_interventions | month | — | T2 period |
| cdc_covid_vaccinations | — | last_modified | T1 pre-probe |
| cms_hospital_quality | year,month | — | T2 period |
| medicaid_drug_utilization | year | — | T2 period |
| cdc_mortality | year | — | T2 period |
| cdc_brfss | year | — | T2 period |
| cms_open_payments | year | — | T2 period |
| rxnorm_drugs | — | hash | T3 hash |

### weather

| Table | Period | Freshness | Tier |
|---|---|---|---|
| nws_stations | — | hash | T3 hash |
| nws_alerts | — | hash | T3 hash |
| cdo_stations | — | hash | T3 hash |
| cdo_monthly_summaries | year | — | T2 period |
| cdo_annual_summaries | year | — | T2 period |
| epa_annual_aqi | year | — | T2 period |
| epa_daily_aqi | year | — | T2 period |
| ghcnd_stations_with_county | — | last_modified | T1 pre-probe |
| ghcnd_daily | year,month | — | T2 period |
| drought_monitor_weekly | year | — | T2 period |
| hms_smoke_daily | year,month | — | T2 period |
| hms_smoke_polygons | year,month | — | T2 period |
| climate_normals_monthly | month | — | T2 period |

### geo

| Table | Period | Freshness | Tier |
|---|---|---|---|
| states | year | — | T2 period |
| counties | year | — | T2 period |
| places | year | — | T2 period |
| zctas | year | — | T2 period |
| census_tracts | year | — | T2 period |
| block_groups | year | — | T2 period |
| cbsa | year | — | T2 period |
| congressional_districts | year | — | T2 period |
| school_districts | year | — | T2 period |
| state_legislative_lower | year | — | T2 period |
| state_legislative_upper | year | — | T2 period |
| county_subdivisions | year | — | T2 period |
| tribal_areas | year | — | T2 period |
| urban_areas | year | — | T2 period |
| pumas | year | — | T2 period |
| voting_districts | year | — | T2 period |
| zip_county_crosswalk | year | — | T2 period |
| zip_cbsa_crosswalk | year | — | T2 period |
| tract_zip_crosswalk | year | — | T2 period |
| zip_tract_crosswalk | year | — | T2 period |
| zip_cd_crosswalk | year | — | T2 period |
| county_zip_crosswalk | year | — | T2 period |
| cd_zip_crosswalk | year | — | T2 period |
| rural_urban_continuum | year | — | T2 period |
| ruca_codes | year | — | T2 period |
| gazetteer_counties | year | — | T2 period |
| gazetteer_places | year | — | T2 period |
| gazetteer_zctas | year | — | T2 period |
| watersheds_huc2 | year | — | T2 period |
| watersheds_huc4 | year | — | T2 period |
| watersheds_huc8 | year | — | T2 period |
| watersheds_huc12 | year | — | T2 period |

### lands

| Table | Period | Freshness | Tier |
|---|---|---|---|
| national_forests | — | etag | T1 pre-probe |
| timber_sales | year | — | T2 period |
| forest_inventory | — | last_modified | T1 pre-probe |
| forest_metrics | — | last_modified | T1 pre-probe |
| fia_plots | — | last_modified | T1 pre-probe |
| fia_tree_grm | — | last_modified | T1 pre-probe |
| fia_seedlings | — | last_modified | T1 pre-probe |
| fia_down_woody_debris | — | last_modified | T1 pre-probe |
| fia_invasives | — | last_modified | T1 pre-probe |
| fia_pop_evaluations | — | last_modified | T1 pre-probe |
| nps_units | — | last_modified | T1 pre-probe |
| nps_visitation | year | — | T2 period |
| blm_field_offices | year | — | T2 period |
| onrr_revenues | — | last_modified | T1 pre-probe |

### patents

| Table | Period | Freshness | Tier |
|---|---|---|---|
| patent_grants | year,quarter | — | T2 period |
| patent_assignees | quarter | — | T2 period |
| patent_inventors | quarter | — | T2 period |
| patent_cpc_classes | quarter | — | T2 period |
| patent_abstracts | quarter | — | T2 period |
| patent_applications | quarter | — | T2 period |
| patent_figures | quarter | — | T2 period |
| patent_locations | quarter | — | T2 period |
| patent_claims | year,quarter | — | T2 period |
| patent_summaries | year,quarter | — | T2 period |
| trademark_applications | year | — | T2 period |

### cftc

| Table | Period | Freshness | Tier |
|---|---|---|---|
| cftc_trades | year,month | — | T2 period |

### fedregister

| Table | Period | Freshness | Tier |
|---|---|---|---|
| fr_documents | year,month | — | T2 period |

### ref

| Table | Period | Freshness | Tier |
|---|---|---|---|
| gleif_entities | — | last_modified | T1 pre-probe |
| gleif_cik_mapping | — | last_modified | T1 pre-probe |
| sec_company_tickers | month | — | T2 period |
| figi_instruments | — | hash | T3 hash |

### energy

| Table | Period | Freshness | Tier |
|---|---|---|---|
| eia_electricity_generation | year,month | — | T2 period |
| eia_electricity_prices | year | — | T2 period |
| eia_utility_annual | year | — | T2 period |
| eia_power_plants | year | — | T2 period |
| eia_capacity_changes | year | — | T2 period |
| eia_fossil_fuel_production | year,month | — | T2 period |
| eia_state_energy_consumption | year | last_modified | T1 pre-probe |
| eia_natural_gas_storage | year | last_modified | T1 pre-probe |
| eia_petroleum_stocks | year | — | T2 period |
| eia_crude_oil_imports | year | — | T2 period |
| eia_refinery_operations | year,month | — | T2 period |
| eia_coal_mines | year | — | T2 period |


## Tier-3 action list (hash tables)

Hash tables download on every run; the hash only saves the Iceberg write. **Since the
freshness-token read fix (see Engine note), all of these are now write-idempotent** — the
hash gate correctly skips the write across runs. So the items below are *fetch-bandwidth*
optimizations, **not correctness gaps**, and each needs a non-trivial change:

- **qwi/trade** would need `time` split into `year`+`quarter`/`month` dimensions — that
  re-partitions the table (`time=*` → `year=*/quarter=*`) and changes the API templating, so
  it's a breaking, full-rebuild change, not config. Deferred.
- **Latest-snapshot tables** (fred_series, figi, rxnorm, nws_*, cdo_stations) have no natural
  period; a cadence period would either hit the snapshot+period self-heal trap (the one rule)
  or require a new completion-only `day_of_year`/`workday-of-year` period slot. Deferred to
  that feature.

Recommended follow-ups, best→worst preference (probe > period > bare hash):

| Table | Today | Recommended | Cadence rationale |
|---|---|---|---|
| census/qwi_employment | hash, `time` partition | canonicalize `time`→`quarter` so per-period completion engages | QWI is quarterly |
| census/trade_exports | hash, `time` partition | canonicalize `time`→`month` | Trade totals are monthly |
| census/trade_imports | hash, `time` partition | canonicalize `time`→`month` | Trade totals are monthly |
| econ-reference/fred_series | hash, no period | add daily / workday cadence | FRED catalog refreshes on business days |
| crime/cde_trends | snapshot + hash, no period | keep snapshot+hash, or month cadence (non-snapshot) | Single LATEST trends blob, ~monthly |
| cyber-threat/ioc_mixed | snapshot + hash | day cadence (ThreatFox POST, no HEAD validator) | IOC feed updates daily |
| cyber-vuln/osv_vulnerabilities | **done — `etag`** (per-ecosystem `all.zip` content-MD5 via HEAD, was hash) | GCS returns a content-MD5 ETag on HEAD, so freshness is one HEAD/ecosystem, no download | OSV all.zip published weekly |
| health/rxnorm_drugs | hash, no period | month cadence | RxNorm full release is monthly |
| weather/nws_stations | hash, `state` fetch dim | month cadence | Station metadata changes rarely |
| weather/nws_alerts | hash, `state` fetch dim | keep bare hash (or hourly) | Active alerts change continuously — no useful cadence |
| weather/cdo_stations | hash, `state` fetch dim | month cadence | Station reference changes rarely |
| ref/figi_instruments | hash, no period | month / workday cadence | OpenFIGI reference, changes rarely |
| edu/ccd_schools | year + releaseWindow (hash REMOVED) | done — hash was redundant (year completion + release window already gate it); it also OOMed before the streaming-hash fix | Annual NCES release (Jul/Aug) |
| edu/ipeds_institutions | year + releaseWindow (hash REMOVED) | done — redundant hash removed (year period gates it) | Annual IPEDS release (Nov) |

## Resolved: cyber-threat/threat_pulses (version watermark + mode-keyed write)

Previously `freshness: {type: hash}` + a fixed `otxDeltaDays` (`CYBER_OTX_DELTA_DAYS`)
`modified_since` window. The hash still **downloaded** the whole subscribed population (~8.6k
pulses, ~31-min full re-pagination) every run just to gate the write, and a fixed-day window
risked gaps (miss a run > N days) and could not advance from the actual last-run point.

The OTX subscribed feed turns out to be ordered **modified-desc**, so `?limit=1` →
`results[0].modified` is the global max-modified in **one page** — a cheap T1 probe, the REST
analogue of `vuln_cross_refs`'s GraphQL watermark. Resolved by:

- **`freshness: {type: version}`** — `probe_url: …/subscribed?limit=1`,
  `version_field: results[0].modified`. The token is the max pulse `modified`; the write is
  skipped when it is unchanged (no new pulses), and the probe is one page, not a full download.
- **`watermark_var: otxModifiedSince`** — the engine recovers the prior token and injects it so
  `OtxResponseTransformer` bounds the fetch with `modified_since=<last max modified>` (exactly
  "since the last committed run", self-adjusting — no fixed window, no gap). Cold/first run has
  no token → full load, which seeds the watermark.
- **Mode-keyed write** (`appendWhenOperand: cyber_threat.otxWriteMode`, `appendWhenValue:
  append`). The launch script sets `otxWriteMode` = **append** only for production daily; it is
  **replace** for historical (full canonical snapshot via replace-partitions), DQ sample runs
  (which full-load for row-count/variety), and any run that omits it (safe default). The same
  operand gates the transformer's full-vs-delta fetch, so fetch and write never disagree:
  - *production daily*: delta fetch → **append**, accumulating pulse version history in each
    `first_seen` partition (query current via `max(modified)` per `pulse_id`).
  - *historical / DQ / cold*: full fetch → **replace-partitions** (idempotent snapshot).

  Per-cycle the pool runs daily *then* historical, so during backfill the daily append is just
  replaced by the historical snapshot (harmless); once historical is retired, daily-only runs
  accumulate history forever.
- **`otxCacheTtlDays`** (`CYBER_OTX_CACHE_TTL_DAYS`, default **0 = off**) remains a rarely-used
  opt-in pull-cache escape hatch — no blind always-on TTL.

## Engine note: in-process httpfs reads → SDK local-download

The in-process (bundled) DuckDB `httpfs` extension **cannot LIST custom S3 endpoints**
(MinIO/R2) — a direct `read_parquet('s3://…/*.parquet')` silently returns nothing. The markers
are authoritative in S3 (written via the AWS SDK); the bug was purely on the read side. It hit
**three** tracker reads, all now routed through the AWS SDK list + transient local-download +
local read (the same path `getCompletedTables` always used; the temp copy is per-read scratch,
deleted immediately — never authoritative):

1. **Freshness/watermark tokens** (`readLatestState`/`getFreshnessToken`) returned null every
   call → Tier-1 probes and Tier-3 hash gates never skipped across runs. Fixed.
2. **Table-completion markers** (`preloadAllCompletions`/`getCachedCompletion` for
   `_table_complete`) returned null → every table hit "cold-start (no marker but Iceberg data
   exists)". Fixed, and made **fail-loud**. *(Note: cold-start alone did NOT cause re-writes —
   a cold-started table still falls through to per-period/per-combo filtering; ACS cold-starts
   yet skips. The real re-write cause was #3.)*
3. **Per-combo "processed" keys** (`loadProcessedKeysForTable`, behind `filterUnprocessed`)
   returned null → `"No tracker data found for X — all N combinations unprocessed"` → the table
   re-fetched and **re-materialised its entire closed history every resume**. Fixed, fail-loud.
   **Validated on econ:** a 2nd resume now logs `"Tracker scan for regional_income: 2074 processed
   keys"` and econ historical writes fell **3103 → 382** (the 1.6M-row `regional_income` skips).

ACS tables were always idempotent because per-period completion (read #1) drops their combos
*before* read #3 runs; the re-writers fell through to the broken #3.

**Known residual (separate cause):** some tables with a `dataLag` year offset — `census`
`lodes_workplace`/`cbp_establishments` (~3.4M rows), and similar with non-canonical keys
`qwi`/`trade` (`time=`) — still re-write. With read #3 fixed the per-combo read now *engages*
(`"Tracker scan for cbp_establishments: 0 processed keys from 191 files"` — it reads the files,
vs "No tracker data found" before), but finds **0 markers** because those combos are never
*written* as complete: the dimension year (from the `{year}` template) differs from the actual
data/partition year (`getPartitionYear` fiscal heuristic), so the marker key and the Iceberg
partition never line up. This is a distinct framework fix (reconcile dimension-year vs data-year
for `dataLag` tables; recognize `time=YYYY-Qn`/`YYYY-MM` as canonical periods) — pending.

**FIA archives:** `FiaStateArchive` now gates its per-state archive download on a content-based
**ETag** (preferred) falling back to `Last-Modified`, so a server re-stamping the timestamp
without a content change no longer forces a multi-GB re-download. (The per-state combo is also
gated by #3, which skips the materialization that would trigger the download.)

## Engine note: compaction never destroys value-bearing markers

Tracker compaction folds individual markers into `year=<y>/_compacted/` with a hard-coded
`state='complete'` — correct for presence markers, but it would overwrite a **freshness/watermark
token** whose meaning lives in the `state` column (ETag/Last-Modified/hash/HWM). Preservation is
now an **invariant of two chokepoints**, not a filter each scan caller must remember:
`compactFromCache` skips any `_freshness` source key on write, and `deleteSpecificFiles` never
deletes a token's individual file. So however a scan populates `stageCache`, a token is never
folded lossily and never deleted — it stays an individual file for `readLatestStateAndAsOf`.

## Engine note: per-table Iceberg commit serialization

The Hadoop/filesystem Iceberg catalog tracks the live table via a non-atomic `version-hint.text`
pointer and is unsafe under concurrent commits. On an **actively-overwritten partition** (e.g.
`cyber-vuln/vulnerabilities` current quarter, rewritten every run via `overwritePartitions`), a
`replace` commit racing the compaction's `expireSnapshots`/orphan-cleanup left the live snapshot
referencing **physically-deleted** data files — a dangling ref that 404s on every read
(`total-data-files` > files present). Fixed: **all** table-mutating commits (append,
replace-partitions, delete, expire-snapshots, compaction rewrite) now run under a JVM-wide lock
keyed on `Table.location()`, so no two commits to the same table ever overlap. Cross-process
writers are already sequential (daily then historical), so a per-process lock fully serializes.
*(A table already corrupted this way does not self-heal — it must be force-reprocessed.)*

## Engine note: sec 8-K earnings completeness (per-filing idempotence)

SEC filings are gated per-accession by `SecFilingCache.checkFiling` → tracker
`getCompletedTables(filingKey, "staging")` → `FileInventory.isComplete(form)`, which requires
every output in `FormType.getExpectedOutputs`. `FORM_8K` expects `{METADATA, EARNINGS, CHUNKS}`,
but most 8-Ks carry no earnings release, so `XbrlToParquetConverter` wrote `_earnings.parquet`
only `if (!earningsRecords.isEmpty())`. With no earnings file, `hasEarnings=false`,
`isComplete(8-K)` never held, and **every current-year 8-K re-processed on every run** — which
also pushed `sec_secondary` past its 60-minute idle limit, so the pool's stuck-worker detector
killed + re-queued it in a restart loop. Fixed: the 8-K now **always** writes the earnings output
(rows when present, an empty schema-only file otherwise), so the `earnings` marker records and the
filing is idempotent. Historical years were unaffected (gated earlier by period-level completion).

## Derived tables (no fetch)

98 tables are views / derived joins computed from the fetch tables above; they declare
no source, so freshness does not apply. Per-schema fetch vs. derived counts:

| Schema | Fetch | Derived |
|---|---|---|
| census | 39 | 5 |
| econ | 29 | 17 |
| econ-reference | 5 | 2 |
| fec | 12 | 2 |
| crime | 16 | 1 |
| cyber-threat | 11 | 1 |
| cyber-vuln | 8 | 6 |
| edu | 11 | 12 |
| health | 15 | 14 |
| weather | 13 | 1 |
| geo | 32 | 0 |
| lands | 14 | 7 |
| patents | 11 | 0 |
| sec | 0 | 16 |
| cftc | 1 | 7 |
| fedregister | 1 | 0 |
| ref | 4 | 1 |
| energy | 12 | 6 |
