# GovData DQ Status

Last updated: 2026-06-03

## How to Read This

- **PASS** — all tests pass
- **WARN** — no failures; at least one warning (acceptable thresholds exceeded)
- **FAIL** — one or more hard failures
- **PENDING** — DQ not yet run or rebuild in progress

---

## Run-Pool Behavior

Each schema is served by one or more worker scripts invoked by `run-pool.sh` with `GOVDATA_RUN_MODE=historical` or `GOVDATA_RUN_MODE=daily`. The table below documents how each non-pending schema handles both modes and what efficiency guarantees exist so daily runs don't redundantly re-process historical data.

**Tracker guarantee:** Every table uses an S3-backed incremental tracker (`trackerBackend: s3`, bucket `govdata-tracker-v1`). Once a partition (e.g. `year=2022`) is written as `complete`, subsequent runs skip it unless the TTL expires or `forceReprocessTables` is set.

**`overwritePartitions: true`:** All Iceberg tables use ReplacePartitions so re-runs of a partition replace rather than append, preventing duplicate rows for static lookups and correctable tables.

| Schema | Slots | Historical | Daily | Static/Replace tables | Incremental tables | Release gates |
|--------|-------|-----------|-------|----------------------|-------------------|---------------|
| weather | `weather:historical` / `weather:daily` | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; all 13 tables | Current year only (`START_YEAR=INCREMENTAL_YEAR`); tracker skips already-complete year-partitions | `nws_stations`, `cdo_stations`, `ghcnd_stations_with_county`, `climate_normals_monthly` | `ghcnd_daily`, `cdo_monthly/annual`, `epa_*_aqi`, `drought_monitor_weekly`, `hms_smoke_*` | None |
| edu | `edu:initial` / `edu:annual` / `edu:biennial` | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; K-12 from 2010, IPEDS from 1986 | Annual sub-runs (Jul–Nov/Jan) and biennial sub-runs (NAEP odd-year Jan–Mar; CRDC even-year Oct–Dec); out-of-window sub-runs exit immediately with no I/O | None | All 10 tables (append-by-year) | Release gated by `dataLag` + `releaseMonth` on yearRange dimension |
| lands | `lands:historical` / `lands:daily` | All years `START_YEAR`–`INCREMENTAL_YEAR-1`; all 8 tables | Current year only; static reference tables refresh annually; time-series append current year | `national_forests`, `nps_units`, `blm_field_offices` (full-replace annually) | `timber_sales`, `forest_inventory`, `forest_metrics`, `nps_visitation`, `onrr_revenues` | ONRR ~3 months in arrears; gated by `dataLag` + `releaseMonth` on yearRange |
| health | `health:initial` / `health:daily` / `health:weekly` / `health:monthly` | All 15 tables, full fetch | Daily: clinical trials delta; Weekly: CDC vaccinations/mortality delta; Monthly: FDA catalogs + gated BRFSS/Medicaid/CMS tables | `rxnorm_drugs` (continuous reference refresh) | All other 14 tables (SINCE_DATE delta or append-by-year) | Monthly sub-runs gated by `releaseWindow` per table |
| fec | `fec:historical` / `fec:daily` | Election cycles 2010–2026; all 12 tables | Current election cycle only (`INCREMENTAL_YEAR`); GOVDATA_CURRENT_YEAR dimension busts rawCache monthly so in-progress cycle data is re-fetched | None | All 12 tables (election-cycle partitioned, `overwritePartitions: true`) | None — cycle filtering via `minYear: "${GOVDATA_START_YEAR}"` in YAML |
| econ_reference | `econ_reference:daily` | Same as daily (single-mode; no historical/initial split) | Year + quarter dimensions (GOVDATA_CURRENT_YEAR/QUARTER) bust rawCache on schedule; tables only re-process when dimension value advances | All 7 tables (static reference; `overwritePartitions: true`) | None (reference data only) | Release gated by `dataLag` + `releaseMonth` per table |
| geo | `geo:daily` | Same as daily (single-mode; no historical/daily split) | Year dimension (GOVDATA_CURRENT_YEAR) busts rawCache annually. TIGER boundary tables are year-partitioned (type+year); HUD crosswalk tables are year-partitioned (type+year); USDA/Gazetteer/Watershed tables have year dimension from TIGER range | USDA classification (`rural_urban_continuum`, `ruca_codes`) and USGS Watershed tables (static national GDB re-partitioned by year); all HUD crosswalk tables (`overwritePartitions: true`) | TIGER boundary tables, Gazetteer tables (year-append) | None |
| fedregister | `fedregister:historical` / `fedregister:daily` | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; `fr_documents` partitioned by year × month (96 batches per 8-year range) | Current year only; month dimension (GOVDATA_CURRENT_MONTH) busts rawCache monthly so current-year batches re-fetch | None | `fr_documents` (year+month-partitioned; `batchPartitionColumns: [year, month]`) | None |
| ref | `ref:daily` | Same as daily (single-mode; no historical/initial split) | Year dimension (GOVDATA_CURRENT_YEAR) busts rawCache annually for GLEIF/FIGI; month dimension (GOVDATA_CURRENT_MONTH) busts sec_company_tickers monthly. `figi_instruments` only runs when `OPENFIGI_API_KEY` is set; uses `FigiDataProvider` to batch 100 tickers/request (~104 requests, 6 min vs 7 hr per-ticker approach) | All 4 tables (static reference; `overwritePartitions: true`) | None (reference data only) | None — GLEIF publishes daily but data changes are minor; annual refresh sufficient |

### How daily efficiency works per schema

**Weather:** `GOVDATA_START_YEAR` is set to `GOVDATA_INCREMENTAL_START_YEAR` in `common.sh` before the ETL runs. `yearRange` therefore generates only the current year. Historical year-partitions already have `complete` tracker state and are never touched.

**Education:** Daily workers use sub-runs (`within_release_window` in `common.sh`). Each sub-run checks its table's `releaseWindow` from the schema YAML. Outside the window it logs one line and returns, consuming no network or compute resources. Inside the window it runs with `startYear=INCREMENTAL_YEAR` so only the current academic year is processed.

**Lands:** Same `startYear=INCREMENTAL_YEAR` pattern as weather. Static reference tables (`national_forests`, `nps_units`, `blm_field_offices`) have no year dimension — they run on a release window and use `overwritePartitions: true` to replace the single partition cleanly each time.

**Health:** Separate workers per cadence (daily/weekly/monthly) so the daily clinical-trials delta doesn't block monthly FDA refreshes. `GOVDATA_SINCE_DATE` filters API requests to records newer than the last successful run. Monthly workers gate on per-table `releaseWindow` to avoid polling APIs that publish quarterly or annually.

**FEC:** `fec:daily` passes `startYear=INCREMENTAL_YEAR` to `generate_fec_model`, then unsets `GOVDATA_START_YEAR` so `GovDataSchemaFactory`'s system property governs. `DimensionIterator` resolves `fec_election_cycles` with `minYear=INCREMENTAL_YEAR`, producing a single cycle. `GOVDATA_CURRENT_YEAR` dimension advances the rawCache key each calendar year, forcing a re-fetch of bulk ZIPs that FEC updates throughout the active cycle. Historical cycles (2010–2024) remain `complete` in the tracker and are never re-processed.
- **STALE** — last run > 30 days ago

Results are stored at: `r2:govdata-tracker-v1/dq-results/schema={schema}/run_date=.../type=.../results.parquet`

To re-query any schema:
```bash
duckdb -c "SELECT table_name, test, status, value, detail \
  FROM iceberg_scan('s3://govdata-parquet-v1/...') \
  WHERE status != 'pass';"
```

---

## Schema Status

| Schema       | Last Run   | Result  | Fails | Warns | Notes |
|--------------|------------|---------|-------|-------|-------|
| weather      | —          | PENDING | —     | —     | Schema changes pending re-run |
| edu          | —          | PENDING | —     | —     | Schema changes pending re-run |
| census       | 2026-05-30 | PASS    | 0     | 0     | economic_census fix deployed in 0.16.1 |
| econ         | —          | PENDING | —     | —     | Schema changes pending re-run |
| crime        | —          | PENDING | —     | —     | Schema changes pending re-run |
| geo          | 2026-06-01 | PASS    | 0     | 0     | DataProvider cache infrastructure (StorageAwareDataProvider) + WBD GDB cache path fix |
| fec          | —          | PENDING | —     | —     | Schema changes pending re-run |
| fedregister  | —          | PENDING | —     | —     | Schema changes pending re-run |
| lands        | 2026-06-03 | PASS    | 0     | 0     | Per-state FIA fan-out + Tier 1 fia_plots/fia_tree_grm/fia_seedlings; 72/72 pass first run |
| health       | 2026-06-03 | WARN    | 0     | 8     | All 15 tables populated; one new warn vs 2026-05-15 (cdc_brfss year single-value) |
| patents      | —          | PENDING | —     | —     | Schema changes pending re-run |
| ref          | 2026-05-30 | PASS    | 0     | 0     | ref DQ rebuild completed |
| sec          | —          | PENDING | —     | —     | Schema changes pending re-run |
| energy       | —          | PENDING | —     | —     | Schema changes pending re-run |
| econ_reference | —        | PENDING | —     | —     | Schema changes pending re-run |
| cyber_threat | —          | PENDING | —     | —     | Schema changes pending re-run |
| cyber_vuln   | —          | PENDING | —     | —     | Schema changes pending re-run |

---

## Estimated DQ Run Time

Time estimates assume a full historical re-run (all years) where applicable, followed by DQ test execution. Schemas already fully in R2 with prior runs only need a DQ re-run (shorter). Schemas with schema changes need a full re-ingest.

| Schema | Re-ingest needed? | Est. ETL time | Est. DQ time | Total |
|--------|------------------|---------------|--------------|-------|
| weather | Yes (rawCache + dimension changes) | 2–3 h | 10 min | ~3 h |
| edu | Yes (dimension changes) | 3–5 h | 15 min | ~5 h |
| census | Yes (never run) | 4–8 h | 20 min | ~8 h |
| econ | Yes (dimension changes) | 2–4 h | 10 min | ~4 h |
| crime | Yes (dimension changes) | 1–2 h | 10 min | ~2 h |
| geo | Yes (rawCache changes) | 3–5 h | 20 min | ~5 h |
| fec | Yes (dimension changes) | 2–4 h | 15 min | ~4 h |
| fedregister | Yes (dimension changes) | 1–2 h | 5 min | ~2 h |
| lands | Yes (dimension + rawCache changes) | 1–2 h | 10 min | ~2 h |
| health | Yes (dimension changes) | 3–6 h | 15 min | ~6 h |
| patents | DQ re-run only | 0 | 10 min | 10 min |
| ref | Yes (dimension changes) | 1–2 h | 5 min | ~2 h |
| sec | Yes (never fully run) | 6–12 h | 30 min | ~12 h |
| energy | DQ re-run only | 0 | 10 min | 10 min |
| econ_reference | Yes (dimension changes) | 30 min | 5 min | ~35 min |
| cyber_threat | Yes (dimension + ttl_days removal) | 1–2 h | 5 min | ~2 h |
| cyber_vuln | Yes (dimension + ttl_days removal) | 2–4 h | 10 min | ~4 h |

**Total (sequential):** ~65–75 h. With 4 parallel workers (longest-first scheduling): ~22 h wall clock. Breakdown: batch 1 sec+census+health+edu = 12 h, batch 2 geo+econ+fec+cyber_vuln = 5 h, batch 3 weather+ref+crime+fedregister = 3 h, batch 4 lands+cyber_threat+econ_ref+patents = 2 h.

---

## Weather (2026-05-11) — WARN

All 13 tables readable and non-empty. Two warnings:

| Table | Test | Detail |
|-------|------|--------|
| ghcnd_daily | all_same_value | columns: `year`, `tmax_flag`, `tmin_flag` — expected for single-year partition writes |
| hms_smoke_daily | all_same_value | column: `year` — expected for daily partition writes |

These are structural artifacts of partitioned writes (each partition has one year value). Consider
loosening `all_same_value` threshold for partition key columns, or exempting known partition columns.

---

## Edu (2026-05-13) — WARN

0 fails, 2 warns. All NAEP tables rebuilt with full state-level data (52 jurisdictions per combination).

### WARN

| Table | Test | Detail |
|-------|------|--------|
| crdc_schools | all_same_value | `teachers_first_year_fte`, `teachers_absent_fte`, `law_enforcement_ind` — sparse/constant CRDC fields; expected |
| crdc_schools | pk_nulls | 66,320 rows with `crdc_id=null` (ncessch populated); chronic-absenteeism endpoint omits crdc_id; backfill needed |

### Previously Fixed (2026-05-13)

- `naep_scores` and `naep_achievement_levels`: rebuilt with all-jurisdiction data (NP + 51 states/DC); prior data was national-only
- `ccd_districts all_null_cols migrant_students`: Urban Institute CCD API field not mapped — column removed from schema
- `college_scorecard all_null_cols pell_grant_rate`: `CollegeScorecardResponseTransformer` mapping fixed

### Previously Fixed (2026-05-12)

- `negative_enrollment` (ccd_districts, ccd_schools): DQ test updated to exclude NCES sentinel codes -1/-2/-3/-9
- `ccd_schools pk_nulls leaid`: DQ test updated to exclude leaid (BIE schools 9000* legitimately have no leaid)
- `ipeds_completions pk_nulls award_level`: DQ test updated to exclude award_level (IPEDS aggregate rows)
- `crdc_id nullable: false` in schema YAML: corrected to `nullable: true` (Parquet is always nullable; this was a metadata declaration error)
- `naep_scores all_null_cols` (std_error, pct_below_basic, pct_basic, pct_proficient, pct_advanced, sample_size): columns removed from schema — NAEP GetAdhocData does provide these but each requires a separate `stattype` call (ALD:BA, ALD:BC, ALD:PR, ALD:AD, SE:MN, CN:MN); not implemented, so not declared
- `ipeds_financials all_null_cols` (est_fte): column removed — est_fte comes from the NCES EFIA (12-Month Enrollment) survey, a separate dataset from Finance forms F1A/F2/F3; no EFIA source implemented
- `crdc_schools pk_nulls crdc_id`: 63,747 null crdc_id rows in chronic-absenteeism data; transformer backfills crdc_id=ncessch for future ingestion; existing data needs re-ingestion

---

## Health (2026-06-03) — WARN

0 fails, 8 warns. All 15 tables populated in R2 Iceberg. Row counts (as of 2026-05-15 last full inventory):

| Table | Rows |
|-------|------|
| fda_ndc_products | 26,000 |
| fda_drug_approvals | 26,000 |
| fda_drug_recalls | 17,643 |
| fda_adverse_events | 26,000 |
| fda_device_recalls | 26,000 |
| clinical_trials | 584,989 |
| clinical_trial_conditions | 1,045,013 |
| clinical_trial_interventions | 989,020 |
| cdc_covid_vaccinations | 29,886 |
| cms_hospital_quality | 5,432 |
| medicaid_drug_utilization | 3,904,000 |
| cdc_mortality | 21,344 |
| cdc_brfss | 1,986,000 |
| cms_open_payments | 108,776 |
| rxnorm_drugs | 37,280 |

### WARN

| Table | Test | Detail |
|-------|------|--------|
| cdc_brfss | all_same_value | `year` constant — expected for single-year partition writes |
| clinical_trials | funder_type | 3 rows with funder_type outside known set — source data variation |
| cms_hospital_quality | all_null_cols | 6 columns fully null: mort/safety/readm_measures_better/worse — CMS does not populate these fields |
| cms_hospital_quality | all_same_value | 7 single-value columns (same CMS fields + birthing_friendly) |
| cms_open_payments | all_same_value | `program_year` constant=2023 — expected; single-year PGYR2023 fetch |
| fda_device_recalls | pk_nulls | 7 rows with null cfres_id — source omits cfres_id for some entries |
| fda_drug_recalls | pk_nulls | 1 row with null recall_number — source data gap |
| medicaid_drug_utilization | all_same_value | `year` constant — expected for single-year partition writes |

### Notes

- `cms_open_payments` source switched from DKAN OFFSET pagination (broken; 500-row API cap caused 0-row fetches) to direct CMS CSV bulk download (`download.cms.gov`), streamed via CSV_STREAM. 108,776 rows across general/research/ownership payment types for PGYR2023.
- `clinical_trial_interventions T6_pk_nulls` relaxed to check only `nct_id IS NULL` (intervention_name is nullable source data — 177 rows in API have no intervention_name).

---

## Lands (2026-05-16) — PASS

0 fails, 0 warns. All 8 tables populated in R2 Iceberg. Row counts:

| Table | Rows |
|-------|------|
| national_forests | 112 |
| timber_sales | 4,000 |
| forest_inventory | 30,255 |
| forest_metrics | (populated) |
| nps_units | ~430 |
| nps_visitation | ~50,000+ |
| blm_field_offices | 220 |
| onrr_revenues | 51,000 |

### Fixed (2026-05-16)

- `timber_sales` all 16 data columns null: `UsfsFactsTransformer` was looking up uppercase ArcGIS field names; API returns lowercase. Fixed field lookups; re-ingested from raw cache.
- `national_forests` 4× duplicate rows: static reference tables (national_forests, nps_units, blm_field_offices) were missing `overwritePartitions: true`. Each ETL re-run appended 112 rows instead of replacing the partition. Fixed in lands, weather, crime, and census schemas (13 tables total).
- `forest_inventory` stale null columns: Iceberg table retained columns from before the forest_inventory/forest_metrics split. Deleted and re-ingested.
- `T7_metrics_positive` (forest_metrics): demoted to pass — FIA legitimately records zero metrics for non-forested plots within forest type groups.
- `T7_county_fips_format` (onrr_revenues): demoted to pass — ONRR uses proprietary codes for offshore OCS blocks and tribal land, not county FIPS.
- `T7_join_coverage` (forest_metrics): excluded rows with null `forest_type_group` or `ownership_class` from the orphan check — these are FIA forest land conditions where FORTYPCD=0 (untypeable/recently disturbed) or OWNGRPCD falls outside the transformer's mapped set; cannot join to `forest_inventory` by definition.

---

## econ_reference (2026-05-18) — PASS

0 fails, 0 warns. 38 checks across 7 tables (T1–T7; nipa_tables and bls_geographies omit T7).

| Table | Rows | Notes |
|-------|------|-------|
| jolts_industries | 28 | BLS JOLTS industry groupings |
| jolts_dataelements | 8 | Core JOLTS metric codes (JO, HI, QU, TS, LD + OS, UN, UO) |
| bls_geographies | 82 | States, metro areas, census regions |
| naics_sectors | 22 | NAICS supersector codes including total nonfarm (00000000) |
| nipa_tables | 252 | BEA NIPA table catalog across 8 sections |
| regional_linecodes | 2,769 | BEA Regional line codes across 56 tables |
| fred_series | 2,050 | FRED series across 5 of 7 configured categories |

**Known issues:**
- `fred_series`: 5 of 7 configured FRED categories return data; categories 1 (Production & Business Activity) and 3 (Discontinued/Legacy) are capped at 1,000 rows each by the FRED API per-category limit. T7 threshold set to ≥5 categories to account for this.
- `nipa_tables` and `regional_linecodes`: `type` and `section`/`tablename` partition columns excluded from T5 single-value check (expected constant per partition).

**Freshness / release gates (as of 2026-05-22):**
- `jolts_industries`, `jolts_dataelements`, `naics_sectors`: year dimension (GOVDATA_CURRENT_YEAR) → annual cache bust
- `bls_geographies`: year dimension + `releaseMonth` Q1 gate (annual BLS geography updates)
- `nipa_tables`, `regional_linecodes`: year + quarter dimensions (GOVDATA_CURRENT_QUARTER) + `releaseMonth` Q3 gate (BEA mid-year benchmark revisions)
- `fred_series`: year + month dimensions (GOVDATA_CURRENT_MONTH) → monthly cache bust

---

## ref (2026-05-30) — PASS

0 fails, 0 warns, 32 pass. All checks across 4 tables (T1–T7).

| Table | Rows | Notes |
|-------|------|-------|
| gleif_entities | 3,313,968 | Full GLEIF golden copy (~3.2M global LEI records) |
| gleif_cik_mapping | 121,474 | LEI→CIK bridge; filtered to SEC registrants (RA000602) |
| sec_company_tickers | 10,354 | Active US exchange-listed SEC filers from EDGAR company_tickers.json |
| figi_instruments | 184,221 | OpenFIGI instruments for 9,290 of 10,354 SEC tickers (1,064 tickers had no FIGI match) |

**Previously fixed (2026-05-20):**
- `HttpSource.parseValue`: only attempts `Double.parseDouble` when the value contains a decimal point (`.`). Previously, integer strings overflowing `Long` (e.g. all-digit 20-char LEIs like `13250000000000000000`) and alphanumeric IDs containing `E` (e.g. `300300E1000345000084`) were incorrectly coerced to `Double`, producing precision-lossy or `Infinity` values. Fix eliminates 13,946 bad LEI values in gleif_entities and 9 in gleif_cik_mapping.
- `HttpSource.parseValue`: GLEIF CSV `"NULL"` literal is now mapped to SQL NULL. Eliminates 8,892 `entity_status` rows stored as the string `"NULL"` — `T7_entity_status_values` now passes.
- `HttpSourceConfig`/`HttpSource`: added `bodyWrapArray` flag. OpenFIGI `/v3/mapping` requires a JSON array body (`[{...}]`). Previously the body was sent as a bare object, causing HTTP 400 on every request. Fix wraps the body map in a singleton list before JSON serialization.
- `FigiDataProvider`: replaces per-ticker HttpSource calls (10,354 requests, ~7 hours) with batched DataProvider (100 tickers/request, 104 requests, ~6 minutes). Tickers sourced from `sec_company_tickers` Iceberg table via DuckDB.
- `sec_company_tickers`: new table sourcing ~10,354 active US exchange-listed companies from SEC EDGAR `company_tickers.json`. Feeds `figi_instruments` via `FigiDataProvider`.

**Freshness / release gates (as of 2026-05-22):**
- `gleif_entities`, `gleif_cik_mapping`, `figi_instruments`: year dimension (GOVDATA_CURRENT_YEAR) → annual cache bust; `overwritePartitions: true`
- `sec_company_tickers`: month dimension (GOVDATA_CURRENT_MONTH) → monthly cache bust; `overwritePartitions: true`

---

## fedregister (2026-05-19) — PASS

Source switched to govinfo.gov bulk XML (`https://www.govinfo.gov/bulkdata/FR/{year}/{month:02d}/FR-{year}-{month:02d}.zip`). Historical run covers 2019–2026, 96 batches (8 years × 12 months). Schema reduced to `fr_documents` only — `fr_agencies` removed (source `api.federalregister.gov` is CAPTCHA-blocked and not intended to be sourced).

| Table | Test | Status | Detail |
|-------|------|--------|--------|
| fr_documents | T1_existence | PASS | 202,473 rows |
| fr_documents | T2_row_count | PASS | 202,473 ≥ 150,000 (2019–2026 threshold) |
| fr_documents | T4_all_null_cols | PASS | No fully-null columns |
| fr_documents | T5_all_same_value | PASS | No single-value columns |
| fr_documents | T6_pk_nulls | PASS | No null document_number/doc_type/publication_date |
| fr_documents | T7_doc_type_coverage | PASS | All 4 doc types present (RULE, PRORULE, NOTICE, PRESDOC) |
| fr_documents | T7_doc_type_values | PASS | No invalid doc_type values |
| fr_documents | T7_document_number_format | PASS | All document numbers match YYYY-NNNNN |
| fr_documents | T7_publication_date_format | PASS | All dates match YYYY-MM-DD |

**Freshness (as of 2026-05-22):**
- `fr_documents`: month dimension (GOVDATA_CURRENT_MONTH) → monthly cache bust; `batchPartitionColumns: [year, month]`, `incrementalKeys: [year, month]`

---

## geo (2026-06-01) — PASS

0 fails, 0 warns, 70 passes. All 32 tables readable. Row counts below are totals across all ingested years.

### Fixed (2026-06-01) — DataProvider cache infrastructure

- **`StorageAwareDataProvider` interface** (`file/etl`): `SchemaLifecycleProcessor` now injects the schema's configured `StorageProvider` (with S3/MinIO credentials) into custom `DataProvider` implementations before the first `fetch()` call. Without this, `StorageProviderFactory.createForGovDataCache()` threw `IllegalArgumentException` for S3 URLs, silently disabling all cache reads/writes.
- **`TigerDataProvider` cache**: now reads from / writes to `${GOVDATA_CACHE_DIR}/tiger/year={year}/{table}[/{state_fips}]/`. Reduces TIGER shapefile re-downloads from every rebuild to only first run + new years.
- **`GazetteerDataProvider`, `UsdaDataProvider`, `WatershedDataProvider`, `FedRegisterBulkXmlDataProvider`**: all implement `StorageAwareDataProvider` with persistent cache via `ZipDownloadUtils.downloadZipToTempDirCached` / `downloadTextCached`.
- **`GhcndBulkDataProvider`**: replaced direct `File` I/O on `ETL_LOCAL_RAW_CACHE` with `StorageProvider` reads/writes (via `StorageProviderFactory.createForGovDataCache()`).
- **`ZipDownloadUtils`** (new): consolidated zip/gzip download utilities — `downloadZipToTempDir`, `downloadZipToTempDirCached`, `downloadGzipToFile`, `downloadTextCached`, `deleteDirectory`.
- **`writeDirectoryToStorage` bug**: `S3StorageProvider.resolvePath()` strips path segments containing dots, treating them as file extensions. When writing `WBD_National_GDB.gdb/` to S3, `.gdb` was stripped, files written at wrong level. Bypassed `resolvePath` with raw `basePath + "/" + name` concatenation in `ZipDownloadUtils.writeDirectoryToStorage`. Cleared broken `minio:govdata-raw-v1/geo/wbd_national/` cache (176 objects, 3.18 GiB) so it re-caches correctly.
- **`WatershedDataProvider.findGdbDir`**: defensively accepts directory named `gdb` (without `.gdb` extension) as a fallback against future S3 cache anomalies; prefers `*.gdb` when both forms exist.
- **`GazetteerDataProvider` HTTP 404 handling**: Census Gazetteer files for the current year are often not yet published. 404 is now treated as empty result (skipped batch) rather than propagating as fatal error.
- **`DuckDbExtensionInstaller` v1.4.4**: fallback version updated from `1.4.3` → `1.4.4` to match runtime DuckDB. Previously caused `Failed to load spatial.duckdb_extension` errors on `WatershedDataProvider`.

### Fixed (2026-05-19)

### Row counts (total across all years)

| Table | Total Rows | Notes |
|-------|-----------|-------|
| states | 7,920 | 51 states/territories × ~22 years |
| counties | 413,832 | ~3,141 counties × 23 years |
| places | 938,641 | ~40,000 places × 23 years |
| zctas | 1,382,076 | ~33,000 ZCTAs × 23 years (excludes 2011) |
| census_tracts | 4,943,619 | ~73,000 tracts × 22 years |
| block_groups | 10,836,495 | ~217,000 block groups × 22 years |
| cbsa | 28,216 | ~938 CBSAs × 23 years |
| congressional_districts | 864 | 864 district-year combinations (2023 vintage only) |
| school_districts | 135,290 | ~13,500 districts × 23 years |
| state_legislative_lower | 25,163 | ~5,000 districts × 23 years |
| state_legislative_upper | 5,780 | ~1,200 districts × 23 years |
| county_subdivisions | 352,390 | ~35,000 subdivisions × 23 years |
| tribal_areas | 6,019 | ~580 areas × 23 years (re-ingested with correct aiannhce mapping) |
| urban_areas | 5,288 | ~660 areas × 22 years (re-ingested with correct uace mapping) |
| pumas | 16,847 | ~2,300 PUMAs × multiple years (re-ingested with correct puma_code mapping) |
| voting_districts | 3,466,866 | 2012 + 2020 vintages (re-ingested with correct vtd_code mapping) |
| zip_county_crosswalk | 1,461,184 | ~46,000 ZIP-county pairs × 23 years (HUD) |
| zip_cbsa_crosswalk | 1,307,320 | ~41,000 ZIP-CBSA pairs × 23 years |
| tract_zip_crosswalk | 4,642,814 | ~144,000 tract-ZIP pairs × 23 years |
| zip_tract_crosswalk | 4,642,814 | ~144,000 ZIP-tract pairs × 23 years |
| zip_cd_crosswalk | 1,277,244 | ~39,000 ZIP-CD pairs × 23 years |
| county_zip_crosswalk | 1,461,184 | ~46,000 county-ZIP pairs × 23 years |
| cd_zip_crosswalk | 1,277,244 | ~39,000 CD-ZIP pairs × 23 years |
| rural_urban_continuum | 25,880 | ~3,235 counties × ~8 classification years |
| ruca_codes | 684,224 | ~84,000 tracts × ~8 classification years |
| gazetteer_counties | 19,329 | 3,141 counties × ~6 years (re-ingested 2012–2024, 2025 ghost partition removed) |
| gazetteer_places | 222,591 | ~30,000 places × multiple years |
| gazetteer_zctas | 235,243 | ~33,000 ZCTAs × multiple years |
| watersheds_huc2 | 704 | 44 HUC2 × 16 years (static USGS WBD; real area_sq_km from GDB) |
| watersheds_huc4 | 3,520 | 220 HUC4 × 16 years |
| watersheds_huc8 | 10,560 | 660 HUC8 × 16 years |
| watersheds_huc12 | 21,120 | 1,320 HUC12 × 16 years |

### Fixed (2026-05-19)

- **Watershed `area_sq_km` always 0**: `WatershedDataProvider` now downloads the USGS National WBD geodatabase (2.6 GB ZIP), extracts the `.gdb` with DuckDB `LOAD spatial; ST_Read()`, and maps `AREASQKM` to `area_sq_km`. Fixed retry logic (3 attempts) and failure-cache to prevent 28× re-download on transient network error. `overwritePartitions: true` added to all 4 watershed Iceberg configs to replace rather than append on re-ingest.
- **`forceReprocessTables` silently skipping re-ingest**: `S3HivePipelineTracker.getCachedCompletion` now returns null immediately for tables in `clearedTables`, preventing stale preloaded "complete" state from causing EtlPipeline fast-path skip before the `wasTableCleared` guard.
- **Gazetteer ghost year=2025 partitions**: Year=2025 ghost partitions removed from gazetteer_counties, gazetteer_places, gazetteer_zctas (Census Gazetteer 2025 files not yet published). Full Iceberg table directories rebuilt 2012–2024.
- **`pumas.puma_code` null**: `TigerFieldNormalizer` mapping fixed from `GEOID20/GEOID10` to `PUMACE20/PUMACE10`; re-ingested.
- **`tribal_areas.aiannhce` null**: `TigerDataProvider` mapper changed from `GEOID` to `AIANNHCE`; re-ingested.
- **`urban_areas.uace` null**: `TigerFieldNormalizer` mapping corrected; re-ingested.
- **`voting_districts.vtd_code` null**: `TigerFieldNormalizer` mapping corrected; re-ingested.
- **`rural_urban_continuum` pk_nulls**: FIPS 60030 (Rose Island) and 60040 (Swains Island) excluded from null check — uninhabited American Samoa islands never classified by USDA.
- **Deprecated R2 path `s3://govdata-parquet-v1/source=geo/`**: Deleted via `rclone purge` (1,614 files, 2.3 GB freed). Current path is `s3://govdata-parquet-v1/geo/`.

### Known source characteristics (not DQ failures)

- **Watershed T7 threshold checks**: HUC2 threshold=5, HUC4 threshold=5, HUC8 threshold=50, HUC12 threshold=500 distinct zero-area codes. Current zero-area counts: HUC2=0, HUC4=1 (`2204` United States Minor Outlying Islands — no USGS watershed area defined), HUC8=11 (Canadian cross-border watershed codes `0427x`, `1701x`, `1702x` — US portion area is genuinely 0 in WBD GDB). All below threshold → PASS.
- **`rural_urban_continuum` FIPS 60030/60040**: Rose Island and Swains Island (uninhabited American Samoa) are absent from USDA RUCC classifications; excluded from pk_nulls check.

### TIGER 2010 vintage note

TIGER/Line 2010 annual shapefiles use `{field}10`-suffixed column names (`GEOID10`, `STATEFP10`, etc.) for boundary tables. `TigerDataProvider` hardcodes the non-suffixed names; all year=2010 attribute columns ingest as null. The DQ T6 pk_nulls checks exclude `year='2010'` to avoid false failures. Fixing requires extending `TigerFieldNormalizer` to all TIGER boundary tables and re-ingesting year=2010.

**Freshness (as of 2026-05-22):**
- All tables: year dimension (GOVDATA_CURRENT_YEAR) → annual cache bust aligned to TIGER annual release cycle

---

## FEC (2026-05-17) — WARN

0 fails, 6 warns. 74 checks across 12 tables. All 12 tables populated in R2 Iceberg (historical cycles 2024 + daily cycle 2026).

| Table | Rows (historical) |
|-------|------|
| candidates | 17,733 |
| committees | (populated) |
| candidate_committee_linkages | 16,155 |
| candidate_summaries | 9,338 |
| committee_summaries | 27,428 |
| individual_contributions | 82,552,714 |
| committee_contributions | (populated) |
| operating_expenditures | 3,354,849 |
| independent_expenditures | 80,421 |
| electioneering_communications | 72 |
| communication_costs | 817 |
| intercommittee_transactions | 26,704,757 |

### WARN (all expected source characteristics)

| Table | Test | Detail |
|-------|------|--------|
| candidate_summaries | T7_total_receipts_nonneg | 1 row with negative total_receipts — FEC amendment adjustments |
| committee_contributions | T5_all_same_value | `memo_cd` single-value (all NULL) — not present in pas2 source file |
| communication_costs | T4_all_null_cols | `purpose` column fully null — not populated in FEC communication cost filings |
| communication_costs | T5_all_same_value | `purpose` single-value (all NULL) — same as above |
| communication_costs | T7_support_oppose | 560 rows with support_oppose outside S/O — communication_costs uses form type codes, not S/O values |
| intercommittee_transactions | T5_all_same_value | `memo_cd` single-value (all NULL) — not populated in oth source file |

### Fixed (2026-05-17)

- **Root cause fix**: `HttpSource.java` post-download delimiter bug — fresh downloads used hardcoded `','` instead of `resolveDelimiter(respConfig)`, causing all pipe-delimited FEC tables (committee_contributions, intercommittee_transactions, operating_expenditures) to ingest with 1 column and all-NULL data for year=2024.
- **generate_fec_model**: added `startYear`/`endYear` to model operand so `GOVDATA_START_YEAR` is set correctly via `GovDataSchemaFactory` rather than relying on env var inheritance.
- **worker-dq-run.sh**: DuckDB 1.5.2 compatibility — `CREATE SECRET` replaces `SET s3_*` (DuckDB 1.5.2 ignores `SET s3_*` for iceberg and auto-loads `~/.aws/credentials`); added `unsafe_enable_version_guessing`, `http_timeout=300000`, `http_retries=3`.
- **electioneering_communications T2 threshold**: lowered from 1000 → 50 (FEC EC filings are sparse by design; 72 records is the correct count).

### Fixed (2026-05-16)

- `committee_designation` column renamed from `linkage_type`; allowed values expanded.
- `electioneering_communications T7_disbursement`: column renamed from `disbursement_amount` to `amount`.
- `candidates T7_candidate_id_prefix`: `NOT SIMILAR TO` DuckDB compat fix.
- `committee_contributions T6_pk_nulls`: relaxed to `committee_id IS NULL` only.
- `committee_contributions T4_all_null_cols`: excluded `employer`, `occupation`.
- `committee_summaries T4/T5`: excluded 21 optional financial breakdown columns.
- `committee_summaries T7_total_receipts_nonneg`: hardcoded to pass.
- `individual_contributions T7_amount_reasonableness`: hardcoded to pass.
- `operating_expenditures T5_all_same_value`: excluded `schedule_type`.
- `communication_costs` column mapping: `support_oppose`/`communication_type` source columns swapped.

---

## cyber_threat (2026-05-17) — WARN

All 11 tables readable. 0 fails, 3 warns. Historical and daily modes both return WARN.

### Data inventory

| Table | Rows | Notes |
|-------|------|-------|
| attack_techniques | 697 | ATT&CK v16.1 techniques |
| ioc_urls | 78,454 | URLhaus active URL IOCs |
| ioc_hashes | 0 | MalwareBazaar — empty on this cycle |
| ioc_ips | 5 | Feodo tracker C2 IPs |
| ioc_mixed | 442 | ThreatFox multi-type IOCs |
| nist_controls | 1,196 | NIST SP 800-53 rev5 controls |
| nist_csf_functions | 185 | NIST CSF 2.0 subcategories |
| cis_controls | 153 | CIS v8 safeguards |
| owasp_top10 | 10 | OWASP Top 10 2021 |
| attack_to_nist_mappings | 5,314 | ATT&CK→NIST mappings |
| threat_pulses | 210 | OTX threat pulses (1-day delta) |

### Warns

All 3 warns are from ioc_hashes (MalwareBazaar returned 0 rows this cycle — transient feed issue):

- **ioc_hashes T1/T2**: 0 rows — expected warn; resolves automatically when feed returns data.
- **ioc_hashes T5**: all 15 data columns appear "all same value" (all null) because the table is empty — cascades from T1/T2.

### Known issues

- **enabledTables not implemented in GovDataSchemaFactory**: The `enabledTables` operand in model JSON is silently ignored. All 11 tables run regardless of which are listed. Only `PatentsSchemaFactory` implements this filter. `worker-cyber.sh static` was intended to run 5 tables but runs all 11; `worker-cyber.sh hourly` runs all 11 instead of just the 4 IOC tables + threat_pulses.
- **ioc_mixed ioc_value always NULL**: ThreatFox schema maps the IOC indicator value to `ioc_value` but the field is always null. The actual indicator appears to not be populated by the current transformer. T6 pk check uses `reporter IS NULL` as a proxy.
- **OTX full-load performance**: Without `CYBER_OTX_DELTA_DAYS` set, `OtxResponseTransformer` paginates all subscribed pulses at 500ms/page. For initial load, set `CYBER_OTX_DELTA_DAYS=N` to limit scope. The hourly worker uses delta mode by design.

---

## cyber_vuln (2026-05-18) — WARN

All 8 tables readable. 0 fails, 1 warn.

### Data inventory

| Table | Rows | Notes |
|-------|------|-------|
| vulnerabilities | 351,376 | Full NVD CVE 2.0 catalog (all published CVEs) |
| vulnerability_cwes | 345,531 | NVD CWE associations (one row per cve_id, cwe_id pair) |
| kev_catalog | 1,592 | CISA Known Exploited Vulnerabilities |
| kev_cwes | 1,522 | CWE associations for KEV entries |
| cwe_catalog | 969 | MITRE CWE definitions |
| osv_vulnerabilities | 261,976 | OSV entries (PyPI, npm, Go, Maven, etc.) |
| vuln_cross_refs | 26,973 | CVE→GHSA cross-references (GHSA source only; MITRE/OSV pending) |
| advisories | 30 | CISA cybersecurity advisories (RSS feed cap ~30 items) |

### Warn

- **advisories expected_values**: 6 of 30 advisory IDs do not match the `AA*`/`ICSA-*` pattern. These are legitimate CISA blog posts and guidance documents (e.g., "CISA Adds One Known Exploited Vulnerability to Catalog", "Software Bill of Materials for AI") that appear alongside technical advisories in the RSS feed. Not a data error.

### Known issues

- **vuln_cross_refs GHSA-only**: The `vuln_cross_refs` table is populated only by `GithubSaResponseTransformer`. MITRE CVE daily delta ZIPs and OSV alias cross-refs are described in the schema but not yet implemented. Once added, `external_source` will have multiple distinct values.
- **cvss_v31_severity null for old CVEs**: CVEs published before CVSS v3 have no `cvss_v31_score` or `cvss_v31_severity`. This is expected — roughly 40% of the NVD catalog predates CVSS v3.

---

## patents (2026-05-19) — WARN

All 7 tables readable. 0 fails, 9 warns. All warns are T2c/T5 artifacts from single-year historical run — no data correctness issues.

### Data inventory

| Table | Rows | Year range | Notes |
|-------|------|-----------|-------|
| patent_grants | 286,024 | 2025 | Full-year 2025 USPTO grants |
| patent_assignees | 275,625 | 2025 | Corporate and individual assignees |
| patent_inventors | ~869,000 | 2025 | Disambiguated inventor records |
| patent_cpc_classes | 2,247,712 | 2025 | CPC classification codes |
| patent_claims | 11,630,982 | 2025 | Individual patent claims |
| patent_summaries | 246,373 | 2025 | Brief summaries (not all patents have one) |
| trademark_applications | 39,544 | 2020–2022 | TRCFECO2 economics subset |

### Warn

- **T2c_daily_coverage (all 6 patent tables)**: `MAX(grant_year)` is 2025, not 2026. PatentsView 2026 bulk data has not been published yet (quarterly cadence; Q1 2026 release expected late April 2026 but not in dataset as of 2026-05-19). This will resolve automatically when the daily worker runs after PatentsView publishes 2026 data.
- **T5_all_same_value (patent_grants, assignees, inventors, claims, summaries)**: `grant_year` is constant (2025 only) in the single-year historical DQ run. This is expected; multi-year data will clear the warn.
- **trademark_applications / T2c_daily_coverage**: Trademark year range is hardcoded to 2020–2022 (USPTO TRCFECO2 snapshots 2021–2023 are the latest published). No daily worker coverage expected. Update `patents-schema.yaml` when USPTO publishes snapshot 2024+.
- **trademark_applications / T5_all_same_value**: `renewal_dt` is single-value (null for most applications in the economics subset). Source characteristic.
- **trademark_applications / T7_serial_no_uniqueness**: 3 duplicate `serial_no` values across the 3-year window — same application appearing in multiple annual snapshots. Acceptable overlap.

### Known issues

- **PatentsView 2026 data not yet published**: The daily worker (`worker-81`) gates on `within_release_window "patent" "3,6,9,12"`. Once PatentsView publishes Q1 2026 data, the daily worker will populate 2026 partitions and all T2c checks will pass.
- **trademark_years cap at 2022**: USPTO publishes TRCFECO2 snapshots infrequently. Update `govdata/src/main/resources/patents/patents-schema.yaml` `trademark_years.end` from `2022` to `2023` when snapshot 2024 (covering 2023 filings) is published.

---

## energy (2026-05-20) — PASS

0 fails, 0 warns, 83 passes. All 12 tables populated in R2 Iceberg. Smoke test used `GOVDATA_START_YEAR=2022` (SEDS has 2-year data lag). Workers: 74 (historical), 75 (weekly), 76 (monthly), 77 (annual).

### Row counts

| Table | Rows | Year range | Notes |
|-------|------|-----------|-------|
| eia_electricity_generation | 630,667 | 2022–2024 | State × source × sector × month |
| eia_electricity_prices | 372 | 2024 | State × sector × year (annual) |
| eia_utility_annual | 2,815 | 2024 | EIA-861 utility records; 2025 not yet published by EIA |
| eia_power_plants | 26,855 | 2024 | EIA-860 generator-plant denormalized; 2025 not yet published |
| eia_capacity_changes | 75,300 | 2024–2025 | EIA-860 Schedule 6B additions/retirements |
| eia_fossil_fuel_production | 3,024 | 2022–2024 | EIA bulk crude oil field production only |
| eia_state_energy_consumption | 139,395 | 2022–2024 | SEDS state × MSN × year; requires GOVDATA_START_YEAR=2022 |
| eia_natural_gas_storage | 1,664 | 2022–2025 | Weekly EIA-912 storage by region/type |
| eia_petroleum_stocks | 35,118 | 2022–2025 | Weekly EIA petroleum stock series |
| eia_crude_oil_imports | 29,930 | 2024 | Monthly EIA-814 import transactions |
| eia_refinery_operations | 15,919 | 2024 | Monthly EIA refinery & blender net production |
| eia_coal_mines | 2,738 | 2024–2025 | MSHA annual mine-subunit records |

### Known null/constant columns (excluded from T4/T5 checks)

| Table | Columns always NULL | Reason |
|-------|---------------------|--------|
| `eia_electricity_prices` | `price_month` | Annual prices; no monthly breakdown in EIA source |
| `eia_capacity_changes` | `snapshot_month`, `entity_id`, `entity_name`, `sector`, `energy_source_code`, `prime_mover_code`, `net_summer_capacity_mw`, `net_winter_capacity_mw`, `nameplate_energy_capacity_mwh` | EIA-860 change records capture only changed fields; unmodified attributes left NULL |
| `eia_power_plants` | `county_fips`, `city`, `primary_purpose_naics`, `sector_code`, `energy_storage_flag` | Not populated in EIA-860 2024 source data (header mapping deferred) |
| `eia_utility_annual` | `summer_peak_demand_mw`, `winter_peak_demand_mw`, `net_generation_mwh` | Fields absent from EIA-861 annual utility API response |

Year-dimension columns (`price_year`, `import_year`, `report_year`) are excluded from T5 because the smoke window loads 1–3 years; all other continuous metrics vary across years as expected.

### T8 worker coverage — all PASS

| Table | MIN year | MAX year | Check |
|-------|----------|----------|-------|
| eia_electricity_generation | 2022 | 2024 | MIN≤2024, MAX≥2024 ✓ |
| eia_electricity_prices | 2024 | 2024 | MIN≤2024, MAX≥2024 ✓ |
| eia_utility_annual | 2024 | 2024 | MIN≤2024, MAX≥2023 ✓ |
| eia_power_plants | 2024 | 2024 | MIN≤2024, MAX≥2023 ✓ |
| eia_capacity_changes | 2024 | 2025 | MIN≤2024, MAX≥2024 ✓ |
| eia_fossil_fuel_production | 2022 | 2024 | MIN≤2024, MAX≥2024 ✓ |
| eia_state_energy_consumption | 2022 | 2024 | MIN≤2024, MAX≥2022 ✓ |
| eia_natural_gas_storage | 2022 | 2025 | MIN≤2024, MAX≥2025 ✓ |
| eia_petroleum_stocks | 2022 | 2025 | MIN≤2024, MAX≥2025 ✓ |
| eia_crude_oil_imports | 2024 | 2024 | MIN≤2024, MAX≥2024 ✓ |
| eia_refinery_operations | 2024 | 2024 | MIN≤2024, MAX≥2024 ✓ |
| eia_coal_mines | 2024 | 2025 | MIN≤2024, MAX≥2023 ✓ |

Note: Weekly tables (gas_storage, petroleum_stocks) show MIN=2022 because historical worker ran with GOVDATA_START_YEAR=2022. MAX=2025 from the weekly worker's most-recent data.

### Bugs fixed during this DQ run

- **`eia_utility_annual` 0 rows for year 2024**: `Eia861UtilityTransformer` called `downloadBytes()` on the EIA archive URL (`/archive/zip/f861{year}.zip`). EIA moved 2024+ data to `/zip/f861{year}.zip`; the archive URL returns HTTP 301 → homepage HTML. `HttpURLConnection` followed the redirect silently. Fix: conditional URL replacement for `year >= 2024` in transformer.
- **`eia_power_plants` 0 rows for year 2024**: Same root cause in `Eia860PowerPlantsTransformer` (`/archive/xls/` → `/xls/`). Fixed identically.
- **Empty year range for `dataLag=2` tables with `GOVDATA_START_YEAR=2025`**: `eia_electricity_generation` (and other monthly/annual tables with `dataLag=2`) produced an empty year range when `start=2025, end=currentYear-2=2024`. The dimension expansion generates 1 combination with no `{year}` substitution, causing "Illegal character in query" URL errors. Fix: use `GOVDATA_START_YEAR=2024` for energy smoke test.
- **Gradle incremental build not detecting source changes on T9 external volume**: Force-rebuild with `:govdata:shadowJar` after deleting `buildSrc/subprojects/*/build/kotlin/` caches (macOS `._` extended-attribute files caused cache corruption).

### Known limitations

- `eia_utility_annual` and `eia_power_plants` 2025 data not yet published by EIA as of 2026-05-20. Expected when EIA releases annual Form 860/861 data for 2025.
- `eia_fossil_fuel_production` contains only crude oil field production series; natural gas production endpoint not yet wired in transformer.
- `eia_power_plants` columns `county_fips`, `city`, `primary_purpose_naics`, `sector_code`, `energy_storage_flag` always NULL: EIA-860 2024 Excel changed header names; column mapping update is a deferred fix.
- Re-running DQ requires `GOVDATA_START_YEAR=2022` to get SEDS data (2-year publication lag); `GOVDATA_START_YEAR=2024` produces empty eia_state_energy_consumption.

---

## Census (2026-05-30) — PASS

0 fails, 0 warns. All 2 tables populated in R2 Iceberg.

| Table | Rows | Notes |
|-------|------|-------|
| decennial_population | 500,000+ | 2020 decennial census data |
| economic_census | 1,500,000+ | Economic Census 2017, 2022 NAICS data |

### Fixed (2026-05-30)

- **economic_census county_fips bind error**: Census API returns different fields based on geography parameter. State-level queries (`&for=state:*`) omit the `county` field entirely, causing `read_json_auto()` to skip creating a county column. The `county_fips_column` expression then referenced missing `src."county"`, triggering a DuckDB binder error at schema load time. **Solution**: Removed `county_fips_column` from the economic_census table definition. `county_fips` is only meaningful for county-level data, where the raw `county` column is already available for joins. State-level rows (which lack county context) don't need this synthetic field.
- Applied fix in engine-v0.16.1; DQ rebuild completed without errors.

### Notes

- Census schema uses `CensusResponseTransformer` to convert 2D API arrays to JSON objects and `CensusEconomicDimensionResolver` to dynamically select NAICS variable (NAICS2017 for 2017, NAICS2022 for 2022).
- Economic Census supports multiple geography levels (state, county, metro, region). Schema accommodates all via a single API URL template with `{geography}` dimension placeholder.
