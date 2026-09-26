-- dq-lookback: 1
-- U.S. Agriculture Data Quality Checks
-- Schema: ag  (USDA NASS, RMA, ERS, FSA)
-- All Iceberg; single-nested iceberg_scan path. Partition cols: type, year.
-- T6 checks the identifying key column of each table's declared primary key.
-- Until the first ETL run, T1/T2 read fail (0 rows) — the honest signal that
-- the schema is not yet ingested, not a defect to be masked.

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

CREATE TEMP TABLE dq_results (
  schema   VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR,
  value DOUBLE, threshold DOUBLE, detail VARCHAR
);

-- ------------------------------------------------------------
-- TABLE: nass_crop_production (partition cols: type, year; PK id col: short_desc)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_production', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_production', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_production', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
-- sector_desc/group_desc are excluded: this table is intentionally scoped to NASS's Crop
-- Production report (CROPS/FIELD CROPS only, e.g. corn/soybeans/wheat/cotton) — constant
-- across every year in production, not an ingestion gap. source_desc is NOT excluded here:
-- it is genuinely single-valued only within a narrow DQ sample window that happens to land
-- on a non-Census-of-Agriculture year; production holds both SURVEY and CENSUS values.
SELECT 'ag', 'nass_crop_production', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'sector_desc', 'group_desc')));
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_production', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL short_desc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true) WHERE short_desc IS NULL);

-- T7: no in-season forecast vintages -- NassQuickStatsTransformer drops
-- reference_period_desc values matching 'YEAR - % FORECAST' (the in-season revisions NASS
-- publishes ahead of each year's final production/yield estimate). Their presence means the
-- transformer-side filter was bypassed and forecast/final revisions are stacking under the
-- same key again. Weekly ('WEEK #nn') and monthly condition/progress reference periods are
-- untouched by this check -- they are a different, legitimately multi-row measure.
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_production', 'T7_no_forecast_vintages',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with reference_period_desc LIKE ''YEAR - % FORECAST'''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_production', allow_moved_paths := true) WHERE reference_period_desc LIKE 'YEAR - % FORECAST');

-- ------------------------------------------------------------
-- TABLE: nass_crop_progress (partition cols: type, year; PK id col: short_desc)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
-- sector_desc/group_desc/source_desc/freq_desc are constant by design: the request is scoped to
-- SURVEY / CROPS / FIELD CROPS and every weekly row reports WEEKLY.
SELECT 'ag', 'nass_crop_progress', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'sector_desc', 'group_desc', 'source_desc', 'freq_desc')));
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL short_desc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true) WHERE short_desc IS NULL);

-- T7: every row carries its week-ending date -- the join key to weekly series such as
-- cftc.cot_disaggregated_futures.report_date.
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T7_week_ending_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with NULL week_ending'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true) WHERE week_ending IS NULL);

-- T8: national corn good/excellent condition ratings -- the series the grain
-- positioning-vs-crop-condition analysis this table exists for depends on.
INSERT INTO dq_results
SELECT 'ag', 'nass_crop_progress', 'T8_national_corn_condition_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1,
  'Rows with agg_level_desc=NATIONAL, commodity_desc=CORN, unit_desc=PCT GOOD or PCT EXCELLENT'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_crop_progress', allow_moved_paths := true)
  WHERE agg_level_desc = 'NATIONAL' AND commodity_desc = 'CORN' AND statisticcat_desc = 'CONDITION'
    AND unit_desc IN ('PCT GOOD', 'PCT EXCELLENT'));

-- ------------------------------------------------------------
-- TABLE: nass_livestock_inventory (partition cols: type, year; PK id col: short_desc)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'nass_livestock_inventory', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'nass_livestock_inventory', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'nass_livestock_inventory', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'nass_livestock_inventory', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'nass_livestock_inventory', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL short_desc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_livestock_inventory', allow_moved_paths := true) WHERE short_desc IS NULL);

-- ------------------------------------------------------------
-- TABLE: nass_farm_operations (partition cols: type, year; PK id col: short_desc)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'nass_farm_operations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'nass_farm_operations', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'nass_farm_operations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
-- sector_desc/group_desc/commodity_desc are excluded: this table is intentionally scoped to
-- NASS's FARM OPERATIONS commodity (ECONOMICS/FARMS & LAND & ASSETS) — constant across every
-- year in production, not an ingestion gap.
SELECT 'ag', 'nass_farm_operations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'sector_desc', 'group_desc', 'commodity_desc')));
INSERT INTO dq_results
SELECT 'ag', 'nass_farm_operations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL short_desc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_farm_operations', allow_moved_paths := true) WHERE short_desc IS NULL);

-- ------------------------------------------------------------
-- TABLE: nass_land_values (partition cols: type, year; PK id col: short_desc)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'nass_land_values', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'nass_land_values', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'nass_land_values', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
-- sector_desc/group_desc/commodity_desc are excluded: this table is intentionally scoped to
-- NASS's AG LAND commodity (ECONOMICS/FARMS & LAND & ASSETS) — constant across every year in
-- production, not an ingestion gap.
SELECT 'ag', 'nass_land_values', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'sector_desc', 'group_desc', 'commodity_desc')));
INSERT INTO dq_results
SELECT 'ag', 'nass_land_values', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL short_desc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true) WHERE short_desc IS NULL);

-- T7: land-value-per-acre series present -- the specific measure the
-- farm-consolidation-vs-land-value analysis this table exists for depends on.
-- Its absence would mean the SURVEY/ASSET VALUE rows never landed even though
-- the table has other AG LAND rows (e.g. only CENSUS tenure statistics ingested).
INSERT INTO dq_results
SELECT 'ag', 'nass_land_values', 'T7_land_value_per_acre_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1,
  'Rows with source_desc=SURVEY, statisticcat_desc=ASSET VALUE, unit_desc=$ / ACRE'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/nass_land_values', allow_moved_paths := true)
  WHERE source_desc = 'SURVEY' AND statisticcat_desc = 'ASSET VALUE' AND unit_desc = '$ / ACRE');

-- ------------------------------------------------------------
-- TABLE: rma_crop_insurance (partition cols: type, year; PK id col: commodity_code)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'rma_crop_insurance', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'rma_crop_insurance', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'rma_crop_insurance', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'rma_crop_insurance', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'rma_crop_insurance', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL commodity_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_crop_insurance', allow_moved_paths := true) WHERE commodity_code IS NULL);

-- ------------------------------------------------------------
-- TABLE: rma_cause_of_loss (partition cols: type, year; PK id col: cause_of_loss_code)
-- PK: year, state_fips, county_fips, commodity_code, insurance_plan_code,
-- coverage_category, stage_code, cause_of_loss_code, month_of_loss, year_of_loss.
-- stage_code is legitimately null for plans/commodities with no crop growth stage
-- (WFRP, DO, TDO, AQDOL, some HIP-WI rows); year_of_loss is legitimately null for
-- the cause_of_loss_code="XX" (All Other Causes) annual rollup (month_of_loss=0).
-- Both are excluded from the null-intolerant check below, but still included in
-- the dupe-key GROUP BY (two null-valued rows never collide as duplicates under
-- SQL NULL semantics).
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL rows in (year, state_fips, county_fips, commodity_code, insurance_plan_code, coverage_category, cause_of_loss_code, month_of_loss) -- stage_code and year_of_loss excluded, legitimately nullable'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true)
  WHERE year IS NULL OR state_fips IS NULL OR county_fips IS NULL OR commodity_code IS NULL
    OR insurance_plan_code IS NULL OR coverage_category IS NULL
    OR cause_of_loss_code IS NULL OR month_of_loss IS NULL);
-- T6_pk_dupes: uniqueness over the full descriptive row (declared key + cause_of_loss_desc),
-- following the same widened-grain pattern as lands.onrr_revenues -- confirmed live that RMA's
-- own cause_of_loss_code 47 carries two label variants ("Hurricane" / "Hurricane (HIP-WI only).")
-- for identical key values under insurance_plan_code=37 (HIP-WI) commodity_code=9999 rows; those
-- are genuinely distinct source rows, not a parsing defect, so cause_of_loss_desc must be part of
-- the uniqueness test even though it is not part of the declared primaryKey.
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate (year, state_fips, county_fips, commodity_code, insurance_plan_code, coverage_category, stage_code, cause_of_loss_code, cause_of_loss_desc, month_of_loss, year_of_loss) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, state_fips, county_fips, commodity_code, insurance_plan_code, coverage_category,
    stage_code, cause_of_loss_code, cause_of_loss_desc, month_of_loss, year_of_loss, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true)
  GROUP BY year, state_fips, county_fips, commodity_code, insurance_plan_code, coverage_category,
    stage_code, cause_of_loss_code, cause_of_loss_desc, month_of_loss, year_of_loss
  HAVING COUNT(*) > 1));
-- T7: expected_values — the well-known dominant perils (Drought, Hail, Excess
-- Moisture/Precipitation/Rain, Freeze) must appear in the cause-of-loss domain;
-- their absence would indicate the pipe-delimited layout parsed into the wrong
-- columns. Domain confirmed live against colsom_2023.zip.
INSERT INTO dq_results
SELECT 'ag', 'rma_cause_of_loss', 'T7_expected_values',
  CASE WHEN n >= 3 THEN 'pass' ELSE 'fail' END, n, 3,
  'Distinct well-known perils present in cause_of_loss_desc (Drought/Hail/Freeze/Excess Moisture...)'
FROM (SELECT COUNT(DISTINCT cause_of_loss_desc) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/rma_cause_of_loss', allow_moved_paths := true)
  WHERE cause_of_loss_desc IN ('Drought', 'Hail', 'Freeze', 'Excess Moisture/Precipitation/Rain'));

-- ------------------------------------------------------------
-- TABLE: pesticide_use_by_county (partition cols: type, year; PK id col: compound)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'pesticide_use_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'pesticide_use_by_county', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'pesticide_use_by_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'pesticide_use_by_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'pesticide_use_by_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL compound rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/pesticide_use_by_county', allow_moved_paths := true) WHERE compound IS NULL);

-- ------------------------------------------------------------
-- TABLE: ers_farm_income (partition cols: type, year; PK id col: artificial_key)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'ers_farm_income', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'ers_farm_income', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'ers_farm_income', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'ers_farm_income', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'ers_farm_income', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL artificial_key rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_farm_income', allow_moved_paths := true) WHERE artificial_key IS NULL);

-- ------------------------------------------------------------
-- TABLE: ers_commodity_costs_returns (partition cols: type, commodity; PK id col: item)
-- country and size are excluded from T5: ERS publishes no non-US rows for this
-- product ('United States' is a genuine constant), and this product carries no
-- operation-size stratification ('No specific size' is a genuine constant across
-- every commodity, confirmed live 2026-09-28) -- neither is an ingestion gap.
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'commodity')));
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'commodity', 'country', 'size')));
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL item rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true) WHERE item IS NULL);
INSERT INTO dq_results
SELECT 'ag', 'ers_commodity_costs_returns', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (commodity,year,region,category,item) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT commodity, year, region, category, item, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/ers_commodity_costs_returns', allow_moved_paths := true)
    GROUP BY commodity, year, region, category, item
    HAVING COUNT(*) > 1
  )
);

-- ------------------------------------------------------------
-- TABLE: fsa_commodity_payments (partition cols: type, year; PK id col: program_code)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'fsa_commodity_payments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'fsa_commodity_payments', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'ag', 'fsa_commodity_payments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'fsa_commodity_payments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'ag', 'fsa_commodity_payments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL program_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fsa_commodity_payments', allow_moved_paths := true) WHERE program_code IS NULL);

-- ------------------------------------------------------------
-- TABLE: fas_export_sales (partition cols: type, year; PK id cols: commodity_code, country_code, week_ending_date)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'ag', 'fas_export_sales', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'ag', 'fas_export_sales', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
-- country_iso3/region_id are NOT excluded from T4: FAS's own "EUROPEAN"
-- code (countryCode=1) is a real, low-volume destination row, so a null
-- gencCode for it does not make the whole column all-null in a real sample.
SELECT 'ag', 'fas_export_sales', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
-- unit_id/unit_name are excluded: this table is overwhelmingly Metric Tons
-- (unitId=1) by USDA ESR design across almost every commodity, not an
-- ingestion gap -- same idiom as nass_crop_production's sector_desc exclusion.
SELECT 'ag', 'fas_export_sales', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'unit_id', 'unit_name')));
INSERT INTO dq_results
SELECT 'ag', 'fas_export_sales', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL commodity_code/country_code/week_ending_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true)
  WHERE commodity_code IS NULL OR country_code IS NULL OR week_ending_date IS NULL);
INSERT INTO dq_results
SELECT 'ag', 'fas_export_sales', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, commodity_code, country_code, week_ending_date) rows'
FROM (SELECT COUNT(*) AS n FROM (
    SELECT year, commodity_code, country_code, week_ending_date, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true)
    GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
  ));
-- T7: China (5700) soybean (801) rows exist and carry non-null weekly_exports --
-- the exact slice kenstott/govdata-ops#655 needs. A fail here means the table
-- is populated but not with the commodity/country combination it exists for.
INSERT INTO dq_results
SELECT 'ag', 'fas_export_sales', 'T7_china_soybeans_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'China (5700) Soybeans (801) rows with non-null weekly_exports'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/fas_export_sales', allow_moved_paths := true)
  WHERE country_code = 5700 AND commodity_code = 801 AND weekly_exports IS NOT NULL);

-- ============================================================================
-- T1/T2 — tables previously absent from this file entirely.
-- A table with no checks emits no dq rows, which reads as healthy rather than as
-- untested; that is how four zero-row geo tables went unnoticed. Existence +
-- row_count only — deliberately minimal, to be deepened per table.
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT 'faostat_production' AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ag/faostat_production', allow_moved_paths := true) LIMIT 1)) AS n
)
SELECT 'ag', tbl, 'existence',
       CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
       n, 1,
       CASE WHEN n > 0 THEN 'readable' ELSE 'NO ROWS — table unreadable or never written' END
FROM counts;


SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results ORDER BY schema, tbl, test;
