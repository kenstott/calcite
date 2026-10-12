-- dq-lookback: 1
-- Fisheries Data Quality Checks
-- Schema: fisheries
-- Tables: noaa_feus_economic_impacts, noaa_commercial_landings, mrip_recreational_catch, mrip_recreational_effort, dart_adult_counts, adfg_fish_counts, cdfw_grandtab_escapement, esa_salmonid_listings, wdfw_salmonid_escapement, stock_smart_assessments
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition column 'year' and the constant partition column 'type'.
-- No PK check: NOAA publishes several source rows per state + species + source + collection
-- (MRIP and confidential-species rows are split), so duplicates of the natural key are expected.

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET unsafe_enable_version_guessing=true;

CREATE TEMP TABLE dq_results (
  schema   VARCHAR,
  tbl      VARCHAR,
  test     VARCHAR,
  status   VARCHAR,
  value    DOUBLE,
  threshold DOUBLE,
  detail   VARCHAR
);

-- T1: existence
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true));

-- T2: row_count (~4,900 rows/year; DQ scope covers several landing years)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T2_row_count',
  CASE WHEN n >= 9000 THEN 'pass' ELSE 'fail' END,
  n, 9000, 'Expected at least 9000 landings rows (2+ years x ~4,900)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('year', 'type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('year', 'type')
  )
);

-- T6: not-null columns (landing_year, collection, source, state_name, is_state, is_salmon)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL landing_year, collection, source, state_name, is_state or is_salmon rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE landing_year IS NULL OR collection IS NULL OR source IS NULL
         OR state_name IS NULL OR is_state IS NULL OR is_salmon IS NULL);

-- T7: collection values
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_collection_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with collection outside (Commercial, Recreational)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE collection NOT IN ('Commercial', 'Recreational'));

-- T7: partition year matches landing_year
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_year_matches_landing_year',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows where partition year <> landing_year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true) WHERE year <> landing_year);

-- T7: every state_name maps to a state_abbr except PROCESS AT SEA
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_state_abbr_mapped',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with null state_abbr whose state_name is not PROCESS AT SEA'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE state_abbr IS NULL AND state_name <> 'PROCESS AT SEA');

-- T7: Alaska commercial salmon pounds are positive in every landing year loaded
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_alaska_salmon_every_year',
  CASE WHEN bad = 0 AND yrs > 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Landing years without Alaska commercial salmon pounds (years checked: ' || yrs || ')'
FROM (
  SELECT (SELECT COUNT(DISTINCT landing_year) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)) AS yrs,
         (SELECT COUNT(*) FROM (SELECT DISTINCT landing_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)) y
          WHERE y.landing_year NOT IN (
            SELECT landing_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
            WHERE state_abbr = 'AK' AND is_salmon AND collection = 'Commercial' AND pounds > 0)) AS bad
);

-- T7: Alaska sockeye pounds in the best loaded year are within the published order of magnitude
-- (2022: 381,177,520 lb; recent years range ~100M-400M lb)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_alaska_sockeye_magnitude',
  CASE WHEN lbs BETWEEN 5e7 AND 6e8 THEN 'pass' ELSE 'fail' END,
  lbs, 5e7, 'Max single-year Alaska commercial sockeye pounds (expect 50M-600M)'
FROM (SELECT MAX(p) AS lbs FROM (
        SELECT landing_year, SUM(pounds) AS p FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
        WHERE state_abbr = 'AK' AND salmon_species = 'sockeye' AND collection = 'Commercial'
        GROUP BY landing_year));

-- T7: salmon flag consistency (salmon rows carry species + origin; others carry neither)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_salmon_flag_consistency',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows where is_salmon disagrees with salmon_species / salmon_origin nullness'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE (is_salmon AND (salmon_species IS NULL OR salmon_origin IS NULL))
         OR (NOT is_salmon AND (salmon_species IS NOT NULL OR salmon_origin IS NOT NULL)));

-- ─────────────────────────────────────────────────────────────
-- dart_adult_counts
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true));

-- 17 projects, most with decades of history across several species
INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected at least 3000 project x year x species rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true))
        WHERE null_percentage = 100.0 AND column_name NOT IN ('project', 'type'))
);

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true))
        WHERE approx_unique <= 1 AND column_name NOT IN ('project', 'type'))
);

-- Natural key: project x year x species
INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL key/NOT NULL columns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true)
      WHERE project_code IS NULL OR project_name IS NULL OR river IS NULL OR state_abbrs IS NULL
         OR count_year IS NULL OR species IS NULL OR adult_count IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate project_code x count_year x species keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT project_code, count_year, species FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true)
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T7_no_large_negatives',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with adult_count below -10 (the source reports net counts, so tiny negatives from fallback are real: 5 rows of -1..-4)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true) WHERE adult_count < -10);

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T7_all_projects_loaded',
  CASE WHEN n = 17 THEN 'pass' ELSE 'fail' END, n, 17, 'Distinct project_code values (expect 17)'
FROM (SELECT COUNT(DISTINCT project_code) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true));

-- Bonneville 2023 Chinook is on the order of 100k-500k adults; the series starts in 1938
INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T7_bonneville_chinook_magnitude',
  CASE WHEN yrs >= 80 AND mx BETWEEN 2e5 AND 2e6 THEN 'pass' ELSE 'fail' END, mx, 2e5,
  'Bonneville Chinook: max annual count (expect 200k-2M) over ' || yrs || ' years (expect >= 80)'
FROM (SELECT MAX(adult_count) AS mx, COUNT(DISTINCT count_year) AS yrs FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true)
      WHERE project_code = 'BON' AND species = 'Chinook');

INSERT INTO dq_results
SELECT 'fisheries', 'dart_adult_counts', 'T7_spans_states_consistency',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows where spans_states disagrees with a comma in state_abbrs'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/dart_adult_counts', allow_moved_paths := true) WHERE spans_states <> (state_abbrs LIKE '%,%'));

-- ─────────────────────────────────────────────────────────────
-- adfg_fish_counts
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true));

-- ~100 location x species pairs with years of history each
INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END, n, 1500, 'Expected at least 1500 site x species x year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true))
        WHERE null_percentage = 100.0 AND column_name NOT IN ('location_id', 'species_id', 'type'))
);

-- state_abbr is a real constant: every ADF&G site is in Alaska
INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true))
        WHERE approx_unique <= 1 AND column_name NOT IN ('location_id', 'species_id', 'type', 'state_abbr'))
);

-- Natural key: site x species x year
INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL key/NOT NULL columns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true)
      WHERE adfg_location_id IS NULL OR location_name IS NULL OR adfg_species_id IS NULL
         OR species_name IS NULL OR count_year IS NULL OR total_count IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate adfg_location_id x adfg_species_id x count_year keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT adfg_location_id, adfg_species_id, count_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true)
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T7_no_negative_counts',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with total_count < 0 or days_counted < 1'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true) WHERE total_count < 0 OR days_counted < 1);

INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T7_dates_in_year',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows whose first/last_count_date fall outside count_year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true)
      WHERE CAST(SUBSTR(first_count_date, 1, 4) AS INTEGER) <> count_year
         OR CAST(SUBSTR(last_count_date, 1, 4) AS INTEGER) <> count_year);

-- The Kenai early-run Chinook sonar and Russian River sockeye are the flagship sites
INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T7_known_sites_loaded',
  CASE WHEN n = 4 THEN 'pass' ELSE 'fail' END, n, 4, 'Kenai Chinook, Russian River, Situk and Karluk sites present (expect 4)'
FROM (SELECT COUNT(DISTINCT adfg_location_id) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true) WHERE adfg_location_id IN (72, 13, 5, 24));

-- Russian River early-run sockeye escapement runs tens of thousands of fish per year
INSERT INTO dq_results
SELECT 'fisheries', 'adfg_fish_counts', 'T7_russian_sockeye_magnitude',
  CASE WHEN mx BETWEEN 5000 AND 500000 THEN 'pass' ELSE 'fail' END, mx, 5000,
  'Russian River early-run sockeye: max annual count (expect 5k-500k)'
FROM (SELECT MAX(total_count) AS mx FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/adfg_fish_counts', allow_moved_paths := true) WHERE adfg_location_id = 13 AND adfg_species_id = 421);

-- ─────────────────────────────────────────────────────────────
-- cdfw_grandtab_escapement
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true));

-- 4 runs x 2 origins x ~70 years; not every run/origin has a series back to 1952
INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T2_row_count',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'fail' END, n, 300, 'Expected at least 300 rows (4 runs x 2 origins)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true))
        WHERE null_percentage = 100.0 AND column_name NOT IN ('year', 'type')));

-- state_abbr is constant CA by construction (Central Valley only)
INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true))
        WHERE approx_unique = 1 AND column_name NOT IN ('year', 'type', 'state_abbr')));

INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with a null key or value column'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true)
      WHERE run_name IS NULL OR origin_type IS NULL OR count_year IS NULL OR escapement IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate run_name x origin_type x count_year keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT run_name, origin_type, count_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true)
                                 GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T7_all_runs_and_origins',
  CASE WHEN n = 8 THEN 'pass' ELSE 'fail' END, n, 8, 'Distinct run x origin combinations (expect 8)'
FROM (SELECT COUNT(*) AS n FROM (SELECT DISTINCT run_name, origin_type FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true)));

INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T7_no_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with escapement < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true) WHERE escapement < 0);

-- Fall-run Chinook is the bulk of the Central Valley return; 2025 in-river + hatchery ~235,769
INSERT INTO dq_results
SELECT 'fisheries', 'cdfw_grandtab_escapement', 'T7_fall_2025_matches_source',
  CASE WHEN total = 235769 THEN 'pass' ELSE 'fail' END, total, 235769,
  'Fall run 2025 In-River + Hatchery vs SacPAS Chinook:Fall type=All (235,769)'
FROM (SELECT COALESCE(SUM(escapement), 0) AS total FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/cdfw_grandtab_escapement', allow_moved_paths := true) WHERE run_name = 'Fall' AND count_year = 2025);

-- ─────────────────────────────────────────────────────────────
-- esa_salmonid_listings
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T2_row_count',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'fail' END, n, 40, 'Expected at least 40 salmon/steelhead listing entities'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL common_name, scientific_name, species_group, esa_status or lead_agency rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true)
      WHERE common_name IS NULL OR scientific_name IS NULL OR species_group IS NULL
         OR esa_status IS NULL OR lead_agency IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T7_species_groups',
  CASE WHEN bad = 0 AND n = 7 THEN 'pass' ELSE 'fail' END, n, 7,
  'Distinct species_group values (expect 7: chinook, coho, chum, sockeye, pink, steelhead, atlantic); out-of-set rows: ' || bad
FROM (SELECT COUNT(DISTINCT species_group) AS n,
             COUNT(*) FILTER (WHERE species_group NOT IN ('chinook','coho','chum','sockeye','pink','steelhead','atlantic')) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true));

-- Every listed/experimental entity carries states (the transformer rejects one without)
INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T7_listed_have_states',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'Listed rows with null state_abbrs'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true) WHERE status_category = 'Listed' AND state_abbrs IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T7_listing_date_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'Non-null listing_date not in yyyy-MM-dd'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true)
      WHERE listing_date IS NOT NULL AND NOT regexp_matches(listing_date, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'));

-- Known fact: Snake River sockeye ESU listed Endangered 1992-01-03 (50 CFR 224.101)
INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T7_snake_sockeye_endangered',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1,
  'Snake River sockeye rows with status Endangered and listing_date 1992-01-03'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true)
      WHERE species_group = 'sockeye' AND esa_status = 'Endangered'
        AND listing_date = '1992-01-03' AND state_abbrs = 'ID,OR,WA');

INSERT INTO dq_results
SELECT 'fisheries', 'esa_salmonid_listings', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate common_name x population_name x esa_status keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT common_name, population_name, esa_status FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/esa_salmonid_listings', allow_moved_paths := true)
                                 GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- wdfw_salmonid_escapement
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Table exists and is readable'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Row count (source has ~26,000 estimate rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')
  )
);

-- state_abbr is constant 'WA' (WDFW covers Washington only)
INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state_abbr')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL stock_number, population_name, data_series_num, species, year, abundance_qty or data_type rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true)
      WHERE stock_number IS NULL OR population_name IS NULL OR data_series_num IS NULL
         OR species IS NULL OR year IS NULL OR abundance_qty IS NULL OR data_type IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T7_value_ranges',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'Negative abundance_qty or year outside 1930-2100'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true)
      WHERE abundance_qty < 0 OR year < 1930 OR year > 2100);

INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T7_species',
  CASE WHEN bad = 0 AND n >= 6 THEN 'pass' ELSE 'fail' END, n, 6,
  'Distinct species (expect >= 6); rows outside the known set: ' || bad
FROM (SELECT COUNT(DISTINCT species) AS n,
             COUNT(*) FILTER (WHERE species NOT IN ('Chinook','Coho','Chum','Pink','Sockeye','Steelhead','Bull Trout','Cutthroat')) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true));

-- Known fact: Discovery Bay Coho (stock 3273) 2023 trap-count escapement = 1778 (live Socrata row)
INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T7_discovery_bay_coho_2023',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1,
  'Stock 3273 year 2023 Escapement Fish rows with abundance_qty = 1778'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true)
      WHERE stock_number = 3273 AND year = 2023 AND data_type = 'Escapement Fish' AND abundance_qty = 1778);

-- The source repeats 13 keys with near-identical values; fail only if that grows materially
INSERT INTO dq_results
SELECT 'fisheries', 'wdfw_salmonid_escapement', 'T8_pk_duplicates',
  CASE WHEN n <= 25 THEN 'pass' ELSE 'fail' END, n, 25,
  'Duplicate stock x series x year x data_type x sub_population keys (source repeats 13)'
FROM (SELECT COUNT(*) AS n FROM (SELECT stock_number, data_series_num, year, data_type, sub_population_name
                                 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/wdfw_salmonid_escapement', allow_moved_paths := true)
                                 GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- MRIP recreational catch and effort
-- ─────────────────────────────────────────────────────────────

-- mrip_recreational_catch
INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Expected at least 20000 rows across the DQ years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true))
        WHERE null_percentage = 100.0 AND column_name NOT IN ('year', 'type'))
);

-- alt_flag is 0 for every standard estimate; status is FINAL for every complete year, so single-valued in a DQ scope of finished years
INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true))
        WHERE approx_unique <= 1 AND column_name NOT IN ('year', 'type', 'alt_flag', 'status'))
);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL in catch_year, wave, status, state_fips, state_abbr, species_code, species_name'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true)
      WHERE catch_year IS NULL OR wave IS NULL OR status IS NULL OR state_fips IS NULL OR state_abbr IS NULL OR species_code IS NULL OR species_name IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate catch_year, wave, state_fips, sub_region_code, mode_code, area_code, species_code keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT catch_year, wave, state_fips, sub_region_code, mode_code, area_code, species_code FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_year_matches_catch_year',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows where partition year <> catch_year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true) WHERE year <> catch_year);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_wave_range',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with wave outside 1-6'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true) WHERE wave NOT BETWEEN 1 AND 6);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_total_catch_identity',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows where landings_fish + released_fish differs from total_catch_fish by > 2 fish'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true)
      WHERE landings_fish IS NOT NULL AND released_fish IS NOT NULL AND total_catch_fish IS NOT NULL
        AND ABS(landings_fish + released_fish - total_catch_fish) > 2);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_nonneg_measures',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with a negative harvest, released, total catch or pounds estimate'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true)
      WHERE landings_fish < 0 OR released_fish < 0 OR total_catch_fish < 0 OR landings_pounds < 0);

-- Striped bass, Atlantic coast, the best loaded year: released fish (~20M in 2022) far exceed harvest (~0.5M)
INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_striped_bass_magnitude',
  CASE WHEN rel BETWEEN 2e6 AND 1e8 AND harv BETWEEN 1e5 AND 2e7 AND rel > harv THEN 'pass' ELSE 'fail' END,
  rel, 2e6, 'Max single-year striped bass released (expect 2M-100M) and harvested (expect 100K-20M), released > harvested'
FROM (SELECT MAX(r) AS rel, MAX(h) AS harv FROM (
        SELECT catch_year, SUM(released_fish) AS r, SUM(landings_fish) AS h FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true)
        WHERE species_name = 'STRIPED BASS' GROUP BY catch_year));

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_catch', 'T7_coastal_states_loaded',
  CASE WHEN n >= 12 THEN 'pass' ELSE 'fail' END, n, 12, 'Distinct state_abbr (expect >= 12 Atlantic/Gulf states)'
FROM (SELECT COUNT(DISTINCT state_abbr) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_catch', allow_moved_paths := true));

-- mrip_recreational_effort
INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T2_row_count',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'fail' END, n, 300, 'Expected at least 300 rows across the DQ years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true))
        WHERE null_percentage = 100.0 AND column_name NOT IN ('year', 'type'))
);

-- status is FINAL for every complete year, so single-valued in a DQ scope of finished years
INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true))
        WHERE approx_unique <= 1 AND column_name NOT IN ('year', 'type', 'status'))
);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL in catch_year, wave, status, state_fips, state_abbr'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true)
      WHERE catch_year IS NULL OR wave IS NULL OR status IS NULL OR state_fips IS NULL OR state_abbr IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate catch_year, wave, state_fips, sub_region_code, mode_code, area_code keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT catch_year, wave, state_fips, sub_region_code, mode_code, area_code FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T7_year_matches_catch_year',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows where partition year <> catch_year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) WHERE year <> catch_year);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T7_wave_range',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with wave outside 1-6'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) WHERE wave NOT BETWEEN 1 AND 6);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T7_nonneg_trips',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'Rows with negative angler_trips'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) WHERE angler_trips < 0);

INSERT INTO dq_results
SELECT 'fisheries', 'mrip_recreational_effort', 'T7_florida_trips_magnitude',
  CASE WHEN t BETWEEN 5e6 AND 2e8 THEN 'pass' ELSE 'fail' END, t, 5e6, 'Max single-year Florida angler trips (expect 5M-200M)'
FROM (SELECT MAX(s) AS t FROM (SELECT catch_year, SUM(angler_trips) AS s FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/mrip_recreational_effort', allow_moved_paths := true) WHERE state_abbr = 'FL' GROUP BY catch_year));

-- ─────────────────────────────────────────────────────────────
-- noaa_feus_economic_impacts
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true));

-- Source carries 1,152 commercial + 500 recreational impact rows
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T2_row_count',
  CASE WHEN n >= 1600 THEN 'pass' ELSE 'fail' END, n, 1600, 'Expected 1,652 rows (1,152 commercial + 500 recreational)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'feus_file')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true))
    -- geo_level (state / national) is exempt: SUMMARIZE's approximate distinct count reads a
    -- two-value column as one; T7_geo_levels checks it exactly.
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'feus_file', 'geo_level')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL industry, geography, geo_level, data_year, sector, row_level, impact_type or unit rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
      WHERE industry IS NULL OR geography IS NULL OR geo_level IS NULL OR data_year IS NULL
         OR sector IS NULL OR row_level IS NULL OR impact_type IS NULL OR unit IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T7_domains',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'Rows outside the expected industry / impact_type / row_level / imports_basis / data_year values'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
      WHERE industry NOT IN ('commercial', 'recreational')
         OR impact_type NOT IN ('jobs', 'sales', 'income', 'value_added')
         OR row_level NOT IN ('component', 'total', 'durable_expenditures')
         OR (industry = 'commercial' AND (imports_basis IS NULL OR imports_basis NOT IN ('with_imports', 'without_imports')))
         OR (industry = 'recreational' AND imports_basis IS NOT NULL)
         OR data_year NOT BETWEEN 2019 AND 2100
         OR (geo_level = 'state' AND state_abbr IS NULL)
         OR (geo_level = 'national' AND state_abbr IS NOT NULL));

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T7_geo_levels',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'Exact distinct geo_level values (state, national)'
FROM (SELECT COUNT(DISTINCT geo_level) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true));

-- Known fact (FEUS 2021): U.S. seafood industry supports 1,364,316 jobs with imports and 193,177,981 thousand dollars of sales
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T7_us_commercial_totals',
  CASE WHEN jobs = 1364316 AND sales = 193177981 THEN 'pass' ELSE 'fail' END, jobs, 1364316,
  'U.S. commercial Total Impacts with imports: jobs ' || jobs || ' (1364316), sales ' || sales || ' (193177981)'
FROM (SELECT COALESCE(SUM(impact_value) FILTER (WHERE impact_type = 'jobs'), 0) AS jobs,
             COALESCE(SUM(impact_value) FILTER (WHERE impact_type = 'sales'), 0) AS sales
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
      WHERE industry = 'commercial' AND geography = 'United States' AND sector = 'Total Impacts'
        AND imports_basis = 'with_imports');

-- Known fact (FEUS 2021): recreational fishing supports 106,057,480 thousand dollars of U.S. sales
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T7_us_recreational_sales',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1,
  'U.S. Total United States Economic Impacts sales = 106057480'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
      WHERE industry = 'recreational' AND geography = 'United States' AND impact_type = 'sales'
        AND sector = 'Total United States Economic Impacts' AND impact_value = 106057480);

-- State recreational trip modes sum to the state total (rounding aside) where every mode is disclosed
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T7_components_sum_to_total',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END, bad, 0,
  'Recreational state x measure groups whose For-Hire + Private Boat + Shore differs from the total by more than 5 (rounding)'
FROM (SELECT COUNT(*) AS bad FROM (
        SELECT geography, impact_type,
               SUM(impact_value) FILTER (WHERE row_level = 'component') AS comp,
               SUM(impact_value) FILTER (WHERE row_level = 'total') AS tot,
               COUNT(*) FILTER (WHERE row_level = 'component' AND impact_value IS NULL) AS nulls
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
        WHERE industry = 'recreational' AND geo_level = 'state'
        GROUP BY 1, 2)
      WHERE nulls = 0 AND tot IS NOT NULL AND ABS(comp - tot) > 5);

INSERT INTO dq_results
SELECT 'fisheries', 'noaa_feus_economic_impacts', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate industry x geography x sector x impact_type x imports_basis keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT industry, geography, sector, impact_type, imports_basis
                                 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_feus_economic_impacts', allow_moved_paths := true)
                                 GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- stock_smart_assessments
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Table exists and is readable'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Row count (Stock SMART export held 3,542 assessments)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')
  )
);

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL stock_id, stock_name, is_fssi, assessment_id or assessment_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true)
      WHERE stock_id IS NULL OR stock_name IS NULL OR is_fssi IS NULL
         OR assessment_id IS NULL OR assessment_year IS NULL);

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T7_year_range',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'assessment_year outside 1990-2030, or last_data_year more than a year after assessment_year (source has an assessment year 1993 and a 2004 assessment with 2005 data)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true)
      WHERE assessment_year NOT BETWEEN 1990 AND 2030 OR last_data_year > assessment_year + 1);

-- Both ratios and their inputs are present on enough rows to be usable
INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T7_ratios_populated',
  CASE WHEN b >= 1500 AND f >= 1500 THEN 'pass' ELSE 'fail' END, LEAST(b, f), 1500,
  'Rows with b_bmsy_ratio: ' || b || ', with f_fmsy_ratio: ' || f
FROM (SELECT COUNT(*) FILTER (WHERE b_bmsy_ratio IS NOT NULL) AS b,
             COUNT(*) FILTER (WHERE f_fmsy_ratio IS NOT NULL) AS f FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true));

-- Where F, Fmsy and F/Fmsy are all reported, the ratio is F / Fmsy (5% tolerance for rounding).
-- The source itself reports ~40-50 of ~2,000 such rows with a ratio computed from other inputs
-- (shark and tuna assessments); more than 80 means the columns have shifted.
INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T7_f_ratio_consistent',
  CASE WHEN bad <= 80 THEN 'pass' ELSE 'fail' END, bad, 80,
  'Rows where f_fmsy_ratio differs from estimated_f / fmsy by more than 5%'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true)
      WHERE estimated_f IS NOT NULL AND fmsy > 0 AND f_fmsy_ratio IS NOT NULL
        AND ABS(f_fmsy_ratio - estimated_f / fmsy) > 0.05 * (estimated_f / fmsy));

-- Known fact (source export): North Atlantic albacore, 2007 assessment: F/Fmsy 1.5, B/Bmsy 0.81
INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T7_albacore_2007',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1,
  'North Atlantic albacore 2007 assessment rows with F/Fmsy 1.5 and B/Bmsy 0.81'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true)
      WHERE stock_name = 'Albacore - North Atlantic' AND assessment_year = 2007
        AND ROUND(f_fmsy_ratio, 2) = 1.5 AND ROUND(b_bmsy_ratio, 2) = 0.81);

INSERT INTO dq_results
SELECT 'fisheries', 'stock_smart_assessments', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate assessment_id keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT assessment_id FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/stock_smart_assessments', allow_moved_paths := true) GROUP BY 1 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
