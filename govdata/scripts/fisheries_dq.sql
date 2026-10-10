-- dq-lookback: 1
-- Fisheries Data Quality Checks
-- Schema: fisheries
-- Tables: noaa_commercial_landings, dart_adult_counts, adfg_fish_counts, cdfw_grandtab_escapement, esa_salmonid_listings
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
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
