-- dq-lookback: 1
-- U.S. Disasters Data Quality Checks
-- Schema: disasters
-- Tables: disaster_declarations, public_assistance_projects, hazard_mitigation_projects,
--         nfip_claims, nfip_policies, nfip_multiple_loss_properties, storm_events,
--         wildfire_perimeters, ihp_housing_assistance_owners, ihp_housing_assistance_renters
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns 'type' and 'year'.
-- nfip_policies is capped by dqRowLimit (50k/year) in DQ mode, so its T2 threshold is a
-- per-year sample floor, not the true population.

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

CREATE TEMP TABLE dq_results (
  schema   VARCHAR,
  tbl      VARCHAR,
  test     VARCHAR,
  status   VARCHAR,
  value    DOUBLE,
  threshold DOUBLE,
  detail   VARCHAR
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: disaster_declarations
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'disaster_declarations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'disaster_declarations', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 declaration rows in window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'disaster_declarations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'disaster_declarations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'disaster_declarations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true) WHERE id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: public_assistance_projects
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'public_assistance_projects', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'public_assistance_projects', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 PA project rows in window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'public_assistance_projects', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'public_assistance_projects', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'public_assistance_projects', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL hash rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/public_assistance_projects', allow_moved_paths := true) WHERE hash IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: hazard_mitigation_projects
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'hazard_mitigation_projects', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'hazard_mitigation_projects', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20, 'Expected >=20 HMA project rows in window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'hazard_mitigation_projects', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'hazard_mitigation_projects', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'hazard_mitigation_projects', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL project_identifier rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/hazard_mitigation_projects', allow_moved_paths := true) WHERE project_identifier IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nfip_claims
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'nfip_claims', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_claims', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 claim rows in window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'nfip_claims', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_claims', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_claims', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_claims', allow_moved_paths := true) WHERE id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nfip_policies (dqRowLimit-sampled)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'nfip_policies', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_policies', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 policy rows in sampled window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'nfip_policies', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_policies', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_policies', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_policies', allow_moved_paths := true) WHERE id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nfip_multiple_loss_properties (snapshot, no year axis)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'nfip_multiple_loss_properties', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_multiple_loss_properties', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 repetitive-loss property rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'nfip_multiple_loss_properties', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_multiple_loss_properties', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true))
    -- boolean flag columns (nfip_rl/nfip_srl/fma_rl/fma_srl/etc.) legitimately have only 2
    -- distinct values; as_of_date is one value for the whole file (a single wholesale
    -- snapshot publish, not a per-row field) — exclude these from the single-value check
    -- rather than the whole table.
    WHERE approx_unique <= 1 AND column_name NOT IN (
      'type', 'post_firm_construction_indicator', 'primary_residence_indicator',
      'nfip_rl', 'nfip_srl', 'fma_rl', 'fma_srl', 'mitigated_indicator', 'insured_indicator',
      'as_of_date')));

INSERT INTO dq_results
SELECT 'disasters', 'nfip_multiple_loss_properties', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/nfip_multiple_loss_properties', allow_moved_paths := true) WHERE id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: ihp_housing_assistance_owners (snapshot, no year axis; full OpenFEMA dataset, not sampled)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T2_row_count',
  CASE WHEN n >= 150000 THEN 'pass' ELSE 'fail' END, n, 150000,
  'Expected >=150000 rows (OpenFEMA published 160625 on 2026-10-16)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name <> 'type'));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name <> 'type'));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T6_pk_nulls_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows plus duplicate id values'
FROM (SELECT (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true) WHERE id IS NULL)
           + (SELECT COUNT(*) FROM (SELECT id FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true) GROUP BY id HAVING COUNT(*) > 1)) AS n);

-- County FIPS comes from the declaration of the same disaster number and area name; rows
-- stay NULL only for tribal areas and area names no declaration lists. Measured 97.5 percent
-- resolved on 2026-10-16; fail below 95 percent.
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T7_county_fips_resolution',
  CASE WHEN share >= 0.95 THEN 'pass' ELSE 'fail' END, ROUND(share, 4), 0.95,
  'Share of rows with a county_fips (unresolved are tribal areas or unlisted area names)'
FROM (SELECT COUNT(county_fips) * 1.0 / COUNT(*) AS share FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T7_fips_shape',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows whose county_fips is not 5 digits or does not start with state_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true)
      WHERE county_fips IS NOT NULL
        AND (LENGTH(county_fips) <> 5 OR state_fips IS NULL OR SUBSTR(county_fips, 1, 2) <> state_fips));

-- Counts and dollars are non-negative; approvals and inspections cannot exceed the valid
-- registrations of the same row. (The approved total is NOT checked against its parts:
-- about 9 percent of FEMA's own rows differ by more than $1, and renters' damage degrees do
-- not sum to total_inspected in the source either.)
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows with a negative count/amount, or approved or inspected above valid registrations'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true)
      WHERE valid_registrations < 0 OR total_damage < 0 OR total_approved_ihp_amount < 0
         OR repair_replace_amount < 0 OR rental_amount < 0 OR other_needs_amount < 0
         OR approved_for_assistance > valid_registrations OR total_inspected > valid_registrations);

-- Known anchor: Hurricane Harvey (DR-4332) damaged owner-occupied homes in Harris County TX
-- (48201); FEMA inspected about 130,000 of them (130,520 on 2026-10-16).
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_owners', 'T7_known_value',
  CASE WHEN insp >= 100000 THEN 'pass' ELSE 'fail' END, insp, 100000,
  'Harvey DR-4332 Harris County TX owners inspected, expected >= 100000 (county_fips 48201)'
FROM (SELECT COALESCE(SUM(total_inspected), 0) AS insp
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_owners', allow_moved_paths := true)
      WHERE disaster_number = 4332 AND county_fips = '48201');

-- ─────────────────────────────────────────────────────────────
-- TABLE: ihp_housing_assistance_renters (snapshot, no year axis; full OpenFEMA dataset, not sampled)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T2_row_count',
  CASE WHEN n >= 140000 THEN 'pass' ELSE 'fail' END, n, 140000,
  'Expected >=140000 rows (OpenFEMA published 148157 on 2026-10-16)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name <> 'type'));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name <> 'type'));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T6_pk_nulls_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL id rows plus duplicate id values'
FROM (SELECT (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true) WHERE id IS NULL)
           + (SELECT COUNT(*) FROM (SELECT id FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true) GROUP BY id HAVING COUNT(*) > 1)) AS n);

-- County FIPS comes from the declaration of the same disaster number and area name; rows
-- stay NULL only for tribal areas and area names no declaration lists. Measured 97.5 percent
-- resolved on 2026-10-16; fail below 95 percent.
INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T7_county_fips_resolution',
  CASE WHEN share >= 0.95 THEN 'pass' ELSE 'fail' END, ROUND(share, 4), 0.95,
  'Share of rows with a county_fips (unresolved are tribal areas or unlisted area names)'
FROM (SELECT COUNT(county_fips) * 1.0 / COUNT(*) AS share FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T7_fips_shape',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows whose county_fips is not 5 digits or does not start with state_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true)
      WHERE county_fips IS NOT NULL
        AND (LENGTH(county_fips) <> 5 OR state_fips IS NULL OR SUBSTR(county_fips, 1, 2) <> state_fips));

INSERT INTO dq_results
SELECT 'disasters', 'ihp_housing_assistance_renters', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows with a negative count/amount, or approved or inspected above valid registrations'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/ihp_housing_assistance_renters', allow_moved_paths := true)
      WHERE valid_registrations < 0 OR total_approved_ihp_amount < 0
         OR repair_replace_amount < 0 OR rental_amount < 0 OR other_needs_amount < 0
         OR approved_for_assistance > valid_registrations OR total_inspected > valid_registrations);

-- ─────────────────────────────────────────────────────────────
-- TABLE: storm_events
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'storm_events', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'storm_events', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 storm event rows in window'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'storm_events', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'storm_events', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'storm_events', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL event_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/storm_events', allow_moved_paths := true) WHERE event_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: wildfire_perimeters (static snapshot)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 perimeter rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL incident_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true) WHERE incident_id IS NULL);

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_perimeters', 'T7_geometry_present',
  CASE WHEN pct >= 0.80 THEN 'pass' ELSE 'warn' END, pct, 0.80, 'Fraction of rows with non-null geometry_wkt'
FROM (SELECT AVG(CASE WHEN geometry_wkt IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_perimeters', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: wildfire_risk_by_county (static snapshot, new 5 Sep 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true));

-- 3,144 US counties/equivalents in the source workbook's Counties sheet
INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected ~3144 county rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true) WHERE county_fips IS NULL);

INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate county_fips rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT county_fips, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true)
  GROUP BY county_fips HAVING COUNT(*) > 1));

-- county_fips must be a well-formed 5-digit FIPS code (catches the GEOID
-- leading-zero-stripping bug the transformer works around via GEOIDFQ)
INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-5-digit) county_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true)
      WHERE county_fips NOT SIMILAR TO '[0-9]{5}');

-- exposure fractions and percentile ranks must fall in [0,1]
INSERT INTO dq_results
SELECT 'disasters', 'wildfire_risk_by_county', 'T7_fraction_bounds',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with an exposure fraction or rank outside [0,1]'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/wildfire_risk_by_county', allow_moved_paths := true)
      WHERE (buildings_fraction_minimal_exposure IS NOT NULL AND (buildings_fraction_minimal_exposure < 0 OR buildings_fraction_minimal_exposure > 1))
         OR (buildings_fraction_indirect_exposure IS NOT NULL AND (buildings_fraction_indirect_exposure < 0 OR buildings_fraction_indirect_exposure > 1))
         OR (buildings_fraction_direct_exposure IS NOT NULL AND (buildings_fraction_direct_exposure < 0 OR buildings_fraction_direct_exposure > 1))
         OR (burn_probability_state_rank IS NOT NULL AND (burn_probability_state_rank < 0 OR burn_probability_state_rank > 1))
         OR (burn_probability_national_rank IS NOT NULL AND (burn_probability_national_rank < 0 OR burn_probability_national_rank > 1))
         OR (risk_state_rank IS NOT NULL AND (risk_state_rank < 0 OR risk_state_rank > 1))
         OR (risk_national_rank IS NOT NULL AND (risk_national_rank < 0 OR risk_national_rank > 1)));

-- ─────────────────────────────────────────────────────────────
-- TABLE: fema_nri_earthquake_by_county (static snapshot, new 11 Sep 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true));

-- one row per county: 3,142 in the FEMA NRI layer (verified live at onboard)
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected ~3142 county rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true) LIMIT 3;

-- earthquake_events is excluded: FEMA's current NRI layer returns null for EVERY county
-- (verified live at onboard 2026-09-11 and again 2026-09-14 — ERQK_EVNTS is null even for
-- San Francisco/Los Angeles), so this column is expectedly all-null, not an ingestion gap.
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'earthquake_events')));

INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'earthquake_events')));

INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true) WHERE county_fips IS NULL);

INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate county_fips rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT county_fips, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true)
  GROUP BY county_fips HAVING COUNT(*) > 1));

-- county_fips must be a well-formed 5-digit FIPS code
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-5-digit) county_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true)
      WHERE county_fips NOT SIMILAR TO '[0-9]{5}');

-- rating vocabulary is NRI's 5-value scale; percentiles are 0-100
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T7_rating_domain',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'earthquake_risk_rating outside the NRI 5-value scale'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true)
      WHERE earthquake_risk_rating IS NOT NULL
        AND earthquake_risk_rating NOT IN ('Very Low', 'Relatively Low', 'Relatively Moderate', 'Relatively High', 'Very High'));

INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T7_percentile_bounds',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with an NRI percentile outside [0,100]'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true)
      WHERE (risk_national_pctl IS NOT NULL AND (risk_national_pctl < 0 OR risk_national_pctl > 100))
         OR (risk_state_pctl IS NOT NULL AND (risk_state_pctl < 0 OR risk_state_pctl > 100))
         OR (earthquake_eal_score IS NOT NULL AND earthquake_eal_score < 0));

-- known-exposure regression guard: San Francisco County CA sits on the San Andreas and
-- must rate Relatively High or above; Story County IA mid-continent must rate Very Low
-- (both verified live against the NRI at onboard)
INSERT INTO dq_results
SELECT 'disasters', 'fema_nri_earthquake_by_county', 'T7_known_exposure',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'San Francisco CA high + Story IA very-low anchor rows present'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/fema_nri_earthquake_by_county', allow_moved_paths := true)
      WHERE (county_fips = '06075' AND earthquake_risk_rating IN ('Relatively High', 'Very High'))
         OR (county_fips = '19169' AND earthquake_risk_rating = 'Very Low'));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
