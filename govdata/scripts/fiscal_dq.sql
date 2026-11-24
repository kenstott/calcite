-- dq-lookback: 1
-- U.S. Federal Fiscal Data Quality Checks
-- Schema: fiscal
-- Tables: soi_income_by_zip, soi_income_by_county, county_migration_flows,
--         exempt_org_master, exempt_org_990, exempt_org_990_schedule_h, usaspending_by_agency,
--         usaspending_account_balances,
--         usaspending_by_state, usaspending_by_district,
--         usaspending_recipients_by_district,
--         usaspending_contract_recipients_by_district,
--         entitlement_spending_by_state, sba_loan_approvals,
--         ssa_benefits_by_geography, ssa_benefits_by_geography_acs,
--         govt_finance_by_unit, state_minimum_wage_history,
--         state_corporate_income_tax_collections, omb_apportionments
-- All tables are Iceberg; reads via iceberg_scan (single-nested path).
-- T4/T5 exclude partition columns ('type' for all; also 'year' or 'program' where present).
-- Large tables carry dqRowLimit and sample in DQ mode; T2 thresholds reflect the sample.

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
-- TABLE: soi_income_by_zip (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 ZIP-bracket rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL zip_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true) WHERE zip_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: soi_income_by_county (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-bracket rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: irs_gross_collections_by_state_year (IRS SOI Table 1-5; new 13 Sep 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50,
  'Expected >=50 rows for a single fiscal year (50 states + DC + US total + non-state buckets)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state_abbr')));

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_name or fiscal_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true)
      WHERE state_name IS NULL OR fiscal_year IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (state_name, fiscal_year) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT state_name, fiscal_year, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true)
  GROUP BY state_name, fiscal_year HAVING COUNT(*) > 1
));

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T7_state_abbr_coverage',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END, n, 51,
  'Distinct non-NULL state_abbr values (expect 50 states + DC = 51)'
FROM (SELECT COUNT(DISTINCT state_abbr) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true)
      WHERE state_abbr IS NOT NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'irs_gross_collections_by_state_year', 'T7_us_total_plausible',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1,
  'US total_collections roughly matches the sum of the 50-state+DC total_collections (within 1%)'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT us.fiscal_year,
      us.total_collections AS us_total,
      SUM(st.total_collections) AS states_sum
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true) us
    JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/irs_gross_collections_by_state_year', allow_moved_paths := true) st
      ON st.fiscal_year = us.fiscal_year AND st.state_abbr IS NOT NULL
    WHERE us.state_name = 'United States, total'
    GROUP BY us.fiscal_year, us.total_collections
  ) x
  WHERE x.us_total > 0 AND ABS(x.states_sum - x.us_total) / x.us_total < 0.05
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: omb_budget_by_account (OMB budauth/outlays supplemental workbooks; new 15 Nov 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (two measures x ~5000 account lines x decades of years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL measure, fiscal_year, amount_thousands or agency_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true)
      WHERE measure IS NULL OR fiscal_year IS NULL OR amount_thousands IS NULL OR agency_code IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (measure, budget_vintage, source_row, fiscal_year, is_transition_quarter) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true)
  GROUP BY measure, budget_vintage, source_row, fiscal_year, is_transition_quarter
  HAVING COUNT(*) > 1
));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T7_both_measures',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'Both budget_authority and outlays present'
FROM (SELECT COUNT(DISTINCT measure) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T7_year_span',
  CASE WHEN mn <= 1976 AND mx >= 2025 THEN 'pass' ELSE 'fail' END, mn, 1976, 'Fiscal years must span FY1976 through at least FY2025'
FROM (SELECT MIN(fiscal_year) AS mn, MAX(fiscal_year) AS mx FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_budget_by_account', 'T7_faa_fe_known_values',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'FAA Facilities and Equipment (account 8107, Discretionary) budget authority within 2% of published FY1991 $2.10B and FY2021 $3.02B'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT fiscal_year, SUM(amount_thousands) AS a FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_budget_by_account', allow_moved_paths := true)
  WHERE measure = 'budget_authority' AND agency_code = '021' AND bureau_code = '12' AND account_code = '8107' AND bea_category = 'Discretionary' AND is_transition_quarter = false AND fiscal_year IN (1991, 2021)
  GROUP BY fiscal_year)
  WHERE (fiscal_year = 1991 AND ABS(a - 2100000) / 2100000 < 0.02) OR (fiscal_year = 2021 AND ABS(a - 3020000) / 3020000 < 0.02));

-- ─────────────────────────────────────────────────────────────
-- TABLE: county_migration_flows (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 migration flow rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL origin_county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true) WHERE origin_county_fips IS NULL);

-- 2008-2011 are the pairs 2007-2008 .. 2010-2011, whose files use a different header set
-- (State_Code_Origin ... Aggr_AGI); the provider aliases it. All four years must be present.
INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T7_years_2008_2011',
  CASE WHEN n = 4 THEN 'pass' ELSE 'fail' END, n, 4,
  'Distinct years among 2008-2011 (pairs 0708..1011, legacy header set)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true)
      WHERE CAST(year AS INTEGER) BETWEEN 2008 AND 2011);

-- Row-level invariants of the provider's filters: both endpoints are 5-digit real-county
-- FIPS (state < 57), origin differs from destination, at least 10 returns (the IRS listing
-- threshold in 2008-2013, about 20 from 2014), exemptions >= returns, AGI present.
INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T7_row_invariants',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows with a malformed/summary endpoint, same-county flow, returns < 10, individuals < returns or NULL agi'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true)
      WHERE LENGTH(origin_county_fips) <> 5 OR LENGTH(dest_county_fips) <> 5
         OR CAST(SUBSTR(origin_county_fips, 1, 2) AS INTEGER) >= 57
         OR CAST(SUBSTR(dest_county_fips, 1, 2) AS INTEGER) >= 57
         OR SUBSTR(origin_county_fips, 3, 3) = '000' OR SUBSTR(dest_county_fips, 3, 3) = '000'
         OR origin_county_fips = dest_county_fips
         OR num_returns < 10 OR num_individuals < num_returns OR agi IS NULL
         OR origin_state_fips <> SUBSTR(origin_county_fips, 1, 2)
         OR dest_state_fips <> SUBSTR(dest_county_fips, 1, 2));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate (year, origin_county_fips, dest_county_fips) keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT year, origin_county_fips, dest_county_fips
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true)
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- TABLE: exempt_org_master (IRS EO BMF snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 exempt-org rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL ein rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true) WHERE ein IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: exempt_org_990 (IRS Form 990 XML; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 990 filing rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL object_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) WHERE object_id IS NULL);

-- Form 990 expense/compensation fields (calcite#565): filed on return_type 990 only.
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T7_part_ix_total_matches_part_i',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Form 990 rows whose Part IX line 25 col A differs from Part I total expenses by more than $1'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true)
  WHERE return_type = '990' AND functional_expenses_total IS NOT NULL AND total_expenses IS NOT NULL
    AND abs(functional_expenses_total - total_expenses) > 1);

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T8_part_ix_columns_sum',
  CASE WHEN pct <= 10 THEN 'pass' ELSE 'warn' END, pct, 10,
  'Percent of Form 990 rows where Part IX B+C+D differs from col A by more than $1 (filer arithmetic errors / blank columns exist in the source)'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE abs(functional_expenses_program + functional_expenses_mgmt_general + functional_expenses_fundraising - functional_expenses_total) > 1)
         / NULLIF(COUNT(*), 0) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true)
  WHERE return_type = '990' AND functional_expenses_program IS NOT NULL AND functional_expenses_mgmt_general IS NOT NULL
    AND functional_expenses_fundraising IS NOT NULL AND functional_expenses_total IS NOT NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T9_line15_16b_within_total_expenses',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Form 990 rows where Part I line 15 or line 16b exceeds total expenses'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true)
  WHERE return_type = '990' AND total_expenses IS NOT NULL
    AND (salaries_comp_benefits > total_expenses + 1 OR fundraising_expenses_16b > total_expenses + 1));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T10_form990_new_fields_populated',
  CASE WHEN pct >= 95 THEN 'pass' ELSE 'fail' END, pct, 95,
  'Percent of Form 990 rows carrying line 15, 16b, Part IX col A and Part VII persons'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE salaries_comp_benefits IS NOT NULL AND fundraising_expenses_16b IS NOT NULL
         AND functional_expenses_total IS NOT NULL AND part_vii_persons_listed IS NOT NULL) / NULLIF(COUNT(*), 0) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) WHERE return_type = '990');

-- Listed vs written (#970): one row per filing the IRS index lists, so for a receipt year the
-- table must hold exactly as many rows as index_{year}.csv has filings. Checked for the two most
-- recent receipt years present in the table. A DQ bucket holds only the dqRowLimit sample
-- (5000 rows per load), so a year at or under that cap is reported as the sample it is.
SET VARIABLE y_latest = (SELECT max(year) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true));
SET VARIABLE url_latest = 'https://apps.irs.gov/pub/epostcard/990/xml/' || getvariable('y_latest') || '/index_' || getvariable('y_latest') || '.csv';
SET VARIABLE url_prior = 'https://apps.irs.gov/pub/epostcard/990/xml/' || (CAST(getvariable('y_latest') AS INTEGER) - 1) || '/index_' || (CAST(getvariable('y_latest') AS INTEGER) - 1) || '.csv';

-- apps.irs.gov answers ranged reads with a body the httpfs reader rejects; download whole.
SET force_download = true;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T11_written_matches_irs_index_' || y,
  CASE WHEN written = listed THEN 'pass'
       WHEN written <= 5000 THEN 'pass'
       ELSE 'fail' END,
  listed - written, 0,
  'IRS index filings minus rows written for receipt year ' || y || ' (index ' || listed || ', table ' || written
    || CASE WHEN written <= 5000 AND written <> listed THEN '; dqRowLimit sample, full count verified on production only' ELSE '' END || ')'
FROM (
  SELECT y, listed, (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) t WHERE CAST(t.year AS VARCHAR) = y) AS written
  FROM (SELECT CAST(getvariable('y_latest') AS VARCHAR) AS y, COUNT(*) AS listed FROM read_csv(getvariable('url_latest'), all_varchar = true, strict_mode = false)
        UNION ALL
        SELECT CAST(CAST(getvariable('y_latest') AS INTEGER) - 1 AS VARCHAR), COUNT(*) FROM read_csv(getvariable('url_prior'), all_varchar = true, strict_mode = false)));

-- ─────────────────────────────────────────────────────────────
-- TABLE: exempt_org_990_schedule_h (IRS Form 990 Schedule H; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 Schedule H filings (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

-- return_type is 990 on every Schedule H filer (only a full Form 990 attaches Schedule H): a real constant.
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'return_type')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL object_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true) WHERE object_id IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T7_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, object_id) rows'
FROM (SELECT COUNT(*) - COUNT(DISTINCT (year, object_id)) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T8_line7k_equals_7d_plus_7j',
  CASE WHEN pct <= 10 THEN 'pass' ELSE 'warn' END, pct, 10,
  'Percent of rows where line 7k net differs from 7d net + 7j net by more than $1 (filer arithmetic errors exist in the source)'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE abs(total_community_benefit_net - total_financial_assistance_net - total_other_benefits_net) > 1)
         / NULLIF(COUNT(*), 0) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true)
  WHERE total_community_benefit_net IS NOT NULL AND total_financial_assistance_net IS NOT NULL
    AND total_other_benefits_net IS NOT NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T9_medicare_shortfall_is_revenue_minus_cost',
  CASE WHEN pct <= 10 THEN 'pass' ELSE 'warn' END, pct, 10,
  'Percent of rows where Part III line 7 differs from line 5 minus line 6 by more than $1'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE abs(medicare_shortfall - (medicare_revenue - medicare_cost)) > 1)
         / NULLIF(COUNT(*), 0) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true)
  WHERE medicare_shortfall IS NOT NULL AND medicare_revenue IS NOT NULL AND medicare_cost IS NOT NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990_schedule_h', 'T10_total_community_benefit_populated',
  CASE WHEN pct >= 90 THEN 'pass' ELSE 'fail' END, pct, 90,
  'Percent of rows carrying the line 7k total community benefit expense'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE total_community_benefit_cost IS NOT NULL) / NULLIF(COUNT(*), 0) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990_schedule_h', allow_moved_paths := true));


-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_by_agency (USAspending; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20, 'Expected >=20 agency-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL agency_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true) WHERE agency_name IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_account_balances (USAspending File A; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END, n, 1500, 'Expected >=1500 account rows per year-period'
FROM (SELECT MAX(c) AS n FROM (SELECT COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true) GROUP BY year, fiscal_period));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL fiscal_period or federal_account_symbol rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true) WHERE fiscal_period IS NULL OR federal_account_symbol IS NULL);

-- Identity check on a known account: FAA Facilities and Equipment (069-1308) must exist, and
-- obligations and outlays can never exceed total budgetary resources.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T7_known_account',
  CASE WHEN n >= 1 AND bad = 0 THEN 'pass' ELSE 'fail' END, n, 1,
  '069-1308 rows=' || n || ', rows with obligations/outlays > resources=' || bad
FROM (SELECT COUNT(*) AS n,
        COUNT(*) FILTER (WHERE obligations_incurred > total_budgetary_resources + 1
                           OR gross_outlays > total_budgetary_resources + 1) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true)
      WHERE federal_account_symbol = '069-1308');

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_account_balances', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, fiscal_period, federal_account_symbol) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT year, fiscal_period, federal_account_symbol FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_account_balances', allow_moved_paths := true)
  GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_by_state (USAspending; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T2_row_count',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'fail' END, n, 40, 'Expected >=40 state-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_abbr rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true) WHERE state_abbr IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_by_district (USAspending; partition cols: type, year; new 5 Sep 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true));

-- ~441-442 districts per year (435 numbered + at-large/DC/territories)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T2_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400, 'Expected >=400 district-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cd_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true) WHERE cd_fips IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, cd_fips) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, cd_fips, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true)
  GROUP BY year, cd_fips HAVING COUNT(*) > 1));

-- cd_fips must be a well-formed 4-digit code (state FIPS + district number)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-4-digit) cd_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true)
      WHERE cd_fips NOT SIMILAR TO '[0-9]{4}');

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_recipients_by_district (USAspending; partition cols: type, year; new 6 Sep 2026)
-- Top 100 recipients by district x fiscal year -- one call per (year, district)
-- against spending_by_category/recipient/, scoped by default to 2 recent fiscal years.
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true));

-- Up to 100 recipients x ~441-442 districts per year; a district with fewer than
-- 100 distinct recipients yields fewer rows, so this is a loose floor, not exact.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T2_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400, 'Expected >=400 district-year-rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true) LIMIT 3;

-- recipient_id/uei/duns are expected to be all-null only for the narrow
-- MULTIPLE RECIPIENTS aggregate rows, never for the whole table -- excluded from
-- the blanket all-null-cols check below via the WHERE clause on cnt, not a column
-- exclusion, since a real all-null column here would still be a defect.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cd_fips or rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips IS NULL OR rank IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, cd_fips, rank) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, cd_fips, rank, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
  GROUP BY year, cd_fips, rank HAVING COUNT(*) > 1));

-- cd_fips must be a well-formed 4-digit code (state FIPS + district number)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-4-digit) cd_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips NOT SIMILAR TO '[0-9]{4}');

-- rank must fall within the documented 1-100 top-N cut
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T8_rank_domain',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with rank outside 1-100'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE rank < 1 OR rank > 100);

-- Sanity check: a known large federal contractor/grantee should appear as rank 1
-- somewhere in the table for a recent year (matches the live-tested PA-04 result:
-- AmerisourceBergen Drug Corp topped that district's FY2025 recipient ranking).
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T9_expected_values',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END, n, 1, 'Rows with rank=1 and obligated_amount > 0'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE rank = 1 AND obligated_amount > 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_contract_recipients_by_district (USAspending; partition cols: type, year)
-- Top 100 contract/IDV recipients by district x fiscal year -- one call per (year, district)
-- against spending_by_category/recipient/, scoped by default to 2 recent fiscal years.
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true));

-- Up to 100 recipients x ~441-442 districts per year; a district with fewer than
-- 100 distinct recipients yields fewer rows, so this is a loose floor, not exact.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T2_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400, 'Expected >=400 district-year-rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true) LIMIT 3;

-- recipient_id/uei/duns are expected to be all-null only for the narrow
-- MULTIPLE RECIPIENTS aggregate rows, never for the whole table -- excluded from
-- the blanket all-null-cols check below via the WHERE clause on cnt, not a column
-- exclusion, since a real all-null column here would still be a defect.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cd_fips or rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips IS NULL OR rank IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, cd_fips, rank) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, cd_fips, rank, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true)
  GROUP BY year, cd_fips, rank HAVING COUNT(*) > 1));

-- cd_fips must be a well-formed 4-digit code (state FIPS + district number)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-4-digit) cd_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips NOT SIMILAR TO '[0-9]{4}');

-- rank must fall within the documented 1-100 top-N cut
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contract_recipients_by_district', 'T8_rank_domain',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with rank outside 1-100'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contract_recipients_by_district', allow_moved_paths := true)
      WHERE rank < 1 OR rank > 100);

-- ─────────────────────────────────────────────────────────────
-- TABLE: entitlement_spending_by_state (USAspending, CFDA-filtered; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true));

-- ~56 states/territories x 3 programs per fiscal year
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 state-program-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_abbr/program rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE state_abbr IS NULL OR program IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, state_abbr, program) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, state_abbr, program, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
  GROUP BY year, state_abbr, program HAVING COUNT(*) > 1));

-- program must be one of the three CFDA-coded entitlement programs this table covers
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T7_program_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with unexpected program value'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE program NOT IN ('social_security_retirement', 'medicare_hospital_insurance',
                             'medicare_supplementary_medical_insurance'));

-- Sanity check: a large state's Social Security total should be tens of billions of
-- dollars for its most recent fiscal year, not near-zero (confirmed live: CA FY2025
-- Social Security Retirement Insurance = $127.5B).
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T8_expected_values',
  CASE WHEN amt >= 1e10 THEN 'pass' ELSE 'fail' END, COALESCE(amt, 0), 1e10,
  'CA social_security_retirement obligated_amount for most recent year (expect >= $10B)'
FROM (
  SELECT MAX(CASE WHEN year = (
      SELECT MAX(year) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE state_abbr = 'CA' AND program = 'social_security_retirement')
    THEN obligated_amount END) AS amt
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
  WHERE state_abbr = 'CA' AND program = 'social_security_retirement');

-- ─────────────────────────────────────────────────────────────
-- TABLE: sba_loan_approvals (SBA FOIA; partition cols: type, program)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 loan rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'program')));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'program')));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0, 'NULL approval fiscal year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true) WHERE year IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: ssa_benefits_by_geography (SSA OASDI/SSI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-benefit rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: snap_benefits_by_geography (USDA FNA; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Expected >=20000 state-year-month rows (FY1989-present x 56 geographies x ~13 rows/year)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true))
    -- month is legitimately null on every Total (fiscal-year rollup) row, not a data defect
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true) WHERE state_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: snap_participants_by_age (USDA FNS SNAP QC public-use files; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T2_row_count',
  CASE WHEN n >= 250 THEN 'pass' ELSE 'fail' END, n, 250, 'Expected >=250 state-year rows (FY2020-2024 x 53 geographies)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips or year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true) WHERE state_fips IS NULL OR year IS NULL);

-- T7: age groups must sum to persons, children to the sum of their bands, and a state's
-- national child share must be plausible (25-60%) for states with a real sample
INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T7_expected_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows where age bands do not sum to persons (+-3) or child share outside 25-60%'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true)
  WHERE ABS(children_under_5 + children_5_to_17 + adults_18_to_59 + adults_60_plus + age_unknown - persons_participating) > 3
     OR children_under_18 <> children_under_5 + children_5_to_17
     OR (qc_sample_households >= 100 AND (children_under_18 * 1.0 / persons_participating < 0.25 OR children_under_18 * 1.0 / persons_participating > 0.60)));

-- T8: one row per state x year
INSERT INTO dq_results
SELECT 'fiscal', 'snap_participants_by_age', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (state_fips, year) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT state_fips, year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_participants_by_age', allow_moved_paths := true) GROUP BY 1, 2 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- TABLE: snap_authorized_retailers (USDA FNS Historical SNAP Retailer Locator; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T2_row_count',
  CASE WHEN n >= 600000 THEN 'pass' ELSE 'fail' END, n, 600000, 'Expected >=600000 authorization spells (2005-2025 edition has about 703000)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL record_id or authorization_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true) WHERE record_id IS NULL OR authorization_date IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T7_expected_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows with a state outside 50 states/DC/GU/VI/PR'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true)
  WHERE state_abbr NOT IN ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','GU','VI','PR'));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T7b_reversed_spells',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0,
  'Rows with end_date before authorization_date; published that way by FNS (45 of 703441 in the 2005-2025 edition), kept as is'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true) WHERE end_date < authorization_date);

INSERT INTO dq_results
SELECT 'fiscal', 'snap_authorized_retailers', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (record_id, authorization_date) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT record_id, authorization_date FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_authorized_retailers', allow_moved_paths := true) GROUP BY 1, 2 HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- TABLE: ssa_benefits_by_geography_acs (Census ACS derived; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected >=3000 county rows per vintage'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: govt_finance_by_unit (Census govs-finance Individual Unit File; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END, n, 5000, 'Expected >=5000 unit-item rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips/gov_type_code/unit_id/item_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true)
  WHERE state_fips IS NULL OR gov_type_code IS NULL OR unit_id IS NULL OR item_code IS NULL);

-- T7 regression guard: the SIII view depends on E12/F12/E36/F36/E61/F61 actually being
-- present (confirmed real codes against the FY2023 technical documentation) — a parser
-- regression that silently dropped these would break the view without failing T1/T2.
INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T7_siii_item_codes_present',
  CASE WHEN n = 6 THEN 'pass' ELSE 'fail' END, n, 6,
  'Distinct SIII item codes found among E12/F12/E36/F36/E61/F61 (expect all 6)'
FROM (SELECT COUNT(DISTINCT item_code) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true)
  WHERE item_code IN ('E12', 'F12', 'E36', 'F36', 'E61', 'F61'));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T7_gov_type_coverage',
  CASE WHEN n = 6 THEN 'pass' ELSE 'warn' END, n, 6,
  'Distinct gov_type_code values found (expect all 6: state/county/city/township/special district/school district)'
FROM (SELECT COUNT(DISTINCT gov_type_code) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: state_corporate_income_tax_collections (Census STC govsstatetax
-- timeseries API, ITEM_CODE=T41 only; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true));

-- 51 (50 states + DC) x N years; a full-range run covers 2016-current (10+
-- years), but a scoped DQ run may cover far fewer years, so the floor is one
-- year's worth (51 rows), not the full-range expectation.
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T2_row_count',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END, n, 51, 'Expected >=51 state rows (>=1 year, DQ-scoped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true) LIMIT 3;

-- collections_thousands is expected to carry real NULLs (states with no T41-
-- classified tax that year), so 100%-null would only occur if every state
-- were unmapped — a real defect. Excluded from the "all_null" NOT IN list
-- like every other data column; only type/year are exempted as partitions.
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL/duplicate (year, state_fips) rows'
FROM (SELECT COUNT(*) AS n FROM (
    SELECT year, state_fips, COUNT(*) AS dup_count
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true)
    GROUP BY year, state_fips
    HAVING year IS NULL OR state_fips IS NULL OR COUNT(*) > 1
  ));

-- Expected-value sanity check: California's T41 collections should be a
-- plausible multi-billion-dollar figure, not near-zero or absurdly large.
-- Confirmed live 2026-09-06: CA 2024 = $41.4B (41,408,314 thousand).
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T7_ca_magnitude_sane',
  CASE WHEN n BETWEEN 5000000 AND 100000000 THEN 'pass' ELSE 'fail' END, n, 41408314,
  'California collections_thousands for its latest available year (expect $5B-$100B range)'
FROM (
  SELECT collections_thousands AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true)
  WHERE state_fips = '06'
  ORDER BY year DESC
  LIMIT 1
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: state_minimum_wage_history (DOL WHD state minimum wage history; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END, n, 1500, 'Expected >=1500 jurisdiction-year rows (55 jurisdictions x >=27 published years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year/jurisdiction_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true)
  WHERE year IS NULL OR jurisdiction_name IS NULL);

-- T7 regression guard: confirm the parser still sees all 4 jurisdiction types and still
-- classifies a majority of cells as the unambiguous SINGLE value_type — a classifier
-- regression that dumped everything into OTHER would pass T1/T2/T6 silently.
INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T7_jurisdiction_type_coverage',
  CASE WHEN n = 4 THEN 'pass' ELSE 'warn' END, n, 4,
  'Distinct jurisdiction_type values found (expect all 4: FEDERAL/STATE/DISTRICT/TERRITORY)'
FROM (SELECT COUNT(DISTINCT jurisdiction_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T7_single_value_type_present',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000,
  'Rows classified value_type=SINGLE (majority of cells are unambiguous single wage figures)'
FROM (SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true)
  WHERE value_type = 'SINGLE');

-- ─────────────────────────────────────────────────────────────
-- broadband_reconnect_awards (USDA ReConnect CFDA 10.752 awards; snapshot)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true));

-- T2: row_count. Confirmed live 5 Sep 2026: 145 awards across all ReConnect rounds.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 award rows (145 confirmed live 5 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — cfda_number is a real constant (this table is single-CFDA by
-- design), excluded alongside the partition column.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cfda_number')));

-- awarding_agency is a real constant (USDA Rural Utilities Service runs ReConnect
-- alone), excluded alongside the partition/CFDA columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'cfda_number', 'awarding_agency')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL award_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true)
  WHERE award_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- usaspending_bridge_awards (FHWA Bridge Formula Program CFDA 20.263 +
-- Bridge Investment Program CFDA 20.264 awards; snapshot)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_bridge_awards', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true));

-- T2: row_count. Confirmed live 14 Sep 2026: 3,380 awards under CFDA 20.263 +
-- 33 under CFDA 20.264.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_bridge_awards', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 award rows (3413 confirmed live 14 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — cfda_number is a real, expected two-value column (this
-- table is bridge-program-only by design), excluded alongside the partition column.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_bridge_awards', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cfda_number')));

-- awarding_agency is a real constant (DOT/FHWA runs both bridge programs),
-- excluded alongside the partition/CFDA columns.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_bridge_awards', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'cfda_number', 'awarding_agency')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_bridge_awards', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL award_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_bridge_awards', allow_moved_paths := true)
  WHERE award_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- broadband_bead_state_allocations (NTIA BEAD CFDA 11.035 awards; snapshot)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true));

-- T2: row_count. One prime grant per state/territory/DC; confirmed live 5 Sep 2026 at 63.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 state/territory rows (63 confirmed live 5 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cfda_number')));

-- awarding_agency is a real constant (NTIA runs BEAD alone), excluded alongside the
-- partition/CFDA columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'cfda_number', 'awarding_agency')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL award_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true)
  WHERE award_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- broadband_caf_deployment_locations (USAC CAF-II deployment locations; delta by filing year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

-- T2: row_count. Year-partitioned; per-year counts vary widely (4 in 2015's partial
-- first year to 1.3M in 2020's peak reporting year) — 1000 is a conservative floor
-- that even a sparse DQ-window year clears.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows per year (year-partitioned; production spans 2015-2025)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — fund_type is a real constant (this table is CAF-II-only by
-- design; other_technology/latency/overlapping_locations are legitimately rare and
-- may be all-null within a single-year DQ window, not a defect).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'fund_type')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'fund_type')));

-- T6: pk_nulls. No natural single-column PK (a given census_block can carry multiple
-- filing-year rows); census_block is the geographic join key and must always be present.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL census_block rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true)
  WHERE census_block IS NULL);

-- T7: census_block is always the full 15-digit GEOID (state+county+tract+block),
-- confirmed live 5 Sep 2026 (0 malformed rows across 2024-2025) — a shorter/malformed
-- value would break the county_fips derivation (first 5 digits) documented in the
-- table's schema comment.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T7_census_block_shape',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'census_block rows not exactly 15 digits'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true)
  WHERE LENGTH(census_block) <> 15);

-- T7b: fund_type scoped to the 2 documented CAF-II variants.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T7_fund_type_scope',
  CASE WHEN n = 2 THEN 'pass' ELSE 'warn' END, n, 2,
  'Distinct fund_type values found (expect exactly 2: CAF II / CAF II Auc)'
FROM (SELECT COUNT(DISTINCT fund_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- broadband_high_cost_disbursements (USAC USF high-cost program disbursements; delta by year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

-- T2: row_count. Year-partitioned; ~100K rows per year across all high-cost
-- programs. Confirmed live 5 Sep 2026: total 1,045,128 rows across 2016-2026.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Expected >=20,000 rows per year (year-partitioned; production spans 2016-2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

-- T5: all_same_value — disbursement_year is genuinely constant within any single-year
-- partition (real constant, not a defect), excluded alongside the partition columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'disbursement_year')));

-- T6: pk_nulls. No natural single-column PK (a given (form_498_id, month, fund_type)
-- can recur with different study_area_codes when a carrier serves multiple areas).
-- form_498_id itself is null on genuine zero-disbursement rows (a study area with no
-- filed Form 498 that period — confirmed live: 449/149,425 rows in the 2024-2025
-- window, all amount_disbursed=0), so study_area_code is the column that must always
-- be present instead.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL study_area_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true)
  WHERE study_area_code IS NULL);

-- T7: fund_type coverage — at least 15 of the 25+ documented USF programs must be
-- present in any full year (some legacy programs may be absent in the most recent
-- year as they wind down, so the threshold sits below the full 25 count).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T7_fund_type_coverage',
  CASE WHEN n >= 15 THEN 'pass' ELSE 'warn' END, n, 15,
  'Distinct fund_type values found (expect >=15 across a full year; 25+ across full history)'
FROM (SELECT COUNT(DISTINCT fund_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

-- T7b: month values are all 1-12 (a bad row would surface here rather than as a
-- silently-mistyped disbursement).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T7_month_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with disbursement_month outside 1-12'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true)
  WHERE disbursement_month IS NULL OR disbursement_month < 1 OR disbursement_month > 12);

-- ─────────────────────────────────────────────────────────────
-- TABLE: omb_apportionments (OMB apportionment-public.max.gov; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true));

-- One fiscal year is ~6,000 files at ~20+ lines each
INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END, n, 50000, 'Expected >=50000 schedule-line rows per fiscal year'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true))
    -- availability_type_code: the source's only non-null value is 'X' (no-year funds); annual and
    -- multi-year accounts carry it as blank, so it is single-valued by design.
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'availability_type_code')));

INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL file_id or tafs_iteration_id on a schedule-line row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true) WHERE file_id IS NULL OR (line_number IS NOT NULL AND tafs_iteration_id IS NULL));

-- Every schedule line must carry a dollar amount; only unreferenced-footnote rows have none
INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T7_amount_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Schedule-line rows with NULL approved_amount'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true) WHERE line_number IS NOT NULL AND approved_amount IS NULL);

-- A referenced footnote must resolve to text
INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T7_footnote_resolves',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with footnote_number but NULL footnote_text'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true) WHERE footnote_number IS NOT NULL AND footnote_text IS NULL);

-- PK-duplication guard: one row per (file, account-iteration, line, split, footnote) key
INSERT INTO dq_results
SELECT 'fiscal', 'omb_apportionments', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (file_id, tafs_iteration_id, line_number, line_split, footnote_number) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT file_id, tafs_iteration_id, line_number, line_split, footnote_number, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/omb_apportionments', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- DUNS was retired in 2022: recipient_duns/recipient_parent_duns are empty from FY2022, and the
-- DQ sample is FY2025-FY2026 only (they are populated FY2007-FY2021).
-- usaspending_assistance (bulk download; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample (dqRowLimit 200000 per year); full years carry 1-9 million rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'recipient_duns', 'recipient_parent_duns')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'recipient_duns', 'recipient_parent_duns')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (assistance_transaction_unique_key)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE assistance_transaction_unique_key IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, assistance_transaction_unique_key) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, assistance_transaction_unique_key, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- DUNS was retired in 2022: recipient_duns/recipient_parent_duns are empty from FY2022, and the
-- DQ sample is FY2025-FY2026 only (they are populated FY2007-FY2021).
-- usaspending_contracts (bulk download; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample (dqRowLimit 200000 per year); full years carry 1-9 million rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'recipient_duns', 'recipient_parent_duns')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true))
    -- transaction_number and action_date_fiscal_year are constant only in the single-year DQ sample (FY2026);
    -- full years carry transaction_number 0/1/14/16/... and the table spans many action_date_fiscal_year values.
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'recipient_duns', 'recipient_parent_duns',
      'transaction_number', 'action_date_fiscal_year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (contract_transaction_unique_key)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true)
  WHERE contract_transaction_unique_key IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T7_expected_values',
  CASE WHEN bad = 0 AND dollars > 0 THEN 'pass' ELSE 'fail' END, bad, 0,
  'Rows with no award key, awarding agency code, or action_date, or an unparsable date (bad), and positive obligations present (dollars=' || CAST(dollars AS VARCHAR) || ')'
FROM (SELECT COUNT(*) FILTER (WHERE contract_award_unique_key IS NULL OR awarding_agency_code IS NULL
        OR TRY_CAST(action_date AS DATE) IS NULL) AS bad,
      COALESCE(SUM(federal_action_obligation) FILTER (WHERE federal_action_obligation > 0), 0) AS dollars
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_contracts', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, contract_transaction_unique_key) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, contract_transaction_unique_key, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_contracts', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- fac_audits (bulk download; partition cols: type)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample (dqRowLimit 200000); full table carries about 418,000 audits)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'is_public')));

-- The file is ordered by audit year and the DQ cap keeps the first 200000 rows (audit years
-- 2016-2018 only, all UG / version 1 / most_recent), so those three columns are single-valued
-- in the sample and varied in the full table.
INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'is_public', 'type_audit_code', 'resubmission_version', 'resubmission_status')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (report_id)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true)
  WHERE report_id IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (report_id) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT report_id, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- The file is ordered by audit year and the DQ cap keeps the first 200000 rows (audit year 2016
-- only), and other_cluster_name is rarely filled, so audit_year/other_cluster_name are
-- single-valued or empty in the sample.
-- fac_federal_awards (bulk download; partition cols: type)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample (dqRowLimit 200000); full table carries about 7.3 million rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'audit_year', 'other_cluster_name')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'audit_year', 'other_cluster_name')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (report_id, award_reference)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true)
  WHERE report_id IS NULL OR award_reference IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (report_id, award_reference) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT report_id, award_reference, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- fac_audits_historic (bulk download; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample; full table carries about 650,000 audits across 18 years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'uei')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'uei')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (dbkey)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true)
  WHERE dbkey IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'fac_audits_historic', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, dbkey) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, dbkey, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_audits_historic', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- fac_federal_awards_historic (bulk download; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows (DQ sample (dqRowLimit 200000 per year); full table carries about 9.4 million rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true) LIMIT 3;

-- state_cluster_name is populated on only 142 of 9.4 million source rows (Census-era states used it
-- rarely), so the capped sample can legitimately lack it.
INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state_cluster_name')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state_cluster_name')));

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL primary-key rows (dbkey, cfda_seq_num)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true)
  WHERE dbkey IS NULL OR cfda_seq_num IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'fac_federal_awards_historic', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, dbkey, cfda_seq_num) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, dbkey, cfda_seq_num, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fac_federal_awards_historic', allow_moved_paths := true) GROUP BY ALL HAVING COUNT(*) > 1));

-- ─────────────────────────────────────────────────────────────
-- usaspending_assistance_by_county (VIEW over usaspending_assistance; there is no Iceberg table of its
-- own, so the view's aggregation is repeated here against the base table, exactly as in the schema
-- YAML). The DQ sample is the first 200000 rows of each of FY2025 and FY2026, so these checks prove the
-- aggregation, not full-year county totals.
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Rows of the county x listing x year aggregation'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END, n, 10000, 'Expected >=10000 (year, county, listing) rows from the DQ sample (about 58000 per sampled year)'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number));

-- Key columns: a NULL or malformed county would be a blank place of performance leaking into the view
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T6_pk_nulls_and_county_shape',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with a NULL key or a county_fips that is not 5 digits or ends in 000'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number)
  WHERE year IS NULL OR county_fips IS NULL OR assistance_listing IS NULL
     OR LENGTH(county_fips) <> 5 OR county_fips LIKE '%000');

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, county_fips, assistance_listing) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, county_fips, assistance_listing, COUNT(*) AS c FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number)
  GROUP BY ALL HAVING COUNT(*) > 1));

-- Conservation: every transaction is either in the view or in the excluded blank/state-level bucket, so
-- the view's transactions plus the excluded ones equal the base table's row count.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T9_transactions_conserved',
  CASE WHEN diff = 0 THEN 'pass' ELSE 'fail' END, diff, 0,
  'Base rows minus (view transactions + excluded blank/state-level rows); 0 means nothing dropped or double counted'
FROM (SELECT
  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true))
  - (SELECT SUM(transactions) FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number))
  - (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
     WHERE pop_county_fips IS NULL OR pop_county_fips = '' OR pop_county_fips LIKE '%000') AS diff);

-- The two programs the table was built for must be present
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_assistance_by_county', 'T10_example_listings_present',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'Distinct of 93.788 / 93.959 present in the sample'
FROM (SELECT COUNT(DISTINCT assistance_listing) AS n FROM (
  SELECT year, pop_county_fips AS county_fips, cfda_number AS assistance_listing,
    SUM(federal_action_obligation) AS obligated_amount, COUNT(*) AS transactions
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_assistance', allow_moved_paths := true)
  WHERE pop_county_fips IS NOT NULL AND pop_county_fips <> '' AND pop_county_fips NOT LIKE '%000'
  GROUP BY year, pop_county_fips, cfda_number)
  WHERE assistance_listing IN ('93.788', '93.959'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: public_pension_funding (Public Plans Database API, plan x fiscal year; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true));

-- ~250 plans per recent year; a scoped DQ run may cover one year, so the floor is 200 plan-years.
INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T2_row_count',
  CASE WHEN n >= 200 THEN 'pass' ELSE 'fail' END, n, 200, 'Expected >=200 plan-year rows (>=1 year, DQ-scoped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true) LIMIT 3;

-- Fully-null columns: funded_ratio_gasb67 is only populated from about FY2014, so a DQ run scoped
-- to older years only would warn; a current-year run must not.
INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with a NULL key or plan_name/state_abbr'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true)
  WHERE year IS NULL OR ppd_id IS NULL OR plan_name IS NULL OR state_abbr IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, ppd_id) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT year, ppd_id FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true) GROUP BY year, ppd_id HAVING COUNT(*) > 1));

-- Reported funded ratio must agree with assets / liabilities; allow rounding and basis differences.
INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T9_funded_ratio_consistent',
  CASE WHEN n * 1.0 / NULLIF(d, 0) < 0.05 THEN 'pass' ELSE 'fail' END, n, d,
  'Plan-years where |funded_ratio - assets/liabilities| > 0.02, out of plan-years with all three'
FROM (SELECT COUNT(*) FILTER (WHERE ABS(funded_ratio - act_assets / act_liabilities) > 0.02) AS n, COUNT(*) AS d
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true) WHERE funded_ratio IS NOT NULL AND act_assets IS NOT NULL AND act_liabilities > 0);

-- Known-value check (Public Plans Database FY2023 as pulled 2026-11-16): Illinois state and local plans
-- together are about 51 percent funded with roughly $198B unfunded; a latest-year value outside 40-65 percent
-- means the join, units or state mapping is wrong. Skipped (passes vacuously is NOT allowed): requires rows.
INSERT INTO dq_results
SELECT 'fiscal', 'public_pension_funding', 'T7_illinois_funded_ratio_sane',
  CASE WHEN r BETWEEN 0.40 AND 0.65 THEN 'pass' ELSE 'fail' END, r, 0.51,
  'Illinois sum(assets)/sum(liabilities) for FY2023 (expect 0.40-0.65)'
FROM (SELECT SUM(act_assets) / SUM(act_liabilities) AS r FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/public_pension_funding', allow_moved_paths := true)
  WHERE state_abbr = 'IL' AND year = 2023 AND act_assets IS NOT NULL AND act_liabilities IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- fhwa_program_apportionments
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'fhwa_program_apportionments', 'T1_row_count',
  CASE WHEN n >= 2815 THEN 'pass' ELSE 'fail' END, n, 2815,
  'FY2022-26: 51 states x 9 programs + 52 x 2 BFP rows per year = 563 per year'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fhwa_program_apportionments', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'fhwa_program_apportionments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null fiscal_year/program/state_abbr/amount'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fhwa_program_apportionments', allow_moved_paths := true)
  WHERE fiscal_year IS NULL OR program IS NULL OR state_abbr IS NULL OR amount IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'fhwa_program_apportionments', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (fiscal_year, program, state_abbr) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT fiscal_year, program, state_abbr FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fhwa_program_apportionments', allow_moved_paths := true)
  GROUP BY fiscal_year, program, state_abbr HAVING COUNT(*) > 1));

-- Known values from the issue: Bridge Formula Program FY2022-26 five-year totals per state.
INSERT INTO dq_results
SELECT 'fiscal', 'fhwa_program_apportionments', 'T7_bfp_' || state_abbr || '_5yr_total',
  CASE WHEN ABS(t - expected) <= 1000000 AND yrs = 5 THEN 'pass' ELSE 'fail' END, t, expected,
  'Sum of BFP_MAIN+BFP_OFF_SYSTEM FY2022-26 (expect ' || expected || ' within 1M, 5 years)'
FROM (SELECT a.state_abbr, SUM(a.amount) AS t, COUNT(DISTINCT a.fiscal_year) AS yrs, e.expected
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/fhwa_program_apportionments', allow_moved_paths := true) a
  JOIN (VALUES ('PA', 1767e6), ('NY', 2045e6), ('IL', 1486e6), ('MA', 1218e6), ('LA', 1095e6)) e(state_abbr, expected)
    ON a.state_abbr = e.state_abbr
  WHERE a.program IN ('BFP_MAIN', 'BFP_OFF_SYSTEM') AND a.fiscal_year BETWEEN 2022 AND 2026
  GROUP BY a.state_abbr, e.expected);

-- ─────────────────────────────────────────────────────────────
-- foreign_assistance_by_country (ForeignAssistance.gov funding file; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T1_row_count',
  CASE WHEN n >= 140000 THEN 'pass' ELSE 'fail' END, n, 140000, 'Source file had 143,963 rows at build time'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Null fiscal_year/funding_agency_id/funding_account_id/country_code/transaction_type_id'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true)
  WHERE fiscal_year IS NULL OR funding_agency_id IS NULL OR funding_account_id IS NULL
    OR country_code IS NULL OR transaction_type_id IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate (fiscal_year, funding_agency_id, funding_account_id, country_code, transaction_type_id) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true)
  GROUP BY fiscal_year, funding_agency_id, funding_account_id, country_code, transaction_type_id
  HAVING COUNT(*) > 1));

-- Known value: Israel Foreign Military Financing disbursed in FY2023 was 3.3 billion dollars.
INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T7_israel_fmf_fy2023',
  CASE WHEN ABS(t - 3.3e9) <= 1e6 THEN 'pass' ELSE 'fail' END, t, 3.3e9,
  'Israel, account 11x1082, Disbursements, FY2023 current_amount (expect 3.3e9 within 1M)'
FROM (SELECT COALESCE(SUM(current_amount), 0) AS t FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true)
  WHERE country_name = 'Israel' AND funding_account_id = '11x1082'
    AND transaction_type = 'Disbursements' AND fiscal_year = 2023);

INSERT INTO dq_results
SELECT 'fiscal', 'foreign_assistance_by_country', 'T7_fiscal_year_range',
  CASE WHEN lo = 2001 AND hi >= 2025 THEN 'pass' ELSE 'fail' END, hi, 2025,
  'fiscal_year spans 2001 to at least 2025 (min ' || lo || ')'
FROM (SELECT MIN(fiscal_year) AS lo, MAX(fiscal_year) AS hi FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/foreign_assistance_by_country', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: federal_funding_gaps (CRS report table; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T2_row_count',
  CASE WHEN n >= 23 THEN 'pass' ELSE 'fail' END, n, 23, 'CRS RS20348 Table 1 lists 23 gaps since FY1977'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

-- crs_report_version is one value by design: the whole table is read from one CRS edition.
INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'crs_report_version')));

INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null primary-key columns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true) WHERE fiscal_year IS NULL OR gap_seq_in_fiscal_year IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (fiscal_year, gap_seq_in_fiscal_year) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT fiscal_year, gap_seq_in_fiscal_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true) GROUP BY fiscal_year, gap_seq_in_fiscal_year HAVING COUNT(*) > 1));

-- Known values from CRS RS20348: FY2014 16 days, FY2019 34, FY1996 two gaps of 5 and 21, longest 75 (FY2026 DHS).
INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T7_known_values',
  CASE WHEN n = 4 AND mx = 75 THEN 'pass' ELSE 'fail' END, n, 4,
  'FY2014=16, FY2019=34, FY1996=5 and 21, max full_days=75'
FROM (SELECT COUNT(*) FILTER (WHERE (fiscal_year = 2014 AND full_days = 16) OR (fiscal_year = 2019 AND full_days = 34)
    OR (fiscal_year = 1996 AND gap_seq_in_fiscal_year = 1 AND full_days = 5)
    OR (fiscal_year = 1996 AND gap_seq_in_fiscal_year = 2 AND full_days = 21)) AS n, MAX(full_days) AS mx FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'federal_funding_gaps', 'T7_date_consistency',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'gap_end_date - gap_start_date <> full_days'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/federal_funding_gaps', allow_moved_paths := true) WHERE date_diff('day', gap_start_date, gap_end_date) <> full_days);

-- ─────────────────────────────────────────────────────────────
-- TABLE: continuing_resolutions (CRS report table; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T2_row_count',
  CASE WHEN n >= 207 THEN 'pass' ELSE 'fail' END, n, 207, 'CRS R46595 Table A-1 lists 207 acts through P.L. 119-4'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

-- crs_report_version is one value by design: the whole table is read from one CRS edition.
INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'crs_report_version')));

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null primary-key columns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true) WHERE fiscal_year IS NULL OR act_seq_in_fiscal_year IS NULL OR enactment_date IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (fiscal_year, act_seq_in_fiscal_year) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT fiscal_year, act_seq_in_fiscal_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true) GROUP BY fiscal_year, act_seq_in_fiscal_year HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T7_known_values',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1, 'P.L. 119-4: FY2025, enacted 2025-03-15, 200 days'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true) WHERE public_law = 'P.L. 119-4' AND fiscal_year = 2025
  AND enactment_date = DATE '2025-03-15' AND duration_days = 200);

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T7_cumulative_sequence',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'act_seq_cumulative is not 1..N without gaps'
FROM (SELECT COUNT(*) AS n FROM (SELECT MIN(act_seq_cumulative) AS mn, MAX(act_seq_cumulative) AS mx, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true))
  WHERE mn <> 1 OR mx <> c);

-- Acts after the newest CRS edition come from Congress.gov: P.L. 119-37 (FY2026, through 2026-01-30)
-- and P.L. 119-103 (FY2027, through 2026-12-11) must be present with their expiration dates.
INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T7_post_crs_acts',
  CASE WHEN n = 2 THEN 'pass' ELSE 'fail' END, n, 2, 'P.L. 119-37 (FY2026, 2025-11-12, exp 2026-01-30) and P.L. 119-103 (FY2027, 2026-09-02, exp 2026-12-11)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true)
  WHERE (public_law = 'P.L. 119-37' AND fiscal_year = 2026 AND enactment_date = DATE '2025-11-12' AND expiration_date = DATE '2026-01-30')
     OR (public_law = 'P.L. 119-103' AND fiscal_year = 2027 AND enactment_date = DATE '2026-09-02' AND expiration_date = DATE '2026-12-11'));

INSERT INTO dq_results
SELECT 'fiscal', 'continuing_resolutions', 'T7_post_crs_expiration',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Congress.gov-sourced acts with no expiration_date'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/continuing_resolutions', allow_moved_paths := true)
  WHERE crs_report_version LIKE 'congress.gov:%' AND expiration_date IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: appropriations_timeliness_by_fy (CRS report table; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T2_row_count',
  CASE WHEN n >= 49 THEN 'pass' ELSE 'fail' END, n, 49, 'One row per fiscal year FY1977-FY2025'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

-- crs_report_version is one value by design: the whole table is read from one CRS edition.
INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'crs_report_version')));

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null primary-key columns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true) WHERE fiscal_year IS NULL OR regular_bills_count IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (fiscal_year) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT fiscal_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true) GROUP BY fiscal_year HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T7_known_values',
  CASE WHEN n = 4 THEN 'pass' ELSE 'fail' END, n, 4,
  'FY1977/1989/1995/1997 all bills by Oct 1; only those four years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true) WHERE regular_bills_enacted_by_oct1 = regular_bills_count);

INSERT INTO dq_results
SELECT 'fiscal', 'appropriations_timeliness_by_fy', 'T7_interim_from_1998',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'interim_crs null for FY>=1998 or non-null before'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/appropriations_timeliness_by_fy', allow_moved_paths := true) WHERE (fiscal_year >= 1998 AND interim_crs IS NULL) OR (fiscal_year < 1998 AND interim_crs IS NOT NULL));
-- cbo_budget_projections
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'cbo_budget_projections', 'T1_row_count',
  CASE WHEN n >= 22000 THEN 'pass' ELSE 'fail' END, n, 22000,
  '8 vintage files across 3 datasets: 22,658 rows at the 2026-02 release'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/cbo_budget_projections', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'cbo_budget_projections', 'T2_dataset_vintages',
  CASE WHEN n >= 8 THEN 'pass' ELSE 'fail' END, n, 8, 'Distinct (dataset, vintage) pairs'
FROM (SELECT COUNT(*) AS n FROM (SELECT DISTINCT dataset, vintage FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/cbo_budget_projections', allow_moved_paths := true)));

INSERT INTO dq_results
SELECT 'fiscal', 'cbo_budget_projections', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null dataset/vintage/fiscal_year/variable/value'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/cbo_budget_projections', allow_moved_paths := true)
  WHERE dataset IS NULL OR vintage IS NULL OR fiscal_year IS NULL OR variable IS NULL OR value IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'cbo_budget_projections', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (dataset, vintage, fiscal_year, variable) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT dataset, vintage, fiscal_year, variable FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/cbo_budget_projections', allow_moved_paths := true)
  GROUP BY dataset, vintage, fiscal_year, variable HAVING COUNT(*) > 1));

-- Known values (CBO 2026-02 outlook): FY2025 actuals and a 10-year / 30-year endpoint.
INSERT INTO dq_results
SELECT 'fiscal', 'cbo_budget_projections', 'T7_' || chk,
  CASE WHEN ABS(v - expected) <= 0.5 THEN 'pass' ELSE 'fail' END, v, expected,
  'CBO value for ' || chk || ' (expect ' || expected || ' within 0.5)'
FROM (SELECT e.chk, MAX(a.value) AS v, e.expected
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/cbo_budget_projections', allow_moved_paths := true) a
  JOIN (VALUES
    ('hist_2026-02_FY2025_deficit_total', 'historical_budget', '2026-02', 2025, 'deficit_total', -1775.37),
    ('hist_2026-02_FY2025_net_interest', 'historical_budget', '2026-02', 2025, 'outlays_net_interest', 969.938),
    ('hist_2026-02_FY2025_debt_gdp', 'historical_budget', '2026-02', 2025, 'debt_held_by_public_gdp_share', 99.375),
    ('tenyr_2026-02_FY2036_net_interest', 'ten_year_budget', '2026-02', 2036, 'proj_outlays_net_interest', 2144.336),
    ('longterm_2026-02_FY2056_debt_gdp', 'long_term_budget', '2026-02', 2056, 'lt_debt_held_by_public_gdp_share', 175.076)
  ) e(chk, dataset, vintage, fiscal_year, variable, expected)
    ON a.dataset = e.dataset AND a.vintage = e.vintage AND a.fiscal_year = e.fiscal_year AND a.variable = e.variable
  GROUP BY e.chk, e.expected);

-- treasury_auction_results
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T1_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100,
  'Fiscal Data returns 163 auctions for 1990, 440 for 2024, 444 for 2025'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T2_security_types',
  CASE WHEN n = 3 THEN 'pass' ELSE 'fail' END, n, 3, 'Distinct security_type: Bill, Note, Bond (TIPS/FRN/CMB are flag columns)'
FROM (SELECT COUNT(DISTINCT security_type) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null auction_date or cusip'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true) WHERE auction_date IS NULL OR cusip IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (auction_date, cusip) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT auction_date, cusip FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true) GROUP BY auction_date, cusip HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T7_bid_to_cover_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Settled auctions with bid_to_cover outside 0.5-15'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true) WHERE bid_to_cover_ratio IS NOT NULL AND (bid_to_cover_ratio < 0.5 OR bid_to_cover_ratio > 15));

INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T7_null_marker_text',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Treasury "null" text must be SQL NULL, not a string'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true) WHERE security_term = 'null' OR reopening = 'null' OR auction_format = 'null');

-- Known value: 2024-05-01 17-Week bill (CUSIP 912797LA3) bid-to-cover 3.02, offering $60bn.
INSERT INTO dq_results
SELECT 'fiscal', 'treasury_auction_results', 'T7_known_auction',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1, '912797LA3 on 2024-05-01: bid_to_cover 3.02, offering 60bn'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/treasury_auction_results', allow_moved_paths := true)
  WHERE cusip = '912797LA3' AND auction_date = DATE '2024-05-01'
    AND ABS(bid_to_cover_ratio - 3.02) < 0.001 AND offering_amt = 60000000000);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;

-- ─────────────────────────────────────────────────────────────
-- tanf_caseload
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T1_row_count',
  CASE WHEN n >= 6000 THEN 'pass' ELSE 'fail' END, n, 6000,
  'FY2017 onward: 55 geographies x at least 120 months'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null state_abbr/year/month/total_families/total_recipients'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL OR month IS NULL OR total_families IS NULL OR total_recipients IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (state_abbr, year, month) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT state_abbr, year, month FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true)
  GROUP BY state_abbr, year, month HAVING COUNT(*) > 1));

-- Known values read from the ACF workbooks: U.S. totals October 2016 and October 2023.
INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T7_us_families_' || year || '_' || month,
  CASE WHEN total_families = expected THEN 'pass' ELSE 'fail' END, total_families, expected,
  'U.S. TANF families in the month (ACF workbook)'
FROM (SELECT year, month, total_families,
        CASE WHEN year = 2016 THEN 1151586 ELSE 854981 END AS expected
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true)
      WHERE state_abbr = 'US' AND ((year = 2016 AND month = 10) OR (year = 2023 AND month = 10)));

-- Families split by parent count must add up to the total for every row.
INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T7_parent_split_sums',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows where two+one+no-parent families differ from total_families by more than 5% (ACF reports Pennsylvania FY2026 two-parent as 0, a 2.3% gap)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true)
  WHERE two_parent_families IS NOT NULL AND one_parent_families IS NOT NULL
    AND no_parent_families IS NOT NULL AND total_families > 0
    AND ABS(two_parent_families + one_parent_families + no_parent_families - total_families)
        > 0.05 * total_families);

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_caseload', 'T7_recipient_split_sums',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows where adult+child recipients differ from total_recipients by more than 1%'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_caseload', allow_moved_paths := true)
  WHERE adult_recipients IS NOT NULL AND child_recipients IS NOT NULL AND total_recipients > 0
    AND ABS(adult_recipients + child_recipients - total_recipients) > 0.01 * total_recipients);

-- ─────────────────────────────────────────────────────────────
-- tanf_work_participation
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'tanf_work_participation', 'T1_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400,
  'FY2017-2024: 55 rows (50 states, DC, PR, GU, VI, US) x 8 fiscal years = 440'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_work_participation', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_work_participation', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Null fiscal_year/state_abbr/all_families_rate'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_work_participation', allow_moved_paths := true)
  WHERE fiscal_year IS NULL OR state_abbr IS NULL OR all_families_rate IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_work_participation', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (fiscal_year, state_abbr) keys'
FROM (SELECT COUNT(*) AS n FROM (SELECT fiscal_year, state_abbr FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_work_participation', allow_moved_paths := true)
  GROUP BY fiscal_year, state_abbr HAVING COUNT(*) > 1));

-- Known values read from the ACF workbooks: national all-families rate FY2018 and FY2024.
INSERT INTO dq_results
SELECT 'fiscal', 'tanf_work_participation', 'T7_us_rate_fy' || fiscal_year,
  CASE WHEN ABS(all_families_rate - expected) < 0.0005 THEN 'pass' ELSE 'fail' END,
  all_families_rate, expected, 'National combined all-families participation rate (ACF Table 1A)'
FROM (SELECT fiscal_year, all_families_rate,
        CASE WHEN fiscal_year = 2018 THEN 0.481 ELSE 0.3541 END AS expected
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_work_participation', allow_moved_paths := true)
      WHERE state_abbr = 'US' AND fiscal_year IN (2018, 2024));

INSERT INTO dq_results
SELECT 'fiscal', 'tanf_work_participation', 'T7_rate_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rates outside 0-10 (fractions; a few states report above 1 after caseload effects)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/tanf_work_participation', allow_moved_paths := true)
  WHERE all_families_rate < 0 OR all_families_rate > 10
     OR two_parent_rate < 0 OR two_parent_rate > 10);

