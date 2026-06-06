-- dq-lookback: 1
-- ============================================================================
-- Data Quality Checks: cftc schema
-- ============================================================================
-- Tests per table:
--   T1  existence        — at least 1 row is readable
--   T2  row_count        — row count meets conservative minimum threshold
--   T3  sample           — print 1 sample NEWT row (informational)
--   T4  all_null_cols    — no non-partition column is 100% NULL
--   T5  all_same_value   — no non-partition column has only 1 distinct value
--   T6  pk_nulls         — dissemination_id has no NULLs
--   T7  expected_values  — all 5 asset classes present; NEWT action_type exists;
--                          cleared values limited to known set
--
-- Source: iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
-- Result: written to dq_results TEMP TABLE (worker appends COPY to Parquet)
-- ============================================================================

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id     = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint          = '21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region            = 'auto';

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

-- ============================================================================
-- T1: EXISTENCE — table must have at least 1 readable row
-- ============================================================================

INSERT INTO dq_results
SELECT
  'cftc' AS schema,
  tbl,
  'existence' AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END AS status,
  CAST(n AS VARCHAR) AS value,
  '1'                AS threshold,
  CASE WHEN n > 0 THEN 'table is readable'
       ELSE 'table returned 0 rows — may not yet be ingested' END AS detail
FROM (
  SELECT 'cftc_trades' AS tbl,
    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true) LIMIT 1) t) AS n
) src;

-- ============================================================================
-- T2: ROW COUNT — conservative minimum for 2024+ daily coverage
--   ~250 trading days/year × 5 asset classes × even conservative daily volumes
--   Threshold of 100,000 accommodates partial ingest (a few months only)
-- ============================================================================

INSERT INTO dq_results
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'row_count'   AS test,
  CASE WHEN n >= 100000 THEN 'pass'
       WHEN n >= 10000  THEN 'warn'
       ELSE 'fail' END AS status,
  CAST(n AS VARCHAR) AS value,
  '100000'           AS threshold,
  'Total swap dissemination records across all asset classes since 2024' AS detail
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
) t;

-- ============================================================================
-- T3: SAMPLE — print 1 new trade for visual inspection (informational)
-- ============================================================================

SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'sample'      AS test,
  dissemination_id,
  asset_class,
  action_type,
  trade_date,
  notional_amount_leg1,
  notional_currency_leg1,
  fixed_rate_leg1,
  cleared,
  settlement_location,
  upi_underlier_name
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
WHERE action_type = 'NEWT'
LIMIT 3;

-- ============================================================================
-- T4: ALL-NULL COLUMNS — no non-partition column should be 100% NULL
--   Partition columns (type, asset_class, year, month) excluded.
--   Many columns are legitimately sparse (FX-only, option-only fields) —
--   checking only the core identity/trade columns that must always populate.
-- ============================================================================

INSERT INTO dq_results
WITH totals AS (
  SELECT COUNT(*) AS total_rows
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
),
null_counts AS (
  SELECT
    SUM(CASE WHEN dissemination_id     IS NULL THEN 1 ELSE 0 END) AS n_dissemination_id,
    SUM(CASE WHEN action_type          IS NULL THEN 1 ELSE 0 END) AS n_action_type,
    SUM(CASE WHEN event_timestamp      IS NULL THEN 1 ELSE 0 END) AS n_event_timestamp,
    SUM(CASE WHEN trade_date           IS NULL THEN 1 ELSE 0 END) AS n_trade_date,
    SUM(CASE WHEN notional_currency_leg1 IS NULL THEN 1 ELSE 0 END) AS n_notional_currency_leg1
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
)
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'all_null_cols' AS test,
  CASE WHEN
    n_dissemination_id      = totals.total_rows OR
    n_action_type           = totals.total_rows OR
    n_event_timestamp       = totals.total_rows OR
    n_trade_date            = totals.total_rows OR
    n_notional_currency_leg1 = totals.total_rows
  THEN 'warn' ELSE 'pass' END AS status,
  '0' AS value,
  '0' AS threshold,
  CASE WHEN n_dissemination_id = totals.total_rows      THEN 'dissemination_id is 100% NULL'
       WHEN n_action_type = totals.total_rows           THEN 'action_type is 100% NULL'
       WHEN n_event_timestamp = totals.total_rows       THEN 'event_timestamp is 100% NULL'
       WHEN n_trade_date = totals.total_rows            THEN 'trade_date is 100% NULL'
       WHEN n_notional_currency_leg1 = totals.total_rows THEN 'notional_currency_leg1 is 100% NULL'
       ELSE 'all core columns populated' END AS detail
FROM null_counts, totals;

-- ============================================================================
-- T5: ALL-SAME-VALUE — core classification columns must have >1 distinct value
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT
    COUNT(DISTINCT asset_class)  AS d_asset_class,
    COUNT(DISTINCT action_type)  AS d_action_type,
    COUNT(DISTINCT cleared)      AS d_cleared
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
)
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'all_same_value' AS test,
  CASE WHEN d_asset_class <= 1 OR d_action_type <= 1 OR d_cleared <= 1
       THEN 'warn' ELSE 'pass' END AS status,
  CONCAT('asset_class=', d_asset_class, ' action_type=', d_action_type, ' cleared=', d_cleared) AS value,
  'asset_class>1,action_type>1,cleared>1' AS threshold,
  CASE WHEN d_asset_class <= 1 THEN 'asset_class has only 1 distinct value — all partitions may not be loaded'
       WHEN d_action_type <= 1 THEN 'action_type has only 1 distinct value — lifecycle events missing'
       WHEN d_cleared <= 1     THEN 'cleared has only 1 distinct value — unexpected'
       ELSE 'classification columns have expected variety' END AS detail
FROM counts;

-- ============================================================================
-- T6: PK NULLS — dissemination_id must never be NULL
-- ============================================================================

INSERT INTO dq_results
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'pk_nulls'    AS test,
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END AS status,
  CAST(n AS VARCHAR) AS value,
  '0'                AS threshold,
  CASE WHEN n = 0 THEN 'dissemination_id: no NULLs'
       ELSE CONCAT(n, ' NULL dissemination_id values found') END AS detail
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
  WHERE dissemination_id IS NULL
) t;

-- ============================================================================
-- T7: EXPECTED VALUES
--   7a: All 5 CFTC asset classes must be present
--   7b: NEWT (new trade) action_type must exist — no trades = something wrong
--   7c: cleared values must only be C, U, or I
--   7d: At least one IR trade should reference a SOFR underlier (regression check)
-- ============================================================================

-- T7a: asset class coverage
INSERT INTO dq_results
WITH present AS (
  SELECT COUNT(DISTINCT asset_class) AS n_classes,
         COUNT(DISTINCT CASE WHEN asset_class = 'RATES'       THEN 1 END) AS has_rates,
         COUNT(DISTINCT CASE WHEN asset_class = 'CREDITS'     THEN 1 END) AS has_credits,
         COUNT(DISTINCT CASE WHEN asset_class = 'FOREX'       THEN 1 END) AS has_forex,
         COUNT(DISTINCT CASE WHEN asset_class = 'EQUITIES'    THEN 1 END) AS has_equities,
         COUNT(DISTINCT CASE WHEN asset_class = 'COMMODITIES' THEN 1 END) AS has_commodities
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
)
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'expected_values:asset_classes' AS test,
  CASE WHEN has_rates > 0 AND has_credits > 0 AND has_forex > 0
            AND has_equities > 0 AND has_commodities > 0
       THEN 'pass'
       WHEN n_classes >= 3 THEN 'warn'
       ELSE 'fail' END AS status,
  CONCAT(n_classes, '/5 asset classes present') AS value,
  '5'                                           AS threshold,
  CONCAT(
    CASE WHEN has_rates > 0       THEN '' ELSE 'RATES missing ' END,
    CASE WHEN has_credits > 0     THEN '' ELSE 'CREDITS missing ' END,
    CASE WHEN has_forex > 0       THEN '' ELSE 'FOREX missing ' END,
    CASE WHEN has_equities > 0    THEN '' ELSE 'EQUITIES missing ' END,
    CASE WHEN has_commodities > 0 THEN '' ELSE 'COMMODITIES missing ' END,
    CASE WHEN n_classes = 5       THEN 'all 5 asset classes present' ELSE '' END
  ) AS detail
FROM present;

-- T7b: NEWT action_type must exist
INSERT INTO dq_results
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'expected_values:newt_trades' AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END AS status,
  CAST(n AS VARCHAR) AS value,
  '1'                AS threshold,
  CASE WHEN n > 0 THEN CONCAT(n, ' new trade (NEWT) records found')
       ELSE 'No NEWT records found — data may be malformed or action_type column unmapped' END AS detail
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
  WHERE action_type = 'NEWT'
) t;

-- T7c: cleared values must be in known set
INSERT INTO dq_results
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'expected_values:cleared_domain' AS test,
  CASE WHEN n_bad = 0 THEN 'pass' ELSE 'warn' END AS status,
  CAST(n_bad AS VARCHAR) AS value,
  '0'                    AS threshold,
  CASE WHEN n_bad = 0 THEN 'cleared values all within expected set {C, U, I, NULL}'
       ELSE CONCAT(n_bad, ' rows with unexpected cleared value') END AS detail
FROM (
  SELECT COUNT(*) AS n_bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
  WHERE cleared IS NOT NULL
    AND cleared NOT IN ('C', 'U', 'I')
) t;

-- T7d: IR/RATES partition must include SOFR underlier references
INSERT INTO dq_results
SELECT
  'cftc'        AS schema,
  'cftc_trades' AS tbl,
  'expected_values:sofr_underlier' AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END AS status,
  CAST(n AS VARCHAR) AS value,
  '1'                AS threshold,
  CASE WHEN n > 0 THEN CONCAT(n, ' SOFR-based IR swap records found')
       ELSE 'No SOFR underlier records found in RATES — unexpected for post-2024 data' END AS detail
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/cftc/cftc_trades', allow_moved_paths := true)
  WHERE asset_class = 'RATES'
    AND upi_underlier_name LIKE '%SOFR%'
) t;

-- ============================================================================
-- Final result output
-- ============================================================================

SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY
  CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END,
  tbl, test;
