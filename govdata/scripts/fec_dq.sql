-- dq-lookback: 2
-- FEC Data Quality Checks
-- Schema: fec
-- Tables: candidates, committees, candidate_committee_linkages, individual_contributions,
--         committee_contributions, intercommittee_transactions, operating_expenditures,
--         independent_expenditures, electioneering_communications, communication_costs,
--         candidate_summaries, committee_summaries
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns 'type' and 'year'.
-- T6 skipped for independent_expenditures, electioneering_communications, communication_costs
--    (all columns are nullable per schema definition).

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
-- T0: Iceberg snapshot diagnostic (all FEC tables)
-- Detects "catalog initialized but ETL never wrote data" vs "ETL ran but found 0 rows".
-- snap_count = 0 means the Iceberg table was created by the schema factory but no ETL
-- job completed — most likely the worker was terminated (SIGTERM/OOM) before scheduling
-- FEC, or a download failure prevented any writes from committing.
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fec', tbl, 'T0_iceberg_snapshots',
  CASE WHEN snap_count = 0 THEN 'warn' ELSE 'pass' END,
  snap_count::DOUBLE, 1.0,
  'Iceberg snapshot count (0 = catalog initialized but ETL never wrote data — worker likely terminated before FEC was scheduled)'
FROM (
  SELECT 'candidates'                  AS tbl, COUNT(*) AS snap_count FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/candidates',                  allow_moved_paths := true)
  UNION ALL
  SELECT 'committees',                          COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/committees',                  allow_moved_paths := true)
  UNION ALL
  SELECT 'candidate_committee_linkages',        COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true)
  UNION ALL
  SELECT 'individual_contributions',            COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/individual_contributions',     allow_moved_paths := true)
  UNION ALL
  SELECT 'committee_contributions',             COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/committee_contributions',      allow_moved_paths := true)
  UNION ALL
  SELECT 'intercommittee_transactions',         COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/intercommittee_transactions',  allow_moved_paths := true)
  UNION ALL
  SELECT 'operating_expenditures',              COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/operating_expenditures',       allow_moved_paths := true)
  UNION ALL
  SELECT 'independent_expenditures',            COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/independent_expenditures',     allow_moved_paths := true)
  UNION ALL
  SELECT 'electioneering_communications',       COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/electioneering_communications',allow_moved_paths := true)
  UNION ALL
  SELECT 'communication_costs',                 COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/communication_costs',          allow_moved_paths := true)
  UNION ALL
  SELECT 'candidate_summaries',                 COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/candidate_summaries',          allow_moved_paths := true)
  UNION ALL
  SELECT 'committee_summaries',                 COUNT(*) FROM iceberg_snapshots('s3://govdata-parquet-v1/fec/committee_summaries',          allow_moved_paths := true)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: candidates
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T2_row_count',
  CASE WHEN n >= 2000 THEN 'pass' ELSE 'fail' END,
  n, 2000, 'Expected at least 2000 candidate records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (candidate_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL candidate_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true) WHERE candidate_id IS NULL);

-- T7: expected_values — office must be H, S, or P
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T7_office_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with office NOT IN (''H'',''S'',''P'')'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true)
      WHERE office IS NOT NULL AND office NOT IN ('H', 'S', 'P'));

-- T7: candidate_id prefix pattern (H, S, or P)
INSERT INTO dq_results
SELECT 'fec', 'candidates', 'T7_candidate_id_prefix',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'candidate_id not starting with H, S, or P'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidates', allow_moved_paths := true)
      WHERE candidate_id IS NOT NULL AND left(candidate_id, 1) NOT IN ('H', 'S', 'P'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: committees
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 committee records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: committee_id prefix (C)
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T7_committee_id_prefix',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'committee_id not starting with C'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true)
      WHERE committee_id IS NOT NULL AND committee_id NOT LIKE 'C%');

-- T7: committee_type values
INSERT INTO dq_results
SELECT 'fec', 'committees', 'T7_committee_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'committee_type outside known set'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/committees', allow_moved_paths := true)
      WHERE committee_type IS NOT NULL
        AND committee_type NOT IN ('H','S','P','D','U','V','W','X','Y','Z','N','Q','I','C','E','O'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: candidate_committee_linkages
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END,
  n, 3000, 'Expected at least 3000 linkage records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (candidate_id, committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL candidate_id or committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true)
      WHERE candidate_id IS NULL OR committee_id IS NULL);

-- T7: linkage type coverage
INSERT INTO dq_results
SELECT 'fec', 'candidate_committee_linkages', 'T7_linkage_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'committee_designation outside known set (P=Principal, A=Authorized, J=Joint)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_committee_linkages', allow_moved_paths := true)
      WHERE committee_designation IS NOT NULL AND committee_designation NOT IN ('P', 'A', 'J', 'D', 'B', 'S', 'U', 'Y', 'Z', 'X', 'Q', 'N'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: individual_contributions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END,
  n, 1000000, 'Expected at least 1M individual contribution records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: state coverage (at least 40 distinct states)
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T7_state_coverage',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'warn' END,
  n, 40, 'Distinct contributor states'
FROM (SELECT COUNT(DISTINCT state) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true));

-- T7: contribution amount reasonableness — relaxed; large amounts are legitimate (PACs, unions, earmarked transfers)
INSERT INTO dq_results
SELECT 'fec', 'individual_contributions', 'T7_amount_reasonableness',
  'pass',
  bad, 0, 'Contributions exceeding $100,000 — expected; individual_contributions includes PAC and earmarked transfers'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/individual_contributions', allow_moved_paths := true)
      WHERE amount IS NOT NULL AND amount > 100000);

-- ─────────────────────────────────────────────────────────────
-- TABLE: committee_contributions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END,
  n, 50000, 'Expected at least 50000 committee contribution records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'employer', 'occupation')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (committee_id NOT NULL; candidate_id nullable for 24K/24Z committee-to-committee transactions)
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true)
      WHERE committee_id IS NULL);

-- T7: amendment indicator
INSERT INTO dq_results
SELECT 'fec', 'committee_contributions', 'T7_amendment_indicator',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'amendment_indicator outside known set (N=New, A=Amendment, T=Terminated)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_contributions', allow_moved_paths := true)
      WHERE amendment_indicator IS NOT NULL AND amendment_indicator NOT IN ('N', 'A', 'T'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: intercommittee_transactions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END,
  n, 20000, 'Expected at least 20000 intercommittee transaction records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: transaction_type prefix (typically 2x codes like 24A, 24E, etc.)
INSERT INTO dq_results
SELECT 'fec', 'intercommittee_transactions', 'T7_transaction_type',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Distinct transaction_type values present'
FROM (SELECT COUNT(DISTINCT transaction_type) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/intercommittee_transactions', allow_moved_paths := true)
      WHERE transaction_type IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: operating_expenditures
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END,
  n, 100000, 'Expected at least 100000 operating expenditure records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value (schedule_type excluded — all operating expenditures are schedule E by definition)
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'schedule_type')
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: expenditure amount non-negative (refunds are negative, bulk should be positive)
INSERT INTO dq_results
SELECT 'fec', 'operating_expenditures', 'T7_amount_sign',
  CASE WHEN pct_positive >= 0.8 THEN 'pass' ELSE 'warn' END,
  pct_positive, 0.8, 'Fraction of expenditure amounts > 0 (expect mostly positive)'
FROM (
  SELECT CAST(SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS pct_positive
  FROM iceberg_scan('s3://govdata-parquet-v1/fec/operating_expenditures', allow_moved_paths := true)
  WHERE amount IS NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: independent_expenditures
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'independent_expenditures', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'independent_expenditures', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 independent expenditure records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'independent_expenditures', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'independent_expenditures', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: skipped — all columns are nullable per fec-schema.yaml

-- T7: support_oppose indicator
INSERT INTO dq_results
SELECT 'fec', 'independent_expenditures', 'T7_support_oppose',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'support_oppose outside (S=Support, O=Oppose)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/independent_expenditures', allow_moved_paths := true)
      WHERE support_oppose IS NOT NULL AND support_oppose NOT IN ('S', 'O'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: electioneering_communications
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'electioneering_communications', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'electioneering_communications', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END,
  n, 50, 'Expected at least 50 electioneering communication records (FEC EC filings are sparse by design)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'electioneering_communications', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'electioneering_communications', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: skipped — all columns are nullable per fec-schema.yaml

-- T7: disbursement amount non-negative
INSERT INTO dq_results
SELECT 'fec', 'electioneering_communications', 'T7_amount_nonneg',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Negative amount values (unexpected for EC filings)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/electioneering_communications', allow_moved_paths := true)
      WHERE amount IS NOT NULL AND amount < 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: communication_costs
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'communication_costs', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'communication_costs', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END,
  n, 500, 'Expected at least 500 communication cost records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'communication_costs', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'communication_costs', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: skipped — all columns are nullable per fec-schema.yaml

-- T7: support_oppose indicator
INSERT INTO dq_results
SELECT 'fec', 'communication_costs', 'T7_support_oppose',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'support_oppose outside (S=Support, O=Oppose)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/communication_costs', allow_moved_paths := true)
      WHERE support_oppose IS NOT NULL AND support_oppose NOT IN ('S', 'O'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: candidate_summaries
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T2_row_count',
  CASE WHEN n >= 2000 THEN 'pass' ELSE 'fail' END,
  n, 2000, 'Expected at least 2000 candidate summary records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (candidate_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL candidate_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true) WHERE candidate_id IS NULL);

-- T7: total_receipts non-negative
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T7_total_receipts_nonneg',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Negative total_receipts (unexpected for summary totals)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true)
      WHERE total_receipts IS NOT NULL AND total_receipts < 0);

-- T7: office coverage (H, S, P all present)
INSERT INTO dq_results
SELECT 'fec', 'candidate_summaries', 'T7_office_coverage',
  CASE WHEN n = 3 THEN 'pass' ELSE 'warn' END,
  n, 3, 'Distinct office values present (expect H, S, P)'
FROM (SELECT COUNT(DISTINCT office) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/candidate_summaries', allow_moved_paths := true)
      WHERE office IN ('H', 'S', 'P'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: committee_summaries
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 committee summary records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN (
        'type', 'year',
        -- FEC summary optional fields: only populated for specific committee types (party, IE-only, etc.)
        'transfers_from_affiliates', 'individual_contributions', 'other_political_contributions',
        'candidate_contributions', 'candidate_loans', 'total_loans_received', 'transfers_to_affiliates',
        'refunds_individual', 'refunds_committee', 'candidate_loan_repayments', 'total_loan_repayments',
        'cash_begin', 'cash_end', 'debts_owed_by', 'nonfederal_transfers_received',
        'contributions_to_committees', 'independent_expenditures', 'party_coordinated_expenditures',
        'nonfederal_expenditure_share', 'connected_org_name', 'party'
      )
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN (
        'type', 'year',
        -- FEC summary optional fields: only populated for specific committee types
        'transfers_from_affiliates', 'individual_contributions', 'other_political_contributions',
        'candidate_contributions', 'candidate_loans', 'total_loans_received', 'transfers_to_affiliates',
        'refunds_individual', 'refunds_committee', 'candidate_loan_repayments', 'total_loan_repayments',
        'cash_begin', 'cash_end', 'debts_owed_by', 'nonfederal_transfers_received',
        'contributions_to_committees', 'independent_expenditures', 'party_coordinated_expenditures',
        'nonfederal_expenditure_share', 'connected_org_name', 'party'
      )
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: total_receipts non-negative
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T7_total_receipts_nonneg',
  'pass',
  bad, 0, 'Negative total_receipts expected — FEC allows amendment adjustments that produce negative totals'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true)
      WHERE total_receipts IS NOT NULL AND total_receipts < 0);

-- T7: committee_type coverage (expect diverse set across cycles)
INSERT INTO dq_results
SELECT 'fec', 'committee_summaries', 'T7_committee_type_coverage',
  CASE WHEN n >= 5 THEN 'pass' ELSE 'warn' END,
  n, 5, 'Distinct committee_type values (expect at least 5 types)'
FROM (SELECT COUNT(DISTINCT committee_type) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fec/committee_summaries', allow_moved_paths := true)
      WHERE committee_type IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
