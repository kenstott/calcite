-- dq-lookback: 1
-- Federal Register Data Quality Checks
-- Schema: fedregister
-- Tables: fr_documents
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 for fr_documents exclude partition columns 'year' and 'month'.

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

-- ─────────────────────────────────────────────────────────────
-- TABLE: fr_documents
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true));

-- T2: row_count (DQ scope 2024-2026 = ~2.5 years × ~25k docs/year = ~50k minimum)
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END,
  n, 50000, 'Expected at least 50000 Federal Register documents (2024-2026 DQ scope)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('year', 'month', 'signing_date')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('year', 'month', 'signing_date')
  )
);

-- T6: pk_nulls (document_number, doc_type, publication_date NOT NULL)
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL document_number, doc_type, or publication_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true)
      WHERE document_number IS NULL OR doc_type IS NULL OR publication_date IS NULL);

-- T7: doc_type values (RULE, PRORULE, NOTICE, PRESDOC only)
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T7_doc_type_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with doc_type outside known set (RULE, PRORULE, NOTICE, PRESDOC)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true)
      WHERE doc_type IS NOT NULL AND doc_type NOT IN ('RULE', 'PRORULE', 'NOTICE', 'PRESDOC'));

-- T7: all four doc types present
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T7_doc_type_coverage',
  CASE WHEN n = 4 THEN 'pass' ELSE 'warn' END,
  n, 4, 'Distinct doc_type values (expect all 4: RULE, PRORULE, NOTICE, PRESDOC)'
FROM (SELECT COUNT(DISTINCT doc_type) AS n FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true));

-- T7: document_number format (YYYY-NNNNN)
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T7_document_number_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'document_number not matching expected YYYY-NNNNN format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true)
      WHERE document_number IS NOT NULL
        AND NOT REGEXP_MATCHES(document_number, '^\d{4}-\d+$'));

-- T7: publication_date format (YYYY-MM-DD)
INSERT INTO dq_results
SELECT 'fedregister', 'fr_documents', 'T7_publication_date_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'publication_date not matching YYYY-MM-DD format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/fedregister/fr_documents', allow_moved_paths := true)
      WHERE publication_date IS NOT NULL
        AND NOT REGEXP_MATCHES(publication_date, '^\d{4}-\d{2}-\d{2}$'));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
