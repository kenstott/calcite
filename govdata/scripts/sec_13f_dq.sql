-- dq-lookback: 1
-- sec_13f_dq.sql — SEC 13F-HR institutional holdings Data Quality.
-- The sec_13f slot fetches 13F-HR / 13F-HR/A filings; its DQ covers sec.institutional_holdings and the
-- 13F rows of sec.filing_metadata. Row-content checks below mirror the verify list of the 13F reprocess:
-- no placeholder-only accessions, no entity-escaping residue in the free text, other_manager present,
-- and value_thousands without a 1000x step at 2023-01-03 (the SEC changed the <value> unit then).

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET unsafe_enable_version_guessing=true;

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec_13f', 'institutional_holdings', 'existence',
  CASE WHEN nmeta > 0 THEN 'pass' ELSE 'fail' END,
  CAST(nmeta AS VARCHAR), '1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (SELECT (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings/metadata/*.json')) AS nmeta);

SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results SELECT 'sec_13f','institutional_holdings','row_count', CASE WHEN n>=50 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'50', CASE WHEN n>=50 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)) t;

SELECT '=== T3: ROW CONTENT ===' AS section;

-- An accession whose every row has a NULL cusip is the placeholder row written when no holdings were
-- extracted. Legitimate only for a filing that reports no information-table entries.
INSERT INTO dq_results
SELECT 'sec_13f', 'institutional_holdings', 'no_placeholder_accessions',
       CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0',
       CASE WHEN n = 0 THEN 'every accession has at least one row with a cusip'
            ELSE CAST(n AS VARCHAR) || ' accession(s) hold only NULL-cusip placeholder rows' END
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT accession_number
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)
    GROUP BY accession_number
    HAVING COUNT(cusip) = 0
  )
) t;

INSERT INTO dq_results
SELECT 'sec_13f', 'institutional_holdings', 'text_entity_residue',
       CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0',
       CASE WHEN n = 0 THEN 'no entity references or lost-ampersand fragments in issuer_name / title_of_class'
            ELSE CAST(n AS VARCHAR) || ' row(s) still carry &amp; / &lt; / &#NN; / Xamp;Y in issuer_name or title_of_class' END
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)
  WHERE regexp_matches(coalesce(issuer_name, '') || ' ' || coalesce(title_of_class, ''),
                       '&(amp|lt|gt|quot|apos|nbsp|#[0-9]+|#[xX][0-9a-fA-F]+);|[A-Za-z0-9]amp;')
) t;

INSERT INTO dq_results
SELECT 'sec_13f', 'institutional_holdings', 'other_manager_populated',
       CASE WHEN total = 0 THEN 'warn' WHEN with_om > 0 THEN 'pass' ELSE 'fail' END,
       CAST(with_om AS VARCHAR), '1',
       CASE WHEN total = 0 THEN 'no rows in this sample'
            WHEN with_om > 0 THEN 'other_manager is populated for some rows'
            ELSE 'no row carries other_manager; the column is not being filled' END
FROM (
  SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE other_manager IS NOT NULL AND other_manager <> '') AS with_om
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)
) t;

-- value_thousands is value/1000 for filings from 2023-01-03 (the filer-reported <value> became whole
-- dollars), so value_thousands*1000/shares (an implied price) must not step ~1000x across that date.
-- Compared on the median implied price of rows with shares > 0 and equity-like units, per side.
INSERT INTO dq_results
SELECT 'sec_13f', 'institutional_holdings', 'value_unit_continuity',
       CASE WHEN pre_n = 0 OR post_n = 0 THEN 'warn'
            WHEN post_px / NULLIF(pre_px, 0) BETWEEN 0.1 AND 10 THEN 'pass' ELSE 'fail' END,
       CAST(round(post_px / NULLIF(pre_px, 0), 3) AS VARCHAR), '0.1..10',
       CASE WHEN pre_n = 0 OR post_n = 0 THEN 'no rows on one side of 2023-01-03 in this sample'
            ELSE 'median implied price after/before 2023-01-03 = ' || CAST(round(post_px / NULLIF(pre_px, 0), 3) AS VARCHAR) END
FROM (
  SELECT
    COUNT(*) FILTER (WHERE d < DATE '2023-01-03') AS pre_n,
    COUNT(*) FILTER (WHERE d >= DATE '2023-01-03') AS post_n,
    median(px) FILTER (WHERE d < DATE '2023-01-03') AS pre_px,
    median(px) FILTER (WHERE d >= DATE '2023-01-03') AS post_px
  FROM (
    SELECT fm.filing_date AS d, h.value_thousands * 1000.0 / h.shares_or_principal AS px
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true) h
    JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true) fm
      ON fm.accession_number = h.accession_number
    WHERE h.shares_or_principal > 0 AND h.shares_or_principal_type = 'SH' AND h.value_thousands > 0
  )
) t;

SELECT '=== DQ RESULTS ===' AS section;
SELECT * FROM dq_results ORDER BY schema, tbl, test;

SELECT '=== SCHEMA SUMMARY ===' AS section;
SELECT
  schema,
  COUNT(*) AS total_tests,
  COUNT(*) FILTER (WHERE status = 'pass') AS passed,
  COUNT(*) FILTER (WHERE status = 'warn') AS warned,
  COUNT(*) FILTER (WHERE status = 'fail') AS failed,
  CASE WHEN COUNT(*) FILTER (WHERE status = 'fail') > 0 THEN 'FAIL' ELSE 'PASS' END AS overall
FROM dq_results
GROUP BY schema
ORDER BY schema;
