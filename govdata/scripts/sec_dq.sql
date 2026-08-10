-- dq-lookback: 1
-- sec_dq.sql — SEC Schema Data Quality
-- Follows the reference template established in weather_dq.sql / econ_dq.sql.
-- Usage: source .env.prod && envsubst < scripts/sec_dq.sql | duckdb
-- Output: structured dq_results table + per-table summary + schema-level pass/fail
--
-- DQ SAMPLE NOTE: SEC DQ runs against the CikRegistry DQ_SAMPLE group (worker.sh sec/
-- sec_prices DQ mode) — the MAGNIFICENT7 mega-caps + Berkshire Hathaway. Mega-caps supply
-- rich 10-K/10-Q/8-K/DEF 14A/insider data; Berkshire supplies Form 13F-HR (institutional_
-- holdings) and Schedule 13D/13G (beneficial_ownership) so every table gets real data. The
-- T2 floors below are CONSERVATIVE placeholders sized to catch zero/near-zero tables for this
-- ~8-CIK × DQ-window sample — calibrate them up to the actual counts after the first run
-- (the iterate loop), exactly as weather/econ did.

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
  status    VARCHAR,   -- pass | warn | fail
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

-- ============================================================
-- T1: EXISTENCE — checks for the Iceberg table's metadata via glob(), which returns 0 (never
-- throws) when a table directory is absent. This is RESILIENT: a missing table reports a row
-- instead of aborting the whole run, unlike iceberg_scan() which raises "Could not guess
-- version" and kills the enclosing statement. A present table → pass; a missing table → fail,
-- EXCEPT vectorized_chunks (warn-only: it needs the Jina embedding pipeline, environment-
-- dependent and legitimately absent in a DQ run without embedding credentials).
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec', tbl, 'existence',
  CASE WHEN nmeta > 0 THEN 'pass'
       WHEN tbl = 'vectorized_chunks' THEN 'warn'
       ELSE 'fail' END,
  CAST(nmeta AS VARCHAR), '1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (
  SELECT 'filing_metadata'       AS tbl, (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata/metadata/*.json'))       AS nmeta
  UNION ALL SELECT 'financial_line_items',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items/metadata/*.json'))
  UNION ALL SELECT 'filing_contexts',       (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts/metadata/*.json'))
  UNION ALL SELECT 'mda_sections',          (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections/metadata/*.json'))
  UNION ALL SELECT 'xbrl_relationships',    (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships/metadata/*.json'))
  UNION ALL SELECT 'insider_transactions',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions/metadata/*.json'))
  -- institutional_holdings (13F-HR) and beneficial_ownership (SC 13D/G) are owned by the
  -- sec_secondary DQ (sec_secondary_dq.sql); not checked here.
  UNION ALL SELECT 'earnings_transcripts',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts/metadata/*.json'))
  UNION ALL SELECT 'stock_prices',          (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices/metadata/*.json'))
  -- SEC chunks are promoted into ref.vectorized_chunks (source_schema='sec' partition); this
  -- schema no longer materializes its own vectorized_chunks Iceberg table, so existence is
  -- checked against the shared table's metadata instead.
  UNION ALL SELECT 'vectorized_chunks',     (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/ref/vectorized_chunks/metadata/*.json'))
);

-- ============================================================
-- T2: ROW COUNTS — conservative, sample-aware floors. Tiers:
--   rich (10-K/10-Q XBRL):  financial_line_items, filing_contexts, xbrl_relationships
--   moderate:               filing_metadata, insider_transactions, mda_sections,
--                           earnings_transcripts, stock_prices
-- (filer-type tables institutional_holdings (13F) / beneficial_ownership (13D/G) moved to
--  sec_secondary_dq.sql — sec no longer checks them.)
-- vectorized_chunks is EXCLUDED from T2 (existence-warn only) — embedding-pipeline dependent.
-- CALIBRATE these to the real DQ_SAMPLE counts after the first clean run. DQ_SAMPLE is now
-- FAANG + Berkshire (Meta, Apple, Amazon, Netflix, Alphabet, Berkshire) — re-derive ALL T2
-- floors from that 6-CIK set, not the old 8-CIK Mag7+Berkshire. Two prerequisites before
-- re-deriving: (1) a jar rebuild so the new CikRegistry/cik-registry.json take effect;
-- (2) stock_prices/sec_prices depend on BULK_STOCK_PRICE_ZIP (d_us_txt.zip on the local MinIO)
-- — without the bulk feed those stay 0 regardless of CIK set. xbrl_relationships/
-- financial_line_items/mda_sections/filing_contexts come from the XBRL transformer over the
-- sampled 10-K/10-Q filings; if still 0 after a clean FAANG run, that's a transformer/coverage
-- issue, not a threshold one.
-- ============================================================
-- Each table is a SEPARATE INSERT so a missing table's iceberg_scan() error aborts only that
-- one statement; the DuckDB CLI continues to the next (a missing table is already flagged FAIL
-- by T1). A present-but-empty table reads 0 here and fails the floor. vectorized_chunks is
-- EXCLUDED (existence-warn only — embedding-pipeline dependent).
SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results SELECT 'sec','filing_metadata','row_count', CASE WHEN n>=200 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'200', CASE WHEN n>=200 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','financial_line_items','row_count', CASE WHEN n>=2000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'2000', CASE WHEN n>=2000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','filing_contexts','row_count', CASE WHEN n>=1000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1000', CASE WHEN n>=1000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','mda_sections','row_count', CASE WHEN n>=20 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'20', CASE WHEN n>=20 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','xbrl_relationships','row_count', CASE WHEN n>=1000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1000', CASE WHEN n>=1000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','insider_transactions','row_count', CASE WHEN n>=200 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'200', CASE WHEN n>=200 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions', allow_moved_paths=true)) t;
-- institutional_holdings (13F-HR) and beneficial_ownership (SC 13D/G) are owned by the
-- sec_secondary DQ (sec_secondary_dq.sql); their row-count floors live there, not here.
INSERT INTO dq_results SELECT 'sec','earnings_transcripts','row_count', CASE WHEN n>=10 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10', CASE WHEN n>=10 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','stock_prices','row_count', CASE WHEN n>=10000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10000', CASE WHEN n>=10000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true)) t;


-- ============================================================================
-- T1/T2 — tables previously absent from this file entirely.
-- A table with no checks emits no dq rows, which reads as healthy rather than as
-- untested; that is how four zero-row geo tables went unnoticed. Existence +
-- row_count only — deliberately minimal, to be deepened per table.
-- ============================================================================

-- institutional_holdings (13F-HR) is a periodic quarterly filing for any active manager in
-- the sample, so zero rows is always a real gap: fail.
INSERT INTO dq_results
SELECT 'sec', 'institutional_holdings', 'existence',
       CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
       n, 1,
       CASE WHEN n > 0 THEN 'readable' ELSE 'NO ROWS — table unreadable or never written' END
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths := true) LIMIT 1)) t;

-- beneficial_ownership (SC 13D/G) is event-driven, not periodic — a filer that crosses no
-- new 5%-ownership threshold in the sample window files nothing. Zero rows over a narrow
-- CIK/year sample is a plausible true state (confirmed live for CIK 0001067983 in 2026: 0
-- SC 13D/G filings via EDGAR submissions.json), so this warns rather than fails.
INSERT INTO dq_results
SELECT 'sec', 'beneficial_ownership', 'existence',
       CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
       n, 1,
       CASE WHEN n > 0 THEN 'readable'
            ELSE '0 rows — plausible for an event-driven filing over a narrow sample; '
                 || 'verify against EDGAR submissions.json before treating as a gap' END
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership', allow_moved_paths := true) LIMIT 1)) t;

-- ============================================================================
-- T3: REFERENTIAL — every filing's rows must have a filing_metadata row.
--
-- Existence, row counts and PK uniqueness all pass while a filing is partly missing.
-- Materialization is per table, so one table can lose a filing's rows while its siblings
-- keep theirs — a staged batch file overwritten before it was absorbed takes exactly one
-- table's slice with it. Nothing downstream notices: the table is present, non-empty and
-- uniquely keyed. The only trace left in the data is the sibling rows now pointing at a
-- filing_metadata row that is not there.
--
-- Counts DISTINCT (cik, accession_number) tuples, not rows: one missing parent orphans
-- every child row referencing it, so a row count would report the size of the child table
-- rather than the size of the loss. Partially-null keys are excluded — a null key is not a
-- reference. EXCEPT rather than NOT EXISTS: the same anti-join expressed as a correlated
-- subquery is silently mis-planned on the Calcite read path, and phrasing it as a set
-- difference keeps this check meaning the same thing wherever it is run.
--
-- Status is 'warn', not 'fail'. The historical backlog is real and large (~31.6k orphans in
-- insider_transactions alone at the time of writing) and is being cleared by fix-sec.sh.
-- Promote to 'fail' once that backfill is complete, so this gates future regressions rather
-- than sitting permanently red and training everyone to ignore it.
-- ============================================================================
SELECT '=== T3: REFERENTIAL ===' AS section;

INSERT INTO dq_results
WITH parent AS (
  SELECT DISTINCT cik, accession_number
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths => true)
), orphans AS (
  SELECT 'financial_line_items' AS tbl, COUNT(*) AS n FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
  UNION ALL
  SELECT 'filing_contexts', COUNT(*) FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
  UNION ALL
  SELECT 'mda_sections', COUNT(*) FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
  UNION ALL
  SELECT 'xbrl_relationships', COUNT(*) FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
  UNION ALL
  SELECT 'insider_transactions', COUNT(*) FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
  UNION ALL
  SELECT 'earnings_transcripts', COUNT(*) FROM (
    SELECT DISTINCT cik, accession_number
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts', allow_moved_paths => true)
     WHERE cik IS NOT NULL AND accession_number IS NOT NULL
    EXCEPT SELECT cik, accession_number FROM parent)
)
SELECT 'sec', tbl, 'orphan_refs',
       CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
       CAST(n AS VARCHAR), '0',
       CASE WHEN n = 0 THEN 'every filing has a filing_metadata row'
            ELSE CAST(n AS VARCHAR) || ' filing(s) present here but missing from filing_metadata' END
FROM orphans;

-- ============================================================================
-- T4: EXTRACTION QUALITY — regression gates for defects found and fixed in this
-- investigation. Each check pins a specific bug so it fails loudly if it ever
-- recurs, instead of surfacing only as a downstream "why is this null" question.
--
-- company_name concatenated with address (grey zone, fixed in cb8608c99):
-- Node.getTextContent() on a <filingManager> container returns the name AND its
-- nested <address> run together with no separator. The garbled form is long
-- (a name plus street/city/state/zip) and carries multiple consecutive spaces
-- where the address fields were joined — a clean company name has neither.
--
-- wrong document fetched (item 4, fixed in aabff1033): a 13F-HR whose primary
-- document was EDGAR's HTML viewer rendering instead of the real XML parsed to
-- entirely empty fields. Checked as an elevated NULL rate on company_name
-- specifically for 13F-HR filings (the form this bug affected).
--
-- unresolvable relationship concept names: reading concepts from the filing
-- document instead of the linkbase produced locator labels and prefix-stripped
-- local names instead of "prefix:LocalName" — 61% of stored concepts were not
-- resolvable. A qualified XBRL concept name always contains ':'.
-- ============================================================================
SELECT '=== T4: EXTRACTION QUALITY ===' AS section;

INSERT INTO dq_results
SELECT 'sec', 'filing_metadata', 'company_name_not_garbled',
       CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
       CAST(n AS VARCHAR), '0',
       CASE WHEN n = 0 THEN 'no company_name values look like a concatenated address'
            ELSE CAST(n AS VARCHAR) || ' company_name value(s) are unusually long with '
                 || 'repeated internal spacing — looks like name+address run together' END
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true)
  WHERE company_name IS NOT NULL
    AND length(company_name) > 100
    AND regexp_matches(company_name, '\s{3,}')
) t;

INSERT INTO dq_results
SELECT 'sec', 'filing_metadata', 'thirteenf_company_name_present',
       CASE WHEN total = 0 THEN 'warn'
            WHEN null_rate <= 0.05 THEN 'pass' ELSE 'fail' END,
       CAST(null_rate AS VARCHAR), '0.05',
       CASE WHEN total = 0 THEN 'no 13F-HR filings in this sample'
            ELSE CAST(nulls AS VARCHAR) || ' of ' || CAST(total AS VARCHAR)
                 || ' 13F-HR filing_metadata rows have a null company_name' END
FROM (
  SELECT COUNT(*) AS total,
         COUNT(*) FILTER (WHERE company_name IS NULL) AS nulls,
         COALESCE(COUNT(*) FILTER (WHERE company_name IS NULL) * 1.0 / NULLIF(COUNT(*), 0), 0) AS null_rate
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true)
  WHERE filing_type = '13F-HR'
) t;

INSERT INTO dq_results
SELECT 'sec', 'institutional_holdings', 'manager_name_not_garbled',
       CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
       CAST(n AS VARCHAR), '0',
       CASE WHEN n = 0 THEN 'no manager_name values look like a concatenated address'
            ELSE CAST(n AS VARCHAR) || ' manager_name value(s) are unusually long with '
                 || 'repeated internal spacing — looks like name+address run together' END
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)
  WHERE manager_name IS NOT NULL
    AND length(manager_name) > 100
    AND regexp_matches(manager_name, '\s{3,}')
) t;

INSERT INTO dq_results
SELECT 'sec', 'xbrl_relationships', 'concept_names_qualified',
       CASE WHEN total = 0 THEN 'warn'
            WHEN unqualified_rate <= 0.05 THEN 'pass' ELSE 'fail' END,
       CAST(unqualified_rate AS VARCHAR), '0.05',
       CASE WHEN total = 0 THEN 'no xbrl_relationships rows in this sample'
            ELSE CAST(unqualified AS VARCHAR) || ' of ' || CAST(total AS VARCHAR)
                 || ' from_concept/to_concept values have no "prefix:LocalName" colon — '
                 || 'looks like a locator label or bare local name, not a resolvable concept' END
FROM (
  SELECT COUNT(*) AS total,
         COUNT(*) FILTER (WHERE from_concept NOT LIKE '%:%' OR to_concept NOT LIKE '%:%') AS unqualified,
         COALESCE(COUNT(*) FILTER (WHERE from_concept NOT LIKE '%:%' OR to_concept NOT LIKE '%:%') * 1.0
                  / NULLIF(COUNT(*), 0), 0) AS unqualified_rate
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships', allow_moved_paths=true)
) t;

-- ============================================================
-- REPORT
-- ============================================================
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
