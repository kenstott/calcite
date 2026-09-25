-- dq-lookback: 1
-- Law Data Quality Checks (U.S. Code)
-- Schema: law
-- Tables: usc_sections, lobbying_* (17 tables from LDA.gov, listed in law-schema.yaml), bills, bill_actions, bill_cosponsors, bill_committees,
--         bill_committee_activities, bill_subjects, bill_text_versions, bill_related_bills,
--         bill_amendments, bill_amendment_actions, bill_amendment_cosponsors,
--         bill_action_committees, bill_recorded_votes, bill_titles, bill_summaries,
--         bill_cbo_cost_estimates, bill_committee_reports, bill_notes, scotus_reports_cases,
--         scotus_slip_opinions, scotus_dockets, scotus_docket_entries
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition column 'title'; T5 also excludes 'release_point' (one value per run
-- by design: the whole table is replaced from a single OLRC release point).
-- The bill_* checks are scoped by Congress and bill type: T4/T5 exclude partition columns 'type',
-- 'congress' and 'bill_type'; row-count floors are low because a scoped run may hold few rows.

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
-- TABLE: usc_sections
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true));

-- T2: row_count. Parsing the 119-111 release point locally gave 60,497 sections (Titles 1-52,
-- 54, 5a, 18a); threshold sits below that observed count to allow for future repeals/omissions.
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T2_row_count',
  CASE WHEN n >= 58000 THEN 'pass' ELSE 'fail' END,
  n, 58000, 'Expected at least 58000 U.S. Code sections'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true));

-- T3: sample
SELECT title_number, section_number, citation, heading, status, LENGTH(section_text) AS text_len
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('title')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('title', 'release_point')
  )
);

-- T6: pk_nulls (every NOT NULL column)
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL title_number, title_name, is_positive_law, release_point, section_number, section_seq or citation rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
      WHERE title_number IS NULL OR title_name IS NULL OR is_positive_law IS NULL
         OR release_point IS NULL OR section_number IS NULL OR section_seq IS NULL
         OR citation IS NULL);

-- T6: primary key uniqueness (title_number, section_number, section_seq)
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (title_number, section_number, section_seq) keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT title_number, section_number, section_seq
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

-- T7: every ingested title present. Titles 1-52 and 54 plus the Title 5 and 18 appendices = 55.
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_title_coverage',
  CASE WHEN n = 55 THEN 'pass' ELSE 'warn' END,
  n, 55, 'Distinct title_number values (expect 55: Titles 1-52, 54, 5a, 18a)'
FROM (SELECT COUNT(DISTINCT title_number) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true));

-- T7: one release point per run
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_single_release_point',
  CASE WHEN n = 1 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Distinct release_point values (>1 means titles from different OLRC release points)'
FROM (SELECT COUNT(DISTINCT release_point) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true));

-- T7: release_point format ({congress}-{public law number})
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_release_point_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'release_point not matching expected {congress}-{release} format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
      WHERE NOT REGEXP_MATCHES(release_point, '^\d+-\d+$'));

-- T7: citation format (e.g. "5 U.S.C. § 101", "5A U.S.C. App. § 1")
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_citation_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'citation not matching "<title> U.S.C. [App. ]§ <section>"'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
      WHERE NOT REGEXP_MATCHES(citation, '^[0-9]+A? U\.S\.C\. (App\. )?§ .+$'));

-- T7: a section without operative text must carry a status, except the sections whose only
-- content is a pointer in a note: the 69 Title 18 "(Rule)" entries (index lines pointing at the
-- Federal Rules of Criminal Procedure) and 19 U.S.C. 1202 (the Harmonized Tariff Schedule is not
-- published in the Code). Confirmed against the 119-111 raw XML: their bodies are <note> elements,
-- which are excluded from section_text by design. More than a handful of other NULL-text,
-- NULL-status rows means the parser dropped real text (status values: repealed, omitted,
-- transferred, renumbered, reserved, vacant, unknown).
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_null_text_has_status',
  CASE WHEN bad <= 5 THEN 'pass' ELSE 'warn' END,
  bad, 5, 'Sections with NULL section_text and NULL status, excluding "(Rule)" pointer entries'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
      WHERE section_text IS NULL AND status IS NULL
        AND (heading IS NULL OR heading NOT LIKE '%(Rule)'));

-- T7: positive-law and non-positive-law titles both present
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_positive_law_both_values',
  CASE WHEN n = 2 THEN 'pass' ELSE 'warn' END,
  n, 2, 'Distinct is_positive_law values (expect true and false)'
FROM (SELECT COUNT(DISTINCT is_positive_law) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true));

-- T7: section text starts with its own citation (chunk context depends on the header line)
INSERT INTO dq_results
SELECT 'law', 'usc_sections', 'T7_text_header_matches_citation',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Sections whose section_text does not start with their citation'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true)
      WHERE section_text IS NOT NULL AND NOT STARTS_WITH(section_text, citation));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bills
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bills', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true));

-- T2: row_count (sconres alone is ~40 bills per Congress; every Congress has thousands overall)
INSERT INTO dq_results
SELECT 'law', 'bills', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bills', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'law_type', 'law_number')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bills', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- law_type is nearly constant: Public Law, with Private Law rare.
      -- on_behalf_of_type is always "Introduced on behalf of" when set.
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'on_behalf_of_type', 'law_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bills', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bills', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number
    HAVING COUNT(*) > 1
  )
);

-- T7: bill_type is one of the eight known slugs
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_bill_type_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_type outside hr, s, hjres, sjres, hconres, sconres, hres, sres'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
      WHERE bill_type NOT IN ('hr', 's', 'hjres', 'sjres', 'hconres', 'sconres', 'hres', 'sres'));

-- T7: date columns are YYYY-MM-DD
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_date_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'introduced_date / latest_action_date not matching ^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
      WHERE (introduced_date IS NOT NULL AND NOT regexp_matches(introduced_date, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))
         OR (latest_action_date IS NOT NULL AND NOT regexp_matches(latest_action_date, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')));

-- T7: sponsor bioguide IDs have the standard shape
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_sponsor_bioguide_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'sponsor_bioguide_id not matching ^[A-Z][0-9]{6}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
      WHERE sponsor_bioguide_id IS NOT NULL AND NOT regexp_matches(sponsor_bioguide_id, '^[A-Z][0-9]{6}$'));

-- T7: a law_number implies a law_type (both come from the same laws/item)
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_law_fields_paired',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with exactly one of law_type / law_number set'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true)
      WHERE (law_type IS NULL) <> (law_number IS NULL));

-- T7: bills.action_count equals the bill_actions rows for the same bill
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_action_count_matches_bill_actions',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Bills whose action_count differs from the bill_actions row count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
  LEFT JOIN (SELECT congress, bill_type, bill_number, COUNT(*) AS c
             FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true) GROUP BY congress, bill_type, bill_number) a
    ON a.congress = b.congress AND a.bill_type = b.bill_type AND a.bill_number = b.bill_number
  WHERE b.action_count <> COALESCE(a.c, 0)
);

-- T7: bills.cosponsor_count equals the bill_cosponsors rows for the same bill
INSERT INTO dq_results
SELECT 'law', 'bills', 'T7_cosponsor_count_matches_bill_cosponsors',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Bills whose cosponsor_count differs from the bill_cosponsors row count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
  LEFT JOIN (SELECT congress, bill_type, bill_number, COUNT(*) AS c
             FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true) GROUP BY congress, bill_type, bill_number) a
    ON a.congress = b.congress AND a.bill_type = b.bill_type AND a.bill_number = b.bill_number
  WHERE b.cosponsor_count <> COALESCE(a.c, 0)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_actions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true));

-- T2: row_count (a single bill type in one Congress has hundreds of actions)
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, action_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR action_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, action_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, action_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, action_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: every action belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T7_orphan_actions',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_actions rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true) a
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON a.congress = b.congress AND a.bill_type = b.bill_type AND a.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: action_seq runs 1..n with no gaps per bill
INSERT INTO dq_results
SELECT 'law', 'bill_actions', 'T7_action_seq_contiguous',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Bills where max(action_seq) differs from the action row count'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number
    HAVING MAX(action_seq) <> COUNT(*)
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_cosponsors
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true));

-- T2: row_count (popular bills carry hundreds of cosponsors)
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'withdrawn_date')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, bioguide_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR bioguide_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Duplicate (bill, bioguide_id, sponsorship_date) rows — a member who withdrew and re-signed is two rows with different dates; identical repeats are upstream (2 in the 108th-119th)'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, bioguide_id, sponsorship_date, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, bioguide_id, sponsorship_date
    HAVING COUNT(*) > 1
  )
);

-- T7: cosponsor bioguide IDs have the standard shape
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T7_bioguide_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'bioguide_id not matching ^[A-Z][0-9]{6}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true)
      WHERE NOT regexp_matches(bioguide_id, '^[A-Z][0-9]{6}$'));

-- T7: every cosponsor row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_cosponsors', 'T7_orphan_cosponsors',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_cosponsors rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cosponsors', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_committees
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true));

-- T2: row_count (every bill is referred to at least one committee)
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'parent_committee_system_code')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, committee_system_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR committee_system_code IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, committee_system_code) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, committee_system_code, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, committee_system_code
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_committees row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_committees rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: committee codes look like hsju00 / ssfi00
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T7_committee_code_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'committee_system_code not matching ^[a-z]{4}[0-9]{2}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true)
      WHERE NOT regexp_matches(committee_system_code, '^[a-z]{4}[0-9]{2}$'));

-- T7: a subcommittee's parent is a full committee on the same bill
INSERT INTO dq_results
SELECT 'law', 'bill_committees', 'T7_subcommittee_parent_exists',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Subcommittee rows whose parent_committee_system_code is not a committee row for the same bill'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true) s
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true) p
    ON p.congress = s.congress AND p.bill_type = s.bill_type AND p.bill_number = s.bill_number
   AND p.committee_system_code = s.parent_committee_system_code
  WHERE s.parent_committee_system_code IS NOT NULL AND p.committee_system_code IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_committee_activities
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true));

-- T2: row_count (nearly every bill has a Referred To activity)
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, committee_system_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR committee_system_code IS NULL);

-- T7: every bill_committee_activities row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_committee_activities rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: every activity belongs to a committee row on the same bill
INSERT INTO dq_results
SELECT 'law', 'bill_committee_activities', 'T7_activity_committee_exists',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Activity rows whose committee_system_code is not in bill_committees for the same bill'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_activities', allow_moved_paths := true) a
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committees', allow_moved_paths := true) c
    ON c.congress = a.congress AND c.bill_type = a.bill_type AND c.bill_number = a.bill_number
   AND c.committee_system_code = a.committee_system_code
  WHERE c.committee_system_code IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_subjects
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true));

-- T2: row_count (bills typically carry several subject terms)
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, subject_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR subject_name IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, subject_name) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, subject_name, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, subject_name
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_subjects row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_subjects', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_subjects rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_subjects', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_text_versions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true));

-- T2: row_count (every bill has at least an introduced text version)
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'format_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- format_type is only one non-null value exists (United States Legislative Markup); the plain XML rows are NULL.
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'format_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, version_type rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR version_type IS NULL);

-- T6: pk_duplicates — (version_type, format_type, url); NULLs group together. Upstream repeats a few identical versions, so this is a warning.
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Duplicate (bill, version_type, format_type, url) rows — the source lists a few versions twice (3 bills in the sampled Congresses), so this warns'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, version_type, format_type, url, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, version_type, format_type, url
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_text_versions row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_text_versions rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: text URLs point at govinfo.gov (NULL is legitimate: version not yet published)
INSERT INTO dq_results
SELECT 'law', 'bill_text_versions', 'T7_url_host',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'url not starting with https://www.govinfo.gov/'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_text_versions', allow_moved_paths := true)
      WHERE url IS NOT NULL AND NOT starts_with(url, 'https://www.govinfo.gov/'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_related_bills
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true));

-- T2: row_count (a scoped run may hold few related bills)
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'related_latest_action_time')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, related_congress, related_bill_type, related_bill_number, relationship_type, identified_by rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR related_congress IS NULL OR related_bill_type IS NULL OR related_bill_number IS NULL);

-- T7: every bill_related_bills row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_related_bills rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: related_bill_type is one of the eight known slugs (joins to bills.bill_type)
INSERT INTO dq_results
SELECT 'law', 'bill_related_bills', 'T7_related_bill_type_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'related_bill_type outside hr, s, hjres, sjres, hconres, sconres, hres, sres'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_related_bills', allow_moved_paths := true)
      WHERE related_bill_type NOT IN ('hr', 's', 'hjres', 'sjres', 'hconres', 'sconres', 'hres', 'sres'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_amendments
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true));

-- T2: row_count (many bill types carry no amendments, so a scoped run can hold few)
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'sponsor_name', 'sponsor_district', 'description', 'purpose', 'proposed_date', 'latest_action_date', 'latest_action_text', 'amended_amendment_type', 'amended_amendment_number')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- sponsor_name, amended_amendment_type: rare (committee sponsors; amendments to amendments), so one Congress can hold a single value.
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'sponsor_name', 'amended_amendment_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, amendment_type, amendment_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR amendment_type IS NULL OR amendment_number IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, amendment_type, amendment_number) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, amendment_type, amendment_number, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, amendment_type, amendment_number
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_amendments row belongs to a bill in the bills table
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T7_orphan_rows',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_amendments rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.bill_number IS NULL
);

-- T7: amendment_type is SAMDT or HAMDT (the only types observed in the 119th)
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T7_amendment_type_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'amendment_type other than SAMDT / HAMDT — confirm it is a real new type before treating as a parsing bug'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true)
      WHERE amendment_type NOT IN ('SAMDT', 'HAMDT'));

-- T7: a sponsor is either a member (bioguide) or a committee (name), never neither
INSERT INTO dq_results
SELECT 'law', 'bill_amendments', 'T7_sponsor_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Amendments with neither sponsor_bioguide_id nor sponsor_name'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true)
      WHERE sponsor_bioguide_id IS NULL AND sponsor_name IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_amendment_actions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true));

-- T2: row_count (many bill types carry no amendments)
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'action_time', 'action_code', 'text', 'action_type', 'source_system')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, amendment_type, amendment_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR amendment_type IS NULL OR amendment_number IS NULL);

-- T7: every bill_amendment_actions row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_amendment_actions rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: every bill_amendment_actions row belongs to a row in bill_amendments
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_actions', 'T7_orphan_rows_bill_amendments',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_amendment_actions rows with no matching bill_amendments row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_actions', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number AND c.amendment_type = b.amendment_type AND c.amendment_number = b.amendment_number
  WHERE b.congress IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_amendment_cosponsors
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true));

-- T2: row_count (only Senate amendments carry cosponsors)
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'withdrawn_date')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- amendment_type is always SAMDT: only Senate amendments carry cosponsors.
      -- withdrawn_date: rare (a handful of withdrawals per Congress), so a single Congress can hold one distinct value.
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'withdrawn_date', 'amendment_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, amendment_type, amendment_number, bioguide_id, sponsorship_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR amendment_type IS NULL OR amendment_number IS NULL OR bioguide_id IS NULL);

-- T6: pk_duplicates (zero duplicates across the 108th-119th)
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, amendment_type, amendment_number, bioguide_id, sponsorship_date) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, amendment_type, amendment_number, bioguide_id, sponsorship_date, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, amendment_type, amendment_number, bioguide_id, sponsorship_date
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_amendment_cosponsors row belongs to a row in bill_amendments
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T7_orphan_rows_bill_amendments',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_amendment_cosponsors rows with no matching bill_amendments row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number AND c.amendment_type = b.amendment_type AND c.amendment_number = b.amendment_number
  WHERE b.congress IS NULL
);

-- T7: cosponsor bioguide IDs have the standard shape
INSERT INTO dq_results
SELECT 'law', 'bill_amendment_cosponsors', 'T7_bioguide_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'bioguide_id not matching ^[A-Z][0-9]{6}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendment_cosponsors', allow_moved_paths := true)
      WHERE NOT regexp_matches(bioguide_id, '^[A-Z][0-9]{6}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_action_committees
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true));

-- T2: row_count (referral actions name a committee)
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, action_seq, committee_system_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR action_seq IS NULL OR committee_system_code IS NULL);

-- T6: pk_duplicates (zero duplicates across the 108th-119th)
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, action_seq, committee_system_code) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, action_seq, committee_system_code, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, action_seq, committee_system_code
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_action_committees row belongs to a row in bill_actions
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T7_orphan_rows_bill_actions',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_action_committees rows with no matching bill_actions row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_actions', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number AND c.action_seq = b.action_seq
  WHERE b.congress IS NULL
);

-- T7: committee codes look like hsju00 / ssfi00
INSERT INTO dq_results
SELECT 'law', 'bill_action_committees', 'T7_committee_code_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'committee_system_code not matching ^[a-z]{4}[0-9]{2}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_action_committees', allow_moved_paths := true)
      WHERE NOT regexp_matches(committee_system_code, '^[a-z]{4}[0-9]{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_recorded_votes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true));

-- T2: row_count (not every bill has a roll-call vote)
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'full_action_name', 'amendment_type', 'amendment_number')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- vote_congress: equals the partition Congress, so it is single-valued in a one-Congress run.
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'vote_congress')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, vote_chamber, roll_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR vote_chamber IS NULL OR roll_number IS NULL);

-- T7: every bill_recorded_votes row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_recorded_votes rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: a vote on an amendment names an amendment in bill_amendments
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T7_amendment_vote_has_amendment',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Amendment votes with no matching bill_amendments row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_amendments', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
   AND c.amendment_type = b.amendment_type AND c.amendment_number = b.amendment_number
  WHERE c.amendment_number IS NOT NULL AND b.congress IS NULL
);

-- T7: chamber and session values
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T7_chamber_session_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'vote_chamber outside House/Senate or session_number outside 1/2'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true)
      WHERE vote_chamber NOT IN ('House', 'Senate') OR session_number NOT IN (1, 2));

-- T7: roll-call URLs are http(s); NULL is legitimate only rarely (one row across 108th-119th)
INSERT INTO dq_results
SELECT 'law', 'bill_recorded_votes', 'T7_url_shape',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'url not starting with http'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_recorded_votes', allow_moved_paths := true)
      WHERE url IS NOT NULL AND NOT starts_with(url, 'http'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_titles
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true));

-- T2: row_count (every bill has at least a display title)
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'title_type_code', 'bill_text_version_code', 'bill_text_version_name', 'chamber_code', 'chamber_name', 'parent_title_type', 'source_system', 'update_date')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- source_system is set on very few rows, always to the same value (Library of Congress).
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'source_system')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, title_type, title rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR title_type IS NULL OR title IS NULL);

-- T7: every bill_titles row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_titles rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: every bill has a Display Title
INSERT INTO dq_results
SELECT 'law', 'bill_titles', 'T7_display_title_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Bills with no Display Title row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
  LEFT JOIN (SELECT DISTINCT congress, bill_type, bill_number
             FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_titles', allow_moved_paths := true) WHERE title_type = 'Display Title') t
    ON t.congress = b.congress AND t.bill_type = b.bill_type AND t.bill_number = b.bill_number
  WHERE t.bill_number IS NULL
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_summaries
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true));

-- T2: row_count (CRS summarises only some bills)
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'summary_name', 'last_summary_update_date')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, version_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR version_code IS NULL);

-- T6: pk_duplicates (zero duplicates across the 108th-119th)
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, bill_type, bill_number, version_code) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, bill_type, bill_number, version_code, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true)
    GROUP BY congress, bill_type, bill_number, version_code
    HAVING COUNT(*) > 1
  )
);

-- T7: every bill_summaries row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_summaries rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: every summary carries text
INSERT INTO dq_results
SELECT 'law', 'bill_summaries', 'T7_summary_text_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Summary rows with NULL or empty summary_text'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_summaries', allow_moved_paths := true)
      WHERE summary_text IS NULL OR length(trim(summary_text)) = 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_cbo_cost_estimates
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true));

-- T2: row_count (CBO estimates only some bills)
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'description')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, url rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR url IS NULL);

-- T7: every bill_cbo_cost_estimates row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_cbo_cost_estimates rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: CBO URLs point at cbo.gov (the same estimate can appear twice with differently
-- encoded titles upstream, so uniqueness is not asserted)
INSERT INTO dq_results
SELECT 'law', 'bill_cbo_cost_estimates', 'T7_url_host',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'url not containing cbo.gov'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_cbo_cost_estimates', allow_moved_paths := true)
      WHERE url IS NOT NULL AND NOT contains(url, 'cbo.gov'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_committee_reports
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true));

-- T2: row_count (only reported bills have committee reports)
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, citation rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR citation IS NULL);

-- T7: every bill_committee_reports row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_committee_reports rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: citations look like "H. Rept. 108-753" / "S. Rept. 109-35"
INSERT INTO dq_results
SELECT 'law', 'bill_committee_reports', 'T7_citation_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'citation not matching ^[HS]\. Rept\. [0-9]+-[0-9]+'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_committee_reports', allow_moved_paths := true)
      WHERE NOT regexp_matches(citation, '^[HS]\. Rept\. [0-9]+-[0-9]+'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: bill_notes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true));

-- T2: row_count (only some bills carry an editorial note)
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected at least 1 rows in a scoped run'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type', 'link_name', 'link_url')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress', 'bill_type')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress, bill_type, bill_number, note_text rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true)
      WHERE congress IS NULL OR bill_type IS NULL OR bill_number IS NULL OR note_text IS NULL);

-- T7: every bill_notes row belongs to a row in bills
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T7_orphan_rows_bills',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'bill_notes rows with no matching bills row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true) c
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bills', allow_moved_paths := true) b
    ON c.congress = b.congress AND c.bill_type = b.bill_type AND c.bill_number = b.bill_number
  WHERE b.congress IS NULL
);

-- T7: link_name and link_url are set together
INSERT INTO dq_results
SELECT 'law', 'bill_notes', 'T7_link_fields_paired',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with exactly one of link_name / link_url set'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/bill_notes', allow_moved_paths := true)
      WHERE (link_name IS NULL) <> (link_url IS NULL));

-- ────────────────────────────────────────────────────────────
-- TABLE: scotus_reports_cases
-- ────────────────────────────────────────────────────────────
-- Partitioned by year (the Court term); a scoped run may hold a single term, so the row-count
-- floor is low. T4/T5 exclude the partition columns 'type' and 'year'.

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true));

-- T2: row_count. Term 2017 (volume 583) alone holds 20 cases; all 582 volumes hold 30,788.
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T2_row_count',
  CASE WHEN n >= 15 THEN 'pass' ELSE 'fail' END,
  n, 15, 'Expected at least 15 cases (one term)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true));

-- T3: sample
SELECT year, volume, us_citation, case_title, docket_numbers, disposition, disposition_source,
       page_count, LENGTH(opinion_text) AS text_len
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (every NOT NULL column)
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL volume, granule_id, case_title, us_citation, first_page, decision_year, page_count or pdf_url rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
      WHERE volume IS NULL OR granule_id IS NULL OR case_title IS NULL OR us_citation IS NULL
         OR first_page IS NULL OR decision_year IS NULL OR page_count IS NULL OR pdf_url IS NULL);

-- T6: primary key uniqueness (granule_id)
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate granule_id keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT granule_id
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
        GROUP BY 1 HAVING COUNT(*) > 1));

-- T7: us_citation agrees with volume and first_page ("583 U.S. 17")
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_citation_matches_volume_page',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows whose us_citation is not "<volume> U.S. <first_page>"'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
      WHERE us_citation <> CAST(volume AS VARCHAR) || ' U.S. ' || CAST(first_page AS VARCHAR));

-- T7: disposition_source is set exactly when disposition is
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_disposition_source_paired',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows where disposition and disposition_source are not both set or both null'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
      WHERE (disposition IS NULL) <> (disposition_source IS NULL)
         OR (disposition_source IS NOT NULL AND disposition_source NOT IN ('govinfo_mods', 'opinion_text')));

-- T7: disposition uses the documented vocabulary (an unseen curated SCDB value is a warn to review)
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_disposition_vocabulary',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'All dispositions in the documented vocabulary' ELSE 'Unexpected: ' || vals END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(disposition, '; ') AS vals
  FROM (
    SELECT DISTINCT disposition
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
    WHERE disposition IS NOT NULL
      AND disposition NOT IN ('Affirmed (includes modified)', 'Reversed', 'Reversed and remanded',
            'Vacated', 'Vacated and remanded', 'Affirmed and reversed (or vacated) in part',
            'Affirmed and reversed (or vacated) in part and remanded',
            'Petition denied or appeal dismissed', 'Dismissed as improvidently granted')
  )
);

-- T7: text-derived and curated outcomes agree where the curated value carries party_winning:
-- a volume-583 case (no MODS outcome) must have been derived from the text, never curated
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_volume_583_outcome_is_text_derived',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Volume 583 rows whose disposition_source is not opinion_text'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
      WHERE volume >= 583 AND disposition IS NOT NULL AND disposition_source <> 'opinion_text');

-- T7: docket numbers were read for (almost) every case; a merits case with none is worth a look
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_docket_numbers_present',
  CASE WHEN pct >= 0.95 THEN 'pass' ELSE 'warn' END,
  pct, 0.95, 'Share of cases with at least one docket number read from the opinion'
FROM (SELECT COUNT(*) FILTER (WHERE docket_numbers IS NOT NULL AND len(docket_numbers) > 0) * 1.0 / COUNT(*) AS pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true));

-- T7: opinion text was extracted (no empty or trivially short text)
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_opinion_text_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with no opinion_text or fewer than 500 characters'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
      WHERE opinion_text IS NULL OR LENGTH(opinion_text) < 500);

-- T7: extracted text is not letter-spaced ("N o. 1 1 – 2 0 4"): no case where most tokens are one character
INSERT INTO dq_results
SELECT 'law', 'scotus_reports_cases', 'T7_text_not_letter_spaced',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows whose first 2,000 characters look letter-spaced (>40% one-character tokens)'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT granule_id,
           list_filter(string_split(regexp_replace(substr(opinion_text, 1, 2000), '\s+', ' ', 'g'), ' '), x -> x <> '') AS toks
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_reports_cases', allow_moved_paths := true)
    WHERE opinion_text IS NOT NULL)
  WHERE len(toks) >= 20 AND len(list_filter(toks, x -> len(x) = 1)) > 0.4 * len(toks)
);

-- ────────────────────────────────────────────────────────────
-- TABLE: scotus_slip_opinions
-- ────────────────────────────────────────────────────────────
-- Partitioned by year (the Court term). A scoped run may hold one term (~60-75 opinions).

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true));

-- T2: row_count. A full term has 56-76 opinions; a partial current term may have fewer.
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END,
  n, 20, 'Expected at least 20 opinions (one term)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true));

-- T3: sample
SELECT year, release, decision_date, listing_docket, docket_numbers, case_name, disposition,
       us_citation, page_count, LENGTH(opinion_text) AS text_len
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value (pdf_page and the citation columns are legitimately constant in a small sample)
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (every NOT NULL column)
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL release, decision_date, listing_docket, case_name, author_initials, pdf_url or page_count rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
      WHERE release IS NULL OR decision_date IS NULL OR listing_docket IS NULL OR case_name IS NULL
         OR author_initials IS NULL OR pdf_url IS NULL OR page_count IS NULL);

-- T6: primary key uniqueness (year, release)
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (year, release) keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT year, release
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
        GROUP BY 1, 2 HAVING COUNT(*) > 1));

-- T7: the lead docket read from the opinion text agrees with the docket the listing gives
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_docket_matches_listing',
  CASE WHEN pct >= 0.95 THEN 'pass' ELSE 'warn' END,
  pct, 0.95, 'Share of opinions whose first docket_numbers entry equals the listing docket'
FROM (SELECT COUNT(*) FILTER (WHERE len(docket_numbers) > 0
                               AND docket_numbers[1] = replace(listing_docket, ', Orig.', '-Orig')) * 1.0
             / COUNT(*) AS pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true));

-- T7: a case's page range in a volume PDF is a plausible size (an unbounded slice would run to the end of the volume)
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_page_count_plausible',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Opinions with page_count over 250, which suggests the slice did not stop at the next case'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true) WHERE page_count > 250);

-- T7: disposition_source is set exactly when disposition is, and is opinion_text
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_disposition_source',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows where disposition and disposition_source are not paired, or the source is not opinion_text'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
      WHERE (disposition IS NULL) <> (disposition_source IS NULL)
         OR (disposition_source IS NOT NULL AND disposition_source <> 'opinion_text'));

-- T7: disposition uses the documented vocabulary
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_disposition_vocabulary',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'All dispositions in the documented vocabulary' ELSE 'Unexpected: ' || vals END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(disposition, '; ') AS vals
  FROM (
    SELECT DISTINCT disposition
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
    WHERE disposition IS NOT NULL
      AND disposition NOT IN ('Affirmed (includes modified)', 'Reversed', 'Reversed and remanded',
            'Vacated', 'Vacated and remanded', 'Affirmed and reversed (or vacated) in part',
            'Affirmed and reversed (or vacated) in part and remanded',
            'Reversed in part and remanded', 'Reversed in part, vacated in part, and remanded',
            'Vacated in part and remanded', 'Reversed in part and remanded in part',
            'Dismissed as improvidently granted', 'Application granted', 'Application denied',
            'Application granted in part', 'Application denied in part')
  )
);

-- T7: most opinions have a derived disposition (original-jurisdiction decrees legitimately have none)
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_disposition_present',
  CASE WHEN pct >= 0.9 THEN 'pass' ELSE 'warn' END,
  pct, 0.9, 'Share of opinions with a derived disposition'
FROM (SELECT COUNT(*) FILTER (WHERE disposition IS NOT NULL) * 1.0 / COUNT(*) AS pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true));

-- T7: us_citation agrees with volume and first_page
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_citation_matches_volume_page',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows whose us_citation is not "<volume> U.S. <first_page>", or set without a page'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
      WHERE (us_citation IS NULL) <> (first_page IS NULL)
         OR (us_citation IS NOT NULL
             AND us_citation <> CAST(volume AS VARCHAR) || ' U.S. ' || CAST(first_page AS VARCHAR)));

-- T7: opinion text was extracted and is not letter-spaced
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_opinion_text_present',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with no opinion_text or fewer than 500 characters'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
      WHERE opinion_text IS NULL OR LENGTH(opinion_text) < 500);

-- T7: decision dates fall in the term: October of `year` through September of `year` + 1
INSERT INTO dq_results
SELECT 'law', 'scotus_slip_opinions', 'T7_decision_date_in_term',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Opinions decided outside October of the term year through September of the next'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true)
      WHERE decision_date < CAST(year AS VARCHAR) || '-09-01'
         OR decision_date > CAST(CAST(year AS INTEGER) + 1 AS VARCHAR) || '-10-31');

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_filings
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads 1,000 filings per quarter window)
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'income', 'expenses', 'expenses_method', 'expenses_method_name', 'termination_date', 'registrant_house_id', 'registrant_description', 'registrant_address_2', 'registrant_ppb_country', 'registrant_different_address', 'client_general_description', 'client_ppb_state', 'client_ppb_country', 'client_government_entity', 'client_self_select')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'registrant_ppb_country')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true)
      WHERE filing_uuid IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true)
    GROUP BY filing_uuid
    HAVING COUNT(*) > 1
  )
);

-- T7: filings whose dt_posted falls outside the year/quarter partition they are in
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_posted_date_in_partition',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'filings whose dt_posted falls outside the year/quarter partition they are in'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) WHERE CAST(substr(dt_posted, 1, 4) AS INTEGER) <> year OR CAST(quarter AS INTEGER) <> ((CAST(substr(dt_posted, 6, 2) AS INTEGER) - 1) // 3) + 1);

-- T7: filings with negative income or expenses
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_money_not_negative',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'filings with negative income or expenses'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) WHERE income < 0 OR expenses < 0);

-- T7: filings whose filing_year is before 1999 or after the posting year
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_filing_year_plausible',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'filings whose filing_year is before 1999 or after the posting year'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) WHERE filing_year < 1999 OR filing_year > year);

-- T7: lobbying_filings.filing_type values missing from lobbying_filing_types
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_filing_type_decodes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'lobbying_filings.filing_type values missing from lobbying_filing_types'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true) r ON c.filing_type = r.filing_type WHERE c.filing_type IS NOT NULL AND r.filing_type IS NULL);

-- T7: lobbying_filings.activity_count differs from the number of lobbying_activities rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_activity_count_matches_lobbying_activities',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_filings.activity_count differs from the number of lobbying_activities rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.activity_count <> COALESCE(a.c, 0));

-- T7: lobbying_filings.foreign_entity_count differs from the number of lobbying_foreign_entities rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_foreign_entity_count_matches_lobbying_foreign_entities',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_filings.foreign_entity_count differs from the number of lobbying_foreign_entities rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.foreign_entity_count <> COALESCE(a.c, 0));

-- T7: lobbying_filings.affiliated_organization_count differs from the number of lobbying_affiliated_organizations rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_affiliated_organization_count_matches_lobbying_affiliated_organizations',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_filings.affiliated_organization_count differs from the number of lobbying_affiliated_organizations rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.affiliated_organization_count <> COALESCE(a.c, 0));

-- T7: lobbying_filings.conviction_disclosure_count differs from the number of lobbying_conviction_disclosures rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_filings', 'T7_conviction_disclosure_count_matches_lobbying_conviction_disclosures',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_filings.conviction_disclosure_count differs from the number of lobbying_conviction_disclosures rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.conviction_disclosure_count <> COALESCE(a.c, 0));

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_activities
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads 1,000 filings per quarter window)
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'foreign_entity_issues', 'description')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'foreign_entity_issues')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, activity_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR activity_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, activity_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, activity_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true)
    GROUP BY filing_uuid, activity_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_activities rows with no matching lobbying_filings row
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T7_orphan_rows_lobbying_filings',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_activities rows with no matching lobbying_filings row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- T7: lobbying_activities.general_issue_code values missing from lobbying_issue_codes
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T7_general_issue_code_decodes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'lobbying_activities.general_issue_code values missing from lobbying_issue_codes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true) r ON c.general_issue_code = r.issue_code WHERE c.general_issue_code IS NOT NULL AND r.issue_code IS NULL);

-- T7: lobbying_activities.lobbyist_count differs from the number of lobbying_activity_lobbyists rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T7_lobbyist_count_matches_lobbying_activity_lobbyists',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_activities.lobbyist_count differs from the number of lobbying_activity_lobbyists rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, activity_seq, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true) GROUP BY filing_uuid, activity_seq) a ON a.filing_uuid = b.filing_uuid AND a.activity_seq = b.activity_seq WHERE b.lobbyist_count <> COALESCE(a.c, 0));

-- T7: lobbying_activities.government_entity_count differs from the number of lobbying_activity_government_entities rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_activities', 'T7_government_entity_count_matches_lobbying_activity_government_entities',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_activities.government_entity_count differs from the number of lobbying_activity_government_entities rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, activity_seq, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true) GROUP BY filing_uuid, activity_seq) a ON a.filing_uuid = b.filing_uuid AND a.activity_seq = b.activity_seq WHERE b.government_entity_count <> COALESCE(a.c, 0));

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_activity_lobbyists
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true));

-- T2: row_count (most activities name a lobbyist)
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'lobbyist_prefix', 'lobbyist_nickname', 'lobbyist_middle_name', 'lobbyist_suffix', 'covered_position')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'is_new')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, activity_seq, lobbyist_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR activity_seq IS NULL OR lobbyist_id IS NULL);

-- T7: lobbying_activity_lobbyists rows with no matching lobbying_activities row
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_lobbyists', 'T7_orphan_rows_lobbying_activities',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_activity_lobbyists rows with no matching lobbying_activities row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_lobbyists', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid AND c.activity_seq = p.activity_seq WHERE p.filing_uuid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_activity_government_entities
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true));

-- T2: row_count (most LD-2 activities contact government entities)
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, activity_seq, government_entity_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR activity_seq IS NULL OR government_entity_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Duplicate (filing_uuid, activity_seq, government_entity_id) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, activity_seq, government_entity_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true)
    GROUP BY filing_uuid, activity_seq, government_entity_id
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_activity_government_entities rows with no matching lobbying_activities row
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T7_orphan_rows_lobbying_activities',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_activity_government_entities rows with no matching lobbying_activities row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activities', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid AND c.activity_seq = p.activity_seq WHERE p.filing_uuid IS NULL);

-- T7: lobbying_activity_government_entities.government_entity_id values missing from lobbying_government_entity_codes
INSERT INTO dq_results
SELECT 'law', 'lobbying_activity_government_entities', 'T7_government_entity_id_decodes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'lobbying_activity_government_entities.government_entity_id values missing from lobbying_government_entity_codes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_activity_government_entities', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true) r ON c.government_entity_id = r.government_entity_id WHERE c.government_entity_id IS NOT NULL AND r.government_entity_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_foreign_entities
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true));

-- T2: row_count (foreign entities are disclosed on a small share of filings)
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Expected at least 1 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'contribution', 'address', 'state', 'ppb_city', 'ppb_state', 'ppb_country')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'country', 'country_name')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, entity_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR entity_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, entity_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, entity_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true)
    GROUP BY filing_uuid, entity_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_foreign_entities rows with no matching lobbying_filings row
INSERT INTO dq_results
SELECT 'law', 'lobbying_foreign_entities', 'T7_orphan_rows_lobbying_filings',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_foreign_entities rows with no matching lobbying_filings row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_foreign_entities', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_affiliated_organizations
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true));

-- T2: row_count (affiliated organizations are disclosed on a small share of filings)
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Expected at least 1 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'url', 'address_2', 'ppb_city', 'ppb_state', 'ppb_country', 'zip', 'state', 'city', 'country', 'address_1')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'country', 'state')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, organization_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR organization_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, organization_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, organization_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true)
    GROUP BY filing_uuid, organization_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_affiliated_organizations rows with no matching lobbying_filings row
INSERT INTO dq_results
SELECT 'law', 'lobbying_affiliated_organizations', 'T7_orphan_rows_lobbying_filings',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_affiliated_organizations rows with no matching lobbying_filings row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_affiliated_organizations', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_conviction_disclosures
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true));

-- T2: row_count (convictions are disclosed on roughly one filing in a few hundred)
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Expected at least 1 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'lobbyist_prefix', 'lobbyist_nickname', 'lobbyist_middle_name', 'lobbyist_suffix')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- lobbyist_prefix, lobbyist_middle_name, lobbyist_nickname, lobbyist_suffix: optional name parts on a table of a handful of rows, so a sample can hold one value.
      AND column_name NOT IN ('type', 'year', 'quarter', 'lobbyist_id', 'lobbyist_first_name', 'lobbyist_last_name', 'conviction_date', 'description', 'lobbyist_prefix', 'lobbyist_middle_name', 'lobbyist_nickname', 'lobbyist_suffix')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, disclosure_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR disclosure_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, disclosure_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, disclosure_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true)
    GROUP BY filing_uuid, disclosure_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_conviction_disclosures rows with no matching lobbying_filings row
INSERT INTO dq_results
SELECT 'law', 'lobbying_conviction_disclosures', 'T7_orphan_rows_lobbying_filings',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_conviction_disclosures rows with no matching lobbying_filings row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_conviction_disclosures', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filings', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_contribution_reports
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads 1,000 reports per quarter window that has any)
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'comments', 'registrant_house_id', 'registrant_description', 'lobbyist_id', 'lobbyist_prefix', 'lobbyist_first_name', 'lobbyist_nickname', 'lobbyist_middle_name', 'lobbyist_last_name', 'lobbyist_suffix', 'filer_state', 'filer_country')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'filer_country')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true)
      WHERE filing_uuid IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true)
    GROUP BY filing_uuid
    HAVING COUNT(*) > 1
  )
);

-- T7: reports whose filer_type is neither 'lobbyist' nor 'organization'
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T7_filer_type_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'reports whose filer_type is neither lobbyist nor organization'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) WHERE filer_type NOT IN ('lobbyist', 'organization'));

-- T7: lobbyist-type reports with no lobbyist_id, or organization-type reports that have one
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T7_individual_filer_has_lobbyist',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'lobbyist-type reports with no lobbyist_id, or organization-type reports that have one'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) WHERE (filer_type = 'lobbyist' AND lobbyist_id IS NULL) OR (filer_type = 'organization' AND lobbyist_id IS NOT NULL));

-- T7: reports whose dt_posted falls outside the year/quarter partition they are in
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T7_posted_date_in_partition',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'reports whose dt_posted falls outside the year/quarter partition they are in'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) WHERE CAST(substr(dt_posted, 1, 4) AS INTEGER) <> year OR CAST(quarter AS INTEGER) <> ((CAST(substr(dt_posted, 6, 2) AS INTEGER) - 1) // 3) + 1);

-- T7: lobbying_contribution_reports.item_count differs from the number of lobbying_contribution_items rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T7_item_count_matches_lobbying_contribution_items',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_contribution_reports.item_count differs from the number of lobbying_contribution_items rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.item_count <> COALESCE(a.c, 0));

-- T7: lobbying_contribution_reports.pac_count differs from the number of lobbying_contribution_pacs rows
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_reports', 'T7_pac_count_matches_lobbying_contribution_pacs',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_contribution_reports.pac_count differs from the number of lobbying_contribution_pacs rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) b LEFT JOIN (SELECT filing_uuid, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true) GROUP BY filing_uuid) a ON a.filing_uuid = b.filing_uuid WHERE b.pac_count <> COALESCE(a.c, 0));

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_contribution_items
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true));

-- T2: row_count (a report lists a handful of contributions)
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'honoree_name', 'contribution_type_name')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'contribution_type', 'contribution_type_name')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, item_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR item_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, item_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, item_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true)
    GROUP BY filing_uuid, item_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_contribution_items rows with no matching lobbying_contribution_reports row
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T7_orphan_rows_lobbying_contribution_reports',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_contribution_items rows with no matching lobbying_contribution_reports row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- T7: lobbying_contribution_items.contribution_type values missing from lobbying_contribution_item_types
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_items', 'T7_contribution_type_decodes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'lobbying_contribution_items.contribution_type values missing from lobbying_contribution_item_types'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_items', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true) r ON c.contribution_type = r.contribution_type WHERE c.contribution_type IS NOT NULL AND r.contribution_type IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_contribution_pacs
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true));

-- T2: row_count (PACs are listed on a small share of reports)
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T2_row_count',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Expected at least 1 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'pac_name')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_uuid, pac_seq rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true)
      WHERE filing_uuid IS NULL OR pac_seq IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_uuid, pac_seq) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_uuid, pac_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true)
    GROUP BY filing_uuid, pac_seq
    HAVING COUNT(*) > 1
  )
);

-- T7: lobbying_contribution_pacs rows with no matching lobbying_contribution_reports row
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_pacs', 'T7_orphan_rows_lobbying_contribution_reports',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'lobbying_contribution_pacs rows with no matching lobbying_contribution_reports row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_pacs', allow_moved_paths := true) c LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_reports', allow_moved_paths := true) p ON c.filing_uuid = p.filing_uuid WHERE p.filing_uuid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbyists
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads the first 250 records)
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T2_row_count',
  CASE WHEN n >= 250 THEN 'pass' ELSE 'fail' END,
  n, 250, 'Expected at least 250 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'prefix', 'nickname', 'middle_name', 'suffix', 'registrant_house_id', 'registrant_city', 'registrant_state', 'registrant_country')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      -- nickname, prefix, suffix, middle_name: optional name parts that a 250-record sample can hold a single value of.
      AND column_name NOT IN ('type', 'year', 'quarter', 'registrant_country', 'registrant_state', 'nickname', 'prefix', 'suffix', 'middle_name')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL lobbyist_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true)
      WHERE lobbyist_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbyists', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (lobbyist_id) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT lobbyist_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbyists', allow_moved_paths := true)
    GROUP BY lobbyist_id
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_registrants
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads the first 250 records)
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T2_row_count',
  CASE WHEN n >= 250 THEN 'pass' ELSE 'fail' END,
  n, 250, 'Expected at least 250 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'address_2', 'address_3', 'address_4', 'description', 'house_registrant_id', 'ppb_country', 'zip', 'city', 'state', 'country', 'address_1')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'country', 'ppb_country')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL registrant_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true)
      WHERE registrant_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_registrants', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (registrant_id) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT registrant_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_registrants', allow_moved_paths := true)
    GROUP BY registrant_id
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_clients
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true));

-- T2: row_count (a DQ sample reads the first 250 records)
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T2_row_count',
  CASE WHEN n >= 250 THEN 'pass' ELSE 'fail' END,
  n, 250, 'Expected at least 250 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'government_entity', 'self_select', 'ppb_state', 'ppb_country', 'state', 'country', 'general_description')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter', 'country', 'ppb_country')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL client_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true)
      WHERE client_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_clients', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (client_id) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT client_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_clients', allow_moved_paths := true)
    GROUP BY client_id
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_issue_codes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true));

-- T2: row_count (LDA.gov lists 79 issue areas)
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T2_row_count',
  CASE WHEN n >= 70 THEN 'pass' ELSE 'fail' END,
  n, 70, 'Expected at least 70 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL issue_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true)
      WHERE issue_code IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_issue_codes', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (issue_code) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT issue_code, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_issue_codes', allow_moved_paths := true)
    GROUP BY issue_code
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_government_entity_codes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true));

-- T2: row_count (LDA.gov lists 257 government entities)
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T2_row_count',
  CASE WHEN n >= 200 THEN 'pass' ELSE 'fail' END,
  n, 200, 'Expected at least 200 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL government_entity_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true)
      WHERE government_entity_id IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_government_entity_codes', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (government_entity_id) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT government_entity_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_government_entity_codes', allow_moved_paths := true)
    GROUP BY government_entity_id
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_filing_types
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true));

-- T2: row_count (LDA.gov lists 50 filing types)
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T2_row_count',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'fail' END,
  n, 40, 'Expected at least 40 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL filing_type rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true)
      WHERE filing_type IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_filing_types', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (filing_type) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT filing_type, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_filing_types', allow_moved_paths := true)
    GROUP BY filing_type
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: lobbying_contribution_item_types
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true));

-- T2: row_count (LDA.gov lists 5 contribution types)
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T2_row_count',
  CASE WHEN n >= 4 THEN 'pass' ELSE 'fail' END,
  n, 4, 'Expected at least 4 rows in a DQ sample'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'year', 'quarter')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL contribution_type rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true)
      WHERE contribution_type IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'law', 'lobbying_contribution_item_types', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (contribution_type) keys'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT contribution_type, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/lobbying_contribution_item_types', allow_moved_paths := true)
    GROUP BY contribution_type
    HAVING COUNT(*) > 1
  )
);

-- ────────────────────────────────────────────────────────────
-- TABLE: scotus_dockets
-- ────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true));

-- T2: a term has 56-76 opinions; original-jurisdiction cases are not fetched
INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20, 'Expected at least 20 dockets (one term)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true));

-- T3: sample
SELECT year, docket_number, title, docketed, lower_court, granted_date, argued_date,
       judgment_issued_date, entry_count, document_count
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
  )
);

INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL docket_number, title, docketed, entry_count or document_count rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true)
      WHERE docket_number IS NULL OR title IS NULL OR docketed IS NULL
         OR entry_count IS NULL OR document_count IS NULL);

INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate docket_number keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT docket_number FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true)
        GROUP BY 1 HAVING COUNT(*) > 1));

-- T7: the key dates that are present are in order: docketed <= granted <= argued <= judgment
INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T7_key_dates_in_order',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Dockets whose docketed, granted, argued and judgment dates are not in order'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true)
      WHERE (granted_date IS NOT NULL AND granted_date < docketed)
         OR (argued_date IS NOT NULL AND granted_date IS NOT NULL AND argued_date < granted_date)
         OR (judgment_issued_date IS NOT NULL AND argued_date IS NOT NULL
             AND judgment_issued_date < argued_date));

-- T7: an argued case was first granted. An application (24A910) argued directly has no petition
-- to grant, and a few stays are treated as petitions without a "Petition GRANTED" entry.
INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T7_argued_implies_granted',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0,
  'Argued petition dockets (not applications) with an argued_date but no granted_date'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true)
      WHERE argued_date IS NOT NULL AND granted_date IS NULL AND docket_number NOT LIKE '%A%');

-- T7: every docket is a decided case in scotus_slip_opinions (listing_docket)
INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T7_docket_is_a_decided_case',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Dockets with no matching listing_docket in scotus_slip_opinions for the same term'
FROM (SELECT COUNT(*) AS n
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true) d
      LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_slip_opinions', allow_moved_paths := true) s
        ON s.year = d.year AND s.listing_docket = d.docket_number
      WHERE s.listing_docket IS NULL);

-- T7: entry_count matches the rows in scotus_docket_entries
INSERT INTO dq_results
SELECT 'law', 'scotus_dockets', 'T7_entry_count_matches_entries',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Dockets whose entry_count differs from the number of scotus_docket_entries rows'
FROM (SELECT COUNT(*) AS n
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_dockets', allow_moved_paths := true) d
      LEFT JOIN (SELECT year, docket_number, COUNT(*) AS c
                 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true) GROUP BY 1, 2) e
        ON e.year = d.year AND e.docket_number = d.docket_number
      WHERE COALESCE(e.c, 0) <> d.entry_count);

-- ────────────────────────────────────────────────────────────
-- TABLE: scotus_docket_entries
-- ────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T2_row_count',
  CASE WHEN n >= 200 THEN 'pass' ELSE 'fail' END, n, 200, 'Expected at least 200 entries (one term, ~20+ dockets)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true));

SELECT year, docket_number, sequence, entry_date, entry_text, document_labels
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL docket_number, sequence, entry_date or entry_text rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true)
      WHERE docket_number IS NULL OR sequence IS NULL OR entry_date IS NULL OR entry_text IS NULL);

INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (docket_number, sequence) keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT docket_number, sequence FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true)
        GROUP BY 1, 2 HAVING COUNT(*) > 1));

-- T7: sequence runs 1..n with no gaps within each docket
INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T7_sequence_contiguous',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Dockets whose sequence is not 1..count'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT docket_number FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true)
        GROUP BY 1 HAVING MIN(sequence) <> 1 OR MAX(sequence) <> COUNT(*)));

-- T7: entries are in date order within a docket
INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T7_dates_non_decreasing',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0,
  'Entries dated earlier than the entry before them in the same docket'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT entry_date, LAG(entry_date) OVER (PARTITION BY docket_number ORDER BY sequence) AS prev
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true))
      WHERE prev IS NOT NULL AND entry_date < prev);

-- T7: document labels and urls are parallel
INSERT INTO dq_results
SELECT 'law', 'scotus_docket_entries', 'T7_documents_parallel',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Rows whose document_labels and document_urls differ in length or in presence'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/scotus_docket_entries', allow_moved_paths := true)
      WHERE (document_labels IS NULL) <> (document_urls IS NULL)
         OR len(document_labels) <> len(document_urls));

-- ─────────────────────────────────────────────────────────────
-- TABLE: usc_subsections
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true));

-- T2: at least one unit per section that has text (a section that fits in 2,000 characters is one unit)
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T2_row_count',
  CASE WHEN n >= sec THEN 'pass' ELSE 'fail' END,
  n, sec, 'Units vs sections with section_text (units must be >= sections with text)'
FROM (SELECT (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true)) AS n,
             (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true) WHERE section_text IS NOT NULL) AS sec);

-- T4: all_null_cols (unit_path is legitimately null for a section that is one unit)
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('title')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL title_number, section_number, section_seq, unit_seq, citation or unit_text rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true)
      WHERE title_number IS NULL OR section_number IS NULL OR section_seq IS NULL
         OR unit_seq IS NULL OR citation IS NULL OR unit_text IS NULL);

-- T6: primary key uniqueness (title_number, section_number, section_seq, unit_seq)
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T6_pk_unique',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (title_number, section_number, section_seq, unit_seq) keys'
FROM (SELECT COUNT(*) AS n FROM (
        SELECT title_number, section_number, section_seq, unit_seq
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true)
        GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1));

-- T7: every unit belongs to a usc_sections row
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T7_parent_section_exists',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Units whose (title_number, section_number, section_seq) has no usc_sections row'
FROM (SELECT COUNT(*) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true) u
      LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true) s
        ON s.title_number = u.title_number AND s.section_number = u.section_number
       AND s.section_seq = u.section_seq
      WHERE s.title_number IS NULL);

-- T7: lossless split. A section's units, joined with newlines, are its section_text minus the
-- citation-and-heading first line. Checked as total characters (units plus one newline between
-- consecutive units) so it is a cheap aggregate rather than a string comparison of 60,000 sections.
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T7_units_add_up_to_section_text',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Sections whose units do not add up to the length of section_text after its first line'
FROM (
  SELECT COUNT(*) AS bad FROM (
    SELECT s.section_text, u.chars, u.n
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_sections', allow_moved_paths := true) s
    JOIN (SELECT title_number, section_number, section_seq,
                 SUM(LENGTH(unit_text)) AS chars, COUNT(*) AS n
          FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true)
          GROUP BY 1, 2, 3) u
      ON s.title_number = u.title_number AND s.section_number = u.section_number
     AND s.section_seq = u.section_seq
    WHERE LENGTH(s.section_text) - LENGTH(SPLIT_PART(s.section_text, chr(10), 1)) - 1
          <> u.chars + (u.n - 1)
  )
);

-- T7: units stay within the 2,000-character limit except an unbroken run (one long paragraph or
-- a table). Warn, not fail: that is the documented exception. Expect a few percent at most.
INSERT INTO dq_results
SELECT 'law', 'usc_subsections', 'T7_oversized_units',
  CASE WHEN pct <= 5.0 THEN 'pass' ELSE 'warn' END,
  pct, 5.0, 'Percent of units over 2000 characters (unbroken paragraphs and tables)'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE LENGTH(unit_text) > 2000) / COUNT(*) AS pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/law/usc_subsections', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
