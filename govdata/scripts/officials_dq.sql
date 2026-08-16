-- dq-lookback: 1
-- Officials Data Quality Checks
-- Schema: officials
-- Tables: members, nominations, federal_judges
-- All tables are Iceberg; reads via iceberg_scan.
-- No `year` dimension anywhere in this schema — members/nominations partition by
-- `congress` (bounded by GOVDATA_START_CONGRESS/GOVDATA_END_CONGRESS, default 117-119,
-- NOT full 1789-present history), federal_judges is a single etag-gated snapshot.
-- T4/T5 exclude partition columns 'type' and 'congress' (members/nominations) / 'type'
-- (federal_judges).
-- Row-count thresholds below are conservative floors derived from live API pagination
-- counts observed 2026-08-01, not exact expected values (congress activity varies).

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
-- T0: Iceberg snapshot diagnostic (all officials tables)
-- Detects "catalog initialized but ETL never wrote data" vs "ETL ran but found 0 rows".
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'officials', tbl, 'T0_iceberg_snapshots',
  CASE WHEN snap_count = 0 THEN 'warn' ELSE 'pass' END,
  snap_count::DOUBLE, 1.0,
  'Iceberg snapshot count (0 = catalog initialized but ETL never wrote data)'
FROM (
  SELECT 'members'         AS tbl, COUNT(*) AS snap_count FROM iceberg_snapshots('s3://${GOVDATA_DQ_BUCKET}/officials/members')
  UNION ALL
  SELECT 'nominations',              COUNT(*) FROM iceberg_snapshots('s3://${GOVDATA_DQ_BUCKET}/officials/nominations')
  UNION ALL
  SELECT 'federal_judges',           COUNT(*) FROM iceberg_snapshots('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges')
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: members
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'members', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true));

-- T2: row_count (3 congresses in the default 117-119 range, ~540 members each,
-- with overlap since a member serving multiple congresses gets one row per congress)
INSERT INTO dq_results
SELECT 'officials', 'members', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 member-per-congress rows across the default congress range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'officials', 'members', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'congress')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'officials', 'members', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress')
  )
);

-- T6: pk_nulls (bioguide_id, congress NOT NULL)
INSERT INTO dq_results
SELECT 'officials', 'members', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL bioguide_id or congress rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true)
      WHERE bioguide_id IS NULL OR congress IS NULL);

-- T7: bioguide_id format (letter + 6 digits, e.g. L000174)
INSERT INTO dq_results
SELECT 'officials', 'members', 'T7_bioguide_id_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'bioguide_id not matching ^[A-Z][0-9]{6}$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/members', allow_moved_paths := true)
      WHERE bioguide_id IS NOT NULL AND NOT regexp_matches(bioguide_id, '^[A-Z][0-9]{6}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: nominations
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true));

-- T2: row_count (119th Congress alone had 2000+ live at pagination-check time;
-- floor set low since nomination volume varies by administration/congress)
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END,
  n, 500, 'Expected at least 500 nomination rows across the default congress range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      -- is_privileged was never observed populated at list level in live samples
      -- (documented in the schema YAML) — exclude so this doesn't perpetually warn.
      AND column_name NOT IN ('type', 'congress', 'is_privileged')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'congress')
  )
);

-- T6: pk_nulls (citation NOT NULL)
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL congress or citation rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true)
      WHERE congress IS NULL OR citation IS NULL);

-- T6: pk_duplicates — citation alone is NOT globally unique (PN numbering restarts
-- each Congress); the actual key is (congress, citation). This check exists because
-- that exact ambiguity was caught and fixed in the schema constraints 2026-08-02 —
-- a regression here means the primaryKey correction didn't hold.
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate (congress, citation) pairs'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT congress, citation, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true)
    GROUP BY congress, citation
    HAVING COUNT(*) > 1
  )
);

-- T7: citation format (PN followed by digits, optionally -digits for a partition)
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T7_citation_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'citation not matching ^PN[0-9]+(-[0-9]+)?$'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true)
      WHERE citation IS NOT NULL AND NOT regexp_matches(citation, '^PN[0-9]+(-[0-9]+)?$'));

-- T7: is_civilian never observed false in live sampling — flag (not fail) if one appears,
-- since a real military nomination would be a legitimate first-observed case, not a bug.
INSERT INTO dq_results
SELECT 'officials', 'nominations', 'T7_is_civilian_false_check',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Rows with is_civilian = false (never seen in live verification — confirm before treating as a parsing bug)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/nominations', allow_moved_paths := true)
      WHERE is_civilian = false);

-- ─────────────────────────────────────────────────────────────
-- TABLE: federal_judges
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true));

-- T2: row_count (live FJC download 2026-08-01 was 4,074 data rows)
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T2_row_count',
  CASE WHEN n >= 3500 THEN 'pass' ELSE 'fail' END,
  n, 3500, 'Expected at least 3500 judge records (live download was 4074 on 2026-08-01)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — groups 2-6 are expected fully-null for judges with only one
-- appointment (most judges), so only check the always-populated global + group-1 columns.
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns among global/group-1 fields' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
      AND column_name NOT LIKE '%\_2' ESCAPE '\'
      AND column_name NOT LIKE '%\_3' ESCAPE '\'
      AND column_name NOT LIKE '%\_4' ESCAPE '\'
      AND column_name NOT LIKE '%\_5' ESCAPE '\'
      AND column_name NOT LIKE '%\_6' ESCAPE '\'
  )
);

-- T6: pk_nulls (jid NOT NULL)
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL jid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true) WHERE jid IS NULL);

-- T7: court_type_1 coverage (expect diverse set: District, Circuit, Supreme, etc.)
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T7_court_type_coverage',
  CASE WHEN n >= 3 THEN 'pass' ELSE 'warn' END,
  n, 3, 'Distinct court_type_1 values (expect at least 3, e.g. District/Circuit/Supreme)'
FROM (SELECT COUNT(DISTINCT court_type_1) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true)
      WHERE court_type_1 IS NOT NULL);

-- T7: multi-appointment judges exist (group 2 populated for at least some judges —
-- confirms the repeating-group mapping isn't silently dropping later appointments)
INSERT INTO dq_results
SELECT 'officials', 'federal_judges', 'T7_multi_appointment_coverage',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Judges with court_type_2 populated (elevated/multiple appointments)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/federal_judges', allow_moved_paths := true)
      WHERE court_type_2 IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: electoral_college_votes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true));

-- T2: row_count (default 1976-2024 = 13 elections; ~51 states x ~2-3 candidates-with-votes
-- x 2 offices is a loose floor, not a tight expectation — third-party/faithless-elector
-- rows vary the real count year to year)
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T2_row_count',
  CASE WHEN n >= 1200 THEN 'pass' ELSE 'fail' END,
  n, 1200, 'Expected at least 1200 rows across the default 1976-2024 range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — candidate_party is EXPECTED mostly-null (only the page's winner/
-- main-opponent get one), so exclude it rather than warn on a documented gap every run.
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year', 'candidate_party', 'candidate_home_state')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL year, state_name, office, or candidate_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true)
      WHERE year IS NULL OR state_name IS NULL OR office IS NULL OR candidate_name IS NULL);

-- T7: office values
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T7_office_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with office NOT IN (''PRESIDENT'',''VICE_PRESIDENT'')'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true)
      WHERE office NOT IN ('PRESIDENT', 'VICE_PRESIDENT'));

-- T7: no state name carries a stray footnote digit (the exact bug caught and fixed
-- 2026-08-02 on the 1824 page — regression guard, not just a format check)
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T7_state_name_footnote_regression',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'state_name values ending in a digit (footnote-marker leakage)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true)
      WHERE regexp_matches(state_name, '[0-9]$'));

-- T7: winner electoral vote total is plausible (270-538 majority range) per election year
INSERT INTO dq_results
SELECT 'officials', 'electoral_college_votes', 'T7_winner_ev_total_plausible',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Election years where the top PRESIDENT candidate''s summed electoral_votes_won is outside [270,538]'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT year, MAX(total) AS top_total
    FROM (
      SELECT year, candidate_name, SUM(electoral_votes_won) AS total
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/electoral_college_votes', allow_moved_paths := true)
      WHERE office = 'PRESIDENT'
      GROUP BY year, candidate_name
    )
    GROUP BY year
    HAVING MAX(total) < 270 OR MAX(total) > 538
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: presidential_election_results
-- ─────────────────────────────────────────────────────────────

-- T0b: known-gap diagnostic — this table only covers years where FEC's per-year listing
-- page had a discoverable presgeresults.xlsx link (confirmed: 2024 only, at build time).
-- A row count far below electoral_college_votes' is the EXPECTED shape, not a failure by
-- itself; T1/T2 below still gate on the years that ARE expected to have resolved.
SELECT 'officials.presidential_election_results covers only the FEC-confirmed XLSX years (2024 at minimum) — a lower row count than electoral_college_votes across the same GOVDATA_START_PRES_YEAR/GOVDATA_END_PRES_YEAR range is expected, not a gap in this DQ pass.' AS note;

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'presidential_election_results', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan — 0 here means even the 2024 confirmed year failed to resolve, investigate'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'officials', 'presidential_election_results', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'officials', 'presidential_election_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL year, state_name, or candidate_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true)
      WHERE year IS NULL OR state_name IS NULL OR candidate_name IS NULL);

-- T7: no leftover "ELECTORAL VOTE:" columns leaked through as a candidate (regression
-- guard for the header-skip logic in PresidentialResultsTransformer)
INSERT INTO dq_results
SELECT 'officials', 'presidential_election_results', 'T7_no_electoral_vote_column_leak',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate_name values starting with ELECTORAL VOTE (header-skip regression)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true)
      WHERE candidate_name ILIKE 'ELECTORAL VOTE%');

-- T7: popular_votes never exceeds state_total_votes for that state/year
INSERT INTO dq_results
SELECT 'officials', 'presidential_election_results', 'T7_votes_within_state_total',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows where popular_votes > state_total_votes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/presidential_election_results', allow_moved_paths := true)
      WHERE popular_votes IS NOT NULL AND state_total_votes IS NOT NULL AND popular_votes > state_total_votes);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
