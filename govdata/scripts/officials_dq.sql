-- dq-lookback: 1
-- Officials Data Quality Checks
-- Schema: officials
-- Tables: members, nominations, federal_judges, committees, committee_membership,
-- electoral_college_votes, presidential_election_results, house_election_results,
-- senate_election_results
-- All tables are Iceberg; reads via iceberg_scan.
-- No `year` dimension anywhere in this schema — members/nominations partition by
-- `congress` (bounded by GOVDATA_START_CONGRESS/GOVDATA_END_CONGRESS, default 117-119,
-- NOT full 1789-present history), federal_judges/committees/committee_membership are
-- single etag-gated snapshots.
-- T4/T5 exclude partition columns 'type' and 'congress' (members/nominations) / 'type'
-- (federal_judges, committees, committee_membership).
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
  UNION ALL
  SELECT 'committees',                COUNT(*) FROM iceberg_snapshots('s3://${GOVDATA_DQ_BUCKET}/officials/committees')
  UNION ALL
  SELECT 'committee_membership',      COUNT(*) FROM iceberg_snapshots('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership')
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
-- TABLE: committees
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'committees', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true));

-- T2: row_count (House + Senate + Joint full committees plus subcommittees; ~40 full
-- committees and ~150 subcommittees observed live 2026-08-24, floor set conservatively)
INSERT INTO dq_results
SELECT 'officials', 'committees', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 committee + subcommittee rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — jurisdiction/jurisdiction_source/minority_url/rss_url are
-- genuinely sparse (populated mainly for House full committees, per the source), so
-- exclude them rather than warn on a documented source-shape gap every run.
INSERT INTO dq_results
SELECT 'officials', 'committees', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'jurisdiction', 'jurisdiction_source', 'minority_url', 'rss_url', 'parent_committee_id')
  )
);

-- T6: pk_nulls (committee_id NOT NULL)
INSERT INTO dq_results
SELECT 'officials', 'committees', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: subcommittees exist and link to a real parent (regression guard for the
-- thomas_id + subcommittee thomas_id composite-id logic in CongressCommitteesTransformer)
INSERT INTO dq_results
SELECT 'officials', 'committees', 'T7_subcommittee_parent_linkage',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Subcommittee rows whose parent_committee_id has no matching full-committee row'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true) sub
  WHERE sub.is_subcommittee = true
    AND NOT EXISTS (
      SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committees', allow_moved_paths := true) parent
      WHERE parent.committee_id = sub.parent_committee_id AND parent.is_subcommittee = false
    )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: committee_membership
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'committee_membership', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true));

-- T2: row_count (~150 committees+subcommittees x ~15-40 members each)
INSERT INTO dq_results
SELECT 'officials', 'committee_membership', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 committee-membership rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — title is EXPECTED mostly-null (only chairs/ranking members get one)
INSERT INTO dq_results
SELECT 'officials', 'committee_membership', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'title')
  )
);

-- T6: pk_nulls (committee_id NOT NULL; bioguide_id is nullable — a few historical
-- membership entries in the source carry no bioguide, not treated as a failure)
INSERT INTO dq_results
SELECT 'officials', 'committee_membership', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL committee_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true) WHERE committee_id IS NULL);

-- T7: party values
INSERT INTO dq_results
SELECT 'officials', 'committee_membership', 'T7_party_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'Rows with party NOT IN (''majority'',''minority'')'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/committee_membership', allow_moved_paths := true)
      WHERE party IS NOT NULL AND party NOT IN ('majority', 'minority'));

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
-- Election results (MIT Election Data + Science Lab): house_election_results,
-- senate_election_results. The race views below are the SQL of the schema views
-- house_race_results / senate_race_results (officials-schema.yaml), over the Iceberg tables,
-- so winners, margins and seat counts are checked on what the DQ bucket holds. Keep the two in sync.
-- ─────────────────────────────────────────────────────────────
CREATE TEMP VIEW house_election_results AS SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true);
CREATE TEMP VIEW senate_election_results AS SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true);
CREATE TEMP VIEW house_race_results AS
        WITH cand AS (
          SELECT
            year, state_fips, state_name, state_postal, district, cd_fips, at_large, special,
            runoff, stage, candidate,
            SUM(candidate_votes) AS votes,
            SUM(CASE WHEN party_major = 'D' THEN candidate_votes ELSE 0 END) AS d_line_votes,
            SUM(CASE WHEN party_major = 'R' THEN candidate_votes ELSE 0 END) AS r_line_votes,
            SUM(CASE WHEN party_major = 'D' THEN 1 ELSE 0 END) AS d_lines,
            SUM(CASE WHEN party_major = 'R' THEN 1 ELSE 0 END) AS r_lines,
            SUM(CASE WHEN party_major IS NOT NULL THEN 1 ELSE 0 END) AS known_lines,
            MAX(total_votes) AS total_votes,
            MAX(CASE WHEN uncontested THEN 1 ELSE 0 END) AS uncontested
          FROM house_election_results
          WHERE candidate_kind = 'candidate'
            AND NOT (stage = 'pri' AND special = FALSE)
          GROUP BY year, state_fips, state_name, state_postal, district, cd_fips, at_large, special,
            runoff, stage, candidate
        ),
        ranked AS (
          SELECT cand.*,
            CASE WHEN d_line_votes > r_line_votes THEN 'D'
                 WHEN r_line_votes > d_line_votes THEN 'R'
                 WHEN d_lines > 0 AND r_lines = 0 THEN 'D'
                 WHEN r_lines > 0 AND d_lines = 0 THEN 'R'
                 WHEN known_lines > 0 THEN 'OTHER' END AS party_group,
            RANK() OVER (PARTITION BY year, state_fips, district, special, runoff, stage
                         ORDER BY votes DESC) AS rnk
          FROM cand
        ),
        round_agg AS (
          SELECT
            year, state_fips, state_name, state_postal, district, cd_fips, at_large, special,
            runoff, stage,
            COUNT(*) AS candidates,
            MAX(total_votes) AS total_votes,
            SUM(votes) AS candidate_votes,
            MAX(uncontested) AS uncontested,
            SUM(CASE WHEN rnk = 1 THEN 1 ELSE 0 END) AS tied_for_lead,
            MAX(CASE WHEN rnk = 1 THEN votes END) AS leader_votes,
            MAX(CASE WHEN rnk = 2 THEN votes END) AS runner_up_votes,
            MAX(CASE WHEN rnk = 1 THEN candidate END) AS leader_candidate,
            MAX(CASE WHEN rnk = 1 THEN party_group END) AS leader_party_major,
            SUM(CASE WHEN party_group = 'D' THEN votes ELSE 0 END) AS d_votes,
            SUM(CASE WHEN party_group = 'R' THEN votes ELSE 0 END) AS r_votes,
            SUM(CASE WHEN d_line_votes > 0 AND r_line_votes > 0 THEN 1 ELSE 0 END)
              AS cross_endorsed_candidates,
            CASE WHEN runoff = TRUE THEN 4
                 WHEN special = FALSE THEN 3
                 WHEN stage IS NULL THEN 2
                 ELSE 1 END AS round_rank
          FROM ranked
          GROUP BY year, state_fips, state_name, state_postal, district, cd_fips, at_large, special,
            runoff, stage
        )
        SELECT
          year, state_fips, state_name, state_postal, district, cd_fips, at_large, special,
          runoff, stage, candidates, total_votes, candidate_votes,
          CASE WHEN tied_for_lead = 1 THEN leader_candidate END AS leader_candidate,
          CASE WHEN tied_for_lead = 1 THEN leader_party_major END AS leader_party_major,
          leader_votes, runner_up_votes,
          tied_for_lead > 1 AS tied,
          uncontested = 1 AS uncontested,
          d_votes, r_votes,
          CASE WHEN uncontested = 0 AND d_votes + r_votes > 0
               THEN 1.0 * (d_votes - r_votes) / (d_votes + r_votes) END AS two_party_margin,
          cross_endorsed_candidates,
          round_rank = MAX(round_rank) OVER (PARTITION BY year, state_fips, district) AS is_decisive
        FROM round_agg;
CREATE TEMP VIEW senate_race_results AS
        WITH cand AS (
          SELECT
            year, state_fips, state_name, state_postal, special, stage, candidate,
            SUM(candidate_votes) AS votes,
            SUM(CASE WHEN party_major = 'D' THEN candidate_votes ELSE 0 END) AS d_line_votes,
            SUM(CASE WHEN party_major = 'R' THEN candidate_votes ELSE 0 END) AS r_line_votes,
            SUM(CASE WHEN party_major = 'D' THEN 1 ELSE 0 END) AS d_lines,
            SUM(CASE WHEN party_major = 'R' THEN 1 ELSE 0 END) AS r_lines,
            SUM(CASE WHEN party_major IS NOT NULL THEN 1 ELSE 0 END) AS known_lines,
            MAX(total_votes) AS total_votes,
            MAX(CASE WHEN uncontested THEN 1 ELSE 0 END) AS uncontested,
            MAX(CASE WHEN unofficial THEN 1 ELSE 0 END) AS unofficial
          FROM senate_election_results
          WHERE candidate_kind = 'candidate'
          GROUP BY year, state_fips, state_name, state_postal, special, stage, candidate
        ),
        ranked AS (
          SELECT cand.*,
            CASE WHEN d_line_votes > r_line_votes THEN 'D'
                 WHEN r_line_votes > d_line_votes THEN 'R'
                 WHEN d_lines > 0 AND r_lines = 0 THEN 'D'
                 WHEN r_lines > 0 AND d_lines = 0 THEN 'R'
                 WHEN known_lines > 0 THEN 'OTHER' END AS party_group,
            RANK() OVER (PARTITION BY year, state_fips, special, stage
                         ORDER BY votes DESC) AS rnk
          FROM cand
        ),
        round_agg AS (
          SELECT
            year, state_fips, state_name, state_postal, special, stage,
            COUNT(*) AS candidates,
            MAX(total_votes) AS total_votes,
            SUM(votes) AS candidate_votes,
            MAX(uncontested) AS uncontested,
            MAX(unofficial) AS unofficial,
            SUM(CASE WHEN rnk = 1 THEN 1 ELSE 0 END) AS tied_for_lead,
            MAX(CASE WHEN rnk = 1 THEN votes END) AS leader_votes,
            MAX(CASE WHEN rnk = 2 THEN votes END) AS runner_up_votes,
            MAX(CASE WHEN rnk = 1 THEN candidate END) AS leader_candidate,
            MAX(CASE WHEN rnk = 1 THEN party_group END) AS leader_party_major,
            SUM(CASE WHEN party_group = 'D' THEN votes ELSE 0 END) AS d_votes,
            SUM(CASE WHEN party_group = 'R' THEN votes ELSE 0 END) AS r_votes,
            SUM(CASE WHEN d_line_votes > 0 AND r_line_votes > 0 THEN 1 ELSE 0 END)
              AS cross_endorsed_candidates,
            CASE stage WHEN 'runoff' THEN 3 WHEN 'gen' THEN 2 ELSE 1 END AS round_rank
          FROM ranked
          GROUP BY year, state_fips, state_name, state_postal, special, stage
        ),
        next_year_runoff AS (
          SELECT DISTINCT year, state_fips, special
          FROM round_agg
          WHERE stage = 'runoff'
        )
        SELECT
          r.year, r.year - MOD(r.year, 2) AS cycle_year, r.state_fips, r.state_name,
          r.state_postal, r.special, r.stage, r.candidates,
          r.total_votes, r.candidate_votes,
          CASE WHEN r.tied_for_lead = 1 THEN r.leader_candidate END AS leader_candidate,
          CASE WHEN r.tied_for_lead = 1 THEN r.leader_party_major END AS leader_party_major,
          r.leader_votes, r.runner_up_votes,
          r.tied_for_lead > 1 AS tied,
          r.uncontested = 1 AS uncontested,
          r.unofficial = 1 AS unofficial,
          r.d_votes, r.r_votes,
          CASE WHEN r.uncontested = 0 AND r.d_votes + r.r_votes > 0
               THEN 1.0 * (r.d_votes - r.r_votes) / (r.d_votes + r.r_votes) END AS two_party_margin,
          r.cross_endorsed_candidates,
          r.round_rank = MAX(r.round_rank) OVER (PARTITION BY r.year, r.state_fips, r.special)
            AND (r.stage = 'runoff' OR n.year IS NULL) AS is_decisive
        FROM round_agg r
        LEFT JOIN next_year_runoff n
          ON n.year = r.year + 1 AND n.state_fips = r.state_fips AND n.special = r.special;

-- ─────────────────────────────────────────────────────────────
-- TABLE: house_election_results
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true));

-- T2: row_count (the Lab's file is frozen, so the count is exact)
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T2_row_count',
  CASE WHEN n = 29636 THEN 'pass' ELSE 'fail' END,
  n, 29636, 'Row count equals the published file (29636 rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols (nullable columns that are legitimately empty on many rows are not fully null)
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value (source_release is one value by design)
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'source_release')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL year, state_fips, district, special, line_no rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE year IS NULL OR state_fips IS NULL OR district IS NULL OR special IS NULL OR line_no IS NULL);

-- T7: stage_values
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_stage_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'stage outside gen, pri or null'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE stage IS NOT NULL AND stage NOT IN ('gen', 'pri'));

-- T7: candidate_kind_values
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_candidate_kind_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate_kind outside candidate, write_in_aggregate, residual'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE candidate_kind NOT IN ('candidate', 'write_in_aggregate', 'residual'));

-- T7: party_major_values
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_party_major_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'party_major outside D, R, OTHER or null'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE party_major IS NOT NULL AND party_major NOT IN ('D', 'R', 'OTHER'));

-- T7: state_fips_format
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_state_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'state_fips not a 2-digit code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE NOT regexp_matches(state_fips, '^[0-9]{2}$'));

-- T7: at_large_and_cd_fips
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_at_large_and_cd_fips',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'at_large differs from district = 0, or cd_fips is not state_fips + 2-digit district'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE at_large <> (district = 0) OR cd_fips <> state_fips || LPAD(CAST(district AS VARCHAR), 2, '0'));

-- T7: votes_nonnegative_within_total
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_votes_nonnegative_within_total',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate_votes negative or above total_votes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE candidate_votes < 0 OR candidate_votes > total_votes);

-- T7: candidate_null_only_for_aggregates
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_candidate_null_only_for_aggregates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate null on a row that is not a write-in aggregate'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE candidate IS NULL AND (candidate_kind <> 'write_in_aggregate' OR NOT writein));

-- T7: uncontested_marker
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_uncontested_marker',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'uncontested true without candidate_votes = 1 and total_votes = 1, or the reverse'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) WHERE uncontested <> (candidate_votes = 1 AND total_votes = 1));

-- T6: pk_duplicates over the full declared key (runoff and stage are nullable, so they are not in the NULL check)

INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T6_pk_duplicates_full_key',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate keys on year, state_fips, district, special, runoff, stage, line_no'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/house_election_results', allow_moved_paths := true) GROUP BY year, state_fips, district, special, runoff, stage, line_no HAVING COUNT(*) > 1));

-- T7: cycles_1976_2018
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_cycles_1976_2018',
  CASE WHEN n = 22 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Distinct election years in the table (22 even years 1976 to 2018)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM house_election_results WHERE year BETWEEN 1976 AND 2018 AND year % 2 = 0);

-- T7: states_50
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_states_50',
  CASE WHEN n = 50 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Distinct states (50; DC and territories have delegates the Lab does not publish)'
FROM (SELECT COUNT(DISTINCT state_fips) AS n FROM house_election_results);

-- T7: decisive_districts_435_every_cycle
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_decisive_districts_435_every_cycle',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Cycles whose decisive rounds do not number exactly 435 districts (expect 0 of 22)'
FROM (SELECT COUNT(*) AS n FROM (SELECT year FROM house_race_results WHERE is_decisive GROUP BY year HAVING COUNT(*) <> 435));

-- T7: one_decisive_round_per_district
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_one_decisive_round_per_district',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'District-cycles with more than one decisive round (expect 0)'
FROM (SELECT COUNT(*) AS n FROM (SELECT year, state_fips, district FROM house_race_results WHERE is_decisive GROUP BY year, state_fips, district HAVING COUNT(*) > 1));

-- T7: no_ties_among_decisive
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_no_ties_among_decisive',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Decisive rounds with two candidates tied for the lead (expect 0)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND tied);

-- T7: seats_2010_R
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2010_R',
  CASE WHEN n = 242 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2010 House seats won by R, (published result 242)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2010 AND leader_party_major = 'R');

-- T7: seats_2010_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2010_D',
  CASE WHEN n = 193 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2010 House seats won by D, (published result 193)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2010 AND leader_party_major = 'D');

-- T7: seats_2012_R
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2012_R',
  CASE WHEN n = 234 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2012 House seats won by R, (published result 234)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2012 AND leader_party_major = 'R');

-- T7: seats_2012_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2012_D',
  CASE WHEN n = 201 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2012 House seats won by D, (published result 201)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2012 AND leader_party_major = 'D');

-- T7: seats_2014_R
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2014_R',
  CASE WHEN n = 247 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2014 House seats won by R, (published result 247)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2014 AND leader_party_major = 'R');

-- T7: seats_2014_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2014_D',
  CASE WHEN n = 188 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2014 House seats won by D, (published result 188)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2014 AND leader_party_major = 'D');

-- T7: seats_2016_R
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2016_R',
  CASE WHEN n = 241 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2016 House seats won by R, (published result 241)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2016 AND leader_party_major = 'R');

-- T7: seats_2016_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2016_D',
  CASE WHEN n = 194 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2016 House seats won by D, (published result 194)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2016 AND leader_party_major = 'D');

-- T7: seats_2018_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2018_D',
  CASE WHEN n = 235 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2018 House seats won by D, (published result 235)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2018 AND leader_party_major = 'D');

-- T7: seats_2008_D
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2008_D',
  CASE WHEN n = 257 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2008 House seats won by D, (published result 257)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2008 AND leader_party_major = 'D');

-- T7: seats_2008_R
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2008_R',
  CASE WHEN n = 178 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2008 House seats won by R, (published result 178)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2008 AND leader_party_major = 'R');

-- T7: seats_2018_party_blank
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_seats_2018_party_blank',
  CASE WHEN n = 3 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2018 winners with no published party (Alabama 5, New Jersey 4, Wisconsin 8, Republicans in officials.members: 235 D + 197 R + 3 blank = 435)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2018 AND leader_party_major IS NULL);

-- T7: spot_ocasio_cortez_ny14_2018
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_spot_ocasio_cortez_ny14_2018',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'NY-14 2018: Ocasio-Cortez 110,318 (D) vs 19,202 (R)'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2018 AND state_postal = 'NY' AND district = 14 AND leader_candidate = 'Alexandria Ocasio-Cortez' AND d_votes = 110318 AND r_votes = 19202);

-- T7: spot_kirk_il10_2008
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_spot_kirk_il10_2008',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'IL-10 2008: Kirk (R) 153,082 vs Seals (D) 138,176'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2008 AND state_postal = 'IL' AND district = 10 AND leader_votes = 153082 AND runner_up_votes = 138176);

-- T7: spot_bacon_ne2_2016
INSERT INTO dq_results
SELECT 'officials', 'house_election_results', 'T7_spot_bacon_ne2_2016',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'NE-2 2016: Bacon (R) 141,066 vs Ashford (D) 137,602'
FROM (SELECT COUNT(*) AS n FROM house_race_results WHERE is_decisive AND year = 2016 AND state_postal = 'NE' AND district = 2 AND d_votes = 137602 AND r_votes = 141066);

-- ─────────────────────────────────────────────────────────────
-- TABLE: senate_election_results
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true));

-- T2: row_count (the Dataverse file had 3,945 rows at release v8; later releases only add)
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T2_row_count',
  CASE WHEN n >= 3945 THEN 'pass' ELSE 'fail' END,
  n, 3945, 'Row count at least the v8 release (3945 rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols (nullable columns that are legitimately empty on many rows are not fully null)
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value (source_release is one value by design)
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND null_percentage < 100.0
      AND column_name NOT IN ('type', 'source_release')
  )
);

-- T6: pk_nulls
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL year, state_fips, special, stage, line_no rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE year IS NULL OR state_fips IS NULL OR special IS NULL OR stage IS NULL OR line_no IS NULL);

-- T6: pk_duplicates
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T6_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Duplicate keys on year, state_fips, special, stage, line_no'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) GROUP BY year, state_fips, special, stage, line_no HAVING COUNT(*) > 1));

-- T7: stage_values
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_stage_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'stage outside gen, pre, runoff'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE stage NOT IN ('gen', 'pre', 'runoff'));

-- T7: candidate_kind_values
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_candidate_kind_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate_kind outside candidate, write_in_aggregate, residual'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE candidate_kind NOT IN ('candidate', 'write_in_aggregate', 'residual'));

-- T7: party_major_values
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_party_major_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'party_major outside D, R, OTHER or null'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE party_major IS NOT NULL AND party_major NOT IN ('D', 'R', 'OTHER'));

-- T7: state_fips_format
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_state_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'state_fips not a 2-digit code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE NOT regexp_matches(state_fips, '^[0-9]{2}$'));

-- T7: votes_nonnegative_within_total
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_votes_nonnegative_within_total',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate_votes negative or above total_votes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE candidate_votes < 0 OR candidate_votes > total_votes);

-- T7: candidate_null_only_for_aggregates
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_candidate_null_only_for_aggregates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'candidate null on a row that is not a write-in aggregate'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE candidate IS NULL AND (candidate_kind <> 'write_in_aggregate' OR NOT writein));

-- T7: uncontested_marker
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_uncontested_marker',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'uncontested true without candidate_votes = 1 and total_votes = 1, or the reverse'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE uncontested <> (candidate_votes = 1 AND total_votes = 1));

-- T7: version_date_format
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_version_date_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'version_date not an ISO date'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/officials/senate_election_results', allow_moved_paths := true) WHERE NOT regexp_matches(version_date, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'));

-- T7: cycles_1976_2024
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_cycles_1976_2024',
  CASE WHEN n = 25 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Distinct even election years in the table (25 from 1976 to 2024)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM senate_election_results WHERE year BETWEEN 1976 AND 2024 AND year % 2 = 0);

-- T7: regular_seats_33_or_34_per_cycle
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_regular_seats_33_or_34_per_cycle',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Cycles (cycle_year 1976 to 2024) whose regular decisive races do not number 33 or 34 (expect 0 of 25; the Georgia January 2021 runoffs count in 2020)'
FROM (SELECT COUNT(*) AS n FROM (SELECT cycle_year FROM senate_race_results WHERE is_decisive AND NOT special AND cycle_year BETWEEN 1976 AND 2024 GROUP BY cycle_year HAVING COUNT(*) NOT IN (33, 34)));

-- T7: no_ties_among_decisive
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_no_ties_among_decisive',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Decisive rounds with two candidates tied for the lead (expect 0)'
FROM (SELECT COUNT(*) AS n FROM senate_race_results WHERE is_decisive AND tied);

-- T7: one_decisive_round_per_race
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_one_decisive_round_per_race',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'State-cycle-special races with more than one decisive round (expect 0)'
FROM (SELECT COUNT(*) AS n FROM (SELECT cycle_year, state_fips, special FROM senate_race_results WHERE is_decisive GROUP BY cycle_year, state_fips, special HAVING COUNT(*) > 1));

-- T7: seats_2012_D23_R8_other2
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_seats_2012_D23_R8_other2',
  CASE WHEN n = 3 THEN 'pass' ELSE 'fail' END,
  n, NULL, '2012 Senate winners: 23 D, 8 R, 2 other (the independents King and Sanders)'
FROM (SELECT COUNT(*) AS n FROM (SELECT leader_party_major, COUNT(*) c FROM senate_race_results WHERE is_decisive AND cycle_year = 2012 GROUP BY leader_party_major) WHERE (leader_party_major = 'D' AND c = 23) OR (leader_party_major = 'R' AND c = 8) OR (leader_party_major = 'OTHER' AND c = 2));

-- T7: spot_cruz_tx_2018
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_spot_cruz_tx_2018',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'TX 2018: Cruz (R) 4,260,553 vs O Rourke (D) 4,045,632'
FROM (SELECT COUNT(*) AS n FROM senate_race_results WHERE is_decisive AND year = 2018 AND state_postal = 'TX' AND d_votes = 4045632 AND r_votes = 4260553);

-- T7: spot_ossoff_ga_runoff_2021
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_spot_ossoff_ga_runoff_2021',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'GA 5 January 2021 runoff: Ossoff (D) 2,269,262 vs Perdue (R) 2,213,979, decisive with cycle_year 2020'
FROM (SELECT COUNT(*) AS n FROM senate_race_results WHERE is_decisive AND year = 2021 AND cycle_year = 2020 AND state_postal = 'GA' AND NOT special AND d_votes = 2269262 AND r_votes = 2213979);

-- T7: spot_fetterman_pa_2022
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_spot_fetterman_pa_2022',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'PA 2022: Fetterman (D) 2,751,012 vs Oz (R) 2,487,260'
FROM (SELECT COUNT(*) AS n FROM senate_race_results WHERE is_decisive AND year = 2022 AND state_postal = 'PA' AND d_votes = 2751012 AND r_votes = 2487260);

-- T7: georgia_2020_first_rounds_not_decisive
INSERT INTO dq_results
SELECT 'officials', 'senate_election_results', 'T7_georgia_2020_first_rounds_not_decisive',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, NULL, 'Georgia 2020 general-election rounds that are marked decisive although the 2021 runoffs decided them (expect 0)'
FROM (SELECT COUNT(*) AS n FROM senate_race_results WHERE year = 2020 AND state_postal = 'GA' AND is_decisive);


-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
