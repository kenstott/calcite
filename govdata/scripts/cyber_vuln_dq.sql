-- ============================================================================
-- Data Quality Script: cyber_vuln schema
-- Sources: NVD CVE 2.0, CISA KEV, OSV, GitHub Security Advisories, MITRE CWE
-- Tables: 8 Iceberg tables
-- ============================================================================

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_region='auto';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';

CREATE TEMP TABLE dq_results(
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     DOUBLE,
  threshold DOUBLE,
  detail    VARCHAR
);

-- ============================================================================
-- cwe_catalog (MITRE CWE taxonomy; ~1,000 entries; single parquet file)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'row_count',
  CASE WHEN n < 800 THEN 'fail' ELSE 'pass' END,
  n, 800, 'expect ~1,000 CWE entries from MITRE'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cwe_id NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cwe_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true)
  WHERE cwe_id IS NULL
);

-- T7: 4 abstraction levels present
INSERT INTO dq_results
SELECT 'cyber_vuln', 'cwe_catalog', 'expected_values',
  CASE WHEN n < 4 THEN 'warn' ELSE 'pass' END,
  n, 4, 'distinct abstraction levels (Class, Base, Variant, Compound)'
FROM (SELECT COUNT(DISTINCT abstraction) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/cwe_catalog', allow_moved_paths := true));

-- ============================================================================
-- vulnerabilities (NVD CVE 2.0; ~347k entries; single parquet file)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'row_count',
  CASE WHEN n < 300000 THEN 'fail' ELSE 'pass' END,
  n, 300000, 'expect ~347k NVD CVE records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'source')
);

-- T6: pk_nulls (cve_id NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cve_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true)
  WHERE cve_id IS NULL
);

-- T7: CVSS v3.1 scores in valid range [0, 10]
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerabilities', 'expected_values',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'cvss_v31_score outside [0, 10]'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerabilities', allow_moved_paths := true)
  WHERE cvss_v31_score IS NOT NULL AND (cvss_v31_score < 0 OR cvss_v31_score > 10)
);

-- ============================================================================
-- vulnerability_cwes (CVE→CWE junction; single parquet file)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'row_count',
  CASE WHEN n < 100000 THEN 'fail' ELSE 'pass' END,
  n, 100000, 'one row per (cve_id, cwe_id) pair from NVD'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cve_id, cwe_id NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cve_id or cwe_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true)
  WHERE cve_id IS NULL OR cwe_id IS NULL
);

-- T7: cwe_id starts with 'CWE-'
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vulnerability_cwes', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'cwe_id not in CWE-NNN format'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vulnerability_cwes', allow_moved_paths := true)
  WHERE cwe_id IS NOT NULL AND NOT cwe_id LIKE 'CWE-%'
);

-- ============================================================================
-- kev_catalog (CISA Known Exploited Vulnerabilities; ~1,585 entries)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'row_count',
  CASE WHEN n < 1000 THEN 'fail' ELSE 'pass' END,
  n, 1000, 'expect ~1,585 CISA KEV entries'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cve_id NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cve_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true)
  WHERE cve_id IS NULL
);

-- T7: known_ransomware_use in (Known, Unknown)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_catalog', 'expected_values',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'known_ransomware_use outside Known/Unknown'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true)
  WHERE known_ransomware_use IS NOT NULL
    AND known_ransomware_use NOT IN ('Known', 'Unknown')
);

-- ============================================================================
-- kev_cwes (CISA KEV CVE→CWE junction; single parquet file)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'row_count',
  CASE WHEN n < 500 THEN 'fail' ELSE 'pass' END,
  n, 500, 'one row per (cve_id, cwe_id) pair from KEV'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cve_id, cwe_id NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cve_id or cwe_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true)
  WHERE cve_id IS NULL OR cwe_id IS NULL
);

-- T7: all kev_cwes cve_ids should appear in kev_catalog
INSERT INTO dq_results
SELECT 'cyber_vuln', 'kev_cwes', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'kev_cwes cve_id not found in kev_catalog'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_cwes', allow_moved_paths := true) kc
  WHERE NOT EXISTS (
    SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/kev_catalog', allow_moved_paths := true) k
    WHERE k.cve_id = kc.cve_id
  )
);

-- ============================================================================
-- osv_vulnerabilities (Open Source Vulnerabilities; multiple ecosystems)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'row_count',
  CASE WHEN n < 50000 THEN 'fail' ELSE 'pass' END,
  n, 50000, 'OSV entries across PyPI, npm, Go, Maven, and other ecosystems'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (osv_id, modified NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL osv_id or modified'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true)
  WHERE osv_id IS NULL OR modified IS NULL
);

-- T7: multiple ecosystems present
INSERT INTO dq_results
SELECT 'cyber_vuln', 'osv_vulnerabilities', 'expected_values',
  CASE WHEN n < 3 THEN 'warn' ELSE 'pass' END,
  n, 3, 'distinct ecosystem count (PyPI, npm, Go, Maven, etc.)'
FROM (SELECT COUNT(DISTINCT ecosystem) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/osv_vulnerabilities', allow_moved_paths := true));

-- ============================================================================
-- vuln_cross_refs (CVE→external ID mappings; GitHub, MITRE, OSV sources)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'row_count',
  CASE WHEN n < 50000 THEN 'fail' ELSE 'pass' END,
  n, 50000, 'CVE→external (GHSA, NVD, CISA, MITRE) cross-reference mappings'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cve_id, external_id, external_source NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cve_id, external_id, or external_source'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true)
  WHERE cve_id IS NULL OR external_id IS NULL OR external_source IS NULL
);

-- T7: external_source in expected set
INSERT INTO dq_results
SELECT 'cyber_vuln', 'vuln_cross_refs', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'external_source outside known set (ghsa, nvd, cisa, mitre, github, cve)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/vuln_cross_refs', allow_moved_paths := true)
  WHERE external_source NOT IN ('ghsa','nvd','cisa','mitre','github','cve')
);

-- ============================================================================
-- advisories (CISA cybersecurity advisories; single parquet file)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'row_count',
  CASE WHEN n < 200 THEN 'fail' ELSE 'pass' END,
  n, 200, 'CISA cybersecurity advisories (ICSA, AA series)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'source')
);

-- T6: pk_nulls (advisory_id, source NOT NULL)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL advisory_id or source'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true)
  WHERE advisory_id IS NULL OR source IS NULL
);

-- T7: advisory_id pattern check (AA or ICSA prefix)
INSERT INTO dq_results
SELECT 'cyber_vuln', 'advisories', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'advisory_id outside expected AA*/ICSA* pattern'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_vuln/cyber_vuln/advisories', allow_moved_paths := true)
  WHERE advisory_id IS NOT NULL
    AND NOT (advisory_id LIKE 'AA%' OR advisory_id LIKE 'ICSA%' OR advisory_id LIKE 'ICSMA%')
);

-- ============================================================================
-- Final results
-- ============================================================================
SELECT schema, tbl AS table_name, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
