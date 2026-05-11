-- ============================================================
-- DQ script: cyber_threat schema
-- Tables: attack_techniques, ioc_urls, ioc_hashes, ioc_ips,
--         ioc_mixed (optional), nist_controls, nist_csf_functions,
--         cis_controls, owasp_top10, attack_to_nist_mappings,
--         threat_pulses (optional)
-- Storage: Iceberg (iceberg_scan)
-- ============================================================

SET s3_region = 'auto';
SET s3_endpoint = '21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     DOUBLE,
  threshold DOUBLE,
  detail    VARCHAR
);

-- ============================================================
-- attack_techniques
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T2_row_count',
  CASE WHEN COUNT(*) >= 800 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 800, 'expected >= 800 ATT&CK techniques'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (technique_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'technique_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true)
WHERE technique_id IS NULL;

-- T7: expected_values — tactic_id must be a known ATT&CK tactic code
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_techniques', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'tactic_id values outside known TA00xx range'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_techniques', allow_moved_paths := true)
  WHERE tactic_id IS NOT NULL
    AND tactic_id NOT LIKE 'TA%'
) t;

-- ============================================================
-- ioc_urls
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'expected >= 1000 URL IOCs'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T6: pk_nulls (url NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'url IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true)
WHERE url IS NULL;

-- T7: expected_values — url must start with http
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_urls', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'url does not start with http'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_urls', allow_moved_paths := true)
  WHERE url NOT LIKE 'http%'
) t;

-- ============================================================
-- ioc_hashes
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'expected >= 1000 hash IOCs'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T6: pk_nulls (sha256 NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'sha256 IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true)
WHERE sha256 IS NULL;

-- T7: expected_values — sha256 must be 64 hex chars
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_hashes', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'sha256 not 64-char hex string'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_hashes', allow_moved_paths := true)
  WHERE sha256 IS NOT NULL
    AND LENGTH(sha256) != 64
) t;

-- ============================================================
-- ioc_ips
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true);

-- T2: row_count (low threshold — source has ~5 entries)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'expected >= 1 IP IOC'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T6: pk_nulls (ip_address NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'ip_address IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true)
WHERE ip_address IS NULL;

-- T7: expected_values — ip_address must contain a dot (IPv4/IPv6 coarse check)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_ips', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'ip_address does not look like a valid IP'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_ips', allow_moved_paths := true)
  WHERE ip_address IS NOT NULL
    AND ip_address NOT LIKE '%.%'
    AND ip_address NOT LIKE '%:%'
) t;

-- ============================================================
-- ioc_mixed  (optional — requires CYBER_THREATFOX_API_KEY)
-- T1/T2 use 'warn' because table may be legitimately empty
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 1, 'optional — requires CYBER_THREATFOX_API_KEY'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 100, 'optional — requires CYBER_THREATFOX_API_KEY'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T6: pk_nulls (ioc_value NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'ioc_value IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true)
WHERE ioc_value IS NULL;

-- T7: expected_values — ioc_type must be a known indicator type
INSERT INTO dq_results
SELECT
  'cyber_threat', 'ioc_mixed', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'ioc_type outside expected set (url, hash, ip, domain, email)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/ioc_mixed', allow_moved_paths := true)
  WHERE ioc_type IS NOT NULL
    AND ioc_type NOT IN ('url', 'hash', 'ip', 'domain', 'email')
) t;

-- ============================================================
-- nist_controls
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T2_row_count',
  CASE WHEN COUNT(*) >= 800 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 800, 'expected >= 800 NIST SP 800-53 controls'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (control_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'control_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true)
WHERE control_id IS NULL;

-- T7: expected_values — control_id follows AC-1, SI-12, etc. pattern
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_controls', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'control_id does not match XX-N[N] pattern'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_controls', allow_moved_paths := true)
  WHERE control_id IS NOT NULL
    AND NOT REGEXP_MATCHES(control_id, '^[A-Z]{2}-[0-9]+')
) t;

-- ============================================================
-- nist_csf_functions
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T2_row_count',
  CASE WHEN COUNT(*) >= 80 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 80, 'expected >= 80 CSF subcategory rows'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (function_id, subcategory_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'function_id IS NULL OR subcategory_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true)
WHERE function_id IS NULL OR subcategory_id IS NULL;

-- T7: expected_values — exactly 6 CSF function_id values (GV, ID, PR, DE, RS, RC)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'nist_csf_functions', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT function_id) = 6 THEN 'pass' ELSE 'fail' END,
  COUNT(DISTINCT function_id), 6,
  'expected 6 CSF functions: GV, ID, PR, DE, RS, RC'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/nist_csf_functions', allow_moved_paths := true);

-- ============================================================
-- cis_controls
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true);

-- T2: row_count (CIS v8 has 153 safeguards)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 100, 'expected >= 100 CIS safeguard rows'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (control_id, safeguard_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'control_id IS NULL OR safeguard_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true)
WHERE control_id IS NULL OR safeguard_id IS NULL;

-- T7: expected_values — 18 CIS Controls (control_id 1-18)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'cis_controls', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT control_id) >= 18 THEN 'pass' ELSE 'fail' END,
  COUNT(DISTINCT control_id), 18,
  'expected 18 CIS Controls (v8)'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/cis_controls', allow_moved_paths := true);

-- ============================================================
-- owasp_top10
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10, 'expected 10 OWASP Top 10 entries'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (entry_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'entry_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true)
WHERE entry_id IS NULL;

-- T7: expected_values — exactly 10 distinct entry_id values
INSERT INTO dq_results
SELECT
  'cyber_threat', 'owasp_top10', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT entry_id) = 10 THEN 'pass' ELSE 'fail' END,
  COUNT(DISTINCT entry_id), 10,
  'expected exactly 10 distinct OWASP entry_id values'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/owasp_top10', allow_moved_paths := true);

-- ============================================================
-- attack_to_nist_mappings
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T2_row_count',
  CASE WHEN COUNT(*) >= 500 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 500, 'expected >= 500 ATT&CK-to-NIST mapping rows'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type')
) t;

-- T6: pk_nulls (technique_id, nist_control_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'technique_id IS NULL OR nist_control_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true)
WHERE technique_id IS NULL OR nist_control_id IS NULL;

-- T7: expected_values — diversity of NIST controls referenced
INSERT INTO dq_results
SELECT
  'cyber_threat', 'attack_to_nist_mappings', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT nist_control_id) >= 50 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT nist_control_id), 50,
  'expected >= 50 distinct NIST controls referenced in mappings'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/attack_to_nist_mappings', allow_moved_paths := true);

-- ============================================================
-- threat_pulses  (optional — requires CYBER_OTX_API_KEY)
-- T1/T2 use 'warn' because table may be legitimately empty
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 1, 'optional — requires CYBER_OTX_API_KEY'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 100, 'optional — requires CYBER_OTX_API_KEY'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'first_seen')
) t;

-- T6: pk_nulls (pulse_id NOT NULL)
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'pulse_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true)
WHERE pulse_id IS NULL;

-- T7: expected_values — pulse_id should be a non-empty string
INSERT INTO dq_results
SELECT
  'cyber_threat', 'threat_pulses', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'pulse_id is empty string'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/cyber_threat/threat_pulses', allow_moved_paths := true)
  WHERE pulse_id IS NOT NULL AND TRIM(pulse_id) = ''
) t;

-- ============================================================
-- Final verdict
-- ============================================================
SELECT
  schema,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END AS verdict,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passes
FROM dq_results
GROUP BY schema;
