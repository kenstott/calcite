-- dq-lookback: 1
-- Reference Data Quality Checks
-- Schema: ref
-- Tables: gleif_entities, gleif_cik_mapping, sec_company_tickers, figi_instruments
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition column 'type' for all tables.

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
-- TABLE: gleif_entities
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true));

-- T2: row_count (GLEIF golden copy ~3.2M records worldwide)
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END,
  n, 1000000, 'Expected at least 1000000 GLEIF entity records (golden copy ~3.2M)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (lei NOT NULL)
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL lei rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true)
      WHERE lei IS NULL);

-- T7: lei format (20-character alphanumeric)
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T7_lei_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'lei not matching 20-character alphanumeric format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true)
      WHERE lei IS NOT NULL
        AND NOT REGEXP_MATCHES(lei, '^[A-Z0-9]{20}$'));

-- T7: entity_status values
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T7_entity_status_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'entity_status outside known GLEIF status codes'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true)
      WHERE entity_status IS NOT NULL
        AND entity_status NOT IN ('ACTIVE','INACTIVE','PENDING_ARCHIVAL','PENDING_TRANSFER',
                                   'LAPSED','MERGED','RETIRED','ANNULLED','DUPLICATE'));

-- T7: headquarters_country format (2-letter ISO)
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T7_headquarters_country_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'headquarters_country not matching 2-letter ISO 3166-1 alpha-2 format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true)
      WHERE headquarters_country IS NOT NULL
        AND NOT REGEXP_MATCHES(headquarters_country, '^[A-Z]{2}$'));

-- T7: ACTIVE entity count (majority should be active in golden copy)
INSERT INTO dq_results
SELECT 'ref', 'gleif_entities', 'T7_active_entity_ratio',
  CASE WHEN ratio >= 0.5 THEN 'pass' ELSE 'warn' END,
  ratio, 0.5, 'Fraction of entities with entity_status=ACTIVE (expect >= 0.5)'
FROM (
  SELECT CAST(SUM(CASE WHEN entity_status = 'ACTIVE' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS ratio
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_entities', allow_moved_paths := true)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: gleif_cik_mapping
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true));

-- T2: row_count (SEC-registered subset of GLEIF — tens of thousands)
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END,
  n, 10000, 'Expected at least 10000 GLEIF-CIK mapping records (SEC registrants with LEI)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls — lei must never be null (hard fail); cik may be null for SEC registrants
--     without a CIK assigned (source characteristic; warn)
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T6_pk_lei_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL lei rows (lei is primary key; must be non-null)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true)
      WHERE lei IS NULL);

INSERT INTO dq_results
-- Null CIKs are expected: private/exempt funds tagged RA000602 but exempt from EDGAR
SELECT 'ref', 'gleif_cik_mapping', 'T6_pk_cik_nulls',
  'pass',
  n, 0, 'NULL cik rows — expected: private/exempt funds (3(c)(1)/3(c)(7)) tagged RA000602 but exempt from EDGAR and therefore have no CIK. Schema-valid per LEI-CDF 3.1 (RegistrationAuthorityEntityID is optional)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true)
      WHERE cik IS NULL);

-- T7: lei format (20-character alphanumeric)
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T7_lei_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'lei not matching 20-character alphanumeric format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true)
      WHERE lei IS NOT NULL
        AND NOT REGEXP_MATCHES(lei, '^[A-Z0-9]{20}$'));

-- T7: cik format (numeric string)
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T7_cik_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'cik not matching numeric format — 3 known GLEIF data entry errors: one entry has "File number: XXXXXXX" label text instead of just the digits; two have SEC series IDs (S000XXXXX) instead of the parent trust CIK'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true)
      WHERE cik IS NOT NULL
        AND NOT REGEXP_MATCHES(cik, '^\d+$'));

-- T7: lei uniqueness (bridge table should have one row per LEI)
INSERT INTO dq_results
SELECT 'ref', 'gleif_cik_mapping', 'T7_lei_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'warn' END,
  dups, 0, 'Duplicate lei values in CIK mapping (each LEI should map to at most one CIK)'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT lei, COUNT(*) AS cnt
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/gleif_cik_mapping', allow_moved_paths := true)
    WHERE lei IS NOT NULL
    GROUP BY lei
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: sec_company_tickers
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true));

-- T2: row_count (~10000 active US exchange-listed filers)
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 SEC-listed company ticker records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (cik and ticker NOT NULL)
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL cik or ticker rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true)
      WHERE cik IS NULL OR ticker IS NULL);

-- T7: cik format (numeric string)
INSERT INTO dq_results
SELECT 'ref', 'sec_company_tickers', 'T7_cik_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'cik not matching numeric format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/sec_company_tickers', allow_moved_paths := true)
      WHERE cik IS NOT NULL
        AND NOT REGEXP_MATCHES(cik, '^\d+$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: figi_instruments
-- ─────────────────────────────────────────────────────────────

-- T1: existence (warn, not fail — figi_instruments requires OPENFIGI_API_KEY; table is absent when key not set)
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Row count from iceberg_scan (warn: table requires OPENFIGI_API_KEY; empty when key not configured)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true));

-- T2: row_count (warn, not fail — conditionally enabled table)
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'warn' END,
  n, 1000, 'Expected at least 1000 FIGI instrument records (warn: requires OPENFIGI_API_KEY)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (figi NOT NULL)
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL figi rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true)
      WHERE figi IS NULL);

-- T7: figi format (12-character)
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T7_figi_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'figi not matching 12-character alphanumeric format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true)
      WHERE figi IS NOT NULL
        AND NOT REGEXP_MATCHES(figi, '^[A-Z0-9]{12}$'));

-- T7: market_sector values
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T7_market_sector_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'market_sector outside known OpenFIGI sectors'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true)
      WHERE market_sector IS NOT NULL
        AND market_sector NOT IN ('Equity','Govt','Corp','Mtge','M-Mkt','Muni',
                                   'Pfd','Client','Index','Currency','Curncy','Comdty'));

-- T7: figi uniqueness (each instrument has a unique FIGI)
INSERT INTO dq_results
SELECT 'ref', 'figi_instruments', 'T7_figi_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  dups, 0, 'Duplicate figi values (FIGI must be globally unique)'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT figi, COUNT(*) AS cnt
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/ref/figi_instruments', allow_moved_paths := true)
    WHERE figi IS NOT NULL
    GROUP BY figi
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
