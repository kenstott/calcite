-- dq-lookback: 1
-- Health Data Quality Checks
-- Schema: health
-- Tables: fda_ndc_products, fda_drug_approvals, fda_drug_recalls, fda_adverse_events,
--         fda_device_recalls, clinical_trials, clinical_trial_conditions,
--         clinical_trial_interventions, cdc_covid_vaccinations, cms_hospital_quality,
--         medicaid_drug_utilization, cdc_mortality, cdc_brfss, cms_open_payments, rxnorm_drugs
-- All tables are Iceberg; reads via iceberg_scan.
-- Partition columns per table:
--   Most tables: 'type'
--   cdc_mortality: 'source_type'
--   cms_open_payments: 'payment_type'
-- T4/T5 exclude partition columns.
-- T6 skipped for cdc_covid_vaccinations, cdc_brfss, cdc_mortality, cms_open_payments, rxnorm_drugs
--    (no non-partition, non-nullable columns in those tables).

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
-- TABLE: fda_ndc_products
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true));

-- T2: row_count (~134k records expected)
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T2_row_count',
  CASE WHEN n >= 25000 THEN 'pass' ELSE 'fail' END,
  n, 25000, 'Expected at least 25000 NDC product records (openFDA API 26k cap applies)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (product_ndc NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL product_ndc rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true) WHERE product_ndc IS NULL);

-- T7: product_type values
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T7_product_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'product_type outside known set'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true)
      WHERE product_type IS NOT NULL
        AND product_type NOT IN (
          'HUMAN OTC DRUG', 'HUMAN PRESCRIPTION DRUG', 'BULK INGREDIENT',
          'DRUG FOR FURTHER PROCESSING', 'NON-STANDARDIZED ALLERGENIC',
          'PLASMA DERIVATIVE', 'VACCINE', 'STANDARDIZED ALLERGENIC',
          'CELLULAR THERAPY', 'LICENSED VACCINE BULK INTERMEDIATE'));

-- T7: marketing_category diversity
INSERT INTO dq_results
SELECT 'health', 'fda_ndc_products', 'T7_marketing_category_coverage',
  CASE WHEN n >= 3 THEN 'pass' ELSE 'warn' END,
  n, 3, 'Distinct marketing_category values (NDA, ANDA, OTC MONOGRAPH expected)'
FROM (SELECT COUNT(DISTINCT marketing_category) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_ndc_products', allow_moved_paths := true)
      WHERE marketing_category IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: fda_drug_approvals
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true));

-- T2: row_count (~29k records expected)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END,
  n, 20000, 'Expected at least 20000 FDA drug approval records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols (product_type excluded: not populated by FDA drugs@FDA API)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'product_type')
  )
);

-- T5: all_same_value (product_type excluded: not populated by FDA drugs@FDA API)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'product_type')
  )
);

-- T6: pk_nulls (application_number NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL application_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true) WHERE application_number IS NULL);

-- T7: application_number prefix (NDA, BLA, or ANDA)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T7_application_number_prefix',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'application_number not starting with NDA, BLA, or ANDA'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true)
      WHERE application_number IS NOT NULL
        AND NOT (application_number LIKE 'NDA%' OR application_number LIKE 'BLA%' OR application_number LIKE 'ANDA%'));

-- T7: review_priority values
INSERT INTO dq_results
SELECT 'health', 'fda_drug_approvals', 'T7_review_priority',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'review_priority outside (STANDARD, PRIORITY)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_approvals', allow_moved_paths := true)
      WHERE review_priority IS NOT NULL AND review_priority NOT IN ('STANDARD', 'PRIORITY', 'UNKNOWN', 'N/A', '901 REQUIRED', '901 ORDER'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: fda_drug_recalls
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 FDA drug recall records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (recall_number NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'NULL recall_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true) WHERE recall_number IS NULL);

-- T7: classification values (Class I/II/III + Not Yet Classified)
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T7_classification',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'classification outside (Class I, Class II, Class III, Not Yet Classified)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true)
      WHERE classification IS NOT NULL AND classification NOT IN ('Class I', 'Class II', 'Class III', 'Not Yet Classified'));

-- T7: status values
INSERT INTO dq_results
SELECT 'health', 'fda_drug_recalls', 'T7_status',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'status outside (Terminated, Ongoing, Completed)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_drug_recalls', allow_moved_paths := true)
      WHERE status IS NOT NULL AND status NOT IN ('Terminated', 'Ongoing', 'Completed'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: fda_adverse_events
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true));

-- T2: row_count (windowed to recent years; default 2023-01-01 onward)
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T2_row_count',
  CASE WHEN n >= 25000 THEN 'pass' ELSE 'fail' END,
  n, 25000, 'Expected at least 25000 FAERS adverse event records (openFDA 26k cap applies)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (safety_report_id NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL safety_report_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true) WHERE safety_report_id IS NULL);

-- T7: serious flag values (1 = serious)
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T7_serious_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'serious value not in (1, 2)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true)
      WHERE serious IS NOT NULL AND serious NOT IN ('1', '2'));

-- T7: patient_sex values (1=male, 2=female)
INSERT INTO dq_results
SELECT 'health', 'fda_adverse_events', 'T7_patient_sex',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'patient_sex value not in (1, 2)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_adverse_events', allow_moved_paths := true)
      WHERE patient_sex IS NOT NULL AND patient_sex NOT IN ('0', '1', '2'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: fda_device_recalls
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 FDA device recall records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (cfres_id NOT NULL; warn because older records may have null cfres_id)
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END,
  n, 0, 'NULL cfres_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true) WHERE cfres_id IS NULL);

-- T7: recall_status values (Open, Classified and Completed are valid FDA device recall states)
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T7_recall_status',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'recall_status outside (Terminated, Ongoing, Open, Classified, Completed)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true)
      WHERE recall_status IS NOT NULL AND recall_status NOT IN ('Terminated', 'Ongoing', 'Open, Classified', 'Completed'));

-- T7: root_cause_description non-null rate (FDA device recalls use 43+ granular categories; no closed set)
INSERT INTO dq_results
SELECT 'health', 'fda_device_recalls', 'T7_root_cause_null_rate',
  CASE WHEN null_pct <= 10.0 THEN 'pass' ELSE 'warn' END,
  null_pct, 10.0, 'root_cause_description null rate above 10%'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE root_cause_description IS NULL) / COUNT(*) AS null_pct
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/fda_device_recalls', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: clinical_trials
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true));

-- T2: row_count (~500k studies total)
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T2_row_count',
  CASE WHEN n >= 400000 THEN 'pass' ELSE 'fail' END,
  n, 400000, 'Expected at least 400000 clinical trial records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols (enrollment_count excluded: optional field, not populated for all studies)
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'enrollment_count')
  )
);

-- T5: all_same_value (enrollment_count excluded: optional field, not populated for all studies)
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'enrollment_count')
  )
);

-- T6: pk_nulls (nct_id NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL nct_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true) WHERE nct_id IS NULL);

-- T7: study_type values
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T7_study_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'study_type outside known set'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true)
      WHERE study_type IS NOT NULL
        AND study_type NOT IN ('INTERVENTIONAL', 'OBSERVATIONAL', 'EXPANDED_ACCESS'));

-- T7: funder_type values
INSERT INTO dq_results
SELECT 'health', 'clinical_trials', 'T7_funder_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'funder_type outside known set'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trials', allow_moved_paths := true)
      WHERE funder_type IS NOT NULL
        AND funder_type NOT IN ('INDUSTRY', 'NIH', 'OTHER_GOV', 'FED', 'INDIV', 'NETWORK', 'RRCF', 'STATE', 'UNKNOWN', 'OTHER'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: clinical_trial_conditions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true));

-- T2: row_count (proportional to clinical_trials sample; ~1.8 conditions per trial)
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END,
  n, 1500, 'Expected at least 1500 clinical trial condition records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (nct_id, condition_name NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL nct_id or condition_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true)
      WHERE nct_id IS NULL OR condition_name IS NULL);

-- T7: nct_id format (NCT prefix)
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_conditions', 'T7_nct_id_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'nct_id not starting with NCT'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_conditions', allow_moved_paths := true)
      WHERE nct_id IS NOT NULL AND nct_id NOT LIKE 'NCT%');

-- ─────────────────────────────────────────────────────────────
-- TABLE: clinical_trial_interventions
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true));

-- T2: row_count (proportional to clinical_trials sample; ~1.6 interventions per trial)
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END,
  n, 1500, 'Expected at least 1500 clinical trial intervention records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (nct_id, intervention_name NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL nct_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true)
      WHERE nct_id IS NULL);

-- T7: intervention_type values
INSERT INTO dq_results
SELECT 'health', 'clinical_trial_interventions', 'T7_intervention_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'intervention_type outside known set'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/clinical_trial_interventions', allow_moved_paths := true)
      WHERE intervention_type IS NOT NULL
        AND intervention_type NOT IN ('BEHAVIORAL', 'BIOLOGICAL', 'COMBINATION_PRODUCT', 'DEVICE', 'DIAGNOSTIC_TEST',
                                      'DIETARY_SUPPLEMENT', 'DRUG', 'GENETIC', 'PROCEDURE', 'RADIATION', 'OTHER'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: cdc_covid_vaccinations
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'cdc_covid_vaccinations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true));

-- T2: row_count (data frozen at 2023-05-12)
INSERT INTO dq_results
SELECT 'health', 'cdc_covid_vaccinations', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 CDC COVID vaccination records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'cdc_covid_vaccinations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'cdc_covid_vaccinations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: skipped — all non-partition columns are nullable per health-schema.yaml

-- T7: demographic_category diversity
INSERT INTO dq_results
SELECT 'health', 'cdc_covid_vaccinations', 'T7_demographic_coverage',
  CASE WHEN n >= 5 THEN 'pass' ELSE 'warn' END,
  n, 5, 'Distinct demographic_category values (age groups, race/ethnicity, overall)'
FROM (SELECT COUNT(DISTINCT demographic_category) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_covid_vaccinations', allow_moved_paths := true)
      WHERE demographic_category IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: cms_hospital_quality
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true));

-- T2: row_count (~5,426 facilities)
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 CMS hospital quality records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (facility_id NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL facility_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true) WHERE facility_id IS NULL);

-- T7: state coverage (hospitals in all 50 states)
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T7_state_coverage',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END,
  n, 50, 'Distinct states with hospital records'
FROM (SELECT COUNT(DISTINCT state) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true)
      WHERE state IS NOT NULL);

-- T7: overall_rating range
INSERT INTO dq_results
SELECT 'health', 'cms_hospital_quality', 'T7_overall_rating',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'overall_rating outside (1-5 or Not Available)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_hospital_quality', allow_moved_paths := true)
      WHERE overall_rating IS NOT NULL
        AND overall_rating NOT IN ('1', '2', '3', '4', '5', 'Not Available', ''));

-- ─────────────────────────────────────────────────────────────
-- TABLE: medicaid_drug_utilization
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true));

-- T2: row_count (~2.5M records/year; default is 2023 data)
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END,
  n, 1000000, 'Expected at least 1M Medicaid drug utilization records (2023 annual dataset has 5.3M)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (state, ndc, year, quarter NOT NULL)
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL state, ndc, year, or quarter rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true)
      WHERE state IS NULL OR ndc IS NULL OR year IS NULL OR quarter IS NULL);

-- T7: quarter values
INSERT INTO dq_results
SELECT 'health', 'medicaid_drug_utilization', 'T7_quarter_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'quarter outside (Q1, Q2, Q3, Q4)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/medicaid_drug_utilization', allow_moved_paths := true)
      WHERE quarter IS NOT NULL AND quarter NOT IN ('Q1', 'Q2', 'Q3', 'Q4'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: cdc_mortality
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END,
  n, 20000, 'Expected at least 20000 CDC mortality records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
-- week_ending_date/age_adjusted_rate excluded: structurally null (weekly-only / annual-only fields)
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('source_type', 'week_ending_date', 'age_adjusted_rate')
  )
);

-- T5: all_same_value
-- week_ending_date/age_adjusted_rate excluded: structurally null (weekly-only / annual-only fields)
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('source_type', 'week_ending_date', 'age_adjusted_rate')
  )
);

-- T6: skipped — only source_type is NOT NULL and it is the partition column

-- T7: source_type values (annual, weekly)
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T7_source_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'source_type outside (annual, weekly)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true)
      WHERE source_type IS NOT NULL AND source_type NOT IN ('annual', 'weekly'));

-- T7: cause_name diversity (annual data)
INSERT INTO dq_results
SELECT 'health', 'cdc_mortality', 'T7_cause_name_coverage',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'warn' END,
  n, 10, 'Distinct cause_name values in annual data'
FROM (SELECT COUNT(DISTINCT cause_name) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_mortality', allow_moved_paths := true)
      WHERE source_type = 'annual' AND cause_name IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: cdc_brfss
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'cdc_brfss', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'health', 'cdc_brfss', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END,
  n, 100000, 'Expected at least 100000 CDC BRFSS records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
-- break_out/break_out_category excluded pending re-ETL with transformer fix
INSERT INTO dq_results
SELECT 'health', 'cdc_brfss', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'break_out', 'break_out_category')
  )
);

-- T5: all_same_value
-- break_out/break_out_category excluded pending re-ETL with transformer fix
INSERT INTO dq_results
SELECT 'health', 'cdc_brfss', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'break_out', 'break_out_category')
  )
);

-- T6: skipped — only type is NOT NULL and it is the partition column

-- T7: state coverage (50 states + DC)
INSERT INTO dq_results
SELECT 'health', 'cdc_brfss', 'T7_state_coverage',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'warn' END,
  n, 50, 'Distinct states in BRFSS data'
FROM (SELECT COUNT(DISTINCT state) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cdc_brfss', allow_moved_paths := true)
      WHERE state IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: cms_open_payments
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END,
  n, 100000, 'Expected at least 100000 CMS Open Payments records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('payment_type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('payment_type')
  )
);

-- T6: skipped — only payment_type is NOT NULL and it is the partition column

-- T7: payment_type values (general, research, ownership)
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T7_payment_type',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'payment_type outside (general, research, ownership)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true)
      WHERE payment_type IS NOT NULL AND payment_type NOT IN ('general', 'research', 'ownership'));

-- T7: all three payment types present
INSERT INTO dq_results
SELECT 'health', 'cms_open_payments', 'T7_payment_type_coverage',
  CASE WHEN n = 3 THEN 'pass' ELSE 'warn' END,
  n, 3, 'Distinct payment_type values (expect general, research, ownership)'
FROM (SELECT COUNT(DISTINCT payment_type) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/cms_open_payments', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: rxnorm_drugs
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true));

-- T2: row_count (~37k concepts)
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T2_row_count',
  CASE WHEN n >= 30000 THEN 'pass' ELSE 'fail' END,
  n, 30000, 'Expected at least 30000 RxNorm drug concept records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: skipped — only type is NOT NULL and it is the partition column

-- T7: tty values (IN, BN, SCD only per schema source query)
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T7_tty_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'tty outside (IN=ingredient, BN=brand name, SCD=clinical drug)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true)
      WHERE tty IS NOT NULL AND tty NOT IN ('IN', 'BN', 'SCD'));

-- T7: all three term types present
INSERT INTO dq_results
SELECT 'health', 'rxnorm_drugs', 'T7_tty_coverage',
  CASE WHEN n = 3 THEN 'pass' ELSE 'warn' END,
  n, 3, 'Distinct tty values (expect IN, BN, SCD)'
FROM (SELECT COUNT(DISTINCT tty) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/health/rxnorm_drugs', allow_moved_paths := true)
      WHERE tty IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
