-- ============================================================
-- DQ script: energy schema
-- Tables: eia_electricity_generation, eia_electricity_prices,
--         eia_utility_annual, eia_power_plants, eia_capacity_changes,
--         eia_fossil_fuel_production, eia_state_energy_consumption,
--         eia_natural_gas_storage, eia_petroleum_stocks,
--         eia_crude_oil_imports, eia_refinery_operations, eia_coal_mines
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
-- eia_electricity_generation
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 100000, 'expected >= 100000 rows (state × source × sector × month)'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (generation_year, generation_month NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'generation_year IS NULL OR generation_month IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true)
WHERE generation_year IS NULL OR generation_month IS NULL;

-- T7: expected_values — generation_thousand_mwh >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_generation', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'generation_thousand_mwh < 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_generation', allow_moved_paths := true)
  WHERE generation_thousand_mwh IS NOT NULL AND generation_thousand_mwh < 0
) t;

-- ============================================================
-- eia_electricity_prices
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5000, 'expected >= 5000 rows (state × sector × year)'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (price_year NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'price_year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true)
WHERE price_year IS NULL;

-- T7: expected_values — avg_price_cents_kwh >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_electricity_prices', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'avg_price_cents_kwh < 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_electricity_prices', allow_moved_paths := true)
  WHERE avg_price_cents_kwh IS NOT NULL AND avg_price_cents_kwh < 0
) t;

-- ============================================================
-- eia_utility_annual
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'expected >= 1000 utility-year rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (utility_id, report_year NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'utility_id IS NULL OR report_year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true)
WHERE utility_id IS NULL OR report_year IS NULL;

-- T7: expected_values — coverage across >= 50 states
INSERT INTO dq_results
SELECT
  'energy', 'eia_utility_annual', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT state_abbr) >= 50 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT state_abbr), 50,
  'expected utilities in >= 50 states'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_utility_annual', allow_moved_paths := true);

-- ============================================================
-- eia_power_plants
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10000, 'expected >= 10000 generator-year rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (plant_id, generator_id, report_year NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'plant_id IS NULL OR generator_id IS NULL OR report_year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true)
WHERE plant_id IS NULL OR generator_id IS NULL OR report_year IS NULL;

-- T7: expected_values — nameplate_capacity_mw >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_power_plants', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'nameplate_capacity_mw < 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_power_plants', allow_moved_paths := true)
  WHERE nameplate_capacity_mw IS NOT NULL AND nameplate_capacity_mw < 0
) t;

-- ============================================================
-- eia_capacity_changes
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T2_row_count',
  CASE WHEN COUNT(*) >= 500 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 500, 'expected >= 500 capacity change records'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (plant_id, generator_id, snapshot_year, snapshot_month, change_type NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'plant_id/generator_id/snapshot_year/snapshot_month/change_type IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true)
WHERE plant_id IS NULL OR generator_id IS NULL OR snapshot_year IS NULL
   OR snapshot_month IS NULL OR change_type IS NULL;

-- T7: expected_values — change_type in known set
INSERT INTO dq_results
SELECT
  'energy', 'eia_capacity_changes', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'change_type outside expected set'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_capacity_changes', allow_moved_paths := true)
  WHERE change_type IS NOT NULL
    AND change_type NOT IN ('Planned Addition', 'Planned Retirement', 'New Unit', 'Retirement')
) t;

-- ============================================================
-- eia_fossil_fuel_production
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5000, 'expected >= 5000 production rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (production_year, production_month, fuel_type NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'production_year IS NULL OR production_month IS NULL OR fuel_type IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true)
WHERE production_year IS NULL OR production_month IS NULL OR fuel_type IS NULL;

-- T7: expected_values — fuel_type must be crude_oil or natural_gas
INSERT INTO dq_results
SELECT
  'energy', 'eia_fossil_fuel_production', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0,
  'fuel_type not in (crude_oil, natural_gas)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_fossil_fuel_production', allow_moved_paths := true)
  WHERE fuel_type IS NOT NULL
    AND fuel_type NOT IN ('crude_oil', 'natural_gas')
) t;

-- ============================================================
-- eia_state_energy_consumption
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T2_row_count',
  CASE WHEN COUNT(*) >= 50000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 50000, 'expected >= 50000 rows (state × MSN × year)'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (consumption_year, msn NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'consumption_year IS NULL OR msn IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true)
WHERE consumption_year IS NULL OR msn IS NULL;

-- T7: expected_values — msn must be exactly 5 characters
INSERT INTO dq_results
SELECT
  'energy', 'eia_state_energy_consumption', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'msn not exactly 5 characters'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_state_energy_consumption', allow_moved_paths := true)
  WHERE msn IS NOT NULL AND LENGTH(msn) != 5
) t;

-- ============================================================
-- eia_natural_gas_storage
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'expected >= 1000 weekly storage rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (report_date, storage_year, storage_week NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'report_date IS NULL OR storage_year IS NULL OR storage_week IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true)
WHERE report_date IS NULL OR storage_year IS NULL OR storage_week IS NULL;

-- T7: expected_values — volume_bcf >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_natural_gas_storage', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'volume_bcf < 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_natural_gas_storage', allow_moved_paths := true)
  WHERE volume_bcf IS NOT NULL AND volume_bcf < 0
) t;

-- ============================================================
-- eia_petroleum_stocks
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10000, 'expected >= 10000 weekly stock rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (report_date, stock_year, stock_week NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'report_date IS NULL OR stock_year IS NULL OR stock_week IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true)
WHERE report_date IS NULL OR stock_year IS NULL OR stock_week IS NULL;

-- T7: expected_values — stocks_kbbl >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_petroleum_stocks', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'stocks_kbbl < 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_petroleum_stocks', allow_moved_paths := true)
  WHERE stocks_kbbl IS NOT NULL AND stocks_kbbl < 0
) t;

-- ============================================================
-- eia_crude_oil_imports
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10000, 'expected >= 10000 import transaction rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (rpt_period, import_year, import_month, importer_name, origin_country_code, refinery_site_id NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'rpt_period/import_year/import_month/importer_name/origin_country_code/refinery_site_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true)
WHERE rpt_period IS NULL OR import_year IS NULL OR import_month IS NULL
   OR importer_name IS NULL OR origin_country_code IS NULL OR refinery_site_id IS NULL;

-- T7: expected_values — api_gravity in physical range (0-70) where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_crude_oil_imports', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'api_gravity outside 0-70 range'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_crude_oil_imports', allow_moved_paths := true)
  WHERE api_gravity IS NOT NULL
    AND (api_gravity < 0 OR api_gravity > 70)
) t;

-- ============================================================
-- eia_refinery_operations
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5000, 'expected >= 5000 refinery series rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (report_year, report_month NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'report_year IS NULL OR report_month IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true)
WHERE report_year IS NULL OR report_month IS NULL;

-- T7: expected_values — value >= 0 where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_refinery_operations', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'value < 0 (refinery metric cannot be negative)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_refinery_operations', allow_moved_paths := true)
  WHERE value IS NOT NULL AND value < 0
) t;

-- ============================================================
-- eia_coal_mines
-- ============================================================

-- T1: existence
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5000, 'expected >= 5000 mine-subunit-year rows'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, null_percentage
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0,
  STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name, approx_unique
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (mine_id, report_year, subunit_code NOT NULL)
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0,
  'mine_id IS NULL OR report_year IS NULL OR subunit_code IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true)
WHERE mine_id IS NULL OR report_year IS NULL OR subunit_code IS NULL;

-- T7: expected_values — coal_type in known set where not null
INSERT INTO dq_results
SELECT
  'energy', 'eia_coal_mines', 'T7_expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0,
  'coal_type outside (Bituminous, Anthracite, Lignite, Subbituminous)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/energy/eia_coal_mines', allow_moved_paths := true)
  WHERE coal_type IS NOT NULL
    AND coal_type NOT IN ('Bituminous', 'Anthracite', 'Lignite', 'Subbituminous')
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
