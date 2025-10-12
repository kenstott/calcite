-- Test SQL to force schema initialization
!connect jdbc:calcite:model=../djia-production-model.json sa ""
!set verbose true
SELECT 'Initializing ECON schema' AS status;
SELECT COUNT(*) AS econ_table_count FROM information_schema.tables WHERE table_schema = 'econ';
SELECT 'Initializing GEO schema' AS status;
SELECT COUNT(*) AS geo_table_count FROM information_schema.tables WHERE table_schema = 'geo';
!quit