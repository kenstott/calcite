!connect jdbc:calcite:model=../djia-production-model.json sa ""
!set verbose true
SELECT 'Initializing schemas...' AS status;
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'econ';
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'geo';
!quit