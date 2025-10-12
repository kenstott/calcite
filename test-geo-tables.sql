!connect jdbc:calcite:model=/Users/kennethstott/calcite/djia-production-model.json;lex=ORACLE;unquotedCasing=TO_LOWER admin admin

-- Test STATES
SELECT COUNT(*) as row_count FROM geo.states;
SELECT * FROM geo.states LIMIT 5;

-- Test COUNTIES
SELECT COUNT(*) as row_count FROM geo.counties;
SELECT * FROM geo.counties LIMIT 5;

-- Test PLACES
SELECT COUNT(*) as row_count FROM geo.places;
SELECT * FROM geo.places LIMIT 5;

-- Test ZCTAS
SELECT COUNT(*) as row_count FROM geo.zctas;
SELECT * FROM geo.zctas LIMIT 5;

-- Test CENSUS_TRACTS
SELECT COUNT(*) as row_count FROM geo.census_tracts;
SELECT * FROM geo.census_tracts LIMIT 5;

-- Test BLOCK_GROUPS
SELECT COUNT(*) as row_count FROM geo.block_groups;
SELECT * FROM geo.block_groups LIMIT 5;

-- Test CBSA
SELECT COUNT(*) as row_count FROM geo.cbsa;
SELECT * FROM geo.cbsa LIMIT 5;

-- Test CONGRESSIONAL_DISTRICTS
SELECT COUNT(*) as row_count FROM geo.congressional_districts;
SELECT * FROM geo.congressional_districts LIMIT 5;

-- Test SCHOOL_DISTRICTS
SELECT COUNT(*) as row_count FROM geo.school_districts;
SELECT * FROM geo.school_districts LIMIT 5;

-- Test ZIP_COUNTY_CROSSWALK
SELECT COUNT(*) as row_count FROM geo.zip_county_crosswalk;
SELECT * FROM geo.zip_county_crosswalk LIMIT 5;

-- Test ZIP_CBSA_CROSSWALK
SELECT COUNT(*) as row_count FROM geo.zip_cbsa_crosswalk;
SELECT * FROM geo.zip_cbsa_crosswalk LIMIT 5;

-- Test TRACT_ZIP_CROSSWALK
SELECT COUNT(*) as row_count FROM geo.tract_zip_crosswalk;
SELECT * FROM geo.tract_zip_crosswalk LIMIT 5;

!quit
