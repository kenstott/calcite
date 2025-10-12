!connect jdbc:calcite:model=/Users/kennethstott/calcite/djia-production-model.json;lex=ORACLE;unquotedCasing=TO_LOWER admin admin

-- Test ACS_POPULATION
SELECT COUNT(*) as row_count FROM census.acs_population;
SELECT * FROM census.acs_population LIMIT 5;

-- Test ACS_DEMOGRAPHICS
SELECT COUNT(*) as row_count FROM census.acs_demographics;
SELECT * FROM census.acs_demographics LIMIT 5;

-- Test ACS_INCOME
SELECT COUNT(*) as row_count FROM census.acs_income;
SELECT * FROM census.acs_income LIMIT 5;

-- Test ACS_POVERTY
SELECT COUNT(*) as row_count FROM census.acs_poverty;
SELECT * FROM census.acs_poverty LIMIT 5;

-- Test ACS_EMPLOYMENT
SELECT COUNT(*) as row_count FROM census.acs_employment;
SELECT * FROM census.acs_employment LIMIT 5;

-- Test ACS_EDUCATION
SELECT COUNT(*) as row_count FROM census.acs_education;
SELECT * FROM census.acs_education LIMIT 5;

-- Test ACS_HOUSING
SELECT COUNT(*) as row_count FROM census.acs_housing;
SELECT * FROM census.acs_housing LIMIT 5;

-- Test ACS_HOUSING_COSTS
SELECT COUNT(*) as row_count FROM census.acs_housing_costs;
SELECT * FROM census.acs_housing_costs LIMIT 5;

-- Test ACS_COMMUTING
SELECT COUNT(*) as row_count FROM census.acs_commuting;
SELECT * FROM census.acs_commuting LIMIT 5;

-- Test ACS_HEALTH_INSURANCE
SELECT COUNT(*) as row_count FROM census.acs_health_insurance;
SELECT * FROM census.acs_health_insurance LIMIT 5;

-- Test ACS_LANGUAGE
SELECT COUNT(*) as row_count FROM census.acs_language;
SELECT * FROM census.acs_language LIMIT 5;

-- Test ACS_DISABILITY
SELECT COUNT(*) as row_count FROM census.acs_disability;
SELECT * FROM census.acs_disability LIMIT 5;

-- Test ACS_VETERANS
SELECT COUNT(*) as row_count FROM census.acs_veterans;
SELECT * FROM census.acs_veterans LIMIT 5;

-- Test ACS_MIGRATION
SELECT COUNT(*) as row_count FROM census.acs_migration;
SELECT * FROM census.acs_migration LIMIT 5;

-- Test ACS_OCCUPATION
SELECT COUNT(*) as row_count FROM census.acs_occupation;
SELECT * FROM census.acs_occupation LIMIT 5;

-- Test DECENNIAL_POPULATION
SELECT COUNT(*) as row_count FROM census.decennial_population;
SELECT * FROM census.decennial_population LIMIT 5;

-- Test DECENNIAL_DEMOGRAPHICS
SELECT COUNT(*) as row_count FROM census.decennial_demographics;
SELECT * FROM census.decennial_demographics LIMIT 5;

-- Test DECENNIAL_HOUSING
SELECT COUNT(*) as row_count FROM census.decennial_housing;
SELECT * FROM census.decennial_housing LIMIT 5;

-- Test ECONOMIC_CENSUS
SELECT COUNT(*) as row_count FROM census.economic_census;
SELECT * FROM census.economic_census LIMIT 5;

-- Test COUNTY_BUSINESS_PATTERNS
SELECT COUNT(*) as row_count FROM census.county_business_patterns;
SELECT * FROM census.county_business_patterns LIMIT 5;

-- Test POPULATION_ESTIMATES
SELECT COUNT(*) as row_count FROM census.population_estimates;
SELECT * FROM census.population_estimates LIMIT 5;

!quit
