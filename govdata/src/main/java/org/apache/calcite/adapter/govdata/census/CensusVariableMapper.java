/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.census;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Census Bureau variable codes to friendly column names for SQL queries.
 *
 * <p>The Census API returns variables with codes like "B01001_001E" which are not
 * user-friendly. This class provides mappings to readable column names like
 * "total_population" while maintaining the semantic meaning.
 */
public class CensusVariableMapper {

  // Population Demographics (B01001)
  private static final Map<String, String> POPULATION_VARIABLES = new HashMap<>();
  static {
    POPULATION_VARIABLES.put("B01001_001E", "total_population");
    POPULATION_VARIABLES.put("B01001_002E", "male_population");
    POPULATION_VARIABLES.put("B01001_026E", "female_population");
    // Age groups
    POPULATION_VARIABLES.put("B01001_003E", "male_under_5");
    POPULATION_VARIABLES.put("B01001_004E", "male_5_to_9");
    POPULATION_VARIABLES.put("B01001_005E", "male_10_to_14");
    POPULATION_VARIABLES.put("B01001_006E", "male_15_to_17");
    POPULATION_VARIABLES.put("B01001_007E", "male_18_19");
    POPULATION_VARIABLES.put("B01001_008E", "male_20");
    POPULATION_VARIABLES.put("B01001_009E", "male_21");
    POPULATION_VARIABLES.put("B01001_010E", "male_22_to_24");
    POPULATION_VARIABLES.put("B01001_011E", "male_25_to_29");
    POPULATION_VARIABLES.put("B01001_012E", "male_30_to_34");
    POPULATION_VARIABLES.put("B01001_013E", "male_35_to_39");
    POPULATION_VARIABLES.put("B01001_014E", "male_40_to_44");
    POPULATION_VARIABLES.put("B01001_015E", "male_45_to_49");
    POPULATION_VARIABLES.put("B01001_016E", "male_50_to_54");
    POPULATION_VARIABLES.put("B01001_017E", "male_55_to_59");
    POPULATION_VARIABLES.put("B01001_018E", "male_60_61");
    POPULATION_VARIABLES.put("B01001_019E", "male_62_64");
    POPULATION_VARIABLES.put("B01001_020E", "male_65_66");
    POPULATION_VARIABLES.put("B01001_021E", "male_67_69");
    POPULATION_VARIABLES.put("B01001_022E", "male_70_to_74");
    POPULATION_VARIABLES.put("B01001_023E", "male_75_to_79");
    POPULATION_VARIABLES.put("B01001_024E", "male_80_to_84");
    POPULATION_VARIABLES.put("B01001_025E", "male_85_and_over");
  }

  // Income Variables (B19013, B19301)
  private static final Map<String, String> INCOME_VARIABLES = new HashMap<>();
  static {
    INCOME_VARIABLES.put("B19013_001E", "median_household_income");
    INCOME_VARIABLES.put("B19301_001E", "per_capita_income");
    INCOME_VARIABLES.put("B19001_001E", "total_households");
    INCOME_VARIABLES.put("B19001_002E", "households_less_than_10k");
    INCOME_VARIABLES.put("B19001_003E", "households_10k_to_15k");
    INCOME_VARIABLES.put("B19001_004E", "households_15k_to_20k");
    INCOME_VARIABLES.put("B19001_005E", "households_20k_to_25k");
    INCOME_VARIABLES.put("B19001_006E", "households_25k_to_30k");
    INCOME_VARIABLES.put("B19001_007E", "households_30k_to_35k");
    INCOME_VARIABLES.put("B19001_008E", "households_35k_to_40k");
    INCOME_VARIABLES.put("B19001_009E", "households_40k_to_45k");
    INCOME_VARIABLES.put("B19001_010E", "households_45k_to_50k");
    INCOME_VARIABLES.put("B19001_011E", "households_50k_to_60k");
    INCOME_VARIABLES.put("B19001_012E", "households_60k_to_75k");
    INCOME_VARIABLES.put("B19001_013E", "households_75k_to_100k");
    INCOME_VARIABLES.put("B19001_014E", "households_100k_to_125k");
    INCOME_VARIABLES.put("B19001_015E", "households_125k_to_150k");
    INCOME_VARIABLES.put("B19001_016E", "households_150k_to_200k");
    INCOME_VARIABLES.put("B19001_017E", "households_200k_and_over");
  }

  // Employment Variables (B23025)
  private static final Map<String, String> EMPLOYMENT_VARIABLES = new HashMap<>();
  static {
    EMPLOYMENT_VARIABLES.put("B23025_001E", "total_population_16_and_over");
    EMPLOYMENT_VARIABLES.put("B23025_002E", "labor_force");
    EMPLOYMENT_VARIABLES.put("B23025_003E", "civilian_labor_force");
    EMPLOYMENT_VARIABLES.put("B23025_004E", "employed");
    EMPLOYMENT_VARIABLES.put("B23025_005E", "unemployed");
    EMPLOYMENT_VARIABLES.put("B23025_006E", "armed_forces");
    EMPLOYMENT_VARIABLES.put("B23025_007E", "not_in_labor_force");
  }

  // Housing Variables (B25001, B25002, B25077)
  private static final Map<String, String> HOUSING_VARIABLES = new HashMap<>();
  static {
    HOUSING_VARIABLES.put("B25001_001E", "total_housing_units");
    HOUSING_VARIABLES.put("B25002_001E", "total_housing_units_occupancy");
    HOUSING_VARIABLES.put("B25002_002E", "occupied_housing_units");
    HOUSING_VARIABLES.put("B25002_003E", "vacant_housing_units");
    HOUSING_VARIABLES.put("B25077_001E", "median_home_value");
    HOUSING_VARIABLES.put("B25064_001E", "median_gross_rent");
    HOUSING_VARIABLES.put("B25003_001E", "total_occupied_units");
    HOUSING_VARIABLES.put("B25003_002E", "owner_occupied_units");
    HOUSING_VARIABLES.put("B25003_003E", "renter_occupied_units");
  }

  // Education Variables (B15003)
  private static final Map<String, String> EDUCATION_VARIABLES = new HashMap<>();
  static {
    EDUCATION_VARIABLES.put("B15003_001E", "total_population_25_and_over");
    EDUCATION_VARIABLES.put("B15003_002E", "no_schooling");
    EDUCATION_VARIABLES.put("B15003_003E", "nursery_school");
    EDUCATION_VARIABLES.put("B15003_004E", "kindergarten");
    EDUCATION_VARIABLES.put("B15003_017E", "high_school_graduate");
    EDUCATION_VARIABLES.put("B15003_018E", "ged_or_alternative");
    EDUCATION_VARIABLES.put("B15003_019E", "some_college_less_than_1_year");
    EDUCATION_VARIABLES.put("B15003_020E", "some_college_1_or_more_years");
    EDUCATION_VARIABLES.put("B15003_021E", "associates_degree");
    EDUCATION_VARIABLES.put("B15003_022E", "bachelors_degree");
    EDUCATION_VARIABLES.put("B15003_023E", "masters_degree");
    EDUCATION_VARIABLES.put("B15003_024E", "professional_degree");
    EDUCATION_VARIABLES.put("B15003_025E", "doctorate_degree");
  }

  // Commuting Variables (B08301, B08303)
  private static final Map<String, String> COMMUTING_VARIABLES = new HashMap<>();
  static {
    COMMUTING_VARIABLES.put("B08301_001E", "total_commuters");
    COMMUTING_VARIABLES.put("B08301_002E", "car_truck_van_alone");
    COMMUTING_VARIABLES.put("B08301_003E", "car_truck_van_carpooled");
    COMMUTING_VARIABLES.put("B08301_010E", "public_transportation");
    COMMUTING_VARIABLES.put("B08301_018E", "walked");
    COMMUTING_VARIABLES.put("B08301_019E", "bicycle");
    COMMUTING_VARIABLES.put("B08301_020E", "motorcycle");
    COMMUTING_VARIABLES.put("B08301_021E", "worked_from_home");
    COMMUTING_VARIABLES.put("B08303_001E", "median_travel_time_to_work");
  }

  // Language Variables (B16001)
  private static final Map<String, String> LANGUAGE_VARIABLES = new HashMap<>();
  static {
    LANGUAGE_VARIABLES.put("B16001_001E", "total_population_5_and_over");
    LANGUAGE_VARIABLES.put("B16001_002E", "speak_only_english");
    LANGUAGE_VARIABLES.put("B16001_003E", "speak_language_other_than_english");
  }

  // Disability Variables (B18101)
  private static final Map<String, String> DISABILITY_VARIABLES = new HashMap<>();
  static {
    DISABILITY_VARIABLES.put("B18101_001E", "total_civilian_population");
    DISABILITY_VARIABLES.put("B18101_004E", "male_under_18_with_disability");
    DISABILITY_VARIABLES.put("B18101_007E", "male_18_to_64_with_disability");
    DISABILITY_VARIABLES.put("B18101_010E", "male_65_and_over_with_disability");
    DISABILITY_VARIABLES.put("B18101_013E", "female_under_18_with_disability");
    DISABILITY_VARIABLES.put("B18101_016E", "female_18_to_64_with_disability");
    DISABILITY_VARIABLES.put("B18101_019E", "female_65_and_over_with_disability");
  }

  // Veterans Variables (B21001)
  private static final Map<String, String> VETERANS_VARIABLES = new HashMap<>();
  static {
    VETERANS_VARIABLES.put("B21001_001E", "total_civilian_population_18_and_over");
    VETERANS_VARIABLES.put("B21001_002E", "veteran");
    VETERANS_VARIABLES.put("B21001_005E", "male_veteran");
    VETERANS_VARIABLES.put("B21001_008E", "female_veteran");
    VETERANS_VARIABLES.put("B21001_011E", "nonveteran");
  }

  // Poverty Variables (B17001)
  private static final Map<String, String> POVERTY_VARIABLES = new HashMap<>();
  static {
    POVERTY_VARIABLES.put("B17001_001E", "total_population_for_poverty");
    POVERTY_VARIABLES.put("B17001_002E", "income_below_poverty_level");
    POVERTY_VARIABLES.put("B17001_031E", "income_at_or_above_poverty_level");
    POVERTY_VARIABLES.put("B17010_001E", "families_total");
    POVERTY_VARIABLES.put("B17010_002E", "families_below_poverty");
  }

  // Demographics Variables (B11001, B12001)
  private static final Map<String, String> DEMOGRAPHICS_VARIABLES = new HashMap<>();
  static {
    DEMOGRAPHICS_VARIABLES.put("B11001_001E", "total_households");
    DEMOGRAPHICS_VARIABLES.put("B11001_002E", "family_households");
    DEMOGRAPHICS_VARIABLES.put("B11001_003E", "married_couple_families");
    DEMOGRAPHICS_VARIABLES.put("B11001_007E", "nonfamily_households");
    DEMOGRAPHICS_VARIABLES.put("B12001_001E", "total_population_15_and_over");
    DEMOGRAPHICS_VARIABLES.put("B12001_002E", "never_married");
    DEMOGRAPHICS_VARIABLES.put("B12001_004E", "now_married");
    DEMOGRAPHICS_VARIABLES.put("B12001_009E", "separated");
    DEMOGRAPHICS_VARIABLES.put("B12001_010E", "widowed");
    DEMOGRAPHICS_VARIABLES.put("B12001_012E", "divorced");
  }

  // Housing Costs Variables (B25077, B25064)
  private static final Map<String, String> HOUSING_COSTS_VARIABLES = new HashMap<>();
  static {
    HOUSING_COSTS_VARIABLES.put("B25077_001E", "median_home_value");
    HOUSING_COSTS_VARIABLES.put("B25064_001E", "median_gross_rent");
    HOUSING_COSTS_VARIABLES.put("B25071_001E", "median_rent_as_percentage_of_income");
    HOUSING_COSTS_VARIABLES.put("B25088_001E", "median_selected_monthly_owner_costs");
  }

  // Health Insurance Variables (B27001)
  private static final Map<String, String> HEALTH_INSURANCE_VARIABLES = new HashMap<>();
  static {
    HEALTH_INSURANCE_VARIABLES.put("B27001_001E", "total_population_health_insurance");
    HEALTH_INSURANCE_VARIABLES.put("B27001_004E", "with_health_insurance_coverage");
    HEALTH_INSURANCE_VARIABLES.put("B27001_007E", "no_health_insurance_coverage");
  }

  // Migration Variables (B07001)
  private static final Map<String, String> MIGRATION_VARIABLES = new HashMap<>();
  static {
    MIGRATION_VARIABLES.put("B07001_001E", "total_population_1_year_and_over");
    MIGRATION_VARIABLES.put("B07001_017E", "same_house_1_year_ago");
    MIGRATION_VARIABLES.put("B07001_033E", "moved_within_same_county");
    MIGRATION_VARIABLES.put("B07001_049E", "moved_from_different_county");
    MIGRATION_VARIABLES.put("B07001_065E", "moved_from_different_state");
    MIGRATION_VARIABLES.put("B07001_081E", "moved_from_abroad");
  }

  // Occupation Variables (C24010 - more stable occupation categories)
  private static final Map<String, String> OCCUPATION_VARIABLES = new HashMap<>();
  static {
    // Use C24010 (Occupation by sex) which is more widely available
    OCCUPATION_VARIABLES.put("C24010_001E", "total_employed_population");
    OCCUPATION_VARIABLES.put("C24010_003E", "management_business_science_arts_occupations");
    OCCUPATION_VARIABLES.put("C24010_019E", "service_occupations");
    OCCUPATION_VARIABLES.put("C24010_030E", "sales_office_occupations");
    OCCUPATION_VARIABLES.put("C24010_047E", "natural_resources_construction_maintenance");
    OCCUPATION_VARIABLES.put("C24010_056E", "production_transportation_material_moving");
  }

  // Geographic Variables
  private static final Map<String, String> GEOGRAPHIC_VARIABLES = new HashMap<>();
  static {
    GEOGRAPHIC_VARIABLES.put("state", "state_fips");
    GEOGRAPHIC_VARIABLES.put("county", "county_fips");
    GEOGRAPHIC_VARIABLES.put("tract", "tract_fips");
    GEOGRAPHIC_VARIABLES.put("block group", "block_group_fips");
  }

  // Master mapping combining all variable types
  private static final Map<String, String> ALL_VARIABLES = new HashMap<>();
  static {
    ALL_VARIABLES.putAll(POPULATION_VARIABLES);
    ALL_VARIABLES.putAll(INCOME_VARIABLES);
    ALL_VARIABLES.putAll(EMPLOYMENT_VARIABLES);
    ALL_VARIABLES.putAll(HOUSING_VARIABLES);
    ALL_VARIABLES.putAll(EDUCATION_VARIABLES);
    ALL_VARIABLES.putAll(COMMUTING_VARIABLES);
    ALL_VARIABLES.putAll(LANGUAGE_VARIABLES);
    ALL_VARIABLES.putAll(DISABILITY_VARIABLES);
    ALL_VARIABLES.putAll(VETERANS_VARIABLES);
    ALL_VARIABLES.putAll(POVERTY_VARIABLES);
    ALL_VARIABLES.putAll(DEMOGRAPHICS_VARIABLES);
    ALL_VARIABLES.putAll(HOUSING_COSTS_VARIABLES);
    ALL_VARIABLES.putAll(HEALTH_INSURANCE_VARIABLES);
    ALL_VARIABLES.putAll(MIGRATION_VARIABLES);
    ALL_VARIABLES.putAll(OCCUPATION_VARIABLES);
    ALL_VARIABLES.putAll(GEOGRAPHIC_VARIABLES);
  }

  /**
   * Maps a Census variable code to a friendly column name.
   *
   * @param variableCode Census variable code (e.g., "B01001_001E")
   * @return Friendly column name (e.g., "total_population") or original code if no mapping exists
   */
  public static String mapVariableToFriendlyName(String variableCode) {
    return ALL_VARIABLES.getOrDefault(variableCode, variableCode.toLowerCase());
  }

  /**
   * Creates a geographic identifier (GEOID) from state and county codes.
   *
   * @param stateFips 2-digit state FIPS code
   * @param countyFips 3-digit county FIPS code (null for state-level)
   * @return Combined GEOID for joining with other datasets
   */
  public static String createGeoid(String stateFips, String countyFips) {
    if (countyFips == null || countyFips.trim().isEmpty()) {
      // State-level GEOID is just the state FIPS
      return stateFips;
    } else {
      // County-level GEOID is state + county FIPS
      return stateFips + countyFips;
    }
  }

  // Economic Census Variables (basic business data)
  private static final Map<String, String> ECONOMIC_CENSUS_VARIABLES = new HashMap<>();
  static {
    // Placeholder variables - Economic Census uses different structure
    ECONOMIC_CENSUS_VARIABLES.put("ESTAB", "establishments");
    ECONOMIC_CENSUS_VARIABLES.put("EMP", "employment");
    ECONOMIC_CENSUS_VARIABLES.put("PAYANN", "annual_payroll");
  }

  // County Business Patterns Variables
  private static final Map<String, String> COUNTY_BUSINESS_PATTERNS_VARIABLES = new HashMap<>();
  static {
    // Copy from economic census for now
    COUNTY_BUSINESS_PATTERNS_VARIABLES.putAll(ECONOMIC_CENSUS_VARIABLES);
    COUNTY_BUSINESS_PATTERNS_VARIABLES.put("NAICS2017", "naics_code");
  }

  // Population Estimates Variables
  private static final Map<String, String> POPULATION_ESTIMATES_VARIABLES = new HashMap<>();
  static {
    POPULATION_ESTIMATES_VARIABLES.put("POP", "total_population");
    POPULATION_ESTIMATES_VARIABLES.put("POPEST", "population_estimate");
  }

  // Decennial Census Variables (P tables for population, H tables for housing)
  private static final Map<String, String> DECENNIAL_POPULATION_VARIABLES = new HashMap<>();
  static {
    // Total population (P001001)
    DECENNIAL_POPULATION_VARIABLES.put("P1_001N", "total_population");

    // Race variables (P003)
    // Removed total_population_race - not needed
    DECENNIAL_POPULATION_VARIABLES.put("P1_003N", "white_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_004N", "black_african_american_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_005N", "american_indian_alaska_native_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_006N", "asian_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_007N", "native_hawaiian_pacific_islander_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_008N", "some_other_race_alone");
    DECENNIAL_POPULATION_VARIABLES.put("P1_009N", "two_or_more_races");

    // Age and sex (P012)
    // Removed age/sex variables - use simpler 2020 format
    // Removed male_total - use simpler 2020 format
    // Removed female_total - use simpler 2020 format
  }

  private static final Map<String, String> DECENNIAL_DEMOGRAPHICS_VARIABLES = new HashMap<>();
  static {
    // Copy population variables for demographics table
    DECENNIAL_DEMOGRAPHICS_VARIABLES.putAll(DECENNIAL_POPULATION_VARIABLES);
  }

  private static final Map<String, String> DECENNIAL_HOUSING_VARIABLES = new HashMap<>();
  static {
    // Total housing units (H001)
    DECENNIAL_HOUSING_VARIABLES.put("H1_001N", "total_housing_units");

    // Occupancy status (H003)
    DECENNIAL_HOUSING_VARIABLES.put("H1_002N", "occupied_housing_units");
    DECENNIAL_HOUSING_VARIABLES.put("H1_003N", "vacant_housing_units");
    // Updated to 2020 format
  }

  /**
   * Returns all mappings for a specific table type.
   */
  public static Map<String, String> getVariablesForTable(String tableName) {
    switch (tableName.toLowerCase()) {
      case "acs_population":
        return new HashMap<>(POPULATION_VARIABLES);
      case "acs_income":
        return new HashMap<>(INCOME_VARIABLES);
      case "acs_employment":
        return new HashMap<>(EMPLOYMENT_VARIABLES);
      case "acs_housing":
        return new HashMap<>(HOUSING_VARIABLES);
      case "acs_education":
        return new HashMap<>(EDUCATION_VARIABLES);
      case "acs_commuting":
        return new HashMap<>(COMMUTING_VARIABLES);
      case "acs_language":
        return new HashMap<>(LANGUAGE_VARIABLES);
      case "acs_disability":
        return new HashMap<>(DISABILITY_VARIABLES);
      case "acs_veterans":
        return new HashMap<>(VETERANS_VARIABLES);
      case "acs_poverty":
        return new HashMap<>(POVERTY_VARIABLES);
      case "acs_demographics":
        return new HashMap<>(DEMOGRAPHICS_VARIABLES);
      case "acs_housing_costs":
        return new HashMap<>(HOUSING_COSTS_VARIABLES);
      case "acs_health_insurance":
        return new HashMap<>(HEALTH_INSURANCE_VARIABLES);
      case "acs_migration":
        return new HashMap<>(MIGRATION_VARIABLES);
      case "acs_occupation":
        return new HashMap<>(OCCUPATION_VARIABLES);
      case "decennial_population":
        return new HashMap<>(DECENNIAL_POPULATION_VARIABLES);
      case "decennial_demographics":
        return new HashMap<>(DECENNIAL_DEMOGRAPHICS_VARIABLES);
      case "decennial_housing":
        return new HashMap<>(DECENNIAL_HOUSING_VARIABLES);
      case "economic_census":
        return new HashMap<>(ECONOMIC_CENSUS_VARIABLES);
      case "county_business_patterns":
        return new HashMap<>(COUNTY_BUSINESS_PATTERNS_VARIABLES);
      case "population_estimates":
        return new HashMap<>(POPULATION_ESTIMATES_VARIABLES);
      default:
        return new HashMap<>();
    }
  }

  /**
   * Returns the Census variables needed to download for a specific table.
   */
  public static String[] getVariablesToDownload(String tableName) {
    Map<String, String> variables = getVariablesForTable(tableName);
    return variables.keySet().toArray(new String[0]);
  }
}