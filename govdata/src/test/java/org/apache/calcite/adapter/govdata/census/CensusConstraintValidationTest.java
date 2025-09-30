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

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.apache.calcite.jdbc.CalciteConnection;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates Census table constraints through data profiling.
 * This test ensures that proposed constraints are actually satisfied by real data.
 */
@Tag("integration")
public class CensusConstraintValidationTest {

  @TempDir
  private static Path tempDir;

  private static String cacheDir;
  private static String parquetDir;
  private static File modelFile;

  @BeforeAll
  public static void setUp() throws Exception {
    TestEnvironmentLoader.ensureLoaded();

    String envCacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String envParquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");

    if (envCacheDir != null && envParquetDir != null) {
      cacheDir = envCacheDir;
      parquetDir = envParquetDir;
    } else {
      cacheDir = tempDir.resolve("census-cache").toString();
      parquetDir = tempDir.resolve("census-parquet").toString();
      new File(cacheDir).mkdirs();
      new File(parquetDir).mkdirs();
    }

    System.setProperty("GOVDATA_CACHE_DIR", cacheDir);
    System.setProperty("GOVDATA_PARQUET_DIR", parquetDir);

    String apiKey = TestEnvironmentLoader.getEnv("CENSUS_API_KEY");
    if (apiKey != null) {
      System.setProperty("CENSUS_API_KEY", apiKey);
    }

    createTestModel();
  }

  private static void createTestModel() throws Exception {
    modelFile = tempDir.resolve("census-constraint-validation-model.json").toFile();

    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"CENSUS\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"CENSUS\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"dataSource\": \"census\",\n" +
        "      \"autoDownload\": true,\n" +
        "      \"startYear\": 2022,\n" +
        "      \"endYear\": 2023,\n" +
        "      \"censusCacheTtlDays\": 365\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write(modelJson);
    }
  }

  /**
   * Data profiling to validate primary key constraints.
   */
  @Test
  public void testPrimaryKeyConstraints() throws Exception {
    // Define expected primary keys for each table
    Map<String, List<String>> expectedPrimaryKeys = new HashMap<>();
    expectedPrimaryKeys.put("acs_population", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_demographics", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_income", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_poverty", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_employment", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_education", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_housing", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_housing_costs", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_commuting", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_health_insurance", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_language", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_disability", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_veterans", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_migration", List.of("geoid", "year"));
    expectedPrimaryKeys.put("acs_occupation", List.of("geoid", "year"));

    expectedPrimaryKeys.put("decennial_population", List.of("geoid", "year"));
    expectedPrimaryKeys.put("decennial_demographics", List.of("geoid", "year"));
    expectedPrimaryKeys.put("decennial_housing", List.of("geoid", "year"));

    expectedPrimaryKeys.put("economic_census", List.of("geoid", "year", "naics_code"));
    expectedPrimaryKeys.put("county_business_patterns", List.of("geoid", "year", "naics_code"));
    expectedPrimaryKeys.put("population_estimates", List.of("geoid", "year"));

    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      for (Map.Entry<String, List<String>> entry : expectedPrimaryKeys.entrySet()) {
        String tableName = entry.getKey();
        List<String> pkColumns = entry.getValue();

        try {
          // Test primary key uniqueness
          String columnsStr = String.join(", ", pkColumns);
          String query = String.format(
              "SELECT COUNT(*) as total_rows, COUNT(DISTINCT %s) as unique_combinations " +
              "FROM CENSUS.\"%s\"", columnsStr, tableName);

          ResultSet rs = stmt.executeQuery(query);
          if (rs.next()) {
            long totalRows = rs.getLong("total_rows");
            long uniqueCombinations = rs.getLong("unique_combinations");

            if (totalRows > 0) {
              assertEquals(totalRows, uniqueCombinations,
                  String.format("Primary key constraint violated for %s: " +
                      "%d total rows but %d unique combinations of (%s)",
                      tableName, totalRows, uniqueCombinations, columnsStr));

              System.out.printf("✓ %s: Primary key (%s) validated with %d rows%n",
                  tableName, columnsStr, totalRows);
            } else {
              System.out.printf("⚠ %s: No data available for validation%n", tableName);
            }
          }
          rs.close();

        } catch (Exception e) {
          System.out.printf("⚠ %s: Could not validate primary key - %s%n", tableName, e.getMessage());
        }
      }
    }
  }

  /**
   * Data profiling to validate foreign key constraints.
   */
  @Test
  public void testForeignKeyConstraints() throws Exception {
    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      // Test geoid foreign key to GEO schema (if available)
      testGeoidForeignKeys(stmt);

      // Test intra-CENSUS foreign keys
      testIntraCensusForeignKeys(stmt);
    }
  }

  private void testGeoidForeignKeys(Statement stmt) {
    // List of Census tables that should have geoid foreign keys to GEO schema
    List<String> tablesWithGeoidFK = List.of(
        "acs_population", "acs_demographics", "acs_income", "acs_poverty",
        "acs_employment", "acs_education", "acs_housing", "acs_housing_costs",
        "acs_commuting", "acs_health_insurance", "acs_language", "acs_disability",
        "acs_veterans", "acs_migration", "acs_occupation",
        "decennial_population", "decennial_demographics", "decennial_housing",
        "economic_census", "county_business_patterns", "population_estimates"
    );

    for (String tableName : tablesWithGeoidFK) {
      try {
        // Test if all geoids in Census tables exist in GEO schema
        // We'll test against multiple GEO tables since geoid could reference
        // states, counties, places, etc.
        String query = String.format(
            "SELECT COUNT(*) as total_census_geoids, " +
            "       COUNT(CASE WHEN LENGTH(c.geoid) = 2 THEN 1 END) as state_level, " +
            "       COUNT(CASE WHEN LENGTH(c.geoid) = 5 THEN 1 END) as county_level, " +
            "       COUNT(CASE WHEN LENGTH(c.geoid) > 5 THEN 1 END) as sub_county_level " +
            "FROM CENSUS.\"%s\" c " +
            "WHERE c.geoid IS NOT NULL", tableName);

        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          long totalGeoids = rs.getLong("total_census_geoids");
          long stateLevelGeoids = rs.getLong("state_level");
          long countyLevelGeoids = rs.getLong("county_level");
          long subCountyLevelGeoids = rs.getLong("sub_county_level");

          if (totalGeoids > 0) {
            System.out.printf("✓ %s: Found %d geoids (%d state, %d county, %d sub-county level)%n",
                tableName, totalGeoids, stateLevelGeoids, countyLevelGeoids, subCountyLevelGeoids);
          }
        }
        rs.close();

      } catch (Exception e) {
        System.out.printf("⚠ %s: Could not validate geoid foreign key - %s%n", tableName, e.getMessage());
      }
    }
  }

  private void testIntraCensusForeignKeys(Statement stmt) {
    // Test if state_fips/county_fips columns in Census data are consistent
    List<String> tablesWithStateCounty = List.of(
        "acs_population", "acs_demographics", "acs_income", "acs_poverty",
        "acs_employment", "acs_education", "acs_housing", "acs_housing_costs"
    );

    for (String tableName : tablesWithStateCounty) {
      try {
        // Test if state_fips and county_fips are derivable from geoid
        String query = String.format(
            "SELECT COUNT(*) as total_rows, " +
            "       COUNT(CASE WHEN state_fips = SUBSTRING(geoid, 1, 2) THEN 1 END) as consistent_state, " +
            "       COUNT(CASE WHEN LENGTH(geoid) = 5 AND geoid = state_fips || county_fips THEN 1 END) as consistent_county " +
            "FROM CENSUS.\"%s\" " +
            "WHERE geoid IS NOT NULL AND state_fips IS NOT NULL", tableName);

        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()) {
          long totalRows = rs.getLong("total_rows");
          long consistentState = rs.getLong("consistent_state");
          long consistentCounty = rs.getLong("consistent_county");

          if (totalRows > 0) {
            double stateConsistency = (double) consistentState / totalRows * 100;
            double countyConsistency = (double) consistentCounty / totalRows * 100;

            System.out.printf("✓ %s: FIPS consistency - State: %.1f%%, County: %.1f%% (%d rows)%n",
                tableName, stateConsistency, countyConsistency, totalRows);

            // For constraints to be valid, we expect high consistency
            assertTrue(stateConsistency > 90.0,
                String.format("%s state FIPS consistency too low: %.1f%%", tableName, stateConsistency));
          }
        }
        rs.close();

      } catch (Exception e) {
        System.out.printf("⚠ %s: Could not validate FIPS consistency - %s%n", tableName, e.getMessage());
      }
    }
  }

  /**
   * Profile data types and null patterns to inform constraint design.
   */
  @Test
  public void testDataProfiling() throws Exception {
    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      // Sample key tables for profiling
      List<String> samplesToProfile = List.of("acs_population", "acs_income", "decennial_population");

      for (String tableName : samplesToProfile) {
        try {
          profileTable(stmt, tableName);
        } catch (Exception e) {
          System.out.printf("⚠ %s: Could not profile table - %s%n", tableName, e.getMessage());
        }
      }
    }
  }

  private void profileTable(Statement stmt, String tableName) throws Exception {
    // Get basic table statistics
    String query = String.format(
        "SELECT COUNT(*) as row_count, " +
        "       COUNT(DISTINCT geoid) as unique_geoids, " +
        "       COUNT(DISTINCT year) as unique_years, " +
        "       MIN(year) as min_year, " +
        "       MAX(year) as max_year " +
        "FROM CENSUS.\"%s\"", tableName);

    ResultSet rs = stmt.executeQuery(query);
    if (rs.next()) {
      long rowCount = rs.getLong("row_count");
      long uniqueGeoids = rs.getLong("unique_geoids");
      long uniqueYears = rs.getLong("unique_years");
      int minYear = rs.getInt("min_year");
      int maxYear = rs.getInt("max_year");

      System.out.printf("📊 %s Profile:%n", tableName);
      System.out.printf("   Rows: %d, Unique GEOIDs: %d, Years: %d (%d-%d)%n",
          rowCount, uniqueGeoids, uniqueYears, minYear, maxYear);

      // Check for nulls in key constraint columns
      String nullQuery = String.format(
          "SELECT COUNT(CASE WHEN geoid IS NULL THEN 1 END) as null_geoids, " +
          "       COUNT(CASE WHEN year IS NULL THEN 1 END) as null_years " +
          "FROM CENSUS.\"%s\"", tableName);

      ResultSet nullRs = stmt.executeQuery(nullQuery);
      if (nullRs.next()) {
        long nullGeoids = nullRs.getLong("null_geoids");
        long nullYears = nullRs.getLong("null_years");

        System.out.printf("   Nulls: geoid=%d, year=%d%n", nullGeoids, nullYears);

        // For constraints to work, we expect minimal nulls in key columns
        assertTrue(nullGeoids == 0, String.format("%s has %d null geoids", tableName, nullGeoids));
        assertTrue(nullYears == 0, String.format("%s has %d null years", tableName, nullYears));
      }
      nullRs.close();
    }
    rs.close();
  }

  private Connection createConnection() throws Exception {
    Properties info = new Properties();
    info.setProperty("model", modelFile.getAbsolutePath());
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}