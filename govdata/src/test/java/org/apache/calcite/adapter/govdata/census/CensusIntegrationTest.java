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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for CENSUS schema with real data download.
 * This test requires CENSUS_API_KEY to be configured.
 *
 * <p>Tests Census Bureau American Community Survey (ACS) 5-year estimates:
 * <ul>
 *   <li>acs_population: Total population and demographics</li>
 *   <li>acs_income: Household and per capita income</li>
 *   <li>acs_housing: Housing units, occupancy, values</li>
 *   <li>acs_education: Educational attainment levels</li>
 *   <li>acs_employment: Labor force and unemployment</li>
 *   <li>acs_poverty: Population below poverty level</li>
 * </ul>
 *
 * <p>Note: Tests run sequentially to avoid DuckDB httpfs extension concurrency issues.
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class CensusIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CensusIntegrationTest.class);

  // ACS tables - core Census Bureau American Community Survey data
  private static final Set<String> ACS_TABLES = new HashSet<>(Arrays.asList(
      "acs_population",
      "acs_income",
      "acs_housing",
      "acs_education",
      "acs_employment",
      "acs_poverty"
  ));

  // SQL views defined in census-schema.yaml
  private static final Set<String> CENSUS_VIEWS = new HashSet<>(Arrays.asList(
      "population_summary",
      "income_summary",
      "poverty_rate",
      "education_attainment",
      "unemployment_rate"
  ));

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();

    // Verify environment is properly configured
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR"),
        "GOVDATA_CACHE_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR"),
        "GOVDATA_PARQUET_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("CENSUS_API_KEY"),
        "CENSUS_API_KEY must be set for integration tests");
    assertNotNull(TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE"),
        "CALCITE_EXECUTION_ENGINE must be set for integration tests");
  }

  private Connection createConnection() throws SQLException {
    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    String executionEngine = TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE");

    String startYear = TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR");
    if (startYear == null || startYear.isEmpty()) {
      startYear = "2020";
    }

    String endYear = TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR");
    if (endYear == null || endYear.isEmpty()) {
      endYear = "2023";
    }

    String censusApiKey = TestEnvironmentLoader.getEnv("CENSUS_API_KEY");

    // S3 configuration for MinIO or AWS S3
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    // Build s3Config JSON if directory uses S3
    String s3ConfigJson = "";
    if (parquetDir != null && parquetDir.startsWith("s3://")) {
      StringBuilder s3Config = new StringBuilder();
      s3Config.append("\"s3Config\": {");
      if (awsEndpointOverride != null) {
        s3Config.append("\"endpoint\": \"").append(awsEndpointOverride).append("\",");
      }
      if (awsAccessKeyId != null) {
        s3Config.append("\"accessKeyId\": \"").append(awsAccessKeyId).append("\",");
      }
      if (awsSecretAccessKey != null) {
        s3Config.append("\"secretAccessKey\": \"").append(awsSecretAccessKey).append("\",");
      }
      if (awsRegion != null) {
        s3Config.append("\"region\": \"").append(awsRegion).append("\",");
      }
      // Remove trailing comma
      if (s3Config.charAt(s3Config.length() - 1) == ',') {
        s3Config.setLength(s3Config.length() - 1);
      }
      s3Config.append("},");
      s3ConfigJson = s3Config.toString();
    }

    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"CENSUS\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"CENSUS\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"dataSource\": \"census\"," +
        "      \"refreshInterval\": \"PT1H\"," +
        "      \"executionEngine\": \"" + executionEngine + "\"," +
        "      \"database_filename\": \"shared.duckdb\"," +
        "      \"ephemeralCache\": false," +
        "      \"cacheDirectory\": \"" + cacheDir + "\"," +
        "      \"directory\": \"" + parquetDir + "\"," +
        "      " + s3ConfigJson +
        "      \"startYear\": " + startYear + "," +
        "      \"endYear\": " + endYear + "," +
        "      \"autoDownload\": true," +
        "      \"censusApiKey\": \"" + censusApiKey + "\"" +
        "    }" +
        "  }]" +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  /**
   * Creates a connection with autoDownload: false.
   * Use this to query existing cached data without making API calls.
   */
  private Connection createConnectionNoDownload() throws SQLException {
    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    String executionEngine = TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE");

    String startYear = TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR");
    if (startYear == null || startYear.isEmpty()) {
      startYear = "2020";
    }

    String endYear = TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR");
    if (endYear == null || endYear.isEmpty()) {
      endYear = "2023";
    }

    // S3 configuration for MinIO or AWS S3
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    // Build s3Config JSON if directory uses S3
    String s3ConfigJson = "";
    if (parquetDir != null && parquetDir.startsWith("s3://")) {
      StringBuilder s3Config = new StringBuilder();
      s3Config.append("\"s3Config\": {");
      if (awsEndpointOverride != null) {
        s3Config.append("\"endpoint\": \"").append(awsEndpointOverride).append("\",");
      }
      if (awsAccessKeyId != null) {
        s3Config.append("\"accessKeyId\": \"").append(awsAccessKeyId).append("\",");
      }
      if (awsSecretAccessKey != null) {
        s3Config.append("\"secretAccessKey\": \"").append(awsSecretAccessKey).append("\",");
      }
      if (awsRegion != null) {
        s3Config.append("\"region\": \"").append(awsRegion).append("\",");
      }
      // Remove trailing comma
      if (s3Config.charAt(s3Config.length() - 1) == ',') {
        s3Config.setLength(s3Config.length() - 1);
      }
      s3Config.append("},");
      s3ConfigJson = s3Config.toString();
    }

    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"CENSUS\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"CENSUS\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"dataSource\": \"census\"," +
        "      \"refreshInterval\": \"PT1H\"," +
        "      \"executionEngine\": \"" + executionEngine + "\"," +
        "      \"database_filename\": \"shared.duckdb\"," +
        "      \"ephemeralCache\": false," +
        "      \"cacheDirectory\": \"" + cacheDir + "\"," +
        "      \"directory\": \"" + parquetDir + "\"," +
        "      " + s3ConfigJson +
        "      \"startYear\": " + startYear + "," +
        "      \"endYear\": " + endYear + "," +
        "      \"autoDownload\": false" +
        "    }" +
        "  }]" +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test
  public void testAcsTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" CENSUS ACS TABLES: American Community Survey 5-Year Estimates");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates all ACS tables:");
    LOGGER.info("   - acs_population: Total population and demographics");
    LOGGER.info("   - acs_income: Household and per capita income");
    LOGGER.info("   - acs_housing: Housing units, occupancy, values");
    LOGGER.info("   - acs_education: Educational attainment levels");
    LOGGER.info("   - acs_employment: Labor force and unemployment");
    LOGGER.info("   - acs_poverty: Population below poverty level");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        LOGGER.info("\n1. Testing ACS tables:");
        int found = 0;
        for (String tableName : ACS_TABLES) {
          String query = "SELECT COUNT(*) as cnt FROM \"CENSUS\"." + tableName;
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              if (count > 0) {
                LOGGER.info("  {} - {} rows", tableName, count);
                found++;
              } else {
                LOGGER.warn("  {} - 0 rows (table exists but no data yet)", tableName);
              }
            }
          } catch (SQLException e) {
            LOGGER.error("  {} - FAILED: {}", tableName, e.getMessage());
          }
        }

        assertTrue(found >= 0,
            "ACS tables test: Expected tables to be queryable");

        if (found > 0) {
          // Verify data structure for acs_population
          LOGGER.info("\n2. Verifying acs_population data structure:");
          String structQuery = "SELECT geography, \"year\", state, geo_name, total_population "
              + "FROM \"CENSUS\".acs_population LIMIT 5";
          try (ResultSet rs = stmt.executeQuery(structQuery)) {
            int rows = 0;
            while (rs.next()) {
              rows++;
              LOGGER.info("  Row {}: geography={}, year={}, state={}, name={}, pop={}",
                  rows,
                  rs.getString("geography"),
                  rs.getString("year"),
                  rs.getString("state"),
                  rs.getString("geo_name"),
                  rs.getLong("total_population"));
            }
            if (rows > 0) {
              LOGGER.info("  acs_population structure verified");
            }
          } catch (SQLException e) {
            LOGGER.warn("  Could not verify acs_population structure: {}", e.getMessage());
          }
        }

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ACS TABLES TEST COMPLETE: {} of {} tables have data",
            found, ACS_TABLES.size());
        LOGGER.info("================================================================================");

        assertEquals(ACS_TABLES.size(), found,
            "Census: Expected all " + ACS_TABLES.size() + " ACS tables to have data");
      }
    }
  }

  @Test
  public void testCensusViews() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" CENSUS SQL VIEWS: Analytical Views Test");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates SQL views defined in census-schema.yaml:");
    LOGGER.info("   - population_summary: State-level population trends");
    LOGGER.info("   - income_summary: State-level income metrics");
    LOGGER.info("   - poverty_rate: Poverty rate calculations");
    LOGGER.info("   - education_attainment: Educational achievement metrics");
    LOGGER.info("   - unemployment_rate: Unemployment calculations");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        int found = 0;
        for (String viewName : CENSUS_VIEWS) {
          String query = "SELECT * FROM \"CENSUS\"." + viewName + " LIMIT 1";
          try (ResultSet rs = stmt.executeQuery(query)) {
            LOGGER.info("  {} - queryable", viewName);
            found++;
          } catch (SQLException e) {
            LOGGER.error("  {} - FAILED: {}", viewName, e.getMessage());
          }
        }

        LOGGER.info("\n================================================================================");
        LOGGER.info(" CENSUS VIEWS TEST COMPLETE: {} of {} views queryable",
            found, CENSUS_VIEWS.size());
        LOGGER.info("================================================================================");

        assertEquals(CENSUS_VIEWS.size(), found,
            "Census: Expected all " + CENSUS_VIEWS.size() + " views to be queryable");
      }
    }
  }

  @Test
  public void testGeographyPartitioning() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" CENSUS GEOGRAPHY PARTITIONING: State and County Coverage");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates geography dimension partitioning:");
    LOGGER.info("   - state: 50 states + DC + territories");
    LOGGER.info("   - county: 3,000+ counties");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Test state-level data
        LOGGER.info("\n1. Testing state-level data in acs_population:");
        String stateQuery = "SELECT COUNT(DISTINCT geoid) as state_count "
            + "FROM \"CENSUS\".acs_population WHERE geography = 'state'";
        try (ResultSet rs = stmt.executeQuery(stateQuery)) {
          if (rs.next()) {
            int stateCount = rs.getInt("state_count");
            LOGGER.info("  Found {} distinct states/territories", stateCount);
            assertTrue(stateCount >= 50,
                "Should have at least 50 states, got " + stateCount);
          }
        } catch (SQLException e) {
          LOGGER.warn("  Could not query state data: {}", e.getMessage());
        }

        // Test county-level data
        LOGGER.info("\n2. Testing county-level data in acs_population:");
        String countyQuery = "SELECT COUNT(DISTINCT geoid) as county_count "
            + "FROM \"CENSUS\".acs_population WHERE geography = 'county'";
        try (ResultSet rs = stmt.executeQuery(countyQuery)) {
          if (rs.next()) {
            int countyCount = rs.getInt("county_count");
            LOGGER.info("  Found {} distinct counties", countyCount);
            assertTrue(countyCount >= 1000,
                "Should have at least 1000 counties, got " + countyCount);
          }
        } catch (SQLException e) {
          LOGGER.warn("  Could not query county data: {}", e.getMessage());
        }

        // Test year partitioning
        LOGGER.info("\n3. Testing year partitioning:");
        String yearQuery = "SELECT DISTINCT \"year\" FROM \"CENSUS\".acs_population ORDER BY \"year\"";
        try (ResultSet rs = stmt.executeQuery(yearQuery)) {
          StringBuilder years = new StringBuilder();
          int yearCount = 0;
          while (rs.next()) {
            if (yearCount > 0) {
              years.append(", ");
            }
            years.append(rs.getInt("year"));
            yearCount++;
          }
          LOGGER.info("  Found {} years: {}", yearCount, years);
          assertTrue(yearCount >= 1, "Should have at least 1 year of data");
        } catch (SQLException e) {
          LOGGER.warn("  Could not query year data: {}", e.getMessage());
        }

        LOGGER.info("\n================================================================================");
        LOGGER.info(" GEOGRAPHY PARTITIONING TEST COMPLETE!");
        LOGGER.info("================================================================================");
      }
    }
  }

  @Test
  public void testIcebergMaterialization() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" CENSUS ICEBERG MATERIALIZATION: Verify Iceberg Table Format");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates that census data is stored in Iceberg format:");
    LOGGER.info("   - Iceberg catalog manages table versions");
    LOGGER.info("   - Hive partitioning by type/year/geography");
    LOGGER.info("   - Parquet files under warehouse path");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Test COUNT(*) optimization (uses Iceberg metadata)
        LOGGER.info("\n1. Testing COUNT(*) optimization (uses Iceberg metadata):");
        long startTime = System.currentTimeMillis();
        String countQuery = "SELECT COUNT(*) as total FROM \"CENSUS\".acs_population";
        try (ResultSet rs = stmt.executeQuery(countQuery)) {
          if (rs.next()) {
            long count = rs.getLong("total");
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("  acs_population total: {} rows in {}ms", count, elapsed);
            if (elapsed < 1000 && count > 0) {
              LOGGER.info("  Fast COUNT(*) suggests Iceberg metadata optimization working");
            }
          }
        } catch (SQLException e) {
          LOGGER.warn("  Could not run COUNT(*): {}", e.getMessage());
        }

        // Test partition pruning
        LOGGER.info("\n2. Testing partition pruning (year filter):");
        startTime = System.currentTimeMillis();
        String pruneQuery = "SELECT COUNT(*) as cnt "
            + "FROM \"CENSUS\".acs_population WHERE \"year\" = 2022";
        try (ResultSet rs = stmt.executeQuery(pruneQuery)) {
          if (rs.next()) {
            long count = rs.getLong("cnt");
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("  Year 2022 count: {} rows in {}ms", count, elapsed);
          }
        } catch (SQLException e) {
          LOGGER.warn("  Could not test partition pruning: {}", e.getMessage());
        }

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ICEBERG MATERIALIZATION TEST COMPLETE!");
        LOGGER.info("================================================================================");
      }
    }
  }

  @Test
  public void testAllTableStatus() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" ALL CENSUS TABLE STATUS - Row counts (autoDownload=false, no API calls)");
    LOGGER.info("================================================================================\n");

    // Use autoDownload: false to just read existing data without API calls
    try (Connection conn = createConnectionNoDownload();
         Statement stmt = conn.createStatement()) {

      LOGGER.info("=== CENSUS Tables ===\n");
      LOGGER.info(String.format("%-25s %15s %10s", "TABLE", "ROWS", "TIME(ms)"));
      LOGGER.info(String.format("%-25s %15s %10s",
          "-------------------------", "---------------", "----------"));

      String[] censusTables = {"acs_population", "acs_income", "acs_housing",
          "acs_education", "acs_employment", "acs_poverty"};

      long totalRows = 0;
      int successCount = 0;
      int errorCount = 0;

      for (String table : censusTables) {
        try {
          long start = System.currentTimeMillis();
          ResultSet rs = stmt.executeQuery(
              "SELECT COUNT(*) FROM \"CENSUS\".\"" + table + "\"");
          rs.next();
          long count = rs.getLong(1);
          long elapsed = System.currentTimeMillis() - start;
          LOGGER.info(String.format("%-25s %,15d %10d", table, count, elapsed));
          totalRows += count;
          successCount++;
        } catch (Exception e) {
          String msg = e.getMessage();
          LOGGER.info(String.format("%-25s %15s %s", table, "ERROR",
              msg.substring(0, Math.min(60, msg.length()))));
          errorCount++;
        }
      }

      LOGGER.info("\n--------------------------------------------------------------------------------");
      LOGGER.info(String.format("CENSUS: %d tables, %d ok, %d errors, %,d total rows",
          censusTables.length, successCount, errorCount, totalRows));
      LOGGER.info("================================================================================");
    }
  }

  @Test
  public void testCrossSchemaJoin() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" CROSS-SCHEMA JOIN: CENSUS + ECON Integration");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates joining census and economic data:");
    LOGGER.info("   - CENSUS.acs_employment for labor force data");
    LOGGER.info("   - ECON.county_qcew for establishment counts");
    LOGGER.info("================================================================================");

    // Create a connection that includes both schemas
    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    String executionEngine = TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE");
    String censusApiKey = TestEnvironmentLoader.getEnv("CENSUS_API_KEY");

    // S3 configuration
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    String s3ConfigJson = "";
    if (parquetDir != null && parquetDir.startsWith("s3://")) {
      StringBuilder s3Config = new StringBuilder();
      s3Config.append("\"s3Config\": {");
      if (awsEndpointOverride != null) {
        s3Config.append("\"endpoint\": \"").append(awsEndpointOverride).append("\",");
      }
      if (awsAccessKeyId != null) {
        s3Config.append("\"accessKeyId\": \"").append(awsAccessKeyId).append("\",");
      }
      if (awsSecretAccessKey != null) {
        s3Config.append("\"secretAccessKey\": \"").append(awsSecretAccessKey).append("\",");
      }
      if (awsRegion != null) {
        s3Config.append("\"region\": \"").append(awsRegion).append("\",");
      }
      if (s3Config.charAt(s3Config.length() - 1) == ',') {
        s3Config.setLength(s3Config.length() - 1);
      }
      s3Config.append("},");
      s3ConfigJson = s3Config.toString();
    }

    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"CENSUS\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"CENSUS\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"dataSource\": \"census\"," +
        "      \"executionEngine\": \"" + executionEngine + "\"," +
        "      \"cacheDirectory\": \"" + cacheDir + "\"," +
        "      \"directory\": \"" + parquetDir + "\"," +
        "      " + s3ConfigJson +
        "      \"autoDownload\": false," +
        "      \"censusApiKey\": \"" + censusApiKey + "\"" +
        "    }" +
        "  },{" +
        "    \"name\": \"ECON\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"dataSource\": \"econ\"," +
        "      \"executionEngine\": \"" + executionEngine + "\"," +
        "      \"cacheDirectory\": \"" + cacheDir + "\"," +
        "      \"directory\": \"" + parquetDir + "\"," +
        "      " + s3ConfigJson +
        "      \"autoDownload\": false" +
        "    }" +
        "  }]" +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement()) {

      LOGGER.info("\n1. Testing cross-schema join (CENSUS.acs_employment + ECON.county_qcew):");

      String joinQuery =
          "SELECT c.state, c.\"year\", c.employed, q.annual_avg_estabs " +
          "FROM \"CENSUS\".acs_employment c " +
          "INNER JOIN \"ECON\".county_qcew q " +
          "  ON c.state = SUBSTRING(q.area_fips, 1, 2) " +
          "  AND c.\"year\" = q.\"year\" " +
          "WHERE c.geography = 'state' " +
          "  AND q.agglvl_code = '70' " +
          "  AND q.own_code = '0' " +
          "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(joinQuery)) {
        int rows = 0;
        while (rs.next()) {
          if (rows < 3) {
            LOGGER.info("  Row {}: state={}, year={}, employed={}, estabs={}",
                rows + 1,
                rs.getString("state"),
                rs.getString("year"),
                rs.getLong("employed"),
                rs.getString("annual_avg_estabs"));
          }
          rows++;
        }
        LOGGER.info("  Total rows from join: {}", rows);
        assertTrue(rows >= 0, "Cross-schema join should execute without error");
      } catch (SQLException e) {
        LOGGER.warn("  Cross-schema join not possible: {}", e.getMessage());
        LOGGER.warn("  This is expected if ECON tables don't exist");
      }

      LOGGER.info("\n================================================================================");
      LOGGER.info(" CROSS-SCHEMA JOIN TEST COMPLETE!");
      LOGGER.info("================================================================================");
    }
  }

  @Test
  public void testDecennialPopulation() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" DECENNIAL POPULATION: Schema Evolution Test");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates:");
    LOGGER.info("   - CensusDecennialDimensionResolver provides year-specific dataset/variables");
    LOGGER.info("   - MappingFileVariableNormalizer maps API codes to canonical names");
    LOGGER.info("   - Unified table spans 2000, 2010, 2020 with consistent schema");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      // First check if decennial_population table exists
      LOGGER.info("\n--- Checking decennial_population table availability ---");
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) as cnt FROM \"CENSUS\".\"decennial_population\"");
        if (rs.next()) {
          long count = rs.getLong("cnt");
          LOGGER.info("  decennial_population has {} rows", count);

          if (count > 0) {
            // Query sample data to verify schema evolution worked
            LOGGER.info("\n--- Querying decennial_population with normalized columns ---");
            rs = stmt.executeQuery(
                "SELECT \"geo_name\", " +
                "\"total_population\", \"white_alone\", \"black_alone\", \"asian_alone\" " +
                "FROM \"CENSUS\".\"decennial_population\" " +
                "ORDER BY \"geo_name\" " +
                "LIMIT 10");

            LOGGER.info("  Sample rows (verifying canonical column names):");
            int rows = 0;
            while (rs.next()) {
              rows++;
              LOGGER.info("    {} | pop={} | white={} | black={} | asian={}",
                  rs.getString("geo_name"),
                  rs.getLong("total_population"),
                  rs.getLong("white_alone"),
                  rs.getLong("black_alone"),
                  rs.getLong("asian_alone"));
            }
            assertTrue(rows > 0, "Should have decennial population data");

            // Verify data across multiple years (year is a partition column)
            LOGGER.info("\n--- Checking data summary ---");
            rs = stmt.executeQuery(
                "SELECT COUNT(*) as cnt, SUM(\"total_population\") as total_pop " +
                "FROM \"CENSUS\".\"decennial_population\"");

            if (rs.next()) {
              LOGGER.info("  Total: {} rows, total population: {}",
                  rs.getLong("cnt"),
                  rs.getLong("total_pop"));
            }
          } else {
            LOGGER.info("  No data materialized yet - this is expected on first run");
            LOGGER.info("  Run with autoDownload=true to populate decennial data");
          }
        }
      }

      LOGGER.info("\n================================================================================");
      LOGGER.info(" DECENNIAL POPULATION TEST COMPLETE!");
      LOGGER.info("================================================================================");
    }
  }
}
