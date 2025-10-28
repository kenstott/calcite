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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
 * Integration test for ECON schema with real data download.
 * This test requires API keys to be configured.
 * Tests phases 1-5 of ECON data adapter implementation.
 */
@Tag("integration")
public class EconIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EconIntegrationTest.class);

  private static final Set<String> PHASE1_EXPECTED_TABLES =
      new HashSet<>(Arrays.asList(
          "fred_indicators",
          "employment_statistics",
          "inflation_metrics",
          "wage_growth"
      ));

  private static final Set<String> PHASE2_EXPECTED_TABLES =
      new HashSet<>(Arrays.asList(
          "regional_employment",
          "treasury_yields",
          "federal_debt"
      ));

  private static final Set<String> PHASE3_EXPECTED_TABLES =
      new HashSet<>(Arrays.asList(
          "world_indicators",
          "gdp_components",
          "gdp_statistics"
      ));

  private static final Set<String> PHASE4_EXPECTED_TABLES =
      new HashSet<>(Arrays.asList(
          "regional_income",
          "state_gdp",
          "trade_statistics",
          "ita_data",
          "industry_gdp"
      ));

  private static final Set<String> PHASE5_EXPECTED_VIEWS =
      new HashSet<>(Arrays.asList(
          "interest_rate_spreads",
          "housing_indicators",
          "monetary_aggregates",
          "business_indicators",
          "trade_balance_summary"
      ));

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();

    // Verify environment is properly configured
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR"),
        "GOVDATA_CACHE_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR"),
        "GOVDATA_PARQUET_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("BLS_API_KEY"),
        "BLS_API_KEY must be set for integration tests");
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
      endYear = "2024";
    }

    String fredApiKey = TestEnvironmentLoader.getEnv("FRED_API_KEY");
    String blsApiKey = TestEnvironmentLoader.getEnv("BLS_API_KEY");
    String beaApiKey = TestEnvironmentLoader.getEnv("BEA_API_KEY");

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
        "  \"defaultSchema\": \"ECON\"," +
        "  \"schemas\": [{" +
        "    \"name\": \"ECON\"," +
        "    \"type\": \"custom\"," +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "    \"operand\": {" +
        "      \"dataSource\": \"econ\"," +
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
        "      \"enabledSources\": [\"fred\", \"bls\", \"treasury\", \"bea\"]," +
        "      \"fredApiKey\": \"" + fredApiKey + "\"," +
        "      \"blsApiKey\": \"" + blsApiKey + "\"," +
        "      \"beaApiKey\": \"" + beaApiKey + "\"," +
        "      \"customFredSeries\": [" +
        "        \"DGS10\", \"DGS2\", \"DGS30\", \"UNRATE\", \"PAYEMS\", \"CPIAUCSL\", \"GDPC1\"" +
        "      ]," +
        "      \"fredSeriesGroups\": {" +
        "        \"treasury_rates\": {" +
        "          \"tableName\": \"fred_treasury_rates\"," +
        "          \"series\": [\"DGS1MO\", \"DGS3MO\", \"DGS6MO\", \"DGS1\", \"DGS2\", \"DGS3\", \"DGS5\", \"DGS7\", \"DGS10\", \"DGS20\", \"DGS30\"]," +
        "          \"partitionStrategy\": \"MANUAL\"," +
        "          \"partitionFields\": [\"year\", \"maturity\"]," +
        "          \"comment\": \"U.S. Treasury constant maturity rates for all standard maturities\"" +
        "        }," +
        "        \"employment_indicators\": {" +
        "          \"tableName\": \"fred_employment_indicators\"," +
        "          \"series\": [\"UNRATE\", \"PAYEMS\", \"CIVPART\", \"EMRATIO\", \"U6RATE\"]," +
        "          \"partitionStrategy\": \"MANUAL\"," +
        "          \"partitionFields\": [\"year\"]," +
        "          \"comment\": \"Employment and labor force participation indicators\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }]" +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test public void testPhase1BasicTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 1: Metadata Cache Bug Fix Validation");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates that the metadata cache bug is fixed for:");
    LOGGER.info("   - treasury_yields (expected ~204 rows, not 0)");
    LOGGER.info("   - federal_debt (expected ~250 rows, not 0)");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // First, test the basic expected tables
        LOGGER.info("\n1. Testing basic economic tables:");
        int found = 0;
        for (String tableName : PHASE1_EXPECTED_TABLES) {
          String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              LOGGER.info("  ✅ {} - {} rows", tableName, count);
              found++;
            }
          } catch (SQLException e) {
            LOGGER.error("  ❌ {} - FAILED: {}", tableName, e.getMessage());
          }
        }
        assertEquals(PHASE1_EXPECTED_TABLES.size(), found,
            "Phase 1: Expected all " + PHASE1_EXPECTED_TABLES.size() + " basic tables to be queryable");

        // Phase 1 CRITICAL TEST: Validate metadata cache bug fix for treasury_yields
        LOGGER.info("\n2. PHASE 1 CRITICAL: Testing treasury_yields metadata cache fix:");
        String treasuryQuery = "SELECT COUNT(*) as cnt FROM \"ECON\".treasury_yields";
        try (ResultSet rs = stmt.executeQuery(treasuryQuery)) {
          if (rs.next()) {
            long count = rs.getLong("cnt");
            LOGGER.info("  treasury_yields: {} rows", count);
            assertTrue(count > 0,
                "Phase 1 FAILED: treasury_yields returned " + count + " rows (expected ~204). "
                + "Metadata cache bug is NOT fixed!");
            LOGGER.info("  ✅ PHASE 1 SUCCESS: treasury_yields has {} rows (> 0) - metadata cache bug is FIXED", count);
          }
        }

        // Phase 1 CRITICAL TEST: Validate metadata cache bug fix for federal_debt
        LOGGER.info("\n3. PHASE 1 CRITICAL: Testing federal_debt metadata cache fix:");
        String debtQuery = "SELECT COUNT(*) as cnt FROM \"ECON\".federal_debt";
        try (ResultSet rs = stmt.executeQuery(debtQuery)) {
          if (rs.next()) {
            long count = rs.getLong("cnt");
            LOGGER.info("  federal_debt: {} rows", count);
            assertTrue(count > 0,
                "Phase 1 FAILED: federal_debt returned " + count + " rows (expected ~250). "
                + "Metadata cache bug is NOT fixed!");
            LOGGER.info("  ✅ PHASE 1 SUCCESS: federal_debt has {} rows (> 0) - metadata cache bug is FIXED", count);
          }
        }

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ✅ PHASE 1 COMPLETE: Metadata cache bug is FIXED!");
        LOGGER.info("================================================================================");
      }
    }
  }

  @Test public void testPhase2AdditionalTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 2: Additional Economic Tables Test");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        int found = 0;
        for (String tableName : PHASE2_EXPECTED_TABLES) {
          String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              LOGGER.info("  ✅ {} - {} rows", tableName, count);
              found++;
            }
          } catch (SQLException e) {
            LOGGER.error("  ❌ {} - FAILED: {}", tableName, e.getMessage());
          }
        }
        assertEquals(PHASE2_EXPECTED_TABLES.size(), found,
            "Phase 2: Expected all " + PHASE2_EXPECTED_TABLES.size() + " tables to be queryable");
      }
    }
  }

  @Test public void testPhase3ComprehensiveTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 3: Comprehensive Economic Tables Test");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        int found = 0;
        for (String tableName : PHASE3_EXPECTED_TABLES) {
          String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              LOGGER.info("  ✅ {} - {} rows", tableName, count);
              found++;
            }
          } catch (SQLException e) {
            LOGGER.error("  ❌ {} - FAILED: {}", tableName, e.getMessage());
          }
        }
        assertEquals(PHASE3_EXPECTED_TABLES.size(), found,
            "Phase 3: Expected all " + PHASE3_EXPECTED_TABLES.size() + " tables to be queryable");
      }
    }
  }

  @Test public void testPhase4AdvancedTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 4: Advanced Economic Tables Test");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        int found = 0;
        for (String tableName : PHASE4_EXPECTED_TABLES) {
          String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              LOGGER.info("  ✅ {} - {} rows", tableName, count);
              found++;
            }
          } catch (SQLException e) {
            LOGGER.error("  ❌ {} - FAILED: {}", tableName, e.getMessage());
          }
        }
        assertEquals(PHASE4_EXPECTED_TABLES.size(), found,
            "Phase 4: Expected all " + PHASE4_EXPECTED_TABLES.size() + " tables to be queryable");
      }
    }
  }

  @Test public void testPhase5AnalyticalViews() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 5: Analytical Views Test");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        int found = 0;
        for (String viewName : PHASE5_EXPECTED_VIEWS) {
          String query = "SELECT * FROM \"ECON\"." + viewName + " LIMIT 1";
          try (ResultSet rs = stmt.executeQuery(query)) {
            LOGGER.info("  ✅ {} - queryable", viewName);
            found++;
          } catch (SQLException e) {
            LOGGER.error("  ❌ {} - FAILED: {}", viewName, e.getMessage());
          }
        }
        assertEquals(PHASE5_EXPECTED_VIEWS.size(), found,
            "Phase 5: Expected all " + PHASE5_EXPECTED_VIEWS.size() + " views to be queryable");
      }
    }
  }
}
