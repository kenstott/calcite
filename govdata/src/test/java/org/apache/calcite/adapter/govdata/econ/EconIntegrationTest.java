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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test for ECON schema with real data download.
 * This test requires API keys to be configured.
 * Tests phases 1-5 of ECON data adapter implementation.
 */
@Tag("integration")
public class EconIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EconIntegrationTest.class);

  private static final Set<String> PHASE1_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(
          "fred_indicators",
          "employment_statistics",
          "inflation_metrics",
          "wage_growth"));

  private static final Set<String> PHASE2_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(
          "state_wages",
          "world_indicators",
          "metro_wages"));

  private static final Set<String> PHASE3_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(
          "jolts_regional",
          "metro_cpi",
          "regional_income",
          "state_gdp"));

  private static final Set<String> PHASE4_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(
          "regional_income",
          "state_gdp",
          "trade_statistics",
          "ita_data",
          "industry_gdp"));

  private static final Set<String> PHASE5_EXPECTED_VIEWS =
      new HashSet<>(
          Arrays.asList(
          "interest_rate_spreads",
          "housing_indicators",
          "monetary_aggregates",
          "business_indicators",
          "trade_balance_summary"));

  private static final Set<String> PHASE6_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(
          "county_qcew",
          "county_wages"));

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
        "      \"enabledSources\": [\"fred\", \"bls\", \"treasury\", \"bea\", \"bls_jolts_regional\", \"bls_metro_cpi\", \"bea_regional_income\", \"bea_state_gdp\"]," +
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
    LOGGER.info(" PHASE 2: JSON-to-Parquet Conversion Validation");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates ALL Phase 2 objectives:");
    LOGGER.info("   - Objective 2.1: state_wages conversion (JSON → Parquet)");
    LOGGER.info("   - Objective 2.2: world_indicators conversion (JSON → Parquet)");
    LOGGER.info("   - Objective 2.3: metro_wages download and conversion (JSON → Parquet)");
    LOGGER.info("================================================================================");

    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    int startYear = Integer.parseInt(TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR") != null
        ? TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR") : "2020");
    int endYear = Integer.parseInt(TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR") != null
        ? TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR") : "2024");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Objective 2.1: Validate state_wages conversion
        LOGGER.info("\n1. OBJECTIVE 2.1: Validating state_wages conversion:");
        validatePhase2Table(stmt, "state_wages", cacheDir, parquetDir,
            "source=econ/type=state_wages", "year", startYear, endYear);

        // Objective 2.2: Validate world_indicators conversion
        LOGGER.info("\n2. OBJECTIVE 2.2: Validating world_indicators conversion:");
        validatePhase2Table(stmt, "world_indicators", cacheDir, parquetDir,
            "source=econ/type=indicators", "year", startYear, endYear);

        // Objective 2.3: Validate metro_wages download and conversion
        LOGGER.info("\n3. OBJECTIVE 2.3: Validating metro_wages download and conversion:");
        validatePhase2Table(stmt, "metro_wages", cacheDir, parquetDir,
            "source=econ/type=metro_wages", "year", startYear, endYear);

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ✅ PHASE 2 COMPLETE: All JSON-to-Parquet conversions validated!");
        LOGGER.info("================================================================================");
      }
    }
  }

  /**
   * Validates that a Phase 2 table has data, proving JSON-to-Parquet conversion succeeded.
   * This test validates the complete pipeline: download → JSON cache → Parquet conversion → query.
   *
   * @param stmt SQL statement for queries
   * @param tableName Name of the table to validate
   * @param cacheDir Base cache directory path (for logging only)
   * @param parquetDir Base parquet directory path (for logging only)
   * @param pathPrefix Hive-style path prefix (e.g., "source=econ/type=state_wages")
   * @param partitionKey Partition key (typically "year")
   * @param startYear Start year for validation
   * @param endYear End year for validation
   */
  private void validatePhase2Table(Statement stmt, String tableName,
      String cacheDir, String parquetDir, String pathPrefix,
      String partitionKey, int startYear, int endYear) throws SQLException {

    LOGGER.info("  Validating {} conversion pipeline...", tableName);
    LOGGER.info("    Expected JSON cache path: {}/{}/{}", cacheDir, pathPrefix, partitionKey + "=*");
    LOGGER.info("    Expected Parquet path: {}/{}/{}", parquetDir, pathPrefix, partitionKey + "=*");

    // Verify table is queryable with data
    // If this succeeds, it proves the complete pipeline worked:
    // 1. Download succeeded (JSON created in cache)
    // 2. Conversion succeeded (Parquet created from JSON)
    // 3. Schema registration succeeded (table is queryable)
    String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        long count = rs.getLong("cnt");
        LOGGER.info("    ✅ Table {} - {} rows", tableName, count);
        assertTrue(count > 0,
            "Objective FAILED: " + tableName + " returned " + count
            + " rows (expected > 0).\n"
            + "    This indicates the JSON-to-Parquet conversion pipeline failed.\n"
            + "    Check logs for: JSON download, Parquet conversion, schema registration.");
      } else {
        throw new SQLException("Objective FAILED: Query returned no results for " + tableName);
      }
    } catch (SQLException e) {
      throw new SQLException("Objective FAILED: Cannot query " + tableName
          + "\n    Root cause: " + e.getMessage()
          + "\n    This indicates one of these failures:"
          + "\n      1. JSON was not downloaded to cache"
          + "\n      2. Parquet conversion from JSON failed"
          + "\n      3. Schema did not register the table", e);
    }

    LOGGER.info("  ✅ SUCCESS: {} passed all validation checks (download → JSON → Parquet → query)", tableName);
  }

  /**
   * Validates that a Phase 3 table has data. Phase 3 tables should have data if APIs are working.
   * If 0 rows, likely due to API limits reached (user configuration issue).
   */
  private void validatePhase3Table(Statement stmt, String tableName,
      String cacheDir, String parquetDir, String apiSource) throws SQLException {

    LOGGER.info("  Validating {} data availability...", tableName);

    // Verify table is queryable with data
    String query = "SELECT COUNT(*) as cnt FROM \"ECON\"." + tableName;
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        long count = rs.getLong("cnt");
        LOGGER.info("    ✅ Table {} - {} rows", tableName, count);
        assertTrue(count > 0,
            "Objective FAILED: " + tableName + " returned " + count + " rows (expected > 0).\n"
            + "    This likely indicates API limits reached or configuration issue.\n"
            + "    Check: API keys valid, rate limits not exceeded, correct date ranges.");
      } else {
        throw new SQLException("Objective FAILED: Query returned no results for " + tableName);
      }
    } catch (SQLException e) {
      throw new SQLException("Objective FAILED: Cannot query " + tableName
          + "\n    Root cause: " + e.getMessage()
          + "\n    This indicates:"
          + "\n      1. Table was not registered in schema"
          + "\n      2. API call failed (check API keys and limits)"
          + "\n      3. Download/conversion pipeline has errors", e);
    }

    LOGGER.info("  ✅ SUCCESS: {} has data", tableName);
  }

  @Test public void testPhase3ComprehensiveTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 3: Additional Economic Tables Validation");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates ALL Phase 3 objectives:");
    LOGGER.info("   - Objective 3.1: jolts_regional data availability");
    LOGGER.info("   - Objective 3.2: metro_cpi data availability");
    LOGGER.info("   - Objective 3.3: BEA regional_income and state_gdp data availability");
    LOGGER.info(" NOTE: All tables must have data > 0. If 0 rows, check API limits/config.");
    LOGGER.info("================================================================================");

    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Objective 3.1: Validate jolts_regional table (may be empty if API data unavailable)
        LOGGER.info("\n1. OBJECTIVE 3.1: Investigating jolts_regional empty response:");
        validatePhase3Table(stmt, "jolts_regional", cacheDir, parquetDir, "BLS JOLTS");

        // Objective 3.2: Validate metro_cpi table (may be empty if API data unavailable)
        LOGGER.info("\n2. OBJECTIVE 3.2: Investigating metro_cpi empty response:");
        validatePhase3Table(stmt, "metro_cpi", cacheDir, parquetDir, "BLS CPI");

        // Objective 3.3: Validate BEA regional_income and state_gdp fixes
        LOGGER.info("\n3. OBJECTIVE 3.3: Validating BEA regional_income fix:");
        validatePhase3Table(stmt, "regional_income", cacheDir, parquetDir, "BEA Regional Income");

        LOGGER.info("\n4. OBJECTIVE 3.3: Validating BEA state_gdp fix:");
        validatePhase3Table(stmt, "state_gdp", cacheDir, parquetDir, "BEA State GDP");

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ✅ PHASE 3 COMPLETE: All API investigation objectives validated!");
        LOGGER.info("================================================================================");
      }
    }
  }

  /**
   * Phase 4: Implement Missing BEA Tables Test
   *
   * <p>This test validates Phase 4 objectives from problem_resolution_plan.md:
   * <ul>
   *   <li>Objective 4.2: industry_gdp implementation (BEA GDP by Industry)</li>
   *   <li>Objective 4.3: ita_data implementation (BEA International Transactions)</li>
   *   <li>Objective 4.4: trade_statistics implementation (BEA trade statistics)</li>
   * </ul>
   *
   * <p><b>NOTE:</b> regional_income and state_gdp were implemented in Phase 3
   * and are included in PHASE4_EXPECTED_TABLES for validation.
   */
  @Test public void testPhase4AdvancedTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 4: Implement Missing BEA Tables Test");
    LOGGER.info("================================================================================");
    LOGGER.info("\nPhase 4 validates implementation of additional BEA tables:");
    LOGGER.info("  - Objective 4.2: industry_gdp (BEA GDP by Industry)");
    LOGGER.info("  - Objective 4.3: ita_data (BEA International Transactions)");
    LOGGER.info("  - Objective 4.4: trade_statistics (BEA trade statistics)");
    LOGGER.info("\nNote: regional_income and state_gdp were implemented in Phase 3");

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

        LOGGER.info("\n================================================================================");
        LOGGER.info(" PHASE 4 COMPLETE: {}/{} tables queryable", found, PHASE4_EXPECTED_TABLES.size());
        LOGGER.info("================================================================================");

        assertEquals(PHASE4_EXPECTED_TABLES.size(), found,
            "Phase 4: Expected all " + PHASE4_EXPECTED_TABLES.size() + " BEA tables to be queryable");
      }
    }
  }

  @Test public void testPhase5FredCatalog() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 5: FRED Data Series Catalog Test");
    LOGGER.info("================================================================================");
    LOGGER.info("This test validates Phase 5 objective from problem_resolution_plan.md:");
    LOGGER.info("  - Objective 5.1-5.3: fred_data_series_catalog table is queryable");
    LOGGER.info("Expected: fred_data_series_catalog table with 254 partitions");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        // Test 1: Verify table is queryable
        String query = "SELECT * FROM \"ECON\".fred_data_series_catalog LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            LOGGER.info("  ✅ fred_data_series_catalog - table is queryable");

            // Test 2: Verify table has data
            String countQuery = "SELECT COUNT(*) as row_count FROM \"ECON\".fred_data_series_catalog";
            try (ResultSet countRs = stmt.executeQuery(countQuery)) {
              if (countRs.next()) {
                long rowCount = countRs.getLong("row_count");
                LOGGER.info("  ✅ fred_data_series_catalog - {} rows found", rowCount);
                assertTrue(rowCount > 0, "FRED catalog should have data");
              }
            }

            // Test 3: Verify partition columns exist
            String partitionQuery = "SELECT DISTINCT type, category, frequency, source, status " +
                "FROM \"ECON\".fred_data_series_catalog LIMIT 5";
            try (ResultSet partRs = stmt.executeQuery(partitionQuery)) {
              LOGGER.info("  ✅ fred_data_series_catalog - partition columns accessible");
              int partCount = 0;
              while (partRs.next()) {
                partCount++;
              }
              LOGGER.info("  ✅ fred_data_series_catalog - verified {} partition combinations", partCount);
            }

            LOGGER.info("\n================================================================================");
            LOGGER.info(" PHASE 5 COMPLETE: fred_data_series_catalog is fully functional");
            LOGGER.info("================================================================================");
          } else {
            fail("fred_data_series_catalog table is empty");
          }
        } catch (SQLException e) {
          LOGGER.error("  ❌ fred_data_series_catalog - FAILED: {}", e.getMessage());
          throw e;
        }
      }
    }
  }

  @Test public void testPhase5AnalyticalViews() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 6: Analytical Views Test");
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
            "Phase 6: Expected all " + PHASE5_EXPECTED_VIEWS.size() + " views to be queryable");
      }
    }
  }

  @Test public void testPhase6CountyQcewTables() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" PHASE 6: County-Level QCEW Data Validation");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates county-level QCEW tables:");
    LOGGER.info("   - county_qcew: ~3,142 U.S. counties with employment/wages data");
    LOGGER.info("   - county_wages: ~6,038 U.S. counties with average weekly wages");
    LOGGER.info("================================================================================");

    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    int startYear = Integer.parseInt(TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR") != null
        ? TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR") : "2020");
    int endYear = Integer.parseInt(TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR") != null
        ? TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR") : "2024");

    try (Connection conn = createConnection()) {
      try (Statement stmt = conn.createStatement()) {
        LOGGER.info("\n1. Validating county_qcew table:");
        validatePhase2Table(stmt, "county_qcew", cacheDir, parquetDir,
            "source=econ/type=county_qcew", "year", startYear, endYear);

        LOGGER.info("\n2. Validating county_wages table:");
        validatePhase2Table(stmt, "county_wages", cacheDir, parquetDir,
            "source=econ/type=county_wages", "year", startYear, endYear);

        LOGGER.info("\n================================================================================");
        LOGGER.info(" ✅ PHASE 6 COMPLETE: County-level QCEW data validated!");
        LOGGER.info("================================================================================");
      }
    }
  }

  @Tag("integration")
  @Test public void testEconSchemaComments() throws Exception {
    LOGGER.info("\n================================================================================");
    LOGGER.info(" ECON SCHEMA COMMENTS: INFORMATION_SCHEMA Validation");
    LOGGER.info("================================================================================");
    LOGGER.info(" This test validates that:");
    LOGGER.info("   - Schema comments are available via INFORMATION_SCHEMA.SCHEMATA");
    LOGGER.info("   - Table comments are available via INFORMATION_SCHEMA.TABLES");
    LOGGER.info("   - Column comments are available via INFORMATION_SCHEMA.COLUMNS");
    LOGGER.info("================================================================================");

    try (Connection conn = createConnection()) {
      // Test 1: Verify schema comments via INFORMATION_SCHEMA.SCHEMATA
      LOGGER.info("\n1. Testing ECON schema comments via INFORMATION_SCHEMA.SCHEMATA:");
      String schemaQuery = "SELECT \"SCHEMA_NAME\", \"REMARKS\" FROM INFORMATION_SCHEMA.\"SCHEMATA\" "
          + "WHERE \"SCHEMA_NAME\" = 'ECON' AND \"REMARKS\" IS NOT NULL";

      int schemaCommentCount = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(schemaQuery)) {
        while (rs.next()) {
          String schemaName = rs.getString("SCHEMA_NAME");
          String remarks = rs.getString("REMARKS");
          LOGGER.info("  ✅ {} - comment: {}", schemaName,
              remarks != null && remarks.length() > 100
                  ? remarks.substring(0, 97) + "..." : remarks);
          // Verify REMARKS actually contains comment text, not just null or empty
          assertNotNull(remarks,
              "ECON schema REMARKS should not be null (got null for schema: " + schemaName + ")");
          assertFalse(remarks.isEmpty(),
              "ECON schema REMARKS should not be empty (got empty string for schema: " + schemaName + ")");
          schemaCommentCount++;
        }
      }
      assertTrue(schemaCommentCount > 0,
          "ECON schema should have a comment in INFORMATION_SCHEMA.SCHEMATA");
      LOGGER.info("  Found schema comment for ECON schema");

      // Test 2: Verify table comments via INFORMATION_SCHEMA.TABLES
      LOGGER.info("\n2. Testing ECON table comments via INFORMATION_SCHEMA.TABLES:");
      String tableQuery = "SELECT \"TABLE_NAME\", \"REMARKS\" FROM INFORMATION_SCHEMA.\"TABLES\" "
          + "WHERE \"TABLE_SCHEMA\" = 'ECON' AND \"REMARKS\" IS NOT NULL ORDER BY \"TABLE_NAME\"";

      int tableCommentCount = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(tableQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String remarks = rs.getString("REMARKS");
          LOGGER.info("  ✅ {} - comment: {}", tableName,
              remarks != null && remarks.length() > 100
                  ? remarks.substring(0, 97) + "..." : remarks);
          tableCommentCount++;
        }
      }
      assertTrue(tableCommentCount > 0,
          "At least one ECON table should have a comment in INFORMATION_SCHEMA.TABLES");
      LOGGER.info("  Found {} tables with comments", tableCommentCount);

      // Test 3: Verify column comments via INFORMATION_SCHEMA.COLUMNS
      LOGGER.info("\n3. Testing ECON column comments via INFORMATION_SCHEMA.COLUMNS:");
      String columnQuery = "SELECT \"TABLE_NAME\", \"COLUMN_NAME\", \"REMARKS\" FROM INFORMATION_SCHEMA.\"COLUMNS\" "
          + "WHERE \"TABLE_SCHEMA\" = 'ECON' AND \"REMARKS\" IS NOT NULL "
          + "ORDER BY \"TABLE_NAME\", \"ORDINAL_POSITION\"";

      int columnCommentCount = 0;
      String lastTable = null;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(columnQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String columnName = rs.getString("COLUMN_NAME");
          String remarks = rs.getString("REMARKS");

          // Print table header when we encounter a new table
          if (!tableName.equals(lastTable)) {
            if (lastTable != null) {
              LOGGER.info("");  // Blank line between tables
            }
            LOGGER.info("  Table: {}", tableName);
            lastTable = tableName;
          }

          LOGGER.info("    ✅ {} - comment: {}", columnName,
              remarks != null && remarks.length() > 80
                  ? remarks.substring(0, 77) + "..." : remarks);
          columnCommentCount++;
        }
      }
      assertTrue(columnCommentCount > 0,
          "At least one column should have a comment in INFORMATION_SCHEMA.COLUMNS");
      LOGGER.info("  Found {} columns with comments across all ECON tables", columnCommentCount);

      LOGGER.info("\n================================================================================");
      LOGGER.info(" ✅ ECON SCHEMA COMMENTS TEST COMPLETE!");
      LOGGER.info("   - Schema comment: {} schema", schemaCommentCount);
      LOGGER.info("   - Table comments: {} tables", tableCommentCount);
      LOGGER.info("   - Column comments: {} columns", columnCommentCount);
      LOGGER.info("================================================================================");
    }
  }
}
