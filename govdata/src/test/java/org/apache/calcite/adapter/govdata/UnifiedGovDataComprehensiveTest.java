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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive test for all four government data sources (SEC, ECON, GEO, CENSUS).
 * Tests with 5 years of data (2021-2025) for all 30 DJIA companies.
 * Validates all tables are discoverable and queryable across all four schemas.
 */
@Tag("integration")
public class UnifiedGovDataComprehensiveTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedGovDataComprehensiveTest.class);

  /**
   * SEC schema expected tables (9 total).
   */
  private static final Set<String> SEC_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList("financial_line_items",
      "filing_metadata",
      "filing_contexts",
      "mda_sections",
      "xbrl_relationships", // Enhanced to support inline XBRL relationship extraction
      "insider_transactions",
      "earnings_transcripts",
      "stock_prices",
      "vectorized_blobs", // Now enabled with textSimilarity configuration
      "company_info"));

  /**
   * ECON schema expected tables (18 total).
   */
  private static final Set<String> ECON_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList("employment_statistics",
      "inflation_metrics",
      "wage_growth",
      "regional_employment",
      "treasury_yields",
      "federal_debt",
      "world_indicators",
      "fred_indicators",
      "gdp_components",
      "gdp_statistics",
      "regional_income",
      "state_gdp",
      "trade_statistics",
      "ita_data",
      "industry_gdp",
      // FRED catalog table
      "reference_fred_series"));

  /**
   * GEO schema expected tables (15 total).
   */
  private static final Set<String> GEO_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList("states",
      "counties",
      "places",
      "zctas",
      "census_tracts",
      "block_groups",
      "cbsa",
      "congressional_districts",
      "school_districts",
      "population_demographics",
      "housing_characteristics",
      "economic_indicators",
      "zip_county_crosswalk",
      "zip_cbsa_crosswalk",
      "tract_zip_crosswalk"));

  /**
   * CENSUS schema expected tables (21 total).
   */
  private static final Set<String> CENSUS_EXPECTED_TABLES =
      new HashSet<>(
          Arrays.asList(// ACS tables (15)
      "acs_population", "acs_demographics", "acs_income", "acs_poverty",
      "acs_employment", "acs_education", "acs_housing", "acs_housing_costs",
      "acs_commuting", "acs_health_insurance", "acs_language", "acs_disability",
      "acs_veterans", "acs_migration", "acs_occupation",
      // Decennial tables (3)
      "decennial_population", "decennial_demographics", "decennial_housing",
      // Economic tables (2)
      "economic_census", "county_business_patterns",
      // Population estimates (1)
      "population_estimates"));

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test public void testAllThreeDataSourcesComprehensive() throws Exception {
    // Create comprehensive model with all four schemas
    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"sec\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"sec\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"sec\"," +
        "        \"executionEngine\": \"DUCKDB\"," +
        "        \"ciks\": [\"DJIA\"]," +  // Dow Jones Industrial Average (DJIA)
        "        \"startYear\": 2021," +
        "        \"endYear\": 2025," +
        "        \"autoDownload\": true," +
        "        \"fetchStockPrices\": true," +
        "        \"textSimilarity\": {" +
        "          \"enabled\": true," +
        "          \"embeddingModel\": \"text-embedding-ada-002\"" +
        "        }" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"econ\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"econ\"," +
        "        \"executionEngine\": \"DUCKDB\"," +
        "        \"autoDownload\": true" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"geo\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"geo\"," +
        "        \"executionEngine\": \"DUCKDB\"," +
        "        \"autoDownload\": true" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"census\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"census\"," +
        "        \"executionEngine\": \"DUCKDB\"," +
        "        \"autoDownload\": true," +
        "        \"startYear\": 2019," +
        "        \"endYear\": 2023" +
        "      }" +
        "    }" +
        "  ]" +
        "}";

    Path modelFile = Files.createTempFile("unified-govdata-test", ".json");
    Files.write(modelFile, modelJson.getBytes(StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
      LOGGER.info("\n{}", createRepeatedString("=", 80));
      LOGGER.info(" UNIFIED GOVERNMENT DATA COMPREHENSIVE TEST");
      LOGGER.info(" Testing SEC (MSFT + AAPL, 2021-2025), ECON, GEO, and CENSUS schemas");
      LOGGER.info("{}", "=".repeat(80));

      Map<String, TestResult> results = new HashMap<>();

      // Test SEC schema
      results.put("SEC", validateSchema(conn, "sec", SEC_EXPECTED_TABLES));

      // Test ECON schema
      results.put("ECON", validateSchema(conn, "econ", ECON_EXPECTED_TABLES));

      // Test GEO schema
      results.put("GEO", validateSchema(conn, "geo", GEO_EXPECTED_TABLES));

      // Test CENSUS schema
      results.put("CENSUS", validateSchema(conn, "census", CENSUS_EXPECTED_TABLES));

      // Test cross-schema queries
      boolean crossSchemaSuccess = testCrossSchemaQueries(conn);

      // Print comprehensive summary
      printComprehensiveSummary(results, crossSchemaSuccess);

      // Determine overall test result
      boolean overallSuccess = true;
      for (TestResult result : results.values()) {
        if (!result.isFullySuccessful()) {
          overallSuccess = false;
        }
      }

      if (!overallSuccess) {
        fail("Comprehensive test failed. See summary above for details.");
      } else {
        LOGGER.info("\n✅ ALL TESTS PASSED - All {} tables across SEC, ECON, GEO, and CENSUS are fully functional!",
            SEC_EXPECTED_TABLES.size() + ECON_EXPECTED_TABLES.size() + GEO_EXPECTED_TABLES.size() + CENSUS_EXPECTED_TABLES.size());
      }
    }
  }

  private TestResult validateSchema(Connection conn, String schemaName, Set<String> expectedTables)
      throws SQLException {
    LOGGER.info("\n{}", "=".repeat(60));
    LOGGER.info(" VALIDATING {} SCHEMA", schemaName.toUpperCase());
    LOGGER.info("{}", "=".repeat(60));

    TestResult result = new TestResult(schemaName);

    try (Statement stmt = conn.createStatement()) {
      // Discover tables
      List<String> discoveredTables = discoverTables(stmt, schemaName);
      result.discoveredTables = new HashSet<>(discoveredTables);

      LOGGER.info("\n📊 Table Discovery:");
      LOGGER.info("  Expected: {} tables", expectedTables.size());
      LOGGER.info("  Discovered: {} tables", discoveredTables.size());

      // Test each table
      LOGGER.info("\n🔍 Testing Table Queries:");
      for (String tableName : discoveredTables) {
        boolean querySuccess = testTable(stmt, schemaName, tableName, result);
        if (querySuccess) {
          result.queryableTables.add(tableName);
        } else {
          result.failedTables.add(tableName);
        }
      }

      // Special validation for SEC schema
      if ("sec".equals(schemaName)) {
        validateSecSpecificData(stmt, result);
      }

      // Special validation for ECON schema with FRED partitioning
      if ("econ".equals(schemaName)) {
        validateEconFredPartitioning(stmt, result);
      }

      // Calculate missing tables
      result.missingTables = new HashSet<>(expectedTables);
      result.missingTables.removeAll(result.discoveredTables);

      // Print schema-specific summary
      printSchemaResult(result, expectedTables);
    }

    return result;
  }

  private List<String> discoverTables(Statement stmt, String schemaName) throws SQLException {
    List<String> tables = new ArrayList<>();
    String query = "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
                   "WHERE \"TABLE_SCHEMA\" = '" + schemaName + "' " +
                   "ORDER BY \"TABLE_NAME\"";

    try (ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME"));
      }
    }

    return tables;
  }

  private boolean testTable(Statement stmt, String schemaName, String tableName, TestResult result) {
    try {
      // Test COUNT query
      String countQuery = "SELECT COUNT(*) as cnt FROM " + schemaName + "." + tableName;
      long rowCount = 0;

      try (ResultSet rs = stmt.executeQuery(countQuery)) {
        if (rs.next()) {
          rowCount = rs.getLong("cnt");
        }
      }

      // Test SELECT with LIMIT
      String selectQuery = "SELECT * FROM " + schemaName + "." + tableName + " LIMIT 5";
      int sampleRows = 0;

      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          sampleRows++;
        }
      }

      result.tableCounts.put(tableName, rowCount);

      String status = rowCount > 0 ? "✅" : "⚠️";
      LOGGER.info("  {} {}.{} - {} rows (sampled {})",
                        status, schemaName, tableName, rowCount, sampleRows);

      return true;
    } catch (SQLException e) {
      LOGGER.error("  ❌ {}.{} - FAILED: {}",
                        schemaName, tableName, e.getMessage());
      return false;
    }
  }

  private void validateSecSpecificData(Statement stmt, TestResult result) throws SQLException {
    LOGGER.info("\n🏢 SEC-Specific Validation (MSFT & AAPL):");

    // Check for Microsoft and Apple data in financial_line_items
    if (result.queryableTables.contains("financial_line_items")) {
      String query =
          "SELECT cik, COUNT(*) as filing_count " +
          "FROM sec.financial_line_items " +
          "WHERE cik IN ('0000789019', '0000320193') " +
          "GROUP BY cik";

      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String cik = rs.getString("cik");
          long count = rs.getLong("filing_count");
          String company = cik.equals("0000789019") ? "Microsoft" : "Apple";
          LOGGER.info("  ✅ {} (CIK {}): {} line items found", company, cik, count);
        }
      }
    }

    // Check year range in filing_metadata
    if (result.queryableTables.contains("filing_metadata")) {
      String query =
          "SELECT MIN(CAST(SUBSTRING(filing_date, 1, 4) AS INTEGER)) as min_year, " +
          "MAX(CAST(SUBSTRING(filing_date, 1, 4) AS INTEGER)) as max_year " +
          "FROM sec.filing_metadata " +
          "WHERE cik IN ('0000789019', '0000320193')";

      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          int minYear = rs.getInt("min_year");
          int maxYear = rs.getInt("max_year");
          LOGGER.info("  ✅ Filing year range: {} - {}", minYear, maxYear);
        }
      }
    }
  }

  private void validateEconFredPartitioning(Statement stmt, TestResult result) throws SQLException {
    LOGGER.info("\n📊 ECON FRED Custom Series Validation:");

    // Validate that partitioning is working by checking for expected FRED series
    if (result.queryableTables.contains("fred_indicators")) {
      try {
        String query = "SELECT series_id, COUNT(*) as obs_count " +
                      "FROM econ.fred_indicators " +
                      "WHERE series_id IN ('UNRATE', 'PAYEMS', 'DGS10', 'DGS30') " +
                      "GROUP BY series_id " +
                      "ORDER BY series_id";

        LOGGER.info("  📈 Custom FRED Series Data:");
        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            String seriesId = rs.getString("series_id");
            long obsCount = rs.getLong("obs_count");
            String description = getSeriesDescription(seriesId);
            LOGGER.info("    ✅ {} ({}): {} observations", seriesId, description, obsCount);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Custom FRED series validation failed: {}", e.getMessage());
      }
    }
  }

  private String getSeriesDescription(String seriesId) {
    switch (seriesId) {
      case "UNRATE": return "Unemployment Rate";
      case "PAYEMS": return "Total Nonfarm Payrolls";
      case "DGS10": return "10-Year Treasury Rate";
      case "DGS30": return "30-Year Treasury Rate";
      case "DGS2": return "2-Year Treasury Rate";
      case "CIVPART": return "Labor Force Participation Rate";
      default: return "Economic Indicator";
    }
  }

  private boolean testCrossSchemaQueries(Connection conn) {
    LOGGER.info("\n{}", "=".repeat(60));
    LOGGER.info(" CROSS-SCHEMA QUERY TESTS");
    LOGGER.info("{}", "=".repeat(60));

    boolean allSuccess = true;

    try (Statement stmt = conn.createStatement()) {
      // Test 1: Join SEC companies with state GDP data
      LOGGER.info("\n📈 Test 1: Companies with State Economic Data");
      String query1 =
          "SELECT COUNT(*) as cnt " +
          "FROM (SELECT DISTINCT cik FROM sec.filing_metadata WHERE cik IN ('0000789019', '0000320193')) s " +
          "CROSS JOIN (SELECT DISTINCT geo_fips FROM econ.state_gdp LIMIT 5) e";

      try (ResultSet rs = stmt.executeQuery(query1)) {
        if (rs.next()) {
          long count = rs.getLong("cnt");
          LOGGER.info("  ✅ Cross-join SEC companies with state GDP: {} combinations", count);
        }
      } catch (SQLException e) {
        LOGGER.error("  ❌ Failed: {}", e.getMessage());
        allSuccess = false;
      }

      // Test 2: Geographic and economic data combination
      LOGGER.info("\n🗺️ Test 2: Geographic Regions with Economic Indicators");
      String query2 =
          "SELECT COUNT(*) as cnt " +
          "FROM (SELECT state_fips FROM geo.states LIMIT 10) g " +
          "CROSS JOIN (SELECT DISTINCT line_description FROM econ.gdp_components LIMIT 3) e";

      try (ResultSet rs = stmt.executeQuery(query2)) {
        if (rs.next()) {
          long count = rs.getLong("cnt");
          LOGGER.info("  ✅ States with GDP components: {} combinations", count);
        }
      } catch (SQLException e) {
        LOGGER.error("  ❌ Failed: {}", e.getMessage());
        allSuccess = false;
      }

    } catch (SQLException e) {
      LOGGER.error("❌ Cross-schema query setup failed: {}", e.getMessage());
      allSuccess = false;
    }

    return allSuccess;
  }

  private void printSchemaResult(TestResult result, Set<String> expectedTables) {
    LOGGER.info("\n📊 {} Schema Summary:", result.schemaName.toUpperCase());
    LOGGER.info("  Total Expected: {}", expectedTables.size());
    LOGGER.info("  Total Discovered: {}", result.discoveredTables.size());
    LOGGER.info("  Successfully Queried: {}", result.queryableTables.size());
    LOGGER.info("  Failed Queries: {}", result.failedTables.size());

    if (!result.missingTables.isEmpty()) {
      LOGGER.warn("  ⚠️ Missing Tables: {}", result.missingTables);
    }

    double successRate = result.discoveredTables.isEmpty() ? 0 :
        (double) result.queryableTables.size() / result.discoveredTables.size() * 100;
    LOGGER.info("  Success Rate: {:.1f}%", successRate);
  }

  private void printComprehensiveSummary(Map<String, TestResult> results, boolean crossSchemaSuccess) {
    LOGGER.info("\n{}", createRepeatedString("=", 80));
    LOGGER.info(" COMPREHENSIVE TEST SUMMARY");
    LOGGER.info("{}", "=".repeat(80));

    int totalExpected = SEC_EXPECTED_TABLES.size() + ECON_EXPECTED_TABLES.size() +
                        GEO_EXPECTED_TABLES.size() + CENSUS_EXPECTED_TABLES.size();
    int totalDiscovered = 0;
    int totalQueryable = 0;
    int totalFailed = 0;

    for (TestResult result : results.values()) {
      totalDiscovered += result.discoveredTables.size();
      totalQueryable += result.queryableTables.size();
      totalFailed += result.failedTables.size();
    }

    LOGGER.info("\n📊 Overall Statistics:");
    LOGGER.info("  Total Expected Tables: {} (SEC: {}, ECON: {}, GEO: {}, CENSUS: {})",
                      totalExpected, SEC_EXPECTED_TABLES.size(),
                      ECON_EXPECTED_TABLES.size(), GEO_EXPECTED_TABLES.size(),
                      CENSUS_EXPECTED_TABLES.size());
    LOGGER.info("  Total Discovered: {}", totalDiscovered);
    LOGGER.info("  Total Queryable: {}", totalQueryable);
    LOGGER.info("  Total Failed: {}", totalFailed);

    LOGGER.info("\n🎯 Schema Results:");
    for (Map.Entry<String, TestResult> entry : results.entrySet()) {
      TestResult result = entry.getValue();
      String status = result.isFullySuccessful() ? "✅ PASS" : "❌ FAIL";
      LOGGER.info("  {} - {} (Discovered: {}/{}, Queryable: {}/{})",
                        entry.getKey(), status,
                        result.discoveredTables.size(), getExpectedCount(entry.getKey()),
                        result.queryableTables.size(), result.discoveredTables.size());
    }

    LOGGER.info("\n🔗 Cross-Schema Queries: {}", crossSchemaSuccess ? "✅ PASS" : "❌ FAIL");

    // Overall test result
    boolean allPassed = totalFailed == 0 && totalDiscovered == totalExpected && crossSchemaSuccess;
    LOGGER.info("\n{}", createRepeatedString("=", 80));
    if (allPassed) {
      LOGGER.info(" ✅ COMPREHENSIVE TEST: PASSED");
      LOGGER.info(" All {} tables across 4 schemas are fully functional!", totalExpected);
    } else {
      LOGGER.error(" ❌ COMPREHENSIVE TEST: FAILED");
      if (totalDiscovered < totalExpected) {
        LOGGER.error(" Missing {} tables from discovery", totalExpected - totalDiscovered);
      }
      if (totalFailed > 0) {
        LOGGER.error(" {} tables failed to query", totalFailed);
      }
      if (!crossSchemaSuccess) {
        LOGGER.error(" Cross-schema queries failed");
      }
    }
    LOGGER.info("{}", "=".repeat(80));
  }

  private int getExpectedCount(String schema) {
    switch (schema.toUpperCase()) {
      case "SEC": return SEC_EXPECTED_TABLES.size();
      case "ECON": return ECON_EXPECTED_TABLES.size();
      case "GEO": return GEO_EXPECTED_TABLES.size();
      case "CENSUS": return CENSUS_EXPECTED_TABLES.size();
      default: return 0;
    }
  }

  /**
   * Helper class to track test results for a schema.
   */
  private static class TestResult {
    final String schemaName;
    Set<String> discoveredTables = new HashSet<>();
    Set<String> queryableTables = new HashSet<>();
    Set<String> failedTables = new HashSet<>();
    Set<String> missingTables = new HashSet<>();
    Map<String, Long> tableCounts = new HashMap<>();

    TestResult(String schemaName) {
      this.schemaName = schemaName;
    }

    boolean isFullySuccessful() {
      return failedTables.isEmpty() && missingTables.isEmpty();
    }
  }

  private static String createRepeatedString(String str, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }
}
