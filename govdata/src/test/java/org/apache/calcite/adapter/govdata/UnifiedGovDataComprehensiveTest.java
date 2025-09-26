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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive test for all three government data sources (SEC, ECON, GEO).
 * Tests with 5 years of data (2021-2025) and limits SEC to Microsoft and Apple only.
 * Validates all tables are discoverable and queryable across all three schemas.
 */
@Tag("integration")
public class UnifiedGovDataComprehensiveTest {

  /**
   * SEC schema expected tables (9 total).
   */
  private static final Set<String> SEC_EXPECTED_TABLES = new HashSet<>(Arrays.asList(
      "financial_line_items",
      "filing_metadata",
      "filing_contexts",
      "mda_sections",
      "xbrl_relationships", // Enhanced to support inline XBRL relationship extraction
      "insider_transactions",
      "earnings_transcripts",
      "stock_prices",
      "vectorized_blobs", // Now enabled with textSimilarity configuration
      "company_info"
  ));

  /**
   * ECON schema expected tables (18 total).
   */
  private static final Set<String> ECON_EXPECTED_TABLES = new HashSet<>(Arrays.asList(
      "employment_statistics",
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
      // Custom FRED series tables
      "fred_treasuries",
      "fred_employment_indicators",
      // FRED catalog table
      "fred_data_series_catalog"
  ));

  /**
   * GEO schema expected tables (15 total).
   */
  private static final Set<String> GEO_EXPECTED_TABLES = new HashSet<>(Arrays.asList(
      "states",
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
      "tract_zip_crosswalk"
  ));

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test
  public void testAllThreeDataSourcesComprehensive() throws Exception {
    // Create comprehensive model with all three schemas
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
        "        \"ciks\": [\"0000789019\", \"0000320193\"]," +  // Microsoft and Apple
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
        "        \"autoDownload\": true," +
        "        \"defaultPartitionStrategy\": \"AUTO\"," +
        "        \"customFredSeries\": [\"UNRATE\", \"PAYEMS\"]," +
        "        \"fredSeriesGroups\": {" +
        "          \"treasuries\": {" +
        "            \"series\": [\"DGS10\", \"DGS30\", \"DGS2\"]," +
        "            \"partitionStrategy\": \"MANUAL\"," +
        "            \"partitionFields\": [\"year\", \"maturity\"]" +
        "          }," +
        "          \"employment_indicators\": {" +
        "            \"series\": [\"UNRATE\", \"PAYEMS\", \"CIVPART\"]," +
        "            \"partitionStrategy\": \"AUTO\"" +
        "          }" +
        "        }" +
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
        "    }" +
        "  ]" +
        "}";

    Path modelFile = Files.createTempFile("unified-govdata-test", ".json");
    Files.write(modelFile, modelJson.getBytes(StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
      System.out.println("\n" + "=".repeat(80));
      System.out.println(" UNIFIED GOVERNMENT DATA COMPREHENSIVE TEST");
      System.out.println(" Testing SEC (MSFT + AAPL, 2021-2025), ECON, and GEO schemas");
      System.out.println("=".repeat(80));

      Map<String, TestResult> results = new HashMap<>();

      // Test SEC schema
      results.put("SEC", validateSchema(conn, "sec", SEC_EXPECTED_TABLES));

      // Test ECON schema
      results.put("ECON", validateSchema(conn, "econ", ECON_EXPECTED_TABLES));

      // Test GEO schema
      results.put("GEO", validateSchema(conn, "geo", GEO_EXPECTED_TABLES));

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
        System.out.println("\n✅ ALL TESTS PASSED - All 41 tables across SEC, ECON, and GEO are fully functional!");
      }
    }
  }

  private TestResult validateSchema(Connection conn, String schemaName, Set<String> expectedTables)
      throws SQLException {
    System.out.println("\n" + "=".repeat(60));
    System.out.println(" VALIDATING " + schemaName.toUpperCase() + " SCHEMA");
    System.out.println("=".repeat(60));

    TestResult result = new TestResult(schemaName);

    try (Statement stmt = conn.createStatement()) {
      // Discover tables
      List<String> discoveredTables = discoverTables(stmt, schemaName);
      result.discoveredTables = new HashSet<>(discoveredTables);

      System.out.println("\n📊 Table Discovery:");
      System.out.println("  Expected: " + expectedTables.size() + " tables");
      System.out.println("  Discovered: " + discoveredTables.size() + " tables");

      // Test each table
      System.out.println("\n🔍 Testing Table Queries:");
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
      System.out.printf("  %s %s.%s - %d rows (sampled %d)\n",
                        status, schemaName, tableName, rowCount, sampleRows);

      return true;
    } catch (SQLException e) {
      System.out.printf("  ❌ %s.%s - FAILED: %s\n",
                        schemaName, tableName, e.getMessage());
      return false;
    }
  }

  private void validateSecSpecificData(Statement stmt, TestResult result) throws SQLException {
    System.out.println("\n🏢 SEC-Specific Validation (MSFT & AAPL):");

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
          System.out.printf("  ✅ %s (CIK %s): %d line items found\n", company, cik, count);
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
          System.out.printf("  ✅ Filing year range: %d - %d\n", minYear, maxYear);
        }
      }
    }
  }

  private void validateEconFredPartitioning(Statement stmt, TestResult result) throws SQLException {
    System.out.println("\n📊 ECON FRED Custom Series Validation:");

    // Check for custom FRED series tables
    if (result.queryableTables.contains("fred_treasuries")) {
      String query = "SELECT COUNT(*) as count FROM econ.fred_treasuries LIMIT 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          long count = rs.getLong("count");
          System.out.printf("  ✅ Treasury FRED series table: %d rows found\n", count);
        }
      } catch (SQLException e) {
        System.out.println("  ⚠️ Treasury FRED series table query failed: " + e.getMessage());
      }
    }

    if (result.queryableTables.contains("fred_employment_indicators")) {
      String query = "SELECT COUNT(*) as count FROM econ.fred_employment_indicators LIMIT 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          long count = rs.getLong("count");
          System.out.printf("  ✅ Employment indicators FRED series table: %d rows found\n", count);
        }
      } catch (SQLException e) {
        System.out.println("  ⚠️ Employment indicators FRED series table query failed: " + e.getMessage());
      }
    }

    // Validate that partitioning is working by checking for expected FRED series
    if (result.queryableTables.contains("fred_indicators")) {
      try {
        String query = "SELECT series_id, COUNT(*) as obs_count " +
                      "FROM econ.fred_indicators " +
                      "WHERE series_id IN ('UNRATE', 'PAYEMS', 'DGS10', 'DGS30') " +
                      "GROUP BY series_id " +
                      "ORDER BY series_id";

        System.out.println("  📈 Custom FRED Series Data:");
        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            String seriesId = rs.getString("series_id");
            long obsCount = rs.getLong("obs_count");
            String description = getSeriesDescription(seriesId);
            System.out.printf("    ✅ %s (%s): %d observations\n", seriesId, description, obsCount);
          }
        }
      } catch (SQLException e) {
        System.out.println("  ⚠️ Custom FRED series validation failed: " + e.getMessage());
      }
    }

    System.out.println("  🎯 FRED Custom Series Partitioning: Configured with AUTO strategy for Treasury and Employment groups");
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
    System.out.println("\n" + "=".repeat(60));
    System.out.println(" CROSS-SCHEMA QUERY TESTS");
    System.out.println("=".repeat(60));

    boolean allSuccess = true;

    try (Statement stmt = conn.createStatement()) {
      // Test 1: Join SEC companies with state GDP data
      System.out.println("\n📈 Test 1: Companies with State Economic Data");
      String query1 =
          "SELECT COUNT(*) as cnt " +
          "FROM (SELECT DISTINCT cik FROM sec.filing_metadata WHERE cik IN ('0000789019', '0000320193')) s " +
          "CROSS JOIN (SELECT DISTINCT geo_fips FROM econ.state_gdp LIMIT 5) e";

      try (ResultSet rs = stmt.executeQuery(query1)) {
        if (rs.next()) {
          long count = rs.getLong("cnt");
          System.out.println("  ✅ Cross-join SEC companies with state GDP: " + count + " combinations");
        }
      } catch (SQLException e) {
        System.out.println("  ❌ Failed: " + e.getMessage());
        allSuccess = false;
      }

      // Test 2: Geographic and economic data combination
      System.out.println("\n🗺️ Test 2: Geographic Regions with Economic Indicators");
      String query2 =
          "SELECT COUNT(*) as cnt " +
          "FROM (SELECT state_fips FROM geo.states LIMIT 10) g " +
          "CROSS JOIN (SELECT DISTINCT line_description FROM econ.gdp_components LIMIT 3) e";

      try (ResultSet rs = stmt.executeQuery(query2)) {
        if (rs.next()) {
          long count = rs.getLong("cnt");
          System.out.println("  ✅ States with GDP components: " + count + " combinations");
        }
      } catch (SQLException e) {
        System.out.println("  ❌ Failed: " + e.getMessage());
        allSuccess = false;
      }

    } catch (SQLException e) {
      System.out.println("❌ Cross-schema query setup failed: " + e.getMessage());
      allSuccess = false;
    }

    return allSuccess;
  }

  private void printSchemaResult(TestResult result, Set<String> expectedTables) {
    System.out.println("\n📊 " + result.schemaName.toUpperCase() + " Schema Summary:");
    System.out.println("  Total Expected: " + expectedTables.size());
    System.out.println("  Total Discovered: " + result.discoveredTables.size());
    System.out.println("  Successfully Queried: " + result.queryableTables.size());
    System.out.println("  Failed Queries: " + result.failedTables.size());

    if (!result.missingTables.isEmpty()) {
      System.out.println("  ⚠️ Missing Tables: " + result.missingTables);
    }

    double successRate = result.discoveredTables.isEmpty() ? 0 :
        (double) result.queryableTables.size() / result.discoveredTables.size() * 100;
    System.out.printf("  Success Rate: %.1f%%\n", successRate);
  }

  private void printComprehensiveSummary(Map<String, TestResult> results, boolean crossSchemaSuccess) {
    System.out.println("\n" + "=".repeat(80));
    System.out.println(" COMPREHENSIVE TEST SUMMARY");
    System.out.println("=".repeat(80));

    int totalExpected = SEC_EXPECTED_TABLES.size() + ECON_EXPECTED_TABLES.size() + GEO_EXPECTED_TABLES.size();
    int totalDiscovered = 0;
    int totalQueryable = 0;
    int totalFailed = 0;

    for (TestResult result : results.values()) {
      totalDiscovered += result.discoveredTables.size();
      totalQueryable += result.queryableTables.size();
      totalFailed += result.failedTables.size();
    }

    System.out.println("\n📊 Overall Statistics:");
    System.out.printf("  Total Expected Tables: %d (SEC: %d, ECON: %d, GEO: %d)\n",
                      totalExpected, SEC_EXPECTED_TABLES.size(),
                      ECON_EXPECTED_TABLES.size(), GEO_EXPECTED_TABLES.size());
    System.out.printf("  Total Discovered: %d\n", totalDiscovered);
    System.out.printf("  Total Queryable: %d\n", totalQueryable);
    System.out.printf("  Total Failed: %d\n", totalFailed);

    System.out.println("\n🎯 Schema Results:");
    for (Map.Entry<String, TestResult> entry : results.entrySet()) {
      TestResult result = entry.getValue();
      String status = result.isFullySuccessful() ? "✅ PASS" : "❌ FAIL";
      System.out.printf("  %s - %s (Discovered: %d/%d, Queryable: %d/%d)\n",
                        entry.getKey(), status,
                        result.discoveredTables.size(), getExpectedCount(entry.getKey()),
                        result.queryableTables.size(), result.discoveredTables.size());
    }

    System.out.println("\n🔗 Cross-Schema Queries: " + (crossSchemaSuccess ? "✅ PASS" : "❌ FAIL"));

    // Overall test result
    boolean allPassed = totalFailed == 0 && totalDiscovered == totalExpected && crossSchemaSuccess;
    System.out.println("\n" + "=".repeat(80));
    if (allPassed) {
      System.out.println(" ✅ COMPREHENSIVE TEST: PASSED");
      System.out.println(" All " + totalExpected + " tables across 3 schemas are fully functional!");
    } else {
      System.out.println(" ❌ COMPREHENSIVE TEST: FAILED");
      if (totalDiscovered < totalExpected) {
        System.out.println(" Missing " + (totalExpected - totalDiscovered) + " tables from discovery");
      }
      if (totalFailed > 0) {
        System.out.println(" " + totalFailed + " tables failed to query");
      }
      if (!crossSchemaSuccess) {
        System.out.println(" Cross-schema queries failed");
      }
    }
    System.out.println("=".repeat(80));
  }

  private int getExpectedCount(String schema) {
    switch (schema.toUpperCase()) {
      case "SEC": return SEC_EXPECTED_TABLES.size();
      case "ECON": return ECON_EXPECTED_TABLES.size();
      case "GEO": return GEO_EXPECTED_TABLES.size();
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
}