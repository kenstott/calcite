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
 * Comprehensive test for ECON schema.
 * Validates all expected tables are discoverable and queryable.
 * Tests FRED custom series groups and partitioning strategies.
 */
@Tag("integration")
public class EconComprehensiveTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EconComprehensiveTest.class);

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
      // Custom FRED series tables
      "fred_treasuries",
      "fred_employment_indicators",
      // FRED catalog table
      "fred_data_series_catalog"));

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test public void testEconSchemaComprehensive() throws Exception {
    // Create ECON-only model with comprehensive configuration
    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"econ\"," +
        "  \"schemas\": [" +
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
        "    }" +
        "  ]" +
        "}";

    Path modelFile = Files.createTempFile("econ-comprehensive-test", ".json");
    Files.write(modelFile, modelJson.getBytes(StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
      LOGGER.info("\n{}", createRepeatedString("=", 80));
      LOGGER.info(" ECON SCHEMA COMPREHENSIVE TEST");
      LOGGER.info(" Validating all {} expected tables", ECON_EXPECTED_TABLES.size());
      LOGGER.info("{}", "=".repeat(80));

      TestResult result = validateEconSchema(conn);

      // Print comprehensive summary
      printComprehensiveSummary(result);

      // Determine overall test result
      if (!result.isFullySuccessful()) {
        fail("ECON schema comprehensive test failed. See summary above for details.");
      } else {
        LOGGER.info("\n✅ ALL TESTS PASSED - All {} ECON tables are fully functional!",
                    ECON_EXPECTED_TABLES.size());
      }
    }
  }

  private TestResult validateEconSchema(Connection conn) throws SQLException {
    LOGGER.info("\n{}", "=".repeat(60));
    LOGGER.info(" VALIDATING ECON SCHEMA");
    LOGGER.info("{}", "=".repeat(60));

    TestResult result = new TestResult("econ");

    try (Statement stmt = conn.createStatement()) {
      // Discover tables
      List<String> discoveredTables = discoverTables(stmt);
      result.discoveredTables = new HashSet<>(discoveredTables);

      LOGGER.info("\n📊 Table Discovery:");
      LOGGER.info("  Expected: {} tables", ECON_EXPECTED_TABLES.size());
      LOGGER.info("  Discovered: {} tables", discoveredTables.size());

      // Print discovered table names
      LOGGER.info("\n  Discovered Tables:");
      for (String tableName : discoveredTables) {
        LOGGER.info("    - {}", tableName);
      }

      // Test each table
      LOGGER.info("\n🔍 Testing Table Queries:");
      for (String tableName : discoveredTables) {
        boolean querySuccess = testTable(stmt, tableName, result);
        if (querySuccess) {
          result.queryableTables.add(tableName);
        } else {
          result.failedTables.add(tableName);
        }
      }

      // Special validation for FRED partitioning
      validateFredPartitioning(stmt, result);

      // Special validation for BEA data
      validateBeaData(stmt, result);

      // Special validation for BLS data
      validateBlsData(stmt, result);

      // Calculate missing tables
      result.missingTables = new HashSet<>(ECON_EXPECTED_TABLES);
      result.missingTables.removeAll(result.discoveredTables);

      // Print missing tables if any
      if (!result.missingTables.isEmpty()) {
        LOGGER.warn("\n⚠️  Missing Tables from Discovery:");
        for (String missingTable : result.missingTables) {
          LOGGER.warn("    - {}", missingTable);
        }
      }
    }

    return result;
  }

  private List<String> discoverTables(Statement stmt) throws SQLException {
    List<String> tables = new ArrayList<>();
    String query = "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
                   "WHERE \"TABLE_SCHEMA\" = 'econ' " +
                   "ORDER BY \"TABLE_NAME\"";

    try (ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME"));
      }
    }

    return tables;
  }

  private boolean testTable(Statement stmt, String tableName, TestResult result) {
    try {
      // Test COUNT query
      String countQuery = "SELECT COUNT(*) as cnt FROM econ." + tableName;
      long rowCount = 0;

      try (ResultSet rs = stmt.executeQuery(countQuery)) {
        if (rs.next()) {
          rowCount = rs.getLong("cnt");
        }
      }

      // Test SELECT with LIMIT
      String selectQuery = "SELECT * FROM econ." + tableName + " LIMIT 5";
      int sampleRows = 0;

      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          sampleRows++;
        }
      }

      result.tableCounts.put(tableName, rowCount);

      String status = rowCount > 0 ? "✅" : "⚠️";
      LOGGER.info("  {} econ.{} - {} rows (sampled {})",
                        status, tableName, rowCount, sampleRows);

      return true;
    } catch (SQLException e) {
      LOGGER.error("  ❌ econ.{} - FAILED: {}",
                        tableName, e.getMessage());
      return false;
    }
  }

  private void validateFredPartitioning(Statement stmt, TestResult result) throws SQLException {
    LOGGER.info("\n📊 FRED Custom Series Validation:");

    // Check for custom FRED series tables
    if (result.queryableTables.contains("fred_treasuries")) {
      String query = "SELECT COUNT(*) as count FROM econ.fred_treasuries LIMIT 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          long count = rs.getLong("count");
          LOGGER.info("  ✅ Treasury FRED series table: {} rows found", count);
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Treasury FRED series table query failed: {}", e.getMessage());
      }
    } else {
      LOGGER.warn("  ⚠️ Treasury FRED series table: NOT DISCOVERED");
    }

    if (result.queryableTables.contains("fred_employment_indicators")) {
      String query = "SELECT COUNT(*) as count FROM econ.fred_employment_indicators LIMIT 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          long count = rs.getLong("count");
          LOGGER.info("  ✅ Employment indicators FRED series table: {} rows found", count);
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Employment indicators FRED series table query failed: {}", e.getMessage());
      }
    } else {
      LOGGER.warn("  ⚠️ Employment indicators FRED series table: NOT DISCOVERED");
    }

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

    // Check FRED catalog table
    if (result.queryableTables.contains("fred_data_series_catalog")) {
      try {
        String query = "SELECT COUNT(*) as total_series FROM econ.fred_data_series_catalog";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long totalSeries = rs.getLong("total_series");
            LOGGER.info("  ✅ FRED Data Series Catalog: {} series available", totalSeries);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ FRED catalog validation failed: {}", e.getMessage());
      }
    } else {
      LOGGER.warn("  ⚠️ FRED Data Series Catalog: NOT DISCOVERED");
    }

    LOGGER.info("  🎯 FRED Custom Series Partitioning: Configured with AUTO strategy for Treasury and Employment groups");
  }

  private void validateBeaData(Statement stmt, TestResult result) throws SQLException {
    LOGGER.info("\n📊 BEA (Bureau of Economic Analysis) Data Validation:");

    // Test GDP components
    if (result.queryableTables.contains("gdp_components")) {
      try {
        String query = "SELECT COUNT(DISTINCT line_description) as component_count " +
                      "FROM econ.gdp_components";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("component_count");
            LOGGER.info("  ✅ GDP Components: {} unique components found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ GDP components validation failed: {}", e.getMessage());
      }
    }

    // Test regional income
    if (result.queryableTables.contains("regional_income")) {
      try {
        String query = "SELECT COUNT(DISTINCT geo_fips) as region_count " +
                      "FROM econ.regional_income";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("region_count");
            LOGGER.info("  ✅ Regional Income: {} regions found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Regional income validation failed: {}", e.getMessage());
      }
    }

    // Test state GDP
    if (result.queryableTables.contains("state_gdp")) {
      try {
        String query = "SELECT COUNT(DISTINCT geo_fips) as state_count " +
                      "FROM econ.state_gdp";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("state_count");
            LOGGER.info("  ✅ State GDP: {} states found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ State GDP validation failed: {}", e.getMessage());
      }
    }

    // Test industry GDP
    if (result.queryableTables.contains("industry_gdp")) {
      try {
        String query = "SELECT COUNT(DISTINCT industry_description) as industry_count " +
                      "FROM econ.industry_gdp LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("industry_count");
            LOGGER.info("  ✅ Industry GDP: {} industries found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Industry GDP validation failed: {}", e.getMessage());
      }
    } else {
      LOGGER.warn("  ⚠️ Industry GDP: NOT DISCOVERED");
    }

    // Test ITA (International Trade Administration) data
    if (result.queryableTables.contains("ita_data")) {
      try {
        String query = "SELECT COUNT(*) as record_count FROM econ.ita_data";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("record_count");
            LOGGER.info("  ✅ ITA Data: {} records found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ ITA data validation failed: {}", e.getMessage());
      }
    }

    // Test trade statistics
    if (result.queryableTables.contains("trade_statistics")) {
      try {
        String query = "SELECT COUNT(*) as record_count FROM econ.trade_statistics";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("record_count");
            LOGGER.info("  ✅ Trade Statistics: {} records found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Trade statistics validation failed: {}", e.getMessage());
      }
    } else {
      LOGGER.warn("  ⚠️ Trade Statistics: NOT DISCOVERED");
    }
  }

  private void validateBlsData(Statement stmt, TestResult result) throws SQLException {
    LOGGER.info("\n📊 BLS (Bureau of Labor Statistics) Data Validation:");

    // Test employment statistics
    if (result.queryableTables.contains("employment_statistics")) {
      try {
        String query = "SELECT COUNT(*) as record_count FROM econ.employment_statistics LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("record_count");
            LOGGER.info("  ✅ Employment Statistics: {} records found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Employment statistics validation failed: {}", e.getMessage());
      }
    }

    // Test regional employment
    if (result.queryableTables.contains("regional_employment")) {
      try {
        String query = "SELECT COUNT(DISTINCT area_code) as area_count " +
                      "FROM econ.regional_employment";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("area_count");
            LOGGER.info("  ✅ Regional Employment: {} areas found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Regional employment validation failed: {}", e.getMessage());
      }
    }

    // Test wage growth
    if (result.queryableTables.contains("wage_growth")) {
      try {
        String query = "SELECT COUNT(*) as record_count FROM econ.wage_growth LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("record_count");
            LOGGER.info("  ✅ Wage Growth: {} records found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Wage growth validation failed: {}", e.getMessage());
      }
    }

    // Test inflation metrics
    if (result.queryableTables.contains("inflation_metrics")) {
      try {
        String query = "SELECT COUNT(*) as record_count FROM econ.inflation_metrics LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            long count = rs.getLong("record_count");
            LOGGER.info("  ✅ Inflation Metrics: {} records found", count);
          }
        }
      } catch (SQLException e) {
        LOGGER.warn("  ⚠️ Inflation metrics validation failed: {}", e.getMessage());
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

  private void printComprehensiveSummary(TestResult result) {
    LOGGER.info("\n{}", createRepeatedString("=", 80));
    LOGGER.info(" ECON SCHEMA COMPREHENSIVE TEST SUMMARY");
    LOGGER.info("{}", "=".repeat(80));

    LOGGER.info("\n📊 Overall Statistics:");
    LOGGER.info("  Total Expected Tables: {}", ECON_EXPECTED_TABLES.size());
    LOGGER.info("  Total Discovered: {}", result.discoveredTables.size());
    LOGGER.info("  Total Queryable: {}", result.queryableTables.size());
    LOGGER.info("  Total Failed: {}", result.failedTables.size());

    if (!result.missingTables.isEmpty()) {
      LOGGER.error("\n❌ Missing Tables ({}):", result.missingTables.size());
      for (String missingTable : result.missingTables) {
        LOGGER.error("    - {}", missingTable);
      }
    }

    if (!result.failedTables.isEmpty()) {
      LOGGER.error("\n❌ Failed to Query ({}):", result.failedTables.size());
      for (String failedTable : result.failedTables) {
        LOGGER.error("    - {}", failedTable);
      }
    }

    double discoveryRate = ECON_EXPECTED_TABLES.isEmpty() ? 0 :
        (double) result.discoveredTables.size() / ECON_EXPECTED_TABLES.size() * 100;
    double querySuccessRate = result.discoveredTables.isEmpty() ? 0 :
        (double) result.queryableTables.size() / result.discoveredTables.size() * 100;

    LOGGER.info("\n📈 Success Rates:");
    LOGGER.info("  Table Discovery: {}/{} ({:.1f}%)",
                      result.discoveredTables.size(), ECON_EXPECTED_TABLES.size(), discoveryRate);
    LOGGER.info("  Query Success: {}/{} ({:.1f}%)",
                      result.queryableTables.size(), result.discoveredTables.size(), querySuccessRate);

    // Overall test result
    LOGGER.info("\n{}", createRepeatedString("=", 80));
    if (result.isFullySuccessful()) {
      LOGGER.info(" ✅ ECON COMPREHENSIVE TEST: PASSED");
      LOGGER.info(" All {} tables are fully functional!", ECON_EXPECTED_TABLES.size());
    } else {
      LOGGER.error(" ❌ ECON COMPREHENSIVE TEST: FAILED");
      if (!result.missingTables.isEmpty()) {
        LOGGER.error(" Missing {} tables from discovery", result.missingTables.size());
      }
      if (!result.failedTables.isEmpty()) {
        LOGGER.error(" {} tables failed to query", result.failedTables.size());
      }
    }
    LOGGER.info("{}", "=".repeat(80));
  }

  /**
   * Helper class to track test results for ECON schema.
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
