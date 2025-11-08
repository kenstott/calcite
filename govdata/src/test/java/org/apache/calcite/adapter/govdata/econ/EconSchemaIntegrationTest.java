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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comprehensive integration test for ECON schema.
 * Verifies that all tables defined in the schema are actually accessible via SQL
 * and that data can be queried from them.
 */
@Tag("integration")
public class EconSchemaIntegrationTest {

  @TempDir
  static Path tempDir;

  private static String cacheDir;
  private static String parquetDir;

  // All 13 tables that should be exposed in ECON schema
  private static final Set<String> EXPECTED_TABLES = new HashSet<>();
  static {
    // BLS tables
    EXPECTED_TABLES.add("employment_statistics");
    EXPECTED_TABLES.add("inflation_metrics");
    EXPECTED_TABLES.add("wage_growth");
    EXPECTED_TABLES.add("regional_employment");

    // Treasury tables
    EXPECTED_TABLES.add("treasury_yields");
    EXPECTED_TABLES.add("federal_debt");

    // World Bank tables
    EXPECTED_TABLES.add("world_indicators");

    // FRED tables
    EXPECTED_TABLES.add("fred_indicators");

    // BEA tables
    EXPECTED_TABLES.add("national_accounts");
    EXPECTED_TABLES.add("regional_income");
    EXPECTED_TABLES.add("trade_statistics");
    EXPECTED_TABLES.add("ita_data");
    EXPECTED_TABLES.add("industry_gdp");
  }

  @BeforeAll
  static void setupTestData() throws Exception {
    // Set up test directories
    cacheDir = tempDir.resolve("econ-cache").toString();
    parquetDir = tempDir.resolve("econ-parquet").toString();

    // Create directory structure for test data
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=employment/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=inflation/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=wages/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=regional_employment/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=treasury/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=debt/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=world/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=fred/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=gdp/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=regional_income/year=2024"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=trade/year_range=2022_2023"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=ita/year_range=2022_2023"));
    Files.createDirectories(tempDir.resolve("econ-parquet/source=econ/type=industry_gdp/year_range=2022_2023"));

    // Create minimal sample Parquet files for each table
    // In a real test, we'd use actual downloaders to create these
    createSampleParquetFiles();
  }

  private static void createSampleParquetFiles() throws Exception {
    // For this test, we'll check if we can download minimal real data
    // Otherwise we'll skip the data query tests

    // Try to create minimal data using the downloaders if API keys are available
    String beaKey = System.getenv("BEA_API_KEY");
    String blsKey = System.getenv("BLS_API_KEY");

    // FRED downloads removed - now handled via metadata-driven approach
    // TODO: Re-enable when executeDownload() is implemented in EconSchemaFactory

    if (beaKey != null) {
      StorageProvider storageProvider = StorageProviderFactory.createFromUrl("file://" + cacheDir);
      BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, cacheDir, storageProvider, storageProvider);

      // Try to download each BEA dataset using metadata-driven methods
      try {
        List<String> nipaTablesList = extractIterationList("national_accounts", "nipaTablesList");
        if (!nipaTablesList.isEmpty()) {
          beaDownloader.downloadNationalAccountsMetadata(2024, 2024, nipaTablesList, java.util.Collections.emptyMap());
          beaDownloader.convertNationalAccountsMetadata(2024, 2024, nipaTablesList, java.util.Collections.emptyMap());
        }
      } catch (Exception e) {
        // Ignore
      }

      try {
        beaDownloader.downloadTradeStatisticsMetadata(2022, 2023);
        beaDownloader.convertTradeStatisticsMetadata(2022, 2023);
      } catch (Exception e) {
        // Ignore
      }

      try {
        List<String> itaIndicatorsList = extractIterationList("ita_data", "itaIndicatorsList");
        if (!itaIndicatorsList.isEmpty()) {
          beaDownloader.downloadItaDataMetadata(2022, 2023, itaIndicatorsList);
          beaDownloader.convertItaDataMetadata(2022, 2023, itaIndicatorsList);
        }
      } catch (Exception e) {
        // Ignore
      }

      try {
        List<String> keyIndustriesList = extractIterationList("industry_gdp", "keyIndustriesList");
        if (!keyIndustriesList.isEmpty()) {
          beaDownloader.downloadIndustryGdpMetadata(2022, 2023, keyIndustriesList);
          beaDownloader.convertIndustryGdpMetadata(2022, 2023, keyIndustriesList);
        }
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test public void testAllTablesExposedInSchema() throws Exception {
    // Create Calcite model JSON
    String modelJson =
        String.format("{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.econ.EconSchemaFactory\","
        + "      \"operand\": {"
        + "        \"directory\": \"%s\","
        + "        \"cacheDirectory\": \"%s\","
        + "        \"executionEngine\": \"DUCKDB\","
        + "        \"tableNameCasing\": \"SMART_CASING\","
        + "        \"columnNameCasing\": \"SMART_CASING\""
        + "      }"
        + "    }"
        + "  ]"
        + "}", parquetDir, cacheDir);

    // Write model file
    Path modelFile = Files.createTempFile("econ-test-model", ".json");
    Files.write(modelFile, modelJson.getBytes());

    // Connect via JDBC
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData meta = conn.getMetaData();

      // Get all tables in ECON schema
      Set<String> actualTables = new HashSet<>();
      try (ResultSet rs = meta.getTables(null, "ECON", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME").toLowerCase();
          String tableComment = rs.getString("REMARKS");

          actualTables.add(tableName);

          // Verify table has a comment
          if (EXPECTED_TABLES.contains(tableName)) {
            assertNotNull(tableComment, "Table " + tableName + " should have a comment");
            assertFalse(tableComment.isEmpty(), "Table " + tableName + " comment should not be empty");
          }
        }
      }

      // Verify all expected tables are present
      for (String expectedTable : EXPECTED_TABLES) {
        assertTrue(actualTables.contains(expectedTable),
            "Expected table '" + expectedTable + "' not found in schema. Found tables: " + actualTables);
      }

      // Report on coverage
      System.out.println("ECON Schema Table Coverage:");
      System.out.println("Expected tables: " + EXPECTED_TABLES.size());
      System.out.println("Found tables: " + actualTables.size());
      System.out.println("All expected tables present: " + actualTables.containsAll(EXPECTED_TABLES));
    }
  }

  @Test public void testTableColumnsAccessible() throws Exception {
    // Only test if we have at least one API key
    boolean hasApiKey = System.getenv("FRED_API_KEY") != null ||
                         System.getenv("BEA_API_KEY") != null ||
                         System.getenv("BLS_API_KEY") != null;

    assumeTrue(hasApiKey, "Skipping column test - no API keys available");

    Path modelFile = createModelFile();

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData meta = conn.getMetaData();

      // Test a few key tables have expected columns
      verifyTableColumns(meta, "trade_statistics",
          "table_id", "year", "value", "trade_type", "category");

      verifyTableColumns(meta, "ita_data",
          "indicator", "year", "value", "units");

      verifyTableColumns(meta, "industry_gdp",
          "table_id", "frequency", "year", "quarter", "industry_code", "value");
    }
  }

  @Test public void testSqlQueriesWork() throws Exception {
    Path modelFile = createModelFile();

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {

      try (Statement stmt = conn.createStatement()) {
        // Test EXPLAIN PLAN for each table - this verifies the table is queryable
        for (String table : EXPECTED_TABLES) {
          String query = "EXPLAIN PLAN FOR SELECT * FROM econ." + table + " LIMIT 1";

          try (ResultSet rs = stmt.executeQuery(query)) {
            assertTrue(rs.next(), "EXPLAIN PLAN should return at least one row for " + table);
            String plan = rs.getString(1);
            assertNotNull(plan, "EXPLAIN PLAN should not be null for " + table);

            // Verify the plan mentions the table
            assertTrue(plan.toLowerCase().contains(table.toLowerCase()) ||
                       plan.contains("PartitionedParquetTable"),
                       "EXPLAIN PLAN should reference table " + table);
          } catch (SQLException e) {
            // If table doesn't exist, we'll get an error here
            throw new AssertionError("Failed to query table " + table + ": " + e.getMessage(), e);
          }
        }
      }
    }
  }

  @Test public void testDataRetrievalFromPopulatedTables() throws Exception {
    // This test only runs if we have actual data files
    File tradeFile = new File(parquetDir, "source=econ/type=trade/year_range=2022_2023/trade_statistics.parquet");
    assumeTrue(tradeFile.exists(), "Skipping data test - no trade_statistics.parquet file");

    Path modelFile = createModelFile();

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {

      try (Statement stmt = conn.createStatement()) {
        // Query actual data from trade_statistics
        String query = "SELECT COUNT(*) as cnt FROM econ.trade_statistics";

        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "COUNT query should return a result");
          int count = rs.getInt("cnt");
          assertTrue(count >= 0, "Count should be non-negative");
          System.out.println("trade_statistics row count: " + count);
        }

        // Test filtering and projection
        query = "SELECT year, trade_type, COUNT(*) as cnt "
              + "FROM econ.trade_statistics "
              + "WHERE year >= 2022 "
              + "GROUP BY year, trade_type "
              + "ORDER BY year, trade_type";

        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            int year = rs.getInt("year");
            String tradeType = rs.getString("trade_type");
            int count = rs.getInt("cnt");

            assertTrue(year >= 2022, "Year should be >= 2022");
            assertNotNull(tradeType, "Trade type should not be null");
            assertTrue(count > 0, "Count should be positive");

            System.out.printf("Year: %d, Trade Type: %s, Count: %d%n", year, tradeType, count);
          }
        }
      }
    }
  }

  @Test public void testCrossTableJoins() throws Exception {
    // Test that we can join between tables if data exists
    File tradeFile = new File(parquetDir, "source=econ/type=trade/year_range=2022_2023/trade_statistics.parquet");
    File itaFile = new File(parquetDir, "source=econ/type=ita/year_range=2022_2023/ita_data.parquet");

    assumeTrue(tradeFile.exists() && itaFile.exists(),
        "Skipping join test - required parquet files don't exist");

    Path modelFile = createModelFile();

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {

      try (Statement stmt = conn.createStatement()) {
        // Join trade_statistics with ita_data on year
        String query = "SELECT t.year, COUNT(DISTINCT t.category) as trade_categories, "
                     + "COUNT(DISTINCT i.indicator) as ita_indicators "
                     + "FROM econ.trade_statistics t "
                     + "JOIN econ.ita_data i ON t.year = i.year "
                     + "GROUP BY t.year "
                     + "ORDER BY t.year";

        try (ResultSet rs = stmt.executeQuery(query)) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            int year = rs.getInt("year");
            int tradeCategories = rs.getInt("trade_categories");
            int itaIndicators = rs.getInt("ita_indicators");

            assertTrue(year >= 2022, "Year should be >= 2022");
            assertTrue(tradeCategories > 0, "Should have trade categories");
            assertTrue(itaIndicators > 0, "Should have ITA indicators");

            System.out.printf("Year: %d, Trade Categories: %d, ITA Indicators: %d%n",
                year, tradeCategories, itaIndicators);
          }

          if (!hasResults) {
            System.out.println("Warning: Join returned no results - tables may be empty");
          }
        }
      }
    }
  }

  private void verifyTableColumns(DatabaseMetaData meta, String tableName, String... expectedColumns)
      throws SQLException {
    Set<String> actualColumns = new HashSet<>();

    try (ResultSet rs = meta.getColumns(null, "ECON", tableName.toUpperCase(), null)) {
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME").toLowerCase();
        String columnComment = rs.getString("REMARKS");
        actualColumns.add(columnName);

        // Verify important columns have comments
        if (columnName.equals("year") || columnName.equals("value")) {
          assertNotNull(columnComment,
              String.format("Column %s.%s should have a comment", tableName, columnName));
        }
      }
    }

    // Verify expected columns exist
    for (String expectedCol : expectedColumns) {
      assertTrue(actualColumns.contains(expectedCol.toLowerCase()),
          String.format("Table %s should have column %s. Found columns: %s",
              tableName, expectedCol, actualColumns));
    }
  }

  private Path createModelFile() throws Exception {
    String modelJson =
        String.format("{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.econ.EconSchemaFactory\","
        + "      \"operand\": {"
        + "        \"directory\": \"%s\","
        + "        \"cacheDirectory\": \"%s\","
        + "        \"executionEngine\": \"DUCKDB\","
        + "        \"tableNameCasing\": \"SMART_CASING\","
        + "        \"columnNameCasing\": \"SMART_CASING\""
        + "      }"
        + "    }"
        + "  ]"
        + "}", parquetDir, cacheDir);

    Path modelFile = Files.createTempFile("econ-test-model", ".json");
    Files.write(modelFile, modelJson.getBytes());
    return modelFile;
  }

  @SuppressWarnings("unchecked")
  private static List<String> extractIterationList(String tableName, String listKey) {
    try {
      InputStream schemaStream = EconSchemaIntegrationTest.class.getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        return Collections.emptyList();
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(schemaStream);
      JsonNode tables = root.get("tables");
      if (tables != null && tables.isArray()) {
        for (JsonNode table : tables) {
          if (tableName.equals(table.get("name").asText())) {
            JsonNode download = table.get("download");
            if (download != null) {
              JsonNode listNode = download.get(listKey);
              if (listNode != null && listNode.isArray()) {
                List<String> result = new ArrayList<>();
                for (JsonNode item : listNode) {
                  result.add(item.asText());
                }
                return result;
              }
            }
          }
        }
      }
      return Collections.emptyList();
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }
}
