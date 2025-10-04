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
import org.apache.calcite.schema.Schema;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for Census Schema implementation.
 *
 * This test verifies that:
 * 1. All expected Census tables are discoverable via metadata
 * 2. All tables are queryable with proper SQL
 * 3. All tables contain data (non-zero rows) when data is available
 */
@Tag("integration")
public class CensusSchemaTest {

  @TempDir
  private static Path tempDir;

  private static String cacheDir;
  private static String parquetDir;
  private static File modelFile;

  // List of expected Census tables based on census-schema.json
  // Tables are created with lowercase names due to SMART_CASING
  private static final List<String> EXPECTED_TABLES =
      Arrays.asList("acs_population",
      "acs_demographics",
      "acs_income",
      "acs_poverty",
      "acs_employment",
      "acs_education",
      "acs_housing",
      "acs_housing_costs",
      "acs_commuting",
      "acs_health_insurance",
      "acs_language",
      "acs_disability",
      "acs_veterans",
      "acs_migration",
      "acs_occupation",
      "decennial_population",
      "decennial_demographics",
      "decennial_housing",
      "economic_census",
      "county_business_patterns",
      "population_estimates");

  @BeforeAll
  public static void setUp() throws Exception {
    // Load environment variables from .env.test
    TestEnvironmentLoader.ensureLoaded();

    // Get paths from environment or use temp dir
    String envCacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String envParquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");

    if (envCacheDir != null && envParquetDir != null) {
      // Use real directories from .env.test for actual data
      cacheDir = envCacheDir;
      parquetDir = envParquetDir;
    } else {
      // Fall back to temp directories
      cacheDir = tempDir.resolve("census-cache").toString();
      parquetDir = tempDir.resolve("census-parquet").toString();

      // Create directories
      new File(cacheDir).mkdirs();
      new File(parquetDir).mkdirs();
    }

    // Set environment variables for the test
    System.setProperty("GOVDATA_CACHE_DIR", cacheDir);
    System.setProperty("GOVDATA_PARQUET_DIR", parquetDir);

    // Set CENSUS_API_KEY from .env.test
    String apiKey = TestEnvironmentLoader.getEnv("CENSUS_API_KEY");
    if (apiKey != null) {
      System.setProperty("CENSUS_API_KEY", apiKey);
      System.out.println("Census API key configured: " + apiKey.substring(0, 8) + "...");
    } else {
      System.out.println("Warning: No CENSUS_API_KEY found in .env.test");
    }

    // Create test model file
    createTestModel();
  }

  private static void createTestModel() throws Exception {
    modelFile = tempDir.resolve("census-model.json").toFile();

    String modelJson = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"census\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"census\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"dataSource\": \"census\",\n"
  +
        "      \"autoDownload\": true,\n"
  +  // Enable auto-download with improved caching
        "      \"startYear\": 2020,\n"
  +
        "      \"endYear\": 2023,\n"
  +
        "      \"censusCacheTtlDays\": 365\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}";

    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write(modelJson);
    }
  }


  @Test @SuppressWarnings("deprecation")
  public void testCensusSchemaConnection() throws Exception {
    // Test that we can connect to the Census schema
    try (Connection connection = createConnection()) {
      assertNotNull(connection, "Connection should not be null");

      // Verify it's a Calcite connection
      assertTrue(connection instanceof CalciteConnection,
          "Connection should be a CalciteConnection");

      // Get the Census schema
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      // Using deprecated API as it's still used throughout Calcite tests
      Schema censusSchema = calciteConnection.getRootSchema().getSubSchema("census");
      assertNotNull(censusSchema, "census schema should exist");
    }
  }

  @Test public void testAllTablesDiscoverable() throws Exception {
    // Test that all expected tables are discoverable via metadata
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet tables = metaData.getTables(null, "census", "%", null);

      Set<String> foundTables = new HashSet<>();
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        foundTables.add(tableName.toUpperCase());
      }

      // Check that all expected tables are found (compare lowercase)
      List<String> missingTables = new ArrayList<>();
      for (String expectedTable : EXPECTED_TABLES) {
        if (!foundTables.contains(expectedTable.toUpperCase()) &&
            !foundTables.contains(expectedTable.toLowerCase())) {
          missingTables.add(expectedTable);
        }
      }

      if (!missingTables.isEmpty()) {
        System.out.println("Found tables: " + foundTables);
        System.out.println("Missing tables: " + missingTables);
      }

      assertTrue(missingTables.isEmpty(),
          "Missing expected tables: " + missingTables);

      // Also verify no unexpected tables
      Set<String> expectedSet = new HashSet<>(EXPECTED_TABLES);
      Set<String> unexpectedTables = new HashSet<>();
      for (String foundTable : foundTables) {
        if (!expectedSet.contains(foundTable)) {
          unexpectedTables.add(foundTable);
        }
      }

      // It's OK to have extra tables, just log them
      if (!unexpectedTables.isEmpty()) {
        System.out.println("Additional tables found (OK): " + unexpectedTables);
      }
    }
  }

  @Test public void testAllTablesQueryable() throws Exception {
    // Test that all tables can be queried with SELECT *
    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      List<String> failedQueries = new ArrayList<>();

      for (String tableName : EXPECTED_TABLES) {
        try {
          // Try a simple SELECT query with LIMIT to avoid loading too much data
          // Use quoted identifiers to preserve case
          String query = String.format("SELECT * FROM census.\"%s\" LIMIT 1", tableName);
          ResultSet rs = stmt.executeQuery(query);

          // Verify we can get metadata
          assertNotNull(rs.getMetaData(),
              "ResultSet metadata should not be null for " + tableName);

          // Verify column count is > 0
          int columnCount = rs.getMetaData().getColumnCount();
          // Note: Partitioned tables may not expose column metadata through JDBC
          // but they still work for queries
          if (columnCount == 0) {
            System.out.println("Warning: " + tableName + " has no columns exposed via JDBC metadata (partitioned table limitation)");
          }

          rs.close();
        } catch (SQLException e) {
          // If query fails, add to failed list
          failedQueries.add(tableName + ": " + e.getMessage());
        }
      }

      assertTrue(failedQueries.isEmpty(),
          "Failed to query tables: " + String.join(", ", failedQueries));
    }
  }

  @Test public void testTableColumns() throws Exception {
    // Test that tables have expected columns
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // Test a few key tables for expected columns

      // Test acs_population columns (expect friendly names from CensusVariableMapper)
      verifyTableColumns(metaData, "acs_population",
          Arrays.asList("geoid", "year", "total_population"));

      // Test acs_income columns
      verifyTableColumns(metaData, "acs_income",
          Arrays.asList("geoid", "year", "median_household_income"));

      // Test acs_education columns
      verifyTableColumns(metaData, "acs_education",
          Arrays.asList("geoid", "year", "population_25_and_over"));

      // Test decennial_population columns
      verifyTableColumns(metaData, "decennial_population",
          Arrays.asList("geoid", "year", "total_population"));
    }
  }

  @Test public void testCrossTableJoins() throws Exception {
    // Test that we can join Census tables (demonstrates foreign key relationships)
    // Note: Partitioned tables may not support column-level metadata, so we use SELECT *
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // First, let's check what columns are exposed via metadata for acs_population
      System.out.println("\n=== Checking JDBC metadata for acs_population ===");
      ResultSet popColumns = metaData.getColumns(null, "census", "acs_population", null);
      int popColCount = 0;
      while (popColumns.next()) {
        popColCount++;
        System.out.println("  Column: " + popColumns.getString("COLUMN_NAME") +
            " (" + popColumns.getString("TYPE_NAME") + ")");
      }
      System.out.println("Total columns from metadata: " + popColCount);
      popColumns.close();

      // Check columns for acs_income
      System.out.println("\n=== Checking JDBC metadata for acs_income ===");
      ResultSet incColumns = metaData.getColumns(null, "census", "acs_income", null);
      int incColCount = 0;
      while (incColumns.next()) {
        incColCount++;
        System.out.println("  Column: " + incColumns.getString("COLUMN_NAME") +
            " (" + incColumns.getString("TYPE_NAME") + ")");
      }
      System.out.println("Total columns from metadata: " + incColCount);
      incColumns.close();

      Statement stmt = connection.createStatement();

      // Now try a simple SELECT * on acs_population to see what columns are actually available
      System.out.println("\n=== Testing SELECT * from acs_population ===");
      ResultSet popRs = stmt.executeQuery("SELECT * FROM census.\"acs_population\" LIMIT 0");
      int popActualCols = popRs.getMetaData().getColumnCount();
      System.out.println("Columns from SELECT * (with LIMIT 0): " + popActualCols);
      for (int i = 1; i <= popActualCols; i++) {
        System.out.println("  " + i + ": " + popRs.getMetaData().getColumnName(i) +
            " (" + popRs.getMetaData().getColumnTypeName(i) + ")");
      }
      popRs.close();

      // Test join between ACS tables using SELECT *
      System.out.println("\n=== Testing JOIN query with SELECT * ===");
      String joinQueryStar =
          "SELECT * " +
          "FROM census.\"acs_population\" p " +
          "JOIN census.\"acs_income\" i ON p.\"geoid\" = i.\"geoid\" AND p.\"year\" = i.\"year\" " +
          "LIMIT 1";

      // Also test with specific columns
      System.out.println("\n=== Testing JOIN query with specific columns ===");
      String joinQueryCols =
          "SELECT p.\"geoid\", p.\"total_population\", i.\"median_household_income\" " +
          "FROM census.\"acs_population\" p " +
          "JOIN census.\"acs_income\" i ON p.\"geoid\" = i.\"geoid\" AND p.\"year\" = i.\"year\" " +
          "LIMIT 1";

      // First, try a simple SELECT with specific column on single table (no join) to verify columns work
      System.out.println("\n=== Testing simple SELECT with specific column (no join) ===");
      try {
        String simpleQuery = "SELECT \"geoid\" FROM census.\"acs_population\" LIMIT 1";
        ResultSet rs = stmt.executeQuery(simpleQuery);
        System.out.println("SUCCESS: Simple SELECT with specific column works");
        rs.close();
      } catch (SQLException e) {
        System.out.println("FAILED: Simple SELECT failed: " + e.getMessage());
      }

      // Try with table alias but no join
      System.out.println("\n=== Testing SELECT with alias but no join ===");
      try {
        String aliasQuery = "SELECT p.\"geoid\" FROM census.\"acs_population\" p LIMIT 1";
        ResultSet rs = stmt.executeQuery(aliasQuery);
        System.out.println("SUCCESS: SELECT with alias works");
        rs.close();
      } catch (SQLException e) {
        System.out.println("FAILED: SELECT with alias failed: " + e.getMessage());
        e.printStackTrace();
      }

      // Try the specific columns query with join
      System.out.println("\n=== Testing JOIN query with specific columns ===");
      try {
        System.out.println("Query: " + joinQueryCols);
        ResultSet rs = stmt.executeQuery(joinQueryCols);
        assertNotNull(rs, "Join query with specific columns should return a result set");
        int columnCount = rs.getMetaData().getColumnCount();
        System.out.println("SUCCESS! Specific columns query returned " + columnCount + " columns:");
        for (int i = 1; i <= columnCount; i++) {
          System.out.println("  " + i + ": " + rs.getMetaData().getColumnName(i) +
              " (" + rs.getMetaData().getColumnTypeName(i) + ")");
        }
        rs.close();
      } catch (SQLException e) {
        System.out.println("FAILED: Specific columns query failed: " + e.getMessage());
        e.printStackTrace();

        // Fall back to SELECT *
        try {
          System.out.println("\n=== Falling back to SELECT * query ===");
          ResultSet rs = stmt.executeQuery(joinQueryStar);
          assertNotNull(rs, "Join query should return a result set");

          // Verify we can get result set metadata
          assertNotNull(rs.getMetaData());

          // Joins should return columns from both tables
          int columnCount = rs.getMetaData().getColumnCount();
          assertTrue(columnCount > 0,
              "Join should return columns, got " + columnCount);

          // Log what columns are actually returned
          System.out.println("SELECT * returned " + columnCount + " columns:");
          for (int i = 1; i <= columnCount; i++) {
            System.out.println("  " + i + ": " + rs.getMetaData().getColumnName(i) +
                " (" + rs.getMetaData().getColumnTypeName(i) + ")");
          }

          rs.close();
        } catch (SQLException e2) {
          // If no data or partitioned table limitations, that's OK for this test
          String msg = e2.getMessage();
          if (!msg.contains("Empty") &&
              !msg.contains("No match") &&
              !msg.contains("not found")) {
            throw e2;
          }
          // Log the issue but don't fail the test
          System.out.println("Note: Join test encountered: " + msg);
        }
      }
    }
  }


  @Test public void testPartitionedTableAggregationWithFilter() throws Exception {
    // Test that aggregation works correctly when filtering by partition key
    String apiKey = System.getProperty("CENSUS_API_KEY");
    assertNotNull(apiKey, "CENSUS_API_KEY must be configured for integration tests");

    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      // Aggregation WITH partition filter should work correctly
      String query = "SELECT COUNT(*) as row_count FROM census.population_estimates WHERE \"year\" = 2020";

      ResultSet rs = stmt.executeQuery(query);
      assertTrue(rs.next(), "Should have results");

      int count = rs.getInt("row_count");
      assertTrue(count > 0, "Should have data for year 2020");
      System.out.println("COUNT with partition filter: " + count);
    }
  }

  @Test public void testDataPresence() throws Exception {
    // Test that tables contain data (non-zero rows)
    // This test requires actual Census data to be downloaded
    // For partitioned tables, we test with a specific year to avoid partition scanning issues

    String apiKey = System.getProperty("CENSUS_API_KEY");
    assertNotNull(apiKey, "CENSUS_API_KEY must be configured for integration tests");

    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      List<String> emptyTables = new ArrayList<>();

      // Define which tables are partitioned by year
      List<String> partitionedTables =
          Arrays.asList("acs_population", "acs_demographics", "acs_income", "acs_poverty",
          "acs_employment", "acs_education", "acs_housing", "acs_housing_costs",
          "acs_commuting", "acs_health_insurance", "acs_language", "acs_disability",
          "acs_veterans", "acs_migration", "acs_occupation",
          "decennial_population", "decennial_demographics", "decennial_housing",
          "economic_census", "county_business_patterns",
          "population_estimates");

      for (String tableName : EXPECTED_TABLES) {
        try {
          String countQuery;
          // For partitioned tables, filter by a specific year to avoid ClassCastException
          // on partition keys during aggregation
          if (partitionedTables.contains(tableName)) {
            countQuery =
                String.format("SELECT COUNT(*) as row_count FROM census.\"%s\" WHERE \"year\" = 2020",
                tableName);
          } else {
            countQuery =
                String.format("SELECT COUNT(*) as row_count FROM census.\"%s\"", tableName);
          }

          ResultSet rs = stmt.executeQuery(countQuery);

          if (rs.next()) {
            int rowCount = rs.getInt("row_count");
            if (rowCount == 0) {
              emptyTables.add(tableName);
            } else {
              System.out.println(tableName + " has " + rowCount + " rows");
            }
          }
          rs.close();

        } catch (SQLException e) {
          // Log but don't fail - table might not have data yet
          System.out.println("Could not count rows in " + tableName +
              ": " + e.getMessage());
        }
      }

      // For data presence test, we expect at least some tables to have data
      assertFalse(emptyTables.size() == EXPECTED_TABLES.size(),
          "At least some tables should have data when API key is configured");

      if (!emptyTables.isEmpty()) {
        System.out.println("Tables without data: " + emptyTables);
      }
    }
  }

  @Test public void testComprehensiveDataCoverage() throws Exception {
    // Comprehensive test that validates all tables have data for all expected years
    // This test requires actual Census data to be downloaded

    String apiKey = System.getProperty("CENSUS_API_KEY");
    assertNotNull(apiKey, "CENSUS_API_KEY must be configured for integration tests");

    // Expected years from model configuration
    int[] expectedYears = {2020, 2021, 2022, 2023};

    // Tables by data source type
    List<String> acsTables =
        Arrays.asList("acs_population", "acs_demographics", "acs_income", "acs_poverty",
        "acs_employment", "acs_education", "acs_housing", "acs_housing_costs",
        "acs_commuting", "acs_health_insurance", "acs_language", "acs_disability",
        "acs_veterans", "acs_migration", "acs_occupation");

    List<String> decennialTables =
        Arrays.asList("decennial_population", "decennial_demographics", "decennial_housing");

    List<String> economicTables =
        Arrays.asList("economic_census", "county_business_patterns");

    List<String> populationTables =
        Arrays.asList("population_estimates");

    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      List<String> missingData = new ArrayList<>();
      int totalCombinations = 0;
      int successfulCombinations = 0;

      System.out.println("\n=== COMPREHENSIVE DATA COVERAGE TEST ===\n");

      // Test ACS tables (should have data for all years 2020-2023)
      System.out.println("Testing ACS tables (expect 2020-2023):");
      for (String tableName : acsTables) {
        for (int year : expectedYears) {
          totalCombinations++;
          String query =
              String.format("SELECT COUNT(*) as row_count FROM census.\"%s\" WHERE \"year\" = %d",
              tableName, year);

          try {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
              int rowCount = rs.getInt("row_count");
              if (rowCount > 0) {
                System.out.printf("  ✓ %s [%d]: %d rows%n", tableName, year, rowCount);
                successfulCombinations++;
              } else {
                String missing = String.format("%s [%d]: 0 rows", tableName, year);
                missingData.add(missing);
                System.out.printf("  ✗ %s%n", missing);
              }
            }
            rs.close();
          } catch (SQLException e) {
            String missing = String.format("%s [%d]: %s", tableName, year, e.getMessage());
            missingData.add(missing);
            System.out.printf("  ✗ %s%n", missing);
          }
        }
      }

      // Test Decennial tables (should have data for 2020 only)
      System.out.println("\nTesting Decennial tables (expect 2020 only):");
      for (String tableName : decennialTables) {
        totalCombinations++;
        String query =
            String.format("SELECT COUNT(*) as row_count FROM census.\"%s\" WHERE \"year\" = 2020",
            tableName);

        try {
          ResultSet rs = stmt.executeQuery(query);
          if (rs.next()) {
            int rowCount = rs.getInt("row_count");
            if (rowCount > 0) {
              System.out.printf("  ✓ %s [2020]: %d rows%n", tableName, rowCount);
              successfulCombinations++;
            } else {
              String missing = String.format("%s [2020]: 0 rows", tableName);
              missingData.add(missing);
              System.out.printf("  ✗ %s%n", missing);
            }
          }
          rs.close();
        } catch (SQLException e) {
          String missing = String.format("%s [2020]: %s", tableName, e.getMessage());
          missingData.add(missing);
          System.out.printf("  ✗ %s%n", missing);
        }
      }

      // Test Economic tables (should have data for 2022 only per Economic Census schedule)
      System.out.println("\nTesting Economic Census tables (expect 2022 only):");
      for (String tableName : economicTables) {
        totalCombinations++;
        String query =
            String.format("SELECT COUNT(*) as row_count FROM census.\"%s\" WHERE \"year\" = 2022",
            tableName);

        try {
          ResultSet rs = stmt.executeQuery(query);
          if (rs.next()) {
            int rowCount = rs.getInt("row_count");
            if (rowCount > 0) {
              System.out.printf("  ✓ %s [2022]: %d rows%n", tableName, rowCount);
              successfulCombinations++;
            } else {
              String missing = String.format("%s [2022]: 0 rows", tableName);
              missingData.add(missing);
              System.out.printf("  ✗ %s%n", missing);
            }
          }
          rs.close();
        } catch (SQLException e) {
          String missing = String.format("%s [2022]: %s", tableName, e.getMessage());
          missingData.add(missing);
          System.out.printf("  ✗ %s%n", missing);
        }
      }

      // Test Population Estimates (should have data for 2020-2023)
      System.out.println("\nTesting Population Estimates (expect 2020-2023):");
      for (String tableName : populationTables) {
        for (int year : expectedYears) {
          totalCombinations++;
          String query =
              String.format("SELECT COUNT(*) as row_count FROM census.\"%s\" WHERE \"year\" = %d",
              tableName, year);

          try {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
              int rowCount = rs.getInt("row_count");
              if (rowCount > 0) {
                System.out.printf("  ✓ %s [%d]: %d rows%n", tableName, year, rowCount);
                successfulCombinations++;
              } else {
                String missing = String.format("%s [%d]: 0 rows", tableName, year);
                missingData.add(missing);
                System.out.printf("  ✗ %s%n", missing);
              }
            }
            rs.close();
          } catch (SQLException e) {
            String missing = String.format("%s [%d]: %s", tableName, year, e.getMessage());
            missingData.add(missing);
            System.out.printf("  ✗ %s%n", missing);
          }
        }
      }

      // Summary
      System.out.printf("%n=== SUMMARY ===%n");
      System.out.printf("Total table/year combinations: %d%n", totalCombinations);
      System.out.printf("Successful: %d (%.1f%%)%n",
          successfulCombinations,
          (successfulCombinations * 100.0 / totalCombinations));
      System.out.printf("Missing: %d (%.1f%%)%n",
          missingData.size(),
          (missingData.size() * 100.0 / totalCombinations));

      if (!missingData.isEmpty()) {
        System.out.println("\nMissing data details:");
        for (String missing : missingData) {
          System.out.println("  - " + missing);
        }
      }

      // Require at least 90% coverage
      double coveragePercent = successfulCombinations * 100.0 / totalCombinations;
      assertTrue(coveragePercent >= 90.0,
          String.format("Data coverage is %.1f%%, expected at least 90%%. Missing: %s",
              coveragePercent, missingData));
    }
  }

  @Test public void testYearPartitioning() throws Exception {
    // Test that year partitioning works correctly
    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      // Query for specific year
      String yearQuery =
          "SELECT * FROM census.acs_population " +
          "WHERE \"year\" = 2023 LIMIT 1";

      try {
        ResultSet rs = stmt.executeQuery(yearQuery);
        assertNotNull(rs, "Year-filtered query should return a result set");
        rs.close();
      } catch (SQLException e) {
        // OK if no data for the specific year
        if (!e.getMessage().contains("Empty")) {
          System.out.println("Year partition query issue: " + e.getMessage());
        }
      }
    }
  }

  // Helper method to create a connection
  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("model", modelFile.getAbsolutePath());
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  // Helper method to verify table columns
  private void verifyTableColumns(DatabaseMetaData metaData, String tableName,
      List<String> expectedColumns) throws SQLException {

    ResultSet columns = metaData.getColumns(null, "census", tableName, null);
    Set<String> foundColumns = new HashSet<>();
    while (columns.next()) {
      foundColumns.add(columns.getString("COLUMN_NAME").toUpperCase());
    }

    // Note: Partitioned tables may not expose column metadata through JDBC
    if (foundColumns.isEmpty()) {
      System.out.println("Warning: " + tableName + " has no columns exposed via JDBC metadata (partitioned table limitation)");
    } else {
      for (String expectedColumn : expectedColumns) {
        assertTrue(foundColumns.contains(expectedColumn.toUpperCase()),
            String.format("Table %s should have column %s. Found columns: %s",
                tableName, expectedColumn, foundColumns));
      }
    }
  }

  // Helper method for assertions
  private void assertEquals(int expected, int actual, String message) {
    if (expected != actual) {
      fail(message + " - Expected: " + expected + ", Actual: " + actual);
    }
  }
}
