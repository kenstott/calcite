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

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive validation test for all GovData schemas and tables.
 * Tests ECON, GEO, and SEC schemas to ensure tables are properly visible and queryable.
 */
@Tag("integration")
public class ComprehensiveSchemaValidationTest {

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
  }

  /**
   * ECON schema design: 15 tables that must be discoverable and queryable.
   */
  private static final Set<String> ECON_DESIGNED_TABLES = new HashSet<>(Arrays.asList(
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
      "industry_gdp"
  ));

  /**
   * GEO schema design: 15 tables that must be discoverable and queryable.
   */
  private static final Set<String> GEO_DESIGNED_TABLES = new HashSet<>(Arrays.asList(
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

  @Test
  public void testEconSchemaTablesComprehensive() throws Exception {
    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"econ\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"econ\"," +
        "        \"executionEngine\": \"DUCKDB\"," +
        "        \"autoDownload\": true" +
        "      }" +
        "    }" +
        "  ]" +
        "}";

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("govdata-test", ".json");
    java.nio.file.Files.write(modelFile, modelJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
      try (Statement stmt = conn.createStatement()) {
        int successCount = 0;
        int failureCount = 0;
        List<String> failedTables = new ArrayList<>();

        // Discover all tables in ECON schema
        System.out.println("\n========== ECON SCHEMA TABLE DISCOVERY ==========");
        List<String> econTables = new ArrayList<>();
        try {
          ResultSet tables = stmt.executeQuery(
              "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
              "WHERE \"TABLE_SCHEMA\" = 'econ' " +
              "ORDER BY \"TABLE_NAME\"");
          while (tables.next()) {
            econTables.add(tables.getString("TABLE_NAME"));
          }
          tables.close();
          System.out.println("Found " + econTables.size() + " tables in ECON schema:");
          for (String tableName : econTables) {
            System.out.println("  - " + tableName);
          }
        } catch (SQLException e) {
          System.out.println("Failed to list tables in econ: " + e.getMessage());
        }

        // Test each ECON table
        System.out.println("\n========== TESTING ECON TABLES ==========");
        for (String tableName : econTables) {
          try {
            // Try to query the table
            String query = "SELECT * FROM econ." + tableName + " LIMIT 5";
            ResultSet rs = stmt.executeQuery(query);

            // Count rows returned
            int rowCount = 0;
            while (rs.next() && rowCount < 5) {
              rowCount++;
            }
            rs.close();

            // Also get total count
            ResultSet countRs = stmt.executeQuery("SELECT COUNT(*) FROM econ." + tableName);
            long totalCount = 0;
            if (countRs.next()) {
              totalCount = countRs.getLong(1);
            }
            countRs.close();

            System.out.println("✓ econ." + tableName + " - queryable (" + totalCount + " total rows, sampled " + rowCount + ")");
            successCount++;
          } catch (SQLException e) {
            System.out.println("✗ econ." + tableName + " - FAILED: " + e.getMessage());
            failedTables.add("econ." + tableName);
            failureCount++;
          }
        }

        // Test metadata visibility for a sample table
        System.out.println("\n========== METADATA VERIFICATION ==========");
        if (!econTables.isEmpty()) {
          String sampleTable = econTables.get(0);
          try {
            ResultSet columns = stmt.executeQuery(
                "SELECT \"COLUMN_NAME\", \"DATA_TYPE\" " +
                "FROM information_schema.\"COLUMNS\" " +
                "WHERE \"TABLE_SCHEMA\" = 'econ' " +
                "AND \"TABLE_NAME\" = '" + sampleTable + "' " +
                "ORDER BY \"ORDINAL_POSITION\"");
            System.out.println("Sample columns from econ." + sampleTable + ":");
            int columnCount = 0;
            while (columns.next() && columnCount < 10) {
              String columnName = columns.getString("COLUMN_NAME");
              String columnType = columns.getString("DATA_TYPE");
              System.out.println("  - " + columnName + " (" + columnType + ")");
              columnCount++;
            }
            if (columnCount > 0) {
              System.out.println("✓ Metadata is accessible via information_schema");
            }
            columns.close();
          } catch (SQLException e) {
            System.out.println("Failed to get metadata for econ." + sampleTable + ": " + e.getMessage());
          }
        }

        // Test constraint metadata using SQL queries
        System.out.println("\n========== CONSTRAINT VERIFICATION (SQL) ==========");

        // Query for primary key constraints
        try {
          ResultSet pkResult = stmt.executeQuery(
              "SELECT \"TABLE_NAME\", \"CONSTRAINT_NAME\", \"CONSTRAINT_TYPE\" " +
              "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
              "WHERE \"TABLE_SCHEMA\" = 'econ' " +
              "AND \"CONSTRAINT_TYPE\" = 'PRIMARY KEY' " +
              "ORDER BY \"TABLE_NAME\"");

          int pkCount = 0;
          while (pkResult.next()) {
            String tableName = pkResult.getString("TABLE_NAME");
            String constraintName = pkResult.getString("CONSTRAINT_NAME");
            System.out.println("✓ PRIMARY KEY found: " + tableName + " (" + constraintName + ")");
            pkCount++;
          }
          pkResult.close();

          if (pkCount > 0) {
            System.out.println("Found " + pkCount + " tables with primary keys");
          } else {
            System.out.println("No primary keys found via information_schema.TABLE_CONSTRAINTS");
            System.out.println("Note: Constraints are defined in econ-schema.json but may not be exposed through information_schema");
          }

          // Query for foreign key constraints
          ResultSet fkResult = stmt.executeQuery(
              "SELECT \"TABLE_NAME\", \"CONSTRAINT_NAME\" " +
              "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
              "WHERE \"TABLE_SCHEMA\" = 'econ' " +
              "AND \"CONSTRAINT_TYPE\" = 'FOREIGN KEY' " +
              "ORDER BY \"TABLE_NAME\"");

          int fkCount = 0;
          while (fkResult.next()) {
            String tableName = fkResult.getString("TABLE_NAME");
            String constraintName = fkResult.getString("CONSTRAINT_NAME");
            System.out.println("✓ FOREIGN KEY found: " + tableName + " (" + constraintName + ")");
            fkCount++;
          }
          fkResult.close();

          if (fkCount > 0) {
            System.out.println("Found " + fkCount + " foreign key constraints");
          }

        } catch (SQLException e) {
          System.out.println("Note: information_schema.TABLE_CONSTRAINTS query not supported: " + e.getMessage());
          System.out.println("Constraints are defined in econ-schema.json and loaded into FileSchema");
        }

        // Test against ECON design specification
        Set<String> expectedTables = ECON_DESIGNED_TABLES;
        System.out.println("\n========== EXPECTED vs DISCOVERED ==========");
        System.out.println("ECON design specifies: " + expectedTables.size() + " tables");
        System.out.println("Actually discovered: " + econTables.size() + " tables");

        // Find missing tables
        Set<String> discoveredSet = new HashSet<>(econTables);
        Set<String> missingTables = new HashSet<>(expectedTables);
        missingTables.removeAll(discoveredSet);

        if (!missingTables.isEmpty()) {
          System.out.println("\nMISSING TABLES (designed but not discovered):");
          for (String table : missingTables) {
            System.out.println("  ✗ " + table + " - NOT DISCOVERED");
          }
        }

        // Find unexpected tables
        Set<String> unexpectedTables = new HashSet<>(discoveredSet);
        unexpectedTables.removeAll(expectedTables);

        if (!unexpectedTables.isEmpty()) {
          System.out.println("\nUNEXPECTED TABLES (discovered but not in design):");
          for (String table : unexpectedTables) {
            System.out.println("  ? " + table + " - not in design specification");
          }
        }

        // Report summary
        System.out.println("\n========== TEST SUMMARY ==========");
        System.out.println("Designed tables: " + expectedTables.size());
        System.out.println("Discovered tables: " + econTables.size());
        System.out.println("Successfully queried: " + successCount + " tables");
        System.out.println("Failed to query: " + failureCount + " tables");
        System.out.println("Missing from discovery: " + missingTables.size() + " tables");

        if (!failedTables.isEmpty()) {
          System.out.println("\nFailed to query:");
          for (String table : failedTables) {
            System.out.println("  - " + table);
          }
        }

        // Test passes only if ALL expected tables are discovered AND queryable
        boolean allExpectedDiscovered = missingTables.isEmpty();
        boolean allDiscoveredQueryable = (failureCount == 0);

        double discoveryRate = expectedTables.isEmpty() ? 0 : (double) discoveredSet.size() / expectedTables.size();
        double queryRate = econTables.isEmpty() ? 0 : (double) successCount / econTables.size();

        System.out.println("\nDiscovery rate: " + String.format("%.1f%%", discoveryRate * 100) +
                           " (" + discoveredSet.size() + "/" + expectedTables.size() + ")");
        System.out.println("Query success rate: " + String.format("%.1f%%", queryRate * 100) +
                           " (" + successCount + "/" + econTables.size() + ")");

        if (!allExpectedDiscovered || !allDiscoveredQueryable) {
          String failureMessage = "ECON schema validation FAILED:\n";
          if (!allExpectedDiscovered) {
            failureMessage += "- " + missingTables.size() + " designed tables not discovered\n";
          }
          if (!allDiscoveredQueryable) {
            failureMessage += "- " + failureCount + " discovered tables failed to query\n";
          }
          failureMessage += "ALL designed tables must be both discoverable and queryable.";
          fail(failureMessage);
        } else {
          System.out.println("\n✓ ECON schema validation successful - ALL designed tables discovered and queryable");
        }
      }
    }
  }

  @Test
  public void testGeoSchemaTablesComprehensive() throws Exception {
    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"schemas\": [" +
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

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("govdata-geo-test", ".json");
    java.nio.file.Files.write(modelFile, modelJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
      try (Statement stmt = conn.createStatement()) {
        int successCount = 0;
        int failureCount = 0;
        List<String> failedTables = new ArrayList<>();

        // Discover all tables in GEO schema
        System.out.println("\n========== GEO SCHEMA TABLE DISCOVERY ==========");
        List<String> geoTables = new ArrayList<>();
        try {
          ResultSet tables = stmt.executeQuery(
              "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
              "WHERE \"TABLE_SCHEMA\" = 'geo' " +
              "ORDER BY \"TABLE_NAME\"");
          while (tables.next()) {
            geoTables.add(tables.getString("TABLE_NAME"));
          }
          tables.close();
          System.out.println("Found " + geoTables.size() + " tables in GEO schema:");
          for (String tableName : geoTables) {
            System.out.println("  - " + tableName);
          }
        } catch (SQLException e) {
          System.out.println("Failed to list tables in geo: " + e.getMessage());
        }

        // Test each GEO table
        System.out.println("\n========== TESTING GEO TABLES ==========");
        for (String tableName : geoTables) {
          try {
            // Try to query the table
            String query = "SELECT * FROM geo." + tableName + " LIMIT 5";
            ResultSet rs = stmt.executeQuery(query);

            // Count rows returned
            int rowCount = 0;
            while (rs.next() && rowCount < 5) {
              rowCount++;
            }
            rs.close();

            // Also get total count
            ResultSet countRs = stmt.executeQuery("SELECT COUNT(*) FROM geo." + tableName);
            long totalCount = 0;
            if (countRs.next()) {
              totalCount = countRs.getLong(1);
            }
            countRs.close();

            System.out.println("✓ geo." + tableName + " - queryable (" + totalCount + " total rows, sampled " + rowCount + ")");
            successCount++;
          } catch (SQLException e) {
            System.out.println("✗ geo." + tableName + " - FAILED: " + e.getMessage());
            failedTables.add("geo." + tableName);
            failureCount++;
          }
        }

        // Test metadata visibility for a sample table
        System.out.println("\n========== METADATA VERIFICATION ==========");
        if (!geoTables.isEmpty()) {
          String sampleTable = geoTables.get(0);
          try {
            ResultSet columns = stmt.executeQuery(
                "SELECT \"COLUMN_NAME\", \"DATA_TYPE\" " +
                "FROM information_schema.\"COLUMNS\" " +
                "WHERE \"TABLE_SCHEMA\" = 'geo' " +
                "AND \"TABLE_NAME\" = '" + sampleTable + "' " +
                "ORDER BY \"ORDINAL_POSITION\"");
            System.out.println("Sample columns from geo." + sampleTable + ":");
            int columnCount = 0;
            while (columns.next() && columnCount < 10) {
              String columnName = columns.getString("COLUMN_NAME");
              String columnType = columns.getString("DATA_TYPE");
              System.out.println("  - " + columnName + " (" + columnType + ")");
              columnCount++;
            }
            if (columnCount > 0) {
              System.out.println("✓ Metadata is accessible via information_schema");
            }
            columns.close();
          } catch (SQLException e) {
            System.out.println("Failed to get metadata for geo." + sampleTable + ": " + e.getMessage());
          }
        }

        // Test against GEO design specification
        Set<String> expectedTables = GEO_DESIGNED_TABLES;
        System.out.println("\n========== EXPECTED vs DISCOVERED ==========");
        System.out.println("GEO design specifies: " + expectedTables.size() + " tables");
        System.out.println("Actually discovered: " + geoTables.size() + " tables");

        // Find missing tables
        Set<String> discoveredSet = new HashSet<>(geoTables);
        Set<String> missingTables = new HashSet<>(expectedTables);
        missingTables.removeAll(discoveredSet);

        if (!missingTables.isEmpty()) {
          System.out.println("\nMISSING TABLES (designed but not discovered):");
          for (String table : missingTables) {
            System.out.println("  ✗ " + table + " - NOT DISCOVERED");
          }
        }

        // Find unexpected tables
        Set<String> unexpectedTables = new HashSet<>(discoveredSet);
        unexpectedTables.removeAll(expectedTables);

        if (!unexpectedTables.isEmpty()) {
          System.out.println("\nUNEXPECTED TABLES (discovered but not in design):");
          for (String table : unexpectedTables) {
            System.out.println("  ? " + table + " - not in design specification");
          }
        }

        // Report summary
        System.out.println("\n========== TEST SUMMARY ==========");
        System.out.println("Designed tables: " + expectedTables.size());
        System.out.println("Discovered tables: " + geoTables.size());
        System.out.println("Successfully queried: " + successCount + " tables");
        System.out.println("Failed to query: " + failureCount + " tables");
        System.out.println("Missing from discovery: " + missingTables.size() + " tables");

        if (!failedTables.isEmpty()) {
          System.out.println("\nFailed to query:");
          for (String table : failedTables) {
            System.out.println("  - " + table);
          }
        }

        // Test passes only if ALL expected tables are discovered AND queryable
        boolean allExpectedDiscovered = missingTables.isEmpty();
        boolean allDiscoveredQueryable = (failureCount == 0);

        double discoveryRate = expectedTables.isEmpty() ? 0 : (double) discoveredSet.size() / expectedTables.size();
        double queryRate = geoTables.isEmpty() ? 0 : (double) successCount / geoTables.size();

        System.out.println("\nDiscovery rate: " + String.format("%.1f%%", discoveryRate * 100) +
                           " (" + discoveredSet.size() + "/" + expectedTables.size() + ")");
        System.out.println("Query success rate: " + String.format("%.1f%%", queryRate * 100) +
                           " (" + successCount + "/" + geoTables.size() + ")");

        if (!allExpectedDiscovered || !allDiscoveredQueryable) {
          String failureMessage = "GEO schema validation FAILED:\n";
          if (!allExpectedDiscovered) {
            failureMessage += "- " + missingTables.size() + " designed tables not discovered\n";
          }
          if (!allDiscoveredQueryable) {
            failureMessage += "- " + failureCount + " discovered tables failed to query\n";
          }
          failureMessage += "ALL designed tables must be both discoverable and queryable.";
          fail(failureMessage);
        } else {
          System.out.println("\n✓ GEO schema validation successful - ALL designed tables discovered and queryable");
        }
      }
    }
  }
}
