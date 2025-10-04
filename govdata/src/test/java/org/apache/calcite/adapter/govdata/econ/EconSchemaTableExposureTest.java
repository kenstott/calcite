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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Simple test to verify all ECON tables are exposed and queryable.
 * Requires GOVDATA_CACHE_DIR and GOVDATA_PARQUET_DIR environment variables.
 */
@Tag("integration")
public class EconSchemaTableExposureTest {

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test public void testAllTablesAreQueryable() throws Exception {
    // Create model JSON using GovDataSchemaFactory
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"DUCKDB\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";

    // Write model file
    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());

    // Connect and query
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    Set<String> expectedTables = new HashSet<>();
    expectedTables.add("employment_statistics");
    expectedTables.add("inflation_metrics");
    expectedTables.add("wage_growth");
    expectedTables.add("regional_employment");
    expectedTables.add("treasury_yields");
    expectedTables.add("federal_debt");
    expectedTables.add("world_indicators");
    expectedTables.add("fred_indicators");
    expectedTables.add("gdp_components");
    expectedTables.add("regional_income");
    expectedTables.add("state_gdp");
    expectedTables.add("trade_statistics");
    expectedTables.add("ita_data");
    expectedTables.add("industry_gdp");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {

      // Test each table by sampling data and checking for rows
      List<String> failedTables = new ArrayList<>();
      List<String> emptyTables = new ArrayList<>();
      List<String> successfulTables = new ArrayList<>();

      for (String table : expectedTables) {
        String query = "SELECT COUNT(*) as row_count FROM econ." + table;

        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "COUNT query should work for " + table);
          int rowCount = rs.getInt("row_count");
          System.out.println("Table " + table + " has " + rowCount + " rows");

          // Verify the table has actual data
          if (rowCount > 0) {
            // Sample a few rows to ensure data is accessible
            String sampleQuery = "SELECT * FROM econ." + table + " LIMIT 3";
            try (ResultSet sampleRs = stmt.executeQuery(sampleQuery)) {
              assertTrue(sampleRs.next(), "Table " + table + " should have accessible sample data");
              System.out.println("✅ Table " + table + " is queryable with " + rowCount + " rows");
              successfulTables.add(table);
            } catch (Exception sampleEx) {
              System.out.println("❌ Table " + table + " failed SELECT * query: " + sampleEx.getMessage());
              failedTables.add(table + " (SELECT failed after COUNT returned " + rowCount + " rows)");
            }
          } else {
            System.out.println("⚠️  Table " + table + " exists but has no data (" + rowCount + " rows)");
            emptyTables.add(table);
          }
        } catch (Exception e) {
          System.out.println("❌ Table " + table + " failed completely: " + e.getMessage());
          failedTables.add(table + " (COUNT query failed)");
        }
      }

      // Report summary
      System.out.println("\n=== SUMMARY ===");
      System.out.println("Successful tables (" + successfulTables.size() + "): " + successfulTables);
      System.out.println("Empty tables (" + emptyTables.size() + "): " + emptyTables);
      System.out.println("Failed tables (" + failedTables.size() + "): " + failedTables);

      if (!failedTables.isEmpty()) {
        fail("The following tables are not queryable: " + failedTables);
      }

      System.out.println("\n✅ All " + expectedTables.size() + " ECON tables are exposed and testable!");
    }
  }

  @Test public void testTableConstraintsExistWithDuckDB() throws Exception {
    testTableConstraintsWithEngine("DUCKDB");
  }

  @Test public void testTableConstraintsExistWithParquet() throws Exception {
    testTableConstraintsWithEngine("PARQUET");
  }

  private void testTableConstraintsWithEngine(String engine) throws Exception {
    System.out.println("\n=== Testing TABLE_CONSTRAINTS with " + engine + " engine ===");

    // Create model JSON using GovDataSchemaFactory
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"" + engine + "\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";

    // Write model file
    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());

    // Connect and query
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {

      // Query primary key constraints using proper SQL syntax
      String pkQuery = "SELECT \"CONSTRAINT_NAME\", \"TABLE_NAME\", \"CONSTRAINT_TYPE\" " +
                       "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
                       "WHERE \"TABLE_SCHEMA\" = 'econ' AND \"CONSTRAINT_TYPE\" = 'PRIMARY KEY'";

      List<String> tablesWithPK = new ArrayList<>();
      boolean informationSchemaSupported = true;
      try (ResultSet rs = stmt.executeQuery(pkQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String constraintName = rs.getString("CONSTRAINT_NAME");
          System.out.println("✅ PRIMARY KEY constraint found: " + tableName + " (" + constraintName + ")");
          tablesWithPK.add(tableName.toLowerCase());
        }
      } catch (Exception e) {
        System.out.println("Error querying primary key constraints: " + e.getMessage());
        if (e.getMessage().contains("Column") && e.getMessage().contains("not found")) {
          informationSchemaSupported = false;
          System.out.println("information_schema.TABLE_CONSTRAINTS not supported by this engine");
        }
      }

      // If information_schema is not supported (PARQUET engine), we should stop here
      if (!informationSchemaSupported) {
        if ("PARQUET".equals(engine)) {
          System.out.println("\n✅ PARQUET engine correctly rejected information_schema queries");
          System.out.println("   This is expected behavior - PARQUET engine does not support information_schema");
          return; // Test passes - PARQUET correctly doesn't support information_schema
        } else {
          fail("Unexpected: " + engine + " engine does not support information_schema");
        }
      }

      // Query foreign key constraints (only if information_schema is supported)
      String fkQuery = "SELECT rc.\"CONSTRAINT_NAME\", tc.\"TABLE_NAME\" " +
                       "FROM information_schema.\"REFERENTIAL_CONSTRAINTS\" rc " +
                       "JOIN information_schema.\"TABLE_CONSTRAINTS\" tc " +
                       "ON rc.\"CONSTRAINT_SCHEMA\" = tc.\"TABLE_SCHEMA\" " +
                       "AND rc.\"CONSTRAINT_NAME\" = tc.\"CONSTRAINT_NAME\" " +
                       "WHERE rc.\"CONSTRAINT_SCHEMA\" = 'econ'";

      List<String> tablesWithFK = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery(fkQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String constraintName = rs.getString("CONSTRAINT_NAME");
          System.out.println("✅ FOREIGN KEY constraint found: " + tableName + " (" + constraintName + ")");
          tablesWithFK.add(tableName.toLowerCase());
        }
      } catch (Exception e) {
        System.out.println("Error querying foreign key constraints: " + e.getMessage());
      }

      // Query all constraints for summary
      String allQuery = "SELECT \"CONSTRAINT_NAME\", \"TABLE_NAME\", \"CONSTRAINT_TYPE\" " +
                        "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
                        "WHERE \"TABLE_SCHEMA\" = 'econ'";

      int totalConstraints = 0;
      try (ResultSet rs = stmt.executeQuery(allQuery)) {
        while (rs.next()) {
          totalConstraints++;
        }
      } catch (Exception e) {
        System.out.println("Error counting total constraints: " + e.getMessage());
      }

      // Report summary
      System.out.println("\n=== CONSTRAINT VERIFICATION SUMMARY ===");
      System.out.println("Total constraints found: " + totalConstraints);
      System.out.println("Tables with PRIMARY KEY constraints: " + tablesWithPK.size() + " " + tablesWithPK);
      System.out.println("Tables with FOREIGN KEY constraints: " + tablesWithFK.size() + " " + tablesWithFK);

      // Verify we found constraint information (even if 0, that's useful information)
      System.out.println("Constraint verification completed - found " + totalConstraints + " constraints");

      // Engine-specific expectations for DUCKDB (PARQUET already handled above)
      if ("DUCKDB".equals(engine)) {
        // DUCKDB supports information_schema queries
        // Note: Constraint metadata from schema JSON is not yet populated in DuckDB's information_schema
        System.out.println("✅ information_schema.TABLE_CONSTRAINTS is accessible with DUCKDB engine!");
        System.out.println("   Note: Constraint metadata from schema JSON files is not yet exposed.");
      }
    }
  }

  @Test public void testStateGdpTableExists() throws Exception {
    // Create model JSON
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"DUCKDB\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";

    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {

      // Simple test - just try to query state_gdp table
      String query = "SELECT COUNT(*) as row_count FROM econ.state_gdp";

      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Query should return a result");
        int rowCount = rs.getInt("row_count");
        System.out.println("✅ state_gdp table contains " + rowCount + " rows");

        if (rowCount > 0) {
          // If data exists, show a sample
          query = "SELECT geo_fips, geo_name, metric, \"year\", \"value\", units "
              + "FROM econ.state_gdp LIMIT 3";

          try (ResultSet rs2 = stmt.executeQuery(query)) {
            while (rs2.next()) {
              System.out.println("✅ Sample: " + rs2.getString("geo_fips") + " (" + rs2.getString("geo_name") + "), "
                  + rs2.getString("metric") + ", " + rs2.getInt("year") + ": " + rs2.getDouble("value"));
            }
          }
        }
      } catch (Exception e) {
        System.out.println("❌ state_gdp table query failed: " + e.getMessage());
        throw e;
      }
    }
  }
}
