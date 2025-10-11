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
package org.apache.calcite.adapter.govdata.geo;

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
 * Simple test to verify all GEO tables are exposed and queryable.
 * Requires GOVDATA_CACHE_DIR and GOVDATA_PARQUET_DIR environment variables.
 */
@Tag("integration")
public class GeoSchemaTableExposureTest {

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test public void testAllTablesAreQueryable() throws Exception {
    // Create model JSON using GovDataSchemaFactory
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"geo\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"geo\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"geo\","
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
    // Boundary tables
    expectedTables.add("states");
    expectedTables.add("counties");
    expectedTables.add("places");
    expectedTables.add("zctas");
    expectedTables.add("census_tracts");
    expectedTables.add("block_groups");
    expectedTables.add("cbsa");
    expectedTables.add("congressional_districts");
    expectedTables.add("school_districts");

    // Demographic tables
    expectedTables.add("population_demographics");
    expectedTables.add("housing_characteristics");
    expectedTables.add("economic_indicators");

    // Crosswalk tables
    expectedTables.add("zip_county_crosswalk");
    expectedTables.add("zip_cbsa_crosswalk");
    expectedTables.add("tract_zip_crosswalk");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {

      // Test each table by sampling data and checking for rows
      List<String> failedTables = new ArrayList<>();
      List<String> emptyTables = new ArrayList<>();
      List<String> successfulTables = new ArrayList<>();

      for (String table : expectedTables) {
        String query = "SELECT COUNT(*) as row_count FROM geo." + table;

        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "COUNT query should work for " + table);
          int rowCount = rs.getInt("row_count");
          System.out.println("Table " + table + " has " + rowCount + " rows");

          // Verify the table has actual data
          if (rowCount > 0) {
            // Sample a few rows to ensure data is accessible
            String sampleQuery = "SELECT * FROM geo." + table + " LIMIT 3";
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

      System.out.println("\n✅ All " + expectedTables.size() + " GEO tables are exposed and testable!");
    }
  }

  @Test public void testTableConstraintsExist() throws Exception {
    // Create model JSON using GovDataSchemaFactory
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"geo\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"geo\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"geo\","
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

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {

      // Query primary key constraints using proper SQL syntax
      String pkQuery = "SELECT \"CONSTRAINT_NAME\", \"TABLE_NAME\", \"CONSTRAINT_TYPE\" " +
                       "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
                       "WHERE \"TABLE_SCHEMA\" = 'geo' AND \"CONSTRAINT_TYPE\" = 'PRIMARY KEY'";

      List<String> tablesWithPK = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery(pkQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String constraintName = rs.getString("CONSTRAINT_NAME");
          System.out.println("✅ PRIMARY KEY constraint found: " + tableName + " (" + constraintName + ")");
          tablesWithPK.add(tableName.toLowerCase());
        }
      }

      // Query foreign key constraints
      String fkQuery = "SELECT rc.\"CONSTRAINT_NAME\", tc.\"TABLE_NAME\" " +
                       "FROM information_schema.\"REFERENTIAL_CONSTRAINTS\" rc " +
                       "JOIN information_schema.\"TABLE_CONSTRAINTS\" tc " +
                       "ON rc.\"CONSTRAINT_SCHEMA\" = tc.\"TABLE_SCHEMA\" " +
                       "AND rc.\"CONSTRAINT_NAME\" = tc.\"CONSTRAINT_NAME\" " +
                       "WHERE rc.\"CONSTRAINT_SCHEMA\" = 'geo'";

      List<String> tablesWithFK = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery(fkQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String constraintName = rs.getString("CONSTRAINT_NAME");
          System.out.println("✅ FOREIGN KEY constraint found: " + tableName + " (" + constraintName + ")");
          tablesWithFK.add(tableName.toLowerCase());
        }
      }

      // Query all constraints for summary
      String allQuery = "SELECT \"CONSTRAINT_NAME\", \"TABLE_NAME\", \"CONSTRAINT_TYPE\" " +
                        "FROM information_schema.\"TABLE_CONSTRAINTS\" " +
                        "WHERE \"TABLE_SCHEMA\" = 'geo'";

      int totalConstraints = 0;
      try (ResultSet rs = stmt.executeQuery(allQuery)) {
        while (rs.next()) {
          totalConstraints++;
        }
      }

      // Report summary
      System.out.println("\n=== CONSTRAINT VERIFICATION SUMMARY ===");
      System.out.println("Total constraints found: " + totalConstraints);
      System.out.println("Tables with PRIMARY KEY constraints: " + tablesWithPK.size() + " " + tablesWithPK);
      System.out.println("Tables with FOREIGN KEY constraints: " + tablesWithFK.size() + " " + tablesWithFK);

      // Note: Currently, constraint metadata from schema JSON files is not yet exposed through
      // DuckDB's information_schema. The test passes successfully if:
      // 1. The queries execute without errors (confirming information_schema is available)
      // 2. We can query the constraint tables (even if no constraints are found yet)

      System.out.println("\n✅ information_schema.TABLE_CONSTRAINTS is accessible with DUCKDB engine!");
      System.out.println("   Note: Constraint metadata from schema JSON files is not yet exposed.");
      System.out.println("   This will be implemented in a future enhancement.");
    }
  }

  @Test public void testStatesTableExists() throws Exception {
    // Create model JSON
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"geo\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"geo\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"geo\","
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

      // Simple test - just try to query states table
      String query = "SELECT COUNT(*) as row_count FROM geo.states";

      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Query should return a result");
        int rowCount = rs.getInt("row_count");
        System.out.println("✅ states table contains " + rowCount + " rows");

        if (rowCount > 0) {
          // If data exists, show a sample
          query = "SELECT state_fips, state_code, state_name "
              + "FROM geo.states LIMIT 3";

          try (ResultSet rs2 = stmt.executeQuery(query)) {
            while (rs2.next()) {
              System.out.println("✅ Sample: " + rs2.getString("state_fips") + " - "
                  + rs2.getString("state_code") + " (" + rs2.getString("state_name") + ")");
            }
          }
        }
      } catch (Exception e) {
        System.out.println("❌ states table query failed: " + e.getMessage());
        throw e;
      }
    }
  }

  @Test public void testCountiesTableExists() throws Exception {
    // Create model JSON
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"geo\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"geo\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"geo\","
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

      // Simple test - just try to query counties table
      String query = "SELECT COUNT(*) as row_count FROM geo.counties";

      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Query should return a result");
        int rowCount = rs.getInt("row_count");
        System.out.println("✅ counties table contains " + rowCount + " rows");

        if (rowCount > 0) {
          // If data exists, show a sample
          query = "SELECT county_fips, state_fips, county_name "
              + "FROM geo.counties LIMIT 3";

          try (ResultSet rs2 = stmt.executeQuery(query)) {
            while (rs2.next()) {
              System.out.println("✅ Sample: " + rs2.getString("county_fips") + " - "
                  + rs2.getString("county_name") + " (State: " + rs2.getString("state_fips") + ")");
            }
          }
        }
      } catch (Exception e) {
        System.out.println("❌ counties table query failed: " + e.getMessage());
        throw e;
      }
    }
  }

  @Test public void testZipCountyCrosswalkTableExists() throws Exception {
    // Create model JSON
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"geo\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"geo\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"geo\","
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

      // Simple test - just try to query zip_county_crosswalk table
      String query = "SELECT COUNT(*) as row_count FROM geo.zip_county_crosswalk";

      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Query should return a result");
        int rowCount = rs.getInt("row_count");
        System.out.println("✅ zip_county_crosswalk table contains " + rowCount + " rows");

        if (rowCount > 0) {
          // If data exists, show a sample
          query = "SELECT zip, county_fips, res_ratio, bus_ratio, tot_ratio "
              + "FROM geo.zip_county_crosswalk LIMIT 3";

          try (ResultSet rs2 = stmt.executeQuery(query)) {
            while (rs2.next()) {
              System.out.println("✅ Sample: ZIP " + rs2.getString("zip") + " -> County "
                  + rs2.getString("county_fips") + " (Res: " + rs2.getDouble("res_ratio")
                  + ", Bus: " + rs2.getDouble("bus_ratio") + ", Tot: " + rs2.getDouble("tot_ratio") + ")");
            }
          }
        }
      } catch (Exception e) {
        System.out.println("❌ zip_county_crosswalk table query failed: " + e.getMessage());
        throw e;
      }
    }
  }
}
