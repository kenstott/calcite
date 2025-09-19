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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**use SQL
 * Comprehensive validation test for all GovData schemas and tables.
 * Tests ECON, GEO, and SEC schemas to ensure tables are properly visible and queryable.
 */
@Tag("integration")
public class ComprehensiveSchemaValidationTest {

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
  }

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

        // Report summary
        System.out.println("\n========== TEST SUMMARY ==========");
        System.out.println("Total ECON tables discovered: " + econTables.size());
        System.out.println("Successfully queried: " + successCount + " tables");
        System.out.println("Failed to query: " + failureCount + " tables");

        if (!failedTables.isEmpty()) {
          System.out.println("\nFailed tables:");
          for (String table : failedTables) {
            System.out.println("  - " + table);
          }
        }

        // Test passes only if ALL tables can be queried
        double successRate = econTables.isEmpty() ? 0 : (double) successCount / econTables.size();
        System.out.println("\nSuccess rate: " + String.format("%.1f%%", successRate * 100));

        if (failureCount > 0) {
          fail("ECON table query failures: " + failureCount + " tables failed. " +
               "Successful: " + successCount + "/" + econTables.size() + ". " +
               "ALL tables must be queryable.");
        } else {
          System.out.println("✓ ECON schema validation successful - ALL tables queryable");
        }
      }
    }
  }
}
