/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify Oracle lexical settings work correctly with case-insensitive information_schema.
 */
@Tag("unit")
public class OracleLexicalSettingsTest {

  @Test void testOracleLexicalSettings() throws SQLException {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    // Inline model with file schema
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'SALES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'SALES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + FileAdapterTests.resourcePath("sales") + "',\n"
        + "        table_name_casing: 'UPPER',\n"
        + "        column_name_casing: 'LOWER',\n"
        + "        ephemeralCache: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    info.put("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test 1: Verify Oracle lexical settings are active
      // This should work - uppercase table and column names
      ResultSet rs1 = statement.executeQuery("SELECT \"name\" FROM \"SALES\".\"DEPTS\" WHERE \"deptno\" = 10");
      assertTrue(rs1.next());
      assertEquals("Sales", rs1.getString("name"));
      rs1.close();

      // Test 2: Verify case-insensitive information_schema access
      // All of these should work regardless of case
      ResultSet rs2 = statement.executeQuery("SELECT COUNT(*) FROM information_schema.TABLES");
      assertTrue(rs2.next());
      assertTrue(rs2.getInt(1) > 0);
      rs2.close();

      ResultSet rs3 = statement.executeQuery("SELECT COUNT(*) FROM information_schema.tables");
      assertTrue(rs3.next());
      assertTrue(rs3.getInt(1) > 0);
      rs3.close();

      ResultSet rs4 = statement.executeQuery("SELECT COUNT(*) FROM information_schema.Tables");
      assertTrue(rs4.next());
      assertTrue(rs4.getInt(1) > 0);
      rs4.close();

      // Test 3: Verify case-insensitive pg_catalog access
      ResultSet rs5 = statement.executeQuery("SELECT COUNT(*) FROM pg_catalog.pg_namespace");
      assertTrue(rs5.next());
      assertTrue(rs5.getInt(1) > 0);
      rs5.close();

      ResultSet rs6 = statement.executeQuery("SELECT COUNT(*) FROM pg_catalog.PG_NAMESPACE");
      assertTrue(rs6.next());
      assertTrue(rs6.getInt(1) > 0);
      rs6.close();

      // Test 4: Verify schema and table access via SQL instead of deprecated methods
      ResultSet schemaTest = statement.executeQuery("SELECT COUNT(*) FROM \"SALES\".\"DEPTS\"");
      assertTrue(schemaTest.next());
      assertTrue(schemaTest.getInt(1) > 0);
      schemaTest.close();

      ResultSet tableTest = statement.executeQuery("SELECT COUNT(*) FROM \"SALES\".\"EMPS\"");
      assertTrue(tableTest.next());
      assertTrue(tableTest.getInt(1) > 0);
      tableTest.close();
    }
  }

  @Test void testCaseSensitivityDemo() throws SQLException {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'SALES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'SALES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + FileAdapterTests.resourcePath("sales") + "',\n"
        + "        table_name_casing: 'UPPER',\n"
        + "        column_name_casing: 'LOWER',\n"
        + "        ephemeralCache: true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    info.put("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // This should work - correct case with schema
      ResultSet rs1 = statement.executeQuery("SELECT COUNT(*) FROM \"SALES\".\"DEPTS\"");
      assertTrue(rs1.next());
      assertEquals(3, rs1.getInt(1));
      rs1.close();

      // This should fail - incorrect case due to Oracle lexical rules
      try {
        statement.executeQuery("SELECT COUNT(*) FROM \"SALES\".depts");
        // If we get here, the test failed because the query should have thrown an exception
        throw new AssertionError("Expected SQLException for lowercase table name 'depts'");
      } catch (SQLException e) {
        // Expected - should contain "not found" message
        assertTrue(e.getMessage().contains("not found") || e.getMessage().contains("depts"));
      }

      // But information_schema should still be case-insensitive
      ResultSet rs3 = statement.executeQuery("SELECT COUNT(*) FROM information_schema.tables");
      assertTrue(rs3.next());
      assertTrue(rs3.getInt(1) > 0);
      rs3.close();

      ResultSet rs4 = statement.executeQuery("SELECT COUNT(*) FROM information_schema.TABLES");
      assertTrue(rs4.next());
      assertTrue(rs4.getInt(1) > 0);
      rs4.close();
    }
  }
}
