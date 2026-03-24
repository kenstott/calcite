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
package org.apache.calcite.adapter.file.config;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Consolidated tests for table and column name casing configuration in the
 * file adapter. Covers UPPER, LOWER, UNCHANGED, mixed, default SMART_CASING,
 * snake_case configuration keys, column-only casing, and case-sensitive access.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class CasingTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasingTest.class);

  @TempDir
  Path tempDir;

  @AfterEach
  public void tearDown() {
    Sources.clearFileCache();
  }

  // -- Tests ----------------------------------------------------------------

  /**
   * UPPER/UPPER casing: table "TestFile" becomes "TESTFILE",
   * columns "ID","Name" become "ID","NAME".
   */
  @Test void testUppercaseTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModel("UPPER", "UPPER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (UPPER/UPPER): {}", tableNames);
      assertTrue(tableNames.contains("TESTFILE"),
          "Expected TESTFILE in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"ID\", \"NAME\" FROM \"TESTFILE\" ORDER BY \"ID\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("ID"));
        assertEquals("Alice", rs.getString("NAME"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("ID"));
        assertEquals("Bob", rs.getString("NAME"));
      }
    }
  }

  /**
   * LOWER/LOWER casing: table "TestFile" becomes "testfile",
   * columns "ID","Name" become "id","name".
   */
  @Test void testLowercaseTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModel("LOWER", "LOWER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (LOWER/LOWER): {}", tableNames);
      assertTrue(tableNames.contains("testfile"),
          "Expected testfile in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"id\", \"name\" FROM \"testfile\" ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("Bob", rs.getString("name"));
      }
    }
  }

  /**
   * UNCHANGED/UNCHANGED casing: table "TestFile" stays "TestFile",
   * columns "ID","Name" stay "ID","Name".
   */
  @Test void testUnchangedTableAndColumnNames() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModel("UNCHANGED", "UNCHANGED"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (UNCHANGED/UNCHANGED): {}", tableNames);
      assertTrue(tableNames.contains("TestFile"),
          "Expected TestFile in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"ID\", \"Name\" FROM \"TestFile\" ORDER BY \"ID\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("ID"));
        assertEquals("Alice", rs.getString("Name"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("ID"));
        assertEquals("Bob", rs.getString("Name"));
      }
    }
  }

  /**
   * Mixed casing: UPPER tables, LOWER columns.
   * Table "TestFile" becomes "TESTFILE", columns become lowercase.
   */
  @Test void testMixedTableAndColumnCasing() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModel("UPPER", "LOWER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (UPPER/LOWER): {}", tableNames);
      assertTrue(tableNames.contains("TESTFILE"),
          "Expected TESTFILE in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"id\", \"name\" FROM \"TESTFILE\" ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("Bob", rs.getString("name"));
      }
    }
  }

  /**
   * Default SMART_CASING with LEX=ORACLE and unquotedCasing=TO_LOWER.
   * "TestFile" is converted to "test_file" by SMART_CASING.
   */
  @Test void testDefaultSmartCasingBehavior() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModelNoCasing());

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model
            + ";lex=ORACLE;unquotedCasing=TO_LOWER")) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (SMART_CASING default): {}", tableNames);
      assertTrue(tableNames.contains("test_file"),
          "Expected test_file in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT id, name FROM test_file ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
      }
    }
  }

  /**
   * Verifies that snake_case configuration keys (table_name_casing,
   * column_name_casing) produce the same result as camelCase
   * (tableNameCasing, columnNameCasing).
   */
  @Test void testSnakeCaseConfigurationKeys() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModelSnakeCase("UPPER", "UPPER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (snake_case keys UPPER/UPPER): {}", tableNames);
      assertTrue(tableNames.contains("TESTFILE"),
          "Expected TESTFILE in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"ID\", \"NAME\" FROM \"TESTFILE\" ORDER BY \"ID\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("ID"));
        assertEquals("Alice", rs.getString("NAME"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("ID"));
        assertEquals("Bob", rs.getString("NAME"));
      }
    }
  }

  /**
   * Tests column casing independently of table casing.
   * With LOWER column casing, "Customer_ID" becomes "customer_id".
   */
  @Test void testColumnNameCasing() throws Exception {
    File csvFile = new File(tempDir.toFile(), "customers.csv");
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("Customer_ID:int,First_Name:string,Last_Name:string\n");
      writer.write("1,John,Doe\n");
      writer.write("2,Jane,Smith\n");
    }

    // Only set column casing; leave table casing at default
    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModelColumnCasingOnly("LOWER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model
            + ";lex=ORACLE;unquotedCasing=TO_LOWER;caseSensitive=true")) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("TEST");

      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Tables (column LOWER): {}", tableNames);

      // Find the table name (SMART_CASING default may transform it)
      String tableName = null;
      for (String name : tableNames) {
        if (name.toLowerCase().contains("customer")) {
          tableName = name;
          break;
        }
      }
      LOGGER.debug("Resolved table name: {}", tableName);
      assertTrue(tableName != null, "Expected a customers table in " + tableNames);

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"customer_id\", \"first_name\" FROM \"" + tableName
                   + "\" ORDER BY \"customer_id\"")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("customer_id"));
        assertEquals("John", rs.getString("first_name"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("customer_id"));
        assertEquals("Jane", rs.getString("first_name"));
      }
    }
  }

  /**
   * With caseSensitive=true and UPPER table casing, using a wrong-case
   * quoted table name must fail.
   */
  @Test void testCaseSensitiveTableAccess() throws Exception {
    createTestCsvFile(new File(tempDir.toFile(), "TestFile.csv"));

    String model = BaseFileTest.addEphemeralCacheToModel(
        buildModel("UPPER", "UPPER"));

    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=inline:" + model
            + ";lex=ORACLE;unquotedCasing=TO_LOWER"
            + ";quotedCasing=UNCHANGED;caseSensitive=true")) {
      // Correct case should succeed
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"ID\" FROM \"TESTFILE\"")) {
        assertTrue(rs.next(), "Query with correct case should return rows");
        LOGGER.debug("Correct case query returned id={}", rs.getInt("ID"));
      }

      // Wrong case (lowercase quoted) should fail
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT \"ID\" FROM \"testfile\"");
        fail("Expected query to fail with wrong-case quoted table name");
      } catch (SQLException e) {
        LOGGER.debug("Expected error for wrong case: {}", e.getMessage());
        assertTrue(
            e.getMessage().contains("testfile")
                || e.getMessage().contains("Object 'testfile' not found"),
            "Error should mention the wrong-case table name: " + e.getMessage());
      }
    }
  }

  // -- Helpers --------------------------------------------------------------

  /**
   * Creates a simple CSV test file with typed columns:
   * ID (int), Name (string), Value (double).
   */
  private void createTestCsvFile(File file) throws IOException {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("ID:int,Name:string,Value:double\n");
      writer.write("1,Alice,10.5\n");
      writer.write("2,Bob,20.3\n");
      writer.write("3,Charlie,30.7\n");
    }
  }

  /**
   * Builds a model JSON with explicit tableNameCasing and columnNameCasing
   * (camelCase keys).
   */
  private String buildModel(String tableNameCasing, String columnNameCasing) {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\",\n"
        + "        \"tableNameCasing\": \"" + tableNameCasing + "\",\n"
        + "        \"columnNameCasing\": \"" + columnNameCasing + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  /**
   * Builds a model JSON with snake_case configuration keys
   * (table_name_casing, column_name_casing).
   */
  private String buildModelSnakeCase(String tableNameCasing,
      String columnNameCasing) {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\",\n"
        + "        \"table_name_casing\": \"" + tableNameCasing + "\",\n"
        + "        \"column_name_casing\": \"" + columnNameCasing + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  /**
   * Builds a model JSON with no casing configuration (uses defaults).
   */
  private String buildModelNoCasing() {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  /**
   * Builds a model JSON with only columnNameCasing set (table casing left
   * at default SMART_CASING).
   */
  private String buildModelColumnCasingOnly(String columnNameCasing) {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\",\n"
        + "        \"columnNameCasing\": \"" + columnNameCasing + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
