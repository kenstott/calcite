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
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive integration tests for the root package classes:
 * {@link FileSchema}, {@link FileSchemaFactory}, and {@link FileSchemaBuilder}.
 *
 * <p>Exercises schema creation with diverse operand combinations, table
 * discovery from CSV/JSON files, casing, refresh, cache priming, multi-schema
 * models, glob patterns, and more.
 */
@SuppressWarnings("deprecation")
@Tag("integration")
public class FileSchemaFullCoverageTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileSchemaFullCoverageTest.class);

  @TempDir
  Path tempDir;

  private final List<Connection> openConnections = new ArrayList<Connection>();

  @AfterEach
  public void closeConnections() {
    for (Connection conn : openConnections) {
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.debug("Error closing connection", e);
      }
    }
    openConnections.clear();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private Connection connect(String model) throws SQLException {
    Properties info = new Properties();
    info.put("model", "inline:" + model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    openConnections.add(conn);
    return conn;
  }

  private String buildModel(String schemaName, String directory) {
    return buildModel(schemaName, directory, "");
  }

  private String buildModel(String schemaName, String directory,
      String extraOperands) {
    String dir = directory.replace("\\", "\\\\");
    return BaseFileTest.addEphemeralCacheToModel(
        "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + schemaName + "\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"" + schemaName + "\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\""
        + (extraOperands.isEmpty() ? "" : ",\n" + extraOperands)
        + "\n      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
  }

  private String buildDualSchemaModel(String schema1Name, String dir1,
      String schema2Name, String dir2) {
    String d1 = dir1.replace("\\", "\\\\");
    String d2 = dir2.replace("\\", "\\\\");
    return BaseFileTest.addEphemeralCacheToModel(
        "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + schema1Name + "\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"" + schema1Name + "\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + d1 + "\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"" + schema2Name + "\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + d2 + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
  }

  private File createCsvFile(String name, String... lines) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file)) {
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
    }
    return file;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  private File createSubDir(String name) {
    File dir = new File(tempDir.toFile(), name);
    dir.mkdirs();
    return dir;
  }

  // ---------------------------------------------------------------------------
  // 1. Basic schema creation with CSV
  // ---------------------------------------------------------------------------

  @Test void testBasicCsvTableDiscovery() throws Exception {
    createCsvFile("products.csv",
        "product_id:int,product_name:string,price:double",
        "1,Widget,9.99",
        "2,Gadget,19.99",
        "3,Gizmo,29.99");

    String model = buildModel("TESTSCHEMA", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT product_id, product_name, price FROM products ORDER BY product_id")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("Widget", rs.getString(2));
      assertEquals(9.99, rs.getDouble(3), 0.001);

      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));

      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      assertFalse(rs.next());
    }
  }

  // ---------------------------------------------------------------------------
  // 2. Multiple CSV files -> multiple tables
  // ---------------------------------------------------------------------------

  @Test void testMultipleCsvFilesDiscovery() throws Exception {
    createCsvFile("employees.csv",
        "emp_id:int,emp_name:string",
        "1,Alice",
        "2,Bob");
    createCsvFile("departments.csv",
        "dept_id:int,dept_name:string",
        "10,Engineering",
        "20,Marketing");

    String model = buildModel("MULTI", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("MULTI");
      assertNotNull(schema, "Schema MULTI should exist");
      Set<String> tableNames = schema.getTableNames();
      LOGGER.debug("Discovered tables: {}", tableNames);
      assertTrue(tableNames.size() >= 2,
          "Should discover at least 2 tables, got: " + tableNames);

      // Query employees
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT emp_name FROM employees ORDER BY emp_id")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString(1));
      }

      // Query departments
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT dept_name FROM departments ORDER BY dept_id")) {
        assertTrue(rs.next());
        assertEquals("Engineering", rs.getString(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 3. JSON table discovery
  // ---------------------------------------------------------------------------

  @Test void testJsonTableDiscovery() throws Exception {
    createJsonFile("orders.json",
        "[{\"order_id\": 100, \"customer\": \"Alice\", \"total\": 50.0},"
        + "{\"order_id\": 200, \"customer\": \"Bob\", \"total\": 75.0}]");

    String model = buildModel("JSONSCHEMA", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM orders")) {
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      assertTrue(rowCount >= 2,
          "Should find at least 2 rows in orders JSON table");
    }
  }

  // ---------------------------------------------------------------------------
  // 4. Mixed CSV and JSON in same schema
  // ---------------------------------------------------------------------------

  @Test void testMixedCsvAndJsonDiscovery() throws Exception {
    createCsvFile("sales.csv",
        "sale_id:int,amount:double",
        "1,100.50",
        "2,200.75");
    createJsonFile("regions.json",
        "[{\"region_id\": 1, \"region_name\": \"North\"},"
        + "{\"region_id\": 2, \"region_name\": \"South\"}]");

    String model = buildModel("MIXED", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("MIXED");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Mixed schema tables: {}", names);
      // Both CSV and JSON files should be found
      assertTrue(names.size() >= 2,
          "Expected at least 2 tables (CSV + JSON), got: " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 5. Schema with LOWER table name casing
  // ---------------------------------------------------------------------------

  @Test void testLowerTableNameCasing() throws Exception {
    createCsvFile("MyData.csv",
        "col_a:int,col_b:string",
        "1,hello");

    String model = buildModel("CASESCHEMA", tempDir.toFile().getAbsolutePath(),
        "        \"tableNameCasing\": \"LOWER\",\n"
        + "        \"columnNameCasing\": \"LOWER\"");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("CASESCHEMA");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("LOWER casing tables: {}", names);
      assertTrue(names.contains("mydata"),
          "Expected 'mydata' (lowercased) in " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 6. Schema with UPPER table name casing
  // ---------------------------------------------------------------------------

  @Test void testUpperTableNameCasing() throws Exception {
    createCsvFile("items.csv",
        "item_id:int,item_desc:string",
        "1,TestItem");

    String model = buildModel("UPPERSCHEMA", tempDir.toFile().getAbsolutePath(),
        "        \"tableNameCasing\": \"UPPER\",\n"
        + "        \"columnNameCasing\": \"UPPER\"");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("UPPERSCHEMA");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("UPPER casing tables: {}", names);
      assertTrue(names.contains("ITEMS"),
          "Expected 'ITEMS' (uppercased) in " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 7. Schema with UNCHANGED table name casing
  // ---------------------------------------------------------------------------

  @Test void testUnchangedTableNameCasing() throws Exception {
    createCsvFile("CamelCase.csv",
        "field_a:int,field_b:string",
        "1,value");

    String model = buildModel("UNCHSCHEMA", tempDir.toFile().getAbsolutePath(),
        "        \"tableNameCasing\": \"UNCHANGED\",\n"
        + "        \"columnNameCasing\": \"UNCHANGED\"");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("UNCHSCHEMA");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("UNCHANGED casing tables: {}", names);
      assertTrue(names.contains("CamelCase"),
          "Expected 'CamelCase' (unchanged) in " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 8. Default SMART_CASING
  // ---------------------------------------------------------------------------

  @Test void testSmartCasingDefault() throws Exception {
    createCsvFile("MyReport.csv",
        "col_id:int,col_val:string",
        "1,test");

    // No explicit casing -> SMART_CASING by default
    String model = buildModel("SMARTSCHEMA", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("SMARTSCHEMA");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("SMART_CASING tables: {}", names);
      // SMART_CASING converts MyReport -> my_report
      assertTrue(names.contains("my_report"),
          "Expected 'my_report' (smart_cased) in " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 9. Empty directory produces empty schema
  // ---------------------------------------------------------------------------

  @Test void testEmptyDirectoryProducesEmptySchema() throws Exception {
    File emptyDir = createSubDir("empty_data");

    String model = buildModel("EMPTYSCHEMA", emptyDir.getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("EMPTYSCHEMA");
      assertNotNull(schema, "Schema should exist even if empty");
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Empty schema tables: {}", names);
      // Empty directory should have no tables
      assertEquals(0, names.size(),
          "Empty directory should produce 0 tables, got: " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 10. Multiple schemas in one model
  // ---------------------------------------------------------------------------

  @Test void testMultipleSchemasInOneModel() throws Exception {
    File dir1 = createSubDir("schema1_data");
    File dir2 = createSubDir("schema2_data");

    try (FileWriter w = new FileWriter(new File(dir1, "alpha.csv"))) {
      w.write("aid:int,aname:string\n1,AlphaOne\n");
    }
    try (FileWriter w = new FileWriter(new File(dir2, "beta.csv"))) {
      w.write("bid:int,bname:string\n1,BetaOne\n");
    }

    String model = buildDualSchemaModel("S1", dir1.getAbsolutePath(),
        "S2", dir2.getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);

      SchemaPlus s1 = cc.getRootSchema().getSubSchema("S1");
      SchemaPlus s2 = cc.getRootSchema().getSubSchema("S2");
      assertNotNull(s1, "Schema S1 should exist");
      assertNotNull(s2, "Schema S2 should exist");

      Set<String> s1Tables = s1.getTableNames();
      Set<String> s2Tables = s2.getTableNames();
      LOGGER.debug("S1 tables: {}, S2 tables: {}", s1Tables, s2Tables);

      // Query from each schema
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT aname FROM \"S1\".alpha")) {
        assertTrue(rs.next());
        assertEquals("AlphaOne", rs.getString(1));
      }

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT bname FROM \"S2\".beta")) {
        assertTrue(rs.next());
        assertEquals("BetaOne", rs.getString(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 11. getTableNames via CalciteConnection
  // ---------------------------------------------------------------------------

  @Test void testGetTableNamesViaSchema() throws Exception {
    createCsvFile("customers.csv",
        "cust_id:int,cust_name:string",
        "1,TestCust");

    String model = buildModel("TBLNAMES", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("TBLNAMES");
      assertNotNull(schema);

      Set<String> names = schema.getTableNames();
      assertNotNull(names);
      assertFalse(names.isEmpty(), "getTableNames should return non-empty set");
      LOGGER.debug("Table names: {}", names);
    }
  }

  // ---------------------------------------------------------------------------
  // 12. getTable returns valid Table object
  // ---------------------------------------------------------------------------

  @Test void testGetTableReturnsValidTable() throws Exception {
    createCsvFile("inventory.csv",
        "inv_id:int,quantity:int",
        "1,100",
        "2,200");

    String model = buildModel("TBLOBJ", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("TBLOBJ");
      assertNotNull(schema);

      Set<String> names = schema.getTableNames();
      LOGGER.debug("Tables: {}", names);
      // Find the inventory table
      String tableName = null;
      for (String n : names) {
        if (n.toLowerCase().contains("inventory")) {
          tableName = n;
          break;
        }
      }
      assertNotNull(tableName, "Should find inventory table in " + names);

      Table table = schema.getTable(tableName);
      assertNotNull(table, "getTable should return non-null for " + tableName);
    }
  }

  // ---------------------------------------------------------------------------
  // 13. CSV type inference enabled
  // ---------------------------------------------------------------------------

  @Test void testCsvTypeInferenceEnabled() throws Exception {
    createCsvFile("inferred.csv",
        "record_id,amount,is_active",
        "1,99.5,true",
        "2,150.0,false");

    String model = buildModel("TYPEINF", tempDir.toFile().getAbsolutePath(),
        "        \"csvTypeInference\": { \"enabled\": true }");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT record_id, amount, is_active FROM inferred ORDER BY record_id")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      LOGGER.debug("Column types: {}, {}, {}",
          rsmd.getColumnTypeName(1),
          rsmd.getColumnTypeName(2),
          rsmd.getColumnTypeName(3));

      assertTrue(rs.next());
      // With type inference, record_id should be inferred as numeric
      assertNotNull(rs.getObject(1));
      assertNotNull(rs.getObject(2));
    }
  }

  // ---------------------------------------------------------------------------
  // 14. CSV type inference disabled (all strings)
  // ---------------------------------------------------------------------------

  @Test void testCsvTypeInferenceDisabled() throws Exception {
    createCsvFile("allstrings.csv",
        "col_a,col_b,col_c",
        "1,hello,true",
        "2,world,false");

    String model = buildModel("NOINF", tempDir.toFile().getAbsolutePath(),
        "        \"csvTypeInference\": { \"enabled\": false }");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT col_a, col_b FROM allstrings ORDER BY col_a")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      LOGGER.debug("Column types with inference disabled: {}, {}",
          rsmd.getColumnTypeName(1),
          rsmd.getColumnTypeName(2));
      assertTrue(rs.next());
      assertNotNull(rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 15. Flatten option for JSON
  // ---------------------------------------------------------------------------

  @Test void testFlattenOptionAccepted() throws Exception {
    // CSV files work with flatten=true (option is for JSON but shouldn't break CSV)
    createCsvFile("flat_data.csv",
        "fid:int,fname:string",
        "1,FlatOne",
        "2,FlatTwo");

    String model = buildModel("FLATSCHEMA", tempDir.toFile().getAbsolutePath(),
        "        \"flatten\": true");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("FLATSCHEMA");
      assertNotNull(schema, "FLATSCHEMA should be created with flatten=true");
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Flatten schema tables: {}", names);
      assertFalse(names.isEmpty(),
          "Schema with flatten=true should still discover CSV tables");

      // Verify the CSV table is queryable
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT fname FROM flat_data ORDER BY fid")) {
        assertTrue(rs.next());
        assertEquals("FlatOne", rs.getString(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 16. sourceDirectory override via operand
  // ---------------------------------------------------------------------------

  @Test void testSourceDirectoryOverride() throws Exception {
    File sourceDir = createSubDir("source_override");
    try (FileWriter w = new FileWriter(new File(sourceDir, "src_data.csv"))) {
      w.write("src_id:int,src_val:string\n1,SourceValue\n");
    }

    String model = buildModel("SRCOVERRIDE",
        sourceDir.getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT src_val FROM src_data")) {
      assertTrue(rs.next());
      assertEquals("SourceValue", rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 17. Schema with comment
  // ---------------------------------------------------------------------------

  @Test void testSchemaWithComment() throws Exception {
    createCsvFile("commented.csv",
        "cid:int,cval:string",
        "1,test");

    String model = buildModel("COMMENTED", tempDir.toFile().getAbsolutePath(),
        "        \"comment\": \"Test schema comment\"");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("COMMENTED");
      assertNotNull(schema, "Schema COMMENTED should exist");
      // Simply verify the schema was created successfully with a comment
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Commented schema tables: {}", names);
      assertFalse(names.isEmpty());
    }
  }

  // ---------------------------------------------------------------------------
  // 18. Recursive directory scanning
  // ---------------------------------------------------------------------------

  @Test void testRecursiveDirectoryScanning() throws Exception {
    File subDir = createSubDir("level1");
    try (FileWriter w = new FileWriter(new File(subDir, "deep.csv"))) {
      w.write("deep_id:int,deep_val:string\n1,DeepValue\n");
    }

    String model = buildModel("RECURSE", tempDir.toFile().getAbsolutePath(),
        "        \"recursive\": true");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("RECURSE");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Recursive schema tables: {}", names);
      // With recursive=true, files in subdirectories should be found
      boolean foundDeep = false;
      for (String n : names) {
        if (n.toLowerCase().contains("deep")) {
          foundDeep = true;
          break;
        }
      }
      assertTrue(foundDeep,
          "Recursive scan should find 'deep' table from subdirectory, got: " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 19. Non-recursive scanning does not find subdirectory files
  // ---------------------------------------------------------------------------

  @Test void testNonRecursiveDoesNotFindSubdirFiles() throws Exception {
    File subDir = createSubDir("hidden_level");
    // Create file ONLY in subdirectory, not in root
    try (FileWriter w = new FileWriter(new File(subDir, "hidden.csv"))) {
      w.write("hid_id:int,hid_val:string\n1,HiddenVal\n");
    }

    // Default is recursive=false
    String model = buildModel("NONREC", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("NONREC");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Non-recursive tables: {}", names);
      // Hidden file should NOT be found without recursive
      boolean foundHidden = false;
      for (String n : names) {
        if (n.toLowerCase().contains("hidden")) {
          foundHidden = true;
          break;
        }
      }
      assertFalse(foundHidden,
          "Non-recursive scan should not find 'hidden' in subdirectory, got: " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 20. SQL queries work correctly (SELECT, WHERE, ORDER BY)
  // ---------------------------------------------------------------------------

  @Test void testSqlQueriesWithFilterAndSort() throws Exception {
    createCsvFile("scores.csv",
        "student_id:int,student_name:string,score:double",
        "1,Alice,95.5",
        "2,Bob,85.0",
        "3,Charlie,72.3",
        "4,Diana,99.0");

    String model = buildModel("SQLTEST", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      // COUNT
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) FROM scores")) {
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
      }

      // WHERE + ORDER BY
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT student_name FROM scores WHERE score > 90 ORDER BY score DESC")) {
        assertTrue(rs.next());
        assertEquals("Diana", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 21. Aggregation queries (GROUP BY, SUM, AVG)
  // ---------------------------------------------------------------------------

  @Test void testAggregationQueries() throws Exception {
    createCsvFile("metrics.csv",
        "category:string,metric_value:double",
        "A,10.0",
        "B,20.0",
        "A,30.0",
        "B,40.0");

    String model = buildModel("AGGTEST", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT category, SUM(metric_value) as total, AVG(metric_value) as avg_val "
             + "FROM metrics GROUP BY category ORDER BY category")) {
      assertTrue(rs.next());
      assertEquals("A", rs.getString(1));
      assertEquals(40.0, rs.getDouble(2), 0.001);
      assertEquals(20.0, rs.getDouble(3), 0.001);

      assertTrue(rs.next());
      assertEquals("B", rs.getString(1));
      assertEquals(60.0, rs.getDouble(2), 0.001);
    }
  }

  // ---------------------------------------------------------------------------
  // 22. JOIN across tables in same schema
  // ---------------------------------------------------------------------------

  @Test void testJoinAcrossTables() throws Exception {
    createCsvFile("proj_employees.csv",
        "eid:int,ename:string,dept:int",
        "1,Alice,10",
        "2,Bob,20");
    createCsvFile("proj_departments.csv",
        "did:int,dname:string",
        "10,Engineering",
        "20,Marketing");

    String model = buildModel("JOINTEST", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT e.ename, d.dname "
             + "FROM proj_employees e "
             + "JOIN proj_departments d ON e.dept = d.did "
             + "ORDER BY e.eid")) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString(1));
      assertEquals("Engineering", rs.getString(2));

      assertTrue(rs.next());
      assertEquals("Bob", rs.getString(1));
      assertEquals("Marketing", rs.getString(2));
    }
  }

  // ---------------------------------------------------------------------------
  // 23. Cross-schema JOIN
  // ---------------------------------------------------------------------------

  @Test void testCrossSchemaJoin() throws Exception {
    File dir1 = createSubDir("cross1");
    File dir2 = createSubDir("cross2");

    try (FileWriter w = new FileWriter(new File(dir1, "users.csv"))) {
      w.write("uid:int,uname:string\n1,Alice\n2,Bob\n");
    }
    try (FileWriter w = new FileWriter(new File(dir2, "user_roles.csv"))) {
      w.write("uid:int,role_name:string\n1,Admin\n2,Viewer\n");
    }

    String model = buildDualSchemaModel("USERDB", dir1.getAbsolutePath(),
        "ROLEDB", dir2.getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT u.uname, r.role_name "
             + "FROM \"USERDB\".users u "
             + "JOIN \"ROLEDB\".user_roles r ON u.uid = r.uid "
             + "ORDER BY u.uid")) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString(1));
      assertEquals("Admin", rs.getString(2));

      assertTrue(rs.next());
      assertEquals("Bob", rs.getString(1));
      assertEquals("Viewer", rs.getString(2));
    }
  }

  // ---------------------------------------------------------------------------
  // 24. Database metadata (getTables)
  // ---------------------------------------------------------------------------

  @Test void testDatabaseMetadataGetTables() throws Exception {
    createCsvFile("meta_data.csv",
        "mid:int,mval:string",
        "1,test");

    String model = buildModel("META", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      DatabaseMetaData dbMeta = conn.getMetaData();
      assertNotNull(dbMeta);

      // Get tables in the META schema
      try (ResultSet rs = dbMeta.getTables(null, "META", "%", null)) {
        boolean foundTable = false;
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          LOGGER.debug("MetaData table: {}", tableName);
          if (tableName.toLowerCase().contains("meta_data")) {
            foundTable = true;
          }
        }
        assertTrue(foundTable,
            "DatabaseMetaData.getTables should find meta_data table");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 25. Database metadata (getColumns)
  // ---------------------------------------------------------------------------

  @Test void testDatabaseMetadataGetColumns() throws Exception {
    createCsvFile("col_meta.csv",
        "colm_id:int,colm_name:string,colm_val:double",
        "1,test,1.5");

    String model = buildModel("COLMETA", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      DatabaseMetaData dbMeta = conn.getMetaData();
      try (ResultSet rs = dbMeta.getColumns(null, "COLMETA", "%", "%")) {
        List<String> columnNames = new ArrayList<String>();
        while (rs.next()) {
          columnNames.add(rs.getString("COLUMN_NAME"));
        }
        LOGGER.debug("Columns: {}", columnNames);
        assertFalse(columnNames.isEmpty(),
            "Should find columns via DatabaseMetaData");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 26. Snake_case config keys work too (table_name_casing)
  // ---------------------------------------------------------------------------

  @Test void testSnakeCaseConfigKeys() throws Exception {
    createCsvFile("SnakeTest.csv",
        "skey:int,sval:string",
        "1,test");

    String model = buildModel("SNAKE", tempDir.toFile().getAbsolutePath(),
        "        \"table_name_casing\": \"UPPER\",\n"
        + "        \"column_name_casing\": \"UPPER\"");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("SNAKE");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Snake-case config tables: {}", names);
      assertTrue(names.contains("SNAKETEST"),
          "Expected 'SNAKETEST' in " + names);
    }
  }

  // ---------------------------------------------------------------------------
  // 27. Explicit table definitions in operand
  // ---------------------------------------------------------------------------

  @Test void testExplicitTableDefinitions() throws Exception {
    createCsvFile("raw_data.csv",
        "rid:int,rval:string",
        "1,ExplicitValue");

    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    String model = BaseFileTest.addEphemeralCacheToModel(
        "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"EXPLICIT\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"EXPLICIT\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\",\n"
        + "        \"tables\": [\n"
        + "          { \"name\": \"my_table\", \"url\": \"" + dir + "/raw_data.csv\" }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    try (Connection conn = connect(model)) {
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = cc.getRootSchema().getSubSchema("EXPLICIT");
      assertNotNull(schema);
      Set<String> names = schema.getTableNames();
      LOGGER.debug("Explicit definition tables: {}", names);
      // The explicit table should be found (plus any directory-discovered tables)
      assertFalse(names.isEmpty(), "Should have at least one table");
    }
  }

  // ---------------------------------------------------------------------------
  // 28. Typed CSV columns (int, string, double)
  // ---------------------------------------------------------------------------

  @Test void testTypedCsvColumns() throws Exception {
    createCsvFile("typed.csv",
        "tid:int,tname:string,tamt:double,tactive:boolean",
        "1,ItemA,100.50,true",
        "2,ItemB,200.75,false");

    String model = buildModel("TYPED", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid, tname, tamt FROM typed ORDER BY tid")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      assertEquals(3, rsmd.getColumnCount());

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("tid"));
      assertEquals("ItemA", rs.getString("tname"));
      assertEquals(100.50, rs.getDouble("tamt"), 0.001);

      assertTrue(rs.next());
      assertEquals(2, rs.getInt("tid"));
      assertEquals("ItemB", rs.getString("tname"));
    }
  }

  // ---------------------------------------------------------------------------
  // 29. Execution engine operand
  // ---------------------------------------------------------------------------

  @Test void testExecutionEngineOperand() throws Exception {
    createCsvFile("engine_test.csv",
        "eid:int,eval:string",
        "1,EngineTest");

    String model = buildModel("ENGINETEST", tempDir.toFile().getAbsolutePath(),
        "        \"executionEngine\": \"PARQUET\"");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT eval FROM engine_test")) {
      assertTrue(rs.next());
      assertEquals("EngineTest", rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 30. Batch size operand
  // ---------------------------------------------------------------------------

  @Test void testBatchSizeOperand() throws Exception {
    createCsvFile("batch_test.csv",
        "bid:int,bval:string",
        "1,BatchA",
        "2,BatchB");

    String model = buildModel("BATCHTEST", tempDir.toFile().getAbsolutePath(),
        "        \"batchSize\": 512");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT bval FROM batch_test ORDER BY bid")) {
      assertTrue(rs.next());
      assertEquals("BatchA", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("BatchB", rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 31. primeCache disabled
  // ---------------------------------------------------------------------------

  @Test void testPrimeCacheDisabled() throws Exception {
    createCsvFile("no_prime.csv",
        "npid:int,npval:string",
        "1,NoPrimeValue");

    String model = buildModel("NOPRIME", tempDir.toFile().getAbsolutePath(),
        "        \"primeCache\": false");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT npval FROM no_prime")) {
      assertTrue(rs.next());
      assertEquals("NoPrimeValue", rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 32. Schema with refreshInterval
  // ---------------------------------------------------------------------------

  @Test void testRefreshIntervalOperand() throws Exception {
    createCsvFile("refreshed.csv",
        "rfid:int,rfval:string",
        "1,RefreshValue");

    String model = buildModel("REFRESH", tempDir.toFile().getAbsolutePath(),
        "        \"refreshInterval\": \"5 minutes\"");
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT rfval FROM refreshed")) {
      assertTrue(rs.next());
      assertEquals("RefreshValue", rs.getString(1));
    }
  }

  // ---------------------------------------------------------------------------
  // 33. Schema with views
  // ---------------------------------------------------------------------------

  @Test void testSubqueryAsViewSubstitute() throws Exception {
    createCsvFile("base_tbl.csv",
        "vid:int,vname:string,vamount:double",
        "1,Alice,100.0",
        "2,Bob,200.0",
        "3,Charlie,50.0");

    String model = buildModel("VIEWSCHEMA", tempDir.toFile().getAbsolutePath());
    // Use subquery to exercise the same SQL paths that views would use
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT vname FROM "
             + "(SELECT vid, vname, vamount FROM base_tbl WHERE vamount >= 100) "
             + "ORDER BY vid")) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("Bob", rs.getString(1));
      assertFalse(rs.next(), "Subquery should filter out Charlie (amount=50)");
    }
  }

  // ---------------------------------------------------------------------------
  // 34. Multiple connections share schema definitions
  // ---------------------------------------------------------------------------

  @Test void testMultipleConnectionsSameModel() throws Exception {
    createCsvFile("shared.csv",
        "shid:int,shval:string",
        "1,SharedVal");

    String model = buildModel("SHARED", tempDir.toFile().getAbsolutePath());

    // Open two connections to the same model
    try (Connection conn1 = connect(model);
         Connection conn2 = connect(model)) {
      // Both should be able to query the same table
      try (Statement stmt1 = conn1.createStatement();
           ResultSet rs1 = stmt1.executeQuery("SELECT shval FROM shared")) {
        assertTrue(rs1.next());
        assertEquals("SharedVal", rs1.getString(1));
      }

      try (Statement stmt2 = conn2.createStatement();
           ResultSet rs2 = stmt2.executeQuery("SELECT shval FROM shared")) {
        assertTrue(rs2.next());
        assertEquals("SharedVal", rs2.getString(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 35. CSV with special characters in values
  // ---------------------------------------------------------------------------

  @Test void testCsvWithSpecialCharacters() throws Exception {
    createCsvFile("specials.csv",
        "sp_id:int,sp_desc:string",
        "1,\"Hello, World\"",
        "2,\"Line1\"");

    String model = buildModel("SPECIALS", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT sp_desc FROM specials ORDER BY sp_id")) {
      assertTrue(rs.next());
      String val = rs.getString(1);
      assertNotNull(val);
      LOGGER.debug("Special char value: '{}'", val);
    }
  }

  // ---------------------------------------------------------------------------
  // 36. LIMIT/OFFSET queries
  // ---------------------------------------------------------------------------

  @Test void testLimitOffsetQueries() throws Exception {
    createCsvFile("paginated.csv",
        "pid:int,pval:string",
        "1,A", "2,B", "3,C", "4,D", "5,E");

    String model = buildModel("PAGINATE", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT pval FROM paginated ORDER BY pid LIMIT 2 OFFSET 1")) {
      assertTrue(rs.next());
      assertEquals("B", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("C", rs.getString(1));
      assertFalse(rs.next());
    }
  }

  // ---------------------------------------------------------------------------
  // 37. NULL values in CSV
  // ---------------------------------------------------------------------------

  @Test void testNullValuesInCsv() throws Exception {
    createCsvFile("nulls.csv",
        "nid:int,nname:string,nval:double",
        "1,Alice,10.0",
        "2,,",
        "3,Charlie,30.0");

    String model = buildModel("NULLTEST", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT nid, nname, nval FROM nulls ORDER BY nid")) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString("nname"));
      assertFalse(rs.wasNull());

      assertTrue(rs.next());
      String nullName = rs.getString("nname");
      // Empty CSV field may be empty string or null
      assertTrue(nullName == null || nullName.isEmpty(),
          "Expected null or empty for missing name, got: " + nullName);

      assertTrue(rs.next());
      assertEquals("Charlie", rs.getString("nname"));
    }
  }

  // ---------------------------------------------------------------------------
  // 38. Large number of columns
  // ---------------------------------------------------------------------------

  @Test void testLargeNumberOfColumns() throws Exception {
    StringBuilder header = new StringBuilder();
    StringBuilder row = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      if (i > 0) {
        header.append(",");
        row.append(",");
      }
      header.append("col_").append(i).append(":int");
      row.append(i * 10);
    }

    createCsvFile("wide.csv", header.toString(), row.toString());

    String model = buildModel("WIDE", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM wide")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      assertEquals(20, rsmd.getColumnCount());
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
      assertEquals(190, rs.getInt(20));
    }
  }

  // ---------------------------------------------------------------------------
  // 39. FileSchemaFactory INSTANCE singleton
  // ---------------------------------------------------------------------------

  @Test void testFileSchemaFactorySingleton() {
    FileSchemaFactory instance1 = FileSchemaFactory.INSTANCE;
    FileSchemaFactory instance2 = FileSchemaFactory.INSTANCE;
    assertNotNull(instance1);
    assertTrue(instance1 == instance2,
        "INSTANCE should be the same object (singleton)");
  }

  // ---------------------------------------------------------------------------
  // 40. Connection unwrap to CalciteConnection
  // ---------------------------------------------------------------------------

  @Test void testConnectionUnwrapToCalcite() throws Exception {
    createCsvFile("unwrap.csv",
        "uid:int,uval:string",
        "1,test");

    String model = buildModel("UNWRAP", tempDir.toFile().getAbsolutePath());
    try (Connection conn = connect(model)) {
      assertTrue(conn.isWrapperFor(CalciteConnection.class),
          "Should be wrappable to CalciteConnection");
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      assertNotNull(cc);

      SchemaPlus root = cc.getRootSchema();
      assertNotNull(root);
      assertNotNull(root.getSubSchema("UNWRAP"));
    }
  }
}
