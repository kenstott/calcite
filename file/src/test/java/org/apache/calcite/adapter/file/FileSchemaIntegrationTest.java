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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for FileSchema, FileSchemaFactory, and FileTable.
 * Exercises schema creation, table discovery, JDBC queries, and various
 * operand configurations through end-to-end JDBC connections.
 */
@Tag("integration")
public class FileSchemaIntegrationTest extends BaseFileTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileSchemaIntegrationTest.class);

  @TempDir
  Path tempDir;

  private void writeCsv(File dir, String name, String content) throws IOException {
    File file = new File(dir, name);
    try (FileWriter w = new FileWriter(file)) {
      w.write(content);
    }
  }

  private void writeJson(File dir, String name, String content) throws IOException {
    File file = new File(dir, name);
    try (FileWriter w = new FileWriter(file)) {
      w.write(content);
    }
  }

  private Connection connect(String modelJson) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + addEphemeralCacheToModel(modelJson));
    applyEngineDefaults(info);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  private String buildModel(String schemaName, String directory) {
    return buildTestModel(schemaName, directory);
  }

  // --- Schema creation and table discovery tests ---

  @Test void testCsvTableDiscovery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "employees.csv",
        "id,name,department,salary\n"
        + "1,Alice,Engineering,95000\n"
        + "2,Bob,Marketing,75000\n"
        + "3,Charlie,Engineering,88000\n");

    try (Connection conn = connect(buildModel("files", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
        boolean found = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          LOGGER.debug("Found table: {}", tableName);
          if (tableName.equalsIgnoreCase("employees")) {
            found = true;
          }
        }
        assertTrue(found, "Should discover employees table from CSV file");
      }
    }
  }

  @Test void testCsvQuery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "products.csv",
        "product_id,product_name,price,quantity\n"
        + "1,Widget,19.99,100\n"
        + "2,Gadget,29.99,50\n"
        + "3,Doohickey,9.99,200\n"
        + "4,Thingamajig,49.99,25\n");

    try (Connection conn = connect(buildModel("shop", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM shop.products ORDER BY product_id")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("product_id"));
        assertEquals("Widget", rs.getString("product_name"));
        assertTrue(rs.next());
        assertEquals("2", rs.getString("product_id"));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvFilterQuery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "orders.csv",
        "order_id,customer,amount,status\n"
        + "1,Alice,150.00,completed\n"
        + "2,Bob,250.00,pending\n"
        + "3,Charlie,75.00,completed\n"
        + "4,Diana,320.00,cancelled\n"
        + "5,Eve,180.00,completed\n");

    try (Connection conn = connect(buildModel("sales", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM sales.orders WHERE status = 'completed'")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testCsvAggregation() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "metrics.csv",
        "region,month,revenue\n"
        + "North,Jan,10000\n"
        + "North,Feb,12000\n"
        + "South,Jan,8000\n"
        + "South,Feb,9000\n"
        + "East,Jan,15000\n"
        + "East,Feb,17000\n");

    try (Connection conn = connect(buildModel("data", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT region, COUNT(*) AS cnt FROM data.metrics GROUP BY region ORDER BY region")) {
        assertTrue(rs.next());
        assertEquals("East", rs.getString("region"));
        assertEquals(2, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("North", rs.getString("region"));
        assertTrue(rs.next());
        assertEquals("South", rs.getString("region"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonTableDiscovery() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "cities.json",
        "[{\"city\":\"New York\",\"population\":8336817,\"state\":\"NY\"},"
        + "{\"city\":\"Los Angeles\",\"population\":3979576,\"state\":\"CA\"},"
        + "{\"city\":\"Chicago\",\"population\":2693976,\"state\":\"IL\"}]");

    try (Connection conn = connect(buildModel("geo", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM geo.cities ORDER BY city")) {
        assertTrue(rs.next());
        assertEquals("Chicago", rs.getString("city"));
        assertTrue(rs.next());
        assertEquals("Los Angeles", rs.getString("city"));
        assertTrue(rs.next());
        assertEquals("New York", rs.getString("city"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonQuery() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "events.json",
        "[{\"event_id\":1,\"name\":\"Conference\",\"attendees\":500},"
        + "{\"event_id\":2,\"name\":\"Workshop\",\"attendees\":50},"
        + "{\"event_id\":3,\"name\":\"Meetup\",\"attendees\":100}]");

    try (Connection conn = connect(buildModel("ev", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM ev.events")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testMultipleCsvTables() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "customers.csv",
        "cust_id,name,email\n"
        + "1,Alice,alice@example.com\n"
        + "2,Bob,bob@example.com\n");
    writeCsv(dir, "orders.csv",
        "order_id,cust_id,amount\n"
        + "101,1,99.99\n"
        + "102,2,149.99\n"
        + "103,1,49.99\n");

    try (Connection conn = connect(buildModel("store", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = meta.getTables(null, "store", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME"));
        }
      }
      assertTrue(tableNames.size() >= 2,
          "Should discover at least 2 tables, found: " + tableNames);
    }
  }

  @Test void testMixedCsvAndJson() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "users.csv",
        "user_id,username\n"
        + "1,alice\n"
        + "2,bob\n");
    writeJson(dir, "settings.json",
        "[{\"key\":\"theme\",\"value\":\"dark\"},"
        + "{\"key\":\"lang\",\"value\":\"en\"}]");

    try (Connection conn = connect(buildModel("app", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = meta.getTables(null, "app", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
      }
      boolean hasUsers = false;
      boolean hasSettings = false;
      for (String name : tableNames) {
        if (name.equals("users")) {
          hasUsers = true;
        }
        if (name.equals("settings")) {
          hasSettings = true;
        }
      }
      assertTrue(hasUsers, "Should discover users CSV table, found: " + tableNames);
      assertTrue(hasSettings, "Should discover settings JSON table, found: " + tableNames);
    }
  }

  @Test void testEmptyCsvFile() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "blank_data.csv", "col1,col2,col3\n");

    try (Connection conn = connect(buildModel("blank_schema", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM blank_schema.blank_data")) {
        assertFalse(rs.next(), "Empty CSV should have no data rows");
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 3,
            "Should still have column definitions");
      }
    }
  }

  @Test void testCsvWithSpecialCharacters() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "special.csv",
        "id,description,value\n"
        + "1,\"Contains, commas\",100\n"
        + "2,\"Has \"\"quotes\"\"\",200\n"
        + "3,Simple text,300\n");

    try (Connection conn = connect(buildModel("special_data", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM special_data.special")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testSchemaWithTableNameCasing() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "my_data.csv",
        "id,name\n"
        + "1,Test\n");

    String model =
        buildTestModel("casing_test", dir.getAbsolutePath(), "tableNameCasing", "LOWER");

    try (Connection conn = connect(model)) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "casing_test", "%", null)) {
        boolean found = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          LOGGER.debug("Table with LOWER casing: {}", tableName);
          if (tableName.equals("my_data")) {
            found = true;
          }
        }
        assertTrue(found, "Should find lowercase table name with LOWER casing");
      }
    }
  }

  @Test void testSchemaWithRecursive() throws Exception {
    File dir = tempDir.toFile();
    File subDir = new File(dir, "subdir");
    subDir.mkdirs();
    writeCsv(dir, "top_level.csv", "id,name\n1,Top\n");
    writeCsv(subDir, "nested.csv", "id,name\n1,Nested\n");

    // Build model manually so "recursive" is a JSON boolean, not a string
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"recursive_test\","
        + "\"schemas\":[{"
        + "\"name\":\"recursive_test\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"recursive\":true,"
        + "\"ephemeralCache\":true"
        + "}}]}";

    try (Connection conn = connect(model)) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = meta.getTables(null, "recursive_test", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
      }
      LOGGER.debug("Recursive tables found: {}", tableNames);
      assertTrue(tableNames.size() >= 2,
          "Should discover tables from subdirectories when recursive=true, found: " + tableNames);
    }
  }

  @Test void testCsvColumnMetadata() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "typed_data.csv",
        "id,name,score,active\n"
        + "1,Alice,95.5,true\n"
        + "2,Bob,87.3,false\n");

    try (Connection conn = connect(buildModel("meta_test", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM meta_test.typed_data LIMIT 1")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 4,
            "Should have at least 4 columns");
        assertNotNull(meta.getColumnName(1));
        assertNotNull(meta.getColumnTypeName(1));
        LOGGER.debug("Column 1: {} ({})", meta.getColumnName(1), meta.getColumnTypeName(1));
        LOGGER.debug("Column 2: {} ({})", meta.getColumnName(2), meta.getColumnTypeName(2));
      }
    }
  }

  @Test void testJsonWithNestedStructure() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "nested.json",
        "[{\"id\":1,\"name\":\"Alice\",\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}},"
        + "{\"id\":2,\"name\":\"Bob\",\"address\":{\"city\":\"LA\",\"zip\":\"90001\"}}]");

    try (Connection conn = connect(buildModel("nested_data", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM nested_data.nested")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testCsvJoinQuery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "departments.csv",
        "dept_id,dept_name\n"
        + "1,Engineering\n"
        + "2,Marketing\n"
        + "3,Sales\n");
    writeCsv(dir, "staff.csv",
        "emp_id,emp_name,dept_id\n"
        + "101,Alice,1\n"
        + "102,Bob,2\n"
        + "103,Charlie,1\n"
        + "104,Diana,3\n");

    try (Connection conn = connect(buildModel("company", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT s.emp_name, d.dept_name "
          + "FROM company.staff s "
          + "JOIN company.departments d ON s.dept_id = d.dept_id "
          + "ORDER BY s.emp_name")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("emp_name"));
        assertEquals("Engineering", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("emp_name"));
        assertEquals("Marketing", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvSubquery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "scores.csv",
        "student,subject,score\n"
        + "Alice,Math,90\n"
        + "Alice,English,85\n"
        + "Bob,Math,78\n"
        + "Bob,English,92\n"
        + "Charlie,Math,88\n"
        + "Charlie,English,76\n");

    try (Connection conn = connect(buildModel("school", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT student, COUNT(*) AS subjects "
          + "FROM school.scores "
          + "GROUP BY student "
          + "HAVING COUNT(*) > 1 "
          + "ORDER BY student")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("student"));
        assertEquals(2, rs.getInt("subjects"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("student"));
        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString("student"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonArrayOfPrimitives() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "simple.json",
        "[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"},{\"a\":3,\"b\":\"z\"}]");

    try (Connection conn = connect(buildModel("prim", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM prim.simple ORDER BY a")) {
        assertTrue(rs.next());
        assertEquals("x", rs.getString("b"));
        assertTrue(rs.next());
        assertEquals("y", rs.getString("b"));
        assertTrue(rs.next());
        assertEquals("z", rs.getString("b"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testSchemaWithFlatten() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "deep.json",
        "[{\"id\":1,\"info\":{\"name\":\"Alice\",\"age\":30}},"
        + "{\"id\":2,\"info\":{\"name\":\"Bob\",\"age\":25}}]");

    // flatten requires explicit table definition with flatten:true per table
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"flat\","
        + "\"schemas\":[{"
        + "\"name\":\"flat\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"tables\":[{"
        + "\"name\":\"deep\","
        + "\"url\":\"" + new File(dir, "deep.json").getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"flatten\":true"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM flat.deep")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testLargerDataset() throws Exception {
    File dir = tempDir.toFile();
    StringBuilder csv = new StringBuilder("id,value,category\n");
    for (int i = 1; i <= 500; i++) {
      csv.append(i).append(",").append(i * 10).append(",")
          .append(i % 5 == 0 ? "A" : "B").append("\n");
    }
    writeCsv(dir, "big_data.csv", csv.toString());

    try (Connection conn = connect(buildModel("big_schema", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM big_schema.big_data")) {
        assertTrue(rs.next());
        assertEquals(500, rs.getInt("cnt"));
      }

      try (ResultSet rs =
          stmt.executeQuery("SELECT category, COUNT(*) AS cnt FROM big_schema.big_data GROUP BY category ORDER BY category")) {
        assertTrue(rs.next());
        assertEquals("A", rs.getString("category"));
        assertEquals(100, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("B", rs.getString("category"));
        assertEquals(400, rs.getInt("cnt"));
      }
    }
  }

  @Test void testCsvWithHeaderOnly() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "headers_only.csv", "col_a,col_b,col_c\n");

    try (Connection conn = connect(buildModel("hdr", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM hdr.headers_only")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 3);
        assertFalse(rs.next());
      }
    }
  }

  @Test void testMultipleSchemas() throws Exception {
    File dir1 = new File(tempDir.toFile(), "schema1");
    File dir2 = new File(tempDir.toFile(), "schema2");
    dir1.mkdirs();
    dir2.mkdirs();
    writeCsv(dir1, "data1.csv", "id,val\n1,aaa\n");
    writeCsv(dir2, "data2.csv", "id,val\n2,bbb\n");

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"s1\","
        + "\"schemas\":["
        + "{\"name\":\"s1\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{\"directory\":\"" + dir1.getAbsolutePath().replace("\\", "\\\\") + "\",\"ephemeralCache\":true}},"
        + "{\"name\":\"s2\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{\"directory\":\"" + dir2.getAbsolutePath().replace("\\", "\\\\") + "\",\"ephemeralCache\":true}}"
        + "]}";

    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT val FROM s1.data1")) {
        assertTrue(rs.next());
        assertEquals("aaa", rs.getString("val"));
      }
      try (ResultSet rs = stmt.executeQuery("SELECT val FROM s2.data2")) {
        assertTrue(rs.next());
        assertEquals("bbb", rs.getString("val"));
      }
    }
  }

  @Test void testCsvOrderByAndLimit() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "ranked.csv",
        "rank,name,score\n"
        + "1,Alpha,100\n"
        + "2,Beta,90\n"
        + "3,Gamma,80\n"
        + "4,Delta,70\n"
        + "5,Epsilon,60\n");

    try (Connection conn = connect(buildModel("rank_data", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT name FROM rank_data.ranked ORDER BY score DESC LIMIT 3")) {
        // Note: score is VARCHAR without type inference, but ORDER BY still works
        assertTrue(rs.next());
        assertNotNull(rs.getString("name"));
      }
    }
  }

  @Test void testCsvDistinct() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "dupes.csv",
        "category,item\n"
        + "A,x\n"
        + "A,y\n"
        + "B,x\n"
        + "B,y\n"
        + "A,x\n");

    try (Connection conn = connect(buildModel("dedup", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT DISTINCT category FROM dedup.dupes ORDER BY category")) {
        assertTrue(rs.next());
        assertEquals("A", rs.getString("category"));
        assertTrue(rs.next());
        assertEquals("B", rs.getString("category"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvUnion() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "table_a.csv",
        "id,name\n"
        + "1,Alice\n"
        + "2,Bob\n");
    writeCsv(dir, "table_b.csv",
        "id,name\n"
        + "3,Charlie\n"
        + "4,Diana\n");

    try (Connection conn = connect(buildModel("union_test", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT id, name FROM union_test.table_a "
          + "UNION ALL "
          + "SELECT id, name FROM union_test.table_b "
          + "ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("id"));
        assertTrue(rs.next());
        assertEquals("2", rs.getString("id"));
        assertTrue(rs.next());
        assertEquals("3", rs.getString("id"));
        assertTrue(rs.next());
        assertEquals("4", rs.getString("id"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonWithMixedTypes() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "mixed.json",
        "[{\"id\":1,\"value\":\"text\",\"count\":10},"
        + "{\"id\":2,\"value\":\"more\",\"count\":20}]");

    try (Connection conn = connect(buildModel("mixed_schema", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM mixed_schema.mixed ORDER BY id")) {
        assertTrue(rs.next());
        assertNotNull(rs.getString("id"));
        assertEquals("text", rs.getString("value"));
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvWithNullValues() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "nulls.csv",
        "id,name,optional\n"
        + "1,Alice,present\n"
        + "2,Bob,\n"
        + "3,,absent\n");

    try (Connection conn = connect(buildModel("null_test", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) AS cnt FROM null_test.nulls")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testSchemaComment() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "data.csv", "id,val\n1,test\n");

    String model =
        buildTestModel("commented", dir.getAbsolutePath(), "comment", "Test schema with comment");

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM commented.data")) {
        assertTrue(rs.next());
      }
    }
  }

  @Test void testEmptyDirectory() throws Exception {
    File dir = new File(tempDir.toFile(), "empty_dir");
    dir.mkdirs();

    try (Connection conn = connect(buildModel("empty_test", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "empty_test", "%", null)) {
        // Empty directory should still create a valid schema
        LOGGER.debug("Tables in empty directory schema - checking schema is valid");
      }
    }
  }

  @Test void testCsvWithManyColumns() throws Exception {
    File dir = tempDir.toFile();
    StringBuilder header = new StringBuilder();
    StringBuilder row = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      if (i > 0) {
        header.append(",");
        row.append(",");
      }
      header.append("col_").append(i);
      row.append("val_").append(i);
    }
    writeCsv(dir, "wide.csv", header.toString() + "\n"
  + row.toString() + "\n");

    try (Connection conn = connect(buildModel("wide_test", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM wide_test.wide")) {
        assertTrue(rs.next());
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(20, meta.getColumnCount());
      }
    }
  }
}
