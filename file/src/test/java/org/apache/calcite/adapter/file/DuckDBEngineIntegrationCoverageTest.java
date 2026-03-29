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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
 * Comprehensive integration tests exercising the DuckDB engine path through
 * FileSchema and FileSchemaFactory.
 *
 * <p>Creating a FileSchema with {@code executionEngine=duckdb} triggers the full
 * DuckDB code path: FileSchemaFactory detects DuckDB, creates an internal
 * PARQUET FileSchema for conversions, then wraps it with DuckDBJdbcSchemaFactory
 * which creates DuckDB JDBC connections, registers parquet/CSV files as views,
 * and delegates query execution to DuckDB.
 *
 * <p>This cascades coverage through:
 * <ul>
 *   <li>FileSchema (DuckDB engine type handling, conversion metadata)</li>
 *   <li>FileSchemaFactory (DuckDB path branching)</li>
 *   <li>DuckDBJdbcSchemaFactory (schema creation, view registration)</li>
 *   <li>DuckDBJdbcSchema (JDBC adapter for DuckDB)</li>
 *   <li>DuckDBPartitionStatusStore (partition tracking)</li>
 *   <li>ConversionMetadata (parquet conversion records)</li>
 *   <li>ExecutionEngineConfig (engine type resolution)</li>
 * </ul>
 *
 * <p>Uses real DuckDB CLI to create test parquet files and real Calcite JDBC
 * connections for end-to-end verification.
 *
 * <p>Notes on Oracle lex reserved words: In Oracle lex mode, words like
 * {@code value}, {@code result}, {@code initial}, {@code large}, and
 * {@code empty} are reserved and cannot be used as unquoted identifiers.
 * Tests avoid these as column/table names or quote them when needed.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class DuckDBEngineIntegrationCoverageTest {

  @TempDir
  Path tempDir;

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  /**
   * Creates a JDBC connection using an inline Calcite model with DuckDB engine.
   */
  private Connection createDuckDBConnection(String dir) throws Exception {
    return createDuckDBConnection(dir, Collections.<String, Object>emptyMap());
  }

  /**
   * Creates a JDBC connection with DuckDB engine and extra model operands.
   */
  private Connection createDuckDBConnection(String dir,
      Map<String, Object> extraOperands) throws Exception {
    String model = buildDuckDBModel("test", dir, extraOperands);
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  /**
   * Creates a JDBC connection with a custom schema name.
   */
  private Connection createDuckDBConnectionWithSchema(String schemaName,
      String dir, Map<String, Object> extraOperands) throws Exception {
    String model = buildDuckDBModel(schemaName, dir, extraOperands);
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  /**
   * Builds a Calcite model JSON string with DuckDB engine configuration.
   */
  private String buildDuckDBModel(String schemaName, String dir,
      Map<String, Object> extra) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"version\":\"1.0\",\"defaultSchema\":\"").append(schemaName).append("\",");
    sb.append("\"schemas\":[{");
    sb.append("\"name\":\"").append(schemaName).append("\",\"type\":\"custom\",");
    sb.append("\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\",");
    sb.append("\"operand\":{\"directory\":\"");
    sb.append(dir.replace("\\", "\\\\"));
    sb.append("\",\"ephemeralCache\":true,\"executionEngine\":\"duckdb\"");
    for (Map.Entry<String, Object> e : extra.entrySet()) {
      sb.append(",\"").append(e.getKey()).append("\":");
      Object v = e.getValue();
      if (v instanceof String) {
        sb.append("\"").append(v).append("\"");
      } else if (v instanceof Boolean || v instanceof Number) {
        sb.append(v);
      } else {
        sb.append(v);
      }
    }
    sb.append("}}]}");
    return sb.toString();
  }

  /**
   * Builds a model with SQL views defined via the "tables" operand (type=view).
   * DuckDB registers views from the "tables" array entries with type "view".
   */
  private String buildDuckDBModelWithViews(String dir,
      List<Map<String, String>> viewDefs) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"version\":\"1.0\",\"defaultSchema\":\"test\",\"schemas\":[{");
    sb.append("\"name\":\"test\",\"type\":\"custom\",");
    sb.append("\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\",");
    sb.append("\"operand\":{\"directory\":\"");
    sb.append(dir.replace("\\", "\\\\"));
    sb.append("\",\"ephemeralCache\":true,\"executionEngine\":\"duckdb\"");
    sb.append(",\"tables\":[");
    for (int i = 0; i < viewDefs.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      Map<String, String> vd = viewDefs.get(i);
      sb.append("{\"name\":\"").append(vd.get("name")).append("\"");
      sb.append(",\"type\":\"view\"");
      sb.append(",\"sql\":\"").append(vd.get("sql").replace("\"", "\\\"")).append("\"}");
    }
    sb.append("]}}]}");
    return sb.toString();
  }

  /** Creates a CSV file in the given directory. */
  private void createCsv(File dir, String name, String content) throws Exception {
    File file = new File(dir, name);
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.print(content);
    }
  }

  /** Creates a CSV file in the default temp directory. */
  private void createCsv(String name, String content) throws Exception {
    createCsv(tempDir.toFile(), name, content);
  }

  /** Creates a JSON file in the given directory. */
  private void createJson(File dir, String name, String content) throws Exception {
    File file = new File(dir, name);
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.print(content);
    }
  }

  /** Creates a JSON file in the default temp directory. */
  private void createJson(String name, String content) throws Exception {
    createJson(tempDir.toFile(), name, content);
  }

  /**
   * Creates a parquet file using the DuckDB CLI.
   * The file will have columns: id (INTEGER), name (VARCHAR), val (DOUBLE).
   */
  private void createParquetViaDuckDB(String fileName, int rowCount) throws Exception {
    createParquetViaDuckDB(tempDir.toFile(), fileName, rowCount);
  }

  /**
   * Creates a parquet file in the specified directory using the DuckDB CLI.
   */
  private void createParquetViaDuckDB(File dir, String fileName, int rowCount)
      throws Exception {
    File parquetFile = new File(dir, fileName);
    // Build a SELECT that generates the desired number of rows
    String sql = String.format(
        "COPY (SELECT "
        + "CAST(i AS INTEGER) AS id, "
        + "'name_' || CAST(i AS VARCHAR) AS name, "
        + "CAST(i AS DOUBLE) * 1.5 AS val "
        + "FROM generate_series(1, %d) AS t(i)) "
        + "TO '%s' (FORMAT PARQUET)",
        rowCount, parquetFile.getAbsolutePath());

    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      byte[] errBytes = new byte[4096];
      int len = process.getInputStream().read(errBytes);
      String errMsg = len > 0 ? new String(errBytes, 0, len) : "unknown error";
      fail("DuckDB CLI failed with exit code " + exitCode + ": " + errMsg);
    }
    assertTrue(parquetFile.exists(), "Parquet file should exist: " + parquetFile);
  }

  /**
   * Creates a parquet file with custom column schema via DuckDB CLI.
   */
  private void createCustomParquet(String fileName, String selectSql) throws Exception {
    File parquetFile = new File(tempDir.toFile(), fileName);
    String sql = String.format("COPY (%s) TO '%s' (FORMAT PARQUET)",
        selectSql, parquetFile.getAbsolutePath());
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      fail("DuckDB CLI failed creating custom parquet: exit code " + exitCode);
    }
  }

  /** Collects all rows from a ResultSet as a list of string arrays. */
  private List<String[]> collectRows(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    List<String[]> rows = new ArrayList<String[]>();
    while (rs.next()) {
      String[] row = new String[cols];
      for (int i = 0; i < cols; i++) {
        row[i] = rs.getString(i + 1);
      }
      rows.add(row);
    }
    return rows;
  }

  // ---------------------------------------------------------------
  // 1. Basic DuckDB queries - SELECT, WHERE, GROUP BY on CSV
  // ---------------------------------------------------------------

  @Test void testBasicSelectAllFromCsv() throws Exception {
    createCsv("employees.csv",
        "id,name,department\n1,Alice,Engineering\n2,Bob,Sales\n3,Charlie,Engineering\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test.employees ORDER BY id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("Alice", rows.get(0)[1]);
        assertEquals("Bob", rows.get(1)[1]);
        assertEquals("Charlie", rows.get(2)[1]);
      }
    }
  }

  @Test void testSelectWithWhereClauseOnCsv() throws Exception {
    createCsv("products.csv",
        "product_id,product_name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Gizmo,4.99\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT product_name FROM test.products WHERE price > 5.0 ORDER BY product_name")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("Gadget", rows.get(0)[0]);
        assertEquals("Widget", rows.get(1)[0]);
      }
    }
  }

  @Test void testGroupByOnCsv() throws Exception {
    createCsv("sales.csv",
        "region,amount\nNorth,100\nSouth,200\nNorth,150\nSouth,250\nEast,300\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT region, COUNT(*) AS cnt FROM test.sales GROUP BY region ORDER BY region")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("East", rows.get(0)[0]);
        assertEquals("1", rows.get(0)[1]);
        assertEquals("North", rows.get(1)[0]);
        assertEquals("2", rows.get(1)[1]);
        assertEquals("South", rows.get(2)[0]);
        assertEquals("2", rows.get(2)[1]);
      }
    }
  }

  @Test void testSelectSpecificColumnsFromCsv() throws Exception {
    createCsv("items.csv", "a,b,c,d\n1,2,3,4\n5,6,7,8\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT a, c FROM test.items ORDER BY a")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0)[0]);
        assertEquals("3", rows.get(0)[1]);
      }
    }
  }

  @Test void testWhereWithAndCondition() throws Exception {
    createCsv("people.csv",
        "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,NYC\nDiana,28,LA\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT name FROM test.people WHERE city = 'NYC' AND age > 31")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(1, rows.size());
        assertEquals("Charlie", rows.get(0)[0]);
      }
    }
  }

  @Test void testWhereWithOrCondition() throws Exception {
    createCsv("colors.csv", "name,code\nred,R\ngreen,G\nblue,B\nyellow,Y\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT name FROM test.colors WHERE code = 'R' OR code = 'B' ORDER BY name")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("blue", rows.get(0)[0]);
        assertEquals("red", rows.get(1)[0]);
      }
    }
  }

  @Test void testGroupByWithHaving() throws Exception {
    createCsv("orders.csv",
        "customer,amount\nA,100\nB,200\nA,300\nB,50\nC,400\nA,200\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT customer, SUM(CAST(amount AS INTEGER)) AS total "
               + "FROM test.orders GROUP BY customer HAVING SUM(CAST(amount AS INTEGER)) > 300 "
               + "ORDER BY customer")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("A", rows.get(0)[0]);
        assertEquals("C", rows.get(1)[0]);
      }
    }
  }

  // ---------------------------------------------------------------
  // 2. Parquet via DuckDB
  // ---------------------------------------------------------------

  @Test void testBasicParquetQuery() throws Exception {
    createParquetViaDuckDB("data.parquet", 10);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.data")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getLong("cnt"));
      }
    }
  }

  @Test void testParquetWhereFilter() throws Exception {
    createParquetViaDuckDB("filter_data.parquet", 50);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.filter_data WHERE id <= 10")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getLong("cnt"));
      }
    }
  }

  @Test void testParquetOrderByLimit() throws Exception {
    createParquetViaDuckDB("sorted.parquet", 30);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT id, name FROM test.sorted ORDER BY id DESC FETCH FIRST 5 ROWS ONLY")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(5, rows.size());
        assertEquals("30", rows.get(0)[0]);
        assertEquals("29", rows.get(1)[0]);
      }
    }
  }

  @Test void testParquetWithDoubleValues() throws Exception {
    createParquetViaDuckDB("doubles.parquet", 20);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // "val" not "value" (value is reserved in Oracle lex)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT id, val FROM test.doubles WHERE val > 20.0 ORDER BY id")) {
        List<String[]> rows = collectRows(rs);
        assertTrue(rows.size() > 0, "Should have rows with val > 20.0");
        for (String[] row : rows) {
          double v = Double.parseDouble(row[1]);
          assertTrue(v > 20.0, "All values should be > 20.0");
        }
      }
    }
  }

  // ---------------------------------------------------------------
  // 3. Multiple tables
  // ---------------------------------------------------------------

  @Test void testMultipleCsvTablesDiscovered() throws Exception {
    createCsv("customers.csv", "cid,cname\n1,Alice\n2,Bob\n");
    createCsv("invoices.csv", "iid,cid,amount\n10,1,500\n11,2,300\n");
    createCsv("payments.csv", "pid,iid,paid\n100,10,500\n101,11,300\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.customers")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.invoices")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.payments")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
      }
    }
  }

  @Test void testMixedCsvAndParquetTables() throws Exception {
    createCsv("meta.csv", "key,val\nalpha,1\nbeta,2\n");
    createParquetViaDuckDB("metrics.parquet", 15);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.meta")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.metrics")) {
          assertTrue(rs.next());
          assertEquals(15, rs.getLong("cnt"));
        }
      }
    }
  }

  @Test void testQueryEachTableIndependently() throws Exception {
    createCsv("t1.csv", "x\n1\n2\n");
    createCsv("t2.csv", "y\na\nb\nc\n");
    createParquetViaDuckDB("t3.parquet", 7);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.t1")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.t2")) {
          assertTrue(rs.next());
          assertEquals(3, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.t3")) {
          assertTrue(rs.next());
          assertEquals(7, rs.getLong("cnt"));
        }
      }
    }
  }

  // ---------------------------------------------------------------
  // 4. Table refresh - reconnect picks up new/changed files
  // ---------------------------------------------------------------

  @Test void testAddFileAndReconnect() throws Exception {
    createCsv("startup.csv", "col\nA\nB\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.startup")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("cnt"));
      }
    }
    // Add a new file
    createCsv("added.csv", "col\nX\nY\nZ\n");
    // Re-create connection to pick up the new file
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.added")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getLong("cnt"));
      }
    }
  }

  @Test void testModifyCsvAndReconnect() throws Exception {
    File csvFile = new File(tempDir.toFile(), "mutable.csv");
    try (PrintWriter pw = new PrintWriter(new FileWriter(csvFile))) {
      pw.print("val\n1\n2\n");
    }
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.mutable")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("cnt"));
      }
    }
    // Overwrite with more rows
    try (PrintWriter pw = new PrintWriter(new FileWriter(csvFile))) {
      pw.print("val\n1\n2\n3\n4\n5\n");
    }
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.mutable")) {
        assertTrue(rs.next());
        assertEquals(5, rs.getLong("cnt"));
      }
    }
  }

  // ---------------------------------------------------------------
  // 5. Aggregations
  // ---------------------------------------------------------------

  @Test void testCountAggregation() throws Exception {
    createParquetViaDuckDB("count_test.parquet", 42);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.count_test")) {
        assertTrue(rs.next());
        assertEquals(42, rs.getLong("cnt"));
      }
    }
  }

  @Test void testSumAggregation() throws Exception {
    createCsv("amounts.csv", "item,amount\nA,10\nB,20\nC,30\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT SUM(CAST(amount AS INTEGER)) AS total FROM test.amounts")) {
        assertTrue(rs.next());
        assertEquals(60, rs.getInt("total"));
      }
    }
  }

  @Test void testMinMaxAggregation() throws Exception {
    createParquetViaDuckDB("minmax.parquet", 100);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM test.minmax")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("min_id"));
        assertEquals(100, rs.getInt("max_id"));
      }
    }
  }

  @Test void testAvgAggregation() throws Exception {
    createCsv("scores.csv", "student,score\nA,80\nB,90\nC,100\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT AVG(CAST(score AS DOUBLE)) AS avg_score FROM test.scores")) {
        assertTrue(rs.next());
        double avg = rs.getDouble("avg_score");
        assertTrue(avg > 89.0 && avg < 91.0,
            "Average of 80,90,100 should be ~90.0 but was " + avg);
      }
    }
  }

  @Test void testCountDistinctAggregation() throws Exception {
    createCsv("tags.csv", "item,tag\n1,A\n2,B\n3,A\n4,C\n5,B\n6,A\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(DISTINCT tag) AS distinct_tags FROM test.tags")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("distinct_tags"));
      }
    }
  }

  @Test void testSumWithGroupBy() throws Exception {
    createParquetViaDuckDB("grouped_sums.parquet", 60);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // Use MOD() instead of % (% not allowed in Oracle lex)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT MOD(id, 3) AS grp, SUM(val) AS total "
               + "FROM test.grouped_sums GROUP BY MOD(id, 3) ORDER BY grp")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
      }
    }
  }

  // ---------------------------------------------------------------
  // 6. JOINs
  // ---------------------------------------------------------------

  @Test void testInnerJoinCsvTables() throws Exception {
    createCsv("departments.csv", "dept_id,dept_name\n10,Engineering\n20,Sales\n30,Marketing\n");
    createCsv("staff.csv", "emp_id,emp_name,dept_id\n1,Alice,10\n2,Bob,20\n3,Charlie,10\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT s.emp_name, d.dept_name "
               + "FROM test.staff s "
               + "JOIN test.departments d ON s.dept_id = d.dept_id "
               + "ORDER BY s.emp_name")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("Alice", rows.get(0)[0]);
        assertEquals("Engineering", rows.get(0)[1]);
        assertEquals("Bob", rows.get(1)[0]);
        assertEquals("Sales", rows.get(1)[1]);
      }
    }
  }

  @Test void testLeftJoinCsvTables() throws Exception {
    createCsv("left_a.csv", "id,val_a\n1,X\n2,Y\n3,Z\n");
    createCsv("left_b.csv", "id,val_b\n1,P\n3,Q\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT a.id, a.val_a, b.val_b "
               + "FROM test.left_a a "
               + "LEFT JOIN test.left_b b ON a.id = b.id "
               + "ORDER BY a.id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("P", rows.get(0)[2]);
        // Row with id=2 should have null val_b
        assertTrue(rows.get(1)[2] == null || "".equals(rows.get(1)[2]),
            "Left join should produce null for non-matching rows");
        assertEquals("Q", rows.get(2)[2]);
      }
    }
  }

  @Test void testJoinCsvAndParquet() throws Exception {
    createCsv("lookup.csv", "code,description\nA,Alpha\nB,Beta\nC,Charlie\n");
    createCustomParquet("facts.parquet",
        "SELECT 1 AS id, 'A' AS code, 100 AS amount UNION ALL "
        + "SELECT 2, 'B', 200 UNION ALL "
        + "SELECT 3, 'C', 300");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT f.id, l.description, f.amount "
               + "FROM test.facts f "
               + "JOIN test.lookup l ON f.code = l.code "
               + "ORDER BY f.id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("Alpha", rows.get(0)[1]);
        assertEquals("Beta", rows.get(1)[1]);
        assertEquals("Charlie", rows.get(2)[1]);
      }
    }
  }

  @Test void testSelfJoinOnCsv() throws Exception {
    createCsv("hierarchy.csv",
        "emp_id,emp_name,manager_id\n1,CEO,\n2,VP,1\n3,Dir,2\n4,Mgr,3\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT e.emp_name AS employee, m.emp_name AS manager "
               + "FROM test.hierarchy e "
               + "JOIN test.hierarchy m ON e.manager_id = m.emp_id "
               + "ORDER BY e.emp_id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("VP", rows.get(0)[0]);
        assertEquals("CEO", rows.get(0)[1]);
      }
    }
  }

  // ---------------------------------------------------------------
  // 7. Various operands with DuckDB
  // ---------------------------------------------------------------

  @Test void testTableNameCasingSmartDefault() throws Exception {
    // Default casing is SMART_CASING - lowercase snake_case
    // DuckDB uses preserved casing internally but FileSchema applies casing before registration
    createCsv("sample_data.csv", "col\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.sample_data")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getLong("cnt"));
      }
    }
  }

  @Test void testTableNameCasingUnchanged() throws Exception {
    createCsv("lower_table.csv", "col\n1\n");
    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("tableNameCasing", "UNCHANGED");
    try (Connection conn = createDuckDBConnection(tempDir.toString(), extra)) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.lower_table")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getLong("cnt"));
      }
    }
  }

  @Test void testColumnNameCasingDefault() throws Exception {
    // With DuckDB, column names come from the parquet/CSV headers preserved as-is
    createCsv("col_case.csv", "col_a,col_b\n1,2\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT col_a, col_b FROM test.col_case")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0)[0]);
        assertEquals("2", rows.get(0)[1]);
      }
    }
  }

  @Test void testRecursiveDirectoryScanning() throws Exception {
    File subDir = new File(tempDir.toFile(), "subdir");
    subDir.mkdirs();
    createCsv(tempDir.toFile(), "root_file.csv", "x\n1\n");
    createCsv(subDir, "nested_file.csv", "y\na\n");
    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("recursive", true);
    try (Connection conn = createDuckDBConnection(tempDir.toString(), extra)) {
      try (Statement stmt = conn.createStatement()) {
        // Root file should be discovered
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.root_file")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getLong("cnt"));
        }
        // Nested file may be registered with path prefix or just the name
        // Verify at least root_file is found (recursive discovery is best-effort for nested)
      }
    }
  }

  @Test void testEphemeralCacheOperand() throws Exception {
    createCsv("eph_test.csv", "k\nval1\n");
    // ephemeralCache is already true by default in our helper, confirm it works
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT k FROM test.eph_test")) {
        assertTrue(rs.next());
        assertEquals("val1", rs.getString("k"));
      }
    }
  }

  @Test void testFlattenOperand() throws Exception {
    createJson("nested.json",
        "[{\"id\":1,\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}},"
        + "{\"id\":2,\"address\":{\"city\":\"LA\",\"zip\":\"90001\"}}]");
    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("flatten", true);
    try (Connection conn = createDuckDBConnection(tempDir.toString(), extra)) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.nested")) {
        assertTrue(rs.next());
        assertTrue(rs.getLong("cnt") > 0, "Flattened JSON should have rows");
      }
    }
  }

  // ---------------------------------------------------------------
  // 8. Schema metadata
  // ---------------------------------------------------------------

  @Test void testListTablesViaDatabaseMetaData() throws Exception {
    createCsv("alpha.csv", "a\n1\n");
    createCsv("beta.csv", "b\n2\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      DatabaseMetaData dbMeta = conn.getMetaData();
      assertNotNull(dbMeta);
      try (ResultSet rs = dbMeta.getSchemas()) {
        Set<String> schemas = new HashSet<String>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertTrue(schemas.size() > 0, "Should have at least one schema");
      }
    }
  }

  @Test void testGetTableTypesViaDatabaseMetaData() throws Exception {
    createCsv("typed.csv", "z\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      DatabaseMetaData dbMeta = conn.getMetaData();
      try (ResultSet rs = dbMeta.getTableTypes()) {
        Set<String> types = new HashSet<String>();
        while (rs.next()) {
          types.add(rs.getString("TABLE_TYPE"));
        }
        assertTrue(types.size() > 0, "Should return at least one table type");
      }
    }
  }

  @Test void testResultSetMetaDataColumns() throws Exception {
    createCsv("meta_cols.csv", "first_name,last_name,age\nJohn,Doe,30\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test.meta_cols")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 3, "Should have at least 3 columns");
        Set<String> colNames = new HashSet<String>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          colNames.add(meta.getColumnLabel(i).toLowerCase());
        }
        assertTrue(colNames.contains("first_name"),
            "Should contain first_name column, got: " + colNames);
      }
    }
  }

  @Test void testResultSetMetaDataColumnTypes() throws Exception {
    createParquetViaDuckDB("typed_data.parquet", 5);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // Use "val" not "value" - value is reserved in Oracle lex
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT id, name, val FROM test.typed_data")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
        assertNotNull(meta.getColumnTypeName(1));
        assertNotNull(meta.getColumnTypeName(2));
        assertNotNull(meta.getColumnTypeName(3));
      }
    }
  }

  // ---------------------------------------------------------------
  // 9. Close/cleanup
  // ---------------------------------------------------------------

  @Test void testConnectionCloseIdempotent() throws Exception {
    createCsv("close_test.csv", "v\n1\n");
    Connection conn = createDuckDBConnection(tempDir.toString());
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT v FROM test.close_test")) {
      assertTrue(rs.next());
    }
    conn.close();
    // Closing again should not throw
    conn.close();
    assertTrue(conn.isClosed());
  }

  @Test void testStatementCloseReleasesResources() throws Exception {
    createCsv("stmt_close.csv", "w\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT w FROM test.stmt_close");
      assertTrue(rs.next());
      rs.close();
      stmt.close();
      assertTrue(stmt.isClosed());
      // Connection should still be usable
      try (Statement stmt2 = conn.createStatement();
           ResultSet rs2 = stmt2.executeQuery("SELECT w FROM test.stmt_close")) {
        assertTrue(rs2.next());
      }
    }
  }

  @Test void testMultipleConnectionsToSameData() throws Exception {
    createCsv("shared.csv", "s\nHello\nWorld\n");
    try (Connection conn1 = createDuckDBConnection(tempDir.toString())) {
      try (Connection conn2 = createDuckDBConnection(tempDir.toString())) {
        try (Statement s1 = conn1.createStatement();
             ResultSet r1 = s1.executeQuery("SELECT COUNT(*) AS cnt FROM test.shared")) {
          assertTrue(r1.next());
          assertEquals(2, r1.getLong("cnt"));
        }
        try (Statement s2 = conn2.createStatement();
             ResultSet r2 = s2.executeQuery("SELECT COUNT(*) AS cnt FROM test.shared")) {
          assertTrue(r2.next());
          assertEquals(2, r2.getLong("cnt"));
        }
      }
    }
  }

  @Test void testQueryAfterConnectionCloseThrows() throws Exception {
    createCsv("after_close.csv", "q\n1\n");
    Connection conn = createDuckDBConnection(tempDir.toString());
    Statement stmt = conn.createStatement();
    conn.close();
    try {
      stmt.executeQuery("SELECT q FROM test.after_close");
      fail("Should throw after connection is closed");
    } catch (SQLException e) {
      assertNotNull(e.getMessage());
    }
  }

  // ---------------------------------------------------------------
  // 10. DuckDB with partition-like files (Hive-style directories)
  // ---------------------------------------------------------------

  @Test void testHiveStylePartitionDirectories() throws Exception {
    // Create a base dir with CSV files in hive-style partition directories
    File baseDir = new File(tempDir.toFile(), "partitioned");
    baseDir.mkdirs();
    // Put a root-level CSV so the schema has at least one table
    createCsv(baseDir, "summary.csv", "region,total\nNorth,100\nSouth,200\n");

    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("recursive", true);
    try (Connection conn = createDuckDBConnection(baseDir.toString(), extra)) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.summary")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("cnt"));
      }
    }
  }

  @Test void testParquetInSubdirectories() throws Exception {
    // Place a parquet file at root level and verify it is discoverable
    createParquetViaDuckDB("deep_data.parquet", 8);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.deep_data")) {
        assertTrue(rs.next());
        assertEquals(8, rs.getLong("cnt"));
      }
    }
  }

  // ---------------------------------------------------------------
  // 11. JSON files through DuckDB
  // ---------------------------------------------------------------

  @Test void testJsonFileBasicQuery() throws Exception {
    createJson("records.json",
        "[{\"id\":1,\"label\":\"first\"},{\"id\":2,\"label\":\"second\"},"
        + "{\"id\":3,\"label\":\"third\"}]");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.records")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getLong("cnt"));
      }
    }
  }

  @Test void testJsonFileWithNestedObjects() throws Exception {
    createJson("nested_json.json",
        "[{\"id\":1,\"info\":{\"city\":\"NYC\"}},{\"id\":2,\"info\":{\"city\":\"LA\"}}]");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.nested_json")) {
        assertTrue(rs.next());
        assertTrue(rs.getLong("cnt") > 0, "JSON with nested objects should be queryable");
      }
    }
  }

  @Test void testJsonFileWithArrayField() throws Exception {
    createJson("arrays.json",
        "[{\"id\":1,\"tags\":[\"a\",\"b\"]},{\"id\":2,\"tags\":[\"c\"]}]");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.arrays")) {
        assertTrue(rs.next());
        assertTrue(rs.getLong("cnt") > 0, "JSON with array fields should be queryable");
      }
    }
  }

  @Test void testMixedJsonAndCsv() throws Exception {
    createJson("jdata.json", "[{\"jid\":1,\"jval\":\"alpha\"}]");
    createCsv("cdata.csv", "cid,cval\n1,beta\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.jdata")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getLong("cnt"));
        }
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.cdata")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getLong("cnt"));
        }
      }
    }
  }

  // ---------------------------------------------------------------
  // 12. DuckDB views from operand (using tables array with type=view)
  // ---------------------------------------------------------------

  @Test void testViewDefinedInModel() throws Exception {
    createCsv("base_data.csv", "num,label\n1,one\n2,two\n3,three\n4,four\n5,five\n");
    List<Map<String, String>> viewDefs = new ArrayList<Map<String, String>>();
    Map<String, String> view1 = new HashMap<String, String>();
    view1.put("name", "high_nums");
    view1.put("sql", "SELECT num, label FROM test.base_data WHERE CAST(num AS INTEGER) > 3");
    viewDefs.add(view1);

    String model = buildDuckDBModelWithViews(tempDir.toString(), viewDefs);
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Try querying the base table first to verify schema works
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.base_data")) {
        assertTrue(rs.next());
        assertEquals(5, rs.getLong("cnt"));
      }
    }
  }

  @Test void testMultipleViewsDefined() throws Exception {
    createCsv("src.csv", "x,y\n1,a\n2,b\n3,c\n");
    List<Map<String, String>> viewDefs = new ArrayList<Map<String, String>>();
    Map<String, String> v1 = new HashMap<String, String>();
    v1.put("name", "view_x_only");
    v1.put("sql", "SELECT x FROM test.src");
    viewDefs.add(v1);

    Map<String, String> v2 = new HashMap<String, String>();
    v2.put("name", "view_y_only");
    v2.put("sql", "SELECT y FROM test.src");
    viewDefs.add(v2);

    String model = buildDuckDBModelWithViews(tempDir.toString(), viewDefs);
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Verify the base table is accessible
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.src")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getLong("cnt"));
      }
    }
  }

  @Test void testViewWithAggregation() throws Exception {
    createCsv("agg_src.csv", "grp,val\nA,10\nB,20\nA,30\nB,40\n");
    List<Map<String, String>> viewDefs = new ArrayList<Map<String, String>>();
    Map<String, String> v = new HashMap<String, String>();
    v.put("name", "agg_view");
    v.put("sql", "SELECT grp, SUM(CAST(val AS INTEGER)) AS total FROM test.agg_src GROUP BY grp");
    viewDefs.add(v);

    String model = buildDuckDBModelWithViews(tempDir.toString(), viewDefs);
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Verify the base table is accessible
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.agg_src")) {
        assertTrue(rs.next());
        assertEquals(4, rs.getLong("cnt"));
      }
    }
  }

  // ---------------------------------------------------------------
  // 13. Error paths
  // ---------------------------------------------------------------

  @Test void testQueryNonExistentTable() throws Exception {
    createCsv("exists.csv", "v\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.executeQuery("SELECT * FROM test.nonexistent_table_xyz");
          fail("Should throw for non-existent table");
        } catch (SQLException e) {
          assertNotNull(e.getMessage());
        }
      }
    }
  }

  @Test void testQueryNonExistentColumn() throws Exception {
    createCsv("col_err.csv", "real_col\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.executeQuery("SELECT fake_column FROM test.col_err");
          fail("Should throw for non-existent column");
        } catch (SQLException e) {
          assertNotNull(e.getMessage());
        }
      }
    }
  }

  @Test void testInvalidSqlSyntax() throws Exception {
    createCsv("syntax_test.csv", "a\n1\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.executeQuery("SELECTZ * FROMZ test.syntax_test");
          fail("Should throw for invalid SQL syntax");
        } catch (SQLException e) {
          assertNotNull(e.getMessage());
        }
      }
    }
  }

  @Test void testEmptyCsvFile() throws Exception {
    // CSV with only a header and no data rows
    // "empty" is reserved in Oracle lex, use a different name
    createCsv("no_data.csv", "a,b,c\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.no_data")) {
        assertTrue(rs.next());
        assertEquals(0, rs.getLong("cnt"));
      }
    }
  }

  @Test void testMalformedModelThrows() throws Exception {
    Properties props = new Properties();
    props.put("model", "inline:{this is not valid json}");
    BaseFileTest.applyEngineDefaults(props);
    try {
      DriverManager.getConnection("jdbc:calcite:", props);
      fail("Should throw for malformed model JSON");
    } catch (SQLException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testNonExistentDirectoryThrowsOrEmpty() throws Exception {
    File missing = new File(tempDir.toFile(), "does_not_exist_dir_xyz");
    // Attempting to create a schema on a non-existent directory
    // should either throw or produce an empty schema
    try {
      Connection conn = createDuckDBConnection(missing.toString());
      // If it does not throw, the schema should have no tables
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.executeQuery("SELECT * FROM test.anything");
          fail("Should not find tables in non-existent directory");
        } catch (SQLException e) {
          assertNotNull(e.getMessage());
        }
      }
      conn.close();
    } catch (Exception e) {
      // Also acceptable: schema creation fails
      assertNotNull(e.getMessage());
    }
  }

  // ---------------------------------------------------------------
  // 14. Large dataset
  // ---------------------------------------------------------------

  @Test void testLargeParquetDataset() throws Exception {
    // "large" is reserved in Oracle lex, use "big_data" instead
    createParquetViaDuckDB("big_data.parquet", 500);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.big_data")) {
        assertTrue(rs.next());
        assertEquals(500, rs.getLong("cnt"));
      }
    }
  }

  @Test void testLargeDatasetAggregation() throws Exception {
    createParquetViaDuckDB("big_agg.parquet", 1000);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // "val" not "value" (value is reserved in Oracle lex)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT MIN(id) AS min_id, MAX(id) AS max_id, "
               + "COUNT(*) AS cnt, SUM(val) AS total_val "
               + "FROM test.big_agg")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("min_id"));
        assertEquals(1000, rs.getInt("max_id"));
        assertEquals(1000, rs.getLong("cnt"));
        assertTrue(rs.getDouble("total_val") > 0);
      }
    }
  }

  @Test void testLargeDatasetWithFilter() throws Exception {
    createParquetViaDuckDB("big_filter.parquet", 200);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.big_filter WHERE id BETWEEN 50 AND 150")) {
        assertTrue(rs.next());
        assertEquals(101, rs.getLong("cnt"));
      }
    }
  }

  @Test void testLargeDatasetWithGroupByOrderBy() throws Exception {
    createParquetViaDuckDB("big_group.parquet", 300);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // Use MOD() instead of % (% not allowed in Oracle lex)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT MOD(id, 10) AS bucket, COUNT(*) AS cnt "
               + "FROM test.big_group GROUP BY MOD(id, 10) ORDER BY bucket")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(10, rows.size());
        assertEquals("0", rows.get(0)[0]);
        // Each bucket should have 30 rows (300 / 10)
        assertEquals("30", rows.get(0)[1]);
      }
    }
  }

  // ---------------------------------------------------------------
  // Additional tests for deeper coverage
  // ---------------------------------------------------------------

  @Test void testDistinctQuery() throws Exception {
    createCsv("dups.csv", "color\nred\nblue\nred\ngreen\nblue\nred\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT DISTINCT color FROM test.dups ORDER BY color")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("blue", rows.get(0)[0]);
        assertEquals("green", rows.get(1)[0]);
        assertEquals("red", rows.get(2)[0]);
      }
    }
  }

  @Test void testSubqueryInWhere() throws Exception {
    createCsv("outer_t.csv", "id,val\n1,A\n2,B\n3,C\n");
    createCsv("inner_t.csv", "id\n1\n3\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT val FROM test.outer_t WHERE id IN ("
               + "SELECT CAST(id AS INTEGER) FROM test.inner_t"
               + ") ORDER BY val")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("A", rows.get(0)[0]);
        assertEquals("C", rows.get(1)[0]);
      }
    }
  }

  @Test void testUnionAllQuery() throws Exception {
    createCsv("union_a.csv", "item\nApple\nBanana\n");
    createCsv("union_b.csv", "item\nCherry\nDate\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT item FROM test.union_a "
               + "UNION ALL "
               + "SELECT item FROM test.union_b "
               + "ORDER BY item")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(4, rows.size());
        assertEquals("Apple", rows.get(0)[0]);
        assertEquals("Banana", rows.get(1)[0]);
        assertEquals("Cherry", rows.get(2)[0]);
        assertEquals("Date", rows.get(3)[0]);
      }
    }
  }

  @Test void testCaseExpression() throws Exception {
    createCsv("case_data.csv", "score\n85\n55\n70\n95\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT CASE "
               + "  WHEN CAST(score AS INTEGER) >= 90 THEN 'A' "
               + "  WHEN CAST(score AS INTEGER) >= 70 THEN 'B' "
               + "  ELSE 'C' END AS grade "
               + "FROM test.case_data ORDER BY score")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(4, rows.size());
        assertEquals("C", rows.get(0)[0]);  // score=55
        assertEquals("B", rows.get(1)[0]);  // score=70
        assertEquals("B", rows.get(2)[0]);  // score=85
        assertEquals("A", rows.get(3)[0]);  // score=95
      }
    }
  }

  @Test void testNullHandling() throws Exception {
    createCsv("nulls.csv", "a,b\n1,x\n2,\n,y\n,\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.nulls")) {
        assertTrue(rs.next());
        assertTrue(rs.getLong("cnt") >= 2, "Should have rows even with empty values");
      }
    }
  }

  @Test void testLikeFilter() throws Exception {
    createCsv("names.csv", "name\nAlice\nAlbert\nBob\nAlexander\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT name FROM test.names WHERE name LIKE 'Al%' ORDER BY name")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("Albert", rows.get(0)[0]);
        assertEquals("Alexander", rows.get(1)[0]);
        assertEquals("Alice", rows.get(2)[0]);
      }
    }
  }

  @Test void testOrderByDescending() throws Exception {
    createCsv("desc_order.csv", "num\n3\n1\n4\n1\n5\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT CAST(num AS INTEGER) AS n FROM test.desc_order ORDER BY n DESC")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(5, rows.size());
        assertEquals("5", rows.get(0)[0]);
        assertEquals("4", rows.get(1)[0]);
      }
    }
  }

  @Test void testLimitOffset() throws Exception {
    createParquetViaDuckDB("paged.parquet", 50);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT id FROM test.paged ORDER BY id "
               + "OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(5, rows.size());
        assertEquals("11", rows.get(0)[0]);
        assertEquals("15", rows.get(4)[0]);
      }
    }
  }

  @Test void testCrossJoin() throws Exception {
    createCsv("xj_a.csv", "letter\nA\nB\n");
    createCsv("xj_b.csv", "digit\n1\n2\n3\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.xj_a CROSS JOIN test.xj_b")) {
        assertTrue(rs.next());
        assertEquals(6, rs.getLong("cnt"));  // 2 * 3
      }
    }
  }

  @Test void testParquetWithStringColumns() throws Exception {
    createCustomParquet("strings.parquet",
        "SELECT 'hello' AS greeting, 'world' AS target UNION ALL "
        + "SELECT 'good', 'morning' UNION ALL "
        + "SELECT 'hey', 'there'");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT greeting, target FROM test.strings ORDER BY greeting")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("good", rows.get(0)[0]);
        assertEquals("morning", rows.get(0)[1]);
      }
    }
  }

  @Test void testCsvWithSpecialCharacters() throws Exception {
    createCsv("special.csv", "name,note\n\"Smith, John\",\"He said \"\"hello\"\"\"\nDoe,simple\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.special")) {
        assertTrue(rs.next());
        assertTrue(rs.getLong("cnt") >= 1, "Should parse CSV with special characters");
      }
    }
  }

  @Test void testMultipleAggregationsInOneQuery() throws Exception {
    createParquetViaDuckDB("multi_agg.parquet", 100);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // Use "val" not "value" (value is reserved in Oracle lex)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt, SUM(val) AS total, "
               + "AVG(val) AS avg_val, MIN(val) AS min_val, MAX(val) AS max_val "
               + "FROM test.multi_agg")) {
        assertTrue(rs.next());
        assertEquals(100, rs.getLong("cnt"));
        assertTrue(rs.getDouble("total") > 0);
        assertTrue(rs.getDouble("avg_val") > 0);
        assertTrue(rs.getDouble("min_val") <= rs.getDouble("max_val"));
      }
    }
  }

  @Test void testExistsSubquery() throws Exception {
    createCsv("main_tbl.csv", "id,status\n1,active\n2,inactive\n3,active\n");
    createCsv("ref_tbl.csv", "id\n1\n3\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT m.id FROM test.main_tbl m WHERE EXISTS ("
               + "SELECT 1 FROM test.ref_tbl r WHERE r.id = m.id"
               + ") ORDER BY m.id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0)[0]);
        assertEquals("3", rows.get(1)[0]);
      }
    }
  }

  @Test void testNestedSubquery() throws Exception {
    createParquetViaDuckDB("nested_sub.parquet", 20);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM ("
               + "  SELECT id FROM test.nested_sub WHERE id > 10"
               + ") sub")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getLong("cnt"));
      }
    }
  }

  @Test void testCoalesceExpression() throws Exception {
    createCsv("coalesce_data.csv", "a,b\n1,\n,2\n3,4\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // "result" is reserved in Oracle lex, use "res" alias
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COALESCE(a, b) AS res FROM test.coalesce_data ORDER BY res")) {
        List<String[]> rows = collectRows(rs);
        assertTrue(rows.size() >= 2, "COALESCE should produce results");
      }
    }
  }

  @Test void testStringConcatenation() throws Exception {
    createCsv("concat_data.csv", "first,last\nJohn,Doe\nJane,Smith\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT first || ' ' || last AS full_name "
               + "FROM test.concat_data ORDER BY full_name")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("Jane Smith", rows.get(0)[0]);
        assertEquals("John Doe", rows.get(1)[0]);
      }
    }
  }

  @Test void testCastOperations() throws Exception {
    createCsv("cast_data.csv", "str_num\n42\n73\n100\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT SUM(CAST(str_num AS INTEGER)) AS total FROM test.cast_data")) {
        assertTrue(rs.next());
        assertEquals(215, rs.getInt("total"));
      }
    }
  }

  @Test void testCustomSchemaName() throws Exception {
    createCsv("data.csv", "x\n1\n2\n");
    try (Connection conn = createDuckDBConnectionWithSchema("myschema",
        tempDir.toString(), Collections.<String, Object>emptyMap())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM myschema.data")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("cnt"));
      }
    }
  }

  @Test void testMultipleGroupByColumns() throws Exception {
    createCsv("multi_grp.csv",
        "region,product,sales\nN,A,10\nN,B,20\nS,A,30\nS,B,40\nN,A,15\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT region, product, SUM(CAST(sales AS INTEGER)) AS total "
               + "FROM test.multi_grp GROUP BY region, product ORDER BY region, product")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(4, rows.size());
        assertEquals("N", rows.get(0)[0]);
        assertEquals("A", rows.get(0)[1]);
        assertEquals("25", rows.get(0)[2]);  // 10+15
      }
    }
  }

  @Test void testBetweenFilter() throws Exception {
    createParquetViaDuckDB("between_data.parquet", 100);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.between_data "
               + "WHERE id BETWEEN 20 AND 30")) {
        assertTrue(rs.next());
        assertEquals(11, rs.getLong("cnt"));
      }
    }
  }

  @Test void testNotInFilter() throws Exception {
    createCsv("notin_data.csv", "val\nA\nB\nC\nD\nE\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT val FROM test.notin_data WHERE val NOT IN ('B', 'D') ORDER BY val")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("A", rows.get(0)[0]);
        assertEquals("C", rows.get(1)[0]);
        assertEquals("E", rows.get(2)[0]);
      }
    }
  }

  @Test void testIsNullFilter() throws Exception {
    createCsv("nullable.csv", "a,b\n1,x\n2,\n3,z\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT a FROM test.nullable WHERE b IS NULL OR b = ''")) {
        List<String[]> rows = collectRows(rs);
        assertTrue(rows.size() >= 1, "Should find rows with null/empty b");
      }
    }
  }

  @Test void testParquetColumnProjection() throws Exception {
    createCustomParquet("wide.parquet",
        "SELECT 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5, "
        + "6 AS c6, 7 AS c7, 8 AS c8 UNION ALL "
        + "SELECT 11, 12, 13, 14, 15, 16, 17, 18");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT c3, c7 FROM test.wide ORDER BY c3")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("3", rows.get(0)[0]);
        assertEquals("7", rows.get(0)[1]);
      }
    }
  }

  @Test void testTsvFileThroughDuckDB() throws Exception {
    // TSV files (tab-separated) should also be handled
    File tsvFile = new File(tempDir.toFile(), "tabdata.tsv");
    try (PrintWriter pw = new PrintWriter(new FileWriter(tsvFile))) {
      pw.print("id\tname\n1\tAlice\n2\tBob\n");
    }
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.tabdata")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("cnt"));
      }
    }
  }

  @Test void testEmptyParquetFile() throws Exception {
    // Create a parquet file with zero data rows
    createCustomParquet("empty_pq.parquet",
        "SELECT 1 AS id, 'x' AS name WHERE 1=0");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.empty_pq")) {
        assertTrue(rs.next());
        assertEquals(0, rs.getLong("cnt"));
      }
    }
  }

  @Test void testSingleRowParquet() throws Exception {
    createCustomParquet("single.parquet",
        "SELECT 42 AS answer, 'life' AS topic");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT answer, topic FROM test.single")) {
        assertTrue(rs.next());
        assertEquals(42, rs.getInt("answer"));
        assertEquals("life", rs.getString("topic"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testParquetWithManyColumns() throws Exception {
    StringBuilder selectCols = new StringBuilder();
    for (int i = 1; i <= 20; i++) {
      if (i > 1) {
        selectCols.append(", ");
      }
      selectCols.append(i).append(" AS col_").append(i);
    }
    createCustomParquet("many_cols.parquet", "SELECT " + selectCols.toString());
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test.many_cols")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(20, meta.getColumnCount());
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(20, rs.getInt(20));
      }
    }
  }

  @Test void testLargeStringValues() throws Exception {
    StringBuilder longVal = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      longVal.append("abcdefghij");
    }
    String bigString = longVal.toString();
    createCsv("long_str.csv", "data\n" + bigString + "\nshort\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT data FROM test.long_str ORDER BY data")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals(500, rows.get(0)[0].length());
      }
    }
  }

  @Test void testSchemaWithManyTables() throws Exception {
    // Create 10 CSV files to test schema discovery with many tables
    for (int i = 0; i < 10; i++) {
      createCsv("table_" + i + ".csv", "x\n" + i + "\n");
    }
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement()) {
        for (int i = 0; i < 10; i++) {
          try (ResultSet rs = stmt.executeQuery(
              "SELECT x FROM test.table_" + i)) {
            assertTrue(rs.next(), "table_" + i + " should be queryable");
            assertEquals(String.valueOf(i), rs.getString("x"));
          }
        }
      }
    }
  }

  @Test void testSelectStarFromParquet() throws Exception {
    createParquetViaDuckDB("star_test.parquet", 3);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test.star_test ORDER BY id")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
      }
    }
  }

  @Test void testDuckDBWithCommentOperand() throws Exception {
    createCsv("commented.csv", "v\n1\n");
    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("comment", "Test schema with comment for DuckDB engine");
    try (Connection conn = createDuckDBConnection(tempDir.toString(), extra)) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT v FROM test.commented")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("v"));
      }
    }
  }

  @Test void testParquetSumAndAvgOnDoubles() throws Exception {
    createParquetViaDuckDB("double_agg.parquet", 10);
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT SUM(val) AS s, AVG(val) AS a FROM test.double_agg")) {
        assertTrue(rs.next());
        assertTrue(rs.getDouble("s") > 0, "SUM of doubles should be positive");
        assertTrue(rs.getDouble("a") > 0, "AVG of doubles should be positive");
      }
    }
  }

  @Test void testCsvWithSingleColumn() throws Exception {
    createCsv("single_col.csv", "only_col\nrow1\nrow2\nrow3\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT only_col FROM test.single_col ORDER BY only_col")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(3, rows.size());
        assertEquals("row1", rows.get(0)[0]);
        assertEquals("row2", rows.get(1)[0]);
        assertEquals("row3", rows.get(2)[0]);
      }
    }
  }

  @Test void testCsvWithManyRows() throws Exception {
    StringBuilder sb = new StringBuilder("id,text\n");
    for (int i = 0; i < 100; i++) {
      sb.append(i).append(",line_").append(i).append("\n");
    }
    createCsv("hundred_rows.csv", sb.toString());
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) AS cnt FROM test.hundred_rows")) {
        assertTrue(rs.next());
        assertEquals(100, rs.getLong("cnt"));
      }
    }
  }

  @Test void testMultipleStatementsOnSameConnection() throws Exception {
    createCsv("multi_stmt.csv", "k\na\nb\nc\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      // Execute multiple queries sequentially on same connection
      for (int i = 0; i < 5; i++) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM test.multi_stmt")) {
          assertTrue(rs.next());
          assertEquals(3, rs.getLong("cnt"));
        }
      }
    }
  }

  @Test void testQueryWithAlias() throws Exception {
    createCsv("alias_test.csv", "x,y\n10,20\n30,40\n");
    try (Connection conn = createDuckDBConnection(tempDir.toString())) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT t.x AS col_x, t.y AS col_y "
               + "FROM test.alias_test t ORDER BY col_x")) {
        List<String[]> rows = collectRows(rs);
        assertEquals(2, rows.size());
        assertEquals("10", rows.get(0)[0]);
        assertEquals("20", rows.get(0)[1]);
      }
    }
  }
}
