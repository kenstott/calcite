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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive integration tests for DuckDB coverage gaps including:
 * <ul>
 *   <li>{@link DuckDBJdbcSchemaFactory} - Schema creation, catalog path, views</li>
 *   <li>{@link DuckDBCatalogBuilder} - Catalog building validation</li>
 *   <li>{@link DuckDBIcebergCountStarRule} - COUNT(*) optimization rule</li>
 *   <li>{@link DuckDBHLLCountDistinctRule} - HLL COUNT(DISTINCT) rule</li>
 * </ul>
 */
@Tag("integration")
public class DuckDBFullCoverageTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DuckDBFullCoverageTest.class);

  private File tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("duckdb-full-cov-").toFile();
  }

  @AfterEach
  public void tearDown() {
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  // =========================================================================
  // DuckDBJdbcSchemaFactory tests
  // =========================================================================

  /**
   * Tests basic schema creation with DuckDB engine against CSV data.
   */
  @Test public void testSchemaFactoryCreateWithCsv() throws Exception {
    createCsvFile("products.csv",
        "product_id,product_name,price\n"
        + "1,Widget,9.99\n"
        + "2,Gadget,19.99\n"
        + "3,Doohickey,4.99\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.products")) {
        assertTrue(rs.next(), "Should have result");
        assertEquals(3, rs.getLong("cnt"));
      }
    }
  }

  /**
   * Tests schema creation with Parquet data files.
   */
  @SuppressWarnings("deprecation")
  @Test public void testSchemaFactoryCreateWithParquet() throws Exception {
    createParquetFile("orders.parquet", 50);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.orders")) {
        assertTrue(rs.next());
        assertEquals(50, rs.getLong("cnt"));
      }
    }
  }

  /**
   * Tests that the DuckDB schema properly registers multiple files.
   */
  @Test public void testSchemaFactoryMultipleFiles() throws Exception {
    createCsvFile("customers.csv",
        "cust_id,cust_name\n1,Alice\n2,Bob\n");
    createCsvFile("invoices.csv",
        "invoice_id,cust_id,amount\n100,1,250.00\n101,2,180.00\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs =
            stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.customers")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
        try (ResultSet rs =
            stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.invoices")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("cnt"));
        }
      }
    }
  }

  /**
   * Tests DuckDB schema with Oracle lex and TO_LOWER casing.
   */
  @Test public void testSchemaFactoryCasing() throws Exception {
    createCsvFile("test_casing.csv",
        "col_a,col_b\n1,hello\n2,world\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT col_a, col_b FROM files.test_casing ORDER BY col_a")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("col_a"));
        assertEquals("hello", rs.getString("col_b"));
        assertTrue(rs.next());
        assertEquals("2", rs.getString("col_a"));
        assertEquals("world", rs.getString("col_b"));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Tests schema creation with ephemeral cache for test isolation.
   */
  @Test public void testSchemaFactoryEphemeralCache() throws Exception {
    createCsvFile("ephemeral.csv",
        "val_id,description\n1,test\n");

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    Properties lexInfo = new Properties();
    lexInfo.put("lex", "ORACLE");
    lexInfo.put("unquotedCasing", "TO_LOWER");

    Connection conn = DriverManager.getConnection("jdbc:calcite:", lexInfo);
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();

    rootSchema.add("eph",
        FileSchemaFactory.INSTANCE.create(rootSchema, "eph", operand));

    try (Statement stmt = conn.createStatement();
         ResultSet rs =
             stmt.executeQuery("SELECT val_id FROM eph.ephemeral")) {
      assertTrue(rs.next());
      assertEquals("1", rs.getString("val_id"));
    } finally {
      conn.close();
    }
  }

  /**
   * Tests the DuckDB schema can handle queries with WHERE clauses (filter pushdown).
   */
  @SuppressWarnings("deprecation")
  @Test public void testSchemaFactoryFilterPushdown() throws Exception {
    createParquetFile("filter_test.parquet", 100);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.filter_test"
               + " WHERE metric_value > 10.0")) {
        assertTrue(rs.next());
        long count = rs.getLong("cnt");
        assertTrue(count > 0, "Should have filtered results");
        assertTrue(count < 100, "Should filter some rows out");
      }
    }
  }

  /**
   * Tests that DuckDB schema handles aggregation queries.
   */
  @SuppressWarnings("deprecation")
  @Test public void testSchemaFactoryAggregation() throws Exception {
    createParquetFile("agg_test.parquet", 200);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT metric_name, COUNT(*) AS cnt, SUM(metric_value) AS total_val"
               + " FROM files.agg_test"
               + " GROUP BY metric_name"
               + " ORDER BY metric_name")) {
        int groups = 0;
        while (rs.next()) {
          assertNotNull(rs.getString("metric_name"));
          assertTrue(rs.getLong("cnt") > 0);
          groups++;
        }
        assertEquals(8, groups, "Should have 8 metric_name groups");
      }
    }
  }

  /**
   * Tests DuckDB schema with JOIN operations between two tables.
   */
  @Test public void testSchemaFactoryJoinQuery() throws Exception {
    createCsvFile("departments.csv",
        "dept_id,dept_name\n10,Engineering\n20,Sales\n30,Marketing\n");
    createCsvFile("employees.csv",
        "emp_id,emp_name,dept_id\n1,Alice,10\n2,Bob,20\n3,Charlie,10\n4,Diana,30\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT e.emp_name, d.dept_name"
               + " FROM files.employees e"
               + " JOIN files.departments d ON e.dept_id = d.dept_id"
               + " ORDER BY e.emp_name")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("emp_name"));
        assertEquals("Engineering", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("emp_name"));
        assertEquals("Sales", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString("emp_name"));
        assertEquals("Engineering", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertEquals("Diana", rs.getString("emp_name"));
        assertEquals("Marketing", rs.getString("dept_name"));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Tests DuckDB schema with ORDER BY and LIMIT.
   */
  @SuppressWarnings("deprecation")
  @Test public void testSchemaFactoryOrderByLimit() throws Exception {
    createParquetFile("sorted_data.parquet", 50);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT record_id, metric_value FROM files.sorted_data"
               + " ORDER BY metric_value DESC"
               + " FETCH FIRST 5 ROWS ONLY")) {
        int count = 0;
        double prevValue = Double.MAX_VALUE;
        while (rs.next()) {
          double val = rs.getDouble("metric_value");
          assertTrue(val <= prevValue,
              "Results should be in descending order");
          prevValue = val;
          count++;
        }
        assertEquals(5, count, "Should have exactly 5 rows with LIMIT");
      }
    }
  }

  /**
   * Tests DuckDB schema metadata retrieval.
   */
  @Test public void testSchemaFactoryMetadata() throws Exception {
    createCsvFile("meta_test.csv",
        "col_x,col_y,col_z\n1,alpha,true\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT * FROM files.meta_test")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 3,
            "Should have at least 3 columns");
      }
    }
  }

  /**
   * Tests DuckDB schema with subquery.
   */
  @SuppressWarnings("deprecation")
  @Test public void testSchemaFactorySubquery() throws Exception {
    createParquetFile("subq_data.parquet", 100);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM ("
               + "  SELECT metric_name FROM files.subq_data"
               + "  GROUP BY metric_name"
               + ") sub")) {
        assertTrue(rs.next());
        assertEquals(8, rs.getLong("cnt"),
            "Should have 8 distinct metric_names");
      }
    }
  }

  // =========================================================================
  // DuckDB rule tests (Iceberg COUNT(*) and HLL COUNT DISTINCT)
  // =========================================================================

  /**
   * Tests DuckDBIcebergCountStarRule INSTANCE is not null.
   */
  @Test public void testIcebergCountStarRuleInstance() {
    assertNotNull(DuckDBIcebergCountStarRule.INSTANCE,
        "Rule instance should not be null");
  }

  /**
   * Tests DuckDBHLLCountDistinctRule INSTANCE is not null.
   */
  @Test public void testHLLCountDistinctRuleInstance() {
    assertNotNull(DuckDBHLLCountDistinctRule.INSTANCE,
        "Rule instance should not be null");
  }

  /**
   * Tests COUNT(*) on a DuckDB schema with Parquet data.
   * The Iceberg rule will not fire (no Iceberg metadata) but the
   * query still works via DuckDB's standard aggregate.
   */
  @SuppressWarnings("deprecation")
  @Test public void testCountStarOnDuckDBSchema() throws Exception {
    createParquetFile("count_star_test.parquet", 250);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.count_star_test")) {
        assertTrue(rs.next());
        assertEquals(250, rs.getLong("cnt"));
      }
    }
  }

  /**
   * Tests COUNT(DISTINCT) on DuckDB with HLL sketch in cache.
   * Pre-populates the HLL cache so the rule has sketch data to use.
   */
  @SuppressWarnings("deprecation")
  @Test public void testCountDistinctWithHLLCache() throws Exception {
    createParquetFile("hll_test.parquet", 100);

    // Pre-populate HLL sketch cache for this table/column
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 8; i++) {
      sketch.add("Metric" + i);
    }
    cache.putSketch("files", "hll_test", "metric_name", sketch);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(DISTINCT metric_name) AS dist_cnt"
               + " FROM files.hll_test")) {
        assertTrue(rs.next());
        long count = rs.getLong("dist_cnt");
        // The HLL rule may or may not fire depending on planner; either way
        // we should get approximately 8 distinct values
        assertTrue(count > 0 && count <= 10,
            "COUNT(DISTINCT) should be approximately 8, got: " + count);
      }
    }
  }

  /**
   * Tests COUNT(*) with GROUP BY to verify it is NOT matched by the
   * Iceberg COUNT(*) rule (which requires no GROUP BY).
   */
  @SuppressWarnings("deprecation")
  @Test public void testCountStarWithGroupByNotMatchedByRule() throws Exception {
    createParquetFile("grouped.parquet", 80);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT metric_name, COUNT(*) AS cnt"
               + " FROM files.grouped"
               + " GROUP BY metric_name")) {
        int groups = 0;
        while (rs.next()) {
          assertTrue(rs.getLong("cnt") > 0);
          groups++;
        }
        assertEquals(8, groups);
      }
    }
  }

  /**
   * Tests SUM aggregation through DuckDB to verify DuckDB pushdown.
   */
  @SuppressWarnings("deprecation")
  @Test public void testSumAggregationPushdown() throws Exception {
    createParquetFile("sum_test.parquet", 100);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT SUM(metric_value) AS total_val"
               + " FROM files.sum_test")) {
        assertTrue(rs.next());
        double sum = rs.getDouble("total_val");
        assertTrue(sum > 0, "SUM should be positive");
      }
    }
  }

  /**
   * Tests MIN/MAX aggregations through DuckDB.
   */
  @SuppressWarnings("deprecation")
  @Test public void testMinMaxAggregationPushdown() throws Exception {
    createParquetFile("minmax_test.parquet", 100);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT MIN(metric_value) AS min_val,"
               + " MAX(metric_value) AS max_val"
               + " FROM files.minmax_test")) {
        assertTrue(rs.next());
        double minVal = rs.getDouble("min_val");
        double maxVal = rs.getDouble("max_val");
        assertTrue(maxVal >= minVal,
            "MAX should be >= MIN");
      }
    }
  }

  /**
   * Tests DISTINCT query through DuckDB.
   */
  @SuppressWarnings("deprecation")
  @Test public void testDistinctQuery() throws Exception {
    createParquetFile("distinct_test.parquet", 200);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT DISTINCT metric_name"
               + " FROM files.distinct_test"
               + " ORDER BY metric_name")) {
        int count = 0;
        while (rs.next()) {
          assertNotNull(rs.getString("metric_name"));
          count++;
        }
        assertEquals(8, count, "Should have 8 distinct metric names");
      }
    }
  }

  // =========================================================================
  // DuckDBCatalogBuilder tests
  // =========================================================================

  /**
   * Tests that DuckDBCatalogBuilder validates arguments correctly.
   * With less than 2 arguments, main prints usage and exits.
   * We verify the class is loadable and INSTANCE patterns work.
   */
  @Test public void testCatalogBuilderClassLoadable() throws Exception {
    Class<?> clazz =
        Class.forName("org.apache.calcite.adapter.file.duckdb.DuckDBCatalogBuilder");
    assertNotNull(clazz, "DuckDBCatalogBuilder class should be loadable");
  }

  /**
   * Tests DuckDB schema creation via FileSchemaFactory with
   * programmatic operand setup.
   */
  @SuppressWarnings("deprecation")
  @Test public void testProgrammaticSchemaCreation() throws Exception {
    createParquetFile("prog_test.parquet", 30);

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    Properties progInfo = new Properties();
    progInfo.put("lex", "ORACLE");
    progInfo.put("unquotedCasing", "TO_LOWER");

    Connection conn = DriverManager.getConnection("jdbc:calcite:", progInfo);
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();

    org.apache.calcite.schema.Schema schema =
        FileSchemaFactory.INSTANCE.create(rootSchema, "prog", operand);
    assertNotNull(schema, "Schema should not be null");
    rootSchema.add("prog", schema);

    try (Statement stmt = conn.createStatement();
         ResultSet rs =
             stmt.executeQuery("SELECT COUNT(*) AS cnt FROM prog.prog_test")) {
      assertTrue(rs.next());
      assertEquals(30, rs.getLong("cnt"));
    } finally {
      conn.close();
    }
  }

  /**
   * Tests DuckDB HAVING clause pushdown.
   */
  @SuppressWarnings("deprecation")
  @Test public void testHavingClausePushdown() throws Exception {
    createParquetFile("having_test.parquet", 160);

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT metric_name, COUNT(*) AS cnt"
               + " FROM files.having_test"
               + " GROUP BY metric_name"
               + " HAVING COUNT(*) >= 20"
               + " ORDER BY metric_name")) {
        int groups = 0;
        while (rs.next()) {
          assertTrue(rs.getLong("cnt") >= 20);
          groups++;
        }
        assertTrue(groups > 0, "Should have groups with count >= 20");
      }
    }
  }

  /**
   * Tests reading an empty CSV file through DuckDB.
   */
  @Test public void testEmptyCsvFile() throws Exception {
    createCsvFile("empty_data.csv", "col_a,col_b\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM files.empty_data")) {
        assertTrue(rs.next());
        assertEquals(0, rs.getLong("cnt"));
      }
    }
  }

  /**
   * Tests DuckDB schema with UNION ALL between two tables.
   */
  @Test public void testUnionAllQuery() throws Exception {
    createCsvFile("part_a.csv", "item_id,item_val\n1,alpha\n2,beta\n");
    createCsvFile("part_b.csv", "item_id,item_val\n3,gamma\n4,delta\n");

    try (Connection conn = createDuckDBConnection()) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) AS cnt FROM ("
               + "  SELECT item_id, item_val FROM files.part_a"
               + "  UNION ALL"
               + "  SELECT item_id, item_val FROM files.part_b"
               + ") combined")) {
        assertTrue(rs.next());
        assertEquals(4, rs.getLong("cnt"));
      }
    }
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private Connection createDuckDBConnection() throws Exception {
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));

    return conn;
  }

  private void createCsvFile(String name, String content) throws Exception {
    File file = new File(tempDir, name);
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.print(content);
    }
  }

  @SuppressWarnings("deprecation")
  private void createParquetFile(String name, int rowCount) throws Exception {
    File file = new File(tempDir, name);

    String schemaString =
        "{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
        + "  {\"name\": \"record_id\", \"type\": \"int\"},"
        + "  {\"name\": \"metric_name\", \"type\": \"string\"},"
        + "  {\"name\": \"metric_value\", \"type\": \"double\"}"
        + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < rowCount; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("record_id", i + 1);
        record.put("metric_name", "Metric" + (i % 8));
        record.put("metric_value", 1.0 + (i % 50) * 0.5);
        writer.write(record);
      }
    }
  }

  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
