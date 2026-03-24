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
package org.apache.calcite.adapter.file.statistics;

import org.apache.calcite.adapter.file.FileSchemaFactory;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link CachePrimer} covering:
 * <ul>
 *   <li>Schema priming with CSV tables</li>
 *   <li>Schema priming with Parquet tables</li>
 *   <li>Multi-schema priming</li>
 *   <li>Parallel priming</li>
 *   <li>Error handling (missing schema)</li>
 *   <li>PrimingResult and TableInfo</li>
 *   <li>Cache clearing</li>
 *   <li>primeForTesting utility</li>
 * </ul>
 */
@Tag("integration")
public class CachePrimerCoverageTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CachePrimerCoverageTest.class);

  private File tempDir;
  private File tempDir2;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("cache-primer-cov-").toFile();
    tempDir2 = Files.createTempDirectory("cache-primer-cov2-").toFile();
  }

  @AfterEach
  public void tearDown() {
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
    if (tempDir2 != null && tempDir2.exists()) {
      deleteDirectory(tempDir2);
    }
  }

  // =========================================================================
  // primeSchema tests
  // =========================================================================

  /**
   * Tests priming a schema with CSV files.
   */
  @Test public void testPrimeSchemaWithCsvFiles() throws Exception {
    createCsvFile(tempDir, "table_a.csv",
        "col_id,col_name\n1,Alice\n2,Bob\n");
    createCsvFile(tempDir, "table_b.csv",
        "item_id,item_price\n100,9.99\n200,19.99\n300,29.99\n");

    try (Connection conn = createConnection("test_csv", tempDir)) {
      CachePrimer.PrimingResult result =
          CachePrimer.primeSchema(conn, "test_csv");

      assertNotNull(result, "Priming result should not be null");
      assertTrue(result.totalTables >= 2,
          "Should have at least 2 tables, got: " + result.totalTables);
      assertEquals(0, result.failed,
          "No tables should fail to prime");
      assertTrue(result.totalTimeMs >= 0,
          "Total time should be non-negative");
      assertNotNull(result.tableTimings,
          "Table timings should not be null");

      // Verify printSummary doesn't throw
      result.printSummary();
    }
  }

  /**
   * Tests priming a schema with Parquet files.
   */
  @SuppressWarnings("deprecation")
  @Test public void testPrimeSchemaWithParquetFiles() throws Exception {
    createParquetFile(tempDir, "parq_table.parquet", 50);

    try (Connection conn = createConnection("test_parquet", tempDir)) {
      CachePrimer.PrimingResult result =
          CachePrimer.primeSchema(conn, "test_parquet");

      assertNotNull(result);
      assertTrue(result.totalTables >= 1,
          "Should have at least 1 table");
      assertTrue(result.successfullyPrimed >= 1,
          "At least 1 table should be primed");
    }
  }

  /**
   * Tests priming with a nonexistent schema name throws.
   */
  @Test public void testPrimeSchemaNotFound() throws Exception {
    createCsvFile(tempDir, "dummy.csv", "val_a\n1\n");

    try (Connection conn = createConnection("real_schema", tempDir)) {
      assertThrows(IllegalArgumentException.class,
          () -> CachePrimer.primeSchema(conn, "nonexistent_schema"),
          "Should throw for nonexistent schema");
    }
  }

  // =========================================================================
  // primeSchemas (multiple) tests
  // =========================================================================

  /**
   * Tests priming multiple schemas simultaneously.
   */
  @Test public void testPrimeMultipleSchemas() throws Exception {
    createCsvFile(tempDir, "alpha.csv",
        "val_x\n1\n2\n3\n");
    createCsvFile(tempDir2, "beta.csv",
        "val_y\n10\n20\n30\n");

    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();

    Map<String, Object> op1 = new LinkedHashMap<>();
    op1.put("directory", tempDir.toString());
    op1.put("ephemeralCache", true);
    rootSchema.add("schema_a",
        FileSchemaFactory.INSTANCE.create(rootSchema, "schema_a", op1));

    Map<String, Object> op2 = new LinkedHashMap<>();
    op2.put("directory", tempDir2.toString());
    op2.put("ephemeralCache", true);
    rootSchema.add("schema_b",
        FileSchemaFactory.INSTANCE.create(rootSchema, "schema_b", op2));

    try {
      CachePrimer.PrimingResult result =
          CachePrimer.primeSchemas(conn, "schema_a", "schema_b");

      assertNotNull(result);
      assertTrue(result.totalTables >= 2,
          "Should have tables from both schemas");
      assertEquals(0, result.failed);
    } finally {
      conn.close();
    }
  }

  // =========================================================================
  // Parallel priming tests
  // =========================================================================

  /**
   * Tests parallel cache priming with multiple tables.
   */
  @SuppressWarnings("deprecation")
  @Test public void testParallelPriming() throws Exception {
    // Create several CSV files
    for (int i = 0; i < 5; i++) {
      createCsvFile(tempDir, "parallel_" + i + ".csv",
          "val_id,val_data\n" + i + ",data_" + i + "\n");
    }

    try (Connection conn = createConnection("parallel_schema", tempDir)) {
      // Collect tables manually
      CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calcConn.getRootSchema();
      SchemaPlus schema = rootSchema.getSubSchema("parallel_schema");
      assertNotNull(schema);

      List<CachePrimer.TableInfo> tables = new ArrayList<>();
      for (String tableName : schema.getTableNames()) {
        tables.add(new CachePrimer.TableInfo(
            "parallel_schema", tableName,
            schema.getTable(tableName), null));
      }

      CachePrimer.PrimingResult result =
          CachePrimer.primeTablesParallel(conn, tables, 2);

      assertNotNull(result);
      assertTrue(result.successfullyPrimed >= tables.size(),
          "All tables should be primed in parallel");
      assertEquals(0, result.failed);
    }
  }

  // =========================================================================
  // Cache clearing tests
  // =========================================================================

  /**
   * Tests clearing all caches.
   */
  @Test public void testClearAllCaches() throws Exception {
    createCsvFile(tempDir, "clearable.csv",
        "val_a\n1\n2\n");

    try (Connection conn = createConnection("clear_schema", tempDir)) {
      // Prime first
      CachePrimer.primeSchema(conn, "clear_schema");

      // Clear - should not throw
      CachePrimer.clearAllCaches(conn);
    }
  }

  // =========================================================================
  // primeForTesting utility tests
  // =========================================================================

  /**
   * Tests the primeForTesting utility method.
   */
  @Test public void testPrimeForTesting() throws Exception {
    createCsvFile(tempDir, "testutil.csv",
        "val_a\n1\n2\n");

    // Build a model string
    String model = buildModel("test_util", tempDir.toString());
    String jdbcUrl = "jdbc:calcite:model=inline:" + model;

    CachePrimer.PrimingResult result =
        CachePrimer.primeForTesting(jdbcUrl, "test_util");

    assertNotNull(result);
    assertTrue(result.totalTables >= 1,
        "Should find at least 1 table");
  }

  /**
   * Tests primeForTesting with invalid URL handles error gracefully.
   */
  @Test public void testPrimeForTestingInvalidUrl() {
    CachePrimer.PrimingResult result =
        CachePrimer.primeForTesting(
            "jdbc:calcite:model=inline:{\"version\":\"1.0\","
            + "\"schemas\":[{\"name\":\"x\",\"type\":\"custom\","
            + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
            + "\"operand\":{\"directory\":\"/nonexistent_dir_xyz123\","
            + "\"ephemeralCache\":true}}]}",
            "x");

    assertNotNull(result);
    // The priming may succeed with 0 tables or report failures
    // Either way, it should not throw
  }

  // =========================================================================
  // TableInfo and PrimingResult tests
  // =========================================================================

  /**
   * Tests TableInfo toString formatting.
   */
  @Test public void testTableInfoToString() {
    CachePrimer.TableInfo info =
        new CachePrimer.TableInfo("myschema", "mytable", null, null);

    String str = info.toString();
    assertNotNull(str);
    assertTrue(str.contains("myschema"),
        "Should contain schema name");
    assertTrue(str.contains("mytable"),
        "Should contain table name");
  }

  /**
   * Tests PrimingResult with failures list.
   */
  @Test public void testPrimingResultWithFailures() {
    List<String> failures = new ArrayList<>();
    failures.add("table1: connection error");
    failures.add("table2: timeout");

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        5, 3, 2, 1000L, 2048L,
        Collections.singletonMap("table1", 500L),
        failures);

    assertEquals(5, result.totalTables);
    assertEquals(3, result.successfullyPrimed);
    assertEquals(2, result.failed);
    assertEquals(1000L, result.totalTimeMs);
    assertEquals(2048L, result.totalBytesProcessed);
    assertEquals(2, result.failures.size());

    // printSummary should not throw
    result.printSummary();
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  @SuppressWarnings("deprecation")
  private Connection createConnection(String schemaName, File dir)
      throws Exception {
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", dir.toString());
    operand.put("ephemeralCache", true);

    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calcConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calcConn.getRootSchema();

    rootSchema.add(schemaName,
        FileSchemaFactory.INSTANCE.create(rootSchema, schemaName, operand));

    return conn;
  }

  private void createCsvFile(File dir, String name, String content)
      throws Exception {
    File file = new File(dir, name);
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.print(content);
    }
  }

  @SuppressWarnings("deprecation")
  private void createParquetFile(File dir, String name, int rowCount)
      throws Exception {
    File file = new File(dir, name);

    String schemaString =
        "{\"type\": \"record\", \"name\": \"TestRec\", \"fields\": ["
        + "  {\"name\": \"rec_id\", \"type\": \"int\"},"
        + "  {\"name\": \"rec_name\", \"type\": \"string\"},"
        + "  {\"name\": \"rec_value\", \"type\": \"double\"}"
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
        record.put("rec_id", i + 1);
        record.put("rec_name", "Item" + (i % 10));
        record.put("rec_value", (i + 1) * 2.5);
        writer.write(record);
      }
    }
  }

  private String buildModel(String schemaName, String directory) {
    return "{\"version\":\"1.0\","
        + "\"defaultSchema\":\"" + schemaName + "\","
        + "\"schemas\":[{"
        + "\"name\":\"" + schemaName + "\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + directory.replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true"
        + "}}]}";
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
