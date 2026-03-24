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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
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

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that verify optimization rules are registered and produce correct results.
 * Uses a 1000-row Parquet test dataset with known statistics.
 */
@Tag("integration")
public class OptimizationRulesTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedHllEnabled;
  private String savedFilterEnabled;
  private String savedJoinReorderEnabled;
  private String savedColumnPruningEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("optimization-rules-test-").toFile();
    cacheDir = new File(tempDir, "hll_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedHllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    savedFilterEnabled = System.getProperty("calcite.file.statistics.filter.enabled");
    savedJoinReorderEnabled =
        System.getProperty("calcite.file.statistics.join.reorder.enabled");
    savedColumnPruningEnabled =
        System.getProperty("calcite.file.statistics.column.pruning.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable all optimizations
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");

    createTestData();
    createHLLSketches();
    setupCalciteConnection("parquet");
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    // Restore system properties
    restoreProperty("calcite.file.statistics.hll.enabled", savedHllEnabled);
    restoreProperty("calcite.file.statistics.filter.enabled",
        savedFilterEnabled);
    restoreProperty("calcite.file.statistics.join.reorder.enabled",
        savedJoinReorderEnabled);
    restoreProperty("calcite.file.statistics.column.pruning.enabled",
        savedColumnPruningEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
    }
  }

  @Test public void testHLLCountDistinctOptimization() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"customer_id\") FROM files.\"optimization_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // With Random(42) and nextInt(246), expect ~246 distinct values
      // HLL approximation should be close
      assertTrue(result > 200 && result < 300,
          "COUNT(DISTINCT customer_id) should be approximately 246, got: "
              + result);
    }
  }

  @Test public void testFilterPushdownEliminatesImpossibleFilter()
      throws Exception {
    // customer_id values are 0-245 (nextInt(246)), so < 0 should yield 0 rows
    String query =
        "SELECT COUNT(*) FROM files.\"optimization_test\""
            + " WHERE \"customer_id\" < 0";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(0, result,
          "Filter customer_id < 0 should return 0 rows");
    }
  }

  @Test public void testColumnPruningReducesColumns() throws Exception {
    // Select only one column - column pruning should avoid reading others
    String query =
        "SELECT \"customer_id\" FROM files.\"optimization_test\" LIMIT 5";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        rs.getInt(1); // Verify column is readable
        count++;
      }
      assertEquals(5, count, "LIMIT 5 should return exactly 5 rows");
      assertEquals(1, rs.getMetaData().getColumnCount(),
          "Should project only 1 column");
    }
  }

  @Test public void testParquetAndDuckDBConsistentResults() throws Exception {
    // Get row count with parquet engine
    long parquetCount;
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT COUNT(*) FROM files.\"optimization_test\"")) {
      assertTrue(rs.next(), "Should have a result row");
      parquetCount = rs.getLong(1);
    }

    assertEquals(1000, parquetCount,
        "Parquet engine should report 1000 rows");

    // Get row count with DuckDB engine
    if (calciteConn != null) {
      calciteConn.close();
    }
    setupCalciteConnection("duckdb");

    long duckdbCount;
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT COUNT(*) FROM files.\"optimization_test\"")) {
      assertTrue(rs.next(), "Should have a result row");
      duckdbCount = rs.getLong(1);
    }

    assertEquals(parquetCount, duckdbCount,
        "Parquet and DuckDB should return same row count");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "optimization_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"OptRecord\",\"fields\": ["
            + "  {\"name\": \"customer_id\", \"type\": \"int\"},"
            + "  {\"name\": \"product_id\", \"type\": \"int\"},"
            + "  {\"name\": \"amount\", \"type\": \"double\"},"
            + "  {\"name\": \"region\", \"type\": \"string\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 1000; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", random.nextInt(246));
        record.put("product_id", random.nextInt(100));
        record.put("amount", 10.0 + random.nextDouble() * 1000.0);
        record.put("region", "Region" + (i % 5));
        writer.write(record);
      }
    }
  }

  private void createHLLSketches() throws Exception {
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 246; i++) {
      sketch.add(String.valueOf(i));
    }
    File sketchFile =
        new File(cacheDir, "optimization_test_customer_id.hll");
    StatisticsCache.saveHLLSketch(sketch, sketchFile);
  }

  private void setupCalciteConnection(String engine) throws Exception {
    calciteConn = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", engine);
    operand.put("ephemeralCache", true);
    operand.put("tableNameCasing", "LOWER");
    operand.put("columnNameCasing", "LOWER");

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
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
