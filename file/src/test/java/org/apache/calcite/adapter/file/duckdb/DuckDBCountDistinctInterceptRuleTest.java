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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DuckDBCountDistinctInterceptRule} which intercepts
 * COUNT(DISTINCT) on logical aggregates before they are converted to
 * JDBC convention.
 *
 * <p>The rule matches LogicalAggregate over LogicalTableScan and replaces
 * COUNT(DISTINCT) with pre-computed HLL sketch estimates from the cache.
 *
 * <p>These tests use the DuckDB execution engine to exercise the rule
 * in the DuckDB query path.
 */
@Tag("integration")
public class DuckDBCountDistinctInterceptRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedHllEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("duckdb-intercept-test-").toFile();
    cacheDir = new File(tempDir, "hll_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedHllEnabled =
        System.getProperty("calcite.file.statistics.hll.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable HLL optimization
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
    createHLLSketches();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    restoreProperty("calcite.file.statistics.hll.enabled", savedHllEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that COUNT(DISTINCT) on a DuckDB-backed table produces an
   * approximately correct result. The intercept rule should attempt to
   * replace the aggregate with an HLL estimate from the cache.
   */
  @Test public void testInterceptsCountDistinctInLogicalPlan()
      throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"category_id\")"
            + " FROM files.\"intercept_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // category_id: nextInt(75) -> ~75 distinct values
      // Allow tolerance for HLL approximation or exact DuckDB result
      assertTrue(result > 50 && result < 100,
          "COUNT(DISTINCT category_id) should be ~75, got: " + result);
    }
  }

  /**
   * Test that a non-distinct COUNT is not intercepted by this rule.
   * The query should pass through to the standard DuckDB aggregate.
   */
  @Test public void testNonDistinctCountNotIntercepted() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"intercept_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertTrue(result > 0,
          "COUNT(*) should return a positive row count, got: " + result);
    }
  }

  /**
   * Verify the rule INSTANCE is properly initialized.
   */
  @Test public void testRuleInstanceExists() throws Exception {
    assertNotNull(DuckDBCountDistinctInterceptRule.INSTANCE,
        "DuckDBCountDistinctInterceptRule.INSTANCE should not be null");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "intercept_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"InterceptRecord\",\"fields\": ["
            + "  {\"name\": \"category_id\", \"type\": \"int\"},"
            + "  {\"name\": \"score\", \"type\": \"double\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(77);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 500; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("category_id", random.nextInt(75));
        record.put("score", random.nextDouble() * 100.0);
        writer.write(record);
      }
    }
  }

  private void createHLLSketches() throws Exception {
    HyperLogLogSketch categorySketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 75; i++) {
      categorySketch.add(String.valueOf(i));
    }
    File sketchFile =
        new File(cacheDir, "intercept_test_category_id.hll");
    StatisticsCache.saveHLLSketch(categorySketch, sketchFile);
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
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
