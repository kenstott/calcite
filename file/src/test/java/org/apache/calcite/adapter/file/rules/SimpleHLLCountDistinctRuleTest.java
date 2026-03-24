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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SimpleHLLCountDistinctRule} which replaces COUNT(DISTINCT)
 * operations with pre-computed HLL sketch estimates via the "simple" path.
 *
 * <p>This rule has two instances:
 * <ul>
 *   <li>INSTANCE: Applies to all COUNT(DISTINCT) (original behavior)</li>
 *   <li>APPROX_ONLY_INSTANCE: Only applies to APPROX_COUNT_DISTINCT</li>
 * </ul>
 *
 * <p>The rule only fires when there is no GROUP BY clause and the aggregate
 * includes at least one COUNT(DISTINCT) call with an available HLL sketch.
 */
@Tag("integration")
public class SimpleHLLCountDistinctRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedHllEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("simple-hll-test-").toFile();
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
   * Test that COUNT(DISTINCT) on a column with an HLL sketch returns an
   * approximately correct result. The SimpleHLLCountDistinctRule should
   * intercept this and return the HLL estimate.
   */
  @Test public void testCountDistinctRewrittenToHLL() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"region_id\") FROM files.\"simple_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // region_id: nextInt(30) -> ~30 distinct values
      assertTrue(result > 20 && result < 40,
          "COUNT(DISTINCT region_id) should be ~30, got: " + result);
    }
  }

  /**
   * Test that a non-distinct COUNT is not intercepted by the rule.
   */
  @Test public void testNonDistinctNotMatched() throws Exception {
    String query = "SELECT COUNT(*) FROM files.\"simple_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(800, result,
          "Non-distinct COUNT(*) should return exact row count of 800");
    }
  }

  /**
   * Test COUNT(DISTINCT) on multiple columns in the same query.
   * Each COUNT(DISTINCT) should get its own HLL estimate.
   */
  @Test public void testMultipleDistinctColumns() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"region_id\"), COUNT(DISTINCT \"dept_id\")"
            + " FROM files.\"simple_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long regionDistinct = rs.getLong(1);
      long deptDistinct = rs.getLong(2);

      // region_id: ~30 distinct (nextInt(30))
      assertTrue(regionDistinct > 20 && regionDistinct < 40,
          "COUNT(DISTINCT region_id) should be ~30, got: " + regionDistinct);
      // dept_id: ~10 distinct (nextInt(10))
      assertTrue(deptDistinct > 5 && deptDistinct < 15,
          "COUNT(DISTINCT dept_id) should be ~10, got: " + deptDistinct);
    }
  }

  /**
   * Verify that the rule instances are available and properly configured.
   */
  @Test public void testRuleInstancesExist() throws Exception {
    assertNotNull(SimpleHLLCountDistinctRule.INSTANCE,
        "INSTANCE should not be null");
    assertNotNull(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE,
        "APPROX_ONLY_INSTANCE should not be null");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "simple_hll_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"SimpleHLLRecord\",\"fields\": ["
            + "  {\"name\": \"region_id\", \"type\": \"int\"},"
            + "  {\"name\": \"dept_id\", \"type\": \"int\"},"
            + "  {\"name\": \"salary\", \"type\": \"double\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(99);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 800; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("region_id", random.nextInt(30));
        record.put("dept_id", random.nextInt(10));
        record.put("salary", 30000.0 + random.nextDouble() * 70000.0);
        writer.write(record);
      }
    }
  }

  private void createHLLSketches() throws Exception {
    // Create HLL sketch for region_id
    HyperLogLogSketch regionSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 30; i++) {
      regionSketch.add(String.valueOf(i));
    }
    File regionFile =
        new File(cacheDir, "simple_hll_test_region_id.hll");
    StatisticsCache.saveHLLSketch(regionSketch, regionFile);

    // Create HLL sketch for dept_id
    HyperLogLogSketch deptSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 10; i++) {
      deptSketch.add(String.valueOf(i));
    }
    File deptFile =
        new File(cacheDir, "simple_hll_test_dept_id.hll");
    StatisticsCache.saveHLLSketch(deptSketch, deptFile);
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
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
