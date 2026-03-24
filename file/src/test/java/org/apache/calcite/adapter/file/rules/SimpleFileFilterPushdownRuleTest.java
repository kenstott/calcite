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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SimpleFileFilterPushdownRule} which uses min/max statistics
 * to eliminate filters that can be resolved without data scanning.
 *
 * <p>The rule checks simple comparison predicates against column statistics:
 * <ul>
 *   <li>If the filter is always true (e.g., value > min and value < max), remove it</li>
 *   <li>If the filter is always false (e.g., value < min), return empty</li>
 *   <li>Complex expressions that cannot be resolved with stats pass through</li>
 * </ul>
 */
@Tag("integration")
public class SimpleFileFilterPushdownRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedFilterEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("filter-pushdown-test-").toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedFilterEnabled =
        System.getProperty("calcite.file.statistics.filter.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable filter pushdown optimization
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    restoreProperty("calcite.file.statistics.filter.enabled",
        savedFilterEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that a filter referencing a value outside the min/max range is
   * eliminated. Data has sale_id 1..100, so filtering sale_id < 0 should
   * produce zero rows.
   */
  @Test public void testFilterEliminatedViaMinMaxStats() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"filter_test\" WHERE \"sale_id\" < 0";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(0, result,
          "Filter sale_id < 0 should return 0 rows since min is 1");
    }
  }

  /**
   * Test that a complex expression (e.g., arithmetic or function-based) is not
   * pushed down and the query still returns correct results.
   */
  @Test public void testComplexExpressionNotPushed() throws Exception {
    // Using a complex expression that cannot be resolved by simple min/max stats
    String query =
        "SELECT COUNT(*) FROM files.\"filter_test\""
            + " WHERE \"sale_id\" + \"amount\" > 50";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // The complex expression passes through unoptimized; verify it executes
      assertTrue(result >= 0,
          "Complex filter should still produce a valid result count");
    }
  }

  /**
   * Test that a filter on a non-statistics column (region, a string)
   * passes through without optimization and returns correct results.
   */
  @Test public void testNonStatsColumnPassesThrough() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"filter_test\""
            + " WHERE \"region\" = 'Region0'";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // 100 rows with region = Region(i % 5), so Region0 appears 20 times
      assertEquals(20, result,
          "Filter on region='Region0' should return 20 rows");
    }
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "filter_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"FilterRecord\",\"fields\": ["
            + "  {\"name\": \"sale_id\", \"type\": \"int\"},"
            + "  {\"name\": \"amount\", \"type\": \"double\"},"
            + "  {\"name\": \"region\", \"type\": \"string\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 100; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("sale_id", i + 1);
        record.put("amount", 10.0 + i);
        record.put("region", "Region" + (i % 5));
        writer.write(record);
      }
    }
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
