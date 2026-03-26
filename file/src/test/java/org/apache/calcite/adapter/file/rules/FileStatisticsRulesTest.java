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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileStatisticsRules}, the utility class that provides
 * cost-based optimization helpers such as selectivity estimation and
 * statistics retrieval.
 *
 * <p>FileStatisticsRules is a utility class (no instances) with:
 * <ul>
 *   <li>String constants for rule names</li>
 *   <li>estimateSelectivity() for filter cost estimation</li>
 *   <li>getTableStatistics() for statistics access</li>
 * </ul>
 */
@Tag("integration")
public class FileStatisticsRulesTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedFilterEnabled;
  private String savedJoinReorderEnabled;
  private String savedColumnPruningEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("file-stats-rules-test-").toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedFilterEnabled =
        System.getProperty("calcite.file.statistics.filter.enabled");
    savedJoinReorderEnabled =
        System.getProperty("calcite.file.statistics.join.reorder.enabled");
    savedColumnPruningEnabled =
        System.getProperty("calcite.file.statistics.column.pruning.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable all statistics-based optimizations
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled",
        "true");
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

  /**
   * Verify that the rule name constants are properly defined and accessible.
   * These are used for rule registration and identification.
   */
  @Test public void testRulesRegisteredCorrectly() throws Exception {
    assertNotNull(FileStatisticsRules.STATISTICS_FILTER_PUSHDOWN_NAME,
        "Filter pushdown rule name should not be null");
    assertNotNull(FileStatisticsRules.STATISTICS_JOIN_REORDER_NAME,
        "Join reorder rule name should not be null");
    assertNotNull(FileStatisticsRules.STATISTICS_COLUMN_PRUNING_NAME,
        "Column pruning rule name should not be null");

    assertEquals("FileStatisticsRules:FilterPushdown",
        FileStatisticsRules.STATISTICS_FILTER_PUSHDOWN_NAME,
        "Filter pushdown name mismatch");
    assertEquals("FileStatisticsRules:JoinReorder",
        FileStatisticsRules.STATISTICS_JOIN_REORDER_NAME,
        "Join reorder name mismatch");
    assertEquals("FileStatisticsRules:ColumnPruning",
        FileStatisticsRules.STATISTICS_COLUMN_PRUNING_NAME,
        "Column pruning name mismatch");
  }

  /**
   * Test that statistics are propagated through the plan by verifying that
   * queries work correctly with all optimizations enabled. The selectivity
   * estimate helper returns 0.3 as default, so queries with filters should
   * still execute properly.
   */
  @Test public void testStatisticsPropagatedThroughPlan() throws Exception {
    // Run a query with a filter that exercises the statistics path
    String query =
        "SELECT \"value\" FROM files.\"stats_test\""
            + " WHERE \"id\" > 50 ORDER BY \"id\" LIMIT 5";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        double value = rs.getDouble(1);
        assertTrue(value >= 0,
            "Value should be non-negative, got: " + value);
        count++;
      }
      assertEquals(5, count, "LIMIT 5 should return exactly 5 rows");
    }
  }

  /**
   * Test the estimateSelectivity method with null statistics returns the
   * default selectivity value of 0.3.
   */
  @Test public void testEstimateSelectivityDefault() throws Exception {
    double selectivity =
        FileStatisticsRules.estimateSelectivity(null, null);
    assertEquals(0.3, selectivity, 0.001,
        "Default selectivity with null stats should be 0.3");
  }

  /**
   * Test the getTableStatistics method with a non-scan object returns null.
   */
  @Test public void testGetTableStatisticsNull() throws Exception {
    Object result = FileStatisticsRules.getTableStatistics("not a scan");
    assertEquals(null, result,
        "getTableStatistics with invalid input should return null");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "stats_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"StatsRecord\",\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"int\"},"
            + "  {\"name\": \"value\", \"type\": \"double\"},"
            + "  {\"name\": \"category\", \"type\": \"string\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 300; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i + 1);
        record.put("value", i * 2.5);
        record.put("category", "Group" + (i % 6));
        writer.write(record);
      }
    }
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
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
