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
 * Tests for {@link SimpleFileColumnPruningRule} which identifies unused columns
 * in a projection over a Parquet table scan and estimates I/O savings.
 *
 * <p>The rule fires when:
 * <ul>
 *   <li>Column pruning is enabled via system property</li>
 *   <li>The scan targets a ParquetTranslatableTable with statistics</li>
 *   <li>The estimated I/O savings exceed 15%</li>
 * </ul>
 */
@Tag("integration")
public class SimpleFileColumnPruningRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedColumnPruningEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("column-pruning-test-").toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedColumnPruningEnabled =
        System.getProperty("calcite.file.statistics.column.pruning.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable column pruning
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

    restoreProperty("calcite.file.statistics.column.pruning.enabled",
        savedColumnPruningEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that selecting a single column out of five returns only that column.
   * The column pruning rule should identify the unused columns.
   */
  @Test public void testUnusedColumnsRemovedFromPlan() throws Exception {
    String query =
        "SELECT \"col_a\" FROM files.\"prune_test\" LIMIT 10";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        rs.getInt(1); // Verify column is readable
        count++;
      }
      assertEquals(10, count, "LIMIT 10 should return exactly 10 rows");
      assertEquals(1, rs.getMetaData().getColumnCount(),
          "Should project only 1 column (col_a)");
    }
  }

  /**
   * Test that selecting all columns means no pruning opportunity.
   * All columns should be present in the result.
   */
  @Test public void testAllColumnsUsedNoChange() throws Exception {
    String query =
        "SELECT \"col_a\", \"col_b\", \"col_c\", \"col_d\", \"col_e\""
            + " FROM files.\"prune_test\" LIMIT 5";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(5, count, "LIMIT 5 should return exactly 5 rows");
      assertEquals(5, rs.getMetaData().getColumnCount(),
          "All 5 columns should be present");
    }
  }

  /**
   * Test that SELECT * preserves all columns and returns the correct
   * number of columns.
   */
  @Test public void testSelectStarPreservesAllColumns() throws Exception {
    String query = "SELECT * FROM files.\"prune_test\" LIMIT 5";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(5, count, "LIMIT 5 should return exactly 5 rows");
      assertTrue(rs.getMetaData().getColumnCount() >= 5,
          "SELECT * should preserve all columns, got: "
              + rs.getMetaData().getColumnCount());
    }
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "prune_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"PruneRecord\",\"fields\": ["
            + "  {\"name\": \"col_a\", \"type\": \"int\"},"
            + "  {\"name\": \"col_b\", \"type\": \"int\"},"
            + "  {\"name\": \"col_c\", \"type\": \"double\"},"
            + "  {\"name\": \"col_d\", \"type\": \"string\"},"
            + "  {\"name\": \"col_e\", \"type\": \"double\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 200; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("col_a", i);
        record.put("col_b", i * 2);
        record.put("col_c", i * 3.14);
        record.put("col_d", "Value" + (i % 20));
        record.put("col_e", i * 1.5);
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
