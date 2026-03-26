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
 * Tests for {@link SimpleFileJoinReorderRule} which uses table statistics to
 * place smaller tables on the right side (build side) of joins for better
 * hash join performance.
 *
 * <p>The rule only reorders when:
 * <ul>
 *   <li>Statistics are available for both sides</li>
 *   <li>The cost difference exceeds 25%</li>
 *   <li>The join type allows reordering (INNER, FULL, certain RIGHT joins)</li>
 * </ul>
 */
@Tag("integration")
public class SimpleFileJoinReorderRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedJoinReorderEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("join-reorder-test-").toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedJoinReorderEnabled =
        System.getProperty("calcite.file.statistics.join.reorder.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable join reorder optimization
    System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
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

    restoreProperty("calcite.file.statistics.join.reorder.enabled",
        savedJoinReorderEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that joining a large table (1000 rows) with a small table (10 rows)
   * produces the correct result count regardless of join ordering.
   * The rule should place the smaller table on the build side.
   */
  @Test public void testSmallTableMovedToBuildSide() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"large_table\" l"
            + " JOIN files.\"small_table\" s"
            + " ON l.\"join_key\" = s.\"join_key\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // large_table has join_key = i % 10, small_table has join_key = 0..9
      // Each small_table row matches 100 rows in large_table -> 10 * 100 = 1000
      assertEquals(1000, result,
          "Join should produce 1000 matched rows");
    }
  }

  /**
   * Test that two equal-sized tables joined together produce the correct
   * result. When sizes are equal, no reordering should be applied.
   */
  @Test public void testEqualSizeTablesUnchanged() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"equal_a\" a"
            + " JOIN files.\"equal_b\" b"
            + " ON a.\"join_key\" = b.\"join_key\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // Both tables have 50 rows with join_key = 0..49, so 50 matches
      assertEquals(50, result,
          "Equal-size join should produce 50 matched rows");
    }
  }

  /**
   * Test a three-way join: large JOIN medium JOIN small.
   * Verifies that the correct result is produced with reordering enabled.
   */
  @Test public void testThreeWayJoinReordering() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"large_table\" l"
            + " JOIN files.\"equal_a\" m ON l.\"join_key\" = m.\"join_key\""
            + " JOIN files.\"small_table\" s ON m.\"join_key\" = s.\"join_key\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // large_table: join_key 0..9 (100 each), equal_a: join_key 0..49,
      // small_table: join_key 0..9
      // l JOIN m: join_key 0..9 match -> 10 * 100 = 1000
      // result JOIN s: join_key 0..9 match -> 1000 matches
      assertEquals(1000, result,
          "Three-way join should produce 1000 matched rows");
    }
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    // Large table: 1000 rows
    createParquetFile("large_table.parquet", 1000, 10);
    // Small table: 10 rows
    createParquetFile("small_table.parquet", 10, 10);
    // Equal-size tables: 50 rows each
    createParquetFile("equal_a.parquet", 50, 50);
    createParquetFile("equal_b.parquet", 50, 50);
  }

  @SuppressWarnings("deprecation")
  private void createParquetFile(String fileName, int rowCount,
      int keyRange) throws Exception {
    File file = new File(tempDir, fileName);

    String schemaString =
        "{\"type\": \"record\",\"name\": \"JoinRecord\",\"fields\": ["
            + "  {\"name\": \"join_key\", \"type\": \"int\"},"
            + "  {\"name\": \"value\", \"type\": \"double\"}"
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
        record.put("join_key", i % keyRange);
        record.put("value", 100.0 + i);
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
