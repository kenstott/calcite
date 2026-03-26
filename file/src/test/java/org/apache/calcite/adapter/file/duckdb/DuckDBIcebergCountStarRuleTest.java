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
 * Tests for {@link DuckDBIcebergCountStarRule} which replaces COUNT(*)
 * operations on DuckDB/JDBC views backed by Iceberg tables with
 * pre-computed row counts from Iceberg metadata.
 *
 * <p>The rule matches:
 * <ul>
 *   <li>Aggregate with no GROUP BY</li>
 *   <li>Single COUNT(*) call (no arguments, not distinct)</li>
 *   <li>Table is ICEBERG_PARQUET with cached row count</li>
 * </ul>
 *
 * <p>Since Iceberg metadata is only available in production environments,
 * these tests verify the rule's behavior with regular DuckDB-backed Parquet
 * files to confirm the query path works correctly. The Iceberg-specific
 * optimization will not fire without Iceberg metadata, so the query will
 * fall through to the standard DuckDB aggregate.
 */
@Tag("integration")
public class DuckDBIcebergCountStarRuleTest {

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("duckdb-iceberg-test-").toFile();
    createTestData();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that COUNT(*) on a DuckDB-backed table returns the correct row
   * count. Without Iceberg metadata, the rule will not fire and DuckDB
   * will compute the count directly.
   */
  @Test public void testCountStarFromIcebergMetadata() throws Exception {
    String query =
        "SELECT COUNT(*) FROM files.\"iceberg_count_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertEquals(400, result,
          "COUNT(*) should return 400 rows");
    }
  }

  /**
   * Test that COUNT(DISTINCT) is NOT matched by this rule (it requires
   * plain COUNT(*) with no arguments). The query should still work,
   * falling through to DuckDB.
   */
  @Test public void testCountDistinctNotMatched() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"metric_name\")"
            + " FROM files.\"iceberg_count_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // metric_name has 8 distinct values: Metric0..Metric7
      assertTrue(result > 0 && result <= 8,
          "COUNT(DISTINCT metric_name) should be <= 8, got: " + result);
    }
  }

  /**
   * Test that COUNT(*) with GROUP BY is NOT matched by the rule.
   * The rule requires a grand total (no GROUP BY).
   */
  @Test public void testCountStarWithGroupByNotMatched() throws Exception {
    String query =
        "SELECT \"metric_name\", COUNT(*)"
            + " FROM files.\"iceberg_count_test\""
            + " GROUP BY \"metric_name\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int groupCount = 0;
      while (rs.next()) {
        String metricName = rs.getString(1);
        long count = rs.getLong(2);
        assertNotNull(metricName,
            "Metric name should not be null");
        assertTrue(count > 0,
            "Each group should have at least 1 row");
        groupCount++;
      }
      assertEquals(8, groupCount,
          "Should have 8 groups (Metric0..Metric7)");
    }
  }

  /**
   * Verify the rule INSTANCE is properly initialized.
   */
  @Test public void testRuleInstanceExists() throws Exception {
    assertNotNull(DuckDBIcebergCountStarRule.INSTANCE,
        "DuckDBIcebergCountStarRule.INSTANCE should not be null");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "iceberg_count_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"IcebergRecord\",\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"int\"},"
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
      for (int i = 0; i < 400; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i + 1);
        record.put("metric_name", "Metric" + (i % 8));
        record.put("metric_value", 1.0 + (i % 100) * 0.5);
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
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

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
