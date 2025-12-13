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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for PartitionDistinctRule which optimizes SELECT DISTINCT on partition columns
 * by using directory listing instead of scanning parquet files.
 */
@Tag("unit")
public class PartitionDistinctRuleTest {

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    // Enable partition distinct optimization
    System.setProperty("calcite.file.partition.distinct.enabled", "true");

    // PartitionDistinctRule is registered globally via FileSchemaFactory's static initializer
    // when the schema factory is loaded

    tempDir = java.nio.file.Files.createTempDirectory("partition-distinct-test-").toFile();
    createHivePartitionedData();
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

  @Test
  public void testDistinctOnSinglePartitionColumn() throws Exception {
    // Query distinct values of a partition column
    String query = "SELECT DISTINCT \"region\" FROM \"files\".\"partitioned_sales\" ORDER BY \"region\"";

    List<String> results = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        results.add(rs.getString(1));
      }
    }

    // Should find both regions
    assertEquals(2, results.size(), "Should find 2 distinct regions");
    assertTrue(results.contains("EAST"), "Should contain EAST");
    assertTrue(results.contains("WEST"), "Should contain WEST");
  }

  @Test
  public void testGroupByPartitionColumn() throws Exception {
    // GROUP BY on partition column should also work
    String query = "SELECT \"region\" FROM \"files\".\"partitioned_sales\" GROUP BY \"region\" ORDER BY \"region\"";

    List<String> results = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        results.add(rs.getString(1));
      }
    }

    assertEquals(2, results.size(), "Should find 2 distinct regions");
    assertTrue(results.contains("EAST"), "Should contain EAST");
    assertTrue(results.contains("WEST"), "Should contain WEST");
  }

  @Test
  public void testPartitionDistinctPerformance() throws Exception {
    // This test verifies the query completes quickly
    String query = "SELECT DISTINCT \"region\" FROM \"files\".\"partitioned_sales\"";

    long startTime = System.nanoTime();
    int resultCount = 0;
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        resultCount++;
      }
    }
    long endTime = System.nanoTime();
    double timeMs = (endTime - startTime) / 1_000_000.0;

    // Should complete very quickly (under 5 seconds for small test data)
    assertTrue(timeMs < 5000, "Query should complete in under 5 seconds, took: " + timeMs + "ms");
    assertEquals(2, resultCount, "Should find 2 distinct regions");
  }

  /**
   * Creates hive-partitioned parquet data with the structure:
   * partitioned_sales/region=EAST/data.parquet
   * partitioned_sales/region=WEST/data.parquet
   */
  @SuppressWarnings("deprecation")
  private void createHivePartitionedData() throws Exception {
    String[] regions = {"EAST", "WEST"};

    String schemaString = "{\"type\": \"record\",\"name\": \"SalesRecord\",\"fields\": [" +
        "  {\"name\": \"sale_id\", \"type\": \"int\"}," +
        "  {\"name\": \"amount\", \"type\": \"double\"}," +
        "  {\"name\": \"product\", \"type\": \"string\"}" +
        "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    int saleId = 1;
    for (String region : regions) {
      // Create partition directory
      File partitionDir = new File(tempDir, "partitioned_sales/region=" + region);
      partitionDir.mkdirs();

      // Create parquet file in partition
      File parquetFile = new File(partitionDir, "data.parquet");

      try (ParquetWriter<GenericRecord> writer =
               AvroParquetWriter
                   .<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath()))
                   .withSchema(avroSchema)
                   .withCompressionCodec(CompressionCodecName.SNAPPY)
                   .build()) {

        // Write a few records per partition
        for (int i = 0; i < 5; i++) {
          GenericRecord record = new GenericData.Record(avroSchema);
          record.put("sale_id", saleId++);
          record.put("amount", 100.0 + i * 10.0);
          record.put("product", "Product" + (i % 3));
          writer.write(record);
        }
      }
    }
  }

  private void setupCalciteConnection() throws Exception {
    setupCalciteConnection("parquet");
  }

  private void setupCalciteConnection(String executionEngine) throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Configure partitioned table
    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("name", "partitioned_sales");
    tableConfig.put("pattern", "partitioned_sales/**/*.parquet");

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", executionEngine);
    operand.put("ephemeralCache", true);  // Use temp cache directory for test isolation
    operand.put("partitionedTables", java.util.Arrays.asList(tableConfig));

    rootSchema.add("files", FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  @Test
  public void testDistinctOnPartitionColumnWithDuckDB() throws Exception {
    // Close existing connection and recreate with DuckDB
    if (calciteConn != null) {
      calciteConn.close();
    }
    setupCalciteConnection("duckdb");

    // Query distinct values of a partition column
    String query = "SELECT DISTINCT \"region\" FROM \"files\".\"partitioned_sales\" ORDER BY \"region\"";

    List<String> results = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        results.add(rs.getString(1));
      }
    }

    // Should find both regions
    assertEquals(2, results.size(), "Should find 2 distinct regions with DuckDB");
    assertTrue(results.contains("EAST"), "Should contain EAST");
    assertTrue(results.contains("WEST"), "Should contain WEST");
  }
}
