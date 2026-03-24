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
package org.apache.calcite.adapter.file.table;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GlobParquetTable}.
 *
 * <p>Verifies multi-file pattern matching, partition discovery from
 * directory structure, and schema handling across files.
 */
@Tag("integration")
public class GlobParquetTableTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GlobParquetTableTest.class);

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory(
        "glob-parquet-test-").toFile();
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
  public void testMultiFilePatternMatchingReturnsAllRows()
      throws Exception {
    // Create 3 parquet files with 5 rows each
    createMultipleParquetFiles(3, 5);
    setupCalciteConnectionWithPartitionedTable("multi_data/**/*.parquet");

    String query =
        "SELECT count(*) AS cnt FROM \"files\".\"partitioned_data\"";
    int rowCount = 0;

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      rowCount = rs.getInt(1);
    }

    // 3 files x 5 rows = 15 total rows
    assertEquals(15, rowCount,
        "Should have 15 total rows from 3 files of 5 rows each");
    LOGGER.debug("Multi-file glob matched {} total rows", rowCount);
  }

  @Test
  public void testPartitionDiscoveryFromDirectoryStructure()
      throws Exception {
    // Create hive-partitioned data: region=EAST and region=WEST
    createHivePartitionedData();
    setupCalciteConnectionWithPartitionedTable(
        "partitioned_sales/**/*.parquet");

    String query = "SELECT DISTINCT \"region\" "
        + "FROM \"files\".\"partitioned_data\" ORDER BY \"region\"";

    List<String> regions = new ArrayList<>();
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        regions.add(rs.getString(1));
      }
    }

    assertEquals(2, regions.size(),
        "Should discover 2 distinct regions");
    assertTrue(regions.contains("EAST"),
        "Should contain region EAST");
    assertTrue(regions.contains("WEST"),
        "Should contain region WEST");

    LOGGER.debug("Discovered partitions: {}", regions);
  }

  @Test
  public void testAllRowsAccessibleFromPartitionedData()
      throws Exception {
    createHivePartitionedData();
    setupCalciteConnectionWithPartitionedTable(
        "partitioned_sales/**/*.parquet");

    String query = "SELECT \"sale_id\", \"amount\", \"region\" "
        + "FROM \"files\".\"partitioned_data\" ORDER BY \"sale_id\"";

    List<Integer> saleIds = new ArrayList<>();
    List<Double> amounts = new ArrayList<>();
    Set<String> regionsFound = new HashSet<>();

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        saleIds.add(rs.getInt("sale_id"));
        amounts.add(rs.getDouble("amount"));
        regionsFound.add(rs.getString("region"));
      }
    }

    // 2 regions x 5 rows each = 10 total rows
    assertEquals(10, saleIds.size(),
        "Should have 10 total rows across partitions");
    assertEquals(2, regionsFound.size(),
        "Should see data from both regions");
    assertTrue(amounts.stream().allMatch(a -> a > 0),
        "All amounts should be positive");

    LOGGER.debug("Retrieved {} rows from {} partitions",
        saleIds.size(), regionsFound.size());
  }

  @Test
  public void testSchemaConsistentAcrossPartitionedFiles()
      throws Exception {
    createHivePartitionedData();
    setupCalciteConnectionWithPartitionedTable(
        "partitioned_sales/**/*.parquet");

    // Verify schema by querying metadata-like information
    String query = "SELECT \"sale_id\", \"amount\", \"product\", \"region\" "
        + "FROM \"files\".\"partitioned_data\" "
        + "WHERE \"sale_id\" = 1";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      // Verify all expected columns are present
      int columnCount = rs.getMetaData().getColumnCount();
      assertTrue(columnCount >= 3,
          "Should have at least 3 columns (sale_id, amount, product)");

      // Verify column names (case-insensitive check)
      Set<String> columnNames = new HashSet<>();
      for (int i = 1; i <= columnCount; i++) {
        columnNames.add(
            rs.getMetaData().getColumnName(i).toLowerCase());
      }
      assertTrue(columnNames.contains("sale_id"),
          "Should have sale_id column");
      assertTrue(columnNames.contains("amount"),
          "Should have amount column");
      assertTrue(columnNames.contains("product"),
          "Should have product column");

      LOGGER.debug("Schema has {} columns: {}",
          columnCount, columnNames);
    }
  }

  @SuppressWarnings("deprecation")
  private void createMultipleParquetFiles(int numFiles, int rowsPerFile)
      throws Exception {
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"amount\",\"type\":\"double\"}"
        + "]}";
    Schema avroSchema = new Schema.Parser().parse(schemaString);

    File dataDir = new File(tempDir, "multi_data");

    int globalId = 1;
    for (int f = 0; f < numFiles; f++) {
      // Create subdirectories for glob matching: multi_data/part_N/
      File partDir = new File(dataDir, "part_" + f);
      partDir.mkdirs();

      File parquetFile = new File(partDir, "data.parquet");
      try (ParquetWriter<GenericRecord> writer =
               AvroParquetWriter
                   .<GenericRecord>builder(
                       new org.apache.hadoop.fs.Path(
                           parquetFile.getAbsolutePath()))
                   .withSchema(avroSchema)
                   .withCompressionCodec(CompressionCodecName.SNAPPY)
                   .build()) {
        for (int i = 0; i < rowsPerFile; i++) {
          GenericRecord record = new GenericData.Record(avroSchema);
          record.put("id", globalId);
          record.put("name", "item_" + globalId);
          record.put("amount", 10.0 * globalId);
          writer.write(record);
          globalId++;
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void createHivePartitionedData() throws Exception {
    String[] regions = {"EAST", "WEST"};

    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"SalesRecord\",\"fields\":["
        + "{\"name\":\"sale_id\",\"type\":\"int\"},"
        + "{\"name\":\"amount\",\"type\":\"double\"},"
        + "{\"name\":\"product\",\"type\":\"string\"}"
        + "]}";
    Schema avroSchema = new Schema.Parser().parse(schemaString);

    int saleId = 1;
    for (String region : regions) {
      File partitionDir = new File(tempDir,
          "partitioned_sales/region=" + region);
      partitionDir.mkdirs();

      File parquetFile = new File(partitionDir, "data.parquet");
      try (ParquetWriter<GenericRecord> writer =
               AvroParquetWriter
                   .<GenericRecord>builder(
                       new org.apache.hadoop.fs.Path(
                           parquetFile.getAbsolutePath()))
                   .withSchema(avroSchema)
                   .withCompressionCodec(CompressionCodecName.SNAPPY)
                   .build()) {
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

  private void setupCalciteConnectionWithPartitionedTable(String pattern)
      throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("name", "partitioned_data");
    tableConfig.put("pattern", pattern);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);
    operand.put("partitionedTables",
        Arrays.asList(tableConfig));

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }
}
