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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.calcite.adapter.file.etl.ColumnConfig;
import org.apache.calcite.adapter.file.etl.HiveParquetWriter;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.MaterializeOptionsConfig;
import org.apache.calcite.adapter.file.etl.MaterializeOutputConfig;
import org.apache.calcite.adapter.file.etl.MaterializePartitionConfig;
import org.apache.calcite.adapter.file.etl.MaterializeResult;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for DirectParquetWriter, HiveParquetWriter,
 * and ParquetConversionUtil.
 */
@Tag("integration")
class ParquetWriterCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  // ---------------------------------------------------------------
  // DirectParquetWriter tests
  // ---------------------------------------------------------------

  @Test void testDirectParquetWriterWithDuckDBResultSet() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("direct_output.parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT 1 AS item_id, 'Widget' AS item_name, 9.99 AS price, true AS in_stock");

      DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));
    }

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);

    // Verify content by reading back with DuckDB
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_parquet('" + parquetFile.toString() + "')");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("item_id"));
      assertEquals("Widget", rs.getString("item_name"));
      assertFalse(rs.next());
    }
  }

  @Test void testDirectParquetWriterWithComments() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("comments_output.parquet");

    Map<String, String> columnComments = new HashMap<String, String>();
    columnComments.put("product_id", "Primary key for products");
    columnComments.put("product_name", "Display name of the product");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT 1 AS product_id, 'Widget' AS product_name");

      DirectParquetWriter.writeResultSetToParquet(
          rs, new Path(parquetFile.toString()),
          "Products reference table", columnComments);
    }

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testDirectParquetWriterAllTypes() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("all_types_output.parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT "
          + "CAST(1 AS TINYINT) AS tiny_col, "
          + "CAST(100 AS SMALLINT) AS small_col, "
          + "42 AS int_col, "
          + "CAST(99999999999 AS BIGINT) AS big_col, "
          + "CAST(3.14 AS FLOAT) AS float_col, "
          + "CAST(2.71828 AS DOUBLE) AS double_col, "
          + "true AS bool_col, "
          + "'hello' AS str_col, "
          + "DATE '2024-01-15' AS date_col, "
          + "TIMESTAMP '2024-01-15 10:30:00' AS ts_col");

      DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));
    }

    assertTrue(Files.exists(parquetFile));

    // Verify by reading back
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_parquet('" + parquetFile.toString() + "')");
      assertTrue(rs.next());
      assertEquals(42, rs.getInt("int_col"));
      assertEquals("hello", rs.getString("str_col"));
      assertTrue(rs.getBoolean("bool_col"));
      assertFalse(rs.next());
    }
  }

  @Test void testDirectParquetWriterNullValues() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("nulls_output.parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT CAST(NULL AS INTEGER) AS nullable_id, "
          + "CAST(NULL AS VARCHAR) AS nullable_name, "
          + "42 AS non_null_val");

      DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));
    }

    assertTrue(Files.exists(parquetFile));

    // Verify nulls are preserved
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM read_parquet('" + parquetFile.toString() + "')");
      assertTrue(rs.next());
      rs.getInt("nullable_id");
      assertTrue(rs.wasNull());
      assertEquals(42, rs.getInt("non_null_val"));
    }
  }

  @Test void testDirectParquetWriterMultipleRows() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("multi_rows.parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery(
          "SELECT * FROM (VALUES "
          + "(1, 'Alice', 30), "
          + "(2, 'Bob', 25), "
          + "(3, 'Charlie', 35)) AS t(person_id, person_name, person_age)");

      DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));
    }

    assertTrue(Files.exists(parquetFile));

    // Verify row count
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(
          "SELECT count(*) AS cnt FROM read_parquet('"
          + parquetFile.toString() + "')");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("cnt"));
    }
  }

  @Test void testDirectParquetWriterEmptyTableComment() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("empty_comment.parquet");

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      ResultSet rs = stmt.executeQuery("SELECT 1 AS val");

      // Empty comments should be handled gracefully
      DirectParquetWriter.writeResultSetToParquet(
          rs, new Path(parquetFile.toString()), "", null);
    }

    assertTrue(Files.exists(parquetFile));
  }

  // ---------------------------------------------------------------
  // HiveParquetWriter tests
  // ---------------------------------------------------------------

  @Test void testHiveParquetWriterMaterializeDisabled() throws IOException {
    HiveParquetWriter writer = new HiveParquetWriter(storageProvider, tempDir.toString());

    MaterializeConfig disabledConfig = MaterializeConfig.builder()
        .enabled(false)
        .build();

    // DataSource is not relevant since materialization is disabled
    MaterializeResult result = writer.materialize(disabledConfig, null);
    assertNotNull(result);
    assertTrue(result.isSkipped());
  }

  @Test void testHiveParquetWriterMaterializeFromJson() throws IOException {
    java.nio.file.Path outputDir = Files.createDirectories(tempDir.resolve("hive_json_output"));

    // Create test JSON file
    java.nio.file.Path jsonFile = tempDir.resolve("input_data.json");
    Files.write(jsonFile, Arrays.asList(
        "[{\"region\": \"US\", \"year\": 2020, \"revenue\": 1000},"
        + " {\"region\": \"EU\", \"year\": 2020, \"revenue\": 2000}]"
    ), StandardCharsets.UTF_8);

    HiveParquetWriter writer = new HiveParquetWriter(storageProvider, tempDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .name("json_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.getElapsedMillis() >= 0);
  }

  @Test void testHiveParquetWriterMaterializeFromCsv() throws IOException {
    java.nio.file.Path outputDir = Files.createDirectories(tempDir.resolve("hive_csv_output"));

    // Create test CSV file
    java.nio.file.Path csvFile = tempDir.resolve("input_data.csv");
    Files.write(csvFile, Arrays.asList(
        "product,quantity,price",
        "Widget,10,9.99",
        "Gadget,5,19.99"
    ), StandardCharsets.UTF_8);

    HiveParquetWriter writer = new HiveParquetWriter(storageProvider, tempDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .name("csv_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .build();

    MaterializeResult result = writer.materializeFromCsv(config, csvFile.toString());
    assertNotNull(result);
    assertTrue(result.getElapsedMillis() >= 0);
  }

  @Test void testHiveParquetWriterMaterializeFromJsonWithPartitions() throws IOException {
    java.nio.file.Path outputDir = Files.createDirectories(
        tempDir.resolve("hive_partition_output"));

    java.nio.file.Path jsonFile = tempDir.resolve("partitioned_data.json");
    Files.write(jsonFile, Arrays.asList(
        "[{\"region\": \"US\", \"year\": 2020, \"revenue\": 1000},"
        + " {\"region\": \"EU\", \"year\": 2021, \"revenue\": 2000}]"
    ), StandardCharsets.UTF_8);

    HiveParquetWriter writer = new HiveParquetWriter(storageProvider, tempDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .name("partitioned_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList("year"))
            .build())
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
    assertTrue(result.getElapsedMillis() >= 0);
  }

  @Test void testHiveParquetWriterMaterializeFromJsonWithColumns() throws IOException {
    java.nio.file.Path outputDir = Files.createDirectories(
        tempDir.resolve("hive_columns_output"));

    java.nio.file.Path jsonFile = tempDir.resolve("column_data.json");
    Files.write(jsonFile, Arrays.asList(
        "[{\"full_name\": \"Alice Smith\", \"raw_age\": 30, \"city\": \"NYC\"}]"
    ), StandardCharsets.UTF_8);

    HiveParquetWriter writer = new HiveParquetWriter(storageProvider, tempDir.toString());

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder()
        .name("person_name")
        .type("VARCHAR")
        .source("full_name")
        .build());
    columns.add(ColumnConfig.builder()
        .name("person_age")
        .type("INTEGER")
        .source("raw_age")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .name("column_materialize")
        .output(MaterializeOutputConfig.builder()
            .location(outputDir.toString())
            .build())
        .columns(columns)
        .build();

    MaterializeResult result = writer.materializeFromJson(config, jsonFile.toString());
    assertNotNull(result);
  }

  // ---------------------------------------------------------------
  // ParquetConversionUtil tests
  // ---------------------------------------------------------------

  @Test void testGetParquetCacheDirDefault() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirCustom() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("custom_cache").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir);
    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test void testGetParquetCacheDirWithSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("cache_with_schema").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, "test_schema");
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains("schema_test_schema"));
  }

  @Test void testGetParquetCacheDirWithEmptyCustom() {
    File baseDir = tempDir.toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, "");
    assertNotNull(cacheDir);
    // Empty custom dir should fall back to default behavior
    assertTrue(cacheDir.getAbsolutePath().contains(".parquet_cache"));
  }

  @Test void testGetParquetCacheDirWithNullSchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("null_schema").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, null);
    assertNotNull(cacheDir);
    // Null schema name should not add schema prefix
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test void testGetParquetCacheDirWithEmptySchemaName() {
    File baseDir = tempDir.toFile();
    String customDir = tempDir.resolve("empty_schema").toString();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(
        baseDir, customDir, "");
    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  // ---------------------------------------------------------------
  // MaterializeResult tests
  // ---------------------------------------------------------------

  @Test void testMaterializeResultSuccess() {
    MaterializeResult result = MaterializeResult.success(100, 5, 1234);
    assertNotNull(result);
    assertFalse(result.isSkipped());
    assertEquals(100, result.getRowCount());
    assertEquals(5, result.getFileCount());
    assertEquals(1234, result.getElapsedMillis());
  }

  @Test void testMaterializeResultSkipped() {
    MaterializeResult result = MaterializeResult.skipped("test reason");
    assertNotNull(result);
    assertTrue(result.isSkipped());
  }

  @Test void testMaterializeResultError() {
    MaterializeResult result = MaterializeResult.error("Something broke", 500);
    assertNotNull(result);
    assertTrue(result.isError());
    assertNotNull(result.getMessage());
  }
}
