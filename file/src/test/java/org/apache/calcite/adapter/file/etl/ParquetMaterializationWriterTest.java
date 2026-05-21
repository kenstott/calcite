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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link ParquetMaterializationWriter}.
 *
 * <p>These tests verify the Parquet materialization write path including:
 * single partition writes, multiple partition directory structures,
 * schema preservation, and overwrite behavior.
 *
 * <p>Requires DuckDB JDBC driver on the classpath.
 */
@Tag("integration")
public class ParquetMaterializationWriterTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetMaterializationWriterTest.class);

  @TempDir
  File tempDir;

  private StorageProvider storageProvider;
  private ParquetMaterializationWriter writer;

  @BeforeEach
  public void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  @Test public void testWriteSingleBatchProducesParquetFiles() throws Exception {
    // Use a non-existent path so DuckDB can create it as a file or directory
    File outputPath = new File(tempDir, "single_batch_output");
    // Do NOT mkdirs - let DuckDB create the output structure
    writer = new ParquetMaterializationWriter(storageProvider, outputPath.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputPath.getAbsolutePath());
    writer.initialize(config);

    // Write a batch of data
    List<Map<String, Object>> rows = createTestRows(10);
    long written = writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());

    assertEquals(10, written, "Should report 10 rows written");
    assertEquals(10, writer.getTotalRowsWritten(), "Total rows should be 10");
    assertEquals(1, writer.getTotalFilesWritten(), "Should have written 1 file");

    // Verify output was created
    assertTrue(outputPath.exists(), "Output path should exist after write");

    // Verify row count by reading back with DuckDB
    long rowCount = countParquetRows(outputPath.getAbsolutePath());
    assertEquals(10, rowCount, "Parquet output should contain 10 rows");
  }

  @Test public void testWriteMultiplePartitionsCreatesDirectoryStructure() throws Exception {
    File outputDir = new File(tempDir, "partitioned_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = buildPartitionedConfig(outputDir.getAbsolutePath(), "region");
    writer.initialize(config);

    // Write batch with partition columns included in data
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 5; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "item_" + i);
      row.put("region", "EAST");
      rows.add(row);
    }
    for (int i = 5; i < 10; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "item_" + i);
      row.put("region", "WEST");
      rows.add(row);
    }

    Map<String, String> partitionVars = new HashMap<String, String>();
    partitionVars.put("region", "ALL");
    long written = writer.writeBatch(rows.iterator(), partitionVars);

    assertEquals(10, written, "Should write 10 rows total");

    // Verify files were created (DuckDB writes partitioned parquet with PARTITION_BY)
    long totalRows = countParquetRows(outputDir.getAbsolutePath());
    assertEquals(10, totalRows, "Partitioned output should contain 10 total rows");
  }

  @Test public void testSchemaPreservationInWrittenParquet() throws Exception {
    // Use a non-existent path so DuckDB can create the output
    File outputPath = new File(tempDir, "schema_test_output");
    writer = new ParquetMaterializationWriter(storageProvider, outputPath.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputPath.getAbsolutePath());
    writer.initialize(config);

    // Write data with specific column types
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "test_record");
    row.put("amount", 99.95);
    rows.add(row);

    writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());

    // Verify schema by reading back with DuckDB and checking column names
    List<String> columns = getParquetColumnNames(outputPath.getAbsolutePath());
    assertTrue(columns.contains("id"), "Schema should contain 'id' column");
    assertTrue(columns.contains("name"), "Schema should contain 'name' column");
    assertTrue(columns.contains("amount"), "Schema should contain 'amount' column");
  }

  @Test public void testOverwriteOrIgnoreBehavior() throws Exception {
    // Use a non-existent path so DuckDB can create it
    File outputPath = new File(tempDir, "overwrite_test_output");
    writer = new ParquetMaterializationWriter(storageProvider, outputPath.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputPath.getAbsolutePath());
    writer.initialize(config);

    // Write first batch
    List<Map<String, Object>> firstBatch = createTestRows(5);
    writer.writeBatch(firstBatch.iterator(), Collections.<String, String>emptyMap());

    // Write second batch (OVERWRITE_OR_IGNORE mode means the second write overwrites)
    List<Map<String, Object>> secondBatch = createTestRows(3);
    writer.writeBatch(secondBatch.iterator(), Collections.<String, String>emptyMap());

    assertEquals(8, writer.getTotalRowsWritten(), "Total rows should be 8 across both batches");
    assertEquals(2, writer.getTotalFilesWritten(), "Should have written 2 files");
  }

  @Test public void testWriteEmptyBatchReturnsZero() throws Exception {
    File outputDir = new File(tempDir, "empty_batch_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputDir.getAbsolutePath());
    writer.initialize(config);

    // Write empty batch
    List<Map<String, Object>> emptyRows = Collections.emptyList();
    long written = writer.writeBatch(emptyRows.iterator(), Collections.<String, String>emptyMap());

    assertEquals(0, written, "Empty batch should return 0 rows written");
    assertEquals(0, writer.getTotalRowsWritten(), "Total rows should be 0");
    assertEquals(0, writer.getTotalFilesWritten(), "Should have written 0 files");
  }

  @Test public void testWriteWithoutInitializeThrowsException() throws Exception {
    File outputDir = new File(tempDir, "uninit_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    List<Map<String, Object>> rows = createTestRows(1);
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap()),
        "Should throw IllegalStateException when writing without initialization");
  }

  @Test public void testInitializeWithNullConfigThrowsException() throws Exception {
    File outputDir = new File(tempDir, "null_config_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(null),
        "Should throw IllegalArgumentException for null config");
  }

  @Test public void testInitializeWithDisabledConfigThrowsException() throws Exception {
    File outputDir = new File(tempDir, "disabled_config_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertThrows(IOException.class,
        () -> writer.initialize(config),
        "Should throw IOException when materialization is disabled");
  }

  @Test public void testGetFormatReturnsParquet() throws Exception {
    File outputDir = new File(tempDir, "format_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    assertEquals(MaterializeConfig.Format.PARQUET, writer.getFormat(),
        "Format should be PARQUET");
  }

  @Test public void testGetTableLocationReturnsOutputPath() throws Exception {
    File outputDir = new File(tempDir, "location_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputDir.getAbsolutePath());
    writer.initialize(config);

    String location = writer.getTableLocation();
    assertNotNull(location, "Table location should not be null");
    assertTrue(location.contains(outputDir.getAbsolutePath()) || location.equals(outputDir.getAbsolutePath()),
        "Table location should contain the output directory");
  }

  @Test public void testCommitIsNoOpForParquet() throws Exception {
    File outputDir = new File(tempDir, "commit_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputDir.getAbsolutePath());
    writer.initialize(config);

    // Commit should not throw - it is a no-op for Parquet format
    writer.commit();
  }

  @Test public void testCloseResetsInitializedState() throws Exception {
    File outputDir = new File(tempDir, "close_output");
    outputDir.mkdirs();
    writer = new ParquetMaterializationWriter(storageProvider, outputDir.getAbsolutePath());

    MaterializeConfig config = buildSimpleConfig(outputDir.getAbsolutePath());
    writer.initialize(config);
    writer.close();

    // After close, writing should fail since initialized is reset to false
    List<Map<String, Object>> rows = createTestRows(1);
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap()),
        "Should throw IllegalStateException after close");
  }

  // --- Helper methods ---

  private List<Map<String, Object>> createTestRows(int count) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "test_value_" + i);
      rows.add(row);
    }
    return rows;
  }

  private MaterializeConfig buildSimpleConfig(String outputPath) {
    return MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .build();
  }

  private MaterializeConfig buildPartitionedConfig(String outputPath, String... partitionColumns) {
    return MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .compression("snappy")
            .build())
        .partition(MaterializePartitionConfig.builder()
            .columns(Arrays.asList(partitionColumns))
            .build())
        .build();
  }

  /**
   * Counts rows in Parquet output using DuckDB.
   * Handles both single-file and directory (partitioned) output.
   */
  private long countParquetRows(String path) throws Exception {
    File outputFile = new File(path);
    String readPath;
    if (outputFile.isDirectory()) {
      readPath = path.replace("'", "''") + "/**/*.parquet";
    } else {
      readPath = path.replace("'", "''");
    }
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        String sql = "SELECT COUNT(*) FROM read_parquet('"
            + readPath + "', union_by_name=true)";
        LOGGER.debug("Counting rows with SQL: {}", sql);
        try (ResultSet rs = stmt.executeQuery(sql)) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        }
      }
    }
    return 0;
  }

  /**
   * Gets column names from Parquet output using DuckDB.
   * Handles both single-file and directory (partitioned) output.
   */
  private List<String> getParquetColumnNames(String path) throws Exception {
    File outputFile = new File(path);
    String readPath;
    if (outputFile.isDirectory()) {
      readPath = path.replace("'", "''") + "/**/*.parquet";
    } else {
      readPath = path.replace("'", "''");
    }
    List<String> columnNames = new ArrayList<String>();
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        String sql = "DESCRIBE SELECT * FROM read_parquet('"
            + readPath + "', union_by_name=true)";
        LOGGER.debug("Getting columns with SQL: {}", sql);
        try (ResultSet rs = stmt.executeQuery(sql)) {
          while (rs.next()) {
            columnNames.add(rs.getString("column_name"));
          }
        }
      }
    }
    return columnNames;
  }
}
