/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for IcebergTableWriter focusing on path handling,
 * partition coercion, data file building, record writing, and maintenance.
 */
@Tag("unit")
public class IcebergTableWriterDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private Table table;
  private Table unpartitionedTable;
  private StorageProvider storageProvider;
  private IcebergTableWriter writer;

  @BeforeEach
  void setUp() {
    IcebergCatalogManager.clearCache();
    storageProvider = new LocalFileStorageProvider();
    String warehousePath = tempDir.resolve("warehouse").toString();
    catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", warehousePath);

    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()),
        Types.NestedField.optional(4, "amount", Types.DoubleType.get()),
        Types.NestedField.optional(5, "flag", Types.BooleanType.get()),
        Types.NestedField.optional(6, "count", Types.LongType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    table = IcebergCatalogManager.createTable(catalogConfig, "test_table", schema, spec);

    // Create unpartitioned table
    Schema unpartSchema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec unpartSpec = PartitionSpec.unpartitioned();
    unpartitionedTable =
        IcebergCatalogManager.createTable(catalogConfig, "unpart_table", unpartSchema, unpartSpec);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  // ====================================================================
  // Tests for constructors
  // ====================================================================

  @Test void testConstructorWithoutHadoopConf() {
    IcebergTableWriter w = new IcebergTableWriter(table, storageProvider);
    assertNotNull(w);
    assertNotNull(w.getTable());
    assertEquals(table.name(), w.getTable().name());
  }

  @Test void testConstructorWithHadoopConf() {
    IcebergTableWriter w = new IcebergTableWriter(table, storageProvider);
    assertNotNull(w);
  }

  // ====================================================================
  // Tests for computeRelativePath
  // ====================================================================

  @Test void testComputeRelativePath() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("computeRelativePath", String.class, String.class);
    method.setAccessible(true);

    // Normal case with trailing slash
    assertEquals("file.parquet",
        method.invoke(writer, "/base/path/", "/base/path/file.parquet"));

    // Normal case without trailing slash
    assertEquals("file.parquet",
        method.invoke(writer, "/base/path", "/base/path/file.parquet"));

    // Nested path
    assertEquals("sub/file.parquet",
        method.invoke(writer, "/base/path", "/base/path/sub/file.parquet"));

    // No common prefix - returns just filename
    assertEquals("file.parquet",
        method.invoke(writer, "/other/path", "/base/path/file.parquet"));

    // No slash in path
    assertEquals("filename",
        method.invoke(writer, "/other/path", "filename"));
  }

  // ====================================================================
  // Tests for getParentPath
  // ====================================================================

  @Test void testGetParentPath() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getParentPath", String.class);
    method.setAccessible(true);

    assertEquals("/parent", method.invoke(writer, "/parent/child"));
    assertNull(method.invoke(writer, "/"));
    assertNull(method.invoke(writer, "file"));

    // S3 paths
    assertNull(method.invoke(writer, "s3://b"));
    assertEquals("s3a://bu", method.invoke(writer, "s3a://bu/"));
    assertEquals("s3://bucket/parent", method.invoke(writer, "s3://bucket/parent/child"));
    assertEquals("s3a://bucket/parent", method.invoke(writer, "s3a://bucket/parent/child"));
  }

  // ====================================================================
  // Tests for estimateRecordCount
  // ====================================================================

  @Test void testEstimateRecordCount() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("estimateRecordCount", long.class);
    method.setAccessible(true);

    assertEquals(1L, method.invoke(writer, 0L));
    assertEquals(1L, method.invoke(writer, 50L));
    assertEquals(1L, method.invoke(writer, 100L));
    assertEquals(10L, method.invoke(writer, 1000L));
    assertEquals(10000L, method.invoke(writer, 1000000L));
  }

  // ====================================================================
  // Tests for coercePartitionValue
  // ====================================================================

  @Test void testCoercePartitionValueAllTypes() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("coercePartitionValue", String.class, org.apache.iceberg.PartitionField.class);
    method.setAccessible(true);

    org.apache.iceberg.PartitionField yearField = table.spec().fields().get(0);

    // Null/empty/missing value indicators
    assertNull(method.invoke(writer, null, yearField));
    assertNull(method.invoke(writer, "", yearField));
    assertNull(method.invoke(writer, "-", yearField));

    // Valid integer
    assertEquals(2020, method.invoke(writer, "2020", yearField));

    // Invalid integer
    assertNull(method.invoke(writer, "not_a_number", yearField));
  }

  // ====================================================================
  // Tests for buildPartitionPath
  // ====================================================================

  @Test void testBuildPartitionPath() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("buildPartitionPath", Map.class);
    method.setAccessible(true);

    // Null/empty
    assertEquals("", method.invoke(writer, (Object) null));
    assertEquals("", method.invoke(writer, new HashMap<String, String>()));

    // With partition values
    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2020");
    String path = (String) method.invoke(writer, partVals);
    assertEquals("year=2020", path);
  }

  // ====================================================================
  // Tests for getFieldValue
  // ====================================================================

  @Test void testGetFieldValue() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("getFieldValue", Map.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("Data", "hello");

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2020");

    // Case-insensitive lookup in row
    assertEquals("hello", method.invoke(writer, row, "data", partVals));
    assertEquals("hello", method.invoke(writer, row, "Data", partVals));

    // Fallback to partition values
    assertEquals("2020", method.invoke(writer, row, "year", partVals));

    // Not found at all
    assertNull(method.invoke(writer, row, "nonexistent", partVals));
    assertNull(method.invoke(writer, row, "nonexistent", null));
  }

  // ====================================================================
  // Tests for commitDataFiles
  // ====================================================================

  @Test void testCommitDataFilesEmpty() {
    writer = new IcebergTableWriter(table, storageProvider);
    // Empty list should not throw
    writer.commitDataFiles(Collections.<DataFile>emptyList(), null);
  }

  // ====================================================================
  // Tests for bulkCommitDataFiles
  // ====================================================================

  @Test void testBulkCommitDataFilesEmpty() {
    writer = new IcebergTableWriter(table, storageProvider);
    // Empty list should not throw
    writer.bulkCommitDataFiles(Collections.<DataFile>emptyList());
  }

  // ====================================================================
  // Tests for stageFiles with empty directory
  // ====================================================================

  @Test void testStageFilesEmptyDirectory() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Path stagingDir = tempDir.resolve("empty_staging");
    Files.createDirectories(stagingDir);

    List<DataFile> result = writer.stageFiles(stagingDir.toString());
    assertTrue(result.isEmpty());
  }

  // ====================================================================
  // Tests for commitFromStaging with empty directory
  // ====================================================================

  @Test void testCommitFromStagingEmptyDir() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Path stagingDir = tempDir.resolve("staging");
    Files.createDirectories(stagingDir);

    // Should not throw
    writer.commitFromStaging(stagingDir.toString(), null);
  }

  @Test void testCommitFromStagingWithPartitionFilter() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Path stagingDir = tempDir.resolve("staging2");
    Files.createDirectories(stagingDir);

    Map<String, Object> filter = new HashMap<String, Object>();
    filter.put("year", 2020);

    // Empty staging dir, should still not throw
    writer.commitFromStaging(stagingDir.toString(), filter);
  }

  // ====================================================================
  // Tests for deletePartition
  // ====================================================================

  @Test void testDeletePartitionWithNullFilter() {
    writer = new IcebergTableWriter(table, storageProvider);
    assertThrows(IllegalArgumentException.class,
        () -> writer.deletePartition(null));
  }

  @Test void testDeletePartitionWithEmptyFilter() {
    writer = new IcebergTableWriter(table, storageProvider);
    assertThrows(IllegalArgumentException.class,
        () -> writer.deletePartition(new HashMap<String, Object>()));
  }

  @Test void testDeletePartitionOnEmptyTable() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Map<String, Object> filter = new HashMap<String, Object>();
    filter.put("year", 2020);
    // Should not throw even on empty table
    writer.deletePartition(filter);
  }

  // ====================================================================
  // Tests for writeRecords
  // ====================================================================

  @Test void testWriteRecordsNullOrEmpty() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    assertNull(writer.writeRecords(null, null));
    assertNull(writer.writeRecords(Collections.<Map<String, Object>>emptyList(), null));
  }

  @Test void testWriteRecordsSimple() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "hello");
    row.put("amount", 9.99);
    row.put("flag", true);
    row.put("count", 42L);
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2020");

    DataFile result = writer.writeRecords(records, partVals);
    assertNotNull(result);
    assertEquals(1, result.recordCount());
    assertTrue(result.path().toString().contains("year=2020"));
    assertTrue(result.path().toString().endsWith(".parquet"));
  }

  @Test void testWriteRecordsUnpartitioned() throws Exception {
    writer = new IcebergTableWriter(unpartitionedTable, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "test");
    records.add(row);

    DataFile result = writer.writeRecords(records, null);
    assertNotNull(result);
    assertEquals(1, result.recordCount());
  }

  @Test void testWriteRecordsMultipleRows() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("data", "row_" + i);
      records.add(row);
    }

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2021");

    DataFile result = writer.writeRecords(records, partVals);
    assertNotNull(result);
    assertEquals(10, result.recordCount());
  }

  // ====================================================================
  // Tests for runMaintenance
  // ====================================================================

  @Test void testRunMaintenanceOnEmptyTable() {
    writer = new IcebergTableWriter(table, storageProvider);
    // Should not throw
    writer.runMaintenance(7, 1);
  }

  @Test void testRunMaintenanceAfterWrite() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("data", "test");
    records.add(row);

    Map<String, String> partVals = new HashMap<String, String>();
    partVals.put("year", "2020");

    DataFile df = writer.writeRecords(records, partVals);
    List<DataFile> files = new ArrayList<DataFile>();
    files.add(df);
    writer.bulkCommitDataFiles(files);

    // Should not throw
    writer.runMaintenance(1, 1);
  }

  // ====================================================================
  // Tests for normalizeS3Path
  // ====================================================================

  @Test void testNormalizeS3Path() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method =
        IcebergTableWriter.class.getDeclaredMethod("normalizeS3Path", String.class);
    method.setAccessible(true);

    // s3a:/ should become s3a://
    assertEquals("s3a://bucket/path", method.invoke(writer, "s3a:/bucket/path"));
    // Already correct
    assertEquals("s3a://bucket/path", method.invoke(writer, "s3a://bucket/path"));
    // Local path unchanged
    assertEquals("/local/path", method.invoke(writer, "/local/path"));
  }

  // ====================================================================
  // Tests for ensureVersionHint
  // ====================================================================

  @Test void testEnsureVersionHint() throws Exception {
    writer = new IcebergTableWriter(table, storageProvider);
    Method method = IcebergTableWriter.class.getDeclaredMethod("ensureVersionHint");
    method.setAccessible(true);
    // Should not throw
    method.invoke(writer);
  }
}
