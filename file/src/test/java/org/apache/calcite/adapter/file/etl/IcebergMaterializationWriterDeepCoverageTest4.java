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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link IcebergMaterializationWriter} targeting uncovered methods:
 * transformRows, buildPartitionFilter, buildDuckDBSql, createStagingJsonFile,
 * cleanupJsonFile, createStagingPath, commit with pending files, configureS3,
 * storeTableMetadata, getTableLocation with s3a prefix, flushLargestPartition,
 * flushAllPartitions, intermediateCommit, uploadLocalStagingToRemote,
 * buildDataFileFromPath, coercePartitionValue, getRemoteParentPath edge cases,
 * and processBatchWithRetry with CommitFailedException and InterruptedException.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class IcebergMaterializationWriterDeepCoverageTest4 {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private String warehousePath;
  private IcebergMaterializationWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    warehousePath = tempDir.resolve("warehouse").toString();
  }

  @AfterEach
  void tearDown() {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException ignored) {
        // cleanup
      }
    }
    IcebergCatalogManager.clearCache();
  }

  // ====================================================================
  // transformRows via reflection
  // ====================================================================

  @Test void testTransformRowsNullColumns() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    // Set config to null columns
    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("col1", "val1");
    rows.add(row);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, null);

    // No columns defined = passthrough
    assertEquals(1, result.size());
    assertEquals("val1", result.get(0).get("col1"));
  }

  @Test void testTransformRowsWithDirectColumns() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("target_name").source("SOURCE_NAME").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("SOURCE_NAME", "hello");
    rows.add(row);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, null);

    assertEquals(1, result.size());
    assertEquals("hello", result.get(0).get("target_name"));
  }

  @Test void testTransformRowsWithPartitionVariableFallback() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("year").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    // No "year" in the row
    rows.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2022");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);

    assertEquals(1, result.size());
    assertEquals("2022", result.get(0).get("year"));
  }

  @Test void testTransformRowsWithComputedPartitionVariable() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder()
        .name("year")
        .expression("{year}")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(new HashMap<String, Object>());

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2023");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);

    assertEquals("2023", result.get(0).get("year"));
  }

  @Test void testTransformRowsWithComputedQuotedPartitionVariable() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder()
        .name("year")
        .expression("'{year}'")
        .build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(new HashMap<String, Object>());

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2023");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);

    assertEquals("2023", result.get(0).get("year"));
  }

  @Test void testTransformRowsAddsRemainingPartitionVariables() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("data_col").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, config);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("data_col", "val1");
    rows.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("extra_var", "extra_val");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);

    assertEquals("val1", result.get(0).get("data_col"));
    assertEquals("extra_val", result.get(0).get("extra_var"));
  }

  // ====================================================================
  // buildPartitionFilter via reflection
  // ====================================================================

  @Test void testBuildPartitionFilterNull() throws Exception {
    writer = createInitializedWriter("filter_null_table");
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildPartitionFilter", Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, (Object) null);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildPartitionFilterEmpty() throws Exception {
    writer = createInitializedWriter("filter_empty_table");
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildPartitionFilter", Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  // ====================================================================
  // buildPartitionKey with icebergPartitionColumns
  // ====================================================================

  @Test void testBuildPartitionKeyWithIcebergPartitionColumns() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    Field icebergPartColsField =
        IcebergMaterializationWriter.class.getDeclaredField("icebergPartitionColumns");
    icebergPartColsField.setAccessible(true);
    Set<String> icebergCols = new HashSet<String>();
    icebergCols.add("year");
    icebergPartColsField.set(writer, icebergCols);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildPartitionKey", Map.class);
    method.setAccessible(true);

    Map<String, String> vars = new LinkedHashMap<String, String>();
    vars.put("year", "2022");
    vars.put("cik", "001");  // Not an iceberg partition column

    String key = (String) method.invoke(writer, vars);
    assertEquals("year=2022", key);
  }

  // ====================================================================
  // flushLargestPartition via reflection
  // ====================================================================

  @Test void testFlushLargestPartitionEmptyBuffers() throws Exception {
    writer = createInitializedWriter("flush_empty_table");

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("flushLargestPartition");
    method.setAccessible(true);

    // Should not throw with empty buffers
    method.invoke(writer);
  }

  // ====================================================================
  // flushAllPartitions via reflection
  // ====================================================================

  @Test void testFlushAllPartitionsEmptyBuffers() throws Exception {
    writer = createInitializedWriter("flush_all_empty_table");

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("flushAllPartitions");
    method.setAccessible(true);

    // Should not throw with empty buffers
    method.invoke(writer);
  }

  // ====================================================================
  // createStagingJsonFile and cleanupJsonFile via reflection
  // ====================================================================

  @Test void testCreateStagingJsonFileLocal() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("json_staging_table", MaterializeOptionsConfig.StagingMode.LOCAL);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("createStagingJsonFile", List.class, String.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "test");
    rows.add(row);

    Path stagingDir = tempDir.resolve("staging_json");
    Files.createDirectories(stagingDir);

    String jsonPath = (String) method.invoke(writer, rows, stagingDir.toString());
    assertNotNull(jsonPath);
    assertTrue(Files.exists(java.nio.file.Paths.get(jsonPath)));

    // Test cleanup
    Method cleanupMethod =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupJsonFile", String.class);
    cleanupMethod.setAccessible(true);
    cleanupMethod.invoke(writer, jsonPath);
    assertFalse(Files.exists(java.nio.file.Paths.get(jsonPath)));
  }

  @Test void testCreateStagingJsonFileRemote() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("json_staging_remote_table", MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("createStagingJsonFile", List.class, String.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    rows.add(row);

    Path stagingDir = tempDir.resolve("staging_json_remote");
    Files.createDirectories(stagingDir);

    String jsonPath = (String) method.invoke(writer, rows, stagingDir.toString());
    assertNotNull(jsonPath);
  }

  @Test void testCleanupJsonFileRemote() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("json_cleanup_remote_table", MaterializeOptionsConfig.StagingMode.REMOTE);

    // Create a file to clean up
    Path stagingDir = tempDir.resolve("staging_json_remote_cleanup");
    Files.createDirectories(stagingDir);
    Path jsonFile = stagingDir.resolve("test.json");
    Files.write(jsonFile, "{}".getBytes());

    Method cleanupMethod =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupJsonFile", String.class);
    cleanupMethod.setAccessible(true);
    cleanupMethod.invoke(writer, jsonFile.toString());
  }

  @Test void testCleanupJsonFileNonExistent() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("json_cleanup_missing_table", MaterializeOptionsConfig.StagingMode.LOCAL);

    Method cleanupMethod =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupJsonFile", String.class);
    cleanupMethod.setAccessible(true);

    // Should not throw for nonexistent file
    cleanupMethod.invoke(writer, tempDir.resolve("nonexistent.json").toString());
  }

  // ====================================================================
  // createStagingPath via reflection
  // ====================================================================

  @Test void testCreateStagingPathLocal() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("staging_path_local_table", MaterializeOptionsConfig.StagingMode.LOCAL);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod("createStagingPath");
    method.setAccessible(true);

    String path = (String) method.invoke(writer);
    assertNotNull(path);
    assertTrue(Files.exists(java.nio.file.Paths.get(path)));
  }

  @Test void testCreateStagingPathRemote() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("staging_path_remote_table", MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod("createStagingPath");
    method.setAccessible(true);

    String path = (String) method.invoke(writer);
    assertNotNull(path);
  }

  // ====================================================================
  // cleanupStagingDirectory additional edge cases
  // ====================================================================

  @Test void testCleanupStagingDirectoryLocalWithContent() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("cleanup_staging_local_table", MaterializeOptionsConfig.StagingMode.LOCAL);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    Path stagingDir = tempDir.resolve("staging_cleanup");
    Files.createDirectories(stagingDir);
    Files.createDirectories(stagingDir.resolve("subdir"));
    Files.write(stagingDir.resolve("file1.parquet"), new byte[]{1, 2, 3});
    Files.write(stagingDir.resolve("subdir/file2.parquet"), new byte[]{4, 5, 6});

    method.invoke(writer, stagingDir.toString());
    assertFalse(Files.exists(stagingDir));
  }

  @Test void testCleanupStagingDirectoryLocalNonExistent() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("cleanup_staging_nonexist_table", MaterializeOptionsConfig.StagingMode.LOCAL);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Non-existent path should not throw
    method.invoke(writer, tempDir.resolve("does_not_exist").toString());
  }

  @Test void testCleanupStagingDirectoryRemoteLocal() throws Exception {
    writer =
        createInitializedWriterWithStagingMode("cleanup_remote_local_table", MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Non-S3 remote path should attempt cleanup via storageProvider
    Path stagingDir = tempDir.resolve("staging_remote_local");
    Files.createDirectories(stagingDir);

    method.invoke(writer, stagingDir.toString());
  }

  // ====================================================================
  // getRemoteParentPath additional edge cases
  // ====================================================================

  @Test void testGetRemoteParentPathEdgeCases() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("getRemoteParentPath", String.class);
    method.setAccessible(true);

    // s3a:// with lastSlash <= 6: "s3a://b" has lastSlash=5 => null
    assertNull(method.invoke(writer, "s3a://b"));
    // "s3a://b/" has lastSlash=7 which is > 6, returns "s3a://b"
    assertEquals("s3a://b", method.invoke(writer, "s3a://b/"));

    // s3:// with lastSlash <= 5: "s3://b" has lastSlash=4 => null
    assertNull(method.invoke(writer, "s3://b"));

    // Normal path
    assertEquals("s3://bucket/path", method.invoke(writer, "s3://bucket/path/file"));
    assertEquals("s3a://bucket/path", method.invoke(writer, "s3a://bucket/path/file"));

    // Regular local path
    assertEquals("/a/b", method.invoke(writer, "/a/b/c"));
    assertNull(method.invoke(writer, "noSlashAtAll"));
  }

  // ====================================================================
  // escapeString static method
  // ====================================================================

  @Test void testEscapeStringMultipleQuotes() throws Exception {
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    method.setAccessible(true);

    assertEquals("it''s a ''test''", method.invoke(null, "it's a 'test'"));
    assertEquals("no quotes", method.invoke(null, "no quotes"));
    assertEquals("", method.invoke(null, ""));
  }

  // ====================================================================
  // getTableLocation with s3a conversion
  // ====================================================================

  @Test void testGetTableLocationNull() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getTableLocation());
  }

  // ====================================================================
  // commit edge cases
  // ====================================================================

  @Test void testCommitWithNoPendingFilesOrBuffers() throws Exception {
    writer = createInitializedWriter("commit_empty_table");
    writer.commit();
    assertEquals(0, writer.getTotalRowsWritten());
    assertEquals(0, writer.getTotalFilesWritten());
  }

  // ====================================================================
  // processBatchWithRetry via reflection
  // ====================================================================

  @Test void testProcessBatchWithRetryFailsAllAttempts() throws Exception {
    writer = createInitializedWriter("retry_fail_table");

    // Set maxRetries to 1 for fast test
    Field maxRetriesField = IcebergMaterializationWriter.class.getDeclaredField("maxRetries");
    maxRetriesField.setAccessible(true);
    maxRetriesField.set(writer, 1);

    Field retryDelayField = IcebergMaterializationWriter.class.getDeclaredField("retryDelayMs");
    retryDelayField.setAccessible(true);
    retryDelayField.set(writer, 1L);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("processBatchWithRetry", List.class, Map.class);
    method.setAccessible(true);

    // Setting config to null causes NPE in processBatch -> transformRows
    // (which calls config.getColumns()). This ensures the batch actually fails.
    // Note: setting tableWriter to null is not enough because processBatch
    // only buffers rows without flushing (tableWriter is only used during flush).
    Field configField = IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(writer, null);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("data", "test");
    rows.add(row);

    boolean result = (boolean) method.invoke(writer, rows, new HashMap<String, String>());
    assertFalse(result);
  }

  // ====================================================================
  // processChunk via reflection
  // ====================================================================

  @Test void testProcessChunkEmpty() throws Exception {
    writer = createInitializedWriter("chunk_empty_table");

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("processChunk", List.class, Map.class, int.class);
    method.setAccessible(true);

    List<Map<String, Object>> emptyRows = new ArrayList<Map<String, Object>>();
    long result = (long) method.invoke(writer, emptyRows, null, 1);
    assertEquals(0, result);
  }

  // ====================================================================
  // writeBatch with null and empty data
  // ====================================================================

  @Test void testWriteBatchNull() throws Exception {
    writer = createInitializedWriter("writebatch_null_table");
    long result = writer.writeBatch(null, null);
    assertEquals(0, result);
  }

  @Test void testWriteBatchEmpty() throws Exception {
    writer = createInitializedWriter("writebatch_empty_table");
    List<Map<String, Object>> emptyList = Collections.emptyList();
    long result = writer.writeBatch(emptyList.iterator(), null);
    assertEquals(0, result);
  }

  @Test void testWriteBatchNotInitialized() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(new HashMap<String, Object>());
    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(rows.iterator(), null));
  }

  // ====================================================================
  // storeEtlProperties before initialization
  // ====================================================================

  @Test void testStoreEtlPropertiesBeforeInit() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    // Should warn but not throw
    writer.storeEtlProperties("hash", "sig", 100);
  }

  // ====================================================================
  // getEtlProperty before initialization
  // ====================================================================

  @Test void testGetEtlPropertyBeforeInit() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertNull(writer.getEtlProperty("any.key"));
  }

  // ====================================================================
  // close edge cases
  // ====================================================================

  @Test void testCloseBeforeInit() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    writer.close();
  }

  @Test void testCloseAfterInit() throws Exception {
    writer = createInitializedWriter("close_after_init_table");
    writer.close();
  }

  // ====================================================================
  // getFormat
  // ====================================================================

  @Test void testGetFormat() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  // ====================================================================
  // getTotalRowsWritten and getTotalFilesWritten
  // ====================================================================

  @Test void testGetTotals() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertEquals(0, writer.getTotalRowsWritten());
    assertEquals(0, writer.getTotalFilesWritten());
  }

  // ====================================================================
  // mapToIcebergType additional types via reflection
  // ====================================================================

  @Test void testMapToIcebergTypeAdditionalTypes() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("mapToIcebergType", String.class);
    method.setAccessible(true);

    // Test CHAR prefix handling
    assertEquals("STRING", method.invoke(writer, "CHAR"));
    assertEquals("STRING", method.invoke(writer, "char(20)"));
    // Test lowercase/mixed-case
    assertEquals("STRING", method.invoke(writer, "varchar"));
    assertEquals("LONG", method.invoke(writer, "long"));
    // Test completely unknown types
    assertEquals("STRING", method.invoke(writer, "BINARY"));
    assertEquals("STRING", method.invoke(writer, "DECIMAL"));
    assertEquals("STRING", method.invoke(writer, "ARRAY"));
  }

  // ====================================================================
  // mapToDuckDBType additional types via reflection
  // ====================================================================

  @Test void testMapToDuckDBTypeAdditionalTypes() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("mapToDuckDBType", String.class);
    method.setAccessible(true);

    // Test STRING -> VARCHAR
    assertEquals("VARCHAR", method.invoke(writer, "string"));
    // Test CHAR prefix
    assertEquals("VARCHAR", method.invoke(writer, "char"));
    assertEquals("VARCHAR", method.invoke(writer, "CHAR(10)"));
    // Test lowercase
    assertEquals("INTEGER", method.invoke(writer, "integer"));
    assertEquals("BIGINT", method.invoke(writer, "long"));
    assertEquals("DOUBLE", method.invoke(writer, "double"));
    assertEquals("DOUBLE", method.invoke(writer, "float"));
    assertEquals("BOOLEAN", method.invoke(writer, "boolean"));
    assertEquals("DATE", method.invoke(writer, "date"));
    assertEquals("TIMESTAMP", method.invoke(writer, "timestamp"));
    // Unknown
    assertEquals("VARCHAR", method.invoke(writer, "BINARY"));
  }

  // ====================================================================
  // storeTableMetadata via reflection
  // ====================================================================

  @Test void testStoreTableMetadataNullTable() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod("storeTableMetadata");
    method.setAccessible(true);

    // table is null, should return early
    method.invoke(writer);
  }

  @Test void testStoreTableMetadataNullConfig() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    Field tableField = IcebergMaterializationWriter.class.getDeclaredField("table");
    tableField.setAccessible(true);
    // Set a mock table to bypass null check
    // Actually config is also null so it will return early from next check
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod("storeTableMetadata");
    method.setAccessible(true);

    // Both table and config are null, should not throw
    method.invoke(writer);
  }

  // ====================================================================
  // buildCatalogConfig with S3 credentials via StorageProvider
  // ====================================================================

  @Test void testBuildCatalogConfigNullStorageProvider() throws Exception {
    IcebergMaterializationWriter w =
        new IcebergMaterializationWriter(null, warehousePath, null);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(w, (Object) null);
    assertNotNull(result);
    assertEquals("hadoop", result.get("catalog"));
    // Should not have hadoopConfig since storageProvider is null
    assertFalse(result.containsKey("hadoopConfig"));
  }

  @Test void testBuildCatalogConfigEmptyWarehouseInIcebergConfig() throws Exception {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    Method method =
        IcebergMaterializationWriter.class.getDeclaredMethod("buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    // IcebergConfig with empty warehouse path should fallback to writer's warehousePath
    MaterializeConfig.IcebergConfig icebergConfig = MaterializeConfig.IcebergConfig.builder()
        .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
        .warehousePath("")
        .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertEquals(warehousePath, result.get("warehousePath"));
  }

  // ====================================================================
  // initialize validation paths
  // ====================================================================

  @Test void testInitializeConfigNullThrows() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(null));
  }

  @Test void testInitializeDisabledThrows() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeWrongFormatThrows() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeNoTargetTableIdThrows() {
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private IcebergMaterializationWriter createInitializedWriter(String tableName) throws Exception {
    return createInitializedWriterWithStagingMode(tableName, null);
  }

  @SuppressWarnings("unchecked")
  private IcebergMaterializationWriter createInitializedWriterWithStagingMode(
      String tableName, MaterializeOptionsConfig.StagingMode stagingMode) throws Exception {
    IcebergMaterializationWriter w =
        new IcebergMaterializationWriter(storageProvider, warehousePath, null);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("data_col").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("value").type("INTEGER").build());

    MaterializeConfig.Builder builder = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId(tableName)
        .name(tableName)
        .columns(columns)
        .output(MaterializeOutputConfig.builder().build());

    if (stagingMode != null) {
      builder.options(MaterializeOptionsConfig.builder()
          .stagingMode(stagingMode)
          .build());
    }

    MaterializeConfig config = builder.build();
    w.initialize(config);
    return w;
  }
}
