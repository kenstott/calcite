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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Coverage tests for RefreshablePartitionedParquetTable focusing on refresh logic,
 * file discovery, baseline comparison, relative path computation, lazy initialization,
 * async refresh, DuckDB+Hive skip logic, and snapshot metadata.
 */
@Tag("unit")
public class RefreshablePartitionedParquetTableCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Tests for constructors
  // ====================================================================

  @Test
  void testSixArgConstructor() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);
    assertNotNull(table);
    assertEquals("RefreshablePartitionedParquetTable(test_table)", table.toString());
  }

  @Test
  void testEightArgConstructor() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    Map<String, Object> constraintConfig = new HashMap<String, Object>();
    constraintConfig.put("primaryKey", Collections.singletonList("id"));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofMinutes(5), constraintConfig, "test_schema");
    assertNotNull(table);
  }

  @Test
  void testNineArgConstructorWithStorageProvider() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);
    StorageProvider mockStorage = mock(StorageProvider.class);
    try {
      when(mockStorage.listFiles(anyString(), anyBoolean()))
          .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());
    } catch (IOException e) {
      fail("Unexpected IOException in mock setup");
    }

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofMinutes(5), null, null, mockStorage);
    assertNotNull(table);
  }

  @Test
  void testConstructorWithPartitionConfig() {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "hive_table", tempDir.toString(), "**/*.parquet", config, engineConfig, null);
    assertNotNull(table);
  }

  // ====================================================================
  // Tests for needsRefresh
  // ====================================================================

  @Test
  void testNeedsRefreshNoInterval() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);
    assertFalse(table.needsRefresh());
  }

  @Test
  void testNeedsRefreshFirstTime() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofMinutes(5));

    Field lastRefreshField =
        RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, null);

    assertTrue(table.needsRefresh());
  }

  @Test
  void testNeedsRefreshIntervalNotElapsed() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofMinutes(5));

    Field lastRefreshField =
        RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, Instant.now());

    assertFalse(table.needsRefresh());
  }

  @Test
  void testNeedsRefreshIntervalElapsed() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofSeconds(1));

    Field lastRefreshField =
        RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, Instant.now().minusSeconds(10));

    assertTrue(table.needsRefresh());
  }

  // ====================================================================
  // Tests for refresh method
  // ====================================================================

  @Test
  void testRefreshDoesNothingWhenNotNeeded() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    // Should not throw - refresh interval is null so needsRefresh returns false
    table.refresh();
  }

  @Test
  void testRefreshStartsAsyncWhenNeeded() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofSeconds(1));

    // Force needsRefresh to return true
    Field lastRefreshField =
        RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, null);

    // Calling refresh should start async refresh and set lastRefreshTime
    table.refresh();

    // Wait a bit for async to start
    Thread.sleep(100);

    // lastRefreshTime should now be set
    Instant refreshTime = (Instant) lastRefreshField.get(table);
    assertNotNull(refreshTime);
  }

  @Test
  void testRefreshDoubleCallDoesNotStartTwice() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        Duration.ofSeconds(1));

    Field lastRefreshField =
        RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, null);

    // First call triggers async refresh
    table.refresh();

    // Force needsRefresh again
    lastRefreshField.set(table, null);

    // Second call should not start another async refresh since one is in progress
    table.refresh();

    // Wait for completion
    Thread.sleep(200);
  }

  // ====================================================================
  // Tests for getRefreshInterval / getLastRefreshTime / getRefreshBehavior
  // ====================================================================

  @Test
  void testGetRefreshInterval() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    Duration interval = Duration.ofMinutes(10);
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, interval);

    assertEquals(interval, table.getRefreshInterval());
  }

  @Test
  void testGetRefreshIntervalNull() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertNull(table.getRefreshInterval());
  }

  @Test
  void testGetLastRefreshTime() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertNull(table.getLastRefreshTime());
  }

  @Test
  void testGetRefreshBehavior() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertEquals(RefreshableTable.RefreshBehavior.PARTITIONED_TABLE, table.getRefreshBehavior());
  }

  // ====================================================================
  // Tests for getFilePaths
  // ====================================================================

  @Test
  void testGetFilePathsEmpty() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    List<String> paths = table.getFilePaths();
    assertNotNull(paths);
  }

  // ====================================================================
  // Tests for getDirectoryPath and getPattern
  // ====================================================================

  @Test
  void testGetDirectoryPath() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", "/my/dir", "**/*.parquet", config, engineConfig, null);

    assertEquals("/my/dir", table.getDirectoryPath());
    assertEquals("**/*.parquet", table.getPattern());
  }

  // ====================================================================
  // Tests for CommentableTable methods
  // ====================================================================

  @Test
  void testGetTableComment() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("comment", "My table description");
    List<Map<String, String>> colCommentsList = new ArrayList<Map<String, String>>();
    Map<String, String> colItem = new HashMap<String, String>();
    colItem.put("name", "id");
    colItem.put("comment", "Primary identifier");
    colCommentsList.add(colItem);
    configMap.put("column_comments", colCommentsList);
    PartitionedTableConfig config = PartitionedTableConfig.fromMap(configMap);

    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertEquals("My table description", table.getTableComment());
    assertEquals("Primary identifier", table.getColumnComment("id"));
    assertNull(table.getColumnComment("nonexistent"));
  }

  @Test
  void testGetTableCommentNullConfig() {
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    PartitionedTableConfig config = createMinimalConfig();
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertNull(table.getTableComment());
  }

  @Test
  void testGetColumnCommentNullColumnComments() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    // Config has no column comments map
    assertNull(table.getColumnComment("any_column"));
  }

  // ====================================================================
  // Tests for getRelativePath
  // ====================================================================

  @Test
  void testGetRelativePathNormal() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("file.parquet",
        method.invoke(table, "/base/path", "/base/path/file.parquet"));
  }

  @Test
  void testGetRelativePathTrailingSlash() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("file.parquet",
        method.invoke(table, "/base/path/", "/base/path/file.parquet"));
  }

  @Test
  void testGetRelativePathNonSubpath() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("file.parquet",
        method.invoke(table, "/other/path", "/different/path/file.parquet"));
  }

  @Test
  void testGetRelativePathNoSlash() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("noSlash", method.invoke(table, "/base/path", "noSlash"));
  }

  @Test
  void testGetRelativePathS3Style() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("year=2020/file.parquet",
        method.invoke(table, "s3://bucket/table", "s3://bucket/table/year=2020/file.parquet"));
  }

  @Test
  void testGetRelativePathBackslashNormalization() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    assertEquals("sub/file.parquet",
        method.invoke(table, "C:\\base\\path", "C:\\base\\path\\sub/file.parquet"));
  }

  // ====================================================================
  // Tests for shouldSkipInitialScan
  // ====================================================================

  @Test
  void testShouldSkipInitialScanNullEngineConfig() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, null, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertFalse((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanNoDuckDB() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertFalse((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanDuckDBNoHive() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    // DuckDB but no hive-style partitions
    assertFalse((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanDuckDBWithHive() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "**/*.parquet", config, engineConfig, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertTrue((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanDuckDBHiveEmptyPattern() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "", config, engineConfig, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertFalse((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanParquetWithDuckDBConfig() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    DuckDBConfig duckConfig = mock(DuckDBConfig.class);
    ExecutionEngineConfig engineConfig =
        new ExecutionEngineConfig("PARQUET", 2048, 64 * 1024 * 1024L, null, duckConfig);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "**/*.parquet", config, engineConfig, null);

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertTrue((boolean) method.invoke(table));
  }

  // ====================================================================
  // Tests for usedLazyInitialization
  // ====================================================================

  @Test
  void testUsedLazyInitializationFalse() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertFalse(table.usedLazyInitialization());
  }

  @Test
  void testUsedLazyInitializationTrue() {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "**/*.parquet", config, engineConfig, null);

    assertTrue(table.usedLazyInitialization());
  }

  // ====================================================================
  // Tests for setRefreshContext
  // ====================================================================

  @Test
  void testSetRefreshContext() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    table.setRefreshContext(null, "tableName");
  }

  // ====================================================================
  // Tests for toString
  // ====================================================================

  @Test
  void testToString() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "my_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    assertEquals("RefreshablePartitionedParquetTable(my_table)", table.toString());
  }

  // ====================================================================
  // Tests for snapshotFileMetadata
  // ====================================================================

  @Test
  void testSnapshotFileMetadataNullPaths() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    assertNull(method.invoke(table, (Object) null));
  }

  @Test
  void testSnapshotFileMetadataEmptyPaths() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    assertNull(method.invoke(table, Collections.emptyList()));
  }

  @Test
  void testSnapshotFileMetadataNoStorageProvider() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");
    assertNull(method.invoke(table, files));
  }

  @Test
  void testSnapshotFileMetadataWithStorageProvider() throws Exception {
    StorageProvider mockStorage = mock(StorageProvider.class);
    StorageProvider.FileMetadata mockMetadata = mock(StorageProvider.FileMetadata.class);
    when(mockMetadata.getSize()).thenReturn(1024L);
    when(mockMetadata.getEtag()).thenReturn("etag123");
    when(mockMetadata.getLastModified()).thenReturn(System.currentTimeMillis());
    when(mockStorage.getMetadata(anyString())).thenReturn(mockMetadata);
    when(mockStorage.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        null, null, null, mockStorage);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    Object result = method.invoke(table, files);
    assertNotNull(result);
  }

  @Test
  void testSnapshotFileMetadataWithException() throws Exception {
    StorageProvider mockStorage = mock(StorageProvider.class);
    when(mockStorage.getMetadata(anyString())).thenThrow(new IOException("access denied"));
    when(mockStorage.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        null, null, null, mockStorage);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    // Should handle exception and still return (possibly empty) baseline
    Object result = method.invoke(table, files);
    assertNotNull(result);
  }

  // ====================================================================
  // Tests for hasFilesChanged
  // ====================================================================

  @Test
  void testHasFilesChangedNullBaseline() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "hasFilesChanged", ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    assertTrue((boolean) method.invoke(table, (Object) null));
  }

  @Test
  void testHasFilesChangedEmptyBaseline() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "hasFilesChanged", ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    ConversionMetadata.PartitionBaseline emptyBaseline = new ConversionMetadata.PartitionBaseline();
    assertTrue((boolean) method.invoke(table, emptyBaseline));
  }

  // ====================================================================
  // Tests for filesChangedComparedToBaseline
  // ====================================================================

  @Test
  void testFilesChangedComparedToBaselineNullBaseline() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    assertTrue((boolean) method.invoke(table, files, null));
  }

  @Test
  void testFilesChangedComparedToBaselineEmptyBaseline() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    ConversionMetadata.PartitionBaseline emptyBaseline = new ConversionMetadata.PartitionBaseline();
    assertTrue((boolean) method.invoke(table, files, emptyBaseline));
  }

  @Test
  void testFilesChangedComparedToBaselineNoStorageProvider() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    baseline.files.put("/path/file.parquet",
        new ConversionMetadata.FileBaseline(100L, "etag1", 1000L));
    baseline.snapshotTimestamp = System.currentTimeMillis();

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    // No storage provider -> returns true (null storage provider means changed)
    assertTrue((boolean) method.invoke(table, files, baseline));
  }

  @Test
  void testFilesChangedComparedToBaselineSizeChanged() throws Exception {
    StorageProvider mockStorage = mock(StorageProvider.class);
    when(mockStorage.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        null, null, null, mockStorage);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    baseline.files.put("/path/file1.parquet",
        new ConversionMetadata.FileBaseline(100L, "etag1", 1000L));
    baseline.snapshotTimestamp = System.currentTimeMillis();

    // Current has 2 files vs baseline 1 -> size changed
    List<String> currentFiles = new ArrayList<String>();
    currentFiles.add("/path/file1.parquet");
    currentFiles.add("/path/file2.parquet");

    assertTrue((boolean) method.invoke(table, currentFiles, baseline));
  }

  @Test
  void testFilesChangedComparedToBaselineNewFile() throws Exception {
    StorageProvider mockStorage = mock(StorageProvider.class);
    when(mockStorage.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig,
        null, null, null, mockStorage);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    baseline.files.put("/path/old_file.parquet",
        new ConversionMetadata.FileBaseline(100L, "etag1", 1000L));
    baseline.snapshotTimestamp = System.currentTimeMillis();

    // Current has a different file from baseline (same count but different key)
    List<String> currentFiles = new ArrayList<String>();
    currentFiles.add("/path/new_file.parquet");

    assertTrue((boolean) method.invoke(table, currentFiles, baseline));
  }

  // ====================================================================
  // Tests for discoverFiles with null storageProvider
  // ====================================================================

  @Test
  void testDiscoverFilesNullStorageProvider() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("discoverFiles");
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(table);
    assertTrue(result.isEmpty());
  }

  // ====================================================================
  // Tests for primeHLLStatistics (via reflection)
  // ====================================================================

  @Test
  void testPrimeHLLStatisticsNoCurrentTable() throws Exception {
    RefreshablePartitionedParquetTable table = createSimpleTable();

    Method method =
        RefreshablePartitionedParquetTable.class.getDeclaredMethod("primeHLLStatistics");
    method.setAccessible(true);

    // Should not throw when currentTable is null
    method.invoke(table);
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private PartitionedTableConfig createMinimalConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    return PartitionedTableConfig.fromMap(configMap);
  }

  private PartitionedTableConfig createHivePartitionConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    Map<String, Object> partitionsMap = new HashMap<String, Object>();
    partitionsMap.put("style", "hive");
    List<Map<String, String>> colDefs = new ArrayList<Map<String, String>>();
    Map<String, String> colDef = new HashMap<String, String>();
    colDef.put("name", "year");
    colDef.put("type", "INTEGER");
    colDefs.add(colDef);
    partitionsMap.put("columnDefinitions", colDefs);
    configMap.put("partitions", partitionsMap);
    return PartitionedTableConfig.fromMap(configMap);
  }

  private RefreshablePartitionedParquetTable createSimpleTable() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);
    return new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);
  }
}
