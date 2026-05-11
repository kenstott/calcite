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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link RefreshablePartitionedParquetTable}.
 * Targets uncovered branches: shouldSkipInitialScan, needsRefresh,
 * refresh, getFilePaths, getRelativePath, snapshotFileMetadata,
 * hasFilesChanged, filesChangedComparedToBaseline, recreateViewWithNewFiles,
 * refreshTableDefinitionAsync, and CommentableTable methods.
 */
@Tag("unit")
class RefreshablePartitionedParquetTableDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Tests for shouldSkipInitialScan (private method)
  // ====================================================================

  @Test
  void testShouldSkipInitialScanWithNullEngineConfig() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip when engineConfig is null");
  }

  @Test
  void testShouldSkipInitialScanWithLinq4jEngine() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("LINQ4J", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip for non-DuckDB engine");
  }

  @Test
  void testShouldSkipInitialScanWithDuckDBEngineNonHivePartition() throws Exception {
    // Non-hive partition config
    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("auto", null, null, null, null);
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", partConfig);
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip for non-hive partition style");
  }

  @Test
  void testShouldSkipInitialScanWithDuckDBEngineNullPartitions() throws Exception {
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", null);
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip when partitions config is null");
  }

  @Test
  void testShouldSkipInitialScanWithDuckDBEngineNullPattern() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), null, config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip when pattern is null");
  }

  @Test
  void testShouldSkipInitialScanWithDuckDBEngineEmptyPattern() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("DUCKDB", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "  ", config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertFalse(result, "Should not skip when pattern is blank");
  }

  @Test
  void testShouldSkipInitialScanWithParquetEngineAndDuckDBConfig() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    DuckDBConfig duckDBConfig = mock(DuckDBConfig.class);
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig(
        "PARQUET", 2048, 64L * 1024 * 1024, null, duckDBConfig);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, null, null, null, storageProvider);

    boolean result = invokeShouldSkipInitialScan(table);
    assertTrue(result, "Should skip for PARQUET engine with DuckDB config and hive pattern");
  }

  // ====================================================================
  // Tests for usedLazyInitialization
  // ====================================================================

  @Test
  void testUsedLazyInitializationReturnsFalseForNonDuckDB() throws Exception {
    PartitionedTableConfig config = createHivePartitionConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("LINQ4J", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, null, null, null, storageProvider);

    assertFalse(table.usedLazyInitialization());
  }

  // ====================================================================
  // Tests for getDirectoryPath and getPattern
  // ====================================================================

  @Test
  void testGetDirectoryPath() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", "/my/data/dir", "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals("/my/data/dir", table.getDirectoryPath());
  }

  @Test
  void testGetPattern() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "year=*/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals("year=*/*.parquet", table.getPattern());
  }

  // ====================================================================
  // Tests for needsRefresh
  // ====================================================================

  @Test
  void testNeedsRefreshWithNullInterval() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertFalse(table.needsRefresh(), "Should not need refresh when interval is null");
  }

  @Test
  void testNeedsRefreshWithNullLastRefreshTime() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, Duration.ofMinutes(5), null, null, storageProvider);

    assertTrue(table.needsRefresh(), "Should need refresh when lastRefreshTime is null");
  }

  @Test
  void testNeedsRefreshWithRecentRefresh() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, Duration.ofHours(1), null, null, storageProvider);

    // Set lastRefreshTime to now
    Field lastRefreshField = RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, Instant.now());

    assertFalse(table.needsRefresh(), "Should not need refresh when recently refreshed");
  }

  @Test
  void testNeedsRefreshWithExpiredInterval() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, Duration.ofSeconds(1), null, null, storageProvider);

    // Set lastRefreshTime to a past time well beyond the 1-second interval
    Field lastRefreshField = RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, Instant.now().minus(Duration.ofMinutes(5)));

    assertTrue(table.needsRefresh(), "Should need refresh when interval has elapsed");
  }

  // ====================================================================
  // Tests for getRefreshInterval and getLastRefreshTime
  // ====================================================================

  @Test
  void testGetRefreshIntervalReturnsConfiguredValue() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    Duration interval = Duration.ofMinutes(30);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, interval, null, null, storageProvider);

    assertEquals(interval, table.getRefreshInterval());
  }

  @Test
  void testGetRefreshIntervalReturnsNull() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertNull(table.getRefreshInterval());
  }

  @Test
  void testGetLastRefreshTimeInitiallyNull() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertNull(table.getLastRefreshTime());
  }

  // ====================================================================
  // Tests for getRefreshBehavior
  // ====================================================================

  @Test
  void testGetRefreshBehavior() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals(RefreshableTable.RefreshBehavior.PARTITIONED_TABLE,
        table.getRefreshBehavior());
  }

  // ====================================================================
  // Tests for getFilePaths
  // ====================================================================

  @Test
  void testGetFilePathsWithNoCurrentTableAndNoDiscoveredFiles() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    List<String> paths = table.getFilePaths();
    assertNotNull(paths);
    assertTrue(paths.isEmpty());
  }

  // ====================================================================
  // Tests for getRelativePath (private method)
  // ====================================================================

  @Test
  void testGetRelativePathSubpath() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table, "/data/base", "/data/base/year=2020/file.parquet");
    assertEquals("year=2020/file.parquet", result);
  }

  @Test
  void testGetRelativePathSubpathTrailingSlash() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table, "/data/base/", "/data/base/file.parquet");
    assertEquals("file.parquet", result);
  }

  @Test
  void testGetRelativePathNonSubpath() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table, "/different/base", "/data/other/file.parquet");
    assertEquals("file.parquet", result);
  }

  @Test
  void testGetRelativePathNoSeparator() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table, "/base", "justfilename");
    assertEquals("justfilename", result);
  }

  @Test
  void testGetRelativePathS3URIs() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table,
        "s3://bucket/table", "s3://bucket/table/part=1/file.parquet");
    assertEquals("part=1/file.parquet", result);
  }

  @Test
  void testGetRelativePathBackslashes() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    String result = invokeGetRelativePath(table,
        "C:\\data\\base", "C:\\data\\base\\year=2020\\file.parquet");
    assertEquals("year=2020/file.parquet", result);
  }

  // ====================================================================
  // Tests for snapshotFileMetadata (private method)
  // ====================================================================

  @Test
  void testSnapshotFileMetadataWithNullFiles() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline result = invokeSnapshotFileMetadata(table, null);
    assertNull(result);
  }

  @Test
  void testSnapshotFileMetadataWithEmptyFiles() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline result =
        invokeSnapshotFileMetadata(table, Collections.emptyList());
    assertNull(result);
  }

  @Test
  void testSnapshotFileMetadataWithNullStorageProvider() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    // Create table without a storage provider
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, null);

    ConversionMetadata.PartitionBaseline result =
        invokeSnapshotFileMetadata(table, Arrays.asList("/file1.parquet"));
    assertNull(result, "Should return null when storageProvider is null");
  }

  @Test
  void testSnapshotFileMetadataWithValidFiles() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    StorageProvider.FileMetadata fileMeta = mock(StorageProvider.FileMetadata.class);
    when(fileMeta.getSize()).thenReturn(1024L);
    when(fileMeta.getEtag()).thenReturn("abc123");
    when(fileMeta.getLastModified()).thenReturn(System.currentTimeMillis());
    when(storageProvider.getMetadata("/file1.parquet")).thenReturn(fileMeta);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline result =
        invokeSnapshotFileMetadata(table, Arrays.asList("/file1.parquet"));
    assertNotNull(result);
    assertFalse(result.isEmpty());
    assertEquals(1, result.files.size());
  }

  @Test
  void testSnapshotFileMetadataWithMetadataException() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());
    when(storageProvider.getMetadata(anyString())).thenThrow(new RuntimeException("Network error"));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline result =
        invokeSnapshotFileMetadata(table, Arrays.asList("/file1.parquet"));
    assertNotNull(result);
    // Files that fail to get metadata are skipped
    assertTrue(result.files.isEmpty());
  }

  // ====================================================================
  // Tests for hasFilesChanged (private method)
  // ====================================================================

  @Test
  void testHasFilesChangedWithNullBaseline() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeHasFilesChanged(table, null);
    assertTrue(result, "Should return true for null baseline");
  }

  @Test
  void testHasFilesChangedWithEmptyBaseline() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    boolean result = invokeHasFilesChanged(table, baseline);
    assertTrue(result, "Should return true for empty baseline");
  }

  @Test
  void testHasFilesChangedWithNullStorageProvider() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, null);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/file.parquet", new ConversionMetadata.FileBaseline(100L, "etag", 1000L));

    boolean result = invokeHasFilesChanged(table, baseline);
    assertTrue(result, "Should return true when storageProvider is null");
  }

  // ====================================================================
  // Tests for filesChangedComparedToBaseline (private method)
  // ====================================================================

  @Test
  void testFilesChangedComparedToBaselineNullBaseline() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/file1.parquet"), null);
    assertTrue(result, "Should return true for null baseline");
  }

  @Test
  void testFilesChangedComparedToBaselineEmptyBaseline() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/file1.parquet"), baseline);
    assertTrue(result, "Should return true for empty baseline");
  }

  @Test
  void testFilesChangedComparedToBaselineNullStorageProvider() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, null);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f.parquet", new ConversionMetadata.FileBaseline(100L, "e", 1000L));

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f.parquet"), baseline);
    assertTrue(result, "Should return true when storageProvider is null");
  }

  @Test
  void testFilesChangedComparedToBaselineFileSizeChanged() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    // baseline has 1 file, current has 2 files
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "e1", 1000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f1.parquet", "/f2.parquet"), baseline);
    assertTrue(result, "Should detect file count change");
  }

  @Test
  void testFilesChangedComparedToBaselineNewFileDetected() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "e1", 1000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    // Same size (1 file each) but different file
    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/new_file.parquet"), baseline);
    assertTrue(result, "Should detect new file not in baseline");
  }

  @Test
  void testFilesChangedComparedToBaselineFileModified() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    StorageProvider.FileMetadata fileMeta = mock(StorageProvider.FileMetadata.class);
    when(fileMeta.getSize()).thenReturn(200L);
    when(fileMeta.getEtag()).thenReturn("new_etag");
    when(fileMeta.getLastModified()).thenReturn(5000L);
    when(storageProvider.getMetadata("/f1.parquet")).thenReturn(fileMeta);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "old_etag", 1000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f1.parquet"), baseline);
    assertTrue(result, "Should detect file modification via etag change");
  }

  @Test
  void testFilesChangedComparedToBaselineNoChanges() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    StorageProvider.FileMetadata fileMeta = mock(StorageProvider.FileMetadata.class);
    when(fileMeta.getSize()).thenReturn(100L);
    when(fileMeta.getEtag()).thenReturn("same_etag");
    when(fileMeta.getLastModified()).thenReturn(1000L);
    when(storageProvider.getMetadata("/f1.parquet")).thenReturn(fileMeta);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "same_etag", 1000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f1.parquet"), baseline);
    assertFalse(result, "Should detect no changes when etags match");
  }

  @Test
  void testFilesChangedComparedToBaselineMetadataException() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());
    when(storageProvider.getMetadata(anyString())).thenThrow(new RuntimeException("S3 error"));

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "e", 1000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f1.parquet"), baseline);
    assertTrue(result, "Should assume changed when metadata retrieval fails");
  }

  @Test
  void testFilesChangedComparedToBaselineRemovedFile() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    StorageProvider.FileMetadata fileMeta1 = mock(StorageProvider.FileMetadata.class);
    when(fileMeta1.getSize()).thenReturn(100L);
    when(fileMeta1.getEtag()).thenReturn("same");
    when(fileMeta1.getLastModified()).thenReturn(1000L);
    when(storageProvider.getMetadata("/f1.parquet")).thenReturn(fileMeta1);

    StorageProvider.FileMetadata fileMeta2 = mock(StorageProvider.FileMetadata.class);
    when(fileMeta2.getSize()).thenReturn(200L);
    when(fileMeta2.getEtag()).thenReturn("same2");
    when(fileMeta2.getLastModified()).thenReturn(2000L);
    when(storageProvider.getMetadata("/f2.parquet")).thenReturn(fileMeta2);

    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/f1.parquet", new ConversionMetadata.FileBaseline(100L, "same", 1000L));
    baseline.files.put("/f2.parquet", new ConversionMetadata.FileBaseline(200L, "same2", 2000L));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    // 2 files in baseline, 2 files current, both etags and sizes match
    boolean result = invokeFilesChangedComparedToBaseline(table,
        Arrays.asList("/f1.parquet", "/f2.parquet"), baseline);
    assertFalse(result, "Should detect no changes when all files match");
  }

  // ====================================================================
  // Tests for CommentableTable methods
  // ====================================================================

  @Test
  void testGetTableCommentWithConfig() throws Exception {
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", null,
        "Test table comment", null);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals("Test table comment", table.getTableComment());
  }

  @Test
  void testGetTableCommentWithNullConfig() throws Exception {
    // We need a non-null config because constructor accesses it, but comment is null
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", null);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertNull(table.getTableComment());
  }

  @Test
  void testGetColumnCommentWithMapping() throws Exception {
    Map<String, String> columnComments = new HashMap<>();
    columnComments.put("year", "The fiscal year");
    columnComments.put("amount", "Transaction amount in USD");

    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", null,
        "Table comment", columnComments);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals("The fiscal year", table.getColumnComment("year"));
    assertEquals("Transaction amount in USD", table.getColumnComment("amount"));
    assertNull(table.getColumnComment("nonexistent"));
  }

  @Test
  void testGetColumnCommentWithNullColumnComments() throws Exception {
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test", "**/*.parquet", "partitioned", null,
        "Table comment", null);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertNull(table.getColumnComment("year"));
  }

  // ====================================================================
  // Tests for toString and setRefreshContext
  // ====================================================================

  @Test
  void testToString() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "my_table", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    assertEquals("RefreshablePartitionedParquetTable(my_table)", table.toString());
  }

  @Test
  void testSetRefreshContext() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    FileSchema fileSchema = mock(FileSchema.class);
    table.setRefreshContext(fileSchema, "test_table");

    // Verify internal fields were set via reflection
    Field fileSchemaField = RefreshablePartitionedParquetTable.class.getDeclaredField("fileSchema");
    fileSchemaField.setAccessible(true);
    assertSame(fileSchema, fileSchemaField.get(table));

    Field tableNameField = RefreshablePartitionedParquetTable.class.getDeclaredField("tableNameForNotification");
    tableNameField.setAccessible(true);
    assertEquals("test_table", tableNameField.get(table));
  }

  // ====================================================================
  // Tests for discoverFiles (private method)
  // ====================================================================

  @Test
  void testDiscoverFilesWithNullStorageProvider() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, null);

    List<String> result = invokeDiscoverFiles(table);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testDiscoverFilesWithStorageProviderException() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean()))
        .thenThrow(new RuntimeException("Storage unavailable"));

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        null, null, null, null, storageProvider);

    List<String> result = invokeDiscoverFiles(table);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ====================================================================
  // Tests for constructors
  // ====================================================================

  @Test
  void testThreeArgConstructor() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("LINQ4J", 2048);
    StorageProvider storageProvider = mock(StorageProvider.class);
    when(storageProvider.listFiles(anyString(), anyBoolean())).thenReturn(Collections.emptyList());

    // 6-arg constructor (no constraint, schemaName, storageProvider)
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, Duration.ofMinutes(10));

    assertNotNull(table);
    assertEquals(Duration.ofMinutes(10), table.getRefreshInterval());
  }

  @Test
  void testEightArgConstructor() throws Exception {
    PartitionedTableConfig config = createSimpleConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("LINQ4J", 2048);
    Map<String, Object> constraintConfig = new HashMap<>();
    constraintConfig.put("primaryKey", Arrays.asList("id"));

    // 8-arg constructor
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test", tempDir.toString(), "**/*.parquet", config,
        engineConfig, Duration.ofMinutes(5), constraintConfig, "myschema");

    assertNotNull(table);
    assertEquals("myschema", getPrivateField(table, "schemaName"));
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private PartitionedTableConfig createSimpleConfig() {
    return new PartitionedTableConfig("test", "**/*.parquet", "partitioned", null);
  }

  private PartitionedTableConfig createHivePartitionConfig() {
    List<PartitionedTableConfig.ColumnDefinition> colDefs = new ArrayList<>();
    colDefs.add(new PartitionedTableConfig.ColumnDefinition("year", "INTEGER"));
    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);
    return new PartitionedTableConfig("test", "**/*.parquet", "partitioned", partConfig);
  }

  private boolean invokeShouldSkipInitialScan(RefreshablePartitionedParquetTable table)
      throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    return (Boolean) method.invoke(table);
  }

  private String invokeGetRelativePath(RefreshablePartitionedParquetTable table,
      String basePath, String fullPath) throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);
    return (String) method.invoke(table, basePath, fullPath);
  }

  @SuppressWarnings("unchecked")
  private ConversionMetadata.PartitionBaseline invokeSnapshotFileMetadata(
      RefreshablePartitionedParquetTable table, List<String> filePaths) throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);
    return (ConversionMetadata.PartitionBaseline) method.invoke(table, filePaths);
  }

  private boolean invokeHasFilesChanged(RefreshablePartitionedParquetTable table,
      ConversionMetadata.PartitionBaseline baseline) throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "hasFilesChanged", ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(table, baseline);
  }

  @SuppressWarnings("unchecked")
  private boolean invokeFilesChangedComparedToBaseline(
      RefreshablePartitionedParquetTable table, List<String> currentFiles,
      ConversionMetadata.PartitionBaseline baseline) throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(table, currentFiles, baseline);
  }

  @SuppressWarnings("unchecked")
  private List<String> invokeDiscoverFiles(RefreshablePartitionedParquetTable table)
      throws Exception {
    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod("discoverFiles");
    method.setAccessible(true);
    return (List<String>) method.invoke(table);
  }

  private Object getPrivateField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}
