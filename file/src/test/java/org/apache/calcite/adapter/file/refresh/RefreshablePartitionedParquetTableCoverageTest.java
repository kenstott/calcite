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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Coverage tests for RefreshablePartitionedParquetTable focusing on refresh logic,
 * file discovery, baseline comparison, relative path computation, and lazy initialization.
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

    // Constructor with null storageProvider will just log warning and have empty files
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

    // Need to reset lastRefreshTime to null
    Field lastRefreshField = RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
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

    // Set lastRefreshTime to now
    Field lastRefreshField = RefreshablePartitionedParquetTable.class.getDeclaredField("lastRefreshTime");
    lastRefreshField.setAccessible(true);
    lastRefreshField.set(table, Instant.now());

    assertFalse(table.needsRefresh());
  }

  // ====================================================================
  // Tests for getRefreshInterval
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

  // ====================================================================
  // Tests for getLastRefreshTime
  // ====================================================================

  @Test
  void testGetLastRefreshTime() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    // Initially null
    assertNull(table.getLastRefreshTime());
  }

  // ====================================================================
  // Tests for getRefreshBehavior
  // ====================================================================

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
  void testGetTableCommentNullConfig() throws Exception {
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    // Use reflection to create table with null config
    PartitionedTableConfig config = createMinimalConfig();
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    // Config exists but has no comment
    assertNull(table.getTableComment());
  }

  // ====================================================================
  // Tests for getRelativePath
  // ====================================================================

  @Test
  void testGetRelativePath() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "getRelativePath", String.class, String.class);
    method.setAccessible(true);

    // Normal case
    assertEquals("file.parquet",
        method.invoke(table, "/base/path", "/base/path/file.parquet"));

    // With trailing slash
    assertEquals("file.parquet",
        method.invoke(table, "/base/path/", "/base/path/file.parquet"));

    // Non-subpath - returns filename
    assertEquals("file.parquet",
        method.invoke(table, "/other/path", "/different/path/file.parquet"));

    // No slash in fullPath
    assertEquals("noSlash", method.invoke(table, "/base/path", "noSlash"));

    // S3-style paths
    assertEquals("year=2020/file.parquet",
        method.invoke(table, "s3://bucket/table", "s3://bucket/table/year=2020/file.parquet"));
  }

  // ====================================================================
  // Tests for shouldSkipInitialScan
  // ====================================================================

  @Test
  void testShouldSkipInitialScanNullEngineConfig() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, null, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertFalse((boolean) method.invoke(table));
  }

  @Test
  void testShouldSkipInitialScanNoDuckDB() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod("shouldSkipInitialScan");
    method.setAccessible(true);
    assertFalse((boolean) method.invoke(table));
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

  // ====================================================================
  // Tests for setRefreshContext
  // ====================================================================

  @Test
  void testSetRefreshContext() {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    // Should not throw with null schema
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
  void testSnapshotFileMetadataNullInputs() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "snapshotFileMetadata", List.class);
    method.setAccessible(true);

    // Null file paths
    assertNull(method.invoke(table, (Object) null));

    // Empty file paths
    assertNull(method.invoke(table, Collections.emptyList()));

    // Non-null but no storage provider
    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");
    assertNull(method.invoke(table, files));
  }

  // ====================================================================
  // Tests for hasFilesChanged
  // ====================================================================

  @Test
  void testHasFilesChangedNullBaseline() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "hasFilesChanged", ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    // Null baseline -> must scan
    assertTrue((boolean) method.invoke(table, (Object) null));

    // Empty baseline -> must scan
    ConversionMetadata.PartitionBaseline emptyBaseline = new ConversionMetadata.PartitionBaseline();
    assertTrue((boolean) method.invoke(table, emptyBaseline));
  }

  // ====================================================================
  // Tests for filesChangedComparedToBaseline
  // ====================================================================

  @Test
  void testFilesChangedComparedToBaselineNull() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod(
        "filesChangedComparedToBaseline", List.class, ConversionMetadata.PartitionBaseline.class);
    method.setAccessible(true);

    List<String> files = new ArrayList<String>();
    files.add("/path/file.parquet");

    // Null baseline
    assertTrue((boolean) method.invoke(table, files, null));

    // Empty baseline
    ConversionMetadata.PartitionBaseline emptyBaseline = new ConversionMetadata.PartitionBaseline();
    assertTrue((boolean) method.invoke(table, files, emptyBaseline));
  }

  // ====================================================================
  // Tests for discoverFiles with null storageProvider
  // ====================================================================

  @Test
  void testDiscoverFilesNullStorageProvider() throws Exception {
    PartitionedTableConfig config = createMinimalConfig();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 2048);

    RefreshablePartitionedParquetTable table = new RefreshablePartitionedParquetTable(
        "test_table", tempDir.toString(), "*.parquet", config, engineConfig, null);

    Method method = RefreshablePartitionedParquetTable.class.getDeclaredMethod("discoverFiles");
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(table);
    assertTrue(result.isEmpty());
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private PartitionedTableConfig createMinimalConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    return PartitionedTableConfig.fromMap(configMap);
  }
}
