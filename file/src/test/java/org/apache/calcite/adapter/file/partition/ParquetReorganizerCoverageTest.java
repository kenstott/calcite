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
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link ParquetReorganizer} and its inner
 * {@link ParquetReorganizer.ReorgConfig} builder.
 */
@Tag("unit")
class ParquetReorganizerCoverageTest {

  @TempDir
  File tempDir;

  private StorageProvider mockStorageProvider;

  @BeforeEach void setUp() {
    mockStorageProvider = new StorageProvider() {
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }

      @Override public StorageProvider.FileMetadata getMetadata(String path) {
        return new StorageProvider.FileMetadata(path, 0, 0, null, null);
      }

      @Override public java.io.InputStream openInputStream(String path) {
        return new java.io.ByteArrayInputStream(new byte[0]);
      }

      @Override public java.io.Reader openReader(String path) {
        return new java.io.StringReader("");
      }

      @Override public boolean exists(String path) {
        return false;
      }

      @Override public boolean isDirectory(String path) {
        return false;
      }

      @Override public String getStorageType() {
        return "mock";
      }

      @Override public String resolvePath(String basePath, String relativePath) {
        return basePath + "/" + relativePath;
      }

      @Override public Map<String, String> getS3Config() {
        return null;
      }
    };
  }

  // ===== ReorgConfig Builder Tests =====

  @Test void testReorgConfigBuilderMinimal() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/*.parquet")
        .targetBase("type=income_consolidated")
        .build();

    assertEquals("type=income/year=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_consolidated", config.getTargetBase());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertTrue(config.getColumnMappings().isEmpty());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertEquals(0, config.getStartYear());
    assertEquals(0, config.getEndYear());
    assertEquals(2, config.getThreads()); // default
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertFalse(config.supportsIncremental());
    assertFalse(config.isSourceIsIceberg());
  }

  @Test void testReorgConfigBuilderFull() {
    IncrementalTracker tracker = new IncrementalTracker() {
      @Override public boolean isProcessed(String a, String s, Map<String, String> k) {
        return false;
      }

      @Override public boolean isProcessedWithTtl(String a, String s,
          Map<String, String> k, long t) {
        return false;
      }

      @Override public void markProcessed(String a, String s,
          Map<String, String> k, String t) {
      }

      @Override public java.util.Set<Map<String, String>> getProcessedKeyValues(String a) {
        return Collections.emptySet();
      }

      @Override public void invalidate(String a, Map<String, String> k) {
      }

      @Override public void invalidateAll(String a) {
      }

      @Override public java.util.Set<Integer> filterUnprocessed(String a, String s,
          List<Map<String, String>> c) {
        return Collections.emptySet();
      }

      @Override public boolean isTableComplete(String p, String d) {
        return false;
      }

      @Override public void markTableComplete(String p, String d) {
      }

      @Override public void invalidateTableCompletion(String p) {
      }

      @Override public void clearAllCompletions() {
      }
    };

    Map<String, String> mappings = new LinkedHashMap<String, String>();
    mappings.put("geo", "GeoFips");

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/geo=*/*.parquet")
        .targetBase("type=income_by_geo")
        .partitionColumns(Arrays.asList("geo"))
        .columnMappings(mappings)
        .batchPartitionColumns(Arrays.asList("year", "geo"))
        .yearRange(2020, 2024)
        .name("income_reorganization")
        .threads(4)
        .incrementalKeys(Arrays.asList("year"))
        .incrementalTracker(tracker)
        .sourceTable("regional_income")
        .sourceIsIceberg(true)
        .icebergWarehousePath("/warehouse/path")
        .currentYearTtlDays(7)
        .build();

    assertEquals("type=income/year=*/geo=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_by_geo", config.getTargetBase());
    assertEquals(Arrays.asList("geo"), config.getPartitionColumns());
    assertEquals(mappings, config.getColumnMappings());
    assertEquals(Arrays.asList("year", "geo"), config.getBatchPartitionColumns());
    assertEquals(2020, config.getStartYear());
    assertEquals(2024, config.getEndYear());
    assertEquals("income_reorganization", config.getName());
    assertEquals(4, config.getThreads());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertNotNull(config.getIncrementalTracker());
    assertTrue(config.supportsIncremental());
    assertEquals("regional_income", config.getSourceTable());
    assertTrue(config.isSourceIsIceberg());
    assertEquals("/warehouse/path", config.getIcebergWarehousePath());
    assertEquals(7, config.getCurrentYearTtlDays());
    assertEquals(7 * 24L * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
  }

  @Test void testReorgConfigMissingSourcePattern() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .targetBase("target")
            .build());
    assertTrue(ex.getMessage().contains("sourcePattern"));
  }

  @Test void testReorgConfigEmptySourcePattern() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("")
            .targetBase("target")
            .build());
    assertTrue(ex.getMessage().contains("sourcePattern"));
  }

  @Test void testReorgConfigMissingTargetBase() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("*.parquet")
            .build());
    assertTrue(ex.getMessage().contains("targetBase"));
  }

  @Test void testReorgConfigEmptyTargetBase() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("*.parquet")
            .targetBase("")
            .build());
    assertTrue(ex.getMessage().contains("targetBase"));
  }

  @Test void testReorgConfigDefaultThreads() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .threads(0)
        .build();
    assertEquals(2, config.getThreads()); // default when 0
  }

  @Test void testReorgConfigDefaultCurrentYearTtlDays() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .currentYearTtlDays(0)
        .build();
    assertEquals(1, config.getCurrentYearTtlDays()); // default when 0
  }

  @Test void testReorgConfigCurrentYearTtlMillis() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .currentYearTtlDays(3)
        .build();
    assertEquals(3 * 24L * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
  }

  @Test void testReorgConfigNoIncremental() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .build();
    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigNoopIncrementalTracker() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .incrementalKeys(Arrays.asList("year"))
        .build();
    // With NOOP tracker, supportsIncremental should be false
    assertFalse(config.supportsIncremental());
  }

  // ===== Factory Method Tests =====

  @Test void testCreateFactory() {
    ParquetReorganizer reorganizer =
        ParquetReorganizer.create(mockStorageProvider, tempDir.getAbsolutePath());
    assertNotNull(reorganizer);
  }

  @Test void testConstructor() {
    ParquetReorganizer reorganizer =
        new ParquetReorganizer(mockStorageProvider, tempDir.getAbsolutePath());
    assertNotNull(reorganizer);
  }

  // ===== ReorgConfig null defaults tests =====

  @Test void testReorgConfigNullPartitionColumnsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .partitionColumns(null)
        .build();
    assertNotNull(config.getPartitionColumns());
    assertTrue(config.getPartitionColumns().isEmpty());
  }

  @Test void testReorgConfigNullColumnMappingsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .columnMappings(null)
        .build();
    assertNotNull(config.getColumnMappings());
    assertTrue(config.getColumnMappings().isEmpty());
  }

  @Test void testReorgConfigNullBatchColumnsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .batchPartitionColumns(null)
        .build();
    assertNotNull(config.getBatchPartitionColumns());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
  }

  @Test void testReorgConfigNullIncrementalKeysDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .incrementalKeys(null)
        .build();
    assertNotNull(config.getIncrementalKeys());
    assertTrue(config.getIncrementalKeys().isEmpty());
  }

  @Test void testReorgConfigNullIncrementalTrackerDefaultsToNoop() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .incrementalTracker(null)
        .build();
    assertNotNull(config.getIncrementalTracker());
    assertEquals(IncrementalTracker.NOOP, config.getIncrementalTracker());
  }
}
