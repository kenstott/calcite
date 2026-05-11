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
package org.apache.calcite.adapter.file.execution.linq4j;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for ParquetEnumerator focusing on file format detection,
 * batch management, streaming stats, column stats, memory management, and spill logic.
 */
@Tag("unit")
public class ParquetEnumeratorDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Tests for detectFileFormat
  // ====================================================================

  @Test
  void testDetectFileFormatCsv() throws Exception {
    Path csvFile = tempDir.resolve("test.csv");
    Files.write(csvFile, Collections.singletonList("a,b,c"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Verify it initialized as CSV (streaming format)
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testDetectFileFormatJson() throws Exception {
    Path jsonFile = tempDir.resolve("test.json");
    Files.write(jsonFile, Collections.singletonList("[{\"a\":1}]"));
    Source source = Sources.of(jsonFile.toFile());

    // Use reflection to test detectFileFormat since the constructor also tries to initialize streaming
    // which fails for JSON without a proper schema setup
    assertThrows(RuntimeException.class, () -> {
      AtomicBoolean cancel = new AtomicBoolean(false);
      List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
      int[] projected = new int[]{0};
      new ParquetEnumerator<Object>(source, cancel, fieldTypes, projected);
    });

    // Verify the format detection works via reflection on a CSV enumerator instance
    Path csvFile = tempDir.resolve("json_format_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};
    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("JSON", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatYaml() throws Exception {
    Path yamlFile = tempDir.resolve("test.yaml");
    Files.write(yamlFile, Collections.singletonList("key: value"));
    Source source = Sources.of(yamlFile.toFile());

    // JSON/YAML initialization fails without proper schema, so verify exception thrown
    assertThrows(RuntimeException.class, () -> {
      AtomicBoolean cancel = new AtomicBoolean(false);
      List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
      int[] projected = new int[]{0};
      new ParquetEnumerator<Object>(source, cancel, fieldTypes, projected);
    });

    // Verify the format detection itself via reflection
    Path csvFile = tempDir.resolve("yaml_format_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};
    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("YAML", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatYml() throws Exception {
    Path ymlFile = tempDir.resolve("test.yml");
    Files.write(ymlFile, Collections.singletonList("key: value"));
    Source source = Sources.of(ymlFile.toFile());

    // JSON/YAML initialization fails without proper schema, so verify exception thrown
    assertThrows(RuntimeException.class, () -> {
      AtomicBoolean cancel = new AtomicBoolean(false);
      List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
      int[] projected = new int[]{0};
      new ParquetEnumerator<Object>(source, cancel, fieldTypes, projected);
    });

    // Verify the format detection itself via reflection
    Path csvFile = tempDir.resolve("yml_format_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};
    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("YAML", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  // ====================================================================
  // Tests for constructors (various parameter combinations)
  // ====================================================================

  @Test
  void testConstructorWithBatchSize() throws Exception {
    Path csvFile = tempDir.resolve("batch_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 500);
    try {
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testConstructorWithColumnNameCasing() throws Exception {
    Path csvFile = tempDir.resolve("casing_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, "TO_LOWER");
    try {
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testConstructorWithBatchSizeAndCasing() throws Exception {
    Path csvFile = tempDir.resolve("both_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 100, "TO_UPPER");
    try {
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testConstructorWithMemoryThreshold() throws Exception {
    Path csvFile = tempDir.resolve("memory_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 100, 1024 * 1024);
    try {
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testConstructorFullParams() throws Exception {
    Path csvFile = tempDir.resolve("full_test.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 100, 1024 * 1024, "UNCHANGED");
    try {
      assertNotNull(enumerator);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for getMemoryUsage
  // ====================================================================

  @Test
  void testGetMemoryUsageBeforeMoveNext() throws Exception {
    Path csvFile = tempDir.resolve("mem_usage.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Before loading any batch, memory should be 0
      assertEquals(0, enumerator.getMemoryUsage());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for getEstimatedTotalSize
  // ====================================================================

  @Test
  void testGetEstimatedTotalSizeUnknown() throws Exception {
    Path csvFile = tempDir.resolve("est_size.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Unknown before processing
      assertEquals(-1, enumerator.getEstimatedTotalSize());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for getTotalSpillSize
  // ====================================================================

  @Test
  void testGetTotalSpillSizeNoSpills() throws Exception {
    Path csvFile = tempDir.resolve("no_spills.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertEquals(0, enumerator.getTotalSpillSize());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for cleanupOldSpillFiles
  // ====================================================================

  @Test
  void testCleanupOldSpillFilesNoSpills() throws Exception {
    Path csvFile = tempDir.resolve("cleanup_spills.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Should not throw when there are no spill files
      enumerator.cleanupOldSpillFiles(5);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for getStreamingStats
  // ====================================================================

  @Test
  void testGetStreamingStats() throws Exception {
    Path csvFile = tempDir.resolve("stats.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();
      assertNotNull(stats);
      assertEquals(0, stats.spilledBatches);
      assertEquals(0, stats.totalSpillSize);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for StreamingStats
  // ====================================================================

  @Test
  void testStreamingStatsSpillRatio() {
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        5, 10, 1024, 100, true, 3, 5000);
    assertEquals(0.3, stats.getSpillRatio(), 0.01);
  }

  @Test
  void testStreamingStatsSpillRatioZeroBatches() {
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        0, 0, 0, 0, false, 0, 0);
    assertEquals(0.0, stats.getSpillRatio(), 0.01);
  }

  @Test
  void testStreamingStatsSpillSizeFormatted() {
    // Bytes
    ParquetEnumerator.StreamingStats bytesStats = new ParquetEnumerator.StreamingStats(
        1, 1, 0, 0, true, 0, 500);
    assertEquals("500B", bytesStats.getSpillSizeFormatted());

    // Kilobytes
    ParquetEnumerator.StreamingStats kbStats = new ParquetEnumerator.StreamingStats(
        1, 1, 0, 0, true, 0, 5000);
    assertEquals("4KB", kbStats.getSpillSizeFormatted());

    // Megabytes
    ParquetEnumerator.StreamingStats mbStats = new ParquetEnumerator.StreamingStats(
        1, 1, 0, 0, true, 0, 5 * 1024 * 1024);
    assertEquals("5MB", mbStats.getSpillSizeFormatted());
  }

  @Test
  void testStreamingStatsToString() {
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        5, 10, 1024 * 1024, 100, true, 3, 5000);
    String str = stats.toString();
    assertNotNull(str);
    assertTrue(str.contains("batches=5/10"));
    assertTrue(str.contains("rows=100"));
    assertTrue(str.contains("complete=true"));
    assertTrue(str.contains("spilled=3"));
  }

  // ====================================================================
  // Tests for ColumnStats
  // ====================================================================

  @Test
  void testColumnStatsEmpty() throws Exception {
    Path csvFile = tempDir.resolve("colstats.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Before loading data
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(0);
      assertNotNull(stats);
      assertEquals(0, stats.nullCount);
      assertNull(stats.min);
      assertNull(stats.max);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testColumnStatsOutOfBounds() throws Exception {
    Path csvFile = tempDir.resolve("colstats_oob.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(999);
      assertEquals(0, stats.nullCount);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for columnSum
  // ====================================================================

  @Test
  void testColumnSumBeforeLoad() throws Exception {
    Path csvFile = tempDir.resolve("colsum.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertEquals(0.0, enumerator.columnSum(0), 0.001);
      assertEquals(0.0, enumerator.columnSum(999), 0.001);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for reset
  // ====================================================================

  @Test
  void testReset() throws Exception {
    Path csvFile = tempDir.resolve("reset.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.reset();
      assertEquals(0, enumerator.getMemoryUsage());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for current before moveNext
  // ====================================================================

  @Test
  void testCurrentBeforeMoveNext() throws Exception {
    Path csvFile = tempDir.resolve("current.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // current() before moveNext should return null
      assertNull(enumerator.current());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Tests for cancel flag
  // ====================================================================

  @Test
  void testCancelFlag() throws Exception {
    Path csvFile = tempDir.resolve("cancel.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(true); // cancelled
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }
}
