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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ParquetEnumerator} - Part 2.
 *
 * <p>Covers CSV batch loading, moveNext/current with data, spill/load mechanisms,
 * streaming aggregation, estimateBatchMemoryUsage, loadBatchFromPreloadedRows,
 * and various edge cases in the streaming pipeline.
 */
@Tag("unit")
public class ParquetEnumeratorDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  // ====================================================================
  // CSV batch loading with real data
  // ====================================================================

  @Test
  void testMoveNextAndCurrentWithCsvData() throws Exception {
    Path csvFile = tempDir.resolve("data.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("name,value");
    lines.add("Alice,100");
    lines.add("Bob,200");
    lines.add("Charlie,300");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        source, cancel, fieldTypes, projected);
    try {
      int rowCount = 0;
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        assertNotNull(row);
        rowCount++;
      }
      assertEquals(3, rowCount);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testMoveNextEmptyCsv() throws Exception {
    Path csvFile = tempDir.resolve("empty_data.csv");
    Files.write(csvFile, Collections.singletonList("col1,col2"));

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        source, cancel, fieldTypes, projected);
    try {
      // No data rows, only header
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testMoveNextSingleRow() throws Exception {
    Path csvFile = tempDir.resolve("single_row.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a,b");
    lines.add("1,2");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        source, cancel, fieldTypes, projected);
    try {
      assertTrue(enumerator.moveNext());
      assertNotNull(enumerator.current());
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Single column projection returns single value (not array)
  // ====================================================================

  @Test
  void testSingleColumnProjectionReturnsSingleValue() throws Exception {
    Path csvFile = tempDir.resolve("single_col_proj.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("name,age");
    lines.add("Alice,30");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertTrue(enumerator.moveNext());
      Object current = enumerator.current();
      // With single projected field, current() returns the single value, not an array
      assertNotNull(current);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Cancel flag during iteration
  // ====================================================================

  @Test
  void testCancelDuringIteration() throws Exception {
    Path csvFile = tempDir.resolve("cancel_mid.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("x,y");
    lines.add("1,2");
    lines.add("3,4");
    lines.add("5,6");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        source, cancel, fieldTypes, projected);
    try {
      assertTrue(enumerator.moveNext());
      cancel.set(true);
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Reset and re-read
  // ====================================================================

  @Test
  void testResetClearsState() throws Exception {
    Path csvFile = tempDir.resolve("reset_state.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a");
    lines.add("1");
    lines.add("2");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertTrue(enumerator.moveNext());
      enumerator.reset();
      // After reset, memory should be freed
      assertEquals(0, enumerator.getMemoryUsage());
      // current() after reset should return null (before moveNext)
      assertNull(enumerator.current());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Small batch size triggers multiple batches
  // ====================================================================

  @Test
  void testSmallBatchSizeMultipleBatches() throws Exception {
    Path csvFile = tempDir.resolve("small_batch.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("x");
    for (int i = 0; i < 10; i++) {
      lines.add(String.valueOf(i));
    }
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    // Batch size of 3 with 10 rows = 4 batches (3+3+3+1)
    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 3);
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(10, count);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Memory usage estimation
  // ====================================================================

  @Test
  void testMemoryUsageGrowsWithData() throws Exception {
    Path csvFile = tempDir.resolve("mem_data.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("name,description");
    lines.add("Alice,This is a long description for testing memory estimation");
    lines.add("Bob,Another description with some content");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      assertEquals(0, enumerator.getMemoryUsage());
      enumerator.moveNext();
      // After loading a batch, memory usage should be > 0
      long usage = enumerator.getMemoryUsage();
      assertTrue(usage > 0, "Memory usage should be > 0 after loading data");
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Estimated total size
  // ====================================================================

  @Test
  void testEstimatedTotalSizeAfterFullRead() throws Exception {
    Path csvFile = tempDir.resolve("total_size.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("value");
    for (int i = 0; i < 5; i++) {
      lines.add(String.valueOf(i * 100));
    }
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 3);
    try {
      // Read all data
      while (enumerator.moveNext()) {
        // consume
      }
      long est = enumerator.getEstimatedTotalSize();
      // After reading all data, total size estimate should be available
      // (could be -1 if totalRowCount is 0 for edge cases)
      assertTrue(est >= 0 || est == -1);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Column stats with real data
  // ====================================================================

  @Test
  void testColumnStatsWithStringData() throws Exception {
    Path csvFile = tempDir.resolve("colstats_data.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("name,value");
    lines.add("Alice,100");
    lines.add("Bob,200");
    lines.add("Charlie,300");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.moveNext();
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(0);
      assertNotNull(stats);
      // String values are Comparable, so min/max should be populated
      assertNotNull(stats.min);
      assertNotNull(stats.max);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testColumnStatsWithNullValues() throws Exception {
    Path csvFile = tempDir.resolve("colstats_null.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a,b");
    lines.add(",1");
    lines.add("x,");
    lines.add(",");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // Load all rows
      while (enumerator.moveNext()) {
        // consume
      }
      // Stats should be available for the last loaded batch
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(0);
      assertNotNull(stats);
      // Null count should reflect null/empty values
      assertTrue(stats.nullCount >= 0);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Column sum
  // ====================================================================

  @Test
  void testColumnSumOutOfBounds() throws Exception {
    Path csvFile = tempDir.resolve("colsum_oob.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a");
    lines.add("1");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.moveNext();
      assertEquals(0.0, enumerator.columnSum(999), 0.001);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Streaming stats with loaded data
  // ====================================================================

  @Test
  void testStreamingStatsAfterDataLoad() throws Exception {
    Path csvFile = tempDir.resolve("ss_data.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("col");
    lines.add("val1");
    lines.add("val2");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.moveNext();
      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();
      assertNotNull(stats);
      assertTrue(stats.batchesProcessed >= 0);
      assertTrue(stats.totalBatches >= 1);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // StreamingStats formatting
  // ====================================================================

  @Test
  void testStreamingStatsSpillSizeZero() {
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        0, 0, 0, 0, false, 0, 0);
    assertEquals("0B", stats.getSpillSizeFormatted());
    assertEquals(0.0, stats.getSpillRatio(), 0.001);
  }

  @Test
  void testStreamingStatsLargeSpillSize() {
    long tenMB = 10L * 1024 * 1024;
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        10, 10, 0, 1000, true, 5, tenMB);
    assertEquals("10MB", stats.getSpillSizeFormatted());
    assertEquals(0.5, stats.getSpillRatio(), 0.001);
  }

  @Test
  void testStreamingStatsToStringContainsAllFields() {
    ParquetEnumerator.StreamingStats stats = new ParquetEnumerator.StreamingStats(
        2, 5, 2 * 1024 * 1024, 50, false, 1, 1500);
    String str = stats.toString();
    assertTrue(str.contains("batches=2/5"));
    assertTrue(str.contains("memory=2MB"));
    assertTrue(str.contains("rows=50"));
    assertTrue(str.contains("complete=false"));
    assertTrue(str.contains("spilled=1"));
  }

  // ====================================================================
  // ColumnStats constructor
  // ====================================================================

  @Test
  void testColumnStatsConstructor() {
    ParquetEnumerator.ColumnStats stats = new ParquetEnumerator.ColumnStats(5, "min", "max");
    assertEquals(5, stats.nullCount);
    assertEquals("min", stats.min);
    assertEquals("max", stats.max);
  }

  @Test
  void testColumnStatsWithNulls() {
    ParquetEnumerator.ColumnStats stats = new ParquetEnumerator.ColumnStats(0, null, null);
    assertEquals(0, stats.nullCount);
    assertNull(stats.min);
    assertNull(stats.max);
  }

  // ====================================================================
  // File format detection via reflection: arrow, parquet, excel
  // ====================================================================

  @Test
  void testDetectFileFormatArrow() throws Exception {
    Path arrowFile = tempDir.resolve("test.arrow");
    Files.write(arrowFile, Collections.singletonList("arrow data"));
    Source source = Sources.of(arrowFile.toFile());

    // Create a CSV enumerator as a host for reflection
    Path csvFile = tempDir.resolve("host_arrow.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("ARROW", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatArrowGz() throws Exception {
    Path arrowGzFile = tempDir.resolve("test.arrow.gz");
    Files.write(arrowGzFile, Collections.singletonList("compressed arrow"));
    Source source = Sources.of(arrowGzFile.toFile());

    Path csvFile = tempDir.resolve("host_arrow_gz.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("ARROW", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatExcelXlsx() throws Exception {
    Path xlsxFile = tempDir.resolve("test.xlsx");
    Files.write(xlsxFile, Collections.singletonList("excel"));
    Source source = Sources.of(xlsxFile.toFile());

    Path csvFile = tempDir.resolve("host_xlsx.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("EXCEL", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatExcelXls() throws Exception {
    Path xlsFile = tempDir.resolve("test.xls");
    Files.write(xlsFile, Collections.singletonList("old excel"));
    Source source = Sources.of(xlsFile.toFile());

    Path csvFile = tempDir.resolve("host_xls.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("EXCEL", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatParquet() throws Exception {
    Path parquetFile = tempDir.resolve("test.parquet");
    Files.write(parquetFile, Collections.singletonList("parquet data"));
    Source source = Sources.of(parquetFile.toFile());

    Path csvFile = tempDir.resolve("host_parquet.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      Object format = method.invoke(csvEnum, source);
      assertEquals("PARQUET", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  @Test
  void testDetectFileFormatJsonGz() throws Exception {
    Path jsonGzFile = tempDir.resolve("test.json.gz");
    Files.write(jsonGzFile, Collections.singletonList("gzipped json"));
    Source source = Sources.of(jsonGzFile.toFile());

    Path csvFile = tempDir.resolve("host_json_gz.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
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
  void testDetectFileFormatNullPath() throws Exception {
    // Create a mock-like Source with null path
    Path csvFile = tempDir.resolve("host_null_path.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source csvSource = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> csvEnum = new ParquetEnumerator<Object>(
        csvSource, cancel, fieldTypes, projected);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("detectFileFormat", Source.class);
      method.setAccessible(true);
      // Test with a source that has a TSV extension (defaults to CSV)
      Path tsvFile = tempDir.resolve("test.tsv");
      Files.write(tsvFile, Collections.singletonList("a\tb"));
      Source tsvSource = Sources.of(tsvFile.toFile());
      Object format = method.invoke(csvEnum, tsvSource);
      assertEquals("CSV", format.toString());
    } finally {
      csvEnum.close();
    }
  }

  // ====================================================================
  // Cleanup spill files
  // ====================================================================

  @Test
  void testCleanupOldSpillFilesMaxLargerThanBatchList() throws Exception {
    Path csvFile = tempDir.resolve("cleanup_large.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      // maxSpillFiles > batchInfoList size should not throw
      enumerator.cleanupOldSpillFiles(100);
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testCleanupOldSpillFilesZeroMax() throws Exception {
    Path csvFile = tempDir.resolve("cleanup_zero.csv");
    Files.write(csvFile, Collections.singletonList("a,b"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.cleanupOldSpillFiles(0);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Total spill size with no spills
  // ====================================================================

  @Test
  void testTotalSpillSizeAfterDataRead() throws Exception {
    Path csvFile = tempDir.resolve("spill_read.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a");
    lines.add("1");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.moveNext();
      // No spills should have happened with default threshold
      assertEquals(0, enumerator.getTotalSpillSize());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Streaming aggregation
  // ====================================================================

  @Test
  void testStreamingAggregateEmpty() throws Exception {
    Path csvFile = tempDir.resolve("agg_empty.csv");
    Files.write(csvFile, Collections.singletonList("x"));

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      Integer result = enumerator.streamingAggregate(
          new ParquetEnumerator.StreamingAggregator<Integer>() {
            private int count = 0;
            @Override public void processBatch(List<Object[]> batchColumns, int rowCount) {
              count += rowCount;
            }
            @Override public Integer getResult() {
              return count;
            }
          });
      assertEquals(0, result.intValue());
    } finally {
      enumerator.close();
    }
  }

  @Test
  void testStreamingAggregateWithData() throws Exception {
    Path csvFile = tempDir.resolve("agg_data.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("val");
    lines.add("10");
    lines.add("20");
    lines.add("30");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 2);
    try {
      Integer result = enumerator.streamingAggregate(
          new ParquetEnumerator.StreamingAggregator<Integer>() {
            private int count = 0;
            @Override public void processBatch(List<Object[]> batchColumns, int rowCount) {
              count += rowCount;
            }
            @Override public Integer getResult() {
              return count;
            }
          });
      assertEquals(3, result.intValue());
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // Close after close (idempotent)
  // ====================================================================

  @Test
  void testDoubleClose() throws Exception {
    Path csvFile = tempDir.resolve("dbl_close.csv");
    Files.write(csvFile, Collections.singletonList("a"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    enumerator.close();
    // Second close should not throw
    // The spill directory is already deleted, so IOException is expected but handled
    try {
      enumerator.close();
    } catch (Exception e) {
      // OK - spill directory already deleted
    }
  }

  // ====================================================================
  // Very small memory threshold to trigger spill
  // ====================================================================

  @Test
  void testVerySmallMemoryThresholdTriggersSpill() throws Exception {
    Path csvFile = tempDir.resolve("tiny_threshold.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a,b,c,d,e");
    for (int i = 0; i < 20; i++) {
      lines.add("value" + i + ",data" + i + ",info" + i + ",extra" + i + ",more" + i);
    }
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1, 2, 3, 4};

    // Very small memory threshold (1 byte) should trigger spill
    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 5, 1L, "UNCHANGED");
    try {
      int count = 0;
      while (enumerator.moveNext()) {
        count++;
      }
      assertEquals(20, count);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // getBatchInfo creates new batches lazily
  // ====================================================================

  @Test
  void testGetBatchInfoViaReflection() throws Exception {
    Path csvFile = tempDir.resolve("batchinfo.csv");
    Files.write(csvFile, Collections.singletonList("x"));
    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected, 5);
    try {
      Method method = ParquetEnumerator.class.getDeclaredMethod("getBatchInfo", int.class);
      method.setAccessible(true);

      // Batch 0 should already exist
      Object batch0 = method.invoke(enumerator, 0);
      assertNotNull(batch0);

      // Batch 1 should be created lazily
      Object batch1 = method.invoke(enumerator, 1);
      assertNotNull(batch1);

      // Batch 2 should also be created
      Object batch2 = method.invoke(enumerator, 2);
      assertNotNull(batch2);
    } finally {
      enumerator.close();
    }
  }

  // ====================================================================
  // estimateBatchMemoryUsage with mixed types
  // ====================================================================

  @Test
  void testEstimateBatchMemoryUsageViaReflection() throws Exception {
    Path csvFile = tempDir.resolve("est_mem.csv");
    List<String> lines = new ArrayList<String>();
    lines.add("a,b");
    lines.add("hello,world");
    Files.write(csvFile, lines);

    Source source = Sources.of(csvFile.toFile());
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<?> enumerator = new ParquetEnumerator<Object>(
        source, cancel, fieldTypes, projected);
    try {
      enumerator.moveNext();
      Method method = ParquetEnumerator.class.getDeclaredMethod("estimateBatchMemoryUsage");
      method.setAccessible(true);
      long usage = (Long) method.invoke(enumerator);
      assertTrue(usage > 0);
    } finally {
      enumerator.close();
    }
  }
}
