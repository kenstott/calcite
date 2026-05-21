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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive coverage tests for {@link ParquetEnumerator}.
 *
 * <p>Tests file format detection, batch management, streaming stats,
 * column stats, memory estimation, spill logic, and CSV iteration.
 */
@Tag("unit")
class ParquetEnumeratorCoverageTest {

  @TempDir
  Path tempDir;

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Create a CSV Source backed by a real temp file with the given content. */
  private Source createCsvSource(String fileName, String content)
      throws IOException {
    File csvFile = tempDir.resolve(fileName).toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write(content);
    }
    return Sources.of(csvFile);
  }

  /** Shorthand that uses a default file name. */
  private Source createCsvSource(String content) throws IOException {
    return createCsvSource("test_" + System.nanoTime() + ".csv", content);
  }

  private List<RelDataType> createVarcharTypes(int count) {
    SqlTypeFactoryImpl tf =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    List<RelDataType> types = new ArrayList<RelDataType>();
    for (int i = 0; i < count; i++) {
      types.add(tf.createSqlType(SqlTypeName.VARCHAR));
    }
    return types;
  }

  /** Create an enumerator from a single-column CSV source. */
  private ParquetEnumerator<Object> createSingleColumnEnumerator(
      Source source) {
    return new ParquetEnumerator<Object>(
        source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0});
  }

  /** Create an enumerator from a two-column CSV source. */
  private ParquetEnumerator<Object[]> createTwoColumnEnumerator(
      Source source) {
    return new ParquetEnumerator<Object[]>(
        source, new AtomicBoolean(false),
        createVarcharTypes(2), new int[]{0, 1});
  }

  /** Invoke the private detectFileFormat via reflection on a helper enumerator. */
  private Object invokeDetectFileFormat(Source targetSource) throws Exception {
    // Build a minimal CSV enumerator so we have an instance to call the method on
    Source helper = createCsvSource("helper_detect.csv", "a\n1\n");
    ParquetEnumerator<?> helperEnum = createSingleColumnEnumerator(helper);
    try {
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("detectFileFormat", Source.class);
      m.setAccessible(true);
      return m.invoke(helperEnum, targetSource);
    } finally {
      helperEnum.close();
    }
  }

  /** Set a private field on a ParquetEnumerator via reflection. */
  private void setField(ParquetEnumerator<?> enumerator,
      String fieldName, Object value) throws Exception {
    Field f = ParquetEnumerator.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    f.set(enumerator, value);
  }

  /** Get a private field from a ParquetEnumerator via reflection. */
  private Object getField(ParquetEnumerator<?> enumerator,
      String fieldName) throws Exception {
    Field f = ParquetEnumerator.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    return f.get(enumerator);
  }

  // =========================================================================
  // detectFileFormat - mock Source with various paths
  // =========================================================================

  @Test void testDetectFileFormat_csv() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/sales.csv");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("CSV", fmt.toString());
  }

  @Test void testDetectFileFormat_tsv() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/sales.tsv");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("CSV", fmt.toString()); // TSV falls through to CSV default
  }

  @Test void testDetectFileFormat_json() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/report.json");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("JSON", fmt.toString());
  }

  @Test void testDetectFileFormat_jsonGz() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/report.json.gz");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("JSON", fmt.toString());
  }

  @Test void testDetectFileFormat_yaml() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/config.yaml");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("YAML", fmt.toString());
  }

  @Test void testDetectFileFormat_yml() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/config.yml");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("YAML", fmt.toString());
  }

  @Test void testDetectFileFormat_arrow() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/table.arrow");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("ARROW", fmt.toString());
  }

  @Test void testDetectFileFormat_arrowGz() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/table.arrow.gz");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("ARROW", fmt.toString());
  }

  @Test void testDetectFileFormat_parquet() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/warehouse.parquet");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("PARQUET", fmt.toString());
  }

  @Test void testDetectFileFormat_xlsx() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/sheet.xlsx");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("EXCEL", fmt.toString());
  }

  @Test void testDetectFileFormat_xls() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/legacy.xls");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("EXCEL", fmt.toString());
  }

  @Test void testDetectFileFormat_nullPath() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn(null);
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("CSV", fmt.toString()); // null path defaults to CSV
  }

  @Test void testDetectFileFormat_unknownExtension() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/file.dat");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("CSV", fmt.toString()); // unknown defaults to CSV
  }

  @Test void testDetectFileFormat_noExtension() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/myfile");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("CSV", fmt.toString()); // no extension defaults to CSV
  }

  @Test void testDetectFileFormat_caseInsensitive() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/FILE.JSON");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("JSON", fmt.toString());
  }

  @Test void testDetectFileFormat_mixedCase() throws Exception {
    Source src = mock(Source.class);
    when(src.path()).thenReturn("/data/Report.Parquet");
    Object fmt = invokeDetectFileFormat(src);
    assertEquals("PARQUET", fmt.toString());
  }

  // =========================================================================
  // estimateBatchMemoryUsage - test with various column data types
  // =========================================================================

  @Test void testEstimateBatchMemoryUsage_emptyColumns() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      // Set currentBatchColumns to an empty list
      setField(e, "currentBatchColumns", new ArrayList<Object[]>());
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      assertEquals(0L, usage);
    } finally {
      e.close();
    }
  }

  @Test void testEstimateBatchMemoryUsage_nullValues() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{null, null, null});
      setField(e, "currentBatchColumns", columns);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      // 3 refs * 8 bytes each = 24 bytes (nulls add no extra overhead)
      assertEquals(24L, usage);
    } finally {
      e.close();
    }
  }

  @Test void testEstimateBatchMemoryUsage_stringValues() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{"hello", "world"});
      setField(e, "currentBatchColumns", columns);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      // 2 refs * 8 = 16 + "hello"=5*2=10 + "world"=5*2=10 = 36
      assertEquals(36L, usage);
    } finally {
      e.close();
    }
  }

  @Test void testEstimateBatchMemoryUsage_numericValues() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{Integer.valueOf(42), Double.valueOf(3.14)});
      setField(e, "currentBatchColumns", columns);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      // 2 refs * 8 = 16 + 24 (Integer) + 24 (Double) = 64
      assertEquals(64L, usage);
    } finally {
      e.close();
    }
  }

  @Test void testEstimateBatchMemoryUsage_mixedValues() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{"ab", null, Integer.valueOf(7)});
      setField(e, "currentBatchColumns", columns);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      // 3 refs * 8 = 24 + "ab"=2*2=4 + null=0 + Integer=24 = 52
      assertEquals(52L, usage);
    } finally {
      e.close();
    }
  }

  @Test void testEstimateBatchMemoryUsage_multipleColumns() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{"x"});          // 1*8 + 1*2 = 10
      columns.add(new Object[]{Integer.valueOf(1)}); // 1*8 + 24 = 32
      setField(e, "currentBatchColumns", columns);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("estimateBatchMemoryUsage");
      m.setAccessible(true);
      long usage = (Long) m.invoke(e);
      assertEquals(42L, usage);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // getBatchInfo - test on-demand batch creation
  // =========================================================================

  @Test void testGetBatchInfo_firstBatch() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("getBatchInfo", int.class);
      m.setAccessible(true);
      Object batch0 = m.invoke(e, 0);
      assertNotNull(batch0);
    } finally {
      e.close();
    }
  }

  @Test void testGetBatchInfo_createsBatchesOnDemand() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("getBatchInfo", int.class);
      m.setAccessible(true);
      // Request batch 3 - should create batches 0..3
      Object batch3 = m.invoke(e, 3);
      assertNotNull(batch3);

      @SuppressWarnings("unchecked")
      List<?> batchList = (List<?>) getField(e, "batchInfoList");
      assertTrue(batchList.size() >= 4);
    } finally {
      e.close();
    }
  }

  @Test void testGetBatchInfo_returnsNullBeyondFullyProcessed()
      throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      // Mark as fully processed to prevent new batch creation
      setField(e, "isFullyProcessed", true);
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("getBatchInfo", int.class);
      m.setAccessible(true);
      Object batch999 = m.invoke(e, 999);
      assertNull(batch999);
    } finally {
      e.close();
    }
  }

  @Test void testGetBatchInfo_existingBatchReturned() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      Method m = ParquetEnumerator.class
          .getDeclaredMethod("getBatchInfo", int.class);
      m.setAccessible(true);
      // Calling twice for the same index should return same object
      Object first = m.invoke(e, 0);
      Object second = m.invoke(e, 0);
      assertNotNull(first);
      assertNotNull(second);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // getColumnStats - test null counts, min/max for numeric data
  // =========================================================================

  @Test void testGetColumnStats_noBatchLoaded() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      ParquetEnumerator.ColumnStats stats = e.getColumnStats(0);
      assertNotNull(stats);
      assertEquals(0, stats.nullCount);
      assertNull(stats.min);
      assertNull(stats.max);
    } finally {
      e.close();
    }
  }

  @Test void testGetColumnStats_outOfBoundsIndex() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      ParquetEnumerator.ColumnStats stats = e.getColumnStats(999);
      assertNotNull(stats);
      assertEquals(0, stats.nullCount);
      assertNull(stats.min);
      assertNull(stats.max);
    } finally {
      e.close();
    }
  }

  @Test void testGetColumnStats_withNumericData() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{
          Integer.valueOf(10), Integer.valueOf(3), Integer.valueOf(7)});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 3);

      ParquetEnumerator.ColumnStats stats = e.getColumnStats(0);
      assertEquals(0, stats.nullCount);
      assertEquals(Integer.valueOf(3), stats.min);
      assertEquals(Integer.valueOf(10), stats.max);
    } finally {
      e.close();
    }
  }

  @Test void testGetColumnStats_withNulls() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{null, "B", null, "A"});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 4);

      ParquetEnumerator.ColumnStats stats = e.getColumnStats(0);
      assertEquals(2, stats.nullCount);
      assertEquals("A", stats.min);
      assertEquals("B", stats.max);
    } finally {
      e.close();
    }
  }

  @Test void testGetColumnStats_allNulls() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{null, null, null});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 3);

      ParquetEnumerator.ColumnStats stats = e.getColumnStats(0);
      assertEquals(3, stats.nullCount);
      assertNull(stats.min);
      assertNull(stats.max);
    } finally {
      e.close();
    }
  }

  @Test void testGetColumnStats_singleValue() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{Double.valueOf(42.5)});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 1);

      ParquetEnumerator.ColumnStats stats = e.getColumnStats(0);
      assertEquals(0, stats.nullCount);
      assertEquals(Double.valueOf(42.5), stats.min);
      assertEquals(Double.valueOf(42.5), stats.max);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // columnSum - test numeric sum and null handling
  // =========================================================================

  @Test void testColumnSum_noBatchLoaded() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertEquals(0.0, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_outOfBoundsIndex() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertEquals(0.0, e.columnSum(999), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_integers() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{
          Integer.valueOf(10), Integer.valueOf(20), Integer.valueOf(30)});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 3);

      assertEquals(60.0, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_doubles() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{
          Double.valueOf(1.5), Double.valueOf(2.5), Double.valueOf(3.0)});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 3);

      assertEquals(7.0, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_withNulls() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{
          Integer.valueOf(5), null, Integer.valueOf(15), null});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 4);

      assertEquals(20.0, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_nonNumericValues() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{"hello", "world"});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 2);

      assertEquals(0.0, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_mixedNumericAndNonNumeric() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{
          Integer.valueOf(10), "skip", Double.valueOf(5.5), null});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 4);

      assertEquals(15.5, e.columnSum(0), 0.001);
    } finally {
      e.close();
    }
  }

  @Test void testColumnSum_secondColumn() throws Exception {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      List<Object[]> columns = new ArrayList<Object[]>();
      columns.add(new Object[]{"a", "b"});
      columns.add(new Object[]{Integer.valueOf(100), Integer.valueOf(200)});
      setField(e, "currentBatchColumns", columns);
      setField(e, "currentBatchRowCount", 2);

      assertEquals(0.0, e.columnSum(0), 0.001);
      assertEquals(300.0, e.columnSum(1), 0.001);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // getTotalSpillSize - test with no spills
  // =========================================================================

  @Test void testGetTotalSpillSize_noSpills() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertEquals(0L, e.getTotalSpillSize());
    } finally {
      e.close();
    }
  }

  @Test void testGetTotalSpillSize_afterIteration() throws IOException {
    Source source = createCsvSource("a\n1\n2\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      while (e.moveNext()) {
        // consume all rows
      }
      // With default 80MB threshold, small CSV should not spill
      assertEquals(0L, e.getTotalSpillSize());
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // getStreamingStats - test stats object creation
  // =========================================================================

  @Test void testGetStreamingStats_initial() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      ParquetEnumerator.StreamingStats stats = e.getStreamingStats();
      assertNotNull(stats);
      assertEquals(0, stats.spilledBatches);
      assertEquals(0L, stats.totalSpillSize);
      assertFalse(stats.isComplete);
    } finally {
      e.close();
    }
  }

  @Test void testGetStreamingStats_afterFullIteration() throws IOException {
    Source source = createCsvSource("a\n1\n2\n3\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      while (e.moveNext()) {
        // consume all
      }
      ParquetEnumerator.StreamingStats stats = e.getStreamingStats();
      assertNotNull(stats);
      assertEquals(0, stats.spilledBatches);
      assertTrue(stats.totalRows > 0 || stats.totalRows == -1);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // StreamingStats - static data class tests
  // =========================================================================

  @Test void testStreamingStats_spillRatioWithBatches() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(5, 10, 1024, 100, true, 3, 5000);
    assertEquals(0.3, stats.getSpillRatio(), 0.001);
  }

  @Test void testStreamingStats_spillRatioZeroBatches() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(0, 0, 0, 0, false, 0, 0);
    assertEquals(0.0, stats.getSpillRatio(), 0.001);
  }

  @Test void testStreamingStats_spillRatioAllSpilled() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(4, 4, 0, 40, true, 4, 80000);
    assertEquals(1.0, stats.getSpillRatio(), 0.001);
  }

  @Test void testStreamingStats_spillSizeFormattedBytes() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 100, true, 0, 500);
    assertEquals("500B", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStats_spillSizeFormattedZero() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 0, true, 0, 0);
    assertEquals("0B", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStats_spillSizeFormattedKB() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 100, true, 0, 2048);
    assertEquals("2KB", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStats_spillSizeFormattedMB() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(
            1, 1, 0, 100, true, 0, 5 * 1024 * 1024);
    assertEquals("5MB", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStats_toString() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(
            5, 10, 2 * 1024 * 1024, 1000, true, 3, 5000);
    String str = stats.toString();
    assertNotNull(str);
    assertTrue(str.contains("batches=5/10"));
    assertTrue(str.contains("rows=1000"));
    assertTrue(str.contains("complete=true"));
    assertTrue(str.contains("spilled=3"));
    assertTrue(str.contains("memory=2MB"));
  }

  @Test void testStreamingStats_toStringIncomplete() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(2, 5, 0, -1, false, 0, 0);
    String str = stats.toString();
    assertTrue(str.contains("complete=false"));
    assertTrue(str.contains("rows=-1"));
  }

  // =========================================================================
  // ColumnStats - data class tests
  // =========================================================================

  @Test void testColumnStats_constructor() {
    ParquetEnumerator.ColumnStats stats =
        new ParquetEnumerator.ColumnStats(5, "min", "max");
    assertEquals(5, stats.nullCount);
    assertEquals("min", stats.min);
    assertEquals("max", stats.max);
  }

  @Test void testColumnStats_nullMinMax() {
    ParquetEnumerator.ColumnStats stats =
        new ParquetEnumerator.ColumnStats(0, null, null);
    assertEquals(0, stats.nullCount);
    assertNull(stats.min);
    assertNull(stats.max);
  }

  @Test void testColumnStats_numericMinMax() {
    ParquetEnumerator.ColumnStats stats =
        new ParquetEnumerator.ColumnStats(1, Integer.valueOf(5),
            Integer.valueOf(99));
    assertEquals(1, stats.nullCount);
    assertEquals(Integer.valueOf(5), stats.min);
    assertEquals(Integer.valueOf(99), stats.max);
  }

  // =========================================================================
  // Constructor variants
  // =========================================================================

  @Test void testConstructor_defaultBatchSize() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0});
    assertNotNull(e);
    e.close();
  }

  @Test void testConstructor_withColumnCasing() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0}, "TO_UPPER");
    assertNotNull(e);
    e.close();
  }

  @Test void testConstructor_withBatchSize() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0}, 500);
    assertNotNull(e);
    e.close();
  }

  @Test void testConstructor_withBatchSizeAndCasing() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0}, 500, "TO_LOWER");
    assertNotNull(e);
    e.close();
  }

  @Test void testConstructor_withBatchSizeAndMemoryThreshold()
      throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0}, 500, 1024 * 1024L);
    assertNotNull(e);
    e.close();
  }

  @Test void testConstructor_fullParams() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
        createVarcharTypes(1), new int[]{0}, 500, 1024 * 1024L, "UNCHANGED");
    assertNotNull(e);
    e.close();
  }

  // =========================================================================
  // Iteration - moveNext / current / reset / close with real CSV
  // =========================================================================

  @Test void testCurrentBeforeMoveNext_returnsNull() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertNull(e.current());
    } finally {
      e.close();
    }
  }

  @Test void testMoveNext_singleRow() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertTrue(e.moveNext());
      Object val = e.current();
      assertNotNull(val);
    } finally {
      e.close();
    }
  }

  @Test void testMoveNext_multipleRows() throws IOException {
    Source source = createCsvSource("a\n1\n2\n3\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      int count = 0;
      while (e.moveNext()) {
        assertNotNull(e.current());
        count++;
      }
      assertEquals(3, count);
    } finally {
      e.close();
    }
  }

  @Test void testMoveNext_emptyFile() throws IOException {
    Source source = createCsvSource("a\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertFalse(e.moveNext());
    } finally {
      e.close();
    }
  }

  @Test void testMoveNext_cancelledMidIteration() throws IOException {
    Source source = createCsvSource("a\n1\n2\n3\n4\n5\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, cancel, createVarcharTypes(1), new int[]{0});
    try {
      assertTrue(e.moveNext());
      cancel.set(true);
      assertFalse(e.moveNext());
    } finally {
      e.close();
    }
  }

  @Test void testCancelFlag_stopsBeforeFirst() throws IOException {
    Source source = createCsvSource("a\n1\n");
    AtomicBoolean cancel = new AtomicBoolean(true);
    ParquetEnumerator<Object> e =
        new ParquetEnumerator<Object>(source, cancel, createVarcharTypes(1), new int[]{0});
    try {
      assertFalse(e.moveNext());
    } finally {
      e.close();
    }
  }

  @Test void testReset_clearsMemory() throws IOException {
    Source source = createCsvSource("a\n1\n2\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      e.moveNext();
      e.reset();
      assertEquals(0L, e.getMemoryUsage());
    } finally {
      e.close();
    }
  }

  @Test void testClose_noException() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    e.moveNext();
    e.close(); // should not throw
  }

  @Test void testClose_withoutMoveNext() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    e.close(); // should not throw even without iterating
  }

  // =========================================================================
  // getMemoryUsage / getEstimatedTotalSize
  // =========================================================================

  @Test void testGetMemoryUsage_beforeLoad() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertEquals(0L, e.getMemoryUsage());
    } finally {
      e.close();
    }
  }

  @Test void testGetMemoryUsage_afterLoad() throws IOException {
    Source source = createCsvSource("a\n1\n2\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      e.moveNext();
      long usage = e.getMemoryUsage();
      assertTrue(usage > 0);
    } finally {
      e.close();
    }
  }

  @Test void testGetEstimatedTotalSize_unknownBeforeIteration()
      throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertEquals(-1L, e.getEstimatedTotalSize());
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // cleanupOldSpillFiles
  // =========================================================================

  @Test void testCleanupOldSpillFiles_noOp() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      e.cleanupOldSpillFiles(10);
      // Should not throw or change anything
      assertEquals(0L, e.getTotalSpillSize());
    } finally {
      e.close();
    }
  }

  @Test void testCleanupOldSpillFiles_zeroMax() throws IOException {
    Source source = createCsvSource("a\n1\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      e.cleanupOldSpillFiles(0);
      // Should not throw
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // Real CSV iteration with two columns
  // =========================================================================

  @Test void testTwoColumnCsv_iterationValues() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n3,Charlie\n");
    ParquetEnumerator<Object[]> e = createTwoColumnEnumerator(source);
    try {
      int count = 0;
      while (e.moveNext()) {
        Object current = e.current();
        assertNotNull(current);
        count++;
      }
      assertEquals(3, count);
    } finally {
      e.close();
    }
  }

  // =========================================================================
  // JSON format - constructor throws due to null typeFactory
  // =========================================================================

  @Test void testJsonFormat_throwsOnConstruction() throws IOException {
    File jsonFile = tempDir.resolve("test.json").toFile();
    try (FileWriter w = new FileWriter(jsonFile)) {
      w.write("[{\"id\":1}]");
    }
    Source source = Sources.of(jsonFile);
    assertThrows(RuntimeException.class, () -> {
      ParquetEnumerator<Object> e =
          new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
          createVarcharTypes(1), new int[]{0});
      e.close();
    });
  }

  // =========================================================================
  // YAML format - constructor throws
  // =========================================================================

  @Test void testYamlFormat_throwsOnConstruction() throws IOException {
    File yamlFile = tempDir.resolve("test.yaml").toFile();
    try (FileWriter w = new FileWriter(yamlFile)) {
      w.write("key: value\n");
    }
    Source source = Sources.of(yamlFile);
    assertThrows(RuntimeException.class, () -> {
      ParquetEnumerator<Object> e =
          new ParquetEnumerator<Object>(source, new AtomicBoolean(false),
          createVarcharTypes(1), new int[]{0});
      e.close();
    });
  }

  // =========================================================================
  // Integration: CSV iteration with stats + column ops
  // =========================================================================

  @Test void testCsvIteration_thenColumnStats() throws IOException {
    Source source = createCsvSource("val\n10\n20\n30\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      // Iterate to load data
      while (e.moveNext()) {
        // consume
      }
      // After iteration, batch should be loaded
      ParquetEnumerator.StreamingStats stats = e.getStreamingStats();
      assertNotNull(stats);
    } finally {
      e.close();
    }
  }

  @Test void testCsvIteration_memoryUsageNonNegative() throws IOException {
    Source source = createCsvSource("a\n1\n2\n3\n4\n5\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      while (e.moveNext()) {
        long usage = e.getMemoryUsage();
        assertTrue(usage >= 0);
      }
    } finally {
      e.close();
    }
  }

  @Test void testCsvIteration_resetAndRecheck() throws IOException {
    Source source = createCsvSource("a\n1\n2\n");
    ParquetEnumerator<?> e = createSingleColumnEnumerator(source);
    try {
      assertTrue(e.moveNext());
      assertNotNull(e.current());

      e.reset();
      // After reset, memory cleared
      assertEquals(0L, e.getMemoryUsage());
      // After reset, current returns null (currentRow == -1)
      assertNull(e.current());
    } finally {
      e.close();
    }
  }
}
