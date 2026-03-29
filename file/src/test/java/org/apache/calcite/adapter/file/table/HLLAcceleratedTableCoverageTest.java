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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link HLLAcceleratedTable}.
 * Tests wrapper table behavior without needing actual Parquet files.
 */
@Tag("unit")
class HLLAcceleratedTableCoverageTest {

  // ---------------------------------------------------------------
  // Constructor and basic property tests
  // ---------------------------------------------------------------

  @Test void testConstructorSetsFields() throws Exception {
    Source source = mockSource("/tmp/test.parquet");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    assertNotNull(table);

    // Verify internal fields via reflection
    Field sourceField = HLLAcceleratedTable.class.getDeclaredField("source");
    sourceField.setAccessible(true);
    assertEquals(source, sourceField.get(table));

    Field tableNameField = HLLAcceleratedTable.class.getDeclaredField("tableName");
    tableNameField.setAccessible(true);
    assertEquals("test_table", tableNameField.get(table));
  }

  @Test void testGetRowTypeReturnsProvidedType() {
    Source source = mockSource("/tmp/test.parquet");
    RelDataType rowType = mock(RelDataType.class);
    RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    assertEquals(rowType, table.getRowType(typeFactory));
  }

  @Test void testGetRowTypeIgnoresTypeFactory() {
    Source source = mockSource("/tmp/test.parquet");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    // Type factory is not used since row type is pre-computed
    RelDataType result = table.getRowType(null);
    assertEquals(rowType, result);
  }

  // ---------------------------------------------------------------
  // Sketch state tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testInitialSketchesNotBuilt() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    Field sketchesBuiltField = HLLAcceleratedTable.class.getDeclaredField("sketchesBuilt");
    sketchesBuiltField.setAccessible(true);
    assertFalse((Boolean) sketchesBuiltField.get(table));
  }

  @Test void testColumnSketchesMapInitiallyEmpty() throws Exception {
    Source source = mockSource("/tmp/test.parquet");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, ?> sketches = (Map<String, ?>) sketchesField.get(table);
    assertTrue(sketches.isEmpty());
  }

  @Test void testExactCountsMapInitiallyEmpty() throws Exception {
    Source source = mockSource("/tmp/test.parquet");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    Field exactCountsField = HLLAcceleratedTable.class.getDeclaredField("exactCounts");
    exactCountsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Long> counts = (Map<String, Long>) exactCountsField.get(table);
    assertTrue(counts.isEmpty());
  }

  // ---------------------------------------------------------------
  // getDistinctCount tests (with pre-injected sketches)
  // ---------------------------------------------------------------

  @Test void testGetDistinctCountForMissingColumn() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    // Mark sketches as built to skip file scan
    setSketchesBuilt(table, true);

    long count = table.getDistinctCount("nonexistent_col");
    assertEquals(-1, count);
  }

  @Test void testGetDistinctCountWithPreInjectedSketch() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    // Pre-inject a sketch via reflection
    org.apache.calcite.adapter.file.statistics.HyperLogLogSketch sketch =
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(42);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1", sketch);

    setSketchesBuilt(table, true);

    long count = table.getDistinctCount("col1");
    assertEquals(42, count);
  }

  // ---------------------------------------------------------------
  // hasHLLSketch tests
  // ---------------------------------------------------------------

  @Test void testHasHLLSketchReturnsFalseForMissing() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    setSketchesBuilt(table, true);

    assertFalse(table.hasHLLSketch("missing_col"));
  }

  @Test void testHasHLLSketchReturnsTrueForPresent() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    // Pre-inject a sketch
    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(10));

    setSketchesBuilt(table, true);

    assertTrue(table.hasHLLSketch("col1"));
  }

  // ---------------------------------------------------------------
  // scan() tests
  // ---------------------------------------------------------------

  @Test void testScanCountDistinctQuery() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    // Pre-inject sketch
    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(99));
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("col1");

    Enumerable<Object[]> result = table.scan(ctx);
    assertNotNull(result);

    Enumerator<Object[]> enumerator = result.enumerator();
    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertEquals(99L, row[0]);
    assertFalse(enumerator.moveNext());
  }

  @Test void testScanCountDistinctWithNullColumn() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn(null);

    // Should fall back to regular scan (which throws UnsupportedOperationException)
    Enumerable<Object[]> result = table.scan(ctx);
    assertNotNull(result);
    assertThrows(UnsupportedOperationException.class, () -> result.enumerator());
  }

  @Test void testScanNonCountDistinctQuery() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("select");

    // Non count_distinct queries fall back to unimplemented scan
    Enumerable<Object[]> result = table.scan(ctx);
    assertNotNull(result);
    assertThrows(UnsupportedOperationException.class, () -> result.enumerator());
  }

  @Test void testScanNullQueryType() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn(null);

    Enumerable<Object[]> result = table.scan(ctx);
    assertNotNull(result);
    assertThrows(UnsupportedOperationException.class, () -> result.enumerator());
  }

  @Test void testScanCountDistinctEnumeratorReset() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(50));
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("col1");

    Enumerator<Object[]> enumerator = table.scan(ctx).enumerator();

    // First iteration
    assertTrue(enumerator.moveNext());
    assertEquals(50L, enumerator.current()[0]);
    assertFalse(enumerator.moveNext());

    // Reset and iterate again
    enumerator.reset();
    assertTrue(enumerator.moveNext());
    assertEquals(50L, enumerator.current()[0]);
    assertFalse(enumerator.moveNext());
  }

  @Test void testScanCountDistinctEnumeratorClose() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(7));
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("col1");

    Enumerator<Object[]> enumerator = table.scan(ctx).enumerator();
    // Close should not throw
    enumerator.close();
  }

  // ---------------------------------------------------------------
  // saveSketchesToCache tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testSaveSketchesToCacheWithoutCacheDir(@TempDir Path tempDir) throws Exception {
    // No system property set, should return without error
    String previousValue = System.getProperty("calcite.file.statistics.cache.directory");
    try {
      System.clearProperty("calcite.file.statistics.cache.directory");

      Source source = mockSource("/tmp/test.csv");
      RelDataType rowType = mock(RelDataType.class);
      HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

      Method method = HLLAcceleratedTable.class.getDeclaredMethod("saveSketchesToCache");
      method.setAccessible(true);
      method.invoke(table);  // Should not throw
    } finally {
      if (previousValue != null) {
        System.setProperty("calcite.file.statistics.cache.directory", previousValue);
      }
    }
  }

  // ---------------------------------------------------------------
  // buildSketchesIfNeeded idempotency test
  // ---------------------------------------------------------------

  @Test void testBuildSketchesIfNeededIsIdempotent() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "test_table", rowType);

    // Mark as already built
    setSketchesBuilt(table, true);

    Method method = HLLAcceleratedTable.class.getDeclaredMethod("buildSketchesIfNeeded");
    method.setAccessible(true);

    // Should return immediately without modifying anything
    method.invoke(table);

    // Column sketches map should still be empty since we bypassed actual building
    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, ?> sketches = (Map<String, ?>) sketchesField.get(table);
    assertTrue(sketches.isEmpty());
  }

  // ---------------------------------------------------------------
  // Table with non-parquet source
  // ---------------------------------------------------------------

  @Test void testBuildSketchesForNonParquetSource() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("id", "name"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "csv_table", rowType);

    // Clear the cache dir property so saveSketchesToCache is a no-op
    String previousValue = System.getProperty("calcite.file.statistics.cache.directory");
    try {
      System.clearProperty("calcite.file.statistics.cache.directory");

      Method method = HLLAcceleratedTable.class.getDeclaredMethod("buildSketchesIfNeeded");
      method.setAccessible(true);
      method.invoke(table);

      // Sketches should be initialized (with zero estimates) but buildFromParquet should not run
      Field sketchesBuiltField = HLLAcceleratedTable.class.getDeclaredField("sketchesBuilt");
      sketchesBuiltField.setAccessible(true);
      assertTrue((Boolean) sketchesBuiltField.get(table));
    } finally {
      if (previousValue != null) {
        System.setProperty("calcite.file.statistics.cache.directory", previousValue);
      }
    }
  }

  // ---------------------------------------------------------------
  // Multiple columns tests
  // ---------------------------------------------------------------

  @Test void testMultipleColumnsGetDistinctCount() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("id", "name", "city"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "multi_col", rowType);

    // Pre-inject sketches for all columns
    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("id",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(1000));
    sketches.put("name",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(500));
    sketches.put("city",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(50));
    setSketchesBuilt(table, true);

    assertEquals(1000, table.getDistinctCount("id"));
    assertEquals(500, table.getDistinctCount("name"));
    assertEquals(50, table.getDistinctCount("city"));
    assertEquals(-1, table.getDistinctCount("nonexistent"));
  }

  @Test void testMultipleColumnsHasHLLSketch() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("a", "b"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("a",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(1));
    setSketchesBuilt(table, true);

    assertTrue(table.hasHLLSketch("a"));
    assertFalse(table.hasHLLSketch("b"));
  }

  // ---------------------------------------------------------------
  // getDistinctCount edge cases
  // ---------------------------------------------------------------

  @Test void testGetDistinctCountZeroEstimate() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(0));
    setSketchesBuilt(table, true);

    assertEquals(0, table.getDistinctCount("col1"));
  }

  @Test void testGetDistinctCountLargeEstimate() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(Long.MAX_VALUE));
    setSketchesBuilt(table, true);

    assertEquals(Long.MAX_VALUE, table.getDistinctCount("col1"));
  }

  @Test void testGetDistinctCountEmptyColumnName() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);
    setSketchesBuilt(table, true);

    assertEquals(-1, table.getDistinctCount(""));
  }

  // ---------------------------------------------------------------
  // scan() additional edge cases
  // ---------------------------------------------------------------

  @Test void testScanCountDistinctMissingColumn() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("nonexistent_col");

    // hasHLLSketch returns false for missing column, so falls through to regular scan
    Enumerable<Object[]> result = table.scan(ctx);
    assertNotNull(result);
    assertThrows(UnsupportedOperationException.class, () -> result.enumerator());
  }

  @Test void testScanCountDistinctCurrentBeforeMoveNext() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(25));
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("col1");

    Enumerator<Object[]> enumerator = table.scan(ctx).enumerator();

    // current() before moveNext() should return the row with value
    Object[] current = enumerator.current();
    assertNotNull(current);
  }

  @Test void testScanCountDistinctMultipleMoveNextCallsAfterEnd() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(10));
    setSketchesBuilt(table, true);

    DataContext ctx = mock(DataContext.class);
    when(ctx.get("query.type")).thenReturn("count_distinct");
    when(ctx.get("query.column")).thenReturn("col1");

    Enumerator<Object[]> enumerator = table.scan(ctx).enumerator();
    assertTrue(enumerator.moveNext());
    assertFalse(enumerator.moveNext());
    // Additional calls after end should still return false
    assertFalse(enumerator.moveNext());
    assertFalse(enumerator.moveNext());
  }

  // ---------------------------------------------------------------
  // hasHLLSketch edge cases
  // ---------------------------------------------------------------

  @Test void testHasHLLSketchEmptyColumnName() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);
    setSketchesBuilt(table, true);

    assertFalse(table.hasHLLSketch(""));
  }

  @Test void testHasHLLSketchCaseSensitive() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("Col1"));

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", rowType);

    Field sketchesField = HLLAcceleratedTable.class.getDeclaredField("columnSketches");
    sketchesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch> sketches =
        (Map<String, org.apache.calcite.adapter.file.statistics.HyperLogLogSketch>) sketchesField.get(table);
    sketches.put("Col1",
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(5));
    setSketchesBuilt(table, true);

    assertTrue(table.hasHLLSketch("Col1"));
    assertFalse(table.hasHLLSketch("col1"));
  }

  // ---------------------------------------------------------------
  // Constructor with different source paths
  // ---------------------------------------------------------------

  @Test void testConstructorWithParquetSource() throws Exception {
    Source source = mockSource("/data/warehouse/table.parquet");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "parquet_table", rowType);
    assertNotNull(table);

    Field tableNameField = HLLAcceleratedTable.class.getDeclaredField("tableName");
    tableNameField.setAccessible(true);
    assertEquals("parquet_table", tableNameField.get(table));
  }

  @Test void testConstructorWithEmptyTableName() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "", rowType);
    assertNotNull(table);

    Field tableNameField = HLLAcceleratedTable.class.getDeclaredField("tableName");
    tableNameField.setAccessible(true);
    assertEquals("", tableNameField.get(table));
  }

  @Test void testConstructorWithNullRowType() throws Exception {
    Source source = mockSource("/tmp/test.csv");

    HLLAcceleratedTable table = new HLLAcceleratedTable(source, "tbl", null);
    assertNotNull(table);

    // getRowType should return null when no row type is provided
    assertNull(table.getRowType(null));
  }

  // ---------------------------------------------------------------
  // Table name field tests
  // ---------------------------------------------------------------

  @Test void testTableNameWithSpecialChars() throws Exception {
    Source source = mockSource("/tmp/test.csv");
    RelDataType rowType = mock(RelDataType.class);

    HLLAcceleratedTable table = new HLLAcceleratedTable(
        source, "my-table_v2.0", rowType);

    Field tableNameField = HLLAcceleratedTable.class.getDeclaredField("tableName");
    tableNameField.setAccessible(true);
    assertEquals("my-table_v2.0", tableNameField.get(table));
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  private Source mockSource(String path) {
    Source source = mock(Source.class);
    when(source.path()).thenReturn(path);
    return source;
  }

  private void setSketchesBuilt(HLLAcceleratedTable table, boolean value) throws Exception {
    Field field = HLLAcceleratedTable.class.getDeclaredField("sketchesBuilt");
    field.setAccessible(true);
    field.set(table, value);
  }
}
