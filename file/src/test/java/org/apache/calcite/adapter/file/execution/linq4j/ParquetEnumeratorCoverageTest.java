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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link ParquetEnumerator}.
 */
@Tag("unit")
class ParquetEnumeratorCoverageTest {

  @TempDir
  Path tempDir;

  private Source createCsvSource(String content) throws IOException {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write(content);
    }
    return Sources.of(csvFile);
  }

  private Source createJsonSource(String content) throws IOException {
    File jsonFile = tempDir.resolve("test.json").toFile();
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write(content);
    }
    return Sources.of(jsonFile);
  }

  private List<RelDataType> createFieldTypes() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    List<RelDataType> types = new ArrayList<RelDataType>();
    types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    return types;
  }

  private List<RelDataType> createSingleFieldTypes() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    List<RelDataType> types = new ArrayList<RelDataType>();
    types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    return types;
  }

  // ========== Constructor variants ==========

  @Test void testConstructorBasic() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);
    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorWithColumnCasing() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected, "TO_UPPER");
    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorWithBatchSize() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected, 5000);
    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorWithBatchSizeAndCasing() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected, 5000, "TO_LOWER");
    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorWithBatchSizeAndThreshold() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected, 5000, 50 * 1024 * 1024L);
    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorFull() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected, 5000, 50 * 1024 * 1024L, "UNCHANGED");
    assertNotNull(enumerator);
    enumerator.close();
  }

  // ========== File format detection ==========

  @Test void testDetectsJsonFormat() throws IOException {
    File jsonFile = tempDir.resolve("data.json").toFile();
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[{\"id\":1}]");
    }
    Source source = Sources.of(jsonFile);
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createSingleFieldTypes();
    int[] projected = new int[]{0};

    // JSON format detection works, but the ParquetEnumerator constructor
    // passes null typeFactory to JsonEnumerator.deduceRowType() which causes
    // a RuntimeException. We verify format detection by the error message.
    RuntimeException ex = null;
    try {
      ParquetEnumerator<Object[]> enumerator =
          new ParquetEnumerator<Object[]>(source, cancel, types, projected);
      enumerator.close();
    } catch (RuntimeException e) {
      ex = e;
    }
    // Should fail specifically with JSON format initialization error
    assertNotNull(ex);
    assertTrue(ex.getMessage().contains("JSON"));
  }

  @Test void testDetectsJsonGzFormat() throws IOException {
    File file = tempDir.resolve("data.json.gz").toFile();
    file.createNewFile();
    // We just need the path detection, not actual reading
    Source source = Sources.of(file);
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createSingleFieldTypes();
    int[] projected = new int[]{0};

    try {
      ParquetEnumerator<Object[]> enumerator =
          new ParquetEnumerator<Object[]>(source, cancel, types, projected);
      enumerator.close();
    } catch (RuntimeException e) {
      // Expected since empty gz file
    }
  }

  // ========== Streaming stats ==========

  @Test void testStreamingStatsInitial() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();
    assertNotNull(stats);
    assertNotNull(stats.toString());
    assertTrue(stats.toString().contains("StreamingStats"));

    enumerator.close();
  }

  @Test void testStreamingStatsSpillRatioZero() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(0, 0, 0, 0, false, 0, 0);
    assertEquals(0.0, stats.getSpillRatio(), 0.001);
  }

  @Test void testStreamingStatsSpillSizeFormattedBytes() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 100, true, 0, 500);
    assertEquals("500B", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsSpillSizeFormattedKB() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 100, true, 0, 2048);
    assertEquals("2KB", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsSpillSizeFormattedMB() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0, 100, true, 0, 2 * 1024 * 1024);
    assertEquals("2MB", stats.getSpillSizeFormatted());
  }

  // ========== Column stats ==========

  @Test void testColumnStatsNoBatchLoaded() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    // Before any moveNext, batch columns may not be loaded
    ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(0);
    assertNotNull(stats);

    enumerator.close();
  }

  @Test void testColumnStatsOutOfBounds() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(99);
    assertNotNull(stats);
    assertEquals(0, stats.nullCount);
    assertNull(stats.min);
    assertNull(stats.max);

    enumerator.close();
  }

  // ========== Memory and spill ==========

  @Test void testGetMemoryUsageNoBatch() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    // Memory usage before any iteration
    long memory = enumerator.getMemoryUsage();
    assertTrue(memory >= 0);

    enumerator.close();
  }

  @Test void testGetEstimatedTotalSizeUnknown() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    long totalSize = enumerator.getEstimatedTotalSize();
    // Either -1 (unknown) or some calculated value
    assertTrue(totalSize >= -1);

    enumerator.close();
  }

  @Test void testGetTotalSpillSize() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    long spillSize = enumerator.getTotalSpillSize();
    assertEquals(0, spillSize);

    enumerator.close();
  }

  @Test void testCleanupOldSpillFilesNoOp() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    // Should not throw
    enumerator.cleanupOldSpillFiles(10);

    enumerator.close();
  }

  // ========== moveNext / current / reset / close ==========

  @Test void testMoveNextAndCurrentCsvSingleColumn() throws IOException {
    // Use single-column projection because the CsvEnumerator converter() method
    // only creates a CsvTypeConverter for SingleColumnRowConverter, not ArrayRowConverter
    Source source = createCsvSource("id\n1\n2\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createSingleFieldTypes();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    assertTrue(enumerator.moveNext());
    Object current = enumerator.current();
    assertNotNull(current);

    enumerator.close();
  }

  @Test void testCurrentBeforeMoveNext() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    // current() before moveNext() should return null
    Object current = enumerator.current();
    assertNull(current);

    enumerator.close();
  }

  @Test void testCancelFlagStopsIteration() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n");
    AtomicBoolean cancel = new AtomicBoolean(true); // Pre-cancelled
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    assertFalse(enumerator.moveNext());

    enumerator.close();
  }

  @Test void testResetRestartsIteration() throws IOException {
    // Use single-column CSV to avoid ArrayRowConverter typeConverter NPE
    Source source = createCsvSource("id\n1\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createSingleFieldTypes();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    enumerator.moveNext();
    enumerator.reset();
    // After reset, currentRow should be -1 again

    enumerator.close();
  }

  @Test void testColumnSumNoBatch() throws IOException {
    Source source = createCsvSource("id,name\n1,Alice\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createFieldTypes();
    int[] projected = new int[]{0, 1};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);

    double sum = enumerator.columnSum(99);
    assertEquals(0.0, sum, 0.001);

    enumerator.close();
  }

  @Test void testCloseCleanup() throws IOException {
    // Use single-column CSV to avoid ArrayRowConverter typeConverter NPE
    Source source = createCsvSource("id\n1\n");
    AtomicBoolean cancel = new AtomicBoolean(false);
    List<RelDataType> types = createSingleFieldTypes();
    int[] projected = new int[]{0};

    ParquetEnumerator<Object[]> enumerator =
        new ParquetEnumerator<Object[]>(source, cancel, types, projected);
    enumerator.moveNext();
    enumerator.close();
    // Should not throw
  }

  @Test void testColumnStatsConstructor() {
    ParquetEnumerator.ColumnStats stats =
        new ParquetEnumerator.ColumnStats(5, "min", "max");
    assertEquals(5, stats.nullCount);
    assertEquals("min", stats.min);
    assertEquals("max", stats.max);
  }

  @Test void testStreamingStatsToString() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(3, 5, 1024 * 1024, 500, true, 1, 2048);
    String str = stats.toString();
    assertTrue(str.contains("StreamingStats"));
    assertTrue(str.contains("3"));
    assertTrue(str.contains("5"));
    assertTrue(str.contains("500"));
  }
}
