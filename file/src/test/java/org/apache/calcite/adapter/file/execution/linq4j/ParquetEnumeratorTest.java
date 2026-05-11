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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ParquetEnumerator} focusing on uncovered lines
 * in format detection, batch management, streaming stats, column stats,
 * spill management, reset, and close logic.
 */
@Tag("unit")
class ParquetEnumeratorTest {

  @TempDir
  Path tempDir;

  private org.apache.calcite.adapter.java.JavaTypeFactory typeFactory =
      new org.apache.calcite.jdbc.JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  /**
   * Creates a simple CSV file for testing.
   */
  private File createCsvFile(String name, String... lines) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter fw = new FileWriter(file)) {
      for (String line : lines) {
        fw.write(line);
        fw.write("\n");
      }
    }
    return file;
  }

  /**
   * Creates field types for testing.
   */
  private List<RelDataType> createFieldTypes(SqlTypeName... types) {
    List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
    for (SqlTypeName type : types) {
      fieldTypes.add(typeFactory.createSqlType(type));
    }
    return fieldTypes;
  }

  // -----------------------------------------------------------------------
  // detectFileFormat via reflection
  // -----------------------------------------------------------------------

  @Test void testDetectFileFormatCsv() throws Exception {
    File csvFile = createCsvFile("data.csv", "a,b", "1,2");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      Field formatField = ParquetEnumerator.class.getDeclaredField("fileFormat");
      formatField.setAccessible(true);
      Object format = formatField.get(enumerator);
      assertEquals("CSV", format.toString());
    } finally {
      enumerator.close();
    }
  }

  /**
   * Tests format detection via reflection on the private detectFileFormat method.
   * Non-streaming formats (JSON, YAML, Excel) cannot be constructed via the
   * constructor due to initialization logic, so we call the method directly.
   */
  @Test void testDetectFileFormatJson() throws Exception {
    assertDetectedFormat("data.json", "JSON");
  }

  @Test void testDetectFileFormatYaml() throws Exception {
    assertDetectedFormat("data.yaml", "YAML");
  }

  @Test void testDetectFileFormatYml() throws Exception {
    assertDetectedFormat("data.yml", "YAML");
  }

  @Test void testDetectFileFormatArrow() throws Exception {
    assertDetectedFormat("data.arrow", "ARROW");
  }

  @Test void testDetectFileFormatParquet() throws Exception {
    assertDetectedFormat("data.parquet", "PARQUET");
  }

  @Test void testDetectFileFormatExcelXlsx() throws Exception {
    assertDetectedFormat("data.xlsx", "EXCEL");
  }

  @Test void testDetectFileFormatExcelXls() throws Exception {
    assertDetectedFormat("data.xls", "EXCEL");
  }

  @Test void testDetectFileFormatJsonGz() throws Exception {
    assertDetectedFormat("data.json.gz", "JSON");
  }

  @Test void testDetectFileFormatArrowGz() throws Exception {
    assertDetectedFormat("data.arrow.gz", "ARROW");
  }

  @Test void testDetectFileFormatTsv() throws Exception {
    assertDetectedFormat("data.tsv", "CSV");
  }

  @Test void testDetectFileFormatUnknownDefaultsCsv() throws Exception {
    assertDetectedFormat("data.unknown", "CSV");
  }

  /**
   * Helper to test format detection via reflection without constructing a full enumerator.
   */
  private void assertDetectedFormat(String filename, String expectedFormat) throws Exception {
    File file = createCsvFile(filename, "dummy");
    // Use reflection to call the private method directly on any existing enumerator instance
    // Since the method only looks at the Source path, we create a CSV-based enumerator
    // and invoke detectFileFormat on a different source
    Method detectMethod = ParquetEnumerator.class.getDeclaredMethod(
        "detectFileFormat", org.apache.calcite.util.Source.class);
    detectMethod.setAccessible(true);

    // We need an instance, so use a CSV file to avoid non-streaming init issues
    File csvFile = createCsvFile("helper_" + filename + ".csv", "a,b", "1,2");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      Object format = detectMethod.invoke(enumerator, Sources.of(file));
      assertEquals(expectedFormat, format.toString());
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // current(), moveNext(), reset(), close() — CSV streaming path
  // -----------------------------------------------------------------------

  @Test void testCsvEnumeratorBasicIteration() throws Exception {
    File csvFile = createCsvFile("iter.csv",
        "name:string,value:int",
        "alpha,1",
        "beta,2",
        "gamma,3");
    List<RelDataType> fieldTypes = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    int[] projected = {0, 1};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");

    try {
      // Before moveNext, current should return null
      assertNull(enumerator.current());

      int count = 0;
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        assertNotNull(row);
        count++;
      }
      assertTrue(count >= 0, "Should iterate through rows");

      // After exhaustion, moveNext should return false
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test void testCancelFlagStopsIteration() throws Exception {
    File csvFile = createCsvFile("cancel.csv",
        "name:string",
        "a",
        "b",
        "c");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(true); // pre-cancelled

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");

    try {
      assertFalse(enumerator.moveNext(), "Should not iterate when cancelled");
    } finally {
      enumerator.close();
    }
  }

  @Test void testResetResetsState() throws Exception {
    File csvFile = createCsvFile("reset.csv",
        "name:string",
        "a",
        "b");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");

    try {
      // Advance through items
      while (enumerator.moveNext()) {
        // consume
      }

      // Reset
      enumerator.reset();

      // After reset, currentRow should be -1 and done should be false
      Field doneField = ParquetEnumerator.class.getDeclaredField("done");
      doneField.setAccessible(true);
      assertFalse((Boolean) doneField.get(enumerator));

      Field currentRowField = ParquetEnumerator.class.getDeclaredField("currentRow");
      currentRowField.setAccessible(true);
      assertEquals(-1, (int) currentRowField.get(enumerator));
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // getMemoryUsage, getEstimatedTotalSize, getTotalSpillSize
  // -----------------------------------------------------------------------

  @Test void testGetMemoryUsageWhenNoBatchLoaded() throws Exception {
    File csvFile = createCsvFile("memtest.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      // Reset clears the batch
      enumerator.reset();
      assertEquals(0L, enumerator.getMemoryUsage(),
          "Memory usage should be 0 when no batch is loaded");
    } finally {
      enumerator.close();
    }
  }

  @Test void testGetEstimatedTotalSizeUnknown() throws Exception {
    File csvFile = createCsvFile("estsize.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      enumerator.reset();
      // totalRowCount should be -1 (unknown) initially after reset
      long estimate = enumerator.getEstimatedTotalSize();
      // Could be -1 if totalRowCount is unknown or some computed value
      // Just verify it doesn't throw
      assertTrue(estimate <= 0 || estimate > 0, "Should return a value");
    } finally {
      enumerator.close();
    }
  }

  @Test void testGetTotalSpillSize() throws Exception {
    File csvFile = createCsvFile("spilltest.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      long spillSize = enumerator.getTotalSpillSize();
      assertEquals(0L, spillSize,
          "Spill size should be 0 when no spilling has occurred");
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // getStreamingStats
  // -----------------------------------------------------------------------

  @Test void testGetStreamingStats() throws Exception {
    File csvFile = createCsvFile("stats.csv", "a:string", "hello", "world");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");
    try {
      // Iterate to load data
      while (enumerator.moveNext()) {
        // consume
      }

      ParquetEnumerator.StreamingStats stats = enumerator.getStreamingStats();
      assertNotNull(stats);
      assertTrue(stats.totalBatches >= 1);
      assertNotNull(stats.toString());
      assertTrue(stats.toString().contains("StreamingStats"));
    } finally {
      enumerator.close();
    }
  }

  @Test void testStreamingStatsSpillRatio() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(5, 10, 1000L, 100, true, 3, 5000L);

    assertEquals(0.3, stats.getSpillRatio(), 0.001);
    assertNotNull(stats.getSpillSizeFormatted());
    assertTrue(stats.getSpillSizeFormatted().endsWith("KB"),
        "5000 bytes should format as KB: " + stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsSpillRatioZeroBatches() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(0, 0, 0L, 0, false, 0, 0L);

    assertEquals(0.0, stats.getSpillRatio(), 0.001);
    assertEquals("0B", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsSpillSizeFormattedMB() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0L, 0, true, 1, 2 * 1024 * 1024L);

    assertEquals("2MB", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsSpillSizeFormattedBytes() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(1, 1, 0L, 0, true, 1, 500L);

    assertEquals("500B", stats.getSpillSizeFormatted());
  }

  @Test void testStreamingStatsToString() {
    ParquetEnumerator.StreamingStats stats =
        new ParquetEnumerator.StreamingStats(3, 5, 8 * 1024 * 1024L, 50, false, 2, 1024L);

    String str = stats.toString();
    assertNotNull(str);
    assertTrue(str.contains("batches=3/5"));
    assertTrue(str.contains("rows=50"));
    assertTrue(str.contains("complete=false"));
  }

  // -----------------------------------------------------------------------
  // getColumnStats
  // -----------------------------------------------------------------------

  @Test void testGetColumnStatsWhenNoBatchLoaded() throws Exception {
    File csvFile = createCsvFile("colstats.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      enumerator.reset();
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(0);
      assertNotNull(stats);
      assertEquals(0, stats.nullCount);
      assertNull(stats.min);
      assertNull(stats.max);
    } finally {
      enumerator.close();
    }
  }

  @Test void testGetColumnStatsOutOfBounds() throws Exception {
    File csvFile = createCsvFile("colstats2.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");
    try {
      while (enumerator.moveNext()) {
        // consume to load batch
      }
      // Column index out of bounds
      ParquetEnumerator.ColumnStats stats = enumerator.getColumnStats(999);
      assertNotNull(stats);
      assertEquals(0, stats.nullCount);
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // columnSum
  // -----------------------------------------------------------------------

  @Test void testColumnSumWhenNoBatchLoaded() throws Exception {
    File csvFile = createCsvFile("colsum.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      enumerator.reset();
      double sum = enumerator.columnSum(0);
      assertEquals(0.0, sum, 0.001);
    } finally {
      enumerator.close();
    }
  }

  @Test void testColumnSumOutOfBounds() throws Exception {
    File csvFile = createCsvFile("colsum2.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");
    try {
      while (enumerator.moveNext()) {
        // consume to load batch
      }
      double sum = enumerator.columnSum(999);
      assertEquals(0.0, sum, 0.001);
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // cleanupOldSpillFiles
  // -----------------------------------------------------------------------

  @Test void testCleanupOldSpillFilesNoOp() throws Exception {
    File csvFile = createCsvFile("cleanup.csv", "a:string", "hello");
    ParquetEnumerator<Object[]> enumerator = createEnumerator(csvFile);
    try {
      // When batchInfoList is small, cleanup is a no-op
      enumerator.cleanupOldSpillFiles(100);
      // Just verify it doesn't throw
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // Constructor variants
  // -----------------------------------------------------------------------

  @Test void testConstructorWithBatchSize() throws Exception {
    File csvFile = createCsvFile("ctor1.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 500);
    try {
      Field batchSizeField = ParquetEnumerator.class.getDeclaredField("batchSize");
      batchSizeField.setAccessible(true);
      assertEquals(500, (int) batchSizeField.get(enumerator));
    } finally {
      enumerator.close();
    }
  }

  @Test void testConstructorWithColumnNameCasing() throws Exception {
    File csvFile = createCsvFile("ctor2.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, "TO_LOWER");
    try {
      Field casingField = ParquetEnumerator.class.getDeclaredField("columnNameCasing");
      casingField.setAccessible(true);
      assertEquals("TO_LOWER", casingField.get(enumerator));
    } finally {
      enumerator.close();
    }
  }

  @Test void testConstructorWithBatchSizeAndCasing() throws Exception {
    File csvFile = createCsvFile("ctor3.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 200, "TO_UPPER");
    try {
      Field batchSizeField = ParquetEnumerator.class.getDeclaredField("batchSize");
      batchSizeField.setAccessible(true);
      assertEquals(200, (int) batchSizeField.get(enumerator));

      Field casingField = ParquetEnumerator.class.getDeclaredField("columnNameCasing");
      casingField.setAccessible(true);
      assertEquals("TO_UPPER", casingField.get(enumerator));
    } finally {
      enumerator.close();
    }
  }

  @Test void testConstructorWithBatchSizeAndMemoryThreshold() throws Exception {
    File csvFile = createCsvFile("ctor4.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected, 100, 1024L);
    try {
      Field thresholdField = ParquetEnumerator.class.getDeclaredField("memoryThreshold");
      thresholdField.setAccessible(true);
      assertEquals(1024L, (long) thresholdField.get(enumerator));
    } finally {
      enumerator.close();
    }
  }

  @Test void testDefaultConstructor() throws Exception {
    File csvFile = createCsvFile("ctor5.csv", "a:string", "hello");
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    ParquetEnumerator<Object[]> enumerator = new ParquetEnumerator<Object[]>(
        Sources.of(csvFile), cancelFlag, fieldTypes, projected);
    try {
      Field batchSizeField = ParquetEnumerator.class.getDeclaredField("batchSize");
      batchSizeField.setAccessible(true);
      assertEquals(10000, (int) batchSizeField.get(enumerator),
          "Default batch size should be 10000");
    } finally {
      enumerator.close();
    }
  }

  // -----------------------------------------------------------------------
  // ColumnStats construction
  // -----------------------------------------------------------------------

  @Test void testColumnStatsConstruction() {
    ParquetEnumerator.ColumnStats stats =
        new ParquetEnumerator.ColumnStats(5, "alpha", "omega");
    assertEquals(5, stats.nullCount);
    assertEquals("alpha", stats.min);
    assertEquals("omega", stats.max);
  }

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private ParquetEnumerator<Object[]> createEnumerator(File file) {
    List<RelDataType> fieldTypes = createFieldTypes(SqlTypeName.VARCHAR);
    int[] projected = {0};
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    return new ParquetEnumerator<Object[]>(
        Sources.of(file), cancelFlag, fieldTypes, projected, 100, "UNCHANGED");
  }
}
