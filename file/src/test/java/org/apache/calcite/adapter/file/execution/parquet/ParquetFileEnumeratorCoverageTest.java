/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.execution.parquet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for {@link ParquetFileEnumerator}.
 * Tests the enumerator lifecycle, CSV reading, batch processing,
 * projection, filtering, memory usage, and streamability.
 */
@Tag("unit")
public class ParquetFileEnumeratorCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Tests for constructor
  // ====================================================================

  @Test void testConstructor() {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertNotNull(enumerator);
    enumerator.close();
  }

  @Test void testConstructorDifferentBatchSizes() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> e1 =
        new ParquetFileEnumerator<Object[]>(source, rowType, 1);
    assertNotNull(e1);
    e1.close();

    Source source2 = createCsvSource("id\n1\n");
    ParquetFileEnumerator<Object[]> e2 =
        new ParquetFileEnumerator<Object[]>(source2, rowType, 10000);
    assertNotNull(e2);
    e2.close();
  }

  // ====================================================================
  // Tests for current() before moveNext
  // ====================================================================

  @Test void testCurrentBeforeMoveNext() {
    Source source = createCsvSource("id,name\n1,Alice\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // current() before moveNext should return null
    assertNull(enumerator.current());
    enumerator.close();
  }

  // ====================================================================
  // Tests for moveNext and current
  // ====================================================================

  @Test void testMoveNextAndCurrentWithData() {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testMoveNextEmptyData() {
    Source source = createCsvSource("id,name\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertFalse(enumerator.moveNext());
    assertNull(enumerator.current());

    enumerator.close();
  }

  @Test void testMoveNextMultipleRows() {
    Source source = createCsvSource("id,name\n1,A\n2,B\n3,C\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }
    assertEquals(3, count);

    enumerator.close();
  }

  @Test void testMoveNextSmallBatchSize() {
    Source source = createCsvSource("id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n");
    RelDataType rowType = createSimpleRowType();

    // Batch size of 2 means multiple batches for 5 rows
    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 2);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }
    assertEquals(5, count);

    enumerator.close();
  }

  @Test void testMoveNextExactlyBatchSize() {
    Source source = createCsvSource("id,name\n1,A\n2,B\n");
    RelDataType rowType = createSimpleRowType();

    // Batch size of 2 and exactly 2 data rows
    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 2);

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }
    assertEquals(2, count);

    enumerator.close();
  }

  // ====================================================================
  // Tests for type conversion in convertRow
  // ====================================================================

  @Test void testConvertRowInteger() {
    Source source = createCsvSource("val\n42\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowBigint() {
    Source source = createCsvSource("val\n9999999999\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.BIGINT);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowFloat() {
    Source source = createCsvSource("val\n3.14\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.FLOAT);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowDouble() {
    Source source = createCsvSource("val\n2.71828\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.DOUBLE);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowBoolean() {
    Source source = createCsvSource("val\ntrue\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.BOOLEAN);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowVarchar() {
    Source source = createCsvSource("val\nhello world\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.VARCHAR);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowNullValue() {
    Source source = createCsvSource("val\n\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    // Empty value should be null
    assertNull(row[0]);

    enumerator.close();
  }

  @Test void testConvertRowInvalidNumber() {
    Source source = createCsvSource("val\nabc\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    // Invalid integer should be null due to NumberFormatException handling
    assertNull(row[0]);

    enumerator.close();
  }

  @Test void testConvertRowFewerColumnsThanSchema() {
    // CSV has 1 column, schema expects 2
    Source source = createCsvSource("val\n1\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    // Second column should be null since CSV only has one column
    assertEquals(2, row.length);

    enumerator.close();
  }

  // ====================================================================
  // Tests for reset
  // ====================================================================

  @Test void testReset() {
    Source source = createCsvSource("id,name\n1,A\n2,B\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    enumerator.reset();

    // After reset, state should be cleared
    assertNull(enumerator.current());

    enumerator.close();
  }

  @Test void testResetBeforeUse() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // Reset before any moveNext should not throw
    enumerator.reset();

    enumerator.close();
  }

  // ====================================================================
  // Tests for close
  // ====================================================================

  @Test void testClose() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // Should not throw
    enumerator.close();
  }

  @Test void testDoubleClose() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    enumerator.close();
    // Double close should not throw
    enumerator.close();
  }

  // ====================================================================
  // Tests for getMemoryUsage
  // ====================================================================

  @Test void testGetMemoryUsageEmpty() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // No batches loaded yet
    assertEquals(0, enumerator.getMemoryUsage());

    enumerator.close();
  }

  @Test void testGetMemoryUsageAfterLoad() {
    Source source = createCsvSource("id,name\n1,Alice\n2,Bob\n3,Charlie\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // Load data by calling moveNext
    enumerator.moveNext();

    // Memory usage should be >= 0 (might be 0 if all batches were consumed)
    long usage = enumerator.getMemoryUsage();
    assertTrue(usage >= 0);

    enumerator.close();
  }

  // ====================================================================
  // Tests for isStreamable
  // ====================================================================

  @Test void testIsStreamableEmpty() {
    Source source = createCsvSource("id\n1\n");
    RelDataType rowType = createIntOnlyRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // No batches loaded
    assertFalse(enumerator.isStreamable());

    enumerator.close();
  }

  // ====================================================================
  // Tests for project
  // ====================================================================

  @Test void testProjectEmptyBatches() {
    Source source = createCsvSource("id,name\n1,A\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // Project before loading any data
    ParquetFileEnumerator<Object[]> projected = enumerator.project(new int[]{0});
    assertNotNull(projected);

    enumerator.close();
  }

  // ====================================================================
  // Tests for filter
  // ====================================================================

  @Test void testFilterEmptyBatches() {
    Source source = createCsvSource("id,name\n1,A\n");
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    // Filter before loading any data
    ParquetFileEnumerator<Object[]> filtered =
        enumerator.filter(0, new java.util.function.Predicate<Object>() {
          @Override public boolean test(Object o) {
            return true;
          }
        });
    assertNotNull(filtered);

    enumerator.close();
  }

  // ====================================================================
  // Tests for Real type
  // ====================================================================

  @Test void testConvertRowRealType() {
    Source source = createCsvSource("val\n1.5\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.REAL);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  // ====================================================================
  // Tests for CHAR type
  // ====================================================================

  @Test void testConvertRowCharType() {
    Source source = createCsvSource("val\nX\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.CHAR);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  // ====================================================================
  // Tests with multiple types in same row
  // ====================================================================

  @Test void testMultipleTypesInRow() {
    Source source = createCsvSource("id,price,active,name\n42,19.99,true,Widget\n");
    RelDataType rowType = createMixedRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(4, row.length);

    enumerator.close();
  }

  // ====================================================================
  // Tests for SMALLINT and TINYINT types
  // ====================================================================

  @Test void testConvertRowSmallint() {
    Source source = createCsvSource("val\n123\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.SMALLINT);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  @Test void testConvertRowTinyint() {
    Source source = createCsvSource("val\n7\n");
    RelDataType rowType = createRowTypeWith(SqlTypeName.TINYINT);

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 100);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);

    enumerator.close();
  }

  // ====================================================================
  // Tests for moveNext with large dataset
  // ====================================================================

  @Test void testMoveNextLargeDataset() {
    StringBuilder csv = new StringBuilder("id,val\n");
    for (int i = 0; i < 100; i++) {
      csv.append(i).append(",value_").append(i).append("\n");
    }
    Source source = createCsvSource(csv.toString());
    RelDataType rowType = createSimpleRowType();

    ParquetFileEnumerator<Object[]> enumerator =
        new ParquetFileEnumerator<Object[]>(source, rowType, 10);

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }
    assertEquals(100, count);

    enumerator.close();
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private Source createCsvSource(String csvContent) {
    try {
      File csvFile = new File(tempDir.toFile(), "test_" + System.nanoTime() + ".csv");
      PrintWriter writer = new PrintWriter(new FileWriter(csvFile));
      writer.print(csvContent);
      writer.close();
      return Sources.of(csvFile);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test CSV file", e);
    }
  }

  private RelDataType createSimpleRowType() {
    // id (INTEGER), name (VARCHAR)
    final org.apache.calcite.jdbc.JavaTypeFactoryImpl typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    return typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR, 100)
        .build();
  }

  private RelDataType createIntOnlyRowType() {
    final org.apache.calcite.jdbc.JavaTypeFactoryImpl typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    return typeFactory.builder()
        .add("val", SqlTypeName.INTEGER)
        .build();
  }

  private RelDataType createRowTypeWith(SqlTypeName typeName) {
    final org.apache.calcite.jdbc.JavaTypeFactoryImpl typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    return typeFactory.builder()
        .add("val", typeName)
        .build();
  }

  private RelDataType createMixedRowType() {
    final org.apache.calcite.jdbc.JavaTypeFactoryImpl typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    return typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("price", SqlTypeName.DOUBLE)
        .add("active", SqlTypeName.BOOLEAN)
        .add("name", SqlTypeName.VARCHAR, 100)
        .build();
  }
}
