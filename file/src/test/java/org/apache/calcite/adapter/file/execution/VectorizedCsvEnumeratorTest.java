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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.vectorized.VectorizedCsvEnumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VectorizedCsvEnumerator}.
 * Covers vectorized batch processing, columnar conversion,
 * type-specific optimizations, and aggregation functions.
 */
@Tag("unit")
public class VectorizedCsvEnumeratorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VectorizedCsvEnumeratorTest.class);

  @TempDir
  Path tempDir;

  private RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(
      org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

  private File createCsvFile(String name, String... lines) throws Exception {
    File csv = tempDir.resolve(name).toFile();
    try (PrintWriter pw = new PrintWriter(csv)) {
      for (String line : lines) {
        pw.println(line);
      }
    }
    return csv;
  }

  private List<RelDataType> createFieldTypes(SqlTypeName... types) {
    List<RelDataType> fieldTypes = new ArrayList<>();
    for (SqlTypeName type : types) {
      fieldTypes.add(typeFactory.createSqlType(type));
    }
    return fieldTypes;
  }

  @Test
  public void testBasicEnumeration() throws Exception {
    File csv = createCsvFile("basic.csv",
        "NAME:string,AGE:int,SCORE:double",
        "Alice,30,95.5",
        "Bob,25,88.0",
        "Charlie,35,92.3");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.DOUBLE);
    List<Integer> fields = Arrays.asList(0, 1, 2);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    List<Object[]> rows = new ArrayList<>();
    while (enumerator.moveNext()) {
      Object[] current = enumerator.current();
      if (current != null) {
        rows.add(current.clone());
      }
    }

    assertEquals(3, rows.size());
    enumerator.close();
  }

  @Test
  public void testSmallBatchSize() throws Exception {
    File csv = createCsvFile("small_batch.csv",
        "NAME:string,VALUE:int",
        "A,1",
        "B,2",
        "C,3",
        "D,4",
        "E,5");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    // Use batch size of 2 to force multiple batches
    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 2);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }

    assertEquals(5, count);
    enumerator.close();
  }

  @Test
  public void testCancelFlag() throws Exception {
    File csv = createCsvFile("cancel.csv",
        "NAME:string,VALUE:int",
        "A,1",
        "B,2",
        "C,3");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);
    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), cancelFlag, types, fields, 1024);

    // Move to first row, then cancel
    assertTrue(enumerator.moveNext());
    cancelFlag.set(true);
    assertFalse(enumerator.moveNext());

    enumerator.close();
  }

  @Test
  public void testEmptyFile() throws Exception {
    File csv = createCsvFile("empty.csv",
        "NAME:string,VALUE:int");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testGetStats() throws Exception {
    File csv = createCsvFile("stats.csv",
        "NAME:string,VALUE:int",
        "A,10",
        "B,20",
        "C,30");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    // Consume all rows
    while (enumerator.moveNext()) {
      // consume
    }

    VectorizedCsvEnumerator.VectorizedStats stats = enumerator.getStats();
    assertNotNull(stats);
    assertEquals(3, stats.totalRowsProcessed);
    assertEquals(2, stats.columnCount);
    assertTrue(stats.memoryUsage > 0);

    String statsStr = stats.toString();
    assertNotNull(statsStr);
    assertTrue(statsStr.contains("VectorizedStats"));

    enumerator.close();
  }

  @Test
  public void testSumColumn() throws Exception {
    File csv = createCsvFile("sum.csv",
        "NAME:string,VALUE:int",
        "A,10",
        "B,20",
        "C,30");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    // Consume all rows to populate batches
    while (enumerator.moveNext()) {
      // consume
    }

    // sumColumn on out-of-bounds returns 0.0
    assertEquals(0.0, enumerator.sumColumn(999), 0.001);

    enumerator.close();
  }

  @Test
  public void testResetThrowsUnsupportedOperation() throws Exception {
    File csv = createCsvFile("reset.csv",
        "NAME:string,VALUE:int",
        "A,1",
        "B,2",
        "C,3");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    int firstPassCount = 0;
    while (enumerator.moveNext()) {
      firstPassCount++;
    }
    assertEquals(3, firstPassCount);

    // Reset delegates to CsvEnumerator which throws UnsupportedOperationException
    assertThrows(UnsupportedOperationException.class, enumerator::reset);

    enumerator.close();
  }

  @Test
  public void testCurrentReturnsNullWhenDone() throws Exception {
    File csv = createCsvFile("done.csv",
        "NAME:string",
        "A");

    List<RelDataType> types = createFieldTypes(SqlTypeName.VARCHAR);
    List<Integer> fields = Arrays.asList(0);

    VectorizedCsvEnumerator<Object> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    // Consume all rows
    while (enumerator.moveNext()) {
      // consume
    }

    // After done, current should return null
    assertNull(enumerator.current());

    enumerator.close();
  }

  @Test
  public void testBatchBoundaryWithExactMultiple() throws Exception {
    // Create exactly 6 rows with batch size 3 - tests exact batch boundary
    File csv = createCsvFile("boundary.csv",
        "VALUE:int",
        "1", "2", "3", "4", "5", "6");

    List<RelDataType> types = createFieldTypes(SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0);

    VectorizedCsvEnumerator<Object> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 3);

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }

    assertEquals(6, count);
    enumerator.close();
  }

  @Test
  public void testVectorizedStatsToString() {
    VectorizedCsvEnumerator.VectorizedStats stats =
        new VectorizedCsvEnumerator.VectorizedStats(100, 50, 3, 2048);

    String str = stats.toString();
    assertTrue(str.contains("rows=100"));
    assertTrue(str.contains("batchSize=50"));
    assertTrue(str.contains("columns=3"));
  }

  @Test
  public void testCloseReleasesResources() throws Exception {
    File csv = createCsvFile("close.csv",
        "NAME:string,VALUE:int",
        "A,1");

    List<RelDataType> types = createFieldTypes(
        SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    List<Integer> fields = Arrays.asList(0, 1);

    VectorizedCsvEnumerator<Object[]> enumerator =
        new VectorizedCsvEnumerator<>(
            Sources.of(csv), new AtomicBoolean(false), types, fields, 1024);

    // Consume all rows first
    while (enumerator.moveNext()) {
      // consume
    }

    // Close should not throw
    enumerator.close();

    // After close, current should be null
    assertNull(enumerator.current());
  }
}
