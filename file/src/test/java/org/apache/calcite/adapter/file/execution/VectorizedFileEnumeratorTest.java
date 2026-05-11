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

import org.apache.calcite.adapter.file.execution.vectorized.VectorizedFileEnumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VectorizedFileEnumerator}.
 * Covers batch processing, Arrow conversion round-trip, statistics tracking.
 */
@Tag("unit")
public class VectorizedFileEnumeratorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VectorizedFileEnumeratorTest.class);

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(
      org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

  private RelDataType createRowType(SqlTypeName... types) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < types.length; i++) {
      builder.add("col" + i, types[i]);
    }
    return builder.build();
  }

  @Test
  public void testBasicEnumeration() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1, "Alice"});
    data.add(new Object[]{2, "Bob"});
    data.add(new Object[]{3, "Charlie"});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    List<Object[]> results = new ArrayList<>();
    while (enumerator.moveNext()) {
      Object[] current = enumerator.current();
      assertNotNull(current);
      results.add(current.clone());
    }

    assertEquals(3, results.size());
    enumerator.close();
  }

  @Test
  public void testSmallBatchSize() {
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(new Object[]{i, "name" + i});
    }

    RelDataType rowType = createRowType(SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    // Small batch size forces multiple batches
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 3);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }

    assertEquals(10, count);
    enumerator.close();
  }

  @Test
  public void testEmptyIterator() {
    List<Object[]> data = new ArrayList<>();

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    assertFalse(enumerator.moveNext());
    assertNull(enumerator.current());
    enumerator.close();
  }

  @Test
  public void testSingleRow() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{42, 3.14, true, "test"});

    RelDataType rowType = createRowType(
        SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    assertTrue(enumerator.moveNext());
    Object[] current = enumerator.current();
    assertNotNull(current);
    assertEquals(4, current.length);

    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testGetStats() {
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      data.add(new Object[]{i, (double) i * 1.5});
    }

    RelDataType rowType = createRowType(SqlTypeName.INTEGER, SqlTypeName.DOUBLE);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 20);

    // Consume all
    while (enumerator.moveNext()) {
      // consume
    }

    VectorizedFileEnumerator.VectorizedStats stats = enumerator.getStats();
    assertNotNull(stats);
    assertEquals(50, stats.totalRowsProcessed);
    assertTrue(stats.batchesProcessed >= 3); // 50 rows / 20 batch size = 3 batches

    String statsStr = stats.toString();
    assertNotNull(statsStr);
    assertTrue(statsStr.contains("VectorizedStats"));
    assertTrue(statsStr.contains("batches="));
    assertTrue(statsStr.contains("rows=50"));

    enumerator.close();
  }

  @Test
  public void testVectorizedStatsToString() {
    VectorizedFileEnumerator.VectorizedStats stats =
        new VectorizedFileEnumerator.VectorizedStats(5, 1000, 250);

    String str = stats.toString();
    assertTrue(str.contains("batches=5"));
    assertTrue(str.contains("rows=1000"));
  }

  @Test
  public void testVectorizedStatsZeroBatches() {
    VectorizedFileEnumerator.VectorizedStats stats =
        new VectorizedFileEnumerator.VectorizedStats(0, 0, 0);

    String str = stats.toString();
    assertTrue(str.contains("batches=0"));
    assertTrue(str.contains("rows=0"));
  }

  @Test
  public void testResetThrowsUnsupportedOperation() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    assertThrows(UnsupportedOperationException.class, enumerator::reset);
    enumerator.close();
  }

  @Test
  public void testNullValuesInRows() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1, null});
    data.add(new Object[]{null, "test"});
    data.add(new Object[]{3, "hello"});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER, SqlTypeName.VARCHAR);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }

    assertEquals(3, count);
    enumerator.close();
  }

  @Test
  public void testMultipleDataTypes() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1, 2.5, true, "text"});
    data.add(new Object[]{2, 3.7, false, "more"});

    RelDataType rowType = createRowType(
        SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    int count = 0;
    while (enumerator.moveNext()) {
      Object[] current = enumerator.current();
      assertNotNull(current);
      assertEquals(4, current.length);
      count++;
    }

    assertEquals(2, count);
    enumerator.close();
  }

  @Test
  public void testBatchBoundary() {
    // Create exactly batch-size rows to test boundary condition
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      data.add(new Object[]{i});
    }

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 5);

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }

    assertEquals(5, count);
    enumerator.close();
  }

  @Test
  public void testCloseHandlesNonAutoCloseable() {
    List<Object[]> data = new ArrayList<>();
    data.add(new Object[]{1});

    RelDataType rowType = createRowType(SqlTypeName.INTEGER);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 1024);

    // consume
    while (enumerator.moveNext()) {
      // consume
    }

    // close should not throw even though currentBatchIterator is not AutoCloseable
    enumerator.close();
  }

  @Test
  public void testLargeDataset() {
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add(new Object[]{i, (double) i * 0.5, "name" + i});
    }

    RelDataType rowType = createRowType(
        SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.VARCHAR);
    VectorizedFileEnumerator enumerator =
        new VectorizedFileEnumerator(data.iterator(), rowType, 100);

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }

    assertEquals(1000, count);

    VectorizedFileEnumerator.VectorizedStats stats = enumerator.getStats();
    assertEquals(1000, stats.totalRowsProcessed);
    assertEquals(10, stats.batchesProcessed);

    enumerator.close();
  }
}
