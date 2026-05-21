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

import org.apache.calcite.adapter.file.execution.arrow.ArrowComputeColumnBatch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ArrowComputeColumnBatch}.
 */
@Tag("unit")
public class ArrowComputeColumnBatchTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ArrowComputeColumnBatchTest.class);

  private RootAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test public void testCreateBatchAndClose() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    // ArrowComputeColumnBatch takes ownership and closes the root
    ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root);
    batch.close();
    // No exception means success
  }

  @Test public void testGetArrowIntColumn() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      assertNotNull(intCol);
    }
  }

  @Test public void testGetArrowIntColumnOnNonIntThrows() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getArrowIntColumn(1));
    }
  }

  @Test public void testGetArrowDoubleColumn() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeDoubleColumn doubleCol =
          batch.getArrowDoubleColumn(1);
      assertNotNull(doubleCol);
    }
  }

  @Test public void testGetArrowDoubleColumnOnNonDoubleThrows() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getArrowDoubleColumn(0));
    }
  }

  @Test public void testIntColumnSumArrowCompute() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      // Values: 1, 2, 3, 4, 5
      long sum = intCol.sumArrowCompute();
      assertEquals(15L, sum);
      LOGGER.debug("ArrowCompute int sum: {}", sum);
    }
  }

  @Test public void testIntColumnSumWithNulls() {
    VectorSchemaRoot root = createIntBatchWithNulls(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      // Values: 1, null, 3, null, 5 -> sum = 9
      long sum = intCol.sumArrowCompute();
      assertEquals(9L, sum);
    }
  }

  @Test public void testIntColumnFilterArrowCompute() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      IntVector result = intCol.filterArrowCompute(3);
      assertNotNull(result);
      LOGGER.debug("ArrowCompute filter result obtained");
    }
  }

  @Test public void testIntColumnSortArrowCompute() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      IntVector result = intCol.sortArrowCompute();
      assertNotNull(result);
      LOGGER.debug("ArrowCompute sort result obtained");
    }
  }

  @Test public void testDoubleColumnComputeStatisticsGPU() {
    VectorSchemaRoot root = createIntDoubleBatch(5);
    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeDoubleColumn doubleCol =
          batch.getArrowDoubleColumn(1);
      ArrowComputeColumnBatch.ComputeStatistics stats =
          doubleCol.computeStatisticsGPU();
      assertNotNull(stats);
      // Values: 1.5, 3.0, 4.5, 6.0, 7.5
      assertEquals(22.5, stats.sum, 0.01);
      assertEquals(1.5, stats.min, 0.01);
      assertEquals(7.5, stats.max, 0.01);
      assertEquals(5, stats.count);
      assertEquals(4.5, stats.mean, 0.01);
      LOGGER.debug("ComputeStatistics: {}", stats);
    }
  }

  @Test public void testDoubleColumnStatisticsWithSingleValue() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("value",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    Float8Vector vector = (Float8Vector) root.getVector("value");
    vector.set(0, 42.0);
    vector.setValueCount(1);
    root.setRowCount(1);

    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeDoubleColumn doubleCol =
          batch.getArrowDoubleColumn(0);
      ArrowComputeColumnBatch.ComputeStatistics stats =
          doubleCol.computeStatisticsGPU();
      assertEquals(42.0, stats.sum, 0.01);
      assertEquals(42.0, stats.min, 0.01);
      assertEquals(42.0, stats.max, 0.01);
      assertEquals(42.0, stats.mean, 0.01);
      assertEquals(1, stats.count);
    }
  }

  @Test public void testComputeStatisticsToString() {
    ArrowComputeColumnBatch.ComputeStatistics stats =
        new ArrowComputeColumnBatch.ComputeStatistics(10.0, 1.0, 5.0, 2.5, 4);
    String str = stats.toString();
    assertNotNull(str);
    assertTrue(str.contains("10.00"), "Should contain sum");
    assertTrue(str.contains("1.00"), "Should contain min");
    assertTrue(str.contains("5.00"), "Should contain max");
    assertTrue(str.contains("2.50"), "Should contain mean");
    assertTrue(str.contains("4"), "Should contain count");
  }

  @Test public void testLargeIntColumnSum() {
    Schema schema =
        new Schema(Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    IntVector vector = (IntVector) root.getVector("id");
    int numRows = 1000;
    long expectedSum = 0;
    for (int i = 0; i < numRows; i++) {
      vector.set(i, i + 1);
      expectedSum += (i + 1);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);

    try (ArrowComputeColumnBatch batch = new ArrowComputeColumnBatch(root)) {
      ArrowComputeColumnBatch.ArrowComputeIntColumn intCol =
          batch.getArrowIntColumn(0);
      long sum = intCol.sumArrowCompute();
      assertEquals(expectedSum, sum);
    }
  }

  // --- Helper methods ---

  private VectorSchemaRoot createIntDoubleBatch(int numRows) {
    Schema schema =
        new Schema(
            Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("amount",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    IntVector intV = (IntVector) root.getVector("id");
    Float8Vector doubleV = (Float8Vector) root.getVector("amount");
    for (int i = 0; i < numRows; i++) {
      intV.set(i, i + 1);
      doubleV.set(i, (i + 1) * 1.5);
    }
    intV.setValueCount(numRows);
    doubleV.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createIntBatchWithNulls(int numRows) {
    Schema schema =
        new Schema(Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    IntVector vector = (IntVector) root.getVector("id");
    for (int i = 0; i < numRows; i++) {
      if (i % 2 == 0) {
        vector.set(i, i + 1);
      } else {
        vector.setNull(i);
      }
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }
}
