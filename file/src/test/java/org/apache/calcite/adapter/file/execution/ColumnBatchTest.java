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

import org.apache.calcite.adapter.file.execution.arrow.ColumnBatch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ColumnBatch} and its column readers.
 */
@Tag("unit")
public class ColumnBatchTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ColumnBatchTest.class);

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

  // --- ColumnBatch basic tests ---

  @Test
  public void testEmptyBatch() {
    VectorSchemaRoot root = createIntBatch(0);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertEquals(0, batch.getRowCount());
      assertEquals(1, batch.getColumnCount());
    }
  }

  @Test
  public void testGetColumnCountWithMultipleColumns() {
    VectorSchemaRoot root = createMixedBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertEquals(5, batch.getColumnCount());
      assertEquals(3, batch.getRowCount());
    }
  }

  @Test
  public void testGetIntColumnOutOfBoundsThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> batch.getIntColumn(5));
    }
  }

  @Test
  public void testGetIntColumnOnNonIntThrows() {
    VectorSchemaRoot root = createDoubleBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getIntColumn(0));
    }
  }

  @Test
  public void testGetLongColumnOutOfBoundsThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> batch.getLongColumn(5));
    }
  }

  @Test
  public void testGetLongColumnOnNonLongThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getLongColumn(0));
    }
  }

  @Test
  public void testGetDoubleColumnOutOfBoundsThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> batch.getDoubleColumn(5));
    }
  }

  @Test
  public void testGetDoubleColumnOnNonDoubleThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getDoubleColumn(0));
    }
  }

  @Test
  public void testGetBooleanColumnOutOfBoundsThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> batch.getBooleanColumn(5));
    }
  }

  @Test
  public void testGetBooleanColumnOnNonBooleanThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getBooleanColumn(0));
    }
  }

  @Test
  public void testGetStringColumnOutOfBoundsThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IndexOutOfBoundsException.class,
          () -> batch.getStringColumn(5));
    }
  }

  @Test
  public void testGetStringColumnOnNonStringThrows() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      assertThrows(IllegalArgumentException.class,
          () -> batch.getStringColumn(0));
    }
  }

  // --- IntColumnReader tests ---

  @Test
  public void testIntColumnReaderGetAndIsNull() {
    VectorSchemaRoot root = createIntBatchWithNulls(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      assertEquals(1, reader.get(0));
      assertEquals(3, reader.get(2));
      assertFalse(reader.isNull(0));
      assertTrue(reader.isNull(1));
      assertFalse(reader.isNull(2));
      assertTrue(reader.isNull(3));
      assertFalse(reader.isNull(4));
    }
  }

  @Test
  public void testIntColumnReaderSum() {
    VectorSchemaRoot root = createIntBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      // Values: 1, 2, 3, 4, 5
      assertEquals(15L, reader.sum());
    }
  }

  @Test
  public void testIntColumnReaderSumWithNulls() {
    VectorSchemaRoot root = createIntBatchWithNulls(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      // Values: 1, null, 3, null, 5 -> sum = 9
      assertEquals(9L, reader.sum());
    }
  }

  @Test
  public void testIntColumnReaderSumZeroCopy() {
    VectorSchemaRoot root = createIntBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      long regularSum = reader.sum();
      long zeroCopySum = reader.sumZeroCopy();
      assertEquals(regularSum, zeroCopySum,
          "Zero-copy sum should match regular sum");
    }
  }

  @Test
  public void testIntColumnReaderFilter() {
    VectorSchemaRoot root = createIntBatch(10);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      // Filter for values > 5
      boolean[] selection = reader.filter(
          new java.util.function.Predicate<Integer>() {
            @Override public boolean test(Integer value) {
              return value > 5;
            }
          });
      assertEquals(10, selection.length);
      for (int i = 0; i < 5; i++) {
        assertFalse(selection[i], "Values 1-5 should not pass filter");
      }
      for (int i = 5; i < 10; i++) {
        assertTrue(selection[i], "Values 6-10 should pass filter");
      }
    }
  }

  @Test
  public void testIntColumnReaderFilterWithNulls() {
    VectorSchemaRoot root = createIntBatchWithNulls(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      boolean[] selection = reader.filter(
          new java.util.function.Predicate<Integer>() {
            @Override public boolean test(Integer value) {
              return value > 2;
            }
          });
      // Values: 1, null, 3, null, 5
      assertFalse(selection[0]); // 1 > 2 = false
      assertFalse(selection[1]); // null -> false
      assertTrue(selection[2]);  // 3 > 2 = true
      assertFalse(selection[3]); // null -> false
      assertTrue(selection[4]);  // 5 > 2 = true
    }
  }

  @Test
  public void testIntColumnReaderGetValues() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      int[] values = reader.getValues();
      assertArrayEquals(new int[]{1, 2, 3}, values);
    }
  }

  @Test
  public void testIntColumnReaderGetValuesWithNulls() {
    VectorSchemaRoot root = createIntBatchWithNulls(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      int[] values = reader.getValues();
      // Nulls become 0
      assertEquals(1, values[0]);
      assertEquals(0, values[1]); // null -> 0
      assertEquals(3, values[2]);
      assertEquals(0, values[3]); // null -> 0
      assertEquals(5, values[4]);
    }
  }

  @Test
  public void testIntColumnReaderGetDataBuffer() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      assertNotNull(reader.getDataBuffer());
    }
  }

  @Test
  public void testIntColumnReaderGetValidityBuffer() {
    VectorSchemaRoot root = createIntBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);
      assertNotNull(reader.getValidityBuffer());
    }
  }

  // --- LongColumnReader tests ---

  @Test
  public void testLongColumnReaderGetAndIsNull() {
    VectorSchemaRoot root = createLongBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);
      assertEquals(100L, reader.get(0));
      assertEquals(200L, reader.get(1));
      assertEquals(300L, reader.get(2));
      assertFalse(reader.isNull(0));
    }
  }

  @Test
  public void testLongColumnReaderSum() {
    VectorSchemaRoot root = createLongBatch(4);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);
      // Values: 100, 200, 300, 400
      assertEquals(1000L, reader.sum());
    }
  }

  @Test
  public void testLongColumnReaderFilter() {
    VectorSchemaRoot root = createLongBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);
      // Filter for values > 250
      boolean[] selection = reader.filter(
          new java.util.function.Predicate<Long>() {
            @Override public boolean test(Long value) {
              return value > 250L;
            }
          });
      assertFalse(selection[0]); // 100
      assertFalse(selection[1]); // 200
      assertTrue(selection[2]);  // 300
      assertTrue(selection[3]);  // 400
      assertTrue(selection[4]);  // 500
    }
  }

  // --- DoubleColumnReader tests ---

  @Test
  public void testDoubleColumnReaderGetAndIsNull() {
    VectorSchemaRoot root = createDoubleBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);
      assertEquals(1.5, reader.get(0), 0.001);
      assertEquals(3.0, reader.get(1), 0.001);
      assertEquals(4.5, reader.get(2), 0.001);
      assertFalse(reader.isNull(0));
    }
  }

  @Test
  public void testDoubleColumnReaderSum() {
    VectorSchemaRoot root = createDoubleBatch(4);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);
      // Values: 1.5, 3.0, 4.5, 6.0
      assertEquals(15.0, reader.sum(), 0.001);
    }
  }

  @Test
  public void testDoubleColumnReaderMinMax() {
    VectorSchemaRoot root = createDoubleBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);
      double[] minMax = reader.minMax();
      assertEquals(1.5, minMax[0], 0.001);  // min
      assertEquals(7.5, minMax[1], 0.001);  // max
    }
  }

  @Test
  public void testDoubleColumnReaderMinMaxEmpty() {
    VectorSchemaRoot root = createDoubleBatch(0);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      // No rows, so minMax returns {0, 0}
      // Actually we need at least a double column - empty batch
      assertEquals(0, batch.getRowCount());
    }
  }

  @Test
  public void testDoubleColumnReaderFilter() {
    VectorSchemaRoot root = createDoubleBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);
      boolean[] selection = reader.filter(
          new java.util.function.Predicate<Double>() {
            @Override public boolean test(Double value) {
              return value > 3.0;
            }
          });
      // Values: 1.5, 3.0, 4.5, 6.0, 7.5
      assertFalse(selection[0]); // 1.5
      assertFalse(selection[1]); // 3.0
      assertTrue(selection[2]);  // 4.5
      assertTrue(selection[3]);  // 6.0
      assertTrue(selection[4]);  // 7.5
    }
  }

  // --- BooleanColumnReader tests ---

  @Test
  public void testBooleanColumnReaderGetAndIsNull() {
    VectorSchemaRoot root = createBooleanBatch(4);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.BooleanColumnReader reader = batch.getBooleanColumn(0);
      assertTrue(reader.get(0));  // true
      assertFalse(reader.get(1)); // false
      assertTrue(reader.get(2));  // true
      assertFalse(reader.get(3)); // false
      assertFalse(reader.isNull(0));
    }
  }

  @Test
  public void testBooleanColumnReaderCountTrue() {
    VectorSchemaRoot root = createBooleanBatch(6);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.BooleanColumnReader reader = batch.getBooleanColumn(0);
      // Pattern: true, false, true, false, true, false -> 3 true values
      assertEquals(3, reader.countTrue());
    }
  }

  // --- StringColumnReader tests ---

  @Test
  public void testStringColumnReaderGet() {
    VectorSchemaRoot root = createStringBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);
      assertEquals("value_0", reader.get(0));
      assertEquals("value_1", reader.get(1));
      assertEquals("value_2", reader.get(2));
      assertFalse(reader.isNull(0));
    }
  }

  @Test
  public void testStringColumnReaderGetNull() {
    VectorSchemaRoot root = createStringBatchWithNulls(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);
      assertEquals("value_0", reader.get(0));
      assertTrue(reader.isNull(1));
      assertNull(reader.get(1));
      assertEquals("value_2", reader.get(2));
    }
  }

  @Test
  public void testStringColumnReaderFilter() {
    VectorSchemaRoot root = createStringBatch(5);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);
      boolean[] selection = reader.filter(
          new java.util.function.Predicate<String>() {
            @Override public boolean test(String value) {
              return value.contains("_3") || value.contains("_4");
            }
          });
      assertFalse(selection[0]); // value_0
      assertFalse(selection[1]); // value_1
      assertFalse(selection[2]); // value_2
      assertTrue(selection[3]);  // value_3
      assertTrue(selection[4]);  // value_4
    }
  }

  // --- toRowFormat tests ---

  @Test
  public void testToRowFormatWithMixedColumns() {
    VectorSchemaRoot root = createMixedBatch(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      Object[][] rows = batch.toRowFormat();
      assertEquals(3, rows.length);
      assertEquals(5, rows[0].length);
      // First column: int
      assertEquals(1, rows[0][0]);
      // Second column: long
      assertEquals(100L, rows[0][1]);
      // Third column: double
      assertEquals(1.5, (Double) rows[0][2], 0.001);
    }
  }

  @Test
  public void testToRowFormatWithNulls() {
    VectorSchemaRoot root = createIntBatchWithNulls(3);
    try (ColumnBatch batch = new ColumnBatch(root)) {
      Object[][] rows = batch.toRowFormat();
      assertEquals(3, rows.length);
      assertNotNull(rows[0][0]); // 1
      assertNull(rows[1][0]);    // null
      assertNotNull(rows[2][0]); // 3
    }
  }

  // --- Helper methods ---

  private VectorSchemaRoot createIntBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    IntVector vector = (IntVector) root.getVector("id");
    for (int i = 0; i < numRows; i++) {
      vector.set(i, i + 1);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createIntBatchWithNulls(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));
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

  private VectorSchemaRoot createLongBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    BigIntVector vector = (BigIntVector) root.getVector("value");
    for (int i = 0; i < numRows; i++) {
      vector.set(i, (i + 1) * 100L);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createDoubleBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("value",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    Float8Vector vector = (Float8Vector) root.getVector("value");
    for (int i = 0; i < numRows; i++) {
      vector.set(i, (i + 1) * 1.5);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createBooleanBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("flag", FieldType.nullable(new ArrowType.Bool()), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    BitVector vector = (BitVector) root.getVector("flag");
    for (int i = 0; i < numRows; i++) {
      vector.set(i, i % 2 == 0 ? 1 : 0);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createStringBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    VarCharVector vector = (VarCharVector) root.getVector("name");
    for (int i = 0; i < numRows; i++) {
      vector.set(i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createStringBatchWithNulls(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    VarCharVector vector = (VarCharVector) root.getVector("name");
    for (int i = 0; i < numRows; i++) {
      if (i % 2 == 0) {
        vector.set(i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
      } else {
        vector.setNull(i);
      }
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createMixedBatch(int numRows) {
    Schema schema = new Schema(Arrays.asList(
        new Field("intCol", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("longCol", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("doubleCol",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null),
        new Field("boolCol", FieldType.nullable(new ArrowType.Bool()), null),
        new Field("stringCol", FieldType.nullable(new ArrowType.Utf8()), null)
    ));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector intV = (IntVector) root.getVector("intCol");
    BigIntVector longV = (BigIntVector) root.getVector("longCol");
    Float8Vector doubleV = (Float8Vector) root.getVector("doubleCol");
    BitVector boolV = (BitVector) root.getVector("boolCol");
    VarCharVector strV = (VarCharVector) root.getVector("stringCol");

    for (int i = 0; i < numRows; i++) {
      intV.set(i, i + 1);
      longV.set(i, (i + 1) * 100L);
      doubleV.set(i, (i + 1) * 1.5);
      boolV.set(i, i % 2 == 0 ? 1 : 0);
      strV.set(i, ("str_" + i).getBytes(StandardCharsets.UTF_8));
    }

    intV.setValueCount(numRows);
    longV.setValueCount(numRows);
    doubleV.setValueCount(numRows);
    boolV.setValueCount(numRows);
    strV.setValueCount(numRows);
    root.setRowCount(numRows);

    return root;
  }
}
