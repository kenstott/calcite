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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ColumnBatch} column readers.
 * Tests Long, Double, Boolean, String readers, toRowFormat, null handling,
 * zero-copy operations, and boundary conditions.
 */
@Tag("unit")
public class ColumnBatchDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ColumnBatchDeepTest.class);

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

  // --- LongColumnReader tests ---

  @Test public void testLongColumnSum() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BigIntVector vec = (BigIntVector) root.getVector(0);
    vec.set(0, 10L);
    vec.set(1, 20L);
    vec.set(2, 30L);
    vec.setValueCount(3);
    root.setRowCount(3);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);

    assertEquals(60L, reader.sum());
    assertEquals(10L, reader.get(0));
    assertEquals(20L, reader.get(1));
    assertEquals(30L, reader.get(2));
    assertFalse(reader.isNull(0));

    batch.close();
  }

  @Test public void testLongColumnWithNulls() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BigIntVector vec = (BigIntVector) root.getVector(0);
    vec.set(0, 100L);
    vec.setNull(1);
    vec.set(2, 200L);
    vec.setValueCount(3);
    root.setRowCount(3);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);

    assertFalse(reader.isNull(0));
    assertTrue(reader.isNull(1));
    assertFalse(reader.isNull(2));
    assertEquals(300L, reader.sum()); // nulls skipped

    batch.close();
  }

  @Test public void testLongColumnFilter() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BigIntVector vec = (BigIntVector) root.getVector(0);
    vec.set(0, 5L);
    vec.set(1, 15L);
    vec.set(2, 25L);
    vec.setNull(3);
    vec.setValueCount(4);
    root.setRowCount(4);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);

    boolean[] filtered = reader.filter(value -> value > 10L);
    assertEquals(4, filtered.length);
    assertFalse(filtered[0]); // 5 <= 10
    assertTrue(filtered[1]);  // 15 > 10
    assertTrue(filtered[2]);  // 25 > 10
    assertFalse(filtered[3]); // null

    batch.close();
  }

  @Test public void testLongColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IllegalArgumentException.class, () -> batch.getLongColumn(0));
    batch.close();
  }

  // --- DoubleColumnReader tests ---

  @Test public void testDoubleColumnSum() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "value", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector(0);
    vec.set(0, 1.5);
    vec.set(1, 2.5);
    vec.set(2, 3.0);
    vec.setValueCount(3);
    root.setRowCount(3);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

    assertEquals(7.0, reader.sum(), 0.001);
    assertEquals(1.5, reader.get(0), 0.001);

    batch.close();
  }

  @Test public void testDoubleColumnMinMax() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "value", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector(0);
    vec.set(0, 5.0);
    vec.set(1, -3.0);
    vec.set(2, 10.0);
    vec.set(3, 2.0);
    vec.setValueCount(4);
    root.setRowCount(4);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

    double[] minMax = reader.minMax();
    assertEquals(-3.0, minMax[0], 0.001);
    assertEquals(10.0, minMax[1], 0.001);

    batch.close();
  }

  @Test public void testDoubleColumnMinMaxEmpty() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "value", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector(0);
    vec.setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

    double[] minMax = reader.minMax();
    assertEquals(0.0, minMax[0], 0.001);
    assertEquals(0.0, minMax[1], 0.001);

    batch.close();
  }

  @Test public void testDoubleColumnMinMaxAllNulls() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "value", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector(0);
    vec.setNull(0);
    vec.setNull(1);
    vec.setValueCount(2);
    root.setRowCount(2);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

    double[] minMax = reader.minMax();
    assertEquals(0.0, minMax[0], 0.001);
    assertEquals(0.0, minMax[1], 0.001);

    batch.close();
  }

  @Test public void testDoubleColumnFilter() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "value", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector(0);
    vec.set(0, -1.0);
    vec.set(1, 0.0);
    vec.set(2, 1.0);
    vec.setNull(3);
    vec.setValueCount(4);
    root.setRowCount(4);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

    boolean[] filtered = reader.filter(value -> value > 0.0);
    assertFalse(filtered[0]); // -1.0 <= 0
    assertFalse(filtered[1]); // 0.0 <= 0
    assertTrue(filtered[2]);  // 1.0 > 0
    assertFalse(filtered[3]); // null

    batch.close();
  }

  // --- BooleanColumnReader tests ---

  @Test public void testBooleanColumnCountTrue() {
    Schema schema =
        new Schema(Arrays.asList(new Field("flag", FieldType.nullable(new ArrowType.Bool()), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BitVector vec = (BitVector) root.getVector(0);
    vec.set(0, 1); // true
    vec.set(1, 0); // false
    vec.set(2, 1); // true
    vec.set(3, 1); // true
    vec.setNull(4); // null
    vec.setValueCount(5);
    root.setRowCount(5);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.BooleanColumnReader reader = batch.getBooleanColumn(0);

    assertEquals(3, reader.countTrue());
    assertTrue(reader.get(0));
    assertFalse(reader.get(1));
    assertTrue(reader.get(2));
    assertFalse(reader.isNull(0));
    assertTrue(reader.isNull(4));

    batch.close();
  }

  // --- StringColumnReader tests ---

  @Test public void testStringColumnFilter() {
    Schema schema =
        new Schema(Arrays.asList(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    VarCharVector vec = (VarCharVector) root.getVector(0);
    vec.set(0, "short".getBytes(StandardCharsets.UTF_8));
    vec.set(1, "a much longer string".getBytes(StandardCharsets.UTF_8));
    vec.setNull(2);
    vec.set(3, "hi".getBytes(StandardCharsets.UTF_8));
    vec.setValueCount(4);
    root.setRowCount(4);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);

    assertEquals("short", reader.get(0));
    assertEquals("a much longer string", reader.get(1));
    assertNull(reader.get(2));
    assertEquals("hi", reader.get(3));
    assertFalse(reader.isNull(0));
    assertTrue(reader.isNull(2));

    boolean[] filtered = reader.filter(value -> value != null && value.length() > 5);
    assertFalse(filtered[0]); // "short" = 5 chars, not > 5
    assertTrue(filtered[1]);  // 20 chars > 5
    assertFalse(filtered[2]); // null
    assertFalse(filtered[3]); // "hi" = 2 chars

    batch.close();
  }

  // --- toRowFormat tests ---

  @Test public void testToRowFormatMixedTypes() {
    Schema schema =
        new Schema(
            Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector intVec = (IntVector) root.getVector(0);
    VarCharVector strVec = (VarCharVector) root.getVector(1);

    intVec.set(0, 1);
    strVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
    intVec.set(1, 2);
    strVec.setNull(1);
    intVec.setValueCount(2);
    strVec.setValueCount(2);
    root.setRowCount(2);

    ColumnBatch batch = new ColumnBatch(root);
    Object[][] rows = batch.toRowFormat();

    assertEquals(2, rows.length);
    assertEquals(2, rows[0].length);
    assertEquals(1, rows[0][0]);
    assertNotNull(rows[0][1]);
    assertEquals(2, rows[1][0]);
    assertNull(rows[1][1]);

    batch.close();
  }

  // --- Index bounds tests ---

  @Test public void testGetIntColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.getIntColumn(5));
    batch.close();
  }

  @Test public void testGetLongColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.getLongColumn(5));
    batch.close();
  }

  @Test public void testGetDoubleColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.getDoubleColumn(5));
    batch.close();
  }

  @Test public void testGetBooleanColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.getBooleanColumn(5));
    batch.close();
  }

  @Test public void testGetStringColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    ColumnBatch batch = new ColumnBatch(root);
    assertThrows(IndexOutOfBoundsException.class, () -> batch.getStringColumn(5));
    batch.close();
  }

  // --- IntColumnReader sumZeroCopy ---

  @Test public void testIntColumnSumZeroCopy() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector(0);
    vec.set(0, 10);
    vec.set(1, 20);
    vec.set(2, 30);
    vec.setValueCount(3);
    root.setRowCount(3);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

    long zeroCopySum = reader.sumZeroCopy();
    long normalSum = reader.sum();
    assertEquals(normalSum, zeroCopySum);

    batch.close();
  }

  @Test public void testIntColumnGetValues() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector(0);
    vec.set(0, 5);
    vec.set(1, 10);
    vec.setNull(2);
    vec.set(3, 15);
    vec.setValueCount(4);
    root.setRowCount(4);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

    int[] values = reader.getValues();
    assertEquals(4, values.length);
    assertEquals(5, values[0]);
    assertEquals(10, values[1]);
    assertEquals(0, values[2]); // null -> 0
    assertEquals(15, values[3]);

    batch.close();
  }

  @Test public void testIntColumnGetDataBuffer() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector(0);
    vec.set(0, 42);
    vec.setValueCount(1);
    root.setRowCount(1);

    ColumnBatch batch = new ColumnBatch(root);
    ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

    assertNotNull(reader.getDataBuffer());
    assertNotNull(reader.getValidityBuffer());

    batch.close();
  }
}
