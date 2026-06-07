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
package org.apache.calcite.adapter.file.execution.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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
 * Integration tests for Arrow ColumnBatch and vectorized column operations.
 */
@Tag("integration")
public class ColumnBatchIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ColumnBatchIntegrationTest.class);

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  // --- ColumnBatch basic tests ---

  @Test public void testColumnBatchRowAndColumnCount() {
    Schema schema =
        new Schema(
            Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", ArrowType.Utf8.INSTANCE),
        Field.nullable("value", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      Float8Vector valueVec = (Float8Vector) root.getVector("value");

      idVec.allocateNew(3);
      nameVec.allocateNew(3);
      valueVec.allocateNew(3);

      idVec.set(0, 1);
      idVec.set(1, 2);
      idVec.set(2, 3);
      nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
      nameVec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
      nameVec.set(2, "Charlie".getBytes(StandardCharsets.UTF_8));
      valueVec.set(0, 10.5);
      valueVec.set(1, 20.3);
      valueVec.set(2, 30.1);

      idVec.setValueCount(3);
      nameVec.setValueCount(3);
      valueVec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      assertEquals(3, batch.getRowCount());
      assertEquals(3, batch.getColumnCount());
    }
  }

  // --- IntColumnReader tests ---

  @Test public void testIntColumnReader() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("id");
      vec.allocateNew(5);
      vec.set(0, 10);
      vec.set(1, 20);
      vec.set(2, 30);
      vec.set(3, 40);
      vec.set(4, 50);
      vec.setValueCount(5);
      root.setRowCount(5);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      assertEquals(10, reader.get(0));
      assertEquals(30, reader.get(2));
      assertEquals(50, reader.get(4));
      assertFalse(reader.isNull(0));
    }
  }

  @Test public void testIntColumnReaderSum() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(4);
      vec.set(0, 10);
      vec.set(1, 20);
      vec.setNull(2); // null value
      vec.set(3, 30);
      vec.setValueCount(4);
      root.setRowCount(4);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      assertEquals(60, reader.sum());
      assertTrue(reader.isNull(2));
    }
  }

  @Test public void testIntColumnReaderSumZeroCopy() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(3);
      vec.set(0, 100);
      vec.set(1, 200);
      vec.set(2, 300);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      assertEquals(600, reader.sumZeroCopy());
    }
  }

  @Test public void testIntColumnReaderFilter() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(5);
      vec.set(0, 5);
      vec.set(1, 15);
      vec.set(2, 25);
      vec.set(3, 10);
      vec.set(4, 30);
      vec.setValueCount(5);
      root.setRowCount(5);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      boolean[] selection = reader.filter(v -> v > 10);
      assertFalse(selection[0]);
      assertTrue(selection[1]);
      assertTrue(selection[2]);
      assertFalse(selection[3]);
      assertTrue(selection[4]);
    }
  }

  @Test public void testIntColumnReaderGetValues() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(3);
      vec.set(0, 1);
      vec.set(1, 2);
      vec.set(2, 3);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      int[] values = reader.getValues();
      assertArrayEquals(new int[]{1, 2, 3}, values);
    }
  }

  @Test public void testIntColumnReaderGetDataBuffer() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(2);
      vec.set(0, 42);
      vec.set(1, 99);
      vec.setValueCount(2);
      root.setRowCount(2);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      assertNotNull(reader.getDataBuffer());
      assertNotNull(reader.getValidityBuffer());
    }
  }

  // --- LongColumnReader tests ---

  @Test public void testLongColumnReader() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("big_val", new ArrowType.Int(64, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      BigIntVector vec = (BigIntVector) root.getVector("big_val");
      vec.allocateNew(3);
      vec.set(0, 1000000000L);
      vec.set(1, 2000000000L);
      vec.set(2, 3000000000L);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);

      assertEquals(1000000000L, reader.get(0));
      assertEquals(6000000000L, reader.sum());
      assertFalse(reader.isNull(1));
    }
  }

  @Test public void testLongColumnReaderFilter() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("big_val", new ArrowType.Int(64, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      BigIntVector vec = (BigIntVector) root.getVector("big_val");
      vec.allocateNew(3);
      vec.set(0, 100L);
      vec.set(1, 500L);
      vec.set(2, 1000L);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.LongColumnReader reader = batch.getLongColumn(0);

      boolean[] selection = reader.filter(v -> v >= 500L);
      assertFalse(selection[0]);
      assertTrue(selection[1]);
      assertTrue(selection[2]);
    }
  }

  // --- DoubleColumnReader tests ---

  @Test public void testDoubleColumnReader() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("price", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      Float8Vector vec = (Float8Vector) root.getVector("price");
      vec.allocateNew(3);
      vec.set(0, 9.99);
      vec.set(1, 19.99);
      vec.set(2, 29.99);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

      assertEquals(9.99, reader.get(0), 0.001);
      assertEquals(59.97, reader.sum(), 0.01);
      assertFalse(reader.isNull(0));
    }
  }

  @Test public void testDoubleColumnReaderMinMax() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      Float8Vector vec = (Float8Vector) root.getVector("val");
      vec.allocateNew(5);
      vec.set(0, 15.0);
      vec.set(1, 3.0);
      vec.set(2, 42.0);
      vec.setNull(3);
      vec.set(4, 8.0);
      vec.setValueCount(5);
      root.setRowCount(5);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

      double[] minMax = reader.minMax();
      assertEquals(3.0, minMax[0], 0.001);
      assertEquals(42.0, minMax[1], 0.001);
    }
  }

  @Test public void testDoubleColumnReaderFilter() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      Float8Vector vec = (Float8Vector) root.getVector("val");
      vec.allocateNew(3);
      vec.set(0, 1.0);
      vec.set(1, 5.5);
      vec.set(2, 2.0);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

      boolean[] selection = reader.filter(v -> v > 3.0);
      assertFalse(selection[0]);
      assertTrue(selection[1]);
      assertFalse(selection[2]);
    }
  }

  // --- BooleanColumnReader tests ---

  @Test public void testBooleanColumnReader() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("active", new ArrowType.Bool())));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      BitVector vec = (BitVector) root.getVector("active");
      vec.allocateNew(4);
      vec.set(0, 1);
      vec.set(1, 0);
      vec.set(2, 1);
      vec.set(3, 1);
      vec.setValueCount(4);
      root.setRowCount(4);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.BooleanColumnReader reader = batch.getBooleanColumn(0);

      assertTrue(reader.get(0));
      assertFalse(reader.get(1));
      assertTrue(reader.get(2));
      assertFalse(reader.isNull(0));
      assertEquals(3, reader.countTrue());
    }
  }

  // --- StringColumnReader tests ---

  @Test public void testStringColumnReader() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VarCharVector vec = (VarCharVector) root.getVector("name");
      vec.allocateNew(3);
      vec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
      vec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
      vec.setNull(2);
      vec.setValueCount(3);
      root.setRowCount(3);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);

      assertEquals("Alice", reader.get(0));
      assertEquals("Bob", reader.get(1));
      assertNull(reader.get(2));
      assertFalse(reader.isNull(0));
      assertTrue(reader.isNull(2));
    }
  }

  @Test public void testStringColumnReaderFilter() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VarCharVector vec = (VarCharVector) root.getVector("name");
      vec.allocateNew(4);
      vec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
      vec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
      vec.set(2, "Alex".getBytes(StandardCharsets.UTF_8));
      vec.setNull(3);
      vec.setValueCount(4);
      root.setRowCount(4);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.StringColumnReader reader = batch.getStringColumn(0);

      boolean[] selection = reader.filter(s -> s.startsWith("A"));
      assertTrue(selection[0]);
      assertFalse(selection[1]);
      assertTrue(selection[2]);
      assertFalse(selection[3]);
    }
  }

  // --- toRowFormat tests ---

  @Test public void testToRowFormat() {
    Schema schema =
        new Schema(
            Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");

      idVec.allocateNew(2);
      nameVec.allocateNew(2);

      idVec.set(0, 1);
      idVec.set(1, 2);
      nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
      nameVec.setNull(1);

      idVec.setValueCount(2);
      nameVec.setValueCount(2);
      root.setRowCount(2);

      ColumnBatch batch = new ColumnBatch(root);
      Object[][] rows = batch.toRowFormat();

      assertEquals(2, rows.length);
      assertEquals(2, rows[0].length);
      assertEquals(1, rows[0][0]);
      assertNotNull(rows[0][1]);
      assertEquals(2, rows[1][0]);
      assertNull(rows[1][1]); // null value
    }
  }

  // --- Error cases ---

  @Test public void testGetIntColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("id");
      vec.allocateNew(1);
      vec.set(0, 1);
      vec.setValueCount(1);
      root.setRowCount(1);

      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IndexOutOfBoundsException.class, () -> batch.getIntColumn(5));
    }
  }

  @Test public void testGetIntColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VarCharVector vec = (VarCharVector) root.getVector("name");
      vec.allocateNew(1);
      vec.set(0, "test".getBytes(StandardCharsets.UTF_8));
      vec.setValueCount(1);
      root.setRowCount(1);

      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IllegalArgumentException.class, () -> batch.getIntColumn(0));
    }
  }

  @Test public void testGetLongColumnOutOfBounds() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(0);
      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IndexOutOfBoundsException.class, () -> batch.getLongColumn(5));
    }
  }

  @Test public void testGetLongColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(0);
      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IllegalArgumentException.class, () -> batch.getLongColumn(0));
    }
  }

  @Test public void testGetDoubleColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(0);
      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IllegalArgumentException.class, () -> batch.getDoubleColumn(0));
    }
  }

  @Test public void testGetBooleanColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(0);
      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IllegalArgumentException.class, () -> batch.getBooleanColumn(0));
    }
  }

  @Test public void testGetStringColumnWrongType() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(0);
      ColumnBatch batch = new ColumnBatch(root);
      assertThrows(IllegalArgumentException.class, () -> batch.getStringColumn(0));
    }
  }

  // --- DoubleColumnReader minMax with all nulls ---

  @Test public void testDoubleMinMaxAllNulls() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      Float8Vector vec = (Float8Vector) root.getVector("val");
      vec.allocateNew(2);
      vec.setNull(0);
      vec.setNull(1);
      vec.setValueCount(2);
      root.setRowCount(2);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.DoubleColumnReader reader = batch.getDoubleColumn(0);

      double[] minMax = reader.minMax();
      assertEquals(0.0, minMax[0], 0.001);
      assertEquals(0.0, minMax[1], 0.001);
    }
  }

  // --- IntColumnReader sum with nulls in zero copy ---

  @Test public void testIntSumZeroCopyWithNulls() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(4);
      vec.set(0, 10);
      vec.setNull(1);
      vec.set(2, 30);
      vec.setNull(3);
      vec.setValueCount(4);
      root.setRowCount(4);

      ColumnBatch batch = new ColumnBatch(root);
      ColumnBatch.IntColumnReader reader = batch.getIntColumn(0);

      assertEquals(40, reader.sumZeroCopy());
      assertEquals(40, reader.sum());
    }
  }
}
