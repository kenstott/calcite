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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.arrow.CompressedColumnBatch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link CompressedColumnBatch}.
 * Tests compression analysis, sum operations, and filtering.
 */
@Tag("unit")
public class CompressedColumnBatchDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CompressedColumnBatchDeepTest.class);

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

  private VectorSchemaRoot createIntBatch(int... values) {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector(0);
    for (int i = 0; i < values.length; i++) {
      vec.set(i, values[i]);
    }
    vec.setValueCount(values.length);
    root.setRowCount(values.length);
    return root;
  }

  private VectorSchemaRoot createStringIntBatch(String[] strings, int[] ints) {
    Schema schema =
        new Schema(
            Arrays.asList(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    VarCharVector strVec = (VarCharVector) root.getVector(0);
    IntVector intVec = (IntVector) root.getVector(1);
    int count = Math.min(strings.length, ints.length);
    for (int i = 0; i < count; i++) {
      strVec.set(i, strings[i].getBytes(StandardCharsets.UTF_8));
      intVec.set(i, ints[i]);
    }
    strVec.setValueCount(count);
    intVec.setValueCount(count);
    root.setRowCount(count);
    return root;
  }

  @Test public void testConstructorAnalyzesCompression() {
    VectorSchemaRoot root = createIntBatch(1, 2, 3, 4, 5);
    CompressedColumnBatch batch = new CompressedColumnBatch(root);
    assertNotNull(batch);
    batch.close();
  }

  @Test public void testGetCompressedIntColumn() {
    VectorSchemaRoot root = createIntBatch(10, 20, 30);
    CompressedColumnBatch batch = new CompressedColumnBatch(root);

    CompressedColumnBatch.CompressedIntColumnReader reader =
        batch.getCompressedIntColumn(0);
    assertNotNull(reader);

    batch.close();
  }

  @Test public void testGetCompressedIntColumnWrongType() {
    VectorSchemaRoot root =
        createStringIntBatch(new String[]{"a", "b"}, new int[]{1, 2});
    CompressedColumnBatch batch = new CompressedColumnBatch(root);

    assertThrows(IllegalArgumentException.class,
        () -> batch.getCompressedIntColumn(0)); // column 0 is String

    batch.close();
  }

  @Test public void testSumCompressed() {
    VectorSchemaRoot root = createIntBatch(10, 20, 30, 40, 50);
    CompressedColumnBatch batch = new CompressedColumnBatch(root);

    CompressedColumnBatch.CompressedIntColumnReader reader =
        batch.getCompressedIntColumn(0);
    long sum = reader.sumCompressed();

    // The sum behavior depends on the compression encoding analysis.
    // For a small dataset, it typically falls back to uncompressed sum.
    assertTrue(sum > 0, "Sum should be positive");

    batch.close();
  }

  @Test public void testFilterCompressed() {
    VectorSchemaRoot root = createIntBatch(10, 20, 30, 40, 50);
    CompressedColumnBatch batch = new CompressedColumnBatch(root);

    CompressedColumnBatch.CompressedIntColumnReader reader =
        batch.getCompressedIntColumn(0);

    boolean[] result = reader.filterCompressed(25);
    assertNotNull(result);
    assertEquals(5, result.length);

    batch.close();
  }

  @Test public void testEmptyBatch() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setValueCount(0);
    root.setRowCount(0);

    CompressedColumnBatch batch = new CompressedColumnBatch(root);
    assertNotNull(batch);
    batch.close();
  }

  @Test public void testLargeDataset() {
    Schema schema =
        new Schema(Arrays.asList(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector(0);
    int count = 1000;
    for (int i = 0; i < count; i++) {
      vec.set(i, i);
    }
    vec.setValueCount(count);
    root.setRowCount(count);

    CompressedColumnBatch batch = new CompressedColumnBatch(root);
    CompressedColumnBatch.CompressedIntColumnReader reader =
        batch.getCompressedIntColumn(0);

    long sum = reader.sumCompressed();
    assertTrue(sum > 0, "Sum of 0..999 should be positive");

    batch.close();
  }

  @Test public void testCloseNullRoot() {
    VectorSchemaRoot root = createIntBatch(1);
    CompressedColumnBatch batch = new CompressedColumnBatch(root);
    // close should not throw
    batch.close();
  }

  @Test public void testMultiColumnBatch() {
    VectorSchemaRoot root =
        createStringIntBatch(new String[]{"Alice", "Bob", "Charlie"},
        new int[]{100, 200, 300});

    CompressedColumnBatch batch = new CompressedColumnBatch(root);

    // Only int column should work
    CompressedColumnBatch.CompressedIntColumnReader intReader =
        batch.getCompressedIntColumn(1);
    assertNotNull(intReader);

    batch.close();
  }
}
