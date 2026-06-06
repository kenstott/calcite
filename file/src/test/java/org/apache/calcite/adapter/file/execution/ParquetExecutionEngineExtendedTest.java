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

import org.apache.calcite.adapter.file.execution.parquet.ParquetExecutionEngine;
import org.apache.calcite.adapter.file.execution.parquet.ParquetExecutionEngine.InMemoryParquetData;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Extended tests for {@link ParquetExecutionEngine}.
 * Complements the existing ParquetExecutionEngineTest with additional
 * edge cases and conversion tests.
 */
@Tag("unit")
public class ParquetExecutionEngineExtendedTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetExecutionEngineExtendedTest.class);

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

  @Test public void testConvertToParquetWithSingleColumn() {
    VectorSchemaRoot batch = createSingleDoubleBatch(3);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      assertNotNull(data);
      assertNotNull(data.getData());
      assertNotNull(data.getSchema());
      assertEquals(1, data.getSchema().getFieldCount());
      assertNotNull(data.getStringTable());
    } finally {
      batch.close();
    }
  }

  @Test public void testConvertToParquetPreservesStringTable() {
    VectorSchemaRoot batch = createSingleDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      assertNotNull(data.getStringTable());
      // Numeric-only data shouldn't need string table entries
      LOGGER.debug("String table size: {}", data.getStringTable().size());
    } finally {
      batch.close();
    }
  }

  @Test public void testConvertToParquetWithEmptyBatch() {
    VectorSchemaRoot batch = createSingleDoubleBatch(0);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      assertNotNull(data);
      // Read the row count from buffer
      data.getData().rewind();
      int rowCount = data.getData().getInt();
      assertEquals(0, rowCount);
    } finally {
      batch.close();
    }
  }

  @Test public void testToArrowConvertsBack() {
    VectorSchemaRoot originalBatch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(originalBatch);

      VectorSchemaRoot arrowBatch = ParquetExecutionEngine.toArrow(data);
      assertNotNull(arrowBatch);
      assertEquals(5, arrowBatch.getRowCount());
      arrowBatch.close();
    } finally {
      originalBatch.close();
    }
  }

  @Test public void testAggregateSumOnFirstColumn() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      // Column 0: 1.0, 2.0, 3.0, 4.0, 5.0
      double sum = ParquetExecutionEngine.aggregateSum(data, 0);
      assertEquals(15.0, sum, 0.01);
    } finally {
      batch.close();
    }
  }

  @Test public void testAggregateSumOnSecondColumn() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      // Column 1: 50.0, 60.0, 70.0, 80.0, 90.0
      double sum = ParquetExecutionEngine.aggregateSum(data, 1);
      assertEquals(350.0, sum, 0.01);
    } finally {
      batch.close();
    }
  }

  @Test public void testFilterAllRows() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      // Filter on column 0 where value > 0 (all rows should pass)
      InMemoryParquetData filtered =
          ParquetExecutionEngine.filter(data, 0,
              new java.util.function.Predicate<Object>() {
                @Override public boolean test(Object value) {
                  return value instanceof Number
                      && ((Number) value).doubleValue() > 0;
                }
              });
      assertNotNull(filtered);
      filtered.getData().rewind();
      int rowCount = filtered.getData().getInt();
      assertEquals(5, rowCount, "All 5 rows should pass filter > 0");
    } finally {
      batch.close();
    }
  }

  @Test public void testFilterNoRows() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      // Filter on column 0 where value > 100 (no rows should pass)
      InMemoryParquetData filtered =
          ParquetExecutionEngine.filter(data, 0,
              new java.util.function.Predicate<Object>() {
                @Override public boolean test(Object value) {
                  return value instanceof Number
                      && ((Number) value).doubleValue() > 100;
                }
              });
      assertNotNull(filtered);
      filtered.getData().rewind();
      int rowCount = filtered.getData().getInt();
      assertEquals(0, rowCount, "No rows should pass filter > 100");
    } finally {
      batch.close();
    }
  }

  @Test public void testGetMemoryUsagePositive() {
    VectorSchemaRoot batch = createMultiDoubleBatch(10);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      long memoryUsage = ParquetExecutionEngine.getMemoryUsage(data);
      assertTrue(memoryUsage > 0, "Memory usage should be positive");
      LOGGER.debug("Memory usage for 10 rows: {} bytes", memoryUsage);
    } finally {
      batch.close();
    }
  }

  @Test public void testIsStreamableSmallData() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      boolean streamable = ParquetExecutionEngine.isStreamable(data);
      assertFalse(streamable,
          "Small data should not be streamable (< 1MB)");
    } finally {
      batch.close();
    }
  }

  @Test public void testProjectSingleColumn() {
    VectorSchemaRoot batch = createMultiDoubleBatch(5);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      InMemoryParquetData projected =
          ParquetExecutionEngine.project(data, new int[]{0});
      assertNotNull(projected);
      assertEquals(1, projected.getSchema().getFieldCount());
    } finally {
      batch.close();
    }
  }

  @Test public void testInMemoryParquetDataGetters() {
    VectorSchemaRoot batch = createMultiDoubleBatch(3);
    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      assertNotNull(data.getData());
      assertNotNull(data.getSchema());
      assertNotNull(data.getStringTable());
      // Metadata is null in simplified implementation
      assertEquals(3, data.getSchema().getFieldCount());
    } finally {
      batch.close();
    }
  }

  @Test public void testAggregateSumWithNulls() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("value",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null)));
    VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator);
    batch.allocateNew();
    Float8Vector vector = (Float8Vector) batch.getVector("value");
    vector.set(0, 10.0);
    vector.setNull(1);
    vector.set(2, 20.0);
    vector.setNull(3);
    vector.set(4, 30.0);
    vector.setValueCount(5);
    batch.setRowCount(5);

    try {
      InMemoryParquetData data =
          ParquetExecutionEngine.convertToParquet(batch);
      // Null values stored as NaN in the simplified format
      // The sum should handle NaN values
      double sum = ParquetExecutionEngine.aggregateSum(data, 0);
      LOGGER.debug("Sum with nulls: {}", sum);
      // The simplified format stores nulls as NaN which are not Numbers
      // So they should be skipped in the sum
    } finally {
      batch.close();
    }
  }

  // --- Helper methods ---

  private VectorSchemaRoot createSingleDoubleBatch(int numRows) {
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
    for (int i = 0; i < numRows; i++) {
      vector.set(i, (i + 1) * 10.0);
    }
    vector.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }

  private VectorSchemaRoot createMultiDoubleBatch(int numRows) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null),
        new Field("score",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null),
        new Field("amount",
            FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    Float8Vector idV = (Float8Vector) root.getVector("id");
    Float8Vector scoreV = (Float8Vector) root.getVector("score");
    Float8Vector amountV = (Float8Vector) root.getVector("amount");
    for (int i = 0; i < numRows; i++) {
      idV.set(i, (double) (i + 1));
      scoreV.set(i, 50.0 + i * 10.0);
      amountV.set(i, 100.0 * (i + 1));
    }
    idV.setValueCount(numRows);
    scoreV.setValueCount(numRows);
    amountV.setValueCount(numRows);
    root.setRowCount(numRows);
    return root;
  }
}
