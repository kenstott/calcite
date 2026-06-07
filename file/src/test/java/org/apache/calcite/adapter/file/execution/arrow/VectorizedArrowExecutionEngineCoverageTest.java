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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for {@link VectorizedArrowExecutionEngine}.
 *
 * <p>Exercises all public methods (project, filter, aggregateSum,
 * aggregateMinMax, filterWithColumnBatch, aggregateSumWithColumnBatch)
 * using real Apache Arrow VectorSchemaRoot objects with test data.
 */
@Tag("unit")
public class VectorizedArrowExecutionEngineCoverageTest {

  private BufferAllocator allocator;
  /**
   * The engine's internal allocator, obtained via reflection.
   * Needed for project() tests because makeTransferPair().transfer()
   * requires both allocators to share the same root.
   */
  private BufferAllocator engineAllocator;
  private final List<VectorSchemaRoot> openRoots = new ArrayList<>();

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      java.lang.reflect.Field f =
          VectorizedArrowExecutionEngine.class.getDeclaredField("ALLOCATOR");
      f.setAccessible(true);
      engineAllocator = (BufferAllocator) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Cannot access engine ALLOCATOR", e);
    }
  }

  @AfterEach
  public void tearDown() {
    for (VectorSchemaRoot root : openRoots) {
      try {
        root.close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
    }
    openRoots.clear();
    allocator.close();
  }

  /**
   * Track a VectorSchemaRoot for cleanup in tearDown.
   */
  private void track(VectorSchemaRoot root) {
    openRoots.add(root);
  }

  // ---------------------------------------------------------------
  // Helper: create a multi-type schema (int, varchar, float8, bigint, bit)
  // ---------------------------------------------------------------

  private Schema createMixedSchema() {
    return new Schema(
        Arrays.asList(
        Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", ArrowType.Utf8.INSTANCE),
        Field.nullable("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("quantity", new ArrowType.Int(64, true)),
        Field.nullable("active", new ArrowType.Bool())));
  }

  /**
   * Populates a mixed-schema root with test data using the test allocator.
   */
  private VectorSchemaRoot createMixedRoot(int rows) {
    return createMixedRootWith(allocator, rows);
  }

  /**
   * Populates a mixed-schema root using the engine's allocator, needed for
   * project() tests where makeTransferPair().transfer() requires both
   * allocators to share the same root.
   */
  private VectorSchemaRoot createMixedRootForProject(int rows) {
    return createMixedRootWith(engineAllocator, rows);
  }

  /**
   * Populates a mixed-schema root with test data. Row layout:
   * <pre>
   *   row 0: 1, "Alice",   10.5, 100L, true
   *   row 1: 2, "Bob",     20.3, 200L, false
   *   row 2: 3, "Charlie", 30.1, 300L, true
   *   row 3: 4, "Diana",   40.7, 400L, true
   *   row 4: 5, "Eve",     50.0, 500L, false
   * </pre>
   */
  private VectorSchemaRoot createMixedRootWith(BufferAllocator alloc, int rows) {
    VectorSchemaRoot root = VectorSchemaRoot.create(createMixedSchema(), alloc);
    root.allocateNew();

    IntVector idVec = (IntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    Float8Vector priceVec = (Float8Vector) root.getVector("price");
    BigIntVector qtyVec = (BigIntVector) root.getVector("quantity");
    BitVector activeVec = (BitVector) root.getVector("active");

    String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
    double[] prices = {10.5, 20.3, 30.1, 40.7, 50.0};
    long[] quantities = {100L, 200L, 300L, 400L, 500L};
    int[] actives = {1, 0, 1, 1, 0};

    for (int i = 0; i < rows; i++) {
      idVec.set(i, i + 1);
      nameVec.set(i, names[i].getBytes(StandardCharsets.UTF_8));
      priceVec.set(i, prices[i]);
      qtyVec.set(i, quantities[i]);
      activeVec.set(i, actives[i]);
    }

    idVec.setValueCount(rows);
    nameVec.setValueCount(rows);
    priceVec.setValueCount(rows);
    qtyVec.setValueCount(rows);
    activeVec.setValueCount(rows);
    root.setRowCount(rows);

    return root;
  }

  /**
   * Creates a simple int-only root (single column "val").
   */
  private VectorSchemaRoot createIntRoot(int... values) {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector vec = (IntVector) root.getVector("val");
    for (int i = 0; i < values.length; i++) {
      vec.set(i, values[i]);
    }
    vec.setValueCount(values.length);
    root.setRowCount(values.length);
    return root;
  }

  /**
   * Creates a simple double-only root (single column "val").
   */
  private VectorSchemaRoot createDoubleRoot(double... values) {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("val",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    Float8Vector vec = (Float8Vector) root.getVector("val");
    for (int i = 0; i < values.length; i++) {
      vec.set(i, values[i]);
    }
    vec.setValueCount(values.length);
    root.setRowCount(values.length);
    return root;
  }

  /**
   * Creates a simple bigint-only root (single column "val").
   */
  private VectorSchemaRoot createBigIntRoot(long... values) {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(64, true))));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    BigIntVector vec = (BigIntVector) root.getVector("val");
    for (int i = 0; i < values.length; i++) {
      vec.set(i, values[i]);
    }
    vec.setValueCount(values.length);
    root.setRowCount(values.length);
    return root;
  }

  // ===================================================================
  //  project() tests
  // ===================================================================

  @Test public void testProjectSingleColumn() {
    VectorSchemaRoot input = createMixedRootForProject(5);
    track(input);

    VectorSchemaRoot result = VectorizedArrowExecutionEngine.project(input, new int[]{0});
    track(result);

    assertEquals(1, result.getSchema().getFields().size());
    assertEquals("id", result.getSchema().getFields().get(0).getName());
    assertEquals(5, result.getRowCount());
    assertEquals(1, ((IntVector) result.getVector(0)).get(0));
    assertEquals(5, ((IntVector) result.getVector(0)).get(4));
  }

  @Test public void testProjectMultipleColumns() {
    VectorSchemaRoot input = createMixedRootForProject(5);
    track(input);

    // project columns: name (1), price (2)
    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{1, 2});
    track(result);

    assertEquals(2, result.getSchema().getFields().size());
    assertEquals("name", result.getSchema().getFields().get(0).getName());
    assertEquals("price", result.getSchema().getFields().get(1).getName());
    assertEquals(5, result.getRowCount());

    VarCharVector nameVec = (VarCharVector) result.getVector(0);
    assertEquals("Alice",
        new String(nameVec.get(0), StandardCharsets.UTF_8));
    assertEquals("Eve",
        new String(nameVec.get(4), StandardCharsets.UTF_8));

    Float8Vector priceVec = (Float8Vector) result.getVector(1);
    assertEquals(10.5, priceVec.get(0), 0.001);
    assertEquals(50.0, priceVec.get(4), 0.001);
  }

  @Test public void testProjectReorderedColumns() {
    VectorSchemaRoot input = createMixedRootForProject(3);
    track(input);

    // Reverse: price (2), id (0)
    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{2, 0});
    track(result);

    assertEquals(2, result.getSchema().getFields().size());
    assertEquals("price", result.getSchema().getFields().get(0).getName());
    assertEquals("id", result.getSchema().getFields().get(1).getName());
    assertEquals(3, result.getRowCount());

    Float8Vector priceVec = (Float8Vector) result.getVector(0);
    assertEquals(10.5, priceVec.get(0), 0.001);
  }

  @Test public void testProjectAllColumns() {
    VectorSchemaRoot input = createMixedRootForProject(3);
    track(input);

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{0, 1, 2, 3, 4});
    track(result);

    assertEquals(5, result.getSchema().getFields().size());
    assertEquals(3, result.getRowCount());
  }

  @Test public void testProjectEmptyBatch() {
    VectorSchemaRoot input = createMixedRootForProject(0);
    track(input);

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{0, 2});
    track(result);

    assertEquals(2, result.getSchema().getFields().size());
    assertEquals(0, result.getRowCount());
  }

  @Test public void testProjectSingleRow() {
    VectorSchemaRoot input = createMixedRootForProject(1);
    track(input);

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{0, 1});
    track(result);

    assertEquals(1, result.getRowCount());
    assertEquals("id", result.getSchema().getFields().get(0).getName());
  }

  // ===================================================================
  //  filter() tests
  // ===================================================================

  @Test public void testFilterIntColumnGreaterThan() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter id > 3
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 3;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(2, result.getRowCount());
    IntVector idVec = (IntVector) result.getVector("id");
    assertEquals(4, idVec.get(0));
    assertEquals(5, idVec.get(1));
  }

  @Test public void testFilterStringColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter names starting with "A" or "C"
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        if (value == null) {
          return false;
        }
        String s = value.toString();
        return s.startsWith("A") || s.startsWith("C");
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 1, predicate);
    track(result);

    assertEquals(2, result.getRowCount());
    // Should have Alice and Charlie
    VarCharVector nameVec = (VarCharVector) result.getVector("name");
    assertEquals("Alice",
        new String(nameVec.get(0), StandardCharsets.UTF_8));
    assertEquals("Charlie",
        new String(nameVec.get(1), StandardCharsets.UTF_8));
  }

  @Test public void testFilterDoubleColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter price >= 30.0
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Double) value) >= 30.0;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 2, predicate);
    track(result);

    assertEquals(3, result.getRowCount());
    Float8Vector priceVec = (Float8Vector) result.getVector("price");
    assertEquals(30.1, priceVec.get(0), 0.001);
    assertEquals(40.7, priceVec.get(1), 0.001);
    assertEquals(50.0, priceVec.get(2), 0.001);
  }

  @Test public void testFilterNoMatchingRows() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter id > 100  (nothing matches)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 100;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(0, result.getRowCount());
  }

  @Test public void testFilterAllRowsMatch() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter id > 0  (all match)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 0;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(5, result.getRowCount());
  }

  @Test public void testFilterEmptyBatch() {
    VectorSchemaRoot input = createMixedRoot(0);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return true;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(0, result.getRowCount());
  }

  @Test public void testFilterSingleRow() {
    VectorSchemaRoot input = createMixedRoot(1);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) == 1;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(1, result.getRowCount());
  }

  @Test public void testFilterPreservesAllColumns() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter on id == 3
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) == 3;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(1, result.getRowCount());
    assertEquals(5, result.getSchema().getFields().size());

    IntVector idVec = (IntVector) result.getVector("id");
    assertEquals(3, idVec.get(0));

    VarCharVector nameVec = (VarCharVector) result.getVector("name");
    assertEquals("Charlie",
        new String(nameVec.get(0), StandardCharsets.UTF_8));

    Float8Vector priceVec = (Float8Vector) result.getVector("price");
    assertEquals(30.1, priceVec.get(0), 0.001);

    BigIntVector qtyVec = (BigIntVector) result.getVector("quantity");
    assertEquals(300L, qtyVec.get(0));

    BitVector activeVec = (BitVector) result.getVector("active");
    assertEquals(1, activeVec.get(0));
  }

  @Test public void testFilterWithNullValues() {
    Schema schema =
        new Schema(
            Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true)),
        Field.nullable("label", ArrowType.Utf8.INSTANCE)));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    IntVector valVec = (IntVector) input.getVector("val");
    VarCharVector labelVec = (VarCharVector) input.getVector("label");

    valVec.set(0, 10);
    valVec.setNull(1);
    valVec.set(2, 30);
    valVec.setNull(3);
    valVec.set(4, 50);

    labelVec.set(0, "a".getBytes(StandardCharsets.UTF_8));
    labelVec.set(1, "b".getBytes(StandardCharsets.UTF_8));
    labelVec.set(2, "c".getBytes(StandardCharsets.UTF_8));
    labelVec.set(3, "d".getBytes(StandardCharsets.UTF_8));
    labelVec.set(4, "e".getBytes(StandardCharsets.UTF_8));

    valVec.setValueCount(5);
    labelVec.setValueCount(5);
    input.setRowCount(5);

    // The predicate receives null for null slots from getObject()
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) >= 30;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(2, result.getRowCount());
    IntVector resultVal = (IntVector) result.getVector("val");
    assertEquals(30, resultVal.get(0));
    assertEquals(50, resultVal.get(1));
  }

  @Test public void testFilterMoreThanEightRows() {
    // The filter processes in chunks of 8, so test with > 8 rows
    // to exercise the chunked loop boundary
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    IntVector vec = (IntVector) input.getVector("val");
    for (int i = 0; i < 20; i++) {
      vec.set(i, i);
    }
    vec.setValueCount(20);
    input.setRowCount(20);

    // Keep only even values
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) % 2 == 0;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(10, result.getRowCount());
    IntVector resultVec = (IntVector) result.getVector("val");
    for (int i = 0; i < 10; i++) {
      assertEquals(i * 2, resultVec.get(i));
    }
  }

  // ===================================================================
  //  aggregateSum() tests
  // ===================================================================

  @Test public void testAggregateSumFloat8() {
    VectorSchemaRoot input = createDoubleRoot(10.0, 20.0, 30.0);
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(60.0, sum, 0.001);
  }

  @Test public void testAggregateSumFloat8WithNulls() {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("val",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    Float8Vector vec = (Float8Vector) input.getVector("val");
    vec.set(0, 10.0);
    vec.setNull(1);
    vec.set(2, 30.0);
    vec.setNull(3);
    vec.setValueCount(4);
    input.setRowCount(4);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(40.0, sum, 0.001);
  }

  @Test public void testAggregateSumInt() {
    VectorSchemaRoot input = createIntRoot(10, 20, 30, 40, 50);
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(150.0, sum, 0.001);
  }

  @Test public void testAggregateSumIntWithNulls() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    IntVector vec = (IntVector) input.getVector("val");
    vec.set(0, 5);
    vec.setNull(1);
    vec.set(2, 15);
    vec.setValueCount(3);
    input.setRowCount(3);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(20.0, sum, 0.001);
  }

  @Test public void testAggregateSumBigInt() {
    VectorSchemaRoot input = createBigIntRoot(1000000000L, 2000000000L, 3000000000L);
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(6000000000.0, sum, 0.001);
  }

  @Test public void testAggregateSumBigIntWithNulls() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(64, true))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    BigIntVector vec = (BigIntVector) input.getVector("val");
    vec.set(0, 100L);
    vec.setNull(1);
    vec.set(2, 300L);
    vec.setValueCount(3);
    input.setRowCount(3);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(400.0, sum, 0.001);
  }

  @Test public void testAggregateSumEmptyBatch() {
    VectorSchemaRoot input = createDoubleRoot();
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(0.0, sum, 0.001);
  }

  @Test public void testAggregateSumSingleValue() {
    VectorSchemaRoot input = createDoubleRoot(42.5);
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(42.5, sum, 0.001);
  }

  @Test public void testAggregateSumMoreThanEightRows() {
    // Exercise the chunked processing (chunks of 8)
    double[] values = new double[20];
    double expected = 0.0;
    for (int i = 0; i < 20; i++) {
      values[i] = (i + 1) * 1.0;
      expected += values[i];
    }

    VectorSchemaRoot input = createDoubleRoot(values);
    track(input);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(expected, sum, 0.001);
  }

  @Test public void testAggregateSumUnsupportedType() {
    // VarChar column -- aggregateSum returns 0.0 for unsupported types
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    VarCharVector vec = (VarCharVector) input.getVector("name");
    vec.set(0, "abc".getBytes(StandardCharsets.UTF_8));
    vec.setValueCount(1);
    input.setRowCount(1);

    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(0.0, sum, 0.001);
  }

  // ===================================================================
  //  aggregateMinMax() tests
  // ===================================================================

  @Test public void testAggregateMinMaxFloat8() {
    VectorSchemaRoot input = createDoubleRoot(15.0, 3.0, 42.0, 8.0);
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(3.0, minMax[0], 0.001);
    assertEquals(42.0, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxFloat8WithNulls() {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("val",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    Float8Vector vec = (Float8Vector) input.getVector("val");
    vec.set(0, 15.0);
    vec.setNull(1);
    vec.set(2, 42.0);
    vec.setNull(3);
    vec.set(4, 8.0);
    vec.setValueCount(5);
    input.setRowCount(5);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(8.0, minMax[0], 0.001);
    assertEquals(42.0, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxInt() {
    VectorSchemaRoot input = createIntRoot(50, 10, 30, 20, 40);
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(10.0, minMax[0], 0.001);
    assertEquals(50.0, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxIntWithNulls() {
    Schema schema =
        new Schema(Arrays.asList(Field.nullable("val", new ArrowType.Int(32, true))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    IntVector vec = (IntVector) input.getVector("val");
    vec.setNull(0);
    vec.set(1, 7);
    vec.setNull(2);
    vec.set(3, 99);
    vec.setValueCount(4);
    input.setRowCount(4);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(7.0, minMax[0], 0.001);
    assertEquals(99.0, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxSingleValue() {
    VectorSchemaRoot input = createDoubleRoot(25.5);
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(25.5, minMax[0], 0.001);
    assertEquals(25.5, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxEmptyBatch() {
    VectorSchemaRoot input = createDoubleRoot();
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    // No values -> returns {0, 0}
    assertArrayEquals(new double[]{0.0, 0.0}, minMax, 0.001);
  }

  @Test public void testAggregateMinMaxAllNulls() {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("val",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    Float8Vector vec = (Float8Vector) input.getVector("val");
    vec.setNull(0);
    vec.setNull(1);
    vec.setValueCount(2);
    input.setRowCount(2);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertArrayEquals(new double[]{0.0, 0.0}, minMax, 0.001);
  }

  @Test public void testAggregateMinMaxNegativeValues() {
    // Note: the source uses Double.MIN_VALUE (smallest positive double) as
    // the initial max, so max behaves correctly for non-negative values.
    // For all-negative inputs, max will be Double.MIN_VALUE due to this.
    // This test documents the current behavior.
    VectorSchemaRoot input = createDoubleRoot(-5.0, -100.0, -1.0, -50.0);
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertEquals(-100.0, minMax[0], 0.001);
    assertEquals(Double.MIN_VALUE, minMax[1], 0.001);
  }

  @Test public void testAggregateMinMaxUnsupportedType() {
    // BigInt is not handled by aggregateMinMax -> returns {0, 0}
    VectorSchemaRoot input = createBigIntRoot(10L, 20L, 30L);
    track(input);

    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 0);
    assertArrayEquals(new double[]{0.0, 0.0}, minMax, 0.001);
  }

  // ===================================================================
  //  aggregateMinMax() on the mixed root via column index
  // ===================================================================

  @Test public void testAggregateMinMaxOnMixedRootPriceColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // price is column index 2
    double[] minMax = VectorizedArrowExecutionEngine.aggregateMinMax(input, 2);
    assertEquals(10.5, minMax[0], 0.001);
    assertEquals(50.0, minMax[1], 0.001);
  }

  // ===================================================================
  //  aggregateSum() on the mixed root via column index
  // ===================================================================

  @Test public void testAggregateSumOnMixedRootIntColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // id is column index 0: 1+2+3+4+5 = 15
    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 0);
    assertEquals(15.0, sum, 0.001);
  }

  @Test public void testAggregateSumOnMixedRootDoubleColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // price is column index 2: 10.5+20.3+30.1+40.7+50.0 = 151.6
    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 2);
    assertEquals(151.6, sum, 0.001);
  }

  @Test public void testAggregateSumOnMixedRootBigIntColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // quantity is column index 3: 100+200+300+400+500 = 1500
    double sum = VectorizedArrowExecutionEngine.aggregateSum(input, 3);
    assertEquals(1500.0, sum, 0.001);
  }

  // ===================================================================
  //  filterWithColumnBatch() tests
  // ===================================================================

  @Test public void testFilterWithColumnBatchIntColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter id > 3  via ColumnBatch path
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 3;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 0, predicate);
    track(result);

    assertEquals(2, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchDoubleColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter price >= 30.0 via ColumnBatch path
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Double) value) >= 30.0;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 2, predicate);
    track(result);

    assertEquals(3, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchStringColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter names containing "li" (Alice, Charlie)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && value.toString().contains("li");
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 1, predicate);
    track(result);

    assertEquals(2, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchBooleanColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter active == true (rows 0, 2, 3)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && Boolean.TRUE.equals(value);
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 4, predicate);
    track(result);

    assertEquals(3, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchEmptyBatch() {
    VectorSchemaRoot input = createMixedRoot(0);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return true;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 0, predicate);
    track(result);

    assertEquals(0, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchNoMatch() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Nothing matches
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 100;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 0, predicate);
    track(result);

    assertEquals(0, result.getRowCount());
  }

  @Test public void testFilterWithColumnBatchAllMatch() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 0;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 0, predicate);
    track(result);

    assertEquals(5, result.getRowCount());
  }

  // ===================================================================
  //  aggregateSumWithColumnBatch() tests
  // ===================================================================

  @Test public void testAggregateSumWithColumnBatchInt() {
    VectorSchemaRoot input = createIntRoot(10, 20, 30, 40, 50);
    track(input);

    double sum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(150.0, sum, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchDouble() {
    VectorSchemaRoot input = createDoubleRoot(1.5, 2.5, 3.5);
    track(input);

    double sum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(7.5, sum, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchBigInt() {
    VectorSchemaRoot input = createBigIntRoot(100L, 200L, 300L);
    track(input);

    double sum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(600.0, sum, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchEmptyBatch() {
    VectorSchemaRoot input = createIntRoot();
    track(input);

    double sum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(0.0, sum, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchSingleValue() {
    VectorSchemaRoot input = createIntRoot(42);
    track(input);

    double sum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(42.0, sum, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchOnMixedRootIntColumn() {
    // Each call to aggregateSumWithColumnBatch creates a ColumnBatch which
    // closes the VectorSchemaRoot, so we need a fresh root per call.
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Sum of id column: 1+2+3+4+5 = 15
    double sumInt =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 0);
    assertEquals(15.0, sumInt, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchOnMixedRootDoubleColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Sum of price column: 10.5+20.3+30.1+40.7+50.0 = 151.6
    double sumDouble =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 2);
    assertEquals(151.6, sumDouble, 0.001);
  }

  @Test public void testAggregateSumWithColumnBatchOnMixedRootLongColumn() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Sum of quantity column: 100+200+300+400+500 = 1500
    double sumLong =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input, 3);
    assertEquals(1500.0, sumLong, 0.001);
  }

  // ===================================================================
  //  Combined / integration-style scenarios
  // ===================================================================

  @Test public void testProjectThenFilter() {
    VectorSchemaRoot input = createMixedRootForProject(5);
    track(input);

    // First project to just id and price
    VectorSchemaRoot projected =
        VectorizedArrowExecutionEngine.project(input, new int[]{0, 2});
    track(projected);

    assertEquals(2, projected.getSchema().getFields().size());

    // Then filter on price (now column index 1 in projected result) >= 30
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Double) value) >= 30.0;
      }
    };

    VectorSchemaRoot filtered =
        VectorizedArrowExecutionEngine.filter(projected, 1, predicate);
    track(filtered);

    assertEquals(3, filtered.getRowCount());
    assertEquals(2, filtered.getSchema().getFields().size());
  }

  @Test public void testFilterThenAggregateSum() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter to id > 2 (rows 3, 4, 5)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) > 2;
      }
    };

    VectorSchemaRoot filtered =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(filtered);

    assertEquals(3, filtered.getRowCount());

    // Sum price column (index 2): 30.1 + 40.7 + 50.0 = 120.8
    double sum = VectorizedArrowExecutionEngine.aggregateSum(filtered, 2);
    assertEquals(120.8, sum, 0.001);
  }

  @Test public void testFilterThenAggregateMinMax() {
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Filter to id <= 3
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) <= 3;
      }
    };

    VectorSchemaRoot filtered =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(filtered);

    assertEquals(3, filtered.getRowCount());

    // MinMax on price (index 2): {10.5, 30.1}
    double[] minMax =
        VectorizedArrowExecutionEngine.aggregateMinMax(filtered, 2);
    assertEquals(10.5, minMax[0], 0.001);
    assertEquals(30.1, minMax[1], 0.001);
  }

  // ===================================================================
  //  Consistency tests: standard vs ColumnBatch paths
  // ===================================================================

  @Test public void testFilterConsistencyStandardVsColumnBatch() {
    VectorSchemaRoot input1 = createMixedRoot(5);
    track(input1);
    VectorSchemaRoot input2 = createMixedRoot(5);
    track(input2);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) >= 2;
      }
    };

    VectorSchemaRoot standardResult =
        VectorizedArrowExecutionEngine.filter(input1, 0, predicate);
    track(standardResult);

    VectorSchemaRoot batchResult =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input2, 0, predicate);
    track(batchResult);

    assertEquals(standardResult.getRowCount(), batchResult.getRowCount());
  }

  @Test public void testSumConsistencyStandardVsColumnBatch() {
    VectorSchemaRoot input1 = createIntRoot(10, 20, 30, 40, 50);
    track(input1);
    VectorSchemaRoot input2 = createIntRoot(10, 20, 30, 40, 50);
    track(input2);

    double standardSum =
        VectorizedArrowExecutionEngine.aggregateSum(input1, 0);
    double batchSum =
        VectorizedArrowExecutionEngine.aggregateSumWithColumnBatch(input2, 0);

    assertEquals(standardSum, batchSum, 0.001);
  }

  // ===================================================================
  //  copyValue coverage via filter (exercises all vector types)
  // ===================================================================

  @Test public void testCopyValueCoversAllTypes() {
    // The private copyValue method is exercised via filter.
    // This test explicitly filters the mixed-schema root to ensure
    // every vector type (Int, VarChar, Float8, BigInt, Bit) gets copied.
    VectorSchemaRoot input = createMixedRoot(5);
    track(input);

    // Pick one row (id == 3)
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value != null && ((Integer) value) == 3;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(1, result.getRowCount());

    // Int
    IntVector idVec = (IntVector) result.getVector("id");
    assertEquals(3, idVec.get(0));

    // VarChar
    VarCharVector nameVec = (VarCharVector) result.getVector("name");
    assertEquals("Charlie",
        new String(nameVec.get(0), StandardCharsets.UTF_8));

    // Float8
    Float8Vector priceVec = (Float8Vector) result.getVector("price");
    assertEquals(30.1, priceVec.get(0), 0.001);

    // BigInt
    BigIntVector qtyVec = (BigIntVector) result.getVector("quantity");
    assertEquals(300L, qtyVec.get(0));

    // Bit
    BitVector activeVec = (BitVector) result.getVector("active");
    assertEquals(1, activeVec.get(0));
  }

  @Test public void testCopyValueHandlesNullSlots() {
    // Ensures copyValue's null path is exercised
    Schema schema =
        new Schema(
            Arrays.asList(Field.nullable("id", new ArrowType.Int(32, true)),
        Field.nullable("name", ArrowType.Utf8.INSTANCE),
        Field.nullable("val",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("big", new ArrowType.Int(64, true)),
        Field.nullable("flag", new ArrowType.Bool())));
    VectorSchemaRoot input = VectorSchemaRoot.create(schema, allocator);
    input.allocateNew();
    track(input);

    IntVector idVec = (IntVector) input.getVector("id");
    VarCharVector nameVec = (VarCharVector) input.getVector("name");
    Float8Vector valVec = (Float8Vector) input.getVector("val");
    BigIntVector bigVec = (BigIntVector) input.getVector("big");
    BitVector flagVec = (BitVector) input.getVector("flag");

    // Row 0: all set
    idVec.set(0, 1);
    nameVec.set(0, "x".getBytes(StandardCharsets.UTF_8));
    valVec.set(0, 1.0);
    bigVec.set(0, 10L);
    flagVec.set(0, 1);

    // Row 1: all null
    idVec.setNull(1);
    nameVec.setNull(1);
    valVec.setNull(1);
    bigVec.setNull(1);
    flagVec.setNull(1);

    idVec.setValueCount(2);
    nameVec.setValueCount(2);
    valVec.setValueCount(2);
    bigVec.setValueCount(2);
    flagVec.setValueCount(2);
    input.setRowCount(2);

    // Filter to keep only row 1 (all nulls) -- using id column
    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return value == null;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertEquals(1, result.getRowCount());
    assertTrue(result.getVector("id").isNull(0));
    assertTrue(result.getVector("name").isNull(0));
    assertTrue(result.getVector("val").isNull(0));
    assertTrue(result.getVector("big").isNull(0));
    assertTrue(result.getVector("flag").isNull(0));
  }

  // ===================================================================
  //  Result not null checks (defensive)
  // ===================================================================

  @Test public void testProjectResultNotNull() {
    VectorSchemaRoot input = createMixedRootForProject(3);
    track(input);

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.project(input, new int[]{0});
    track(result);

    assertNotNull(result);
    assertNotNull(result.getSchema());
  }

  @Test public void testFilterResultNotNull() {
    VectorSchemaRoot input = createMixedRoot(3);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return true;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filter(input, 0, predicate);
    track(result);

    assertNotNull(result);
  }

  @Test public void testFilterWithColumnBatchResultNotNull() {
    VectorSchemaRoot input = createMixedRoot(3);
    track(input);

    Predicate<Object> predicate = new Predicate<Object>() {
      @Override public boolean test(Object value) {
        return true;
      }
    };

    VectorSchemaRoot result =
        VectorizedArrowExecutionEngine.filterWithColumnBatch(input, 0, predicate);
    track(result);

    assertNotNull(result);
  }
}
