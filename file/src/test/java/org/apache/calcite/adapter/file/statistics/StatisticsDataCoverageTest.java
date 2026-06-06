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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for {@link ColumnStatistics} and {@link TableStatistics}.
 * Covers constructors, getters, selectivity calculations, range selectivity,
 * HLL integration, factory methods, hash validation, and toString formatting.
 */
@Tag("unit")
public class StatisticsDataCoverageTest {

  // ===================================================================
  // ColumnStatistics tests
  // ===================================================================

  @Nested
  @DisplayName("ColumnStatistics - Constructor and Getters")
  class ColumnStatsConstructorAndGetters {

    @Test
    @DisplayName("Constructor should store all fields correctly")
    void constructorStoresAllFields() {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      sketch.add("a");
      sketch.add("b");

      ColumnStatistics cs = new ColumnStatistics(
          "age", 1, 100, 5L, 1000L, sketch);

      assertEquals("age", cs.getColumnName());
      assertEquals(1, cs.getMinValue());
      assertEquals(100, cs.getMaxValue());
      assertEquals(5L, cs.getNullCount());
      assertEquals(1000L, cs.getTotalCount());
      assertNotNull(cs.getHllSketch());
    }

    @Test
    @DisplayName("Constructor should accept null min and max values")
    void constructorAcceptsNullMinMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 100L, null);
      assertNull(cs.getMinValue());
      assertNull(cs.getMaxValue());
    }

    @Test
    @DisplayName("Constructor should accept null HLL sketch")
    void constructorAcceptsNullSketch() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 0, 10, 0L, 50L, null);
      assertNull(cs.getHllSketch());
    }

    @Test
    @DisplayName("Constructor should accept string min/max values")
    void constructorAcceptsStringMinMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "name", "Alice", "Zara", 2L, 500L, null);
      assertEquals("Alice", cs.getMinValue());
      assertEquals("Zara", cs.getMaxValue());
    }

    @Test
    @DisplayName("Constructor should accept zero counts")
    void constructorAcceptsZeroCounts() {
      ColumnStatistics cs = new ColumnStatistics(
          "empty", null, null, 0L, 0L, null);
      assertEquals(0L, cs.getNullCount());
      assertEquals(0L, cs.getTotalCount());
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getDistinctCount
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getDistinctCount")
  class ColumnStatsDistinctCount {

    @Test
    @DisplayName("getDistinctCount with HLL sketch should use sketch estimate")
    void distinctCountWithHll() {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      for (int i = 0; i < 500; i++) {
        sketch.add("value_" + i);
      }
      long hllEstimate = sketch.getEstimate();

      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 10000L, sketch);

      assertEquals(hllEstimate, cs.getDistinctCount(),
          "Should use HLL estimate when sketch is present");
    }

    @Test
    @DisplayName("getDistinctCount without HLL should return min(1000, totalCount)")
    void distinctCountWithoutHll() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 5000L, null);
      assertEquals(1000L, cs.getDistinctCount(),
          "Without HLL and totalCount > 1000, should return 1000");
    }

    @Test
    @DisplayName("getDistinctCount without HLL and small totalCount should return totalCount")
    void distinctCountWithoutHllSmallTotal() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 50L, null);
      assertEquals(50L, cs.getDistinctCount(),
          "Without HLL and totalCount < 1000, should return totalCount");
    }

    @Test
    @DisplayName("getDistinctCount without HLL and exactly 1000 totalCount")
    void distinctCountWithoutHllExactly1000() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 1000L, null);
      assertEquals(1000L, cs.getDistinctCount(),
          "Without HLL and totalCount == 1000, should return 1000");
    }

    @Test
    @DisplayName("getDistinctCount without HLL and zero totalCount")
    void distinctCountWithoutHllZeroTotal() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 0L, null);
      assertEquals(0L, cs.getDistinctCount(),
          "Without HLL and totalCount == 0, should return 0");
    }

    @Test
    @DisplayName("getDistinctCount with precomputed HLL estimate")
    void distinctCountWithPrecomputedHll() {
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(42L);
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 10000L, sketch);
      assertEquals(42L, cs.getDistinctCount(),
          "Should use precomputed HLL estimate");
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getSelectivity: equality operators
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getSelectivity equality operators")
  class ColumnStatsSelectivityEquality {

    @Test
    @DisplayName("Equality selectivity should be 1/distinctCount")
    void equalitySelectivity() {
      // Use precomputed HLL with known estimate of 100
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(100L);
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 0L, 1000L, sketch);

      double sel = cs.getSelectivity("=", 50);
      assertEquals(1.0 / 100.0, sel, 0.0001,
          "Equality selectivity should be 1/distinctCount");
    }

    @Test
    @DisplayName("Not-equal selectivity should be 1 - 1/distinctCount")
    void notEqualSelectivity() {
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(100L);
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 0L, 1000L, sketch);

      double sel = cs.getSelectivity("!=", 50);
      assertEquals(1.0 - 1.0 / 100.0, sel, 0.0001,
          "Not-equal selectivity should be 1 - 1/distinctCount");
    }

    @Test
    @DisplayName("Equality selectivity with single distinct value")
    void equalitySingleDistinct() {
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(1L);
      ColumnStatistics cs = new ColumnStatistics(
          "col", 5, 5, 0L, 100L, sketch);

      double sel = cs.getSelectivity("=", 5);
      assertEquals(1.0, sel, 0.0001,
          "Single distinct value equality should be 1.0");
    }

    @Test
    @DisplayName("Not-equal selectivity with single distinct value")
    void notEqualSingleDistinct() {
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(1L);
      ColumnStatistics cs = new ColumnStatistics(
          "col", 5, 5, 0L, 100L, sketch);

      double sel = cs.getSelectivity("!=", 5);
      assertEquals(0.0, sel, 0.0001,
          "Single distinct value not-equal should be 0.0");
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getSelectivity: null handling
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getSelectivity null value handling")
  class ColumnStatsSelectivityNull {

    @Test
    @DisplayName("IS NULL with null value should return nullCount/totalCount")
    void isNullSelectivity() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 200L, 1000L, null);

      double sel = cs.getSelectivity("IS NULL", null);
      assertEquals(200.0 / 1000.0, sel, 0.0001,
          "IS NULL selectivity should be nullCount/totalCount");
    }

    @Test
    @DisplayName("IS NOT NULL with null value should return 1 - nullCount/totalCount")
    void isNotNullSelectivity() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 200L, 1000L, null);

      double sel = cs.getSelectivity("IS NOT NULL", null);
      assertEquals(1.0 - 200.0 / 1000.0, sel, 0.0001,
          "IS NOT NULL selectivity should be 1 - nullCount/totalCount");
    }

    @Test
    @DisplayName("IS NULL with all nulls should return 1.0")
    void isNullAllNulls() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 500L, 500L, null);

      double sel = cs.getSelectivity("IS NULL", null);
      assertEquals(1.0, sel, 0.0001,
          "IS NULL when all values are null should be 1.0");
    }

    @Test
    @DisplayName("IS NOT NULL with zero nulls should return 1.0")
    void isNotNullZeroNulls() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("IS NOT NULL", null);
      assertEquals(1.0, sel, 0.0001,
          "IS NOT NULL with no nulls should be 1.0");
    }

    @Test
    @DisplayName("Null value with other operators should return 0.0")
    void nullValueOtherOperator() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 0L, 1000L, null);

      assertEquals(0.0, cs.getSelectivity("<", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity("<=", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity(">", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity(">=", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity("=", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity("!=", null), 0.0001);
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getSelectivity: totalCount zero
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getSelectivity with zero totalCount")
  class ColumnStatsSelectivityZeroTotal {

    @Test
    @DisplayName("Any operator with totalCount 0 should return 0.0")
    void zeroTotalCountReturnsZero() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 0L, null);

      assertEquals(0.0, cs.getSelectivity("=", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity("!=", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity("<", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity(">", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity("<=", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity(">=", 42), 0.0001);
      assertEquals(0.0, cs.getSelectivity("IS NULL", null), 0.0001);
      assertEquals(0.0, cs.getSelectivity("LIKE", "pattern"), 0.0001);
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getSelectivity: range operators
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getSelectivity range operators")
  class ColumnStatsSelectivityRange {

    @Test
    @DisplayName("Less-than with numeric min/max should calculate position")
    void lessThanNumeric() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 50);
      // position = (50 - 0) / (100 - 0) = 0.5
      assertEquals(0.5, sel, 0.0001,
          "< 50 in range [0, 100] should give selectivity 0.5");
    }

    @Test
    @DisplayName("Less-than-or-equal with numeric min/max")
    void lessThanOrEqualNumeric() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<=", 25);
      // position = (25 - 0) / (100 - 0) = 0.25
      assertEquals(0.25, sel, 0.0001);
    }

    @Test
    @DisplayName("Greater-than with numeric min/max should calculate 1-position")
    void greaterThanNumeric() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity(">", 25);
      // position = (25 - 0) / (100 - 0) = 0.25; selectivity = 1 - 0.25 = 0.75
      assertEquals(0.75, sel, 0.0001,
          "> 25 in range [0, 100] should give selectivity 0.75");
    }

    @Test
    @DisplayName("Greater-than-or-equal with numeric min/max")
    void greaterThanOrEqualNumeric() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity(">=", 80);
      // position = (80 - 0) / (100 - 0) = 0.80; selectivity = 1 - 0.80 = 0.20
      assertEquals(0.20, sel, 0.0001);
    }

    @Test
    @DisplayName("Range query at min value should give position 0")
    void rangeAtMinValue() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 10, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 10);
      // position = (10 - 10) / (100 - 10) = 0.0
      assertEquals(0.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range query at max value should give position 1")
    void rangeAtMaxValue() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 10, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 100);
      // position = (100 - 10) / (100 - 10) = 1.0
      assertEquals(1.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range query beyond max value should clamp to 1.0")
    void rangeBeyondMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 200);
      // position = (200 - 0)/(100 - 0) = 2.0, clamped to 1.0
      assertEquals(1.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range query below min value should clamp to 0.0")
    void rangeBelowMin() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 10, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 0);
      // position = (0 - 10)/(100 - 10) = negative, clamped to 0.0
      assertEquals(0.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range with equal min and max should return 0.5")
    void rangeEqualMinMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 50, 50, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 50);
      // min == max => position = 0.5
      assertEquals(0.5, sel, 0.0001);
    }

    @Test
    @DisplayName("Range with null min/max should return 0.3 default")
    void rangeNullMinMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", null, null, 0L, 1000L, null);

      assertEquals(0.3, cs.getSelectivity("<", 50), 0.0001);
      assertEquals(0.3, cs.getSelectivity(">", 50), 0.0001);
      assertEquals(0.3, cs.getSelectivity("<=", 50), 0.0001);
      assertEquals(0.3, cs.getSelectivity(">=", 50), 0.0001);
    }

    @Test
    @DisplayName("Range with only min null should return 0.3 default")
    void rangeOnlyMinNull() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", null, 100, 0L, 1000L, null);

      assertEquals(0.3, cs.getSelectivity("<", 50), 0.0001);
    }

    @Test
    @DisplayName("Range with only max null should return 0.3 default")
    void rangeOnlyMaxNull() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, null, 0L, 1000L, null);

      assertEquals(0.3, cs.getSelectivity("<", 50), 0.0001);
    }

    @Test
    @DisplayName("Range with string min/max should handle basic comparison")
    void rangeWithStrings() {
      ColumnStatistics cs = new ColumnStatistics(
          "name", "A", "Z", 0L, 1000L, null);

      // String value between min and max returns 0.5 (simplified implementation)
      double sel = cs.getSelectivity("<", "M");
      assertEquals(0.5, sel, 0.0001,
          "String range selectivity for mid-range value should be 0.5");
    }

    @Test
    @DisplayName("Range with string at or below min should return 0.0")
    void rangeStringAtMin() {
      ColumnStatistics cs = new ColumnStatistics(
          "name", "B", "Z", 0L, 1000L, null);

      double sel = cs.getSelectivity("<", "A");
      assertEquals(0.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range with string at or above max should return 1.0 for <")
    void rangeStringAtMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "name", "A", "M", 0L, 1000L, null);

      double sel = cs.getSelectivity("<", "Z");
      assertEquals(1.0, sel, 0.0001);
    }

    @Test
    @DisplayName("Range with mismatched types should return 0.3")
    void rangeMismatchedTypes() {
      // min/max are Integer but query value is String
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", "not-a-number");
      assertEquals(0.3, sel, 0.0001,
          "Mismatched types should return default 0.3");
    }

    @Test
    @DisplayName("Range with double min/max and double value")
    void rangeWithDoubles() {
      ColumnStatistics cs = new ColumnStatistics(
          "price", 0.0, 200.0, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 100.0);
      // position = (100.0 - 0.0)/(200.0 - 0.0) = 0.5
      assertEquals(0.5, sel, 0.0001);
    }

    @Test
    @DisplayName("Range with long min/max")
    void rangeWithLongs() {
      ColumnStatistics cs = new ColumnStatistics(
          "id", 0L, 1000L, 0L, 1000L, null);

      double sel = cs.getSelectivity("<", 250L);
      // position = (250 - 0)/(1000 - 0) = 0.25
      assertEquals(0.25, sel, 0.0001);
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - getSelectivity: unknown operator
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - getSelectivity unknown operators")
  class ColumnStatsSelectivityUnknown {

    @Test
    @DisplayName("Unknown operator should return 0.1")
    void unknownOperator() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 1, 100, 0L, 1000L, null);

      assertEquals(0.1, cs.getSelectivity("LIKE", "pattern%"), 0.0001);
      assertEquals(0.1, cs.getSelectivity("IN", "list"), 0.0001);
      assertEquals(0.1, cs.getSelectivity("BETWEEN", 50), 0.0001);
      assertEquals(0.1, cs.getSelectivity("~", "regex"), 0.0001);
    }
  }

  // ---------------------------------------------------------------
  // ColumnStatistics - toString
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("ColumnStatistics - toString")
  class ColumnStatsToString {

    @Test
    @DisplayName("toString should contain column name and stats")
    void toStringContainsFields() {
      ColumnStatistics cs = new ColumnStatistics(
          "age", 18, 99, 5L, 1000L, null);
      String str = cs.toString();

      assertTrue(str.contains("age"), "toString should contain column name");
      assertTrue(str.contains("18"), "toString should contain min value");
      assertTrue(str.contains("99"), "toString should contain max value");
      assertTrue(str.contains("5"), "toString should contain null count");
      assertTrue(str.contains("1000"), "toString should contain total count");
    }

    @Test
    @DisplayName("toString should start with ColumnStatistics prefix")
    void toStringPrefix() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 0L, null);
      assertTrue(cs.toString().startsWith("ColumnStatistics{"));
    }

    @Test
    @DisplayName("toString should match expected format")
    void toStringFormat() {
      ColumnStatistics cs = new ColumnStatistics(
          "score", 0, 100, 10L, 500L, null);
      long distinct = cs.getDistinctCount();
      String expected = String.format(
          "ColumnStatistics{column='%s', min=%s, max=%s, nulls=%d, "
              + "total=%d, distinct=%d}",
          "score", 0, 100, 10L, 500L, distinct);
      assertEquals(expected, cs.toString());
    }

    @Test
    @DisplayName("toString should handle null min/max gracefully")
    void toStringNullMinMax() {
      ColumnStatistics cs = new ColumnStatistics(
          "empty", null, null, 0L, 0L, null);
      String str = cs.toString();
      assertTrue(str.contains("null"), "toString should contain null for null values");
    }
  }

  // ===================================================================
  // TableStatistics tests
  // ===================================================================

  @Nested
  @DisplayName("TableStatistics - Constructor and Getters")
  class TableStatsConstructorAndGetters {

    @Test
    @DisplayName("Constructor should store rowCount and dataSize")
    void constructorStoresBasicFields() {
      Map<String, ColumnStatistics> colStats = new HashMap<String, ColumnStatistics>();
      TableStatistics ts = new TableStatistics(1000L, 50000L, colStats, "hash123");

      assertEquals(1000L, ts.getRowCount());
      assertEquals(50000L, ts.getDataSize());
    }

    @Test
    @DisplayName("Constructor should set lastUpdated to approximately current time")
    void constructorSetsLastUpdated() {
      long before = System.currentTimeMillis();
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "hash");
      long after = System.currentTimeMillis();

      assertTrue(ts.getLastUpdated() >= before,
          "lastUpdated should be >= time before construction");
      assertTrue(ts.getLastUpdated() <= after,
          "lastUpdated should be <= time after construction");
    }

    @Test
    @DisplayName("Constructor should store sourceHash")
    void constructorStoresSourceHash() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "abc123");
      assertEquals("abc123", ts.getSourceHash());
    }

    @Test
    @DisplayName("Constructor should accept null sourceHash")
    void constructorAcceptsNullSourceHash() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), null);
      assertNull(ts.getSourceHash());
    }

    @Test
    @DisplayName("Constructor should copy column stats map")
    void constructorCopiesColumnStats() {
      Map<String, ColumnStatistics> original = new HashMap<String, ColumnStatistics>();
      ColumnStatistics cs = new ColumnStatistics(
          "col1", 0, 100, 0L, 500L, null);
      original.put("col1", cs);

      TableStatistics ts = new TableStatistics(500L, 25000L, original, "hash");

      // Modify original map after construction
      original.put("col2", new ColumnStatistics(
          "col2", 0, 50, 0L, 500L, null));

      // TableStatistics should not see the modification
      assertNull(ts.getColumnStatistics("col2"),
          "Internal map should be a copy, not affected by original changes");
      assertNotNull(ts.getColumnStatistics("col1"));
    }

    @Test
    @DisplayName("Constructor should accept empty column stats map")
    void constructorAcceptsEmptyMap() {
      TableStatistics ts = new TableStatistics(
          0L, 0L, new HashMap<String, ColumnStatistics>(), null);
      assertTrue(ts.getColumnStatistics().isEmpty());
    }
  }

  // ---------------------------------------------------------------
  // TableStatistics - getColumnStatistics
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("TableStatistics - getColumnStatistics")
  class TableStatsColumnAccess {

    @Test
    @DisplayName("getColumnStatistics(name) should return correct column stats")
    void getByNameReturnsCorrectStats() {
      ColumnStatistics cs1 = new ColumnStatistics(
          "age", 1, 100, 5L, 1000L, null);
      ColumnStatistics cs2 = new ColumnStatistics(
          "name", "A", "Z", 0L, 1000L, null);

      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("age", cs1);
      map.put("name", cs2);

      TableStatistics ts = new TableStatistics(1000L, 50000L, map, "hash");

      assertEquals(cs1, ts.getColumnStatistics("age"));
      assertEquals(cs2, ts.getColumnStatistics("name"));
    }

    @Test
    @DisplayName("getColumnStatistics(name) should return null for unknown column")
    void getByNameReturnsNullForUnknown() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "hash");
      assertNull(ts.getColumnStatistics("nonexistent"));
    }

    @Test
    @DisplayName("getColumnStatistics() should return a copy of the map")
    void getAllReturnsDefensiveCopy() {
      ColumnStatistics cs = new ColumnStatistics(
          "col", 0, 10, 0L, 100L, null);
      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("col", cs);

      TableStatistics ts = new TableStatistics(100L, 5000L, map, "hash");

      Map<String, ColumnStatistics> returned = ts.getColumnStatistics();
      assertNotSame(map, returned,
          "getColumnStatistics() should return a different map instance");

      // Modifying returned map should not affect internal state
      returned.put("extra", new ColumnStatistics(
          "extra", null, null, 0L, 0L, null));
      assertNull(ts.getColumnStatistics("extra"),
          "Modifying returned map should not affect internal state");
    }

    @Test
    @DisplayName("getColumnStatistics() should contain all added columns")
    void getAllContainsAllColumns() {
      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("a", new ColumnStatistics("a", 0, 1, 0L, 10L, null));
      map.put("b", new ColumnStatistics("b", 0, 2, 0L, 20L, null));
      map.put("c", new ColumnStatistics("c", 0, 3, 0L, 30L, null));

      TableStatistics ts = new TableStatistics(30L, 1500L, map, "hash");

      Map<String, ColumnStatistics> result = ts.getColumnStatistics();
      assertEquals(3, result.size());
      assertTrue(result.containsKey("a"));
      assertTrue(result.containsKey("b"));
      assertTrue(result.containsKey("c"));
    }
  }

  // ---------------------------------------------------------------
  // TableStatistics - isValidFor
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("TableStatistics - isValidFor")
  class TableStatsIsValidFor {

    @Test
    @DisplayName("isValidFor should return true when hashes match")
    void validWhenHashesMatch() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "abc123");
      assertTrue(ts.isValidFor("abc123"));
    }

    @Test
    @DisplayName("isValidFor should return false when hashes differ")
    void invalidWhenHashesDiffer() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "abc123");
      assertFalse(ts.isValidFor("different_hash"));
    }

    @Test
    @DisplayName("isValidFor should return false when sourceHash is null")
    void invalidWhenSourceHashNull() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), null);
      assertFalse(ts.isValidFor("any_hash"));
    }

    @Test
    @DisplayName("isValidFor should return false when comparing null to non-null")
    void invalidWhenCurrentHashNull() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "abc");
      assertFalse(ts.isValidFor(null));
    }

    @Test
    @DisplayName("isValidFor should return false when both hashes are null")
    void invalidWhenBothNull() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), null);
      assertFalse(ts.isValidFor(null));
    }

    @Test
    @DisplayName("isValidFor should be case-sensitive")
    void caseSensitive() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "Hash123");
      assertFalse(ts.isValidFor("hash123"),
          "Hash comparison should be case-sensitive");
    }
  }

  // ---------------------------------------------------------------
  // TableStatistics - getSelectivity
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("TableStatistics - getSelectivity")
  class TableStatsSelectivity {

    @Test
    @DisplayName("getSelectivity should delegate to column statistics")
    void delegatesToColumnStats() {
      HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(50L);
      ColumnStatistics cs = new ColumnStatistics(
          "age", 0, 100, 10L, 1000L, sketch);

      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("age", cs);
      TableStatistics ts = new TableStatistics(1000L, 50000L, map, "hash");

      double expected = cs.getSelectivity("=", 25);
      double actual = ts.getSelectivity("age", "=", 25);
      assertEquals(expected, actual, 0.0001,
          "TableStatistics should delegate to column's getSelectivity");
    }

    @Test
    @DisplayName("getSelectivity should return 0.1 when column not found")
    void returnsDefaultWhenNoColumn() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "hash");

      assertEquals(0.1, ts.getSelectivity("nonexistent", "=", 42), 0.0001,
          "Missing column should return default selectivity 0.1");
    }

    @Test
    @DisplayName("getSelectivity should work with different operators through delegation")
    void worksWithDifferentOperators() {
      ColumnStatistics cs = new ColumnStatistics(
          "val", 0, 100, 20L, 500L, null);

      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("val", cs);
      TableStatistics ts = new TableStatistics(500L, 25000L, map, "hash");

      // Test that different operators are properly delegated
      assertTrue(ts.getSelectivity("val", "<", 50) > 0);
      assertTrue(ts.getSelectivity("val", ">", 50) > 0);
      assertTrue(ts.getSelectivity("val", "IS NULL", null) > 0);
    }
  }

  // ---------------------------------------------------------------
  // TableStatistics - createBasicEstimate
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("TableStatistics - createBasicEstimate")
  class TableStatsBasicEstimate {

    @Test
    @DisplayName("createBasicEstimate should set rowCount")
    void setsRowCount() {
      TableStatistics ts = TableStatistics.createBasicEstimate(5000L);
      assertEquals(5000L, ts.getRowCount());
    }

    @Test
    @DisplayName("createBasicEstimate should set dataSize to rowCount * 100")
    void setsDataSize() {
      TableStatistics ts = TableStatistics.createBasicEstimate(5000L);
      assertEquals(500000L, ts.getDataSize(),
          "Data size should be rowCount * 100");
    }

    @Test
    @DisplayName("createBasicEstimate should have empty column stats")
    void hasEmptyColumnStats() {
      TableStatistics ts = TableStatistics.createBasicEstimate(100L);
      assertTrue(ts.getColumnStatistics().isEmpty(),
          "Basic estimate should have no column statistics");
    }

    @Test
    @DisplayName("createBasicEstimate should have null sourceHash")
    void hasNullSourceHash() {
      TableStatistics ts = TableStatistics.createBasicEstimate(100L);
      assertNull(ts.getSourceHash(),
          "Basic estimate should have null source hash");
    }

    @Test
    @DisplayName("createBasicEstimate should set lastUpdated")
    void setsLastUpdated() {
      long before = System.currentTimeMillis();
      TableStatistics ts = TableStatistics.createBasicEstimate(100L);
      long after = System.currentTimeMillis();

      assertTrue(ts.getLastUpdated() >= before);
      assertTrue(ts.getLastUpdated() <= after);
    }

    @Test
    @DisplayName("createBasicEstimate with zero rows")
    void zeroRows() {
      TableStatistics ts = TableStatistics.createBasicEstimate(0L);
      assertEquals(0L, ts.getRowCount());
      assertEquals(0L, ts.getDataSize());
    }

    @Test
    @DisplayName("createBasicEstimate with large row count")
    void largeRowCount() {
      TableStatistics ts = TableStatistics.createBasicEstimate(1000000L);
      assertEquals(1000000L, ts.getRowCount());
      assertEquals(100000000L, ts.getDataSize());
    }

    @Test
    @DisplayName("createBasicEstimate isValidFor should return false (null hash)")
    void basicEstimateInvalidForAnyHash() {
      TableStatistics ts = TableStatistics.createBasicEstimate(100L);
      assertFalse(ts.isValidFor("any_hash"));
    }
  }

  // ---------------------------------------------------------------
  // TableStatistics - toString
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("TableStatistics - toString")
  class TableStatsToString {

    @Test
    @DisplayName("toString should start with TableStatistics prefix")
    void toStringPrefix() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "hash");
      assertTrue(ts.toString().startsWith("TableStatistics{"));
    }

    @Test
    @DisplayName("toString should contain rowCount and dataSize")
    void toStringContainsFields() {
      TableStatistics ts = new TableStatistics(
          1234L, 56789L, new HashMap<String, ColumnStatistics>(), "hash");
      String str = ts.toString();
      assertTrue(str.contains("1234"), "Should contain rowCount");
      assertTrue(str.contains("56789"), "Should contain dataSize");
    }

    @Test
    @DisplayName("toString should contain column count")
    void toStringContainsColumnCount() {
      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("a", new ColumnStatistics("a", 0, 1, 0L, 10L, null));
      map.put("b", new ColumnStatistics("b", 0, 2, 0L, 20L, null));

      TableStatistics ts = new TableStatistics(20L, 1000L, map, "hash");
      String str = ts.toString();
      assertTrue(str.contains("columns=2"),
          "Should contain columns=2 for 2 column stats");
    }

    @Test
    @DisplayName("toString should contain lastUpdated")
    void toStringContainsLastUpdated() {
      TableStatistics ts = new TableStatistics(
          100L, 5000L, new HashMap<String, ColumnStatistics>(), "hash");
      String str = ts.toString();
      assertTrue(str.contains("lastUpdated="),
          "Should contain lastUpdated field");
    }

    @Test
    @DisplayName("toString format should match expected pattern")
    void toStringMatchesFormat() {
      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("x", new ColumnStatistics("x", 0, 1, 0L, 10L, null));

      TableStatistics ts = new TableStatistics(10L, 500L, map, "hash");
      String expected = String.format(
          "TableStatistics{rowCount=%d, dataSize=%d, columns=%d, lastUpdated=%d}",
          10L, 500L, 1, ts.getLastUpdated());
      assertEquals(expected, ts.toString());
    }
  }

  // ---------------------------------------------------------------
  // Integration: ColumnStatistics with real HLL sketch
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Integration - ColumnStatistics with real HLL sketch")
  class ColumnStatsWithRealHll {

    @Test
    @DisplayName("ColumnStatistics with populated HLL should use HLL for distinctCount")
    void populatedHllUsedForDistinctCount() {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      for (int i = 0; i < 200; i++) {
        sketch.add("user_" + i);
      }

      ColumnStatistics cs = new ColumnStatistics(
          "user_id", "user_0", "user_199", 0L, 200L, sketch);

      long distinct = cs.getDistinctCount();
      // HLL estimate should be close to 200
      assertTrue(distinct >= 150 && distinct <= 250,
          "HLL-based distinct count should be approximately 200, got " + distinct);
    }

    @Test
    @DisplayName("Equality selectivity should reflect HLL-based distinct count")
    void equalityReflectsHll() {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      for (int i = 0; i < 1000; i++) {
        sketch.add("item_" + i);
      }

      ColumnStatistics cs = new ColumnStatistics(
          "item", "item_0", "item_999", 0L, 1000L, sketch);

      double sel = cs.getSelectivity("=", "item_500");
      // selectivity = 1/distinctCount; distinctCount ~ 1000
      // So selectivity should be around 0.001
      assertTrue(sel > 0.0005 && sel < 0.005,
          "Equality selectivity with HLL should be approximately 1/1000, got " + sel);
    }

    @Test
    @DisplayName("getHllSketch should return the same sketch passed to constructor")
    void getHllSketchReturnsSame() {
      HyperLogLogSketch sketch = new HyperLogLogSketch(14);
      sketch.add("test");

      ColumnStatistics cs = new ColumnStatistics(
          "col", null, null, 0L, 100L, sketch);

      assertNotNull(cs.getHllSketch());
      assertEquals(sketch.getEstimate(), cs.getHllSketch().getEstimate());
    }
  }

  // ---------------------------------------------------------------
  // Integration: TableStatistics with ColumnStatistics
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Integration - TableStatistics with ColumnStatistics")
  class TableStatsWithColumns {

    @Test
    @DisplayName("Full workflow: create table stats with multiple column stats")
    void fullWorkflow() {
      HyperLogLogSketch ageSketch = HyperLogLogSketch.fromEstimate(80L);
      ColumnStatistics ageStat = new ColumnStatistics(
          "age", 18, 65, 5L, 1000L, ageSketch);

      ColumnStatistics nameStat = new ColumnStatistics(
          "name", "Alice", "Zoe", 0L, 1000L, null);

      ColumnStatistics salaryStat = new ColumnStatistics(
          "salary", 30000.0, 150000.0, 50L, 1000L, null);

      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("age", ageStat);
      map.put("name", nameStat);
      map.put("salary", salaryStat);

      TableStatistics ts = new TableStatistics(1000L, 100000L, map, "file_v1_hash");

      // Verify table-level
      assertEquals(1000L, ts.getRowCount());
      assertEquals(100000L, ts.getDataSize());
      assertEquals("file_v1_hash", ts.getSourceHash());
      assertTrue(ts.isValidFor("file_v1_hash"));
      assertFalse(ts.isValidFor("file_v2_hash"));

      // Verify column access
      assertEquals(3, ts.getColumnStatistics().size());

      // Verify selectivity delegation
      double ageSel = ts.getSelectivity("age", "=", 30);
      assertEquals(1.0 / 80.0, ageSel, 0.0001);

      double salarySel = ts.getSelectivity("salary", "<", 90000.0);
      // position = (90000 - 30000)/(150000 - 30000) = 0.5
      assertEquals(0.5, salarySel, 0.0001);

      // Missing column
      assertEquals(0.1, ts.getSelectivity("unknown", "=", 1), 0.0001);
    }

    @Test
    @DisplayName("getColumnStatistics(name) and getColumnStatistics() should be consistent")
    void namedAndMapAccessConsistent() {
      ColumnStatistics cs = new ColumnStatistics(
          "id", 1L, 10000L, 0L, 10000L, null);
      Map<String, ColumnStatistics> map = new HashMap<String, ColumnStatistics>();
      map.put("id", cs);

      TableStatistics ts = new TableStatistics(10000L, 500000L, map, "hash");

      ColumnStatistics fromName = ts.getColumnStatistics("id");
      ColumnStatistics fromMap = ts.getColumnStatistics().get("id");

      assertNotNull(fromName);
      assertNotNull(fromMap);
      assertEquals(fromName.getColumnName(), fromMap.getColumnName());
      assertEquals(fromName.getTotalCount(), fromMap.getTotalCount());
    }
  }
}
