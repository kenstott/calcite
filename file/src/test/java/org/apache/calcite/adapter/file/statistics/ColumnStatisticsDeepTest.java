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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for ColumnStatistics covering selectivity estimation,
 * range calculations, null handling, and edge cases.
 */
@Tag("unit")
public class ColumnStatisticsDeepTest {

  @Test void testBasicGetters() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add("a");
    hll.add("b");
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 5, 1000, hll);
    assertEquals("col1", stats.getColumnName());
    assertEquals(1, stats.getMinValue());
    assertEquals(100, stats.getMaxValue());
    assertEquals(5, stats.getNullCount());
    assertEquals(1000, stats.getTotalCount());
    assertNotNull(stats.getHllSketch());
  }

  @Test void testDistinctCountWithHLL() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(42);
    ColumnStatistics stats = new ColumnStatistics("col1", null, null, 0, 100, hll);
    assertEquals(42, stats.getDistinctCount());
  }

  @Test void testDistinctCountWithoutHLL() {
    ColumnStatistics stats = new ColumnStatistics("col1", null, null, 0, 5000, null);
    // Without HLL, should return min(1000, totalCount)
    assertEquals(1000, stats.getDistinctCount());
  }

  @Test void testDistinctCountWithoutHLLSmallTotal() {
    ColumnStatistics stats = new ColumnStatistics("col1", null, null, 0, 50, null);
    // min(1000, 50) = 50
    assertEquals(50, stats.getDistinctCount());
  }

  // ===== Selectivity: equality =====

  @Test void testSelectivityEqualityWithDistinct() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(100);
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 1000, 0, 10000, hll);
    double sel = stats.getSelectivity("=", 42);
    assertEquals(0.01, sel, 0.001); // 1/100
  }

  @Test void testSelectivityNotEqual() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(100);
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 1000, 0, 10000, hll);
    double sel = stats.getSelectivity("!=", 42);
    assertEquals(0.99, sel, 0.001); // 1 - 1/100
  }

  // ===== Selectivity: null comparisons =====

  @Test void testSelectivityIsNull() {
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 200, 1000, null);
    double sel = stats.getSelectivity("IS NULL", null);
    assertEquals(0.2, sel, 0.001); // 200/1000
  }

  @Test void testSelectivityIsNotNull() {
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 200, 1000, null);
    double sel = stats.getSelectivity("IS NOT NULL", null);
    assertEquals(0.8, sel, 0.001); // 1 - 200/1000
  }

  @Test void testSelectivityNullWithOtherOperator() {
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 0, 1000, null);
    double sel = stats.getSelectivity("=", null);
    assertEquals(0.0, sel);
  }

  // ===== Selectivity: empty table =====

  @Test void testSelectivityEmptyTable() {
    ColumnStatistics stats = new ColumnStatistics("col1", null, null, 0, 0, null);
    assertEquals(0.0, stats.getSelectivity("=", 42));
  }

  // ===== Selectivity: range queries with numeric min/max =====

  @Test void testSelectivityLessThanNumeric() {
    ColumnStatistics stats = new ColumnStatistics("col1", 0, 100, 0, 1000, null);
    double sel = stats.getSelectivity("<", 50);
    // position = (50-0)/(100-0) = 0.5
    assertEquals(0.5, sel, 0.01);
  }

  @Test void testSelectivityGreaterThanNumeric() {
    ColumnStatistics stats = new ColumnStatistics("col1", 0, 100, 0, 1000, null);
    double sel = stats.getSelectivity(">", 50);
    // 1 - position = 1 - 0.5 = 0.5
    assertEquals(0.5, sel, 0.01);
  }

  @Test void testSelectivityLessThanEqualNumeric() {
    ColumnStatistics stats = new ColumnStatistics("col1", 0, 200, 0, 1000, null);
    double sel = stats.getSelectivity("<=", 100);
    assertEquals(0.5, sel, 0.01);
  }

  @Test void testSelectivityGreaterThanEqualNumeric() {
    ColumnStatistics stats = new ColumnStatistics("col1", 0, 200, 0, 1000, null);
    double sel = stats.getSelectivity(">=", 100);
    assertEquals(0.5, sel, 0.01);
  }

  @Test void testSelectivityRangeValueBelowMin() {
    ColumnStatistics stats = new ColumnStatistics("col1", 10, 100, 0, 1000, null);
    double sel = stats.getSelectivity("<", 0);
    // position = (0-10)/(100-10) = negative -> clamped to 0
    assertEquals(0.0, sel, 0.01);
  }

  @Test void testSelectivityRangeValueAboveMax() {
    ColumnStatistics stats = new ColumnStatistics("col1", 10, 100, 0, 1000, null);
    double sel = stats.getSelectivity("<", 200);
    // position = (200-10)/(100-10) > 1 -> clamped to 1
    assertEquals(1.0, sel, 0.01);
  }

  @Test void testSelectivityRangeSingleValue() {
    // min == max (single distinct value)
    ColumnStatistics stats = new ColumnStatistics("col1", 50, 50, 0, 1000, null);
    double sel = stats.getSelectivity("<", 50);
    // position = 0.5 (single value fallback)
    assertEquals(0.5, sel, 0.01);
  }

  // ===== Selectivity: range queries with string min/max =====

  @Test void testSelectivityLessThanString() {
    ColumnStatistics stats = new ColumnStatistics("col1", "aaa", "zzz", 0, 1000, null);
    double sel = stats.getSelectivity("<", "mmm");
    // String position returns 0.5 for mid-range values
    assertEquals(0.5, sel, 0.01);
  }

  @Test void testSelectivityLessThanStringBelowMin() {
    ColumnStatistics stats = new ColumnStatistics("col1", "bbb", "zzz", 0, 1000, null);
    double sel = stats.getSelectivity("<", "aaa");
    assertEquals(0.0, sel, 0.01);
  }

  @Test void testSelectivityLessThanStringAboveMax() {
    ColumnStatistics stats = new ColumnStatistics("col1", "aaa", "mmm", 0, 1000, null);
    double sel = stats.getSelectivity("<", "zzz");
    assertEquals(1.0, sel, 0.01);
  }

  // ===== Selectivity: range with null min/max =====

  @Test void testSelectivityRangeNullMinMax() {
    ColumnStatistics stats = new ColumnStatistics("col1", null, null, 0, 1000, null);
    double sel = stats.getSelectivity("<", 50);
    assertEquals(0.3, sel, 0.001); // Default for range without min/max
  }

  // ===== Selectivity: range with incompatible types =====

  @Test void testSelectivityRangeIncompatibleTypes() {
    // min/max are Integer but value is String -> isComparable returns false
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 0, 1000, null);
    double sel = stats.getSelectivity("<", "not a number");
    assertEquals(0.3, sel, 0.001); // fallback
  }

  // ===== Selectivity: unknown operator =====

  @Test void testSelectivityUnknownOperator() {
    ColumnStatistics stats = new ColumnStatistics("col1", 1, 100, 0, 1000, null);
    double sel = stats.getSelectivity("LIKE", "abc%");
    assertEquals(0.1, sel, 0.001); // Default for unknown
  }

  // ===== Selectivity: with non-Number/non-String types =====

  @Test void testSelectivityRangeWithDateType() {
    // Non-Number, non-String Comparable types should return 0.5
    java.sql.Date min = java.sql.Date.valueOf("2020-01-01");
    java.sql.Date max = java.sql.Date.valueOf("2020-12-31");
    java.sql.Date val = java.sql.Date.valueOf("2020-06-15");
    ColumnStatistics stats = new ColumnStatistics("col1", min, max, 0, 1000, null);
    double sel = stats.getSelectivity("<", val);
    // Date is not handled in calculateRelativePosition, falls through to 0.5
    assertEquals(0.5, sel, 0.01);
  }

  // ===== toString =====

  @Test void testToString() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(50);
    ColumnStatistics stats = new ColumnStatistics("mycol", 1, 100, 5, 1000, hll);
    String str = stats.toString();
    assertTrue(str.contains("mycol"));
    assertTrue(str.contains("min=1"));
    assertTrue(str.contains("max=100"));
    assertTrue(str.contains("nulls=5"));
    assertTrue(str.contains("total=1000"));
    assertTrue(str.contains("distinct=50"));
  }
}
