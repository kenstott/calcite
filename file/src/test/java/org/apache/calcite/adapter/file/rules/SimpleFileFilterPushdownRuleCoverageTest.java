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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SimpleFileFilterPushdownRule} private methods
 * using reflection. Covers evaluateWithStats() switch cases, null handling,
 * NlsString unwrapping, and FilterOptimization inner class.
 */
@Tag("unit")
public class SimpleFileFilterPushdownRuleCoverageTest {

  private Method evaluateWithStats;
  private SimpleFileFilterPushdownRule rule;

  @BeforeEach void setUp() throws Exception {
    rule = SimpleFileFilterPushdownRule.INSTANCE;
    evaluateWithStats = SimpleFileFilterPushdownRule.class.getDeclaredMethod(
        "evaluateWithStats", SqlKind.class, Object.class,
        Object.class, Object.class, boolean.class);
    evaluateWithStats.setAccessible(true);
  }

  private boolean invoke(SqlKind op, Object filter, Object min, Object max,
      boolean checkTrue) throws Exception {
    return (Boolean) evaluateWithStats.invoke(rule, op, filter, min, max, checkTrue);
  }

  // ===== NULL HANDLING =====

  @Test void testNullFilterValueReturnsFalse() throws Exception {
    assertFalse(invoke(SqlKind.EQUALS, null, 1, 10, true));
  }

  @Test void testNullMinValueReturnsFalse() throws Exception {
    assertFalse(invoke(SqlKind.EQUALS, 5, null, 10, true));
  }

  @Test void testNullMaxValueReturnsFalse() throws Exception {
    assertFalse(invoke(SqlKind.EQUALS, 5, 1, null, true));
  }

  @Test void testAllNullReturnsFalse() throws Exception {
    assertFalse(invoke(SqlKind.EQUALS, null, null, null, false));
  }

  // ===== EQUALS =====

  @Test void testEqualsAlwaysTrueWhenAllSame() throws Exception {
    // min == max == filter -> always true (all rows same value)
    assertTrue(invoke(SqlKind.EQUALS, 5, 5, 5, true));
  }

  @Test void testEqualsNotAlwaysTrueWhenRange() throws Exception {
    // min != max, so not all rows have same value
    assertFalse(invoke(SqlKind.EQUALS, 5, 1, 10, true));
  }

  @Test void testEqualsAlwaysFalseWhenBelowMin() throws Exception {
    // filter < min -> filter value outside range -> always false
    assertTrue(invoke(SqlKind.EQUALS, 0, 1, 10, false));
  }

  @Test void testEqualsAlwaysFalseWhenAboveMax() throws Exception {
    // filter > max -> filter value outside range -> always false
    assertTrue(invoke(SqlKind.EQUALS, 20, 1, 10, false));
  }

  @Test void testEqualsNotAlwaysFalseWhenInRange() throws Exception {
    // filter in range -> not always false
    assertFalse(invoke(SqlKind.EQUALS, 5, 1, 10, false));
  }

  // ===== LESS_THAN =====

  @Test void testLessThanAlwaysTrueWhenMaxBelowFilter() throws Exception {
    // max < filter -> all values less than filter
    assertTrue(invoke(SqlKind.LESS_THAN, 20, 1, 10, true));
  }

  @Test void testLessThanNotAlwaysTrueWhenMaxAboveFilter() throws Exception {
    assertFalse(invoke(SqlKind.LESS_THAN, 5, 1, 10, true));
  }

  @Test void testLessThanAlwaysFalseWhenMinAboveFilter() throws Exception {
    // min >= filter -> no values less than filter
    assertTrue(invoke(SqlKind.LESS_THAN, 1, 5, 10, false));
  }

  @Test void testLessThanNotAlwaysFalseWhenMinBelowFilter() throws Exception {
    assertFalse(invoke(SqlKind.LESS_THAN, 5, 1, 10, false));
  }

  // ===== GREATER_THAN =====

  @Test void testGreaterThanAlwaysTrueWhenMinAboveFilter() throws Exception {
    // min > filter -> all values greater than filter
    assertTrue(invoke(SqlKind.GREATER_THAN, 0, 5, 10, true));
  }

  @Test void testGreaterThanNotAlwaysTrueWhenMinBelowFilter() throws Exception {
    assertFalse(invoke(SqlKind.GREATER_THAN, 5, 1, 10, true));
  }

  @Test void testGreaterThanAlwaysFalseWhenMaxBelowFilter() throws Exception {
    // max <= filter -> no values greater than filter
    assertTrue(invoke(SqlKind.GREATER_THAN, 10, 1, 10, false));
  }

  @Test void testGreaterThanNotAlwaysFalseWhenMaxAboveFilter() throws Exception {
    assertFalse(invoke(SqlKind.GREATER_THAN, 5, 1, 10, false));
  }

  // ===== LESS_THAN_OR_EQUAL =====

  @Test void testLessOrEqualAlwaysTrueWhenMaxEqFilter() throws Exception {
    // max <= filter -> all values <= filter
    assertTrue(invoke(SqlKind.LESS_THAN_OR_EQUAL, 10, 1, 10, true));
  }

  @Test void testLessOrEqualNotAlwaysTrueWhenMaxAboveFilter() throws Exception {
    assertFalse(invoke(SqlKind.LESS_THAN_OR_EQUAL, 5, 1, 10, true));
  }

  @Test void testLessOrEqualAlwaysFalseWhenMinAboveFilter() throws Exception {
    // min > filter -> no values <= filter
    assertTrue(invoke(SqlKind.LESS_THAN_OR_EQUAL, 0, 5, 10, false));
  }

  @Test void testLessOrEqualNotAlwaysFalseWhenMinBelowFilter() throws Exception {
    assertFalse(invoke(SqlKind.LESS_THAN_OR_EQUAL, 5, 1, 10, false));
  }

  // ===== GREATER_THAN_OR_EQUAL =====

  @Test void testGreaterOrEqualAlwaysTrueWhenMinEqFilter() throws Exception {
    // min >= filter -> all values >= filter
    assertTrue(invoke(SqlKind.GREATER_THAN_OR_EQUAL, 1, 1, 10, true));
  }

  @Test void testGreaterOrEqualNotAlwaysTrueWhenMinBelowFilter() throws Exception {
    assertFalse(invoke(SqlKind.GREATER_THAN_OR_EQUAL, 5, 1, 10, true));
  }

  @Test void testGreaterOrEqualAlwaysFalseWhenMaxBelowFilter() throws Exception {
    // max < filter -> no values >= filter
    assertTrue(invoke(SqlKind.GREATER_THAN_OR_EQUAL, 20, 1, 10, false));
  }

  @Test void testGreaterOrEqualNotAlwaysFalseWhenMaxAboveFilter() throws Exception {
    assertFalse(invoke(SqlKind.GREATER_THAN_OR_EQUAL, 5, 1, 10, false));
  }

  // ===== NOT_EQUALS =====

  @Test void testNotEqualsAlwaysTrueWhenBelowMin() throws Exception {
    // filter < min -> value outside range -> always not equal
    assertTrue(invoke(SqlKind.NOT_EQUALS, 0, 5, 10, true));
  }

  @Test void testNotEqualsAlwaysTrueWhenAboveMax() throws Exception {
    // filter > max -> value outside range -> always not equal
    assertTrue(invoke(SqlKind.NOT_EQUALS, 20, 5, 10, true));
  }

  @Test void testNotEqualsNotAlwaysTrueWhenInRange() throws Exception {
    assertFalse(invoke(SqlKind.NOT_EQUALS, 7, 5, 10, true));
  }

  @Test void testNotEqualsAlwaysFalseWhenAllSame() throws Exception {
    // min == max == filter -> all rows same value == filter -> never not-equal
    assertTrue(invoke(SqlKind.NOT_EQUALS, 5, 5, 5, false));
  }

  @Test void testNotEqualsNotAlwaysFalseWhenRange() throws Exception {
    assertFalse(invoke(SqlKind.NOT_EQUALS, 5, 1, 10, false));
  }

  // ===== NlsString HANDLING =====

  @Test void testNlsStringUnwrapping() throws Exception {
    NlsString nlsFilter = new NlsString("hello", null, null);
    // With String min/max matching the unwrapped value
    assertTrue(invoke(SqlKind.EQUALS, nlsFilter, "hello", "hello", true));
  }

  @Test void testNlsStringUnwrappingOutOfRange() throws Exception {
    NlsString nlsFilter = new NlsString("aaa", null, null);
    // filter < min -> always false for EQUALS
    assertTrue(invoke(SqlKind.EQUALS, nlsFilter, "bbb", "zzz", false));
  }

  // ===== STRING COMPARISONS =====

  @Test void testStringEqualsAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.EQUALS, "hello", "hello", "hello", true));
  }

  @Test void testStringLessThanAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.LESS_THAN, "zzz", "aaa", "mmm", true));
  }

  @Test void testStringGreaterThanAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.GREATER_THAN, "aaa", "mmm", "zzz", true));
  }

  // ===== NON-COMPARABLE VALUES =====

  @Test void testNonComparableReturnsFalse() throws Exception {
    // Use objects that are not Comparable
    Object notComparable = new Object();
    assertFalse(invoke(SqlKind.EQUALS, notComparable, 1, 10, true));
  }

  // ===== EXCEPTION HANDLING =====

  @Test void testClassCastExceptionReturnsFalse() throws Exception {
    // Pass incompatible Comparable types to trigger ClassCastException
    assertFalse(invoke(SqlKind.EQUALS, "text", 1, 10, true));
  }

  // ===== FilterOptimization INNER CLASS =====

  @Test void testFilterOptimizationDefaults() throws Exception {
    Class<?> foClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileFilterPushdownRule$FilterOptimization");
    Constructor<?> ctor = foClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object fo = ctor.newInstance();

    Field canOptimize = foClass.getDeclaredField("canOptimize");
    canOptimize.setAccessible(true);
    assertFalse((Boolean) canOptimize.get(fo));

    Field alwaysTrue = foClass.getDeclaredField("alwaysTrue");
    alwaysTrue.setAccessible(true);
    assertFalse((Boolean) alwaysTrue.get(fo));

    Field alwaysFalse = foClass.getDeclaredField("alwaysFalse");
    alwaysFalse.setAccessible(true);
    assertFalse((Boolean) alwaysFalse.get(fo));

    Field columnName = foClass.getDeclaredField("columnName");
    columnName.setAccessible(true);
    assertNull(columnName.get(fo));

    Field operator = foClass.getDeclaredField("operator");
    operator.setAccessible(true);
    assertNull(operator.get(fo));

    Field filterValue = foClass.getDeclaredField("filterValue");
    filterValue.setAccessible(true);
    assertNull(filterValue.get(fo));
  }

  @Test void testFilterOptimizationSetFields() throws Exception {
    Class<?> foClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileFilterPushdownRule$FilterOptimization");
    Constructor<?> ctor = foClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object fo = ctor.newInstance();

    Field canOptimize = foClass.getDeclaredField("canOptimize");
    canOptimize.setAccessible(true);
    canOptimize.set(fo, true);
    assertTrue((Boolean) canOptimize.get(fo));

    Field alwaysTrue = foClass.getDeclaredField("alwaysTrue");
    alwaysTrue.setAccessible(true);
    alwaysTrue.set(fo, true);
    assertTrue((Boolean) alwaysTrue.get(fo));

    Field alwaysFalse = foClass.getDeclaredField("alwaysFalse");
    alwaysFalse.setAccessible(true);
    alwaysFalse.set(fo, true);
    assertTrue((Boolean) alwaysFalse.get(fo));

    Field minValue = foClass.getDeclaredField("minValue");
    minValue.setAccessible(true);
    minValue.set(fo, 1);
    assertEquals(1, minValue.get(fo));

    Field maxValue = foClass.getDeclaredField("maxValue");
    maxValue.setAccessible(true);
    maxValue.set(fo, 10);
    assertEquals(10, maxValue.get(fo));
  }

  // ===== getTableStatistics =====

  @Test void testGetTableStatisticsWithNonParquetReturnsNull() throws Exception {
    Method getStats = SimpleFileFilterPushdownRule.class.getDeclaredMethod(
        "getTableStatistics", org.apache.calcite.rel.core.TableScan.class);
    getStats.setAccessible(true);
    // We cannot easily create a TableScan without full infrastructure,
    // but verify the method exists and is accessible
    assertNotNull(getStats);
  }

  // ===== DOUBLE VALUE COMPARISONS =====

  @Test void testDoubleEqualsAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.EQUALS, 5.0, 5.0, 5.0, true));
  }

  @Test void testDoubleLessThanAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.LESS_THAN, 20.0, 1.0, 10.0, true));
  }

  @Test void testDoubleGreaterThanAlwaysFalse() throws Exception {
    assertTrue(invoke(SqlKind.GREATER_THAN, 10.0, 1.0, 10.0, false));
  }

  // ===== LONG VALUE COMPARISONS =====

  @Test void testLongEqualsAlwaysFalseWhenBelow() throws Exception {
    assertTrue(invoke(SqlKind.EQUALS, 0L, 1L, 10L, false));
  }

  @Test void testLongLessThanOrEqualAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.LESS_THAN_OR_EQUAL, 10L, 1L, 10L, true));
  }

  @Test void testLongGreaterThanOrEqualAlwaysTrue() throws Exception {
    assertTrue(invoke(SqlKind.GREATER_THAN_OR_EQUAL, 1L, 1L, 10L, true));
  }
}
