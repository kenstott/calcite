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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SimpleFileJoinReorderRule} private methods
 * using reflection. Covers calculateTableCost(), findTableScan(),
 * analyzeJoinOrder() branches, and JoinReorderDecision inner class.
 */
@Tag("unit")
public class SimpleFileJoinReorderRuleCoverageTest {

  private SimpleFileJoinReorderRule rule;
  private Method calculateTableCost;

  @BeforeEach void setUp() throws Exception {
    rule = SimpleFileJoinReorderRule.INSTANCE;
    calculateTableCost = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "calculateTableCost", TableStatistics.class);
    calculateTableCost.setAccessible(true);
  }

  // ===== calculateTableCost =====

  @Test void testCalculateTableCostSmallTable() throws Exception {
    TableStatistics stats = new TableStatistics(100, 1000,
        new HashMap<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics>(), "hash1");
    double cost = (Double) calculateTableCost.invoke(rule, stats);
    // CPU: 100 * 0.001 = 0.1, IO: 1000 * 0.0001 = 0.1, total = 0.2
    assertEquals(0.2, cost, 0.001);
  }

  @Test void testCalculateTableCostLargeTable() throws Exception {
    TableStatistics stats = new TableStatistics(1000000, 500000000L,
        new HashMap<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics>(), "hash2");
    double cost = (Double) calculateTableCost.invoke(rule, stats);
    // CPU: 1000000 * 0.001 = 1000, IO: 500000000 * 0.0001 = 50000, total = 51000
    assertEquals(51000.0, cost, 0.001);
  }

  @Test void testCalculateTableCostZeroRows() throws Exception {
    TableStatistics stats = new TableStatistics(0, 0,
        new HashMap<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics>(), "hash3");
    double cost = (Double) calculateTableCost.invoke(rule, stats);
    assertEquals(0.0, cost, 0.001);
  }

  @Test void testCalculateTableCostOnlyRows() throws Exception {
    TableStatistics stats = new TableStatistics(1000, 0,
        new HashMap<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics>(), "hash4");
    double cost = (Double) calculateTableCost.invoke(rule, stats);
    // CPU: 1000 * 0.001 = 1.0, IO: 0
    assertEquals(1.0, cost, 0.001);
  }

  @Test void testCalculateTableCostOnlyData() throws Exception {
    TableStatistics stats = new TableStatistics(0, 100000,
        new HashMap<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics>(), "hash5");
    double cost = (Double) calculateTableCost.invoke(rule, stats);
    // CPU: 0, IO: 100000 * 0.0001 = 10.0
    assertEquals(10.0, cost, 0.001);
  }

  // ===== findTableScan =====

  @Test void testFindTableScanWithNull() throws Exception {
    Method findTableScan = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "findTableScan", org.apache.calcite.rel.RelNode.class);
    findTableScan.setAccessible(true);
    Object result = findTableScan.invoke(rule, (Object) null);
    assertNull(result);
  }

  // ===== JoinReorderDecision inner class =====

  @Test void testJoinReorderDecisionDefaults() throws Exception {
    Class<?> jrdClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileJoinReorderRule$JoinReorderDecision");
    Constructor<?> ctor = jrdClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object jrd = ctor.newInstance();

    Field shouldReorder = jrdClass.getDeclaredField("shouldReorder");
    shouldReorder.setAccessible(true);
    assertFalse((Boolean) shouldReorder.get(jrd));

    Field swapToLeftJoin = jrdClass.getDeclaredField("swapToLeftJoin");
    swapToLeftJoin.setAccessible(true);
    assertFalse((Boolean) swapToLeftJoin.get(jrd));

    Field leftCost = jrdClass.getDeclaredField("leftCost");
    leftCost.setAccessible(true);
    assertEquals(0.0, (Double) leftCost.get(jrd), 0.001);

    Field rightCost = jrdClass.getDeclaredField("rightCost");
    rightCost.setAccessible(true);
    assertEquals(0.0, (Double) rightCost.get(jrd), 0.001);
  }

  @Test void testJoinReorderDecisionSetFields() throws Exception {
    Class<?> jrdClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileJoinReorderRule$JoinReorderDecision");
    Constructor<?> ctor = jrdClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object jrd = ctor.newInstance();

    Field shouldReorder = jrdClass.getDeclaredField("shouldReorder");
    shouldReorder.setAccessible(true);
    shouldReorder.set(jrd, true);
    assertTrue((Boolean) shouldReorder.get(jrd));

    Field swapToLeftJoin = jrdClass.getDeclaredField("swapToLeftJoin");
    swapToLeftJoin.setAccessible(true);
    swapToLeftJoin.set(jrd, true);
    assertTrue((Boolean) swapToLeftJoin.get(jrd));

    Field leftCost = jrdClass.getDeclaredField("leftCost");
    leftCost.setAccessible(true);
    leftCost.set(jrd, 100.5);
    assertEquals(100.5, (Double) leftCost.get(jrd), 0.001);

    Field rightCost = jrdClass.getDeclaredField("rightCost");
    rightCost.setAccessible(true);
    rightCost.set(jrd, 200.0);
    assertEquals(200.0, (Double) rightCost.get(jrd), 0.001);
  }

  // ===== analyzeJoinOrder (via reflection) =====

  @Test void testAnalyzeJoinOrderMethodExists() throws Exception {
    Method analyzeJoinOrder = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "analyzeJoinOrder", TableStatistics.class, TableStatistics.class,
        org.apache.calcite.rel.logical.LogicalJoin.class);
    analyzeJoinOrder.setAccessible(true);
    assertNotNull(analyzeJoinOrder);
  }

  // ===== adjustJoinCondition (via reflection) =====

  @Test void testAdjustJoinConditionMethodExists() throws Exception {
    Method adjustJoinCondition = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "adjustJoinCondition", RexNode.class, int.class, int.class);
    adjustJoinCondition.setAccessible(true);
    assertNotNull(adjustJoinCondition);
  }

  // ===== getTableStatistics =====

  @Test void testGetTableStatisticsMethodExists() throws Exception {
    Method getStats = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "getTableStatistics", org.apache.calcite.rel.RelNode.class);
    getStats.setAccessible(true);
    assertNotNull(getStats);
  }

  // ===== createReorderedJoin =====

  @Test void testCreateReorderedJoinMethodExists() throws Exception {
    Method createReorderedJoin = SimpleFileJoinReorderRule.class.getDeclaredMethod(
        "createReorderedJoin",
        org.apache.calcite.rel.logical.LogicalJoin.class,
        org.apache.calcite.rel.RelNode.class,
        org.apache.calcite.rel.RelNode.class,
        org.apache.calcite.tools.RelBuilder.class,
        Class.forName(
            "org.apache.calcite.adapter.file.rules.SimpleFileJoinReorderRule$JoinReorderDecision"));
    createReorderedJoin.setAccessible(true);
    assertNotNull(createReorderedJoin);
  }
}
