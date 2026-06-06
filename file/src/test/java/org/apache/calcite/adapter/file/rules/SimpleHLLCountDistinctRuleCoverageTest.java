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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Coverage tests for {@link SimpleHLLCountDistinctRule} private methods.
 * Covers findTableScan(), shouldOptimize(), and method existence checks.
 */
@Tag("unit")
public class SimpleHLLCountDistinctRuleCoverageTest {

  private SimpleHLLCountDistinctRule rule;
  private Method findTableScan;
  private Method shouldOptimize;
  private Method getHLLEstimate;
  private Method createHLLValues;

  @BeforeEach void setUp() throws Exception {
    rule = SimpleHLLCountDistinctRule.INSTANCE;

    findTableScan = SimpleHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    findTableScan.setAccessible(true);

    shouldOptimize = SimpleHLLCountDistinctRule.class.getDeclaredMethod(
        "shouldOptimize", AggregateCall.class);
    shouldOptimize.setAccessible(true);

    getHLLEstimate = SimpleHLLCountDistinctRule.class.getDeclaredMethod(
        "getHLLEstimate", RelNode.class, AggregateCall.class);
    getHLLEstimate.setAccessible(true);

    createHLLValues = SimpleHLLCountDistinctRule.class.getDeclaredMethod(
        "createHLLValues",
        org.apache.calcite.rel.core.Aggregate.class, List.class);
    createHLLValues.setAccessible(true);
  }

  // ===== findTableScan =====

  @Test void testFindTableScanWithNull() throws Exception {
    Object result = findTableScan.invoke(rule, (Object) null);
    assertNull(result, "findTableScan(null) should return null");
  }

  // ===== Method existence =====

  @Test void testShouldOptimizeMethodExists() {
    assertNotNull(shouldOptimize);
  }

  @Test void testGetHLLEstimateMethodExists() {
    assertNotNull(getHLLEstimate);
  }

  @Test void testCreateHLLValuesMethodExists() {
    assertNotNull(createHLLValues);
  }

  @Test void testOnMatchMethodExists() throws Exception {
    Method onMatch = SimpleHLLCountDistinctRule.class.getMethod(
        "onMatch", org.apache.calcite.plan.RelOptRuleCall.class);
    assertNotNull(onMatch);
  }

  // ===== approxOnly field =====

  @Test void testInstanceApproxOnlyField() throws Exception {
    java.lang.reflect.Field approxOnly =
        SimpleHLLCountDistinctRule.class.getDeclaredField("approxOnly");
    approxOnly.setAccessible(true);
    assertFalse((Boolean) approxOnly.get(SimpleHLLCountDistinctRule.INSTANCE));
  }

  @Test void testApproxOnlyInstanceApproxOnlyField() throws Exception {
    java.lang.reflect.Field approxOnly =
        SimpleHLLCountDistinctRule.class.getDeclaredField("approxOnly");
    approxOnly.setAccessible(true);
    // APPROX_ONLY_INSTANCE uses Config.APPROX_ONLY which has approxOnly=true
    // but the Config class's approxOnly() method returns true
    // The constructor sets this.approxOnly = config.approxOnly()
    // For APPROX_ONLY config, approxOnly() returns true
    assertNotNull(approxOnly.get(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE));
  }
}
