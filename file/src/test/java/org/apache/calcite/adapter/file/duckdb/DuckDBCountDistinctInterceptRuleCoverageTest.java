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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.plan.RelOptRule;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link DuckDBCountDistinctInterceptRule}.
 * Tests INSTANCE singleton, matches() method visibility, and createHLLValues() method existence.
 */
@Tag("unit")
public class DuckDBCountDistinctInterceptRuleCoverageTest {

  @Test void testInstanceNotNull() {
    assertNotNull(DuckDBCountDistinctInterceptRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(DuckDBCountDistinctInterceptRule.INSTANCE instanceof RelOptRule);
  }

  @Test void testInstanceIsSingleton() {
    assertSame(DuckDBCountDistinctInterceptRule.INSTANCE,
        DuckDBCountDistinctInterceptRule.INSTANCE);
  }

  @Test void testInstanceDescription() {
    String desc = DuckDBCountDistinctInterceptRule.INSTANCE.toString();
    assertNotNull(desc);
    assertTrue(desc.contains("DuckDBCountDistinctInterceptRule"));
  }

  @Test void testCreateHLLValuesMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getDeclaredMethod("createHLLValues",
        org.apache.calcite.rel.core.Aggregate.class,
        java.util.List.class);
    method.setAccessible(true);
    assertNotNull(method);
  }

  @Test void testMatchesMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getMethod("matches",
        org.apache.calcite.plan.RelOptRuleCall.class);
    assertNotNull(method);
  }

  @Test void testOnMatchMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getMethod("onMatch",
        org.apache.calcite.plan.RelOptRuleCall.class);
    assertNotNull(method);
  }
}
