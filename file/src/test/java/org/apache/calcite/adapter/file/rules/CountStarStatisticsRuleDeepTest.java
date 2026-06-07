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

import org.apache.calcite.plan.RelOptRule;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep unit tests for {@link CountStarStatisticsRule} Config class,
 * singleton instance, and rule configuration.
 */
@Tag("unit")
public class CountStarStatisticsRuleDeepTest {

  // ===== INSTANCE =====

  @Test void testInstanceNotNull() {
    assertNotNull(CountStarStatisticsRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(CountStarStatisticsRule.INSTANCE instanceof CountStarStatisticsRule);
  }

  @Test void testInstanceIsRelOptRule() {
    assertTrue(CountStarStatisticsRule.INSTANCE instanceof RelOptRule);
  }

  // ===== Config =====

  @Test void testConfigDefault() {
    assertNotNull(CountStarStatisticsRule.Config.DEFAULT);
  }

  @Test void testConfigDescription() {
    assertEquals("CountStarStatisticsRule",
        CountStarStatisticsRule.Config.DEFAULT.description());
  }

  @Test void testConfigOperandSupplier() {
    assertNotNull(CountStarStatisticsRule.Config.DEFAULT.operandSupplier());
  }

  @Test void testConfigToRule() {
    RelOptRule rule = CountStarStatisticsRule.Config.DEFAULT.toRule();
    assertNotNull(rule);
    assertTrue(rule instanceof CountStarStatisticsRule);
  }

  @Test void testConfigWithOperandSupplierReturnsSelf() {
    CountStarStatisticsRule.Config config = CountStarStatisticsRule.Config.DEFAULT;
    assertSame(config, config.withOperandSupplier(null));
  }

  @Test void testConfigWithDescriptionReturnsSelf() {
    CountStarStatisticsRule.Config config = CountStarStatisticsRule.Config.DEFAULT;
    assertSame(config, config.withDescription("test"));
  }

  @Test void testConfigWithRelBuilderFactoryReturnsSelf() {
    CountStarStatisticsRule.Config config = CountStarStatisticsRule.Config.DEFAULT;
    assertSame(config, config.withRelBuilderFactory(null));
  }

  @Test void testMultipleToRuleCreateDifferentInstances() {
    RelOptRule rule1 = CountStarStatisticsRule.Config.DEFAULT.toRule();
    RelOptRule rule2 = CountStarStatisticsRule.Config.DEFAULT.toRule();
    assertNotSame(rule1, rule2);
  }
}
