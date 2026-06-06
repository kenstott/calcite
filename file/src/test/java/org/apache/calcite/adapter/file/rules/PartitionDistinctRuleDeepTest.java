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
 * Deep unit tests for {@link PartitionDistinctRule} covering the Config class,
 * singleton instance, and rule configuration.
 */
@Tag("unit")
public class PartitionDistinctRuleDeepTest {

  // ===== INSTANCE =====

  @Test void testInstanceNotNull() {
    assertNotNull(PartitionDistinctRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(PartitionDistinctRule.INSTANCE instanceof PartitionDistinctRule);
  }

  @Test void testInstanceIsRelOptRule() {
    assertTrue(PartitionDistinctRule.INSTANCE instanceof RelOptRule);
  }

  // ===== Config =====

  @Test void testConfigDefault() {
    assertNotNull(PartitionDistinctRule.Config.DEFAULT);
  }

  @Test void testConfigDescription() {
    assertEquals("PartitionDistinctRule",
        PartitionDistinctRule.Config.DEFAULT.description());
  }

  @Test void testConfigOperandSupplier() {
    assertNotNull(PartitionDistinctRule.Config.DEFAULT.operandSupplier());
  }

  @Test void testConfigToRule() {
    RelOptRule rule = PartitionDistinctRule.Config.DEFAULT.toRule();
    assertNotNull(rule);
    assertTrue(rule instanceof PartitionDistinctRule);
  }

  @Test void testConfigWithOperandSupplierReturnsSelf() {
    PartitionDistinctRule.Config config = PartitionDistinctRule.Config.DEFAULT;
    assertSame(config, config.withOperandSupplier(null));
  }

  @Test void testConfigWithDescriptionReturnsSelf() {
    PartitionDistinctRule.Config config = PartitionDistinctRule.Config.DEFAULT;
    assertSame(config, config.withDescription("test"));
  }

  @Test void testConfigWithRelBuilderFactoryReturnsSelf() {
    PartitionDistinctRule.Config config = PartitionDistinctRule.Config.DEFAULT;
    assertSame(config, config.withRelBuilderFactory(null));
  }
}
