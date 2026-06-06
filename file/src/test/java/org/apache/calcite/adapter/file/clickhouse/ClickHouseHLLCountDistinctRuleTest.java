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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.Aggregate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link ClickHouseHLLCountDistinctRule}.
 *
 * <p>Tests verify rule initialization and basic properties. Full integration tests
 * that exercise the rule with actual ClickHouse connections are in
 * {@link ClickHouseLocalIntegrationTest}.
 */
@Tag("unit")
class ClickHouseHLLCountDistinctRuleTest {

  @Test void testRuleInstanceExists() {
    assertNotNull(ClickHouseHLLCountDistinctRule.INSTANCE,
        "ClickHouseHLLCountDistinctRule.INSTANCE should not be null");
  }

  @Test void testRuleIsRelOptRule() {
    RelOptRule rule = ClickHouseHLLCountDistinctRule.INSTANCE;
    assertNotNull(rule, "Rule should be a valid RelOptRule");
  }

  @Test void testRuleName() {
    assertEquals("ClickHouseHLLCountDistinctRule",
        ClickHouseHLLCountDistinctRule.INSTANCE.toString());
  }

  @Test void testRuleOperandMatchesAggregate() {
    // The rule's operand should match Aggregate nodes
    RelOptRule rule = ClickHouseHLLCountDistinctRule.INSTANCE;
    Class<?> operandClass = rule.getOperand().getMatchedClass();
    assertEquals(Aggregate.class, operandClass,
        "Rule should match Aggregate nodes");
  }
}
