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
