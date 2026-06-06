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
 * Unit tests for {@link ClickHouseIcebergCountStarRule}.
 *
 * <p>Tests verify rule initialization and basic properties. The Iceberg-specific
 * optimization only fires with actual Iceberg metadata (ICEBERG_PARQUET tables
 * with cached row counts), so full integration tests require a production-like
 * environment with ClickHouse and Iceberg tables.
 */
@Tag("unit")
class ClickHouseIcebergCountStarRuleTest {

  @Test void testRuleInstanceExists() {
    assertNotNull(ClickHouseIcebergCountStarRule.INSTANCE,
        "ClickHouseIcebergCountStarRule.INSTANCE should not be null");
  }

  @Test void testRuleIsRelOptRule() {
    RelOptRule rule = ClickHouseIcebergCountStarRule.INSTANCE;
    assertNotNull(rule, "Rule should be a valid RelOptRule");
  }

  @Test void testRuleName() {
    assertEquals("ClickHouseIcebergCountStarRule",
        ClickHouseIcebergCountStarRule.INSTANCE.toString());
  }

  @Test void testRuleOperandMatchesAggregate() {
    RelOptRule rule = ClickHouseIcebergCountStarRule.INSTANCE;
    Class<?> operandClass = rule.getOperand().getMatchedClass();
    assertEquals(Aggregate.class, operandClass,
        "Rule should match Aggregate nodes");
  }
}
