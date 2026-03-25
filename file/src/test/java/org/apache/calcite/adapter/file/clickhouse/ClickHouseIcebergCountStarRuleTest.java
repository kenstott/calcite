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
