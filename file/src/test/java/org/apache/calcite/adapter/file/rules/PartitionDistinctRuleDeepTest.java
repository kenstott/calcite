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
