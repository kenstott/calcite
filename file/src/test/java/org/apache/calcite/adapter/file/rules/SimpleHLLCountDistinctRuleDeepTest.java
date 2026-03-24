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
 * Deep unit tests for {@link SimpleHLLCountDistinctRule} covering Config class,
 * both INSTANCE and APPROX_ONLY_INSTANCE, and rule configuration methods.
 */
@Tag("unit")
public class SimpleHLLCountDistinctRuleDeepTest {

  // ===== INSTANCE =====

  @Test void testInstanceNotNull() {
    assertNotNull(SimpleHLLCountDistinctRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(SimpleHLLCountDistinctRule.INSTANCE instanceof SimpleHLLCountDistinctRule);
  }

  @Test void testInstanceIsRelOptRule() {
    assertTrue(SimpleHLLCountDistinctRule.INSTANCE instanceof RelOptRule);
  }

  // ===== APPROX_ONLY_INSTANCE =====

  @Test void testApproxOnlyInstanceNotNull() {
    assertNotNull(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE);
  }

  @Test void testApproxOnlyInstanceType() {
    assertTrue(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE instanceof SimpleHLLCountDistinctRule);
  }

  @Test void testInstancesAreDifferent() {
    assertNotSame(SimpleHLLCountDistinctRule.INSTANCE,
        SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE);
  }

  // ===== Config =====

  @Test void testConfigDefault() {
    assertNotNull(SimpleHLLCountDistinctRule.Config.DEFAULT);
  }

  @Test void testConfigApproxOnly() {
    assertNotNull(SimpleHLLCountDistinctRule.Config.APPROX_ONLY);
  }

  @Test void testConfigDefaultApproxOnlyFalse() {
    assertFalse(SimpleHLLCountDistinctRule.Config.DEFAULT.approxOnly());
  }

  @Test void testConfigApproxOnlyTrue() {
    assertTrue(SimpleHLLCountDistinctRule.Config.APPROX_ONLY.approxOnly());
  }

  @Test void testConfigDescription() {
    assertEquals("SimpleHLLCountDistinctRule",
        SimpleHLLCountDistinctRule.Config.DEFAULT.description());
  }

  @Test void testConfigApproxOnlyDescription() {
    assertEquals("SimpleHLLCountDistinctRule",
        SimpleHLLCountDistinctRule.Config.APPROX_ONLY.description());
  }

  @Test void testConfigOperandSupplier() {
    assertNotNull(SimpleHLLCountDistinctRule.Config.DEFAULT.operandSupplier());
  }

  @Test void testConfigToRule() {
    RelOptRule rule = SimpleHLLCountDistinctRule.Config.DEFAULT.toRule();
    assertNotNull(rule);
    assertTrue(rule instanceof SimpleHLLCountDistinctRule);
  }

  @Test void testConfigApproxOnlyToRule() {
    RelOptRule rule = SimpleHLLCountDistinctRule.Config.APPROX_ONLY.toRule();
    assertNotNull(rule);
    assertTrue(rule instanceof SimpleHLLCountDistinctRule);
  }

  @Test void testConfigWithOperandSupplierReturnsSelf() {
    SimpleHLLCountDistinctRule.Config config = SimpleHLLCountDistinctRule.Config.DEFAULT;
    assertSame(config, config.withOperandSupplier(null));
  }

  @Test void testConfigWithDescriptionReturnsSelf() {
    SimpleHLLCountDistinctRule.Config config = SimpleHLLCountDistinctRule.Config.DEFAULT;
    assertSame(config, config.withDescription("test"));
  }

  @Test void testConfigWithRelBuilderFactoryReturnsSelf() {
    SimpleHLLCountDistinctRule.Config config = SimpleHLLCountDistinctRule.Config.DEFAULT;
    assertSame(config, config.withRelBuilderFactory(null));
  }

  @Test void testConfigDefaultAndApproxOnlyAreDifferent() {
    assertNotSame(SimpleHLLCountDistinctRule.Config.DEFAULT,
        SimpleHLLCountDistinctRule.Config.APPROX_ONLY);
  }
}
