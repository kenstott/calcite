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
 * Deep unit tests for {@link SimpleFileJoinReorderRule} covering the
 * INSTANCE singleton and Config interface.
 */
@Tag("unit")
public class SimpleFileJoinReorderRuleDeepTest {

  @Test void testInstanceNotNull() {
    assertNotNull(SimpleFileJoinReorderRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(SimpleFileJoinReorderRule.INSTANCE instanceof SimpleFileJoinReorderRule);
  }

  @Test void testInstanceIsRelOptRule() {
    assertTrue(SimpleFileJoinReorderRule.INSTANCE instanceof RelOptRule);
  }

  @Test void testInstanceSameOnMultipleAccess() {
    SimpleFileJoinReorderRule instance1 = SimpleFileJoinReorderRule.INSTANCE;
    SimpleFileJoinReorderRule instance2 = SimpleFileJoinReorderRule.INSTANCE;
    assertSame(instance1, instance2);
  }
}
