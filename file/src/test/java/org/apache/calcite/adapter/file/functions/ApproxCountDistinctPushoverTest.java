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
package org.apache.calcite.adapter.file.functions;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Additional unit tests for {@link ApproxCountDistinct} to push
 * functions package coverage past 75%.
 */
@Tag("unit")
public class ApproxCountDistinctPushoverTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ApproxCountDistinctPushoverTest.class);

  @Test @DisplayName("getAccumulatorType returns Accumulator class")
  void testGetAccumulatorType() {
    ApproxCountDistinct fn = new ApproxCountDistinct("col1", null);
    Class<?> accType = fn.getAccumulatorType();
    assertEquals(ApproxCountDistinct.Accumulator.class, accType);
    LOGGER.debug("Accumulator type: {}", accType);
  }

  @Test @DisplayName("getParameters returns non-null list")
  void testGetParametersIsNotNull() {
    ApproxCountDistinct fn = new ApproxCountDistinct("testCol", null);
    assertNotNull(fn.getParameters());
    assertEquals(0, fn.getParameters().size());
  }

  @Test @DisplayName("HLLAccumulator with single value returns estimate >= 1")
  void testHllAccumulatorSingleValue() {
    ApproxCountDistinct.HLLAccumulator acc = new ApproxCountDistinct.HLLAccumulator();
    acc.add("only_value");
    long result = acc.getResult();
    assertEquals(1L, result, "Single unique value should estimate 1");
  }

  @Test @DisplayName("PrecomputedAccumulator multiple add calls still use sketch")
  void testPrecomputedAccumulatorMultipleAdds() {
    org.apache.calcite.adapter.file.statistics.HyperLogLogSketch sketch =
        org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(100L);
    ApproxCountDistinct.PrecomputedAccumulator acc =
        new ApproxCountDistinct.PrecomputedAccumulator(sketch);

    // Multiple adds - all ignored
    for (int i = 0; i < 10; i++) {
      acc.add("value_" + i);
    }

    assertEquals(100L, acc.getResult(),
        "Precomputed should always return sketch estimate");
  }
}
