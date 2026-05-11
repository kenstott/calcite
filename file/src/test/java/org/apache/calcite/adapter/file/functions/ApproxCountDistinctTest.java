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
package org.apache.calcite.adapter.file.functions;

import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ApproxCountDistinct} and its inner accumulator classes.
 */
@Tag("unit")
public class ApproxCountDistinctTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApproxCountDistinctTest.class);

  @Test
  @DisplayName("HLLAccumulator with 100 distinct values estimates within 20%")
  void testHllAccumulatorDistinctValues() {
    ApproxCountDistinct.HLLAccumulator acc = new ApproxCountDistinct.HLLAccumulator();

    for (int i = 0; i < 100; i++) {
      acc.add("value_" + i);
    }

    long estimate = acc.getResult();
    LOGGER.debug("HLL estimate for 100 distinct values: {}", estimate);
    assertTrue(estimate >= 80 && estimate <= 120,
        "Estimate " + estimate + " should be within 20% of 100");
  }

  @Test
  @DisplayName("HLLAccumulator with duplicates estimates correctly")
  void testHllAccumulatorDuplicateValues() {
    ApproxCountDistinct.HLLAccumulator acc = new ApproxCountDistinct.HLLAccumulator();

    for (int rep = 0; rep < 1000; rep++) {
      for (int i = 0; i < 5; i++) {
        acc.add("dup_" + i);
      }
    }

    long estimate = acc.getResult();
    LOGGER.debug("HLL estimate for 5 distinct values x1000: {}", estimate);
    assertTrue(estimate >= 3 && estimate <= 8,
        "Estimate " + estimate + " should approximate 5");
  }

  @Test
  @DisplayName("HLLAccumulator ignores nulls")
  void testHllAccumulatorNullsIgnored() {
    ApproxCountDistinct.HLLAccumulator acc = new ApproxCountDistinct.HLLAccumulator();

    for (int i = 0; i < 50; i++) {
      acc.add("val_" + i);
    }
    for (int i = 0; i < 50; i++) {
      acc.add(null);
    }

    long estimate = acc.getResult();
    LOGGER.debug("HLL estimate for 50 distinct + 50 nulls: {}", estimate);
    assertTrue(estimate >= 40 && estimate <= 60,
        "Estimate " + estimate + " should approximate 50 (nulls ignored)");
  }

  @Test
  @DisplayName("HLLAccumulator empty returns zero")
  void testHllAccumulatorEmptyReturnsZero() {
    ApproxCountDistinct.HLLAccumulator acc = new ApproxCountDistinct.HLLAccumulator();

    long estimate = acc.getResult();
    assertEquals(0L, estimate, "Empty accumulator should return 0");
  }

  @Test
  @DisplayName("PrecomputedAccumulator ignores runtime adds and returns sketch estimate")
  void testPrecomputedAccumulatorIgnoresAdds() {
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(42L);
    ApproxCountDistinct.PrecomputedAccumulator acc =
        new ApproxCountDistinct.PrecomputedAccumulator(sketch);

    // Add values that should be ignored
    acc.add("ignored_1");
    acc.add("ignored_2");
    acc.add("ignored_3");

    assertEquals(42L, acc.getResult(),
        "PrecomputedAccumulator should return sketch estimate regardless of added values");
  }

  @Test
  @DisplayName("PrecomputedAccumulator returns sketch cardinality")
  void testPrecomputedAccumulatorReturnsSketchCardinality() {
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(999L);
    ApproxCountDistinct.PrecomputedAccumulator acc =
        new ApproxCountDistinct.PrecomputedAccumulator(sketch);

    assertEquals(999L, acc.getResult());
  }

  @Test
  @DisplayName("createAccumulator with null tableScan returns HLLAccumulator")
  void testCreateAccumulatorNullTableScan() {
    ApproxCountDistinct fn = new ApproxCountDistinct("col1", null);

    ApproxCountDistinct.Accumulator acc = fn.createAccumulator();
    assertTrue(acc instanceof ApproxCountDistinct.HLLAccumulator,
        "With null tableScan, should create HLLAccumulator, got "
            + acc.getClass().getSimpleName());
  }

  @Test
  @DisplayName("Return type is Long")
  void testReturnTypeIsLong() {
    ApproxCountDistinct fn = new ApproxCountDistinct("col1", null);

    assertEquals(Long.class, fn.getReturnType());
  }

  @Test
  @DisplayName("getParameters returns empty list")
  void testGetParametersReturnsEmptyList() {
    ApproxCountDistinct fn = new ApproxCountDistinct("col1", null);

    assertTrue(fn.getParameters().isEmpty(),
        "getParameters should return empty list");
  }

  @Test
  @DisplayName("Both accumulator types fulfill add/getResult contract")
  void testAccumulatorContract() {
    // HLLAccumulator contract
    ApproxCountDistinct.HLLAccumulator hll = new ApproxCountDistinct.HLLAccumulator();
    hll.add("a");
    hll.add("b");
    long hllResult = hll.getResult();
    assertTrue(hllResult > 0, "HLLAccumulator getResult should be positive after adds");

    // PrecomputedAccumulator contract
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(10L);
    ApproxCountDistinct.PrecomputedAccumulator pre =
        new ApproxCountDistinct.PrecomputedAccumulator(sketch);
    pre.add("ignored");
    long preResult = pre.getResult();
    assertEquals(10L, preResult, "PrecomputedAccumulator should return sketch estimate");
  }
}
