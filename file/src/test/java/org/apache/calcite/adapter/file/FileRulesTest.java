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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link FileRules}.
 */
@Tag("unit")
class FileRulesTest {

  @Test void testProjectScanRuleExists() {
    assertNotNull(FileRules.PROJECT_SCAN);
  }

  @Test void testHllCountDistinctRuleExists() {
    assertNotNull(FileRules.HLL_COUNT_DISTINCT);
  }

  @Test void testAlternatePartitionSelectionRuleExists() {
    assertNotNull(FileRules.ALTERNATE_PARTITION_SELECTION);
  }

  @Test void testCountStarStatisticsRuleExists() {
    assertNotNull(FileRules.COUNT_STAR_STATISTICS);
  }

  @Test void testPartitionDistinctRuleExists() {
    assertNotNull(FileRules.PARTITION_DISTINCT);
  }
}
