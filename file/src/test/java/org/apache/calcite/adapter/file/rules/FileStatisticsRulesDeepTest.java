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

import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.TableStatistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep unit tests for {@link FileStatisticsRules} covering all branches
 * of the utility methods and constant values.
 */
@Tag("unit")
public class FileStatisticsRulesDeepTest {

  // ===== String constants =====

  @Test void testFilterPushdownNameConstant() {
    assertEquals("FileStatisticsRules:FilterPushdown",
        FileStatisticsRules.STATISTICS_FILTER_PUSHDOWN_NAME);
  }

  @Test void testJoinReorderNameConstant() {
    assertEquals("FileStatisticsRules:JoinReorder",
        FileStatisticsRules.STATISTICS_JOIN_REORDER_NAME);
  }

  @Test void testColumnPruningNameConstant() {
    assertEquals("FileStatisticsRules:ColumnPruning",
        FileStatisticsRules.STATISTICS_COLUMN_PRUNING_NAME);
  }

  // ===== estimateSelectivity =====

  @Test void testEstimateSelectivityNullStats() {
    double result = FileStatisticsRules.estimateSelectivity("some condition", null);
    assertEquals(0.3, result, 0.001);
  }

  @Test void testEstimateSelectivityNullCondition() {
    double result = FileStatisticsRules.estimateSelectivity(null, null);
    assertEquals(0.3, result, 0.001);
  }

  @Test void testEstimateSelectivityWithStats() {
    Map<String, ColumnStatistics> colStats = new HashMap<>();
    colStats.put("col1", new ColumnStatistics("col1", 1, 100, 0L, 100L, null));
    TableStatistics stats = new TableStatistics(100L, 1024L, colStats, null);

    double result = FileStatisticsRules.estimateSelectivity("condition", stats);
    assertEquals(0.3, result, 0.001); // Currently returns 0.3 regardless
  }

  @Test void testEstimateSelectivityBothNull() {
    double result = FileStatisticsRules.estimateSelectivity(null, null);
    assertEquals(0.3, result, 0.001);
  }

  // ===== getTableStatistics =====

  @Test void testGetTableStatisticsNull() {
    assertNull(FileStatisticsRules.getTableStatistics(null));
  }

  @Test void testGetTableStatisticsNonScanObject() {
    assertNull(FileStatisticsRules.getTableStatistics("not a scan"));
  }

  @Test void testGetTableStatisticsIntegerInput() {
    assertNull(FileStatisticsRules.getTableStatistics(42));
  }

  @Test void testGetTableStatisticsListInput() {
    assertNull(FileStatisticsRules.getTableStatistics(Collections.emptyList()));
  }
}
