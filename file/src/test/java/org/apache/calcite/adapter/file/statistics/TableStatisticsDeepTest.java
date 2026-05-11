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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for TableStatistics.
 */
@Tag("unit")
public class TableStatisticsDeepTest {

  @Test void testBasicGetters() {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 0, 1000, null));
    TableStatistics stats = new TableStatistics(5000, 1024000, cols, "abc123");

    assertEquals(5000, stats.getRowCount());
    assertEquals(1024000, stats.getDataSize());
    assertEquals("abc123", stats.getSourceHash());
    assertTrue(stats.getLastUpdated() > 0);
  }

  @Test void testGetColumnStatisticsByName() {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    ColumnStatistics colStats = new ColumnStatistics("col1", 1, 100, 5, 1000, null);
    cols.put("col1", colStats);
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash");

    assertNotNull(stats.getColumnStatistics("col1"));
    assertEquals("col1", stats.getColumnStatistics("col1").getColumnName());
    assertNull(stats.getColumnStatistics("nonexistent"));
  }

  @Test void testGetAllColumnStatisticsReturnsDefensiveCopy() {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 0, 1000, null));
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash");

    Map<String, ColumnStatistics> returned = stats.getColumnStatistics();
    returned.put("col2", new ColumnStatistics("col2", 1, 50, 0, 500, null));

    // Original should be unchanged
    assertNull(stats.getColumnStatistics("col2"));
  }

  @Test void testIsValidForMatchingHash() {
    TableStatistics stats = new TableStatistics(1000, 50000, new HashMap<>(), "hash123");
    assertTrue(stats.isValidFor("hash123"));
  }

  @Test void testIsValidForDifferentHash() {
    TableStatistics stats = new TableStatistics(1000, 50000, new HashMap<>(), "hash123");
    assertFalse(stats.isValidFor("different"));
  }

  @Test void testIsValidForNullSourceHash() {
    TableStatistics stats = new TableStatistics(1000, 50000, new HashMap<>(), null);
    assertFalse(stats.isValidFor("anything"));
  }

  @Test void testIsValidForNullArgument() {
    TableStatistics stats = new TableStatistics(1000, 50000, new HashMap<>(), "hash123");
    assertFalse(stats.isValidFor(null));
  }

  // ===== Selectivity delegation =====

  @Test void testSelectivityDelegatesToColumn() {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(50);
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 0, 1000, hll));
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash");

    double sel = stats.getSelectivity("col1", "=", 42);
    assertEquals(1.0 / 50, sel, 0.001); // 1/distinct
  }

  @Test void testSelectivityUnknownColumn() {
    TableStatistics stats = new TableStatistics(1000, 50000, new HashMap<>(), "hash");
    double sel = stats.getSelectivity("unknown", "=", 42);
    assertEquals(0.1, sel, 0.001); // Default for unknown column
  }

  // ===== createBasicEstimate =====

  @Test void testCreateBasicEstimate() {
    TableStatistics basic = TableStatistics.createBasicEstimate(500);
    assertEquals(500, basic.getRowCount());
    assertEquals(50000, basic.getDataSize()); // 500 * 100
    assertNull(basic.getSourceHash());
    assertTrue(basic.getColumnStatistics().isEmpty());
  }

  @Test void testCreateBasicEstimateZeroRows() {
    TableStatistics basic = TableStatistics.createBasicEstimate(0);
    assertEquals(0, basic.getRowCount());
    assertEquals(0, basic.getDataSize());
  }

  // ===== toString =====

  @Test void testToString() {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 0, 1000, null));
    cols.put("col2", new ColumnStatistics("col2", "a", "z", 0, 1000, null));
    TableStatistics stats = new TableStatistics(5000, 1024000, cols, "hash");

    String str = stats.toString();
    assertTrue(str.contains("rowCount=5000"));
    assertTrue(str.contains("dataSize=1024000"));
    assertTrue(str.contains("columns=2"));
  }
}
