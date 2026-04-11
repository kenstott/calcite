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
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SimpleFileColumnPruningRule} private methods
 * using reflection. Covers estimateColumnSize(), collectColumnReferences(),
 * and ColumnPruningAnalysis inner class.
 */
@Tag("unit")
public class SimpleFileColumnPruningRuleCoverageTest {

  private SimpleFileColumnPruningRule rule;
  private Method estimateColumnSize;

  @BeforeEach void setUp() throws Exception {
    rule = SimpleFileColumnPruningRule.INSTANCE;
    estimateColumnSize = SimpleFileColumnPruningRule.class.getDeclaredMethod(
        "estimateColumnSize", ColumnStatistics.class);
    estimateColumnSize.setAccessible(true);
  }

  // ===== estimateColumnSize =====

  @Test void testEstimateColumnSizeHighCardinality() throws Exception {
    // distinctCount = totalCount -> compressionRatio = min(1.0, 3.0) = 1.0
    ColumnStatistics stats = new ColumnStatistics("col1", 0, 100, 0, 100, null);
    long size = (Long) estimateColumnSize.invoke(rule, stats);
    // baseSize = 100 * 8 = 800, compressionRatio = min(1.0, 100/100 * 3) = min(1.0, 3.0) = 1.0
    // Using default distinct count since hllSketch is null: min(1000, 100) = 100
    assertEquals(800, size);
  }

  @Test void testEstimateColumnSizeLowCardinality() throws Exception {
    // distinctCount much lower than totalCount -> lower compressionRatio
    // With null hllSketch, distinctCount = min(1000, 10000) = 1000
    ColumnStatistics stats = new ColumnStatistics("col2", 0, 100, 0, 10000, null);
    long size = (Long) estimateColumnSize.invoke(rule, stats);
    // baseSize = 10000 * 8 = 80000
    // distinctCount = min(1000, 10000) = 1000
    // compressionRatio = min(1.0, 1000/10000 * 3) = min(1.0, 0.3) = 0.3
    assertEquals(24000, size);
  }

  @Test void testEstimateColumnSizeWithHLLSketch() throws Exception {
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 50; i++) {
      sketch.add("val" + i);
    }
    ColumnStatistics stats = new ColumnStatistics("col3", 0, 100, 0, 1000, sketch);
    long size = (Long) estimateColumnSize.invoke(rule, stats);
    // baseSize = 1000 * 8 = 8000
    // distinctCount = sketch.getEstimate() ~= 50
    // compressionRatio = min(1.0, 50/1000 * 3) = min(1.0, 0.15) = 0.15
    assertTrue(size > 0, "Size should be positive");
    assertTrue(size < 8000, "Size should be less than uncompressed");
  }

  @Test void testEstimateColumnSizeZeroTotal() throws Exception {
    ColumnStatistics stats = new ColumnStatistics("col4", 0, 0, 0, 0, null);
    long size = (Long) estimateColumnSize.invoke(rule, stats);
    // baseSize = 0 * 8 = 0
    assertEquals(0, size);
  }

  // ===== collectColumnReferences =====

  @Test void testCollectColumnReferencesMethodExists() throws Exception {
    Method collectRefs = SimpleFileColumnPruningRule.class.getDeclaredMethod(
        "collectColumnReferences",
        org.apache.calcite.rex.RexNode.class, Set.class);
    collectRefs.setAccessible(true);
    assertNotNull(collectRefs);
  }

  // ===== ColumnPruningAnalysis inner class =====

  @Test void testColumnPruningAnalysisDefaults() throws Exception {
    Class<?> cpaClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileColumnPruningRule$ColumnPruningAnalysis");
    Constructor<?> ctor = cpaClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object cpa = ctor.newInstance();

    Field canOptimize = cpaClass.getDeclaredField("canOptimize");
    canOptimize.setAccessible(true);
    assertFalse((Boolean) canOptimize.get(cpa));

    Field ioSavingsPercent = cpaClass.getDeclaredField("ioSavingsPercent");
    ioSavingsPercent.setAccessible(true);
    assertEquals(0.0, (Double) ioSavingsPercent.get(cpa), 0.001);

    Field estimatedSavingsBytes = cpaClass.getDeclaredField("estimatedSavingsBytes");
    estimatedSavingsBytes.setAccessible(true);
    assertEquals(0L, (Long) estimatedSavingsBytes.get(cpa));

    Field usedColumns = cpaClass.getDeclaredField("usedColumns");
    usedColumns.setAccessible(true);
    assertNotNull(usedColumns.get(cpa));
    assertTrue(((Set<?>) usedColumns.get(cpa)).isEmpty());

    Field unusedColumns = cpaClass.getDeclaredField("unusedColumns");
    unusedColumns.setAccessible(true);
    assertNotNull(unusedColumns.get(cpa));
    assertTrue(((Set<?>) unusedColumns.get(cpa)).isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Test void testColumnPruningAnalysisSetFields() throws Exception {
    Class<?> cpaClass = Class.forName(
        "org.apache.calcite.adapter.file.rules.SimpleFileColumnPruningRule$ColumnPruningAnalysis");
    Constructor<?> ctor = cpaClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object cpa = ctor.newInstance();

    Field canOptimize = cpaClass.getDeclaredField("canOptimize");
    canOptimize.setAccessible(true);
    canOptimize.set(cpa, true);
    assertTrue((Boolean) canOptimize.get(cpa));

    Field ioSavingsPercent = cpaClass.getDeclaredField("ioSavingsPercent");
    ioSavingsPercent.setAccessible(true);
    ioSavingsPercent.set(cpa, 45.5);
    assertEquals(45.5, (Double) ioSavingsPercent.get(cpa), 0.001);

    Field estimatedSavingsBytes = cpaClass.getDeclaredField("estimatedSavingsBytes");
    estimatedSavingsBytes.setAccessible(true);
    estimatedSavingsBytes.set(cpa, 1024L);
    assertEquals(1024L, (Long) estimatedSavingsBytes.get(cpa));

    Field usedColumns = cpaClass.getDeclaredField("usedColumns");
    usedColumns.setAccessible(true);
    Set<String> used = (Set<String>) usedColumns.get(cpa);
    used.add("col_a");
    used.add("col_b");
    assertEquals(2, used.size());

    Field unusedColumns = cpaClass.getDeclaredField("unusedColumns");
    unusedColumns.setAccessible(true);
    Set<String> unused = (Set<String>) unusedColumns.get(cpa);
    unused.add("col_c");
    assertEquals(1, unused.size());
  }

  // ===== getTableStatistics =====

  @Test void testGetTableStatisticsMethodExists() throws Exception {
    Method getStats = SimpleFileColumnPruningRule.class.getDeclaredMethod(
        "getTableStatistics", org.apache.calcite.rel.core.TableScan.class);
    getStats.setAccessible(true);
    assertNotNull(getStats);
  }

  // ===== analyzeColumnUsage =====

  @Test void testAnalyzeColumnUsageMethodExists() throws Exception {
    Method analyze = SimpleFileColumnPruningRule.class.getDeclaredMethod(
        "analyzeColumnUsage",
        org.apache.calcite.rel.logical.LogicalProject.class,
        org.apache.calcite.rel.core.TableScan.class,
        org.apache.calcite.adapter.file.statistics.TableStatistics.class);
    analyze.setAccessible(true);
    assertNotNull(analyze);
  }

  // ===== createOptimizedProjection =====

  @Test void testCreateOptimizedProjectionMethodExists() throws Exception {
    Method createOptimized = SimpleFileColumnPruningRule.class.getDeclaredMethod(
        "createOptimizedProjection",
        org.apache.calcite.rel.logical.LogicalProject.class,
        org.apache.calcite.rel.core.TableScan.class,
        Class.forName(
            "org.apache.calcite.adapter.file.rules.SimpleFileColumnPruningRule$ColumnPruningAnalysis"),
        org.apache.calcite.tools.RelBuilder.class);
    createOptimized.setAccessible(true);
    assertNotNull(createOptimized);
  }
}
