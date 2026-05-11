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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for {@link StatisticsBuilder}, {@link StatisticsConfig},
 * {@link StatisticsCache}, {@link TableStatistics}, {@link ColumnStatistics},
 * and {@link HyperLogLogSketch}.
 */
@Tag("unit")
public class StatisticsBuilderCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // StatisticsBuilder constructor tests
  // ====================================================================

  @Test void testDefaultConstructor() {
    StatisticsBuilder builder = new StatisticsBuilder();
    assertNotNull(builder);
  }

  @Test void testConstructorWithConfig() {
    StatisticsConfig config = StatisticsConfig.NO_HLL;
    StatisticsBuilder builder = new StatisticsBuilder(config);
    assertNotNull(builder);
  }

  @Test void testConstructorWithDefaultConfig() {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.DEFAULT);
    assertNotNull(builder);
  }

  @Test void testConstructorWithCustomConfig() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(false)
        .hllPrecision(10)
        .hllThreshold(500)
        .maxCacheAge(3600000L)
        .backgroundGeneration(false)
        .autoGenerateStatistics(false)
        .build();
    StatisticsBuilder builder = new StatisticsBuilder(config);
    assertNotNull(builder);
  }

  // ====================================================================
  // StatisticsBuilder.calculateSourceHash via reflection
  // ====================================================================

  @Test void testCalculateSourceHash() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod("calculateSourceHash", File.class);
    m.setAccessible(true);
    File testFile = tempDir.resolve("hash_test.txt").toFile();
    testFile.createNewFile();
    String hash = (String) m.invoke(builder, testFile);
    assertNotNull(hash);
    assertFalse(hash.isEmpty());
    assertEquals(32, hash.length()); // MD5 hex = 32 chars
  }

  @Test void testCalculateSourceHashDeterministic() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod("calculateSourceHash", File.class);
    m.setAccessible(true);
    File testFile = tempDir.resolve("determ_test.txt").toFile();
    testFile.createNewFile();
    String hash1 = (String) m.invoke(builder, testFile);
    String hash2 = (String) m.invoke(builder, testFile);
    assertEquals(hash1, hash2, "Same file should produce same hash");
  }

  @Test void testCalculateSourceHashDifferentFiles() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod("calculateSourceHash", File.class);
    m.setAccessible(true);

    File f1 = tempDir.resolve("file1.txt").toFile();
    File f2 = tempDir.resolve("file2.txt").toFile();
    f1.createNewFile();
    f2.createNewFile();

    String hash1 = (String) m.invoke(builder, f1);
    String hash2 = (String) m.invoke(builder, f2);
    // Different paths should produce different hashes
    assertNotEquals(hash1, hash2);
  }

  // ====================================================================
  // StatisticsBuilder.getStatisticsFile via reflection
  // ====================================================================

  @Test void testGetStatisticsFileAperioDir() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "getStatisticsFile", File.class, File.class);
    m.setAccessible(true);

    File sourceFile = new File("/data/test_data.parquet");
    File cacheDir = new File("/home/.aperio/test_schema/.parquet_cache");

    File result = (File) m.invoke(builder, sourceFile, cacheDir);
    assertNotNull(result);
    assertTrue(result.getName().endsWith(".aperio_stats"));
    assertTrue(result.getName().startsWith("test_data"));
  }

  @Test void testGetStatisticsFileNonAperioDir() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "getStatisticsFile", File.class, File.class);
    m.setAccessible(true);

    File sourceFile = new File("/data/source.csv");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();

    File result = (File) m.invoke(builder, sourceFile, cacheDir);
    assertNotNull(result);
    assertTrue(result.getName().equals("source.aperio_stats"));
  }

  @Test void testGetStatisticsFileStripsExtension() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "getStatisticsFile", File.class, File.class);
    m.setAccessible(true);

    File sourceFile = new File("/data/my_table.parquet");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();

    File result = (File) m.invoke(builder, sourceFile, cacheDir);
    assertEquals("my_table.aperio_stats", result.getName());
  }

  @Test void testGetStatisticsFileNoExtension() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "getStatisticsFile", File.class, File.class);
    m.setAccessible(true);

    File sourceFile = new File("/data/myfile");
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();

    File result = (File) m.invoke(builder, sourceFile, cacheDir);
    assertEquals("myfile.aperio_stats", result.getName());
  }

  // ====================================================================
  // StatisticsBuilder.shouldGenerateHLL via reflection
  // ====================================================================

  @Test void testShouldGenerateHLLDisabled() throws Exception {
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "shouldGenerateHLL",
        Class.forName(
            "org.apache.calcite.adapter.file.statistics.StatisticsBuilder$ColumnStatsBuilder"),
        long.class);
    m.setAccessible(true);

    // Create a ColumnStatsBuilder via reflection
    Class<?> csbClass = Class.forName(
        "org.apache.calcite.adapter.file.statistics.StatisticsBuilder$ColumnStatsBuilder");
    java.lang.reflect.Constructor<?> csbCtor = csbClass.getDeclaredConstructor(String.class);
    csbCtor.setAccessible(true);
    Object csb = csbCtor.newInstance("test_col");

    boolean result = (Boolean) m.invoke(builder, csb, 10000L);
    assertFalse(result, "HLL should not be generated when disabled");
  }

  // ====================================================================
  // StatisticsBuilder CSV statistics via reflection
  // ====================================================================

  @Test void testBuildCsvStatisticsEmpty() throws Exception {
    File csvFile = tempDir.resolve("empty.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      // Completely empty file (no header)
    }
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildCsvStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, csvFile, "hash123");
    assertNotNull(stats);
    assertEquals(0, stats.getRowCount());
  }

  @Test void testBuildCsvStatisticsHeaderOnly() throws Exception {
    File csvFile = tempDir.resolve("header_only.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name,value\n");
    }
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildCsvStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, csvFile, "hash456");
    assertNotNull(stats);
    assertEquals(0, stats.getRowCount());
  }

  @Test void testBuildCsvStatisticsWithData() throws Exception {
    File csvFile = tempDir.resolve("data.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name,value\n");
      fw.write("1,Alice,100.5\n");
      fw.write("2,Bob,200.3\n");
      fw.write("3,Charlie,300.7\n");
    }
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildCsvStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, csvFile, "hash789");
    assertNotNull(stats);
    assertEquals(3, stats.getRowCount());
    assertTrue(stats.getDataSize() > 0);
  }

  @Test void testBuildCsvStatisticsWithNulls() throws Exception {
    File csvFile = tempDir.resolve("nulls.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name,value\n");
      fw.write("1,,100\n");
      fw.write("2,Bob,NULL\n");
      fw.write("3,null,300\n");
    }
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildCsvStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, csvFile, "hashnull");
    assertNotNull(stats);
    assertEquals(3, stats.getRowCount());
  }

  @Test void testBuildCsvStatisticsWithHll() throws Exception {
    File csvFile = tempDir.resolve("hll_csv.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n");
      fw.write("1,Alice\n");
      fw.write("2,Bob\n");
    }
    StatisticsConfig hllConfig = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(10)
        .build();
    StatisticsBuilder builder = new StatisticsBuilder(hllConfig);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildCsvStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, csvFile, "hashHll");
    assertNotNull(stats);
    assertEquals(2, stats.getRowCount());
  }

  // ====================================================================
  // StatisticsBuilder.buildGenericStatistics via reflection
  // ====================================================================

  @Test void testBuildGenericStatisticsUnknownFormat() throws Exception {
    File unknownFile = tempDir.resolve("data.json").toFile();
    unknownFile.createNewFile();
    StatisticsBuilder builder = new StatisticsBuilder(StatisticsConfig.NO_HLL);
    Method m = StatisticsBuilder.class.getDeclaredMethod(
        "buildGenericStatistics", File.class, String.class);
    m.setAccessible(true);
    TableStatistics stats = (TableStatistics) m.invoke(builder, unknownFile, "hashgen");
    assertNotNull(stats);
    // For unknown formats, uses file-size based estimate
  }

  // ====================================================================
  // StatisticsConfig tests
  // ====================================================================

  @Test void testDefaultConfigValues() {
    StatisticsConfig config = StatisticsConfig.DEFAULT;
    assertTrue(config.isHllEnabled());
    assertEquals(14, config.getHllPrecision());
    assertEquals(1000, config.getHllThreshold());
    assertTrue(config.isBackgroundGeneration());
    assertTrue(config.isAutoGenerateStatistics());
  }

  @Test void testNoHllConfig() {
    StatisticsConfig config = StatisticsConfig.NO_HLL;
    assertFalse(config.isHllEnabled());
  }

  @Test void testBuilderDefaults() {
    StatisticsConfig config = new StatisticsConfig.Builder().build();
    assertTrue(config.isHllEnabled());
    assertEquals(14, config.getHllPrecision());
    assertEquals(1000, config.getHllThreshold());
    assertTrue(config.isBackgroundGeneration());
    assertTrue(config.isAutoGenerateStatistics());
  }

  @Test void testBuilderHllEnabled() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(false).build();
    assertFalse(config.isHllEnabled());
  }

  @Test void testBuilderHllPrecisionMin() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllPrecision(4).build();
    assertEquals(4, config.getHllPrecision());
  }

  @Test void testBuilderHllPrecisionMax() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllPrecision(16).build();
    assertEquals(16, config.getHllPrecision());
  }

  @Test void testBuilderHllPrecisionTooLow() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new StatisticsConfig.Builder().hllPrecision(3).build();
      }
    });
  }

  @Test void testBuilderHllPrecisionTooHigh() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new StatisticsConfig.Builder().hllPrecision(17).build();
      }
    });
  }

  @Test void testBuilderHllThreshold() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllThreshold(5000).build();
    assertEquals(5000, config.getHllThreshold());
  }

  @Test void testBuilderHllThresholdZero() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllThreshold(0).build();
    assertEquals(0, config.getHllThreshold());
  }

  @Test void testBuilderHllThresholdNegative() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new StatisticsConfig.Builder().hllThreshold(-1).build();
      }
    });
  }

  @Test void testBuilderMaxCacheAge() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .maxCacheAge(86400000L).build();
    assertEquals(86400000L, config.getMaxCacheAge());
  }

  @Test void testBuilderMaxCacheAgeZero() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .maxCacheAge(0L).build();
    assertEquals(0, config.getMaxCacheAge());
  }

  @Test void testBuilderMaxCacheAgeNegative() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new StatisticsConfig.Builder().maxCacheAge(-1).build();
      }
    });
  }

  @Test void testBuilderBackgroundGeneration() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .backgroundGeneration(false).build();
    assertFalse(config.isBackgroundGeneration());
  }

  @Test void testBuilderAutoGenerateStatistics() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .autoGenerateStatistics(false).build();
    assertFalse(config.isAutoGenerateStatistics());
  }

  @Test void testConfigToString() {
    StatisticsConfig config = StatisticsConfig.DEFAULT;
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("hllEnabled"));
    assertTrue(str.contains("hllPrecision"));
  }

  @Test void testFromSystemProperties() {
    StatisticsConfig config = StatisticsConfig.fromSystemProperties();
    assertNotNull(config);
  }

  @Test void testFromEnvironmentVariables() {
    StatisticsConfig config = StatisticsConfig.fromEnvironmentVariables();
    assertNotNull(config);
  }

  @Test void testGetEffectiveConfig() {
    StatisticsConfig config = StatisticsConfig.getEffectiveConfig();
    assertNotNull(config);
  }

  // ====================================================================
  // TableStatistics tests
  // ====================================================================

  @Test void testTableStatisticsBasic() {
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash123");
    assertEquals(1000, stats.getRowCount());
    assertEquals(50000, stats.getDataSize());
    assertNotNull(stats.getColumnStatistics());
    assertEquals("hash123", stats.getSourceHash());
    assertTrue(stats.getLastUpdated() > 0);
  }

  @Test void testTableStatisticsIsValidFor() {
    TableStatistics stats = new TableStatistics(100, 5000,
        new HashMap<String, ColumnStatistics>(), "abc123");
    assertTrue(stats.isValidFor("abc123"));
    assertFalse(stats.isValidFor("different"));
    assertFalse(stats.isValidFor(null));
  }

  @Test void testTableStatisticsNullHash() {
    TableStatistics stats = new TableStatistics(100, 5000,
        new HashMap<String, ColumnStatistics>(), null);
    assertFalse(stats.isValidFor("anything"));
    assertFalse(stats.isValidFor(null));
  }

  @Test void testTableStatisticsGetColumnStatistics() {
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 5, 1000, null));
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash");
    assertNotNull(stats.getColumnStatistics("col1"));
    assertNull(stats.getColumnStatistics("nonexistent"));
  }

  @Test void testTableStatisticsGetColumnStatisticsMap() {
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    cols.put("c1", new ColumnStatistics("c1", null, null, 0, 100, null));
    cols.put("c2", new ColumnStatistics("c2", null, null, 0, 100, null));
    TableStatistics stats = new TableStatistics(100, 5000, cols, "h");
    Map<String, ColumnStatistics> returned = stats.getColumnStatistics();
    assertEquals(2, returned.size());
    // Verify it is a copy
    returned.put("c3", new ColumnStatistics("c3", null, null, 0, 0, null));
    assertEquals(2, stats.getColumnStatistics().size());
  }

  @Test void testTableStatisticsSelectivityNoColumn() {
    TableStatistics stats = new TableStatistics(100, 5000,
        new HashMap<String, ColumnStatistics>(), "h");
    double sel = stats.getSelectivity("missing", "=", "val");
    assertEquals(0.1, sel, 0.001);
  }

  @Test void testTableStatisticsSelectivityWithColumn() {
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    cols.put("id", new ColumnStatistics("id", 1, 100, 0, 100, null));
    TableStatistics stats = new TableStatistics(100, 5000, cols, "h");
    double sel = stats.getSelectivity("id", "=", 50);
    assertTrue(sel > 0 && sel < 1);
  }

  @Test void testTableStatisticsCreateBasicEstimate() {
    TableStatistics stats = TableStatistics.createBasicEstimate(500);
    assertEquals(500, stats.getRowCount());
    assertEquals(50000, stats.getDataSize());
    assertNull(stats.getSourceHash());
    assertTrue(stats.getColumnStatistics().isEmpty());
  }

  @Test void testTableStatisticsCreateBasicEstimateZero() {
    TableStatistics stats = TableStatistics.createBasicEstimate(0);
    assertEquals(0, stats.getRowCount());
  }

  @Test void testTableStatisticsToString() {
    TableStatistics stats = new TableStatistics(100, 5000,
        new HashMap<String, ColumnStatistics>(), "h");
    String str = stats.toString();
    assertTrue(str.contains("100"));
    assertTrue(str.contains("5000"));
  }

  // ====================================================================
  // ColumnStatistics tests
  // ====================================================================

  @Test void testColumnStatisticsBasic() {
    ColumnStatistics cs = new ColumnStatistics("name", "A", "Z", 5, 100, null);
    assertEquals("name", cs.getColumnName());
    assertEquals("A", cs.getMinValue());
    assertEquals("Z", cs.getMaxValue());
    assertEquals(5, cs.getNullCount());
    assertEquals(100, cs.getTotalCount());
    assertNull(cs.getHllSketch());
  }

  @Test void testColumnStatisticsDistinctCountNoHll() {
    ColumnStatistics cs = new ColumnStatistics("c", null, null, 0, 500, null);
    long distinct = cs.getDistinctCount();
    assertEquals(500, distinct); // min(1000, 500) = 500
  }

  @Test void testColumnStatisticsDistinctCountLargeTotal() {
    ColumnStatistics cs = new ColumnStatistics("c", null, null, 0, 5000, null);
    long distinct = cs.getDistinctCount();
    assertEquals(1000, distinct); // min(1000, 5000) = 1000
  }

  @Test void testColumnStatisticsDistinctCountWithHll() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 50; i++) {
      hll.add("val_" + i);
    }
    ColumnStatistics cs = new ColumnStatistics("c", null, null, 0, 100, hll);
    long distinct = cs.getDistinctCount();
    assertTrue(distinct > 0);
  }

  @Test void testColumnStatisticsSelectivityEquality() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 0, 100, null);
    double sel = cs.getSelectivity("=", 50);
    // 1/distinct = 1/1000 for totalCount=100 without HLL
    assertTrue(sel > 0 && sel <= 1);
  }

  @Test void testColumnStatisticsSelectivityNotEqual() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 0, 100, null);
    double sel = cs.getSelectivity("!=", 50);
    assertTrue(sel > 0 && sel <= 1);
  }

  @Test void testColumnStatisticsSelectivityIsNull() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 20, 100, null);
    double sel = cs.getSelectivity("IS NULL", null);
    assertEquals(0.2, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityIsNotNull() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 20, 100, null);
    double sel = cs.getSelectivity("IS NOT NULL", null);
    assertEquals(0.8, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityNullWithOtherOp() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 0, 100, null);
    double sel = cs.getSelectivity("=", null);
    assertEquals(0.0, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityZeroTotal() {
    ColumnStatistics cs = new ColumnStatistics("c", null, null, 0, 0, null);
    double sel = cs.getSelectivity("=", "val");
    assertEquals(0.0, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityRangeNumeric() {
    ColumnStatistics cs = new ColumnStatistics("c", 0.0, 100.0, 0, 100, null);
    double selLt = cs.getSelectivity("<", 50.0);
    assertTrue(selLt >= 0 && selLt <= 1);
    double selGt = cs.getSelectivity(">", 50.0);
    assertTrue(selGt >= 0 && selGt <= 1);
  }

  @Test void testColumnStatisticsSelectivityRangeNoMinMax() {
    ColumnStatistics cs = new ColumnStatistics("c", null, null, 0, 100, null);
    double sel = cs.getSelectivity("<", 50);
    assertEquals(0.3, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityUnknownOp() {
    ColumnStatistics cs = new ColumnStatistics("c", 1, 100, 0, 100, null);
    double sel = cs.getSelectivity("LIKE", "abc");
    assertEquals(0.1, sel, 0.001);
  }

  @Test void testColumnStatisticsSelectivityRangeString() {
    ColumnStatistics cs = new ColumnStatistics("c", "A", "Z", 0, 100, null);
    double selLt = cs.getSelectivity("<", "M");
    assertTrue(selLt >= 0 && selLt <= 1);
  }

  @Test void testColumnStatisticsToString() {
    ColumnStatistics cs = new ColumnStatistics("col", 1, 100, 5, 200, null);
    String str = cs.toString();
    assertTrue(str.contains("col"));
    assertTrue(str.contains("100"));
  }

  // ====================================================================
  // HyperLogLogSketch tests
  // ====================================================================

  @Test void testHllDefaultPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    assertEquals(14, hll.getPrecision());
    assertEquals(16384, hll.getNumBuckets());
  }

  @Test void testHllCustomPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    assertEquals(10, hll.getPrecision());
    assertEquals(1024, hll.getNumBuckets());
  }

  @Test void testHllPrecisionTooLow() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new HyperLogLogSketch(3);
      }
    });
  }

  @Test void testHllPrecisionTooHigh() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        new HyperLogLogSketch(17);
      }
    });
  }

  @Test void testHllAddString() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.add("hello");
    hll.add("world");
    assertTrue(hll.getEstimate() >= 1);
  }

  @Test void testHllAddNumber() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.addNumber(42);
    hll.addNumber(84);
    assertTrue(hll.getEstimate() >= 1);
  }

  @Test void testHllAddNull() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.add(null);
    // Should not throw; null values are skipped
  }

  @Test void testHllAddStringMethod() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.addString("test");
    assertTrue(hll.getEstimate() >= 1);
  }

  @Test void testHllAddNumberMethod() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.addNumber(Long.valueOf(123L));
    assertTrue(hll.getEstimate() >= 1);
  }

  @Test void testHllEstimateEmpty() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    long est = hll.getEstimate();
    // Empty sketch should estimate 0
    assertEquals(0, est);
  }

  @Test void testHllEstimateAccuracy() {
    HyperLogLogSketch hll = new HyperLogLogSketch(14);
    for (int i = 0; i < 1000; i++) {
      hll.add("item_" + i);
    }
    long est = hll.getEstimate();
    // Should be within 10% for precision 14
    assertTrue(est >= 900 && est <= 1100,
        "Estimate " + est + " should be near 1000");
  }

  @Test void testHllMerge() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch(10);
    HyperLogLogSketch hll2 = new HyperLogLogSketch(10);
    for (int i = 0; i < 100; i++) {
      hll1.add("a_" + i);
    }
    for (int i = 0; i < 100; i++) {
      hll2.add("b_" + i);
    }
    hll1.merge(hll2);
    long est = hll1.getEstimate();
    assertTrue(est >= 150, "Merged estimate should reflect both sets");
  }

  @Test void testHllMergeDifferentPrecision() {
    final HyperLogLogSketch hll1 = new HyperLogLogSketch(10);
    final HyperLogLogSketch hll2 = new HyperLogLogSketch(12);
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        hll1.merge(hll2);
      }
    });
  }

  @Test void testHllGetBuckets() {
    HyperLogLogSketch hll = new HyperLogLogSketch(4);
    byte[] buckets = hll.getBuckets();
    assertNotNull(buckets);
    assertEquals(16, buckets.length);
  }

  @Test void testHllFromBuckets() {
    HyperLogLogSketch original = new HyperLogLogSketch(10);
    for (int i = 0; i < 50; i++) {
      original.add("v_" + i);
    }
    byte[] buckets = original.getBuckets();

    HyperLogLogSketch restored = new HyperLogLogSketch(10, buckets);
    assertEquals(original.getEstimate(), restored.getEstimate());
  }

  @Test void testHllFromEstimate() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(12345);
    assertEquals(12345, hll.getEstimate());
  }

  @Test void testHllMemoryUsage() {
    HyperLogLogSketch hll = new HyperLogLogSketch(14);
    int memUsage = hll.getMemoryUsage();
    assertEquals(16384 + 64, memUsage);
  }

  @Test void testHllToString() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    String str = hll.toString();
    assertTrue(str.contains("HyperLogLog"));
    assertTrue(str.contains("precision=10"));
  }

  // ====================================================================
  // StatisticsCache tests
  // ====================================================================

  @Test void testSaveAndLoadStatistics() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    cols.put("c1", new ColumnStatistics("c1", 1, 100, 5, 200, null));
    TableStatistics stats = new TableStatistics(200, 10000, cols, "hash_abc");

    File statsFile = tempDir.resolve("test.aperio_stats").toFile();
    StatisticsCache.saveStatistics(stats, statsFile);
    assertTrue(statsFile.exists());

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    assertEquals(200, loaded.getRowCount());
    assertEquals(10000, loaded.getDataSize());
    assertNotNull(loaded.getColumnStatistics("c1"));
  }

  @Test void testSaveAndLoadStatisticsWithHll() throws IOException {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 50; i++) {
      hll.add("val_" + i);
    }
    Map<String, ColumnStatistics> cols = new HashMap<String, ColumnStatistics>();
    cols.put("c1", new ColumnStatistics("c1", "A", "Z", 2, 50, hll));
    TableStatistics stats = new TableStatistics(50, 5000, cols, "hashWithHll");

    File statsFile = tempDir.resolve("hll_stats.aperio_stats").toFile();
    StatisticsCache.saveStatistics(stats, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics loadedCol = loaded.getColumnStatistics("c1");
    assertNotNull(loadedCol);
    assertNotNull(loadedCol.getHllSketch());
    assertTrue(loadedCol.getHllSketch().getEstimate() > 0);
  }

  @Test void testLoadStatisticsNonexistent() {
    File f = tempDir.resolve("nonexistent.aperio_stats").toFile();
    assertThrows(IOException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() throws Throwable {
        StatisticsCache.loadStatistics(f);
      }
    });
  }

  @Test void testSaveHllSketchAndLoad() throws IOException {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 30; i++) {
      hll.add("item_" + i);
    }

    File hllFile = tempDir.resolve("test.hll").toFile();
    StatisticsCache.saveHLLSketch(hll, hllFile);
    assertTrue(hllFile.exists());

    HyperLogLogSketch loaded = StatisticsCache.loadHLLSketch(hllFile);
    assertNotNull(loaded);
    assertEquals(hll.getEstimate(), loaded.getEstimate());
  }

  @Test void testLoadHllSketchNonexistent() {
    File f = tempDir.resolve("missing.hll").toFile();
    assertThrows(IOException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() throws Throwable {
        StatisticsCache.loadHLLSketch(f);
      }
    });
  }

  @Test void testCleanupOldStatisticsNoFiles() {
    File cacheDir = tempDir.resolve("empty_cache").toFile();
    cacheDir.mkdirs();
    // Should not throw
    StatisticsCache.cleanupOldStatistics(cacheDir, 1000);
  }

  @Test void testCleanupOldStatisticsNonexistent() {
    File cacheDir = tempDir.resolve("nonexistent_cache").toFile();
    // Should not throw
    StatisticsCache.cleanupOldStatistics(cacheDir, 1000);
  }

  @Test void testGetStatisticsCacheSizeEmpty() {
    File cacheDir = tempDir.resolve("empty_size").toFile();
    cacheDir.mkdirs();
    long size = StatisticsCache.getStatisticsCacheSize(cacheDir);
    assertEquals(0, size);
  }

  @Test void testGetStatisticsCacheSizeNonexistent() {
    File cacheDir = tempDir.resolve("no_exist").toFile();
    long size = StatisticsCache.getStatisticsCacheSize(cacheDir);
    assertEquals(0, size);
  }
}
