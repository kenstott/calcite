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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for StatisticsCache covering save/load,
 * HLL serialization, cleanup, error paths, and cache size.
 */
@Tag("unit")
public class StatisticsCacheDeepTest {

  @TempDir
  Path tempDir;

  // ===== Save and Load statistics =====

  @Test void testSaveAndLoadRoundTrip() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    HyperLogLogSketch hll = new HyperLogLogSketch(8);
    hll.add("a");
    hll.add("b");
    hll.add("c");
    cols.put("name", new ColumnStatistics("name", "Alice", "Zoe", 5, 1000, hll));
    cols.put("age", new ColumnStatistics("age", 1, 100, 10, 1000, null));

    TableStatistics original = new TableStatistics(1000, 50000, cols, "hash123");

    File statsDir = tempDir.resolve(".aperio").resolve("test").resolve(".stats").toFile();
    statsDir.mkdirs();
    File statsFile = new File(statsDir, "test.aperio_stats");

    StatisticsCache.saveStatistics(original, statsFile);
    assertTrue(statsFile.exists());

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    assertEquals(1000, loaded.getRowCount());
    assertEquals(50000, loaded.getDataSize());
    assertEquals("hash123", loaded.getSourceHash());
    assertTrue(loaded.isValidFor("hash123"));

    // Check column stats survived round-trip
    assertNotNull(loaded.getColumnStatistics("name"));
    assertNotNull(loaded.getColumnStatistics("age"));
    assertEquals(5, loaded.getColumnStatistics("name").getNullCount());
    assertEquals(10, loaded.getColumnStatistics("age").getNullCount());
  }

  @Test void testSaveAndLoadWithHLL() throws IOException {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 100; i++) {
      hll.add("val_" + i);
    }

    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("id", new ColumnStatistics("id", 1, 1000, 0, 1000, hll));
    TableStatistics original = new TableStatistics(1000, 50000, cols, "hash");

    File statsFile = tempDir.resolve("stats_hll.aperio_stats").toFile();
    StatisticsCache.saveStatistics(original, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics loadedCol = loaded.getColumnStatistics("id");
    assertNotNull(loadedCol);
    assertNotNull(loadedCol.getHllSketch());
    // HLL estimate should be preserved (approximately)
    assertTrue(loadedCol.getHllSketch().getEstimate() > 50);
  }

  @Test void testSaveAndLoadWithNullMinMax() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("nullable_col", new ColumnStatistics("nullable_col", null, null, 500, 1000, null));
    TableStatistics original = new TableStatistics(1000, 50000, cols, null);

    File statsFile = tempDir.resolve("stats_null.aperio_stats").toFile();
    StatisticsCache.saveStatistics(original, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics loadedCol = loaded.getColumnStatistics("nullable_col");
    assertNotNull(loadedCol);
    assertEquals(500, loadedCol.getNullCount());
    assertNull(loadedCol.getHllSketch());
  }

  @Test void testLoadNonexistentFileThrows() {
    File missing = tempDir.resolve("nonexistent.aperio_stats").toFile();
    assertThrows(IOException.class, () -> StatisticsCache.loadStatistics(missing));
  }

  @Test void testLoadInvalidVersionThrows() throws IOException {
    File invalidFile = tempDir.resolve("bad_version.aperio_stats").toFile();
    java.io.FileWriter writer = new java.io.FileWriter(invalidFile);
    writer.write("{\"version\": \"2.0\", \"rowCount\": 100}");
    writer.close();
    assertThrows(IOException.class, () -> StatisticsCache.loadStatistics(invalidFile));
  }

  // ===== Value type deserialization =====

  @Test void testSaveAndLoadWithLongMinMax() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("longcol", new ColumnStatistics("longcol", 1L, 999999L, 0, 1000, null));
    TableStatistics original = new TableStatistics(1000, 50000, cols, "hash");

    File statsFile = tempDir.resolve("stats_long.aperio_stats").toFile();
    StatisticsCache.saveStatistics(original, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("longcol");
    assertNotNull(col);
    assertNotNull(col.getMinValue());
    assertNotNull(col.getMaxValue());
  }

  @Test void testSaveAndLoadWithDoubleMinMax() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("doublecol", new ColumnStatistics("doublecol", 1.5, 99.9, 0, 1000, null));
    TableStatistics original = new TableStatistics(1000, 50000, cols, "hash");

    File statsFile = tempDir.resolve("stats_double.aperio_stats").toFile();
    StatisticsCache.saveStatistics(original, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("doublecol");
    assertNotNull(col);
    assertNotNull(col.getMinValue());
  }

  @Test void testSaveAndLoadWithBooleanMinMax() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("boolcol", new ColumnStatistics("boolcol", Boolean.FALSE, Boolean.TRUE, 0, 1000, null));
    TableStatistics original = new TableStatistics(1000, 50000, cols, "hash");

    File statsFile = tempDir.resolve("stats_bool.aperio_stats").toFile();
    StatisticsCache.saveStatistics(original, statsFile);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("boolcol");
    assertNotNull(col);
  }

  // ===== HLL sketch save/load =====

  @Test void testSaveAndLoadHLLSketch() throws IOException {
    HyperLogLogSketch original = new HyperLogLogSketch(12);
    for (int i = 0; i < 200; i++) {
      original.add("item_" + i);
    }

    File hllFile = tempDir.resolve("test.hll").toFile();
    StatisticsCache.saveHLLSketch(original, hllFile);
    assertTrue(hllFile.exists());

    HyperLogLogSketch loaded = StatisticsCache.loadHLLSketch(hllFile);
    assertEquals(original.getPrecision(), loaded.getPrecision());
    assertEquals(original.getEstimate(), loaded.getEstimate());
  }

  @Test void testLoadHLLSketchNonexistentThrows() {
    File missing = tempDir.resolve("nonexistent.hll").toFile();
    assertThrows(IOException.class, () -> StatisticsCache.loadHLLSketch(missing));
  }

  @Test void testLoadHLLSketchEmptyBucketsThrows() throws IOException {
    File badFile = tempDir.resolve("bad.hll").toFile();
    java.io.FileWriter writer = new java.io.FileWriter(badFile);
    writer.write("{\"version\": \"1.0\", \"precision\": 14, \"buckets\": \"\"}");
    writer.close();
    assertThrows(IOException.class, () -> StatisticsCache.loadHLLSketch(badFile));
  }

  // ===== Cleanup =====

  @Test void testCleanupOldStatistics() throws IOException {
    // Create .aperio structure
    File aperioDir = tempDir.resolve(".aperio").resolve("schema").toFile();
    File statsDir = new File(aperioDir, ".stats");
    statsDir.mkdirs();

    // Create a stats file
    File oldFile = new File(statsDir, "old.aperio_stats");
    java.io.FileWriter w = new java.io.FileWriter(oldFile);
    w.write("{}");
    w.close();
    // Set last modified to very old
    oldFile.setLastModified(1000);

    File cacheDir = new File(aperioDir, ".parquet_cache");
    cacheDir.mkdirs();

    StatisticsCache.cleanupOldStatistics(cacheDir, 1); // 1ms TTL
    assertFalse(oldFile.exists());
  }

  @Test void testCleanupNonexistentDir() {
    File noDir = tempDir.resolve("nonexistent").toFile();
    // Should not throw
    StatisticsCache.cleanupOldStatistics(noDir, 1000);
  }

  @Test void testCleanupRegularFile() throws IOException {
    File regularFile = tempDir.resolve("regular.txt").toFile();
    regularFile.createNewFile();
    // Should not throw (not a directory)
    StatisticsCache.cleanupOldStatistics(regularFile, 1000);
  }

  // ===== Cache size =====

  @Test void testGetStatisticsCacheSizeWithFiles() throws IOException {
    File aperioDir = tempDir.resolve(".aperio").resolve("schema").toFile();
    File statsDir = new File(aperioDir, ".stats");
    statsDir.mkdirs();

    File statsFile = new File(statsDir, "test.aperio_stats");
    java.io.FileWriter w = new java.io.FileWriter(statsFile);
    w.write("{\"data\": \"some content here to give file some size\"}");
    w.close();

    File cacheDir = new File(aperioDir, ".parquet_cache");
    cacheDir.mkdirs();

    long size = StatisticsCache.getStatisticsCacheSize(cacheDir);
    assertTrue(size > 0);
  }

  @Test void testGetStatisticsCacheSizeNoDir() {
    File noDir = tempDir.resolve("nonexistent").toFile();
    assertEquals(0, StatisticsCache.getStatisticsCacheSize(noDir));
  }

  @Test void testGetStatisticsCacheSizeEmptyDir() throws IOException {
    File aperioDir = tempDir.resolve(".aperio").resolve("schema").toFile();
    aperioDir.mkdirs();
    File cacheDir = new File(aperioDir, ".parquet_cache");
    cacheDir.mkdirs();
    // No .stats dir exists
    assertEquals(0, StatisticsCache.getStatisticsCacheSize(cacheDir));
  }

  // ===== Save to directory that needs creation =====

  @Test void testSaveCreatesParentDirectories() throws IOException {
    Map<String, ColumnStatistics> cols = new HashMap<>();
    cols.put("col1", new ColumnStatistics("col1", 1, 100, 0, 1000, null));
    TableStatistics stats = new TableStatistics(1000, 50000, cols, "hash");

    File deepDir = tempDir.resolve("a").resolve("b").resolve("c").toFile();
    File statsFile = new File(deepDir, "test.aperio_stats");

    StatisticsCache.saveStatistics(stats, statsFile);
    assertTrue(statsFile.exists());
  }
}
