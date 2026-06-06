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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StatisticsCache covering save/load of statistics,
 * HLL sketch serialization, cleanup, cache size calculation,
 * and deserialization of various value types.
 */
@Tag("unit")
public class StatisticsCacheTest {

  @TempDir
  Path tempDir;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void setUp() {
    // Ensure .aperio and .stats directories exist for cleanup tests
  }

  // --- Save and Load round-trip tests ---

  @Test
  void testSaveAndLoadStatisticsRoundTrip() throws IOException {
    // Create statistics with column data
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    hll.add("value1");
    hll.add("value2");
    hll.add("value3");

    ColumnStatistics colStats = new ColumnStatistics(
        "test_col", 10L, 100L, 5L, 50L, hll);

    Map<String, ColumnStatistics> columnStats = new HashMap<>();
    columnStats.put("test_col", colStats);

    TableStatistics stats = new TableStatistics(1000L, 50000L, columnStats, "abc123");

    // Save
    File statsFile = tempDir.resolve("stats.aperio_stats").toFile();
    StatisticsCache.saveStatistics(stats, statsFile);

    assertTrue(statsFile.exists(), "Statistics file should exist after save");

    // Load
    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);

    assertEquals(1000L, loaded.getRowCount(), "Row count should match");
    assertEquals(50000L, loaded.getDataSize(), "Data size should match");
    assertEquals("abc123", loaded.getSourceHash(), "Source hash should match");

    ColumnStatistics loadedCol = loaded.getColumnStatistics("test_col");
    assertNotNull(loadedCol, "Column statistics should be loaded");
    assertEquals(5L, loadedCol.getNullCount(), "Null count should match");
    assertEquals(50L, loadedCol.getTotalCount(), "Total count should match");
  }

  @Test
  void testSaveAndLoadWithAllValueTypes() throws IOException {
    // Create column stats with Long min/max
    ColumnStatistics longCol = new ColumnStatistics(
        "long_col", 1L, 100L, 0L, 100L, null);
    // Create column stats with Integer min/max
    ColumnStatistics intCol = new ColumnStatistics(
        "int_col", Integer.valueOf(5), Integer.valueOf(50), 2L, 100L, null);
    // Create column stats with Double min/max
    ColumnStatistics doubleCol = new ColumnStatistics(
        "double_col", 1.5, 99.9, 3L, 100L, null);
    // Create column stats with Float min/max
    ColumnStatistics floatCol = new ColumnStatistics(
        "float_col", 2.0f, 8.0f, 0L, 50L, null);
    // Create column stats with Boolean min/max
    ColumnStatistics boolCol = new ColumnStatistics(
        "bool_col", Boolean.FALSE, Boolean.TRUE, 0L, 100L, null);
    // Create column stats with String min/max
    ColumnStatistics strCol = new ColumnStatistics(
        "str_col", "aaa", "zzz", 1L, 100L, null);
    // Create column stats with null min/max
    ColumnStatistics nullCol = new ColumnStatistics(
        "null_col", null, null, 100L, 100L, null);

    Map<String, ColumnStatistics> columnStats = new HashMap<>();
    columnStats.put("long_col", longCol);
    columnStats.put("int_col", intCol);
    columnStats.put("double_col", doubleCol);
    columnStats.put("float_col", floatCol);
    columnStats.put("bool_col", boolCol);
    columnStats.put("str_col", strCol);
    columnStats.put("null_col", nullCol);

    TableStatistics stats = new TableStatistics(500L, 25000L, columnStats, "hash456");

    File statsFile = tempDir.resolve("all_types.aperio_stats").toFile();
    StatisticsCache.saveStatistics(stats, statsFile);
    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);

    // Verify Long values
    ColumnStatistics loadedLong = loaded.getColumnStatistics("long_col");
    assertNotNull(loadedLong);
    assertEquals(1L, loadedLong.getMinValue());
    assertEquals(100L, loadedLong.getMaxValue());

    // Verify Integer values
    ColumnStatistics loadedInt = loaded.getColumnStatistics("int_col");
    assertNotNull(loadedInt);
    assertEquals(5, loadedInt.getMinValue());
    assertEquals(50, loadedInt.getMaxValue());

    // Verify Double values
    ColumnStatistics loadedDouble = loaded.getColumnStatistics("double_col");
    assertNotNull(loadedDouble);
    assertEquals(1.5, ((Number) loadedDouble.getMinValue()).doubleValue(), 0.001);
    assertEquals(99.9, ((Number) loadedDouble.getMaxValue()).doubleValue(), 0.001);

    // Verify Float values
    ColumnStatistics loadedFloat = loaded.getColumnStatistics("float_col");
    assertNotNull(loadedFloat);

    // Verify Boolean values
    ColumnStatistics loadedBool = loaded.getColumnStatistics("bool_col");
    assertNotNull(loadedBool);

    // Verify String values
    ColumnStatistics loadedStr = loaded.getColumnStatistics("str_col");
    assertNotNull(loadedStr);
    assertEquals("aaa", loadedStr.getMinValue());
    assertEquals("zzz", loadedStr.getMaxValue());

    // Verify null values
    ColumnStatistics loadedNull = loaded.getColumnStatistics("null_col");
    assertNotNull(loadedNull);
    assertNull(loadedNull.getMinValue(), "Null min should remain null");
    assertNull(loadedNull.getMaxValue(), "Null max should remain null");
  }

  @Test
  void testSaveStatisticsCreatesParentDirectory() throws IOException {
    // Save to a nested directory that doesn't exist yet
    File nestedDir = tempDir.resolve("a/b/c").toFile();
    File statsFile = new File(nestedDir, "stats.aperio_stats");

    Map<String, ColumnStatistics> empty = new HashMap<>();
    TableStatistics stats = new TableStatistics(0L, 0L, empty, null);

    StatisticsCache.saveStatistics(stats, statsFile);

    assertTrue(nestedDir.exists(), "Parent directories should be created");
  }

  // --- Load error handling tests ---

  @Test
  void testLoadStatisticsFileNotFound() {
    File nonExistent = tempDir.resolve("nonexistent.aperio_stats").toFile();
    assertThrows(IOException.class,
        () -> StatisticsCache.loadStatistics(nonExistent),
        "Loading non-existent file should throw IOException");
  }

  @Test
  void testLoadStatisticsUnsupportedVersion() throws IOException {
    File statsFile = tempDir.resolve("bad_version.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "2.0");
    root.put("rowCount", 100);
    MAPPER.writeValue(statsFile, root);

    assertThrows(IOException.class,
        () -> StatisticsCache.loadStatistics(statsFile),
        "Unsupported version should throw IOException");
  }

  @Test
  void testLoadStatisticsWithNoColumns() throws IOException {
    File statsFile = tempDir.resolve("no_cols.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 100);
    root.put("dataSize", 5000);
    root.put("sourceHash", "hash");
    // No "columns" node
    MAPPER.writeValue(statsFile, root);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    assertEquals(100L, loaded.getRowCount());
    assertEquals(0, loaded.getColumnStatistics().size(),
        "No columns should produce empty column statistics map");
  }

  @Test
  void testLoadStatisticsWithCorruptedColumnData() throws IOException {
    File statsFile = tempDir.resolve("corrupt_col.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 100);
    root.put("dataSize", 5000);

    ObjectNode columnsNode = root.putObject("columns");
    // Put a string value instead of an object (corrupted)
    columnsNode.put("bad_col", "not_an_object");

    MAPPER.writeValue(statsFile, root);

    // Should handle the corruption gracefully
    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    assertNotNull(loaded);
    assertEquals(100L, loaded.getRowCount());
  }

  // --- HLL Sketch save/load tests ---

  @Test
  void testSaveAndLoadHLLSketchRoundTrip() throws IOException {
    HyperLogLogSketch sketch = new HyperLogLogSketch(10);
    for (int i = 0; i < 1000; i++) {
      sketch.add("value_" + i);
    }

    File hllFile = tempDir.resolve("test.hll").toFile();
    StatisticsCache.saveHLLSketch(sketch, hllFile);

    assertTrue(hllFile.exists(), "HLL file should exist after save");

    HyperLogLogSketch loaded = StatisticsCache.loadHLLSketch(hllFile);

    assertEquals(10, loaded.getPrecision(), "Precision should match");
    // Estimates should be reasonably close (within HLL error margin)
    assertTrue(Math.abs(loaded.getEstimate() - sketch.getEstimate()) <= 100,
        "HLL estimates should be close after round-trip");
  }

  @Test
  void testSaveHLLSketchCreatesParentDirectory() throws IOException {
    File nestedDir = tempDir.resolve("x/y/z").toFile();
    File hllFile = new File(nestedDir, "sketch.hll");

    HyperLogLogSketch sketch = new HyperLogLogSketch(8);
    StatisticsCache.saveHLLSketch(sketch, hllFile);

    assertTrue(nestedDir.exists(), "Parent directories should be created for HLL sketch");
  }

  @Test
  void testLoadHLLSketchFileNotFound() {
    File nonExistent = tempDir.resolve("missing.hll").toFile();
    assertThrows(IOException.class,
        () -> StatisticsCache.loadHLLSketch(nonExistent),
        "Loading non-existent HLL file should throw IOException");
  }

  @Test
  void testLoadHLLSketchWithMissingBuckets() throws IOException {
    File hllFile = tempDir.resolve("no_buckets.hll").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("precision", 10);
    root.put("estimate", 500);
    root.put("buckets", ""); // Empty buckets

    MAPPER.writeValue(hllFile, root);

    assertThrows(IOException.class,
        () -> StatisticsCache.loadHLLSketch(hllFile),
        "Missing bucket data should throw IOException");
  }

  @Test
  void testLoadHLLSketchWithInvalidBase64() throws IOException {
    File hllFile = tempDir.resolve("bad_base64.hll").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("precision", 10);
    root.put("estimate", 500);
    root.put("buckets", "!!!not-valid-base64!!!");

    MAPPER.writeValue(hllFile, root);

    assertThrows(IOException.class,
        () -> StatisticsCache.loadHLLSketch(hllFile),
        "Invalid base64 bucket data should throw IOException");
  }

  // --- Cleanup tests ---

  @Test
  void testCleanupOldStatisticsNonExistentDir() {
    File nonExistent = tempDir.resolve("nonexistent_dir").toFile();
    // Should not throw
    StatisticsCache.cleanupOldStatistics(nonExistent, 0);
  }

  @Test
  void testCleanupOldStatisticsNotDirectory() throws IOException {
    File regularFile = tempDir.resolve("not_a_dir").toFile();
    regularFile.createNewFile();
    // Should not throw
    StatisticsCache.cleanupOldStatistics(regularFile, 0);
  }

  @Test
  void testCleanupOldStatisticsWithNoStatsDir() throws IOException {
    // Create a directory without .stats subdirectory
    File cacheDir = tempDir.resolve("cache").toFile();
    cacheDir.mkdirs();

    // Should not throw
    StatisticsCache.cleanupOldStatistics(cacheDir, 0);
  }

  @Test
  void testCleanupOldStatisticsDeletesOldFiles() throws IOException {
    // Create .aperio/<schema>/.stats and .parquet_cache structure
    File aperioDir = tempDir.resolve(".aperio").toFile();
    File schemaDir = new File(aperioDir, "test_schema");
    File statsDir = new File(schemaDir, ".stats");
    File cacheDir = new File(schemaDir, ".parquet_cache");
    statsDir.mkdirs();
    cacheDir.mkdirs();

    // Create old stats file
    File oldStats = new File(statsDir, "old_table.aperio_stats");
    try (FileWriter writer = new FileWriter(oldStats)) {
      writer.write("{}");
    }
    // Set modification time to 10 days ago
    oldStats.setLastModified(System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000));

    // Create recent stats file
    File newStats = new File(statsDir, "new_table.aperio_stats");
    try (FileWriter writer = new FileWriter(newStats)) {
      writer.write("{}");
    }

    // The cacheDir parameter must be a child of the schema dir (e.g. .parquet_cache)
    // so that cacheDir.getParentFile() returns schemaDir which contains ".aperio" in path
    StatisticsCache.cleanupOldStatistics(cacheDir, 7L * 24 * 60 * 60 * 1000);

    assertFalse(oldStats.exists(), "Old stats file should be deleted");
    assertTrue(newStats.exists(), "New stats file should be kept");
  }

  @Test
  void testCleanupOldStatisticsWithAperioPath() throws IOException {
    // Test the path that contains ".aperio"
    File aperioDir = tempDir.resolve(".aperio").toFile();
    File schemaDir = new File(aperioDir, "myschema");
    File statsDir = new File(schemaDir, ".stats");
    File cacheDir = new File(schemaDir, ".parquet_cache");
    statsDir.mkdirs();
    cacheDir.mkdirs();

    // Create old stats file
    File oldStats = new File(statsDir, "table1.aperio_stats");
    try (FileWriter writer = new FileWriter(oldStats)) {
      writer.write("{}");
    }
    oldStats.setLastModified(System.currentTimeMillis() - (30L * 24 * 60 * 60 * 1000));

    // cacheDir.getParentFile() is schemaDir which contains ".aperio"
    StatisticsCache.cleanupOldStatistics(cacheDir, 7L * 24 * 60 * 60 * 1000);

    assertFalse(oldStats.exists(), "Old stats file should be deleted via .aperio path");
  }

  // --- Cache size tests ---

  @Test
  void testGetStatisticsCacheSizeNonExistentDir() {
    File nonExistent = tempDir.resolve("no_dir").toFile();
    assertEquals(0L, StatisticsCache.getStatisticsCacheSize(nonExistent));
  }

  @Test
  void testGetStatisticsCacheSizeNotDirectory() throws IOException {
    File regularFile = tempDir.resolve("regular_file").toFile();
    regularFile.createNewFile();
    assertEquals(0L, StatisticsCache.getStatisticsCacheSize(regularFile));
  }

  @Test
  void testGetStatisticsCacheSizeWithNoStatsDir() throws IOException {
    File cacheDir = tempDir.resolve("empty_cache").toFile();
    cacheDir.mkdirs();
    assertEquals(0L, StatisticsCache.getStatisticsCacheSize(cacheDir));
  }

  @Test
  void testGetStatisticsCacheSizeWithFiles() throws IOException {
    // Create .aperio structure
    File aperioDir = tempDir.resolve(".aperio").toFile();
    File schemaDir = new File(aperioDir, "schema");
    File statsDir = new File(schemaDir, ".stats");
    File cacheDir = new File(schemaDir, ".parquet_cache");
    statsDir.mkdirs();
    cacheDir.mkdirs();

    // Create stats files with known content
    File stats1 = new File(statsDir, "table1.aperio_stats");
    try (FileWriter writer = new FileWriter(stats1)) {
      writer.write("{\"rowCount\": 100}");
    }
    File stats2 = new File(statsDir, "table2.aperio_stats");
    try (FileWriter writer = new FileWriter(stats2)) {
      writer.write("{\"rowCount\": 200}");
    }

    long size = StatisticsCache.getStatisticsCacheSize(cacheDir);
    assertTrue(size > 0, "Cache size should be > 0 when stats files exist");
  }

  @Test
  void testGetStatisticsCacheSizeWithoutAperioPath() throws IOException {
    // Test the path fallback when parent doesn't contain ".aperio"
    File normalDir = tempDir.resolve("normal_cache").toFile();
    File statsDir = new File(normalDir, ".stats");
    statsDir.mkdirs();

    File stats = new File(statsDir, "t.aperio_stats");
    try (FileWriter writer = new FileWriter(stats)) {
      writer.write("{\"data\": true}");
    }

    long size = StatisticsCache.getStatisticsCacheSize(normalDir);
    assertTrue(size > 0, "Should find stats when aperioSchemaDir falls back to cacheDir");
  }

  // --- Deserialization edge cases ---

  @Test
  void testDeserializeValueWithMissingNodes() throws IOException {
    // Statistics file where columns have missing min/max values
    File statsFile = tempDir.resolve("partial_col.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 50);
    root.put("dataSize", 1000);

    ObjectNode columnsNode = root.putObject("columns");
    ObjectNode colNode = columnsNode.putObject("partial_col");
    colNode.put("nullCount", 10);
    colNode.put("totalCount", 50);
    // No minValue, maxValue, minType, maxType -> should return null for those

    MAPPER.writeValue(statsFile, root);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("partial_col");
    assertNotNull(col);
    assertNull(col.getMinValue(), "Missing min value should be null");
    assertNull(col.getMaxValue(), "Missing max value should be null");
  }

  @Test
  void testDeserializeValueWithHLLSketch() throws IOException {
    // Create a full statistics file with HLL data
    HyperLogLogSketch sketch = new HyperLogLogSketch(8);
    sketch.add("a");
    sketch.add("b");
    sketch.add("c");

    File statsFile = tempDir.resolve("hll_col.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 3);
    root.put("dataSize", 300);

    ObjectNode columnsNode = root.putObject("columns");
    ObjectNode colNode = columnsNode.putObject("hll_col");
    colNode.put("nullCount", 0);
    colNode.put("totalCount", 3);
    colNode.put("minValue", "a");
    colNode.put("minType", "String");
    colNode.put("maxValue", "c");
    colNode.put("maxType", "String");

    ObjectNode hllNode = colNode.putObject("hll");
    hllNode.put("precision", sketch.getPrecision());
    hllNode.put("estimate", sketch.getEstimate());
    String bucketsBase64 = Base64.getEncoder().encodeToString(sketch.getBuckets());
    hllNode.put("buckets", bucketsBase64);

    MAPPER.writeValue(statsFile, root);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("hll_col");
    assertNotNull(col);
    assertNotNull(col.getHllSketch(), "HLL sketch should be deserialized");
    assertEquals(8, col.getHllSketch().getPrecision());
  }

  @Test
  void testDeserializeValueInvalidType() throws IOException {
    // Statistics file with an unrecognized type for value deserialization
    File statsFile = tempDir.resolve("bad_type.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 10);
    root.put("dataSize", 100);

    ObjectNode columnsNode = root.putObject("columns");
    ObjectNode colNode = columnsNode.putObject("bad_type_col");
    colNode.put("nullCount", 0);
    colNode.put("totalCount", 10);
    colNode.put("minValue", "some_value");
    colNode.put("minType", "UnknownType");
    colNode.put("maxValue", "other_value");
    colNode.put("maxType", "String");

    MAPPER.writeValue(statsFile, root);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("bad_type_col");
    assertNotNull(col);
    // Unknown type falls through to default which returns the string value
    assertEquals("some_value", col.getMinValue());
    assertEquals("other_value", col.getMaxValue());
  }

  @Test
  void testDeserializeValueParseFailure() throws IOException {
    // Statistics file where value cannot be parsed as its declared type
    File statsFile = tempDir.resolve("parse_fail.aperio_stats").toFile();
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("rowCount", 10);
    root.put("dataSize", 100);

    ObjectNode columnsNode = root.putObject("columns");
    ObjectNode colNode = columnsNode.putObject("bad_parse_col");
    colNode.put("nullCount", 0);
    colNode.put("totalCount", 10);
    colNode.put("minValue", "not_a_number");
    colNode.put("minType", "Long");
    colNode.put("maxValue", "also_not_a_number");
    colNode.put("maxType", "Integer");

    MAPPER.writeValue(statsFile, root);

    TableStatistics loaded = StatisticsCache.loadStatistics(statsFile);
    ColumnStatistics col = loaded.getColumnStatistics("bad_parse_col");
    assertNotNull(col);
    // Parse failure should return the string as fallback
    assertEquals("not_a_number", col.getMinValue());
    assertEquals("also_not_a_number", col.getMaxValue());
  }

  @Test
  void testTableStatisticsIsValidFor() {
    Map<String, ColumnStatistics> empty = new HashMap<>();
    TableStatistics stats = new TableStatistics(100L, 1000L, empty, "hash123");

    assertTrue(stats.isValidFor("hash123"), "Should be valid for matching hash");
    assertFalse(stats.isValidFor("different_hash"), "Should be invalid for different hash");
    assertFalse(stats.isValidFor(null), "Should be invalid for null hash");
  }

  @Test
  void testTableStatisticsWithNullSourceHash() {
    Map<String, ColumnStatistics> empty = new HashMap<>();
    TableStatistics stats = new TableStatistics(100L, 1000L, empty, null);

    assertFalse(stats.isValidFor("anything"), "Null source hash should not be valid");
    assertFalse(stats.isValidFor(null), "Null vs null should not be valid");
  }

  @Test
  void testTableStatisticsCreateBasicEstimate() {
    TableStatistics basic = TableStatistics.createBasicEstimate(500);
    assertEquals(500L, basic.getRowCount());
    assertEquals(50000L, basic.getDataSize(), "Data size should be rowCount * 100");
    assertTrue(basic.getColumnStatistics().isEmpty(), "Basic estimate should have no column stats");
  }

  @Test
  void testTableStatisticsGetSelectivityUnknownColumn() {
    Map<String, ColumnStatistics> empty = new HashMap<>();
    TableStatistics stats = new TableStatistics(100L, 1000L, empty, null);

    double selectivity = stats.getSelectivity("unknown_col", "=", "value");
    assertEquals(0.1, selectivity, 0.001, "Unknown column should return default 0.1 selectivity");
  }

  @Test
  void testTableStatisticsToString() {
    Map<String, ColumnStatistics> empty = new HashMap<>();
    TableStatistics stats = new TableStatistics(100L, 1000L, empty, null);
    String str = stats.toString();
    assertNotNull(str);
    assertTrue(str.contains("rowCount=100"));
    assertTrue(str.contains("dataSize=1000"));
  }

  // --- ColumnStatistics selectivity tests ---

  @Test
  void testColumnStatisticsSelectivityForEmptyColumn() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 0L, 0L, null);
    assertEquals(0.0, col.getSelectivity("=", "value"), 0.001);
  }

  @Test
  void testColumnStatisticsSelectivityForNullIsNull() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 30L, 100L, null);
    assertEquals(0.3, col.getSelectivity("IS NULL", null), 0.001);
  }

  @Test
  void testColumnStatisticsSelectivityForNullIsNotNull() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 30L, 100L, null);
    assertEquals(0.7, col.getSelectivity("IS NOT NULL", null), 0.001);
  }

  @Test
  void testColumnStatisticsSelectivityNullWithOtherOp() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 30L, 100L, null);
    assertEquals(0.0, col.getSelectivity("=", null), 0.001,
        "Null comparison with non-null operator should return 0.0");
  }

  @Test
  void testColumnStatisticsEqualitySelectivity() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 100; i++) {
      hll.add("val_" + i);
    }
    ColumnStatistics col = new ColumnStatistics("col", "val_0", "val_99", 0L, 100L, hll);
    double selectivity = col.getSelectivity("=", "val_50");
    assertTrue(selectivity > 0.0 && selectivity < 1.0,
        "Equality selectivity should be between 0 and 1");
  }

  @Test
  void testColumnStatisticsNotEqualSelectivity() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 100; i++) {
      hll.add("val_" + i);
    }
    ColumnStatistics col = new ColumnStatistics("col", "val_0", "val_99", 0L, 100L, hll);
    double selectivity = col.getSelectivity("!=", "val_50");
    assertTrue(selectivity > 0.0 && selectivity <= 1.0);
  }

  @Test
  void testColumnStatisticsRangeSelectivityWithNumbers() {
    ColumnStatistics col = new ColumnStatistics("col", 0L, 100L, 0L, 100L, null);

    double lt = col.getSelectivity("<", 50L);
    assertTrue(lt >= 0.0 && lt <= 1.0, "Range selectivity should be between 0 and 1");

    double gt = col.getSelectivity(">", 50L);
    assertTrue(gt >= 0.0 && gt <= 1.0);

    double le = col.getSelectivity("<=", 50L);
    assertTrue(le >= 0.0 && le <= 1.0);

    double ge = col.getSelectivity(">=", 50L);
    assertTrue(ge >= 0.0 && ge <= 1.0);
  }

  @Test
  void testColumnStatisticsRangeSelectivityWithStrings() {
    ColumnStatistics col = new ColumnStatistics("col", "aaa", "zzz", 0L, 100L, null);

    double lt = col.getSelectivity("<", "mmm");
    assertTrue(lt >= 0.0 && lt <= 1.0);
  }

  @Test
  void testColumnStatisticsRangeSelectivityWithoutMinMax() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 0L, 100L, null);
    double selectivity = col.getSelectivity("<", 50L);
    assertEquals(0.3, selectivity, 0.001, "No min/max should return default 0.3");
  }

  @Test
  void testColumnStatisticsRangeSelectivityTypeMismatch() {
    // Min/max are Long but comparison value is String - isComparable returns false
    ColumnStatistics col = new ColumnStatistics("col", 0L, 100L, 0L, 100L, null);
    double selectivity = col.getSelectivity("<", "string_value");
    assertEquals(0.3, selectivity, 0.001, "Type mismatch should return default 0.3");
  }

  @Test
  void testColumnStatisticsUnknownOperator() {
    ColumnStatistics col = new ColumnStatistics("col", 0L, 100L, 0L, 100L, null);
    double selectivity = col.getSelectivity("LIKE", "pattern");
    assertEquals(0.1, selectivity, 0.001, "Unknown operator should return default 0.1");
  }

  @Test
  void testColumnStatisticsSingleValueRange() {
    // Same min and max value
    ColumnStatistics col = new ColumnStatistics("col", 50L, 50L, 0L, 100L, null);
    double selectivity = col.getSelectivity("<", 50L);
    // Should return 0.5 for single value range
    assertTrue(selectivity >= 0.0 && selectivity <= 1.0);
  }

  @Test
  void testColumnStatisticsDistinctCountWithoutHll() {
    ColumnStatistics col = new ColumnStatistics("col", null, null, 0L, 500L, null);
    long distinct = col.getDistinctCount();
    assertEquals(500L, distinct,
        "Without HLL, distinct count should be min(1000, totalCount)");
  }

  @Test
  void testColumnStatisticsDistinctCountWithHll() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10);
    for (int i = 0; i < 50; i++) {
      hll.add("val_" + i);
    }
    ColumnStatistics col = new ColumnStatistics("col", null, null, 0L, 100L, hll);
    long distinct = col.getDistinctCount();
    assertTrue(distinct > 0, "With HLL, distinct count should be > 0");
  }

  @Test
  void testColumnStatisticsToString() {
    ColumnStatistics col = new ColumnStatistics("test_col", 1L, 100L, 5L, 100L, null);
    String str = col.toString();
    assertNotNull(str);
    assertTrue(str.contains("test_col"));
    assertTrue(str.contains("min=1"));
    assertTrue(str.contains("max=100"));
  }

  @Test
  void testColumnStatisticsStringLessThanMin() {
    ColumnStatistics col = new ColumnStatistics("col", "ddd", "zzz", 0L, 100L, null);
    double selectivity = col.getSelectivity("<", "aaa");
    assertTrue(selectivity >= 0.0 && selectivity <= 1.0);
  }

  @Test
  void testColumnStatisticsStringGreaterThanMax() {
    ColumnStatistics col = new ColumnStatistics("col", "aaa", "mmm", 0L, 100L, null);
    double selectivity = col.getSelectivity("<", "zzz");
    assertTrue(selectivity >= 0.0 && selectivity <= 1.0);
  }
}
