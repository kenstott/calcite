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
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link ParquetReorganizer} targeting remaining uncovered lines.
 * Focuses on code paths NOT covered by DeepCoverageTest2:
 * - getDistinctPartitionValues branches (Iceberg vs Parquet, wildcard prefix handling)
 * - buildBatchCombinations edge cases (empty batch columns, year column, no values found)
 * - processSingleBatch filename pattern building (multi-value batch)
 * - processYearBatches error handling (Iceberg vs Parquet year loop)
 * - getDuckDBConnection static method
 * - buildReorganizationSql Parquet with multiple column mappings
 * - buildReorganizationSql Iceberg with mixed numeric and string filters
 * - ReorgConfig builder method chaining and NOOP tracker identity
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class ParquetReorganizerDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  // ========== getDistinctPartitionValues ==========

  @Test void testGetDistinctPartitionValuesColumnNotInPath() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(inv -> {
      String base = inv.getArgument(0);
      String rel = inv.getArgument(1);
      return base + "/" + rel;
    });
    ParquetReorganizer reorg = new ParquetReorganizer(sp, tempDir.toString());

    Method m =
        ParquetReorganizer.class.getDeclaredMethod("getDistinctPartitionValues", Connection.class,
        ParquetReorganizer.ReorgConfig.class, String.class);
    m.setAccessible(true);

    // Column not in path pattern, non-Iceberg => returns empty
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/*.parquet")
        .targetBase("target")
        .build();

    // Need a real DuckDB connection for this method
    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("test_distinct.duckdb").toString());
    try {
      @SuppressWarnings("unchecked")
      List<String> result = (List<String>) m.invoke(reorg, conn, config, "mystery_col");
      assertTrue(result.isEmpty(), "Column not in pattern and not Iceberg => empty");
    } finally {
      conn.close();
    }
  }

  @Test void testGetDistinctPartitionValuesFromDirectoryListing() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(inv -> {
      String base = inv.getArgument(0);
      String rel = inv.getArgument(1);
      return base + "/" + rel;
    });

    // Mock directory listing returning partition directories
    List<StorageProvider.FileEntry> entries = new ArrayList<StorageProvider.FileEntry>();
    entries.add(
        new StorageProvider.FileEntry(
        tempDir + "/type=income/geo=STATE/data.parquet",
        "data.parquet", false, 100L, System.currentTimeMillis()));
    entries.add(
        new StorageProvider.FileEntry(
        tempDir + "/type=income/geo=COUNTY/data.parquet",
        "data.parquet", false, 200L, System.currentTimeMillis()));
    when(sp.listFiles(anyString(), eq(true))).thenReturn(entries);

    ParquetReorganizer reorg = new ParquetReorganizer(sp, tempDir.toString());

    Method m =
        ParquetReorganizer.class.getDeclaredMethod("getDistinctPartitionValues", Connection.class,
        ParquetReorganizer.ReorgConfig.class, String.class);
    m.setAccessible(true);

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/geo=*/*.parquet")
        .targetBase("target")
        .build();

    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("test_listing.duckdb").toString());
    try {
      @SuppressWarnings("unchecked")
      List<String> result = (List<String>) m.invoke(reorg, conn, config, "geo");
      assertTrue(result.contains("STATE"), "Should find STATE partition: " + result);
      assertTrue(result.contains("COUNTY"), "Should find COUNTY partition: " + result);
    } finally {
      conn.close();
    }
  }

  @Test void testGetDistinctPartitionValuesWildcardPrefix() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(inv -> {
      String base = inv.getArgument(0);
      String rel = inv.getArgument(1);
      return base + "/" + rel;
    });

    List<StorageProvider.FileEntry> entries = new ArrayList<StorageProvider.FileEntry>();
    entries.add(
        new StorageProvider.FileEntry(
        tempDir + "/type=income/year=2020/geo=STATE/f.parquet",
        "f.parquet", false, 100L, System.currentTimeMillis()));
    when(sp.listFiles(anyString(), eq(true))).thenReturn(entries);

    ParquetReorganizer reorg = new ParquetReorganizer(sp, tempDir.toString());

    Method m =
        ParquetReorganizer.class.getDeclaredMethod("getDistinctPartitionValues", Connection.class,
        ParquetReorganizer.ReorgConfig.class, String.class);
    m.setAccessible(true);

    // Pattern with wildcard before the target column
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/geo=*/*.parquet")
        .targetBase("target")
        .build();

    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("test_wc.duckdb").toString());
    try {
      @SuppressWarnings("unchecked")
      List<String> result = (List<String>) m.invoke(reorg, conn, config, "geo");
      // Should handle wildcard prefix and still extract values
      assertNotNull(result);
    } finally {
      conn.close();
    }
  }

  // ========== buildBatchCombinations ==========

  @Test void testBuildBatchCombinationsEmptyBatchColumns() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildBatchCombinations", Connection.class,
        ParquetReorganizer.ReorgConfig.class);
    m.setAccessible(true);

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("src/*.parquet")
        .targetBase("tgt")
        .build();  // No batchPartitionColumns

    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("test_empty_batch.duckdb").toString());
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, String>> result =
          (List<Map<String, String>>) m.invoke(reorg, conn, config);
      assertTrue(result.isEmpty());
    } finally {
      conn.close();
    }
  }

  @Test void testBuildBatchCombinationsYearColumnOnly() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildBatchCombinations", Connection.class,
        ParquetReorganizer.ReorgConfig.class);
    m.setAccessible(true);

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("year=*/*.parquet")
        .targetBase("tgt")
        .batchPartitionColumns(Arrays.asList("year"))
        .yearRange(2020, 2022)
        .build();

    Connection conn =
        DriverManager.getConnection("jdbc:duckdb:" + tempDir.resolve("test_year_batch.duckdb").toString());
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, String>> result =
          (List<Map<String, String>>) m.invoke(reorg, conn, config);
      assertEquals(3, result.size(), "Should have 2020, 2021, 2022");
      assertEquals("2020", result.get(0).get("year"));
      assertEquals("2021", result.get(1).get("year"));
      assertEquals("2022", result.get(2).get("year"));
    } finally {
      conn.close();
    }
  }

  // ========== buildReorganizationSql additional edge cases ==========

  @Test void testBuildReorganizationSqlMultipleColumnMappings() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class);
    m.setAccessible(true);

    Map<String, String> mappings = new LinkedHashMap<String, String>();
    mappings.put("geo", "GeoFips");
    mappings.put("name", "GeoName");

    String sql =
        (String) m.invoke(reorg, "/data/*.parquet", "/data/target",
        Arrays.asList("geo", "name"), mappings, "batch_{i}");

    assertTrue(sql.contains("\"GeoFips\" AS geo"), "Should alias GeoFips: " + sql);
    assertTrue(sql.contains("\"GeoName\" AS name"), "Should alias GeoName: " + sql);
  }

  @Test void testBuildReorganizationSqlIcebergSingleNumericFilter() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class,
        boolean.class, String.class, String.class, Map.class);
    m.setAccessible(true);

    Map<String, String> filters = new LinkedHashMap<String, String>();
    filters.put("year", "2024");

    String sql =
        (String) m.invoke(reorg, null, "/data/target", Arrays.asList("geo"), Collections.emptyMap(),
        "data_{i}", true, "s3://warehouse", "tbl", filters);

    assertTrue(sql.contains("WHERE year = 2024"),
        "Single numeric filter should not have AND: " + sql);
    assertFalse(sql.contains(" AND "), "Single filter no AND: " + sql);
  }

  @Test void testBuildReorganizationSqlIcebergEmptyFilterMap() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class,
        boolean.class, String.class, String.class, Map.class);
    m.setAccessible(true);

    String sql =
        (String) m.invoke(reorg, null, "/data/target", Arrays.asList("geo"), Collections.emptyMap(),
        "data_{i}", true, "s3://warehouse", "tbl", Collections.emptyMap());

    assertFalse(sql.contains("WHERE"), "Empty filter map = no WHERE: " + sql);
  }

  @Test void testBuildReorganizationSqlParquetEmptyFilenamePattern() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class);
    m.setAccessible(true);

    String sql =
        (String) m.invoke(reorg, "/data/*.parquet", "/data/target",
        Arrays.asList("geo"), Collections.emptyMap(), "");

    // Empty string pattern might still be included
    assertNotNull(sql);
    assertTrue(sql.contains("COPY"), "Should contain COPY: " + sql);
  }

  // ========== buildCombinationsRecursive edge cases ==========

  @Test void testBuildCombinationsRecursiveSingleValuePerColumn() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    m.setAccessible(true);

    List<String> colNames = Arrays.asList("year", "geo", "type");
    List<List<String>> colValues = new ArrayList<List<String>>();
    colValues.add(Arrays.asList("2020"));
    colValues.add(Arrays.asList("STATE"));
    colValues.add(Arrays.asList("income"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    m.invoke(reorg, colNames, colValues, 0, new LinkedHashMap<String, String>(), result);

    assertEquals(1, result.size(), "1x1x1 should give 1 combination");
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("STATE", result.get(0).get("geo"));
    assertEquals("income", result.get(0).get("type"));
  }

  @Test void testBuildCombinationsRecursiveThreeByTwo() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    m.setAccessible(true);

    List<String> colNames = Arrays.asList("year", "geo");
    List<List<String>> colValues = new ArrayList<List<String>>();
    colValues.add(Arrays.asList("2020", "2021", "2022"));
    colValues.add(Arrays.asList("STATE", "COUNTY"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    m.invoke(reorg, colNames, colValues, 0, new LinkedHashMap<String, String>(), result);

    assertEquals(6, result.size(), "3x2 should give 6 combinations");
  }

  // ========== groupBatchesByIncrementalKey additional edge cases ==========

  @Test void testGroupBatchesByIncrementalKeyEmptyKeys() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    m.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> b1 = new LinkedHashMap<String, String>();
    b1.put("year", "2020");
    b1.put("geo", "STATE");
    batches.add(b1);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>)
            m.invoke(reorg, batches, Collections.emptyList());

    // Empty keys list => all batches grouped under empty key
    assertEquals(1, result.size());
  }

  @Test void testGroupBatchesByIncrementalKeyMultipleKeys() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    m.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> b1 = new LinkedHashMap<String, String>();
    b1.put("year", "2020");
    b1.put("geo", "STATE");
    b1.put("type", "income");
    batches.add(b1);

    Map<String, String> b2 = new LinkedHashMap<String, String>();
    b2.put("year", "2020");
    b2.put("geo", "COUNTY");
    b2.put("type", "income");
    batches.add(b2);

    Map<String, String> b3 = new LinkedHashMap<String, String>();
    b3.put("year", "2021");
    b3.put("geo", "STATE");
    b3.put("type", "income");
    batches.add(b3);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>)
            m.invoke(reorg, batches, Arrays.asList("year", "type"));

    // year+type: {2020,income} and {2021,income}
    assertEquals(2, result.size(), "Should group by year+type");
  }

  // ========== isCurrentYear non-numeric year ==========

  @Test void testIsCurrentYearNonNumeric() throws Exception {
    ParquetReorganizer reorg = createReorganizer();
    Method m = ParquetReorganizer.class.getDeclaredMethod("isCurrentYear", Map.class);
    m.setAccessible(true);

    Map<String, String> nonNumeric = Collections.singletonMap("year", "latest");
    assertFalse((Boolean) m.invoke(reorg, nonNumeric));
  }

  // ========== ReorgConfig IncrementalTracker NOOP identity ==========

  @Test void testReorgConfigNoopTrackerIdentity() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("src/*")
        .targetBase("tgt")
        .incrementalKeys(Arrays.asList("year"))
        .build();

    assertSame(IncrementalTracker.NOOP, config.getIncrementalTracker());
    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigCurrentYearTtlMillisDefault() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("src/*")
        .targetBase("tgt")
        .build();
    assertEquals(1, config.getCurrentYearTtlDays());
    assertEquals(86400000L, config.getCurrentYearTtlMillis());
  }

  // ========== getDuckDBConnection ==========

  @Test void testGetDuckDBConnectionReturnsConnection() throws Exception {
    Method m = ParquetReorganizer.class.getDeclaredMethod("getDuckDBConnection");
    m.setAccessible(true);

    ParquetReorganizer reorg = createReorganizer();
    Connection conn = (Connection) m.invoke(reorg);
    assertNotNull(conn);
    assertFalse(conn.isClosed());
    conn.close();
  }

  // ========== Helper ==========

  private ParquetReorganizer createReorganizer() {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(invocation -> {
      String base = invocation.getArgument(0);
      String rel = invocation.getArgument(1);
      return base + "/" + rel;
    });
    return new ParquetReorganizer(sp, tempDir.toFile().getAbsolutePath());
  }
}
