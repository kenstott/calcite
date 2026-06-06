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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for ParquetReorganizer focusing on uncovered branches:
 * ReorgConfig builder, buildReorganizationSql, buildConsolidationSql, isNumeric,
 * quoteLiteral, isCurrentYear, buildBatchCombinations, buildCombinationsRecursive,
 * groupBatchesByIncrementalKey, and the create() factory method.
 */
@Tag("unit")
public class ParquetReorganizerDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorageProvider;
  private ParquetReorganizer reorganizer;

  @BeforeEach
  void setUp() {
    mockStorageProvider = mock(StorageProvider.class);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0) + "/" + inv.getArgument(1));
    reorganizer = new ParquetReorganizer(mockStorageProvider, tempDir.toString());
  }

  // ====== ReorgConfig.Builder tests ======

  @Test void testReorgConfigBuilderMinimal() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/*.parquet")
        .targetBase("type=income_consolidated")
        .build();

    assertEquals("type=income/year=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_consolidated", config.getTargetBase());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertTrue(config.getColumnMappings().isEmpty());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertEquals(0, config.getStartYear());
    assertEquals(0, config.getEndYear());
    assertNull(config.getName());
    assertEquals(2, config.getThreads()); // default
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertNotNull(config.getIncrementalTracker());
    assertNull(config.getSourceTable());
    assertFalse(config.isSourceIsIceberg());
    assertNull(config.getIcebergWarehousePath());
    assertEquals(1, config.getCurrentYearTtlDays()); // default
    assertEquals(86400000L, config.getCurrentYearTtlMillis());
    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigBuilderFull() {
    IncrementalTracker tracker = mock(IncrementalTracker.class);

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/geo=*/*.parquet")
        .targetBase("type=income_by_geo")
        .partitionColumns(Arrays.asList("geo"))
        .columnMappings(Collections.singletonMap("geo", "GeoFips"))
        .batchPartitionColumns(Arrays.asList("year", "geo_fips_set"))
        .yearRange(2020, 2024)
        .name("income_reorg")
        .threads(4)
        .incrementalKeys(Arrays.asList("year"))
        .incrementalTracker(tracker)
        .sourceTable("regional_income")
        .sourceIsIceberg(true)
        .icebergWarehousePath("/warehouse")
        .currentYearTtlDays(3)
        .build();

    assertEquals("type=income/year=*/geo=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_by_geo", config.getTargetBase());
    assertEquals(Arrays.asList("geo"), config.getPartitionColumns());
    assertEquals(Collections.singletonMap("geo", "GeoFips"), config.getColumnMappings());
    assertEquals(Arrays.asList("year", "geo_fips_set"), config.getBatchPartitionColumns());
    assertEquals(2020, config.getStartYear());
    assertEquals(2024, config.getEndYear());
    assertEquals("income_reorg", config.getName());
    assertEquals(4, config.getThreads());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertEquals(tracker, config.getIncrementalTracker());
    assertEquals("regional_income", config.getSourceTable());
    assertTrue(config.isSourceIsIceberg());
    assertEquals("/warehouse", config.getIcebergWarehousePath());
    assertEquals(3, config.getCurrentYearTtlDays());
    assertEquals(3 * 24L * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
    assertTrue(config.supportsIncremental());
  }

  @Test void testReorgConfigBuilderMissingSourcePattern() {
    try {
      ParquetReorganizer.ReorgConfig.builder()
          .targetBase("target")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("sourcePattern"));
    }
  }

  @Test void testReorgConfigBuilderEmptySourcePattern() {
    try {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("")
          .targetBase("target")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("sourcePattern"));
    }
  }

  @Test void testReorgConfigBuilderMissingTargetBase() {
    try {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("*.parquet")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("targetBase"));
    }
  }

  @Test void testReorgConfigBuilderEmptyTargetBase() {
    try {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("*.parquet")
          .targetBase("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("targetBase"));
    }
  }

  @Test void testReorgConfigBuilderNullSourcePattern() {
    try {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern(null)
          .targetBase("target")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("sourcePattern"));
    }
  }

  @Test void testReorgConfigDefaultThreads() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .threads(0) // Invalid, should use default
        .build();

    assertEquals(2, config.getThreads());
  }

  @Test void testReorgConfigDefaultCurrentYearTtl() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .currentYearTtlDays(0) // Invalid, should use default
        .build();

    assertEquals(1, config.getCurrentYearTtlDays());
  }

  @Test void testReorgConfigSupportsIncrementalNoTracker() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .incrementalKeys(Arrays.asList("year"))
        // No tracker set - defaults to NOOP
        .build();

    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigSupportsIncrementalNoKeys() {
    IncrementalTracker tracker = mock(IncrementalTracker.class);

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .incrementalTracker(tracker)
        // No incrementalKeys
        .build();

    assertFalse(config.supportsIncremental());
  }

  // ====== isNumeric tests ======

  @Test void testIsNumeric() throws Exception {
    Method method = ParquetReorganizer.class.getDeclaredMethod("isNumeric", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(reorganizer, "123"));
    assertTrue((Boolean) method.invoke(reorganizer, "0"));
    assertTrue((Boolean) method.invoke(reorganizer, "-42"));
    assertTrue((Boolean) method.invoke(reorganizer, "2024"));
    assertFalse((Boolean) method.invoke(reorganizer, "abc"));
    assertFalse((Boolean) method.invoke(reorganizer, "12.34"));
    assertFalse((Boolean) method.invoke(reorganizer, ""));
    assertFalse((Boolean) method.invoke(reorganizer, (String) null));
    assertFalse((Boolean) method.invoke(reorganizer, "12abc"));
  }

  // ====== quoteLiteral tests ======

  @Test void testQuoteLiteral() throws Exception {
    Method method = ParquetReorganizer.class.getDeclaredMethod("quoteLiteral", String.class);
    method.setAccessible(true);

    assertEquals("'hello'", method.invoke(null, "hello"));
    assertEquals("'it''s'", method.invoke(null, "it's"));
    assertEquals("'path/to/file'", method.invoke(null, "path/to/file"));
    assertEquals("''", method.invoke(null, ""));
  }

  // ====== isCurrentYear tests ======

  @Test void testIsCurrentYear() throws Exception {
    Method method = ParquetReorganizer.class.getDeclaredMethod("isCurrentYear", Map.class);
    method.setAccessible(true);

    int currentYear = java.time.Year.now().getValue();

    Map<String, String> currentYearMap = new HashMap<>();
    currentYearMap.put("year", String.valueOf(currentYear));
    assertTrue((Boolean) method.invoke(reorganizer, currentYearMap));

    Map<String, String> pastYearMap = new HashMap<>();
    pastYearMap.put("year", "2020");
    assertFalse((Boolean) method.invoke(reorganizer, pastYearMap));

    // No year key
    Map<String, String> noYearMap = new HashMap<>();
    noYearMap.put("geo", "STATE");
    assertFalse((Boolean) method.invoke(reorganizer, noYearMap));

    // Null map
    assertFalse((Boolean) method.invoke(reorganizer, (Map<String, String>) null));

    // Empty map
    assertFalse((Boolean) method.invoke(reorganizer, Collections.emptyMap()));
  }

  // ====== buildCombinationsRecursive tests ======

  @Test void testBuildCombinationsRecursive() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildCombinationsRecursive", List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> columnNames = Arrays.asList("year", "geo");
    List<List<String>> columnValues = new ArrayList<>();
    columnValues.add(Arrays.asList("2020", "2021"));
    columnValues.add(Arrays.asList("STATE", "COUNTY"));

    List<Map<String, String>> result = new ArrayList<>();
    method.invoke(reorganizer, columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size());
    // Verify all combinations
    assertTrue(
        result.stream().anyMatch(m ->
        "2020".equals(m.get("year")) && "STATE".equals(m.get("geo"))));
    assertTrue(
        result.stream().anyMatch(m ->
        "2020".equals(m.get("year")) && "COUNTY".equals(m.get("geo"))));
    assertTrue(
        result.stream().anyMatch(m ->
        "2021".equals(m.get("year")) && "STATE".equals(m.get("geo"))));
    assertTrue(
        result.stream().anyMatch(m ->
        "2021".equals(m.get("year")) && "COUNTY".equals(m.get("geo"))));
  }

  @Test void testBuildCombinationsRecursiveSingleColumn() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildCombinationsRecursive", List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> columnNames = Arrays.asList("year");
    List<List<String>> columnValues = new ArrayList<>();
    columnValues.add(Arrays.asList("2020", "2021", "2022"));

    List<Map<String, String>> result = new ArrayList<>();
    method.invoke(reorganizer, columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(3, result.size());
  }

  // ====== groupBatchesByIncrementalKey tests ======

  @Test void testGroupBatchesByIncrementalKey() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<>();

    Map<String, String> b1 = new LinkedHashMap<>();
    b1.put("year", "2020");
    b1.put("geo", "STATE");
    batches.add(b1);

    Map<String, String> b2 = new LinkedHashMap<>();
    b2.put("year", "2020");
    b2.put("geo", "COUNTY");
    batches.add(b2);

    Map<String, String> b3 = new LinkedHashMap<>();
    b3.put("year", "2021");
    b3.put("geo", "STATE");
    batches.add(b3);

    List<String> incrementalKeys = Arrays.asList("year");

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            reorganizer, batches, incrementalKeys);

    assertEquals(2, result.size()); // Two year groups: 2020 and 2021

    Map<String, String> key2020 = new LinkedHashMap<>();
    key2020.put("year", "2020");
    assertEquals(2, result.get(key2020).size());

    Map<String, String> key2021 = new LinkedHashMap<>();
    key2021.put("year", "2021");
    assertEquals(1, result.get(key2021).size());
  }

  @Test void testGroupBatchesByIncrementalKeyNullKeys() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<>();
    Map<String, String> b1 = new LinkedHashMap<>();
    b1.put("year", "2020");
    batches.add(b1);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            reorganizer, batches, null);

    // With null incremental keys, all batches should be grouped under empty key
    assertEquals(1, result.size());
  }

  @Test void testGroupBatchesByIncrementalKeyEmptyKeys() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<>();
    Map<String, String> b1 = new LinkedHashMap<>();
    b1.put("year", "2020");
    batches.add(b1);

    Map<String, String> b2 = new LinkedHashMap<>();
    b2.put("year", "2021");
    batches.add(b2);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            reorganizer, batches, Collections.emptyList());

    // Empty incremental keys - all batches grouped under empty key
    assertEquals(1, result.size());
    assertEquals(2, result.values().iterator().next().size());
  }

  @Test void testGroupBatchesByIncrementalKeyMissingValue() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<>();
    Map<String, String> b1 = new LinkedHashMap<>();
    b1.put("geo", "STATE"); // No "year" key
    batches.add(b1);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            reorganizer, batches, Arrays.asList("year"));

    // "year" key not found in batch, so keyValues is empty
    assertEquals(1, result.size());
  }

  // ====== buildReorganizationSql tests ======

  @Test void testBuildReorganizationSqlParquet() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class, String.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/data/*.parquet", "/output", Arrays.asList("year", "geo"),
        Collections.singletonMap("geo", "GeoFips"), "data_{i}");

    assertNotNull(result);
    assertTrue(result.contains("COPY"));
    assertTrue(result.contains("read_parquet"));
    assertTrue(result.contains("hive_partitioning=true"));
    assertTrue(result.contains("PARTITION_BY"));
    assertTrue(result.contains("FILENAME_PATTERN"));
    assertTrue(result.contains("OVERWRITE_OR_IGNORE"));
    assertTrue(result.contains("GeoFips"));
  }

  @Test void testBuildReorganizationSqlNoPartitions() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class, String.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/data/*.parquet", "/output", Collections.emptyList(),
        Collections.emptyMap(), "data_{i}");

    assertNotNull(result);
    assertTrue(result.contains("COPY"));
    assertFalse(result.contains("PARTITION_BY"));
  }

  @Test void testBuildReorganizationSqlNoFilename() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class, String.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/data/*.parquet", "/output", Collections.emptyList(),
        Collections.emptyMap(), null);

    assertNotNull(result);
    assertFalse(result.contains("FILENAME_PATTERN"));
  }

  @Test void testBuildReorganizationSqlEmptyFilename() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class, String.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/data/*.parquet", "/output", Collections.emptyList(),
        Collections.emptyMap(), "");

    assertNotNull(result);
    assertFalse(result.contains("FILENAME_PATTERN"));
  }

  // ====== buildReorganizationSql Iceberg variant tests ======

  @Test void testBuildReorganizationSqlIceberg() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class,
        String.class, boolean.class, String.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> filters = new LinkedHashMap<>();
    filters.put("year", "2020");
    filters.put("state", "CA");

    String result =
        (String) method.invoke(reorganizer, null, "/output", Arrays.asList("geo"), Collections.emptyMap(), "data_{i}",
        true, "/warehouse", "income_table", filters);

    assertNotNull(result);
    assertTrue(result.contains("iceberg_scan"));
    assertTrue(result.contains("WHERE"));
    assertTrue(result.contains("year = 2020")); // numeric filter
    assertTrue(result.contains("state = 'CA'")); // string filter
  }

  @Test void testBuildReorganizationSqlIcebergNoFilters() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class,
        String.class, boolean.class, String.class, String.class, Map.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, null, "/output", Arrays.asList("geo"), Collections.emptyMap(), "data_{i}",
        true, "/warehouse", "income_table", null);

    assertNotNull(result);
    assertTrue(result.contains("iceberg_scan"));
    assertFalse(result.contains("WHERE"));
  }

  @Test void testBuildReorganizationSqlIcebergWithColumnMappings() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class,
        String.class, boolean.class, String.class, String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> columnMappings = new LinkedHashMap<>();
    columnMappings.put("geo", "GeoFips");

    String result =
        (String) method.invoke(reorganizer, null, "/output", Arrays.asList("geo"), columnMappings, "data_{i}",
        true, "/warehouse", "income_table", null);

    assertNotNull(result);
    assertTrue(result.contains("iceberg_scan"));
    assertTrue(result.contains("GeoFips"));
    assertTrue(result.contains("_aliased"));
  }

  @Test void testBuildReorganizationSqlIcebergEmptyFilters() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildReorganizationSql", String.class, String.class, List.class, Map.class,
        String.class, boolean.class, String.class, String.class, Map.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, null, "/output", Collections.emptyList(), Collections.emptyMap(), "data_{i}",
        true, "/warehouse", "income_table", Collections.emptyMap());

    assertNotNull(result);
    assertTrue(result.contains("iceberg_scan"));
    assertFalse(result.contains("WHERE"));
  }

  // ====== buildConsolidationSql tests ======

  @Test void testBuildConsolidationSql() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildConsolidationSql", String.class, String.class, List.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/temp/base", "/final/base", Arrays.asList("year", "geo"));

    assertNotNull(result);
    assertTrue(result.contains("COPY"));
    assertTrue(result.contains("read_parquet"));
    assertTrue(result.contains("/**/*.parquet"));
    assertTrue(result.contains("PARTITION_BY"));
    assertTrue(result.contains("year"));
    assertTrue(result.contains("geo"));
    assertTrue(result.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildConsolidationSqlNoPartitions() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildConsolidationSql", String.class, String.class, List.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/temp/base", "/final/base", Collections.emptyList());

    assertNotNull(result);
    assertTrue(result.contains("COPY"));
    assertFalse(result.contains("PARTITION_BY"));
    assertFalse(result.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildConsolidationSqlNullPartitions() throws Exception {
    Method method =
        ParquetReorganizer.class.getDeclaredMethod("buildConsolidationSql", String.class, String.class, List.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(reorganizer, "/temp/base", "/final/base", null);

    assertNotNull(result);
    assertFalse(result.contains("PARTITION_BY"));
  }

  // ====== create() factory method test ======

  @Test void testCreateFactoryMethod() {
    ParquetReorganizer created =
        ParquetReorganizer.create(mockStorageProvider, "/base/dir");
    assertNotNull(created);
  }
}
