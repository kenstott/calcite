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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ParquetReorganizer}.
 * Focuses on ReorgConfig builder, batch combination logic, and utility methods.
 */
@Tag("unit")
class ParquetReorganizerDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ===== ReorgConfig Builder Tests =====

  @Test void testReorgConfigBuilderMinimal() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/*.parquet")
        .targetBase("type=income_by_geo")
        .build();

    assertEquals("type=income/year=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_by_geo", config.getTargetBase());
    assertNotNull(config.getPartitionColumns());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertNotNull(config.getColumnMappings());
    assertTrue(config.getColumnMappings().isEmpty());
    assertNotNull(config.getBatchPartitionColumns());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertEquals(0, config.getStartYear());
    assertEquals(0, config.getEndYear());
    // Default threads is 2
    assertEquals(2, config.getThreads());
    assertNotNull(config.getIncrementalKeys());
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertNotNull(config.getIncrementalTracker());
    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigBuilderFull() {
    List<String> partCols = Arrays.asList("geo", "year");
    Map<String, String> mappings = new HashMap<String, String>();
    mappings.put("geo", "GeoFips");
    List<String> batchCols = Arrays.asList("year", "geo_fips_set");
    List<String> incrKeys = Arrays.asList("year");

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("type=income/year=*/*.parquet")
        .targetBase("type=income_by_geo")
        .partitionColumns(partCols)
        .columnMappings(mappings)
        .batchPartitionColumns(batchCols)
        .yearRange(2020, 2024)
        .name("test-reorg")
        .threads(4)
        .incrementalKeys(incrKeys)
        .incrementalTracker(IncrementalTracker.NOOP)
        .sourceTable("source_table")
        .sourceIsIceberg(true)
        .icebergWarehousePath("/warehouse")
        .currentYearTtlDays(7)
        .build();

    assertEquals("type=income/year=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_by_geo", config.getTargetBase());
    assertEquals(partCols, config.getPartitionColumns());
    assertEquals(mappings, config.getColumnMappings());
    assertEquals(batchCols, config.getBatchPartitionColumns());
    assertEquals(2020, config.getStartYear());
    assertEquals(2024, config.getEndYear());
    assertEquals("test-reorg", config.getName());
    assertEquals(4, config.getThreads());
    assertEquals(incrKeys, config.getIncrementalKeys());
    assertEquals("source_table", config.getSourceTable());
    assertTrue(config.isSourceIsIceberg());
    assertEquals("/warehouse", config.getIcebergWarehousePath());
    assertEquals(7, config.getCurrentYearTtlDays());
    // getCurrentYearTtlMillis = 7 * 24 * 60 * 60 * 1000
    assertEquals(7L * 24 * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
    // NOOP tracker with keys means supportsIncremental is false because NOOP == IncrementalTracker.NOOP
    assertFalse(config.supportsIncremental());
  }

  @Test void testReorgConfigBuilderNullSourcePattern() {
    assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .targetBase("target")
            .build());
  }

  @Test void testReorgConfigBuilderEmptySourcePattern() {
    assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("")
            .targetBase("target")
            .build());
  }

  @Test void testReorgConfigBuilderNullTargetBase() {
    assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("*.parquet")
            .build());
  }

  @Test void testReorgConfigBuilderEmptyTargetBase() {
    assertThrows(IllegalArgumentException.class, () ->
        ParquetReorganizer.ReorgConfig.builder()
            .sourcePattern("*.parquet")
            .targetBase("")
            .build());
  }

  @Test void testReorgConfigDefaultCurrentYearTtlDays() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .currentYearTtlDays(0)
        .build();

    // Default is 1 day when 0 or negative
    assertEquals(1, config.getCurrentYearTtlDays());
    assertEquals(24L * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
  }

  @Test void testReorgConfigDefaultThreads() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("target")
        .threads(0)
        .build();

    // Default is 2 when 0 or negative
    assertEquals(2, config.getThreads());
  }

  // ===== Create Method Test =====

  @Test void testCreateStaticMethod() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = ParquetReorganizer.create(mockProvider, "/base");
    assertNotNull(reorganizer);
  }

  // ===== isNumeric via Reflection =====

  @Test void testIsNumericViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method isNumeric = ParquetReorganizer.class.getDeclaredMethod("isNumeric", String.class);
    isNumeric.setAccessible(true);

    assertTrue((Boolean) isNumeric.invoke(reorganizer, "123"));
    assertTrue((Boolean) isNumeric.invoke(reorganizer, "0"));
    assertTrue((Boolean) isNumeric.invoke(reorganizer, "-42"));
    assertTrue((Boolean) isNumeric.invoke(reorganizer, "9999999999"));
    assertFalse((Boolean) isNumeric.invoke(reorganizer, "abc"));
    assertFalse((Boolean) isNumeric.invoke(reorganizer, "12.34"));
    assertFalse((Boolean) isNumeric.invoke(reorganizer, ""));
    assertFalse((Boolean) isNumeric.invoke(reorganizer, (Object) null));
  }

  // ===== quoteLiteral via Reflection =====

  @Test void testQuoteLiteralViaReflection() throws Exception {
    Method quoteLiteral = ParquetReorganizer.class.getDeclaredMethod("quoteLiteral", String.class);
    quoteLiteral.setAccessible(true);

    assertEquals("'hello'", quoteLiteral.invoke(null, "hello"));
    assertEquals("'it''s'", quoteLiteral.invoke(null, "it's"));
    assertEquals("'path/to/file'", quoteLiteral.invoke(null, "path/to/file"));
    assertEquals("''", quoteLiteral.invoke(null, ""));
  }

  // ===== isCurrentYear via Reflection =====

  @Test void testIsCurrentYearViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method isCurrentYear = ParquetReorganizer.class.getDeclaredMethod("isCurrentYear", Map.class);
    isCurrentYear.setAccessible(true);

    int currentYear = java.time.Year.now().getValue();

    Map<String, String> currentYearMap = new HashMap<String, String>();
    currentYearMap.put("year", String.valueOf(currentYear));
    assertTrue((Boolean) isCurrentYear.invoke(reorganizer, currentYearMap));

    Map<String, String> pastYearMap = new HashMap<String, String>();
    pastYearMap.put("year", "2020");
    assertFalse((Boolean) isCurrentYear.invoke(reorganizer, pastYearMap));

    Map<String, String> noYearMap = new HashMap<String, String>();
    noYearMap.put("geo", "US");
    assertFalse((Boolean) isCurrentYear.invoke(reorganizer, noYearMap));

    assertFalse((Boolean) isCurrentYear.invoke(reorganizer, (Object) null));
    assertFalse((Boolean) isCurrentYear.invoke(reorganizer, Collections.emptyMap()));
  }

  // ===== buildCombinationsRecursive via Reflection =====

  @Test void testBuildCombinationsRecursiveViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildCombinations = ParquetReorganizer.class.getDeclaredMethod(
        "buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    buildCombinations.setAccessible(true);

    List<String> colNames = Arrays.asList("year", "geo");
    List<List<String>> colValues = Arrays.asList(
        Arrays.asList("2020", "2021"),
        Arrays.asList("STATE", "COUNTY")
    );
    java.util.List<Map<String, String>> result = new java.util.ArrayList<Map<String, String>>();

    buildCombinations.invoke(reorganizer, colNames, colValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size());
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("STATE", result.get(0).get("geo"));
    assertEquals("2020", result.get(1).get("year"));
    assertEquals("COUNTY", result.get(1).get("geo"));
    assertEquals("2021", result.get(2).get("year"));
    assertEquals("STATE", result.get(2).get("geo"));
    assertEquals("2021", result.get(3).get("year"));
    assertEquals("COUNTY", result.get(3).get("geo"));
  }

  @Test void testBuildCombinationsRecursiveSingleColumn() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildCombinations = ParquetReorganizer.class.getDeclaredMethod(
        "buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    buildCombinations.setAccessible(true);

    List<String> colNames = Arrays.asList("year");
    List<List<String>> colValues = Arrays.asList(
        Arrays.asList("2020", "2021", "2022")
    );
    java.util.List<Map<String, String>> result = new java.util.ArrayList<Map<String, String>>();

    buildCombinations.invoke(reorganizer, colNames, colValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(3, result.size());
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("2021", result.get(1).get("year"));
    assertEquals("2022", result.get(2).get("year"));
  }

  // ===== groupBatchesByIncrementalKey via Reflection =====

  @Test void testGroupBatchesByIncrementalKeyViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method groupMethod = ParquetReorganizer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    groupMethod.setAccessible(true);

    Map<String, String> batch1 = new LinkedHashMap<String, String>();
    batch1.put("year", "2020");
    batch1.put("geo", "STATE");

    Map<String, String> batch2 = new LinkedHashMap<String, String>();
    batch2.put("year", "2020");
    batch2.put("geo", "COUNTY");

    Map<String, String> batch3 = new LinkedHashMap<String, String>();
    batch3.put("year", "2021");
    batch3.put("geo", "STATE");

    List<Map<String, String>> batches = Arrays.asList(batch1, batch2, batch3);
    List<String> incrementalKeys = Arrays.asList("year");

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        (Map<Map<String, String>, List<Map<String, String>>>) groupMethod.invoke(
            reorganizer, batches, incrementalKeys);

    assertEquals(2, grouped.size());

    // Key {"year":"2020"} should have 2 batches
    Map<String, String> key2020 = new LinkedHashMap<String, String>();
    key2020.put("year", "2020");
    assertTrue(grouped.containsKey(key2020));
    assertEquals(2, grouped.get(key2020).size());

    // Key {"year":"2021"} should have 1 batch
    Map<String, String> key2021 = new LinkedHashMap<String, String>();
    key2021.put("year", "2021");
    assertTrue(grouped.containsKey(key2021));
    assertEquals(1, grouped.get(key2021).size());
  }

  @Test void testGroupBatchesByIncrementalKeyNullKeys() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method groupMethod = ParquetReorganizer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    groupMethod.setAccessible(true);

    Map<String, String> batch1 = new LinkedHashMap<String, String>();
    batch1.put("geo", "STATE");

    List<Map<String, String>> batches = Arrays.asList(batch1);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        (Map<Map<String, String>, List<Map<String, String>>>) groupMethod.invoke(
            reorganizer, batches, (Object) null);

    // All batches grouped under empty key
    assertEquals(1, grouped.size());
    assertTrue(grouped.containsKey(Collections.emptyMap()));
  }

  // ===== buildConsolidationSql via Reflection =====

  @Test void testBuildConsolidationSqlViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildConsolidation = ParquetReorganizer.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class);
    buildConsolidation.setAccessible(true);

    String sql = (String) buildConsolidation.invoke(reorganizer,
        "/tmp/temp", "/output/final", Arrays.asList("geo", "year"));

    assertNotNull(sql);
    assertTrue(sql.contains("/tmp/temp/**/*.parquet"));
    assertTrue(sql.contains("/output/final"));
    assertTrue(sql.contains("PARTITION_BY (geo, year)"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildConsolidationSqlNoPartitions() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildConsolidation = ParquetReorganizer.class.getDeclaredMethod(
        "buildConsolidationSql", String.class, String.class, List.class);
    buildConsolidation.setAccessible(true);

    String sql = (String) buildConsolidation.invoke(reorganizer,
        "/tmp/temp", "/output/final", Collections.emptyList());

    assertNotNull(sql);
    assertFalse(sql.contains("PARTITION_BY"));
  }

  // ===== buildReorganizationSql via Reflection =====

  @Test void testBuildReorganizationSqlParquetSource() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    // Access the 5-arg overload
    Method buildReorgSql = ParquetReorganizer.class.getDeclaredMethod(
        "buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class);
    buildReorgSql.setAccessible(true);

    Map<String, String> mappings = new LinkedHashMap<String, String>();
    mappings.put("geo", "GeoFips");

    String sql = (String) buildReorgSql.invoke(reorganizer,
        "/data/source/*.parquet", "/data/target",
        Arrays.asList("geo", "year"), mappings, "batch_{i}");

    assertNotNull(sql);
    assertTrue(sql.contains("read_parquet"));
    assertTrue(sql.contains("hive_partitioning=true"));
    assertTrue(sql.contains("PARTITION_BY (geo, year)"));
    assertTrue(sql.contains("FILENAME_PATTERN"));
    assertTrue(sql.contains("GeoFips"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildReorganizationSqlIcebergSource() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    // Access the 9-arg overload
    Method buildReorgSql = ParquetReorganizer.class.getDeclaredMethod(
        "buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class,
        boolean.class, String.class, String.class, Map.class);
    buildReorgSql.setAccessible(true);

    Map<String, String> filters = new LinkedHashMap<String, String>();
    filters.put("year", "2020");
    filters.put("state", "CA");

    String sql = (String) buildReorgSql.invoke(reorganizer,
        null, "/data/target",
        Arrays.asList("geo"), Collections.emptyMap(), "data_{i}",
        true, "/warehouse", "my_table", filters);

    assertNotNull(sql);
    assertTrue(sql.contains("iceberg_scan"));
    assertTrue(sql.contains("/warehouse/my_table"));
    assertTrue(sql.contains("year = 2020"));
    assertTrue(sql.contains("state = 'CA'"));
    assertTrue(sql.contains("PARTITION_BY (geo)"));
  }

  @Test void testBuildReorganizationSqlIcebergWithColumnMappings() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildReorgSql = ParquetReorganizer.class.getDeclaredMethod(
        "buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class,
        boolean.class, String.class, String.class, Map.class);
    buildReorgSql.setAccessible(true);

    Map<String, String> mappings = new LinkedHashMap<String, String>();
    mappings.put("geo", "GeoFips");

    String sql = (String) buildReorgSql.invoke(reorganizer,
        null, "/data/target",
        Arrays.asList("geo"), mappings, "data_{i}",
        true, "/warehouse", "table", null);

    assertNotNull(sql);
    assertTrue(sql.contains("iceberg_scan"));
    assertTrue(sql.contains("_aliased"));
    assertTrue(sql.contains("GeoFips"));
  }

  @Test void testBuildReorganizationSqlNoPartitions() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    ParquetReorganizer reorganizer = new ParquetReorganizer(mockProvider, "/base");

    Method buildReorgSql = ParquetReorganizer.class.getDeclaredMethod(
        "buildReorganizationSql", String.class, String.class,
        List.class, Map.class, String.class);
    buildReorgSql.setAccessible(true);

    String sql = (String) buildReorgSql.invoke(reorganizer,
        "/data/source/*.parquet", "/data/target",
        Collections.emptyList(), Collections.emptyMap(), null);

    assertNotNull(sql);
    assertFalse(sql.contains("PARTITION_BY"));
    assertFalse(sql.contains("FILENAME_PATTERN"));
  }
}
