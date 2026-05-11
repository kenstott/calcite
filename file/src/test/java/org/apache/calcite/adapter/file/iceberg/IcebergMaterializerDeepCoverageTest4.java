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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.ColumnDefinition;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link IcebergMaterializer} targeting uncovered lines:
 * buildSelectSql, buildSelectSqlWithPaging, buildCountSql, buildDuckDBSql,
 * buildBatchCombinations, groupBatchesByIncrementalKey, coerceValue, findColumnType,
 * extractCiksFromRowFilter, getFilteredSourceAccessions, getSourceAccessions,
 * needsFilenameEmbedding, buildFilenamePattern, selfHealTracker,
 * getTrackedAccessions, processBatchWithRetry error paths,
 * cleanupStagingDirectory, createStagingPath, getSourceFileWatermark,
 * isSourceWatermarkEnabled, partitionHasData,
 * MaterializationConfig builder and getters, MaterializationResult.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class IcebergMaterializerDeepCoverageTest4 {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorage;
  private IncrementalTracker mockTracker;
  private IcebergMaterializer materializer;

  @BeforeEach
  void setUp() {
    mockStorage = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
    doCallRealMethod().when(mockTracker).getProcessedKeyValues(anyString(), any());
    materializer = new IcebergMaterializer(
        tempDir.resolve("warehouse").toString(), mockStorage, mockTracker);
  }

  // ====================================================================
  // MaterializationConfig builder and getter coverage
  // ====================================================================

  @Test
  void testMaterializationConfigFullBuilder() {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    List<IcebergCatalogManager.ColumnDef> tableCols = new ArrayList<IcebergCatalogManager.ColumnDef>();
    tableCols.add(new IcebergCatalogManager.ColumnDef("name", "STRING"));

    Map<String, String> computedCols = new HashMap<String, String>();
    computedCols.put("embedding", "embed_jina(text)::FLOAT[768]");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
            .targetTableId("test_table")
            .sourceTableName("source_table")
            .partitionColumns(partCols)
            .tableColumns(tableCols)
            .batchPartitionColumns(Arrays.asList("year"))
            .incrementalKeys(Arrays.asList("year"))
            .yearRange(2020, 2025)
            .threads(4)
            .description("Test description")
            .computedColumns(computedCols)
            .rowBatchSize(100)
            .rowFilter("cik IN ('001', '002')")
            .icebergTableLocation("/iceberg/table")
            .accessionColumn("acc_num")
            .build();

    assertEquals("/data/*.parquet", config.getSourcePattern());
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
    assertEquals("test_table", config.getTargetTableId());
    assertEquals("source_table", config.getSourceTableName());
    assertEquals(1, config.getPartitionColumns().size());
    assertEquals(1, config.getTableColumns().size());
    assertEquals(Arrays.asList("year"), config.getPartitionColumnNames());
    assertEquals(Arrays.asList("year"), config.getBatchPartitionColumns());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertEquals(2020, config.getStartYear());
    assertEquals(2025, config.getEndYear());
    assertEquals(4, config.getThreads());
    assertEquals("Test description", config.getDescription());
    assertEquals(computedCols, config.getComputedColumns());
    assertEquals(100, config.getRowBatchSize());
    assertEquals("cik IN ('001', '002')", config.getRowFilter());
    assertEquals("/iceberg/table", config.getIcebergTableLocation());
    assertEquals("acc_num", config.getAccessionColumn());
    assertTrue(config.supportsIncremental());
  }

  @Test
  void testMaterializationConfigDefaults() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("table1")
            .build();

    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertTrue(config.getTableColumns().isEmpty());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertTrue(config.getComputedColumns().isEmpty());
    assertEquals(0, config.getRowBatchSize());
    assertNull(config.getRowFilter());
    assertNull(config.getIcebergTableLocation());
    assertEquals("accession_number", config.getAccessionColumn());
    assertFalse(config.supportsIncremental());
    // Description defaults to targetTableId when null
    assertEquals("table1", config.getDescription());
  }

  @Test
  void testMaterializationConfigJsonFormat() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("json_table")
            .build();

    assertEquals(IcebergMaterializer.SourceFormat.JSON, config.getSourceFormat());
  }

  @Test
  void testMaterializationConfigBuilderValidation() {
    // Missing sourcePattern
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .targetTableId("table1")
            .build());

    // Empty sourcePattern
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("")
            .targetTableId("table1")
            .build());

    // Missing targetTableId
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .build());

    // Empty targetTableId
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("")
            .build());
  }

  // ====================================================================
  // MaterializationResult coverage
  // ====================================================================

  @Test
  void testMaterializationResultAllGetters() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table1", 10, 2, 3, 5000);

    assertEquals("table1", result.getTableId());
    assertEquals(10, result.getSuccessCount());
    assertEquals(2, result.getFailedCount());
    assertEquals(3, result.getSkippedCount());
    assertEquals(5000, result.getDurationMs());
    assertFalse(result.isFullySuccessful());
    assertFalse(result.isTableRecreated());
    assertNotNull(result.toString());
    assertTrue(result.toString().contains("table1"));
  }

  @Test
  void testMaterializationResultFullySuccessful() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table2", 5, 0, 0, 1000);
    assertTrue(result.isFullySuccessful());
  }

  @Test
  void testMaterializationResultWithTableRecreated() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table3", 5, 0, 0, 1000, true);
    assertTrue(result.isTableRecreated());
    assertTrue(result.toString().contains("recreated=true"));
  }

  // ====================================================================
  // extractCiksFromRowFilter (package-level static method)
  // ====================================================================

  @Test
  void testExtractCiksFromRowFilterNull() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(null);
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterEmpty() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterNoCikClause() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("year = 2022");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterStandard() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ('0000320193', '0000004962')");
    assertEquals(2, ciks.size());
    assertTrue(ciks.contains("0000320193"));
    assertTrue(ciks.contains("0000004962"));
  }

  @Test
  void testExtractCiksFromRowFilterDoubleSpace() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik  IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test
  void testExtractCiksFromRowFilterNoParen() {
    // "cik IN" but no opening paren
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("cik IN '001'");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterNoCloseParen() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("cik IN ('001', '002'");
    assertTrue(ciks.isEmpty());
  }

  // ====================================================================
  // buildSelectSql via reflection
  // ====================================================================

  @Test
  void testBuildSelectSqlBasicParquet() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), null);

    assertNotNull(sql);
    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("read_parquet"));
    assertTrue(sql.contains("hive_partitioning=true"));
  }

  @Test
  void testBuildSelectSqlJsonFormat() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.json", new HashMap<String, String>(), null);

    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildSelectSqlWithComputedColumns() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Map<String, String> computed = new HashMap<String, String>();
    computed.put("full_name", "first_name || ' ' || last_name");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .computedColumns(computed)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), null);

    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("AS full_name"));
  }

  @Test
  void testBuildSelectSqlWithRowFilter() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .rowFilter("cik IN ('001')")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), null);

    assertTrue(sql.contains("WHERE cik IN ('001')"));
  }

  @Test
  void testBuildSelectSqlWithSmallExclusions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");
    exclusions.add("acc2");

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), exclusions);

    assertTrue(sql.contains("NOT IN"));
    assertTrue(sql.contains("'acc1'") || sql.contains("'acc2'"));
  }

  @Test
  void testBuildSelectSqlWithLargeExclusions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    // Build a large exclusion set > 100
    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 150; i++) {
      exclusions.add("acc_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), exclusions);

    assertTrue(sql.contains("NOT EXISTS"));
    assertTrue(sql.contains("_exclusions"));
  }

  @Test
  void testBuildSelectSqlWithRowFilterAndExclusions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .rowFilter("year = 2022")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), exclusions);

    assertTrue(sql.contains("WHERE year = 2022"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("NOT IN"));
  }

  // ====================================================================
  // buildSelectSqlWithPaging via reflection
  // ====================================================================

  @Test
  void testBuildSelectSqlWithPagingBasic() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), 100, 0, null);

    assertTrue(sql.contains("LIMIT 100"));
    assertFalse(sql.contains("OFFSET"));
  }

  @Test
  void testBuildSelectSqlWithPagingAndOffset() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), 100, 200, null);

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 200"));
  }

  @Test
  void testBuildSelectSqlWithPagingJsonFormat() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.json", new HashMap<String, String>(), 50, 0, null);

    assertTrue(sql.contains("read_json"));
    assertTrue(sql.contains("LIMIT 50"));
  }

  @Test
  void testBuildSelectSqlWithPagingComputedColumns() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    Map<String, String> computed = new HashMap<String, String>();
    computed.put("total", "price * quantity");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .computedColumns(computed)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), 100, 0, null);

    assertTrue(sql.contains("SELECT *,"));
    assertTrue(sql.contains("AS total"));
  }

  @Test
  void testBuildSelectSqlWithPagingRowFilterAndExclusions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .rowFilter("status = 'active'")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), 100, 50, exclusions);

    assertTrue(sql.contains("WHERE status = 'active'"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("NOT IN"));
    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 50"));
  }

  @Test
  void testBuildSelectSqlWithPagingLargeExclusions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 150; i++) {
      exclusions.add("acc_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", new HashMap<String, String>(), 100, 0, exclusions);

    assertTrue(sql.contains("NOT EXISTS"));
  }

  // ====================================================================
  // buildCountSql via reflection
  // ====================================================================

  @Test
  void testBuildCountSqlParquet() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config, "/data/*.parquet");

    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertTrue(sql.contains("read_parquet"));
  }

  @Test
  void testBuildCountSqlJson() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config, "/data/*.json");

    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildCountSqlWithRowFilter() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .rowFilter("year = 2022")
            .build();

    String sql = (String) method.invoke(materializer, config, "/data/*.parquet");

    assertTrue(sql.contains("WHERE year = 2022"));
  }

  // ====================================================================
  // buildDuckDBSql via reflection
  // ====================================================================

  @Test
  void testBuildDuckDBSqlBasic() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out.parquet",
        new HashMap<String, String>(), -1, -1);

    assertTrue(sql.contains("COPY ("));
    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("FORMAT PARQUET"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test
  void testBuildDuckDBSqlJsonFormat() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.json", "/staging/out",
        new HashMap<String, String>(), -1, -1);

    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildDuckDBSqlWithComputedColumns() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    Map<String, String> computed = new HashMap<String, String>();
    computed.put("embed", "embed_jina(text)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .computedColumns(computed)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out",
        new HashMap<String, String>(), -1, -1);

    assertTrue(sql.contains("AS embed"));
  }

  @Test
  void testBuildDuckDBSqlWithPartitionColumns() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));
    partCols.add(new ColumnDefinition("month", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .partitionColumns(partCols)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out",
        new HashMap<String, String>(), -1, -1);

    assertTrue(sql.contains("PARTITION_BY (year, month)"));
  }

  @Test
  void testBuildDuckDBSqlWithRowFilter() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .rowFilter("status = 'active'")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out",
        new HashMap<String, String>(), -1, -1);

    assertTrue(sql.contains("WHERE status = 'active'"));
  }

  @Test
  void testBuildDuckDBSqlWithLimitOffset() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out",
        new HashMap<String, String>(), 100, 200);

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 200"));
    // With limit > 0, no PARTITION_BY or OVERWRITE_OR_IGNORE
    assertFalse(sql.contains("PARTITION_BY"));
  }

  // ====================================================================
  // needsFilenameEmbedding and buildFilenamePattern via reflection
  // ====================================================================

  @Test
  void testNeedsFilenameEmbeddingEmptyBatch() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    assertFalse((Boolean) method.invoke(materializer, config,
        new HashMap<String, String>()));
  }

  @Test
  void testNeedsFilenameEmbeddingBatchInPartition() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .partitionColumns(partCols)
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2022");

    assertFalse((Boolean) method.invoke(materializer, config, batch));
  }

  @Test
  void testNeedsFilenameEmbeddingBatchNotInPartition() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .partitionColumns(partCols)
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("source_type", "10-K");

    assertTrue((Boolean) method.invoke(materializer, config, batch));
  }

  @Test
  void testBuildFilenamePattern() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    method.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("type", "10-K");
    batch.put("year", "2022");

    String pattern = (String) method.invoke(materializer, batch);
    assertNotNull(pattern);
    assertTrue(pattern.contains("type_10-K"));
    assertTrue(pattern.contains("year_2022"));
    assertTrue(pattern.endsWith("_{i}"));
  }

  // ====================================================================
  // buildBatchCombinations and groupBatchesByIncrementalKey via reflection
  // ====================================================================

  @Test
  void testBuildBatchCombinationsEmpty() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations",
        IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    @SuppressWarnings("unchecked")
    List<Map<String, String>> batches =
        (List<Map<String, String>>) method.invoke(materializer, config);
    assertTrue(batches.isEmpty());
  }

  @Test
  void testBuildBatchCombinationsWithYearRange() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations",
        IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .batchPartitionColumns(Arrays.asList("year"))
            .yearRange(2020, 2022)
            .build();

    @SuppressWarnings("unchecked")
    List<Map<String, String>> batches =
        (List<Map<String, String>>) method.invoke(materializer, config);
    assertEquals(3, batches.size());
    assertEquals("2020", batches.get(0).get("year"));
    assertEquals("2021", batches.get(1).get("year"));
    assertEquals("2022", batches.get(2).get("year"));
  }

  @Test
  void testGroupBatchesByIncrementalKey() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> b1 = new LinkedHashMap<String, String>();
    b1.put("year", "2020");
    b1.put("type", "10-K");
    batches.add(b1);

    Map<String, String> b2 = new LinkedHashMap<String, String>();
    b2.put("year", "2020");
    b2.put("type", "10-Q");
    batches.add(b2);

    Map<String, String> b3 = new LinkedHashMap<String, String>();
    b3.put("year", "2021");
    b3.put("type", "10-K");
    batches.add(b3);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, Arrays.asList("year"));

    assertEquals(2, grouped.size()); // Two unique year values
  }

  @Test
  void testGroupBatchesByIncrementalKeyNoKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    batches.add(new HashMap<String, String>());
    batches.add(new HashMap<String, String>());

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, (Object) null);

    // All batches in one group since no incremental keys
    assertEquals(1, grouped.size());
  }

  // ====================================================================
  // coerceValue via reflection
  // ====================================================================

  @Test
  void testCoerceValue() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(42, method.invoke(materializer, "42", "INTEGER"));
    assertEquals(42, method.invoke(materializer, "42", "INT"));
    assertEquals(42L, method.invoke(materializer, "42", "BIGINT"));
    assertEquals(42L, method.invoke(materializer, "42", "LONG"));
    assertEquals(3.14, method.invoke(materializer, "3.14", "DOUBLE"));
    assertEquals(true, method.invoke(materializer, "true", "BOOLEAN"));
    assertEquals("hello", method.invoke(materializer, "hello", "VARCHAR"));
    // Null cases
    assertNull(method.invoke(materializer, null, "INTEGER"));
    assertEquals("hello", method.invoke(materializer, "hello", null));
  }

  // ====================================================================
  // findColumnType via reflection
  // ====================================================================

  @Test
  void testFindColumnType() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    method.setAccessible(true);

    List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
    columns.add(new ColumnDefinition("year", "INTEGER"));
    columns.add(new ColumnDefinition("name", "VARCHAR"));

    assertEquals("INTEGER", method.invoke(materializer, columns, "year"));
    assertEquals("VARCHAR", method.invoke(materializer, columns, "name"));
    assertEquals("VARCHAR", method.invoke(materializer, columns, "nonexistent"));
  }

  // ====================================================================
  // countAllAccessions via reflection
  // ====================================================================

  @Test
  void testCountAllAccessions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    Map<String, Set<String>> cikToAccessions = new HashMap<String, Set<String>>();
    cikToAccessions.put("cik1", new HashSet<String>(Arrays.asList("a1", "a2")));
    cikToAccessions.put("cik2", new HashSet<String>(Arrays.asList("a3")));

    assertEquals(3, method.invoke(materializer, cikToAccessions));
  }

  @Test
  void testCountAllAccessionsEmpty() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    assertEquals(0, method.invoke(materializer, new HashMap<String, Set<String>>()));
  }

  // ====================================================================
  // getFilteredSourceAccessions via reflection
  // ====================================================================

  @Test
  void testGetFilteredSourceAccessionsNullStorageProvider() throws Exception {
    // Use a materializer with null storageProvider
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    Set<String> result =
        (Set<String>) method.invoke(mat, "s3://bucket/year=*/*.parquet", "2022", null);
    assertNull(result);
  }

  // ====================================================================
  // getSourceAccessions with storage provider returning null
  // ====================================================================

  @Test
  void testGetSourceAccessionsNoYearWildcard() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    // Source pattern without year=* wildcard
    Object result = method.invoke(materializer, "/data/files/*.parquet", "2022");
    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsNullStorageProvider() throws Exception {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    Object result = method.invoke(mat, "s3://bucket/year=*/*.parquet", "2022");
    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsWithCaching() throws Exception {
    // Set up mock storage to return file entries
    List<StorageProvider.FileEntry> fileEntries = new ArrayList<StorageProvider.FileEntry>();
    fileEntries.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2022/0001_0001234-22-001_metadata.parquet",
        "0001_0001234-22-001_metadata.parquet",
        false, 1000, System.currentTimeMillis()));

    when(mockStorage.listFiles(anyString(), anyBoolean())).thenReturn(fileEntries);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    // First call populates cache
    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result1 =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_metadata.parquet", "2022");
    assertNotNull(result1);

    // Second call should use cache
    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result2 =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_metadata.parquet", "2022");
    assertNotNull(result2);
  }

  @Test
  void testGetSourceAccessionsFilePatternParsing() throws Exception {
    List<StorageProvider.FileEntry> fileEntries = new ArrayList<StorageProvider.FileEntry>();
    fileEntries.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2022/0001_acc1_facts.parquet",
        "0001_acc1_facts.parquet",
        false, 1000, System.currentTimeMillis()));
    fileEntries.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2022/0002_acc2_facts.parquet",
        "0002_acc2_facts.parquet",
        false, 2000, System.currentTimeMillis()));

    when(mockStorage.listFiles(anyString(), anyBoolean())).thenReturn(fileEntries);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2022");

    assertNotNull(result);
    assertTrue(result.containsKey("0001"));
    assertTrue(result.containsKey("0002"));
    assertTrue(result.get("0001").contains("acc1"));
    assertTrue(result.get("0002").contains("acc2"));
  }

  // ====================================================================
  // getTrackedAccessions via reflection
  // ====================================================================

  @Test
  void testGetTrackedAccessions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry1 = new LinkedHashMap<String, String>();
    entry1.put("year", "2022");
    entry1.put("accession_number", "acc-001");
    processed.add(entry1);

    Map<String, String> entry2 = new LinkedHashMap<String, String>();
    entry2.put("year", "2023");
    entry2.put("accession_number", "acc-002");
    processed.add(entry2);

    when(mockTracker.getProcessedKeyValues("test_table")).thenReturn(processed);

    @SuppressWarnings("unchecked")
    Set<String> accessions = (Set<String>) method.invoke(
        materializer, "test_table", "2022");

    assertEquals(1, accessions.size());
    assertTrue(accessions.contains("acc-001"));
  }

  @Test
  void testGetTrackedAccessionsNullYear() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry1 = new LinkedHashMap<String, String>();
    entry1.put("year", "2022");
    entry1.put("accession_number", "acc-001");
    processed.add(entry1);

    when(mockTracker.getProcessedKeyValues("test_table")).thenReturn(processed);

    @SuppressWarnings("unchecked")
    Set<String> accessions = (Set<String>) method.invoke(
        materializer, "test_table", null);

    // When yearValue is null, all entries are included
    assertEquals(1, accessions.size());
  }

  @Test
  void testGetTrackedAccessionsException() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    when(mockTracker.getProcessedKeyValues("test_table"))
        .thenThrow(new RuntimeException("db error"));

    @SuppressWarnings("unchecked")
    Set<String> accessions = (Set<String>) method.invoke(
        materializer, "test_table", "2022");

    assertTrue(accessions.isEmpty());
  }

  // ====================================================================
  // getExcludedAccessions via reflection
  // ====================================================================

  @Test
  void testGetExcludedAccessionsNoIcebergLocation() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions",
        IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    @SuppressWarnings("unchecked")
    Set<String> excluded = (Set<String>) method.invoke(
        materializer, config, null, new HashMap<String, String>());

    assertTrue(excluded.isEmpty());
  }

  // ====================================================================
  // isSourceWatermarkEnabled via reflection
  // ====================================================================

  @Test
  void testIsSourceWatermarkEnabled() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "isSourceWatermarkEnabled",
        IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .build();

    assertTrue((Boolean) method.invoke(materializer, config));
  }

  // ====================================================================
  // cleanupStagingDirectory via reflection
  // ====================================================================

  @Test
  void testCleanupStagingDirectoryS3Skipped() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // S3 paths should be skipped
    method.invoke(materializer, "s3://bucket/staging/test");
    method.invoke(materializer, "s3a://bucket/staging/test");
    // No exception means success
  }

  @Test
  void testCleanupStagingDirectoryLocalPath() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Local path that doesn't exist should not throw
    when(mockStorage.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    method.invoke(materializer, tempDir.resolve("nonexistent").toString());
  }

  // ====================================================================
  // fetchRows via DuckDB (real connection)
  // ====================================================================

  @Test
  void testFetchRows() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "fetchRows", Connection.class, String.class);
    method.setAccessible(true);

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = (List<Map<String, Object>>) method.invoke(
          materializer, conn, "SELECT 1 AS id, 'hello' AS name");

      assertEquals(1, rows.size());
      assertEquals(1, rows.get(0).get("id"));
      assertEquals("hello", rows.get(0).get("name"));
    } finally {
      conn.close();
    }
  }

  @Test
  void testFetchRowsEmpty() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "fetchRows", Connection.class, String.class);
    method.setAccessible(true);

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = (List<Map<String, Object>>) method.invoke(
          materializer, conn,
          "SELECT 1 AS id WHERE 1 = 0");
      assertTrue(rows.isEmpty());
    } finally {
      conn.close();
    }
  }

  @Test
  void testFetchRowsNullValues() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "fetchRows", Connection.class, String.class);
    method.setAccessible(true);

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = (List<Map<String, Object>>) method.invoke(
          materializer, conn,
          "SELECT 1 AS id, NULL AS empty_col");

      assertEquals(1, rows.size());
      assertEquals(1, rows.get(0).get("id"));
      // Null values should NOT be added to the map
      assertFalse(rows.get(0).containsKey("empty_col"));
    } finally {
      conn.close();
    }
  }

  // ====================================================================
  // createExclusionTempTable via DuckDB (real connection)
  // ====================================================================

  @Test
  void testCreateExclusionTempTable() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "createExclusionTempTable", Connection.class, String.class, Set.class);
    method.setAccessible(true);

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      Set<String> accessions = new HashSet<String>();
      for (int i = 0; i < 150; i++) {
        accessions.add("acc_" + i);
      }

      method.invoke(materializer, conn, "accession_number", accessions);

      // Verify the temp table was created and populated
      java.sql.Statement stmt = conn.createStatement();
      java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM _exclusions");
      assertTrue(rs.next());
      assertEquals(150, rs.getLong(1));
      rs.close();
      stmt.close();
    } finally {
      conn.close();
    }
  }

  // ====================================================================
  // Constructor S3 config branches
  // ====================================================================

  @Test
  void testConstructorWithS3Config() {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "SECRET");
    s3Config.put("endpoint", "https://s3.example.com");

    when(mockStorage.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorage, mockTracker);
    assertNotNull(mat);
  }

  @Test
  void testConstructorWithEmptyS3Config() {
    when(mockStorage.getS3Config())
        .thenReturn(new HashMap<String, String>());

    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorage, mockTracker);
    assertNotNull(mat);
  }

  @Test
  void testConstructorWithNullS3Config() {
    when(mockStorage.getS3Config()).thenReturn(null);

    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorage, mockTracker);
    assertNotNull(mat);
  }

  @Test
  void testConstructorWithNullTracker() {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorage, null);
    assertNotNull(mat);
  }

  @Test
  void testConstructorWithCustomRetries() {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorage, mockTracker, 5, 2000);
    assertNotNull(mat);
  }

  @Test
  void testConstructorWithNullStorageProvider() {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), null, mockTracker);
    assertNotNull(mat);
  }

  // ====================================================================
  // SourceFormat enum
  // ====================================================================

  @Test
  void testSourceFormatValues() {
    assertEquals(2, IcebergMaterializer.SourceFormat.values().length);
    assertEquals(IcebergMaterializer.SourceFormat.JSON,
        IcebergMaterializer.SourceFormat.valueOf("JSON"));
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET,
        IcebergMaterializer.SourceFormat.valueOf("PARQUET"));
  }

  // ====================================================================
  // getSourceCountForBatch via reflection using real DuckDB
  // ====================================================================

  @Test
  void testGetSourceCountForBatchException() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/nonexistent/*.parquet")
            .targetTableId("t1")
            .build();

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      long count = (long) method.invoke(materializer, conn, config,
          "/nonexistent/*.parquet", new HashMap<String, String>());
      // Should return -1 on error
      assertEquals(-1, count);
    } finally {
      conn.close();
    }
  }

  @Test
  void testGetSourceCountForBatchWithConditions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/nonexistent/*.parquet")
            .targetTableId("t1")
            .rowFilter("status = 'active'")
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2022");
    batch.put("type", "annual");

    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try {
      long count = (long) method.invoke(materializer, conn, config,
          "/nonexistent/*.parquet", batch);
      // Should return -1 on error since files don't exist
      assertEquals(-1, count);
    } finally {
      conn.close();
    }
  }

  // ====================================================================
  // buildCombinationsRecursive via reflection
  // ====================================================================

  @Test
  void testBuildCombinationsRecursive() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> names = Arrays.asList("year", "type");
    List<List<String>> values = new ArrayList<List<String>>();
    values.add(Arrays.asList("2020", "2021"));
    values.add(Arrays.asList("A", "B"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    method.invoke(materializer, names, values, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size()); // 2 x 2 = 4 combinations
  }

  // ====================================================================
  // DuckDB buildDuckDBSql with filename embedding
  // ====================================================================

  @Test
  void testBuildDuckDBSqlWithFilenameEmbedding() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("/data/*.parquet")
            .targetTableId("t1")
            .partitionColumns(partCols)
            .build();

    // Batch key not in partition columns
    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("source_type", "10-K");

    String sql = (String) method.invoke(materializer, config,
        "/data/*.parquet", "/staging/out",
        batch, -1, -1);

    assertTrue(sql.contains("FILENAME_PATTERN"));
    assertTrue(sql.contains("source_type_10-K"));
  }
}
