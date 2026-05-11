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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
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

/**
 * Coverage test 3 for {@link IcebergMaterializer} targeting remaining uncovered lines.
 *
 * <p>Focuses on SQL generation methods, batch combination logic, coercion,
 * configuration edge cases, result objects, and error handling paths.
 */
@Tag("unit")
public class IcebergMaterializerCoverageTest3 {

  @TempDir
  Path tempDir;

  private IcebergMaterializer materializer;
  private StorageProvider mockStorageProvider;

  @BeforeEach
  void setUp() {
    mockStorageProvider = new StorageProvider() {
      @Override public java.io.InputStream openInputStream(String path) {
        return new java.io.ByteArrayInputStream(new byte[0]);
      }
      @Override public void writeFile(String path, java.io.InputStream data) {
      }
      @Override public void writeFile(String path, byte[] data) {
      }
      @Override public boolean delete(String path) {
        return true;
      }
      @Override public int deleteBatch(List<String> paths) {
        return 0;
      }
      @Override public String getStorageType() {
        return "test";
      }
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }
      @Override public void createDirectories(String path) {
      }
      @Override public String resolvePath(String base, String relative) {
        return base + "/" + relative;
      }
      @Override public FileMetadata getMetadata(String path) {
        return null;
      }
      @Override public Map<String, String> getS3Config() {
        Map<String, String> cfg = new HashMap<String, String>();
        cfg.put("accessKeyId", "testKey");
        cfg.put("secretAccessKey", "testSecret");
        cfg.put("endpoint", "http://localhost:9000");
        return cfg;
      }
      @Override public void ensureLifecycleRule(String prefix, int days) {
      }
      @Override public boolean isDirectory(String path) {
        return false;
      }
      @Override public boolean exists(String path) {
        return false;
      }
      @Override public java.io.Reader openReader(String path) {
        return new java.io.StringReader("");
      }
    };
    materializer = new IcebergMaterializer(
        tempDir.toString(), mockStorageProvider, IncrementalTracker.NOOP);
  }

  // ========== MaterializationConfig Builder Tests ==========

  @Test void testBuilderMinimalConfig() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/data/*.parquet")
            .targetTableId("test.table")
            .build();
    assertEquals("s3://bucket/data/*.parquet", config.getSourcePattern());
    assertEquals("test.table", config.getTargetTableId());
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
  }

  @Test void testBuilderAllFields() {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));
    List<IcebergCatalogManager.ColumnDef> tableCols = new ArrayList<IcebergCatalogManager.ColumnDef>();
    tableCols.add(new IcebergCatalogManager.ColumnDef("id", "STRING"));
    List<String> batchCols = Arrays.asList("year");
    List<String> incrKeys = Arrays.asList("year");
    Map<String, String> computedCols = new HashMap<String, String>();
    computedCols.put("hash", "md5(text)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/*.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("db.table1")
            .sourceTableName("src_table")
            .partitionColumns(partCols)
            .tableColumns(tableCols)
            .batchPartitionColumns(batchCols)
            .incrementalKeys(incrKeys)
            .yearRange(2020, 2025)
            .threads(4)
            .description("Test description")
            .computedColumns(computedCols)
            .rowBatchSize(100)
            .rowFilter("cik IN ('0001')")
            .icebergTableLocation("s3://bucket/iceberg/table")
            .accessionColumn("accession_id")
            .build();

    assertEquals("s3://bucket/*.json", config.getSourcePattern());
    assertEquals(IcebergMaterializer.SourceFormat.JSON, config.getSourceFormat());
    assertEquals("db.table1", config.getTargetTableId());
    assertEquals("src_table", config.getSourceTableName());
    assertEquals(1, config.getPartitionColumns().size());
    assertEquals(1, config.getTableColumns().size());
    assertEquals(1, config.getBatchPartitionColumns().size());
    assertEquals(1, config.getIncrementalKeys().size());
    assertEquals(2020, config.getStartYear());
    assertEquals(2025, config.getEndYear());
    assertEquals(4, config.getThreads());
    assertEquals("Test description", config.getDescription());
    assertEquals(1, config.getComputedColumns().size());
    assertEquals(100, config.getRowBatchSize());
    assertEquals("cik IN ('0001')", config.getRowFilter());
    assertEquals("s3://bucket/iceberg/table", config.getIcebergTableLocation());
    assertEquals("accession_id", config.getAccessionColumn());
    assertTrue(config.supportsIncremental());
  }

  @Test void testBuilderDefaultsForNullFields() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("test.parquet")
            .targetTableId("tbl")
            .build();

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
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
  }

  @Test void testBuilderSourcePatternRequired() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .targetTableId("tbl")
            .build());
  }

  @Test void testBuilderEmptySourcePatternRequired() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("")
            .targetTableId("tbl")
            .build());
  }

  @Test void testBuilderTargetTableIdRequired() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .build());
  }

  @Test void testBuilderEmptyTargetTableIdRequired() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("")
            .build());
  }

  @Test void testBuilderDefaultThreads() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .threads(0)
            .build();
    assertEquals(2, config.getThreads()); // DEFAULT_THREADS
  }

  @Test void testBuilderNegativeThreads() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .threads(-1)
            .build();
    assertEquals(2, config.getThreads()); // DEFAULT_THREADS
  }

  @Test void testDescriptionDefaultsToTableId() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("my_table")
            .build();
    assertEquals("my_table", config.getDescription());
  }

  @Test void testGetPartitionColumnNames() {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));
    partCols.add(new ColumnDefinition("month", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .partitionColumns(partCols)
            .build();

    List<String> names = config.getPartitionColumnNames();
    assertEquals(2, names.size());
    assertEquals("year", names.get(0));
    assertEquals("month", names.get(1));
  }

  @Test void testGetPartitionColumnNamesEmpty() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    List<String> names = config.getPartitionColumnNames();
    assertTrue(names.isEmpty());
  }

  // ========== MaterializationResult Tests ==========

  @Test void testMaterializationResult() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("tbl", 5, 2, 1, 3000);
    assertEquals("tbl", result.getTableId());
    assertEquals(5, result.getSuccessCount());
    assertEquals(2, result.getFailedCount());
    assertEquals(1, result.getSkippedCount());
    assertEquals(3000, result.getDurationMs());
    assertFalse(result.isFullySuccessful());
    assertFalse(result.isTableRecreated());
  }

  @Test void testMaterializationResultFullySuccessful() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("tbl", 10, 0, 0, 1000);
    assertTrue(result.isFullySuccessful());
  }

  @Test void testMaterializationResultWithRecreated() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("tbl", 5, 0, 0, 1000, true);
    assertTrue(result.isTableRecreated());
    assertTrue(result.isFullySuccessful());
  }

  @Test void testMaterializationResultToString() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("tbl", 5, 2, 1, 3000, true);
    String str = result.toString();
    assertTrue(str.contains("tbl"));
    assertTrue(str.contains("success=5"));
    assertTrue(str.contains("failed=2"));
    assertTrue(str.contains("skipped=1"));
    assertTrue(str.contains("duration=3000ms"));
    assertTrue(str.contains("recreated=true"));
  }

  // ========== SourceFormat Enum Tests ==========

  @Test void testSourceFormatValues() {
    IcebergMaterializer.SourceFormat[] values = IcebergMaterializer.SourceFormat.values();
    assertEquals(2, values.length);
    assertEquals(IcebergMaterializer.SourceFormat.JSON,
        IcebergMaterializer.SourceFormat.valueOf("JSON"));
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET,
        IcebergMaterializer.SourceFormat.valueOf("PARQUET"));
  }

  // ========== extractCiksFromRowFilter Tests ==========

  @Test void testExtractCiksFromRowFilterNull() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(null);
    assertTrue(ciks.isEmpty());
  }

  @Test void testExtractCiksFromRowFilterEmpty() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("");
    assertTrue(ciks.isEmpty());
  }

  @Test void testExtractCiksFromRowFilterNoCikClause() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("name = 'test'");
    assertTrue(ciks.isEmpty());
  }

  @Test void testExtractCiksFromRowFilterSingleCik() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test void testExtractCiksFromRowFilterMultipleCiks() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter(
            "cik IN ('0000320193', '0000004962', '0001234567')");
    assertEquals(3, ciks.size());
    assertTrue(ciks.contains("0000320193"));
    assertTrue(ciks.contains("0000004962"));
    assertTrue(ciks.contains("0001234567"));
  }

  @Test void testExtractCiksFromRowFilterCaseInsensitive() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test void testExtractCiksFromRowFilterDoubleSpaceIn() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK  IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test void testExtractCiksFromRowFilterNoOpenParen() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK IN '0000320193'");
    assertTrue(ciks.isEmpty());
  }

  @Test void testExtractCiksFromRowFilterNoCloseParen() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK IN ('0000320193'");
    assertTrue(ciks.isEmpty());
  }

  @Test void testExtractCiksFromRowFilterNoQuotes() {
    // Values without quotes should not be parsed
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN (320193, 4962)");
    assertTrue(ciks.isEmpty());
  }

  // ========== Private Method Tests via Reflection ==========

  @Test void testCoerceValueNull() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    Object result = m.invoke(materializer, null, "INTEGER");
    assertNull(result);
  }

  @Test void testCoerceValueNullType() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    Object result = m.invoke(materializer, "42", null);
    assertEquals("42", result);
  }

  @Test void testCoerceValueInteger() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(42, m.invoke(materializer, "42", "INTEGER"));
  }

  @Test void testCoerceValueInt() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(42, m.invoke(materializer, "42", "INT"));
  }

  @Test void testCoerceValueBigint() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(999999999999L, m.invoke(materializer, "999999999999", "BIGINT"));
  }

  @Test void testCoerceValueLong() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(123L, m.invoke(materializer, "123", "LONG"));
  }

  @Test void testCoerceValueDouble() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(3.14, m.invoke(materializer, "3.14", "DOUBLE"));
  }

  @Test void testCoerceValueBoolean() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals(true, m.invoke(materializer, "true", "BOOLEAN"));
  }

  @Test void testCoerceValueString() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod("coerceValue", String.class, String.class);
    m.setAccessible(true);
    assertEquals("hello", m.invoke(materializer, "hello", "VARCHAR"));
  }

  @Test void testFindColumnTypeFound() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    m.setAccessible(true);
    List<ColumnDefinition> cols = new ArrayList<ColumnDefinition>();
    cols.add(new ColumnDefinition("year", "INTEGER"));
    cols.add(new ColumnDefinition("name", "VARCHAR"));
    assertEquals("INTEGER", m.invoke(materializer, cols, "year"));
  }

  @Test void testFindColumnTypeNotFound() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    m.setAccessible(true);
    List<ColumnDefinition> cols = new ArrayList<ColumnDefinition>();
    cols.add(new ColumnDefinition("year", "INTEGER"));
    assertEquals("VARCHAR", m.invoke(materializer, cols, "missing"));
  }

  // ========== buildBatchCombinations and related via Reflection ==========

  @Test void testBuildCombinationsRecursiveBase() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    m.setAccessible(true);

    List<String> colNames = Arrays.asList("year");
    List<List<String>> colValues = new ArrayList<List<String>>();
    colValues.add(Arrays.asList("2020", "2021", "2022"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    m.invoke(materializer, colNames, colValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(3, result.size());
    assertEquals("2020", result.get(0).get("year"));
    assertEquals("2021", result.get(1).get("year"));
    assertEquals("2022", result.get(2).get("year"));
  }

  @Test void testBuildCombinationsRecursiveMultiColumn() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive",
        List.class, List.class, int.class, Map.class, List.class);
    m.setAccessible(true);

    List<String> colNames = Arrays.asList("year", "type");
    List<List<String>> colValues = new ArrayList<List<String>>();
    colValues.add(Arrays.asList("2020", "2021"));
    colValues.add(Arrays.asList("10-K", "10-Q"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    m.invoke(materializer, colNames, colValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size()); // 2 * 2
  }

  // ========== groupBatchesByIncrementalKey Tests ==========

  @Test void testGroupBatchesByIncrementalKeyNull() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    m.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> batch1 = new HashMap<String, String>();
    batch1.put("year", "2020");
    batches.add(batch1);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>)
            m.invoke(materializer, batches, null);

    assertEquals(1, result.size());
  }

  @Test void testGroupBatchesByIncrementalKeyWithKeys() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    m.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> batch1 = new HashMap<String, String>();
    batch1.put("year", "2020");
    batch1.put("type", "10-K");
    batches.add(batch1);

    Map<String, String> batch2 = new HashMap<String, String>();
    batch2.put("year", "2020");
    batch2.put("type", "10-Q");
    batches.add(batch2);

    Map<String, String> batch3 = new HashMap<String, String>();
    batch3.put("year", "2021");
    batch3.put("type", "10-K");
    batches.add(batch3);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>)
            m.invoke(materializer, batches, Arrays.asList("year"));

    assertEquals(2, result.size()); // two distinct year values
  }

  // ========== needsFilenameEmbedding Tests ==========

  @Test void testNeedsFilenameEmbeddingEmptyBatch() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    boolean result = (boolean) m.invoke(materializer, config,
        Collections.emptyMap());
    assertFalse(result);
  }

  @Test void testNeedsFilenameEmbeddingBatchKeyInPartition() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    m.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .partitionColumns(partCols)
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2020");

    boolean result = (boolean) m.invoke(materializer, config, batch);
    assertFalse(result);
  }

  @Test void testNeedsFilenameEmbeddingBatchKeyNotInPartition() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding",
        IcebergMaterializer.MaterializationConfig.class, Map.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("type", "10-K");

    boolean result = (boolean) m.invoke(materializer, config, batch);
    assertTrue(result);
  }

  // ========== buildFilenamePattern Tests ==========

  @Test void testBuildFilenamePatternSingleKey() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    m.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("type", "10-K");

    String result = (String) m.invoke(materializer, batch);
    assertTrue(result.contains("type_10-K"));
    assertTrue(result.endsWith("_{i}"));
  }

  @Test void testBuildFilenamePatternMultiKey() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    m.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2020");
    batch.put("type", "10-K");

    String result = (String) m.invoke(materializer, batch);
    assertTrue(result.contains("year_2020"));
    assertTrue(result.contains("type_10-K"));
    assertTrue(result.contains("_"));
    assertTrue(result.endsWith("_{i}"));
  }

  // ========== buildSelectSql Tests ==========

  @Test void testBuildSelectSqlParquetNoFilter() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/year=2020/*.parquet",
        Collections.emptyMap(), null);

    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("read_parquet("));
    assertTrue(sql.contains("hive_partitioning=true"));
  }

  @Test void testBuildSelectSqlJsonFormat() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.json",
        Collections.emptyMap(), null);

    assertTrue(sql.contains("read_json("));
  }

  @Test void testBuildSelectSqlWithComputedColumns() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("hash_col", "md5(text)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .computedColumns(computed)
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.parquet",
        Collections.emptyMap(), null);

    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("md5(text) AS hash_col"));
  }

  @Test void testBuildSelectSqlWithRowFilter() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .rowFilter("cik IN ('001')")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.parquet",
        Collections.emptyMap(), null);

    assertTrue(sql.contains("WHERE cik IN ('001')"));
  }

  @Test void testBuildSelectSqlWithSmallExclusionList() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    Set<String> excludeAccessions = new HashSet<String>();
    excludeAccessions.add("acc1");
    excludeAccessions.add("acc2");

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.parquet",
        Collections.emptyMap(), excludeAccessions);

    assertTrue(sql.contains("NOT IN"));
    assertTrue(sql.contains("'acc1'") || sql.contains("'acc2'"));
  }

  @Test void testBuildSelectSqlWithLargeExclusionList() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    // Create > 100 exclusions to trigger anti-join path
    Set<String> excludeAccessions = new HashSet<String>();
    for (int i = 0; i < 150; i++) {
      excludeAccessions.add("acc_" + i);
    }

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.parquet",
        Collections.emptyMap(), excludeAccessions);

    assertTrue(sql.contains("NOT EXISTS"));
    assertTrue(sql.contains("_exclusions"));
  }

  @Test void testBuildSelectSqlWithRowFilterAndExclusions() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .rowFilter("cik = '001'")
            .build();

    Set<String> excludeAccessions = new HashSet<String>();
    excludeAccessions.add("acc1");

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/data.parquet",
        Collections.emptyMap(), excludeAccessions);

    assertTrue(sql.contains("WHERE cik = '001'"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("NOT IN"));
  }

  // ========== buildSelectSqlWithPaging Tests ==========

  @Test void testBuildSelectSqlWithPagingBasic() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.parquet",
        Collections.emptyMap(), 100, 0, null);

    assertTrue(sql.contains("LIMIT 100"));
    assertFalse(sql.contains("OFFSET"));
  }

  @Test void testBuildSelectSqlWithPagingAndOffset() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.parquet",
        Collections.emptyMap(), 100, 500, null);

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 500"));
  }

  @Test void testBuildSelectSqlWithPagingJsonFormat() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.json",
        Collections.emptyMap(), 50, 0, null);

    assertTrue(sql.contains("read_json("));
    assertTrue(sql.contains("LIMIT 50"));
  }

  // ========== buildCountSql Tests ==========

  @Test void testBuildCountSqlParquet() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config, "s3://bucket/*.parquet");
    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertTrue(sql.contains("read_parquet("));
  }

  @Test void testBuildCountSqlJson() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config, "s3://bucket/*.json");
    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertTrue(sql.contains("read_json("));
  }

  @Test void testBuildCountSqlWithRowFilter() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql",
        IcebergMaterializer.MaterializationConfig.class, String.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .rowFilter("status = 'active'")
            .build();

    String sql = (String) m.invoke(materializer, config, "s3://bucket/*.parquet");
    assertTrue(sql.contains("WHERE status = 'active'"));
  }

  // ========== buildDuckDBSql Tests ==========

  @Test void testBuildDuckDBSqlBasic() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.parquet", "/tmp/staging/out.parquet",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("COPY ("));
    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("read_parquet("));
    assertTrue(sql.contains("TO '/tmp/staging/out.parquet'"));
    assertTrue(sql.contains("FORMAT PARQUET"));
  }

  @Test void testBuildDuckDBSqlWithLimitOffset() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.parquet", "/tmp/out.parquet",
        Collections.emptyMap(), 100, 50);

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 50"));
  }

  @Test void testBuildDuckDBSqlWithPartitionColumns() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    m.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .partitionColumns(partCols)
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.parquet", "/tmp/staging/",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("PARTITION_BY"));
    assertTrue(sql.contains("year"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test void testBuildDuckDBSqlJsonFormat() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql",
        IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.json")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("tbl")
            .build();

    String sql = (String) m.invoke(materializer, config,
        "s3://bucket/*.json", "/tmp/out.parquet",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("read_json("));
  }

  // ========== Constructor Tests ==========

  @Test void testConstructorWithNullIncrementalTracker() throws Exception {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorageProvider, null);
    Field trackerField = IcebergMaterializer.class.getDeclaredField("incrementalTracker");
    trackerField.setAccessible(true);
    assertNotNull(trackerField.get(mat));
  }

  @Test void testConstructorWithCustomRetrySettings() throws Exception {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), mockStorageProvider, IncrementalTracker.NOOP, 5, 2000);
    Field maxRetriesField = IcebergMaterializer.class.getDeclaredField("maxRetries");
    maxRetriesField.setAccessible(true);
    assertEquals(5, maxRetriesField.getInt(mat));

    Field retryDelayField = IcebergMaterializer.class.getDeclaredField("retryDelayMs");
    retryDelayField.setAccessible(true);
    assertEquals(2000L, retryDelayField.getLong(mat));
  }

  @Test void testConstructorSetsUpCatalogConfig() throws Exception {
    Field configField = IcebergMaterializer.class.getDeclaredField("catalogConfig");
    configField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) configField.get(materializer);

    assertEquals("hadoop", config.get("catalog"));
    assertNotNull(config.get("warehousePath"));
    assertNotNull(config.get("hadoopConfig"));
  }

  @Test void testConstructorWithS3Credentials() throws Exception {
    Field configField = IcebergMaterializer.class.getDeclaredField("catalogConfig");
    configField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) configField.get(materializer);

    @SuppressWarnings("unchecked")
    Map<String, String> hadoopConfig = (Map<String, String>) config.get("hadoopConfig");
    assertNotNull(hadoopConfig);
    assertEquals("testKey", hadoopConfig.get("fs.s3a.access.key"));
    assertEquals("testSecret", hadoopConfig.get("fs.s3a.secret.key"));
    assertEquals("http://localhost:9000", hadoopConfig.get("fs.s3a.endpoint"));
    assertEquals("true", hadoopConfig.get("fs.s3a.path.style.access"));
  }

  @Test void testConstructorNullStorageProvider() throws Exception {
    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), null, IncrementalTracker.NOOP);
    Field configField = IcebergMaterializer.class.getDeclaredField("catalogConfig");
    configField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> config = (Map<String, Object>) configField.get(mat);

    // No hadoopConfig key should be present for null storage provider
    assertFalse(config.containsKey("hadoopConfig"));
  }

  @Test void testConstructorEmptyS3Config() {
    StorageProvider emptyS3Provider = new StorageProvider() {
      @Override public java.io.InputStream openInputStream(String path) { return null; }
      @Override public void writeFile(String path, java.io.InputStream data) { }
      @Override public void writeFile(String path, byte[] data) { }
      @Override public boolean delete(String path) { return false; }
      @Override public int deleteBatch(List<String> paths) { return 0; }
      @Override public String getStorageType() { return "test"; }
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }
      @Override public void createDirectories(String path) { }
      @Override public String resolvePath(String base, String relative) { return base; }
      @Override public FileMetadata getMetadata(String path) { return null; }
      @Override public Map<String, String> getS3Config() { return Collections.emptyMap(); }
      @Override public void ensureLifecycleRule(String prefix, int days) { }
      @Override public boolean isDirectory(String path) { return false; }
      @Override public boolean exists(String path) { return false; }
      @Override public java.io.Reader openReader(String path) {
        return new java.io.StringReader("");
      }
    };

    IcebergMaterializer mat = new IcebergMaterializer(
        tempDir.toString(), emptyS3Provider, IncrementalTracker.NOOP);
    assertNotNull(mat);
  }

  // ========== cleanupStagingDirectory Tests ==========

  @Test void testCleanupStagingDirectoryS3Path() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    // S3 paths should skip cleanup (no exception)
    m.invoke(materializer, "s3://bucket/.staging/test");
  }

  @Test void testCleanupStagingDirectoryS3aPath() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    // S3a paths should skip cleanup (no exception)
    m.invoke(materializer, "s3a://bucket/.staging/test");
  }

  // ========== isSourceWatermarkEnabled Tests ==========

  @Test void testIsSourceWatermarkEnabled() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "isSourceWatermarkEnabled",
        IcebergMaterializer.MaterializationConfig.class);
    m.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data.parquet")
            .targetTableId("tbl")
            .build();

    assertTrue((boolean) m.invoke(materializer, config));
  }

  // ========== TableSetupResult Inner Class ==========

  @Test void testTableSetupResultClass() throws Exception {
    Class<?> tsrClass = null;
    for (Class<?> inner : IcebergMaterializer.class.getDeclaredClasses()) {
      if (inner.getSimpleName().equals("TableSetupResult")) {
        tsrClass = inner;
        break;
      }
    }
    assertNotNull(tsrClass, "TableSetupResult inner class should exist");
  }

  // ========== selfHealTracker edge cases ==========

  @Test void testCountAllAccessions() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    m.setAccessible(true);

    Map<String, Set<String>> cikToAccessions = new HashMap<String, Set<String>>();
    Set<String> acc1 = new HashSet<String>();
    acc1.add("a");
    acc1.add("b");
    Set<String> acc2 = new HashSet<String>();
    acc2.add("c");
    cikToAccessions.put("cik1", acc1);
    cikToAccessions.put("cik2", acc2);

    int count = (int) m.invoke(materializer, cikToAccessions);
    assertEquals(3, count);
  }

  @Test void testCountAllAccessionsEmpty() throws Exception {
    Method m = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    m.setAccessible(true);

    Map<String, Set<String>> empty = new HashMap<String, Set<String>>();
    int count = (int) m.invoke(materializer, empty);
    assertEquals(0, count);
  }

  // ========== getSourceFileWatermark for S3 ==========

  @Test void testGetSourceFileWatermarkS3() {
    long watermark = materializer.getSourceFileWatermark(
        "s3://bucket/data/*.parquet",
        IcebergMaterializer.SourceFormat.PARQUET);
    assertEquals(0, watermark); // S3 returns 0
  }

  @Test void testGetSourceFileWatermarkS3a() {
    long watermark = materializer.getSourceFileWatermark(
        "s3a://bucket/data/*.parquet",
        IcebergMaterializer.SourceFormat.PARQUET);
    assertEquals(0, watermark);
  }
}
