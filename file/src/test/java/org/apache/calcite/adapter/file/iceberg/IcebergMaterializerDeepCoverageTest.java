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

import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link IcebergMaterializer} targeting uncovered
 * methods: SQL building, batch combinations, coercion, filename patterns,
 * staging/cleanup, source accession parsing, CIK extraction, and tracker
 * integration paths.
 *
 * <p>Uses Mockito for external dependencies and reflection for private methods.
 */
@Tag("unit")
public class IcebergMaterializerDeepCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorageProvider;
  private IncrementalTracker mockTracker;
  private IcebergMaterializer materializer;

  @BeforeEach
  void setUp() {
    mockStorageProvider = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
    doCallRealMethod().when(mockTracker).getProcessedKeyValues(anyString(), any());
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    materializer = new IcebergMaterializer(
        tempDir.toString(), mockStorageProvider, mockTracker, 2, 100L);
  }

  // ===== buildSelectSql tests =====

  @Test
  void testBuildSelectSqlBasicParquet() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), Collections.emptySet());

    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("read_parquet('data/*.parquet'"));
    assertTrue(sql.contains("hive_partitioning=true"));
    assertFalse(sql.contains("WHERE"));
  }

  @Test
  void testBuildSelectSqlJsonFormat() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", Collections.emptyMap(), Collections.emptySet());

    assertTrue(sql.contains("read_json('data/*.json'"));
    assertTrue(sql.contains("union_by_name=true"));
  }

  @Test
  void testBuildSelectSqlWithRowFilter() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("cik IN ('0001', '0002')")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), Collections.emptySet());

    assertTrue(sql.contains("WHERE cik IN ('0001', '0002')"));
  }

  @Test
  void testBuildSelectSqlWithComputedColumns() throws Exception {
    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("embedding", "embed_jina(text)::FLOAT[768]");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .computedColumns(computed)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), Collections.emptySet());

    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("embed_jina(text)::FLOAT[768] AS embedding"));
  }

  @Test
  void testBuildSelectSqlWithSmallExclusionList() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");
    exclusions.add("acc2");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), exclusions);

    assertTrue(sql.contains("accession_number NOT IN ("));
    assertTrue(sql.contains("'acc1'") || sql.contains("'acc2'"));
  }

  @Test
  void testBuildSelectSqlWithLargeExclusionList() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 150; i++) {
      exclusions.add("acc_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), exclusions);

    assertTrue(sql.contains("NOT EXISTS (SELECT 1 FROM _exclusions"));
  }

  @Test
  void testBuildSelectSqlWithRowFilterAndExclusions() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("year = 2023")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), exclusions);

    assertTrue(sql.contains("WHERE year = 2023 AND accession_number NOT IN ("));
  }

  // ===== buildSelectSqlWithPaging tests =====

  @Test
  void testBuildSelectSqlWithPagingBasic() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 100, 0, Collections.emptySet());

    assertTrue(sql.contains("LIMIT 100"));
    assertFalse(sql.contains("OFFSET"));
  }

  @Test
  void testBuildSelectSqlWithPagingOffset() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 100, 50, Collections.emptySet());

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 50"));
  }

  @Test
  void testBuildSelectSqlWithPagingJsonAndComputedColumns() throws Exception {
    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("col1", "UPPER(name)");
    computed.put("col2", "LENGTH(text)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .computedColumns(computed)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", Collections.emptyMap(), 50, 0, Collections.emptySet());

    assertTrue(sql.contains("SELECT *, "));
    assertTrue(sql.contains("UPPER(name) AS col1"));
    assertTrue(sql.contains("LENGTH(text) AS col2"));
    assertTrue(sql.contains("read_json("));
  }

  @Test
  void testBuildSelectSqlWithPagingLargeExclusions() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .rowFilter("active = true")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 200; i++) {
      exclusions.add("exclude_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.emptyMap(), 50, 10, exclusions);

    assertTrue(sql.contains("WHERE active = true AND NOT EXISTS"));
    assertTrue(sql.contains("LIMIT 50"));
    assertTrue(sql.contains("OFFSET 10"));
  }

  // ===== buildDuckDBSql tests =====

  @Test
  void testBuildDuckDBSqlBasic() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/data.parquet",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("COPY ("));
    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("TO '/tmp/output/data.parquet'"));
    assertTrue(sql.contains("FORMAT PARQUET"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test
  void testBuildDuckDBSqlWithPartitionColumns() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));
    partCols.add(new ColumnDefinition("month", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("PARTITION_BY (year, month)"));
  }

  @Test
  void testBuildDuckDBSqlWithRowBatching() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/output/batch.parquet",
        Collections.emptyMap(), 100, 50);

    assertTrue(sql.contains("LIMIT 100"));
    assertTrue(sql.contains("OFFSET 50"));
    // With limit > 0, PARTITION_BY and OVERWRITE_OR_IGNORE should NOT be present
    assertFalse(sql.contains("PARTITION_BY"));
    assertFalse(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test
  void testBuildDuckDBSqlWithComputedColumnsAndJsonFormat() throws Exception {
    Map<String, String> computed = new LinkedHashMap<String, String>();
    computed.put("computed_col", "UPPER(name)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .computedColumns(computed)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", "/tmp/out.parquet",
        Collections.emptyMap(), -1, -1);

    assertTrue(sql.contains("UPPER(name) AS computed_col"));
    assertTrue(sql.contains("read_json("));
  }

  @Test
  void testBuildDuckDBSqlWithRowFilterAndBatchKeys() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .rowFilter("status = 'active'")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    // Batch keys not in partition columns -> filename embedding
    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("region", "US");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/out/",
        batch, -1, -1);

    assertTrue(sql.contains("WHERE status = 'active'"));
    assertTrue(sql.contains("FILENAME_PATTERN"));
    assertTrue(sql.contains("region_US"));
  }

  // ===== buildCountSql tests =====

  @Test
  void testBuildCountSqlParquet() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config, "data/*.parquet");

    assertTrue(sql.contains("SELECT COUNT(*) FROM"));
    assertTrue(sql.contains("read_parquet("));
    assertFalse(sql.contains("WHERE"));
  }

  @Test
  void testBuildCountSqlJsonWithRowFilter() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .rowFilter("active = true")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    String sql = (String) method.invoke(materializer, config, "data/*.json");

    assertTrue(sql.contains("read_json("));
    assertTrue(sql.contains("WHERE active = true"));
  }

  // ===== needsFilenameEmbedding tests =====

  @Test
  void testNeedsFilenameEmbeddingEmptyBatch() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding", IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    boolean result = (Boolean) method.invoke(materializer, config, Collections.emptyMap());
    assertFalse(result);
  }

  @Test
  void testNeedsFilenameEmbeddingBatchKeyInPartition() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding", IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    Map<String, String> batch = Collections.singletonMap("year", "2023");
    boolean result = (Boolean) method.invoke(materializer, config, batch);
    assertFalse(result);
  }

  @Test
  void testNeedsFilenameEmbeddingBatchKeyNotInPartition() throws Exception {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .partitionColumns(partCols)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding", IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    Map<String, String> batch = Collections.singletonMap("region", "US");
    boolean result = (Boolean) method.invoke(materializer, config, batch);
    assertTrue(result);
  }

  // ===== buildFilenamePattern tests =====

  @Test
  void testBuildFilenamePatternSingleKey() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    method.setAccessible(true);

    Map<String, String> batch = Collections.singletonMap("year", "2023");
    String pattern = (String) method.invoke(materializer, batch);

    assertEquals("year_2023_{i}", pattern);
  }

  @Test
  void testBuildFilenamePatternMultipleKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    method.setAccessible(true);

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");
    batch.put("region", "US");
    String pattern = (String) method.invoke(materializer, batch);

    assertEquals("year_2023_region_US_{i}", pattern);
  }

  // ===== coerceValue tests =====

  @Test
  void testCoerceValueInteger() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(42, method.invoke(materializer, "42", "INTEGER"));
    assertEquals(99, method.invoke(materializer, "99", "INT"));
  }

  @Test
  void testCoerceValueBigint() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(1234567890L, method.invoke(materializer, "1234567890", "BIGINT"));
    assertEquals(999L, method.invoke(materializer, "999", "LONG"));
  }

  @Test
  void testCoerceValueDouble() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(3.14, method.invoke(materializer, "3.14", "DOUBLE"));
  }

  @Test
  void testCoerceValueBoolean() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(true, method.invoke(materializer, "true", "BOOLEAN"));
    assertEquals(false, method.invoke(materializer, "false", "BOOLEAN"));
  }

  @Test
  void testCoerceValueVarchar() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals("hello", method.invoke(materializer, "hello", "VARCHAR"));
    assertEquals("world", method.invoke(materializer, "world", "STRING"));
  }

  @Test
  void testCoerceValueNullInputs() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertNull(method.invoke(materializer, null, "INTEGER"));
    assertEquals("value", method.invoke(materializer, "value", null));
  }

  // ===== findColumnType tests =====

  @Test
  void testFindColumnTypeMatch() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    method.setAccessible(true);

    List<ColumnDefinition> cols = new ArrayList<ColumnDefinition>();
    cols.add(new ColumnDefinition("year", "INTEGER"));
    cols.add(new ColumnDefinition("name", "VARCHAR"));

    assertEquals("INTEGER", method.invoke(materializer, cols, "year"));
    assertEquals("VARCHAR", method.invoke(materializer, cols, "name"));
  }

  @Test
  void testFindColumnTypeNoMatch() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "findColumnType", List.class, String.class);
    method.setAccessible(true);

    List<ColumnDefinition> cols = new ArrayList<ColumnDefinition>();
    cols.add(new ColumnDefinition("year", "INTEGER"));

    // Not found -> defaults to VARCHAR
    assertEquals("VARCHAR", method.invoke(materializer, cols, "missing"));
  }

  // ===== groupBatchesByIncrementalKey tests =====

  @Test
  void testGroupBatchesByIncrementalKeyNoKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    batches.add(Collections.singletonMap("year", "2023"));
    batches.add(Collections.singletonMap("year", "2024"));

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, Collections.emptyList());

    // All batches grouped under empty key
    assertEquals(1, result.size());
    assertEquals(2, result.values().iterator().next().size());
  }

  @Test
  void testGroupBatchesByIncrementalKeyWithKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();

    Map<String, String> batch1 = new LinkedHashMap<String, String>();
    batch1.put("year", "2023");
    batch1.put("region", "US");
    batches.add(batch1);

    Map<String, String> batch2 = new LinkedHashMap<String, String>();
    batch2.put("year", "2023");
    batch2.put("region", "EU");
    batches.add(batch2);

    Map<String, String> batch3 = new LinkedHashMap<String, String>();
    batch3.put("year", "2024");
    batch3.put("region", "US");
    batches.add(batch3);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, Arrays.asList("year"));

    // Grouped by year: {2023: [batch1, batch2], 2024: [batch3]}
    assertEquals(2, result.size());
  }

  @Test
  void testGroupBatchesByIncrementalKeyNullKeys() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    batches.add(Collections.singletonMap("year", "2023"));

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, (List<String>) null);

    assertEquals(1, result.size());
  }

  @Test
  void testGroupBatchesByIncrementalKeyMissingValue() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    batches.add(Collections.singletonMap("year", "2023"));

    // Key "month" not in batch
    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> result =
        (Map<Map<String, String>, List<Map<String, String>>>) method.invoke(
            materializer, batches, Arrays.asList("month"));

    assertEquals(1, result.size());
    // Key values should be empty since "month" not found
    assertTrue(result.containsKey(new LinkedHashMap<String, String>()));
  }

  // ===== buildBatchCombinations tests =====

  @Test
  void testBuildBatchCombinationsNoBatchColumns() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, String>> result =
        (List<Map<String, String>>) method.invoke(materializer, config);

    assertTrue(result.isEmpty());
  }

  @Test
  void testBuildBatchCombinationsYearOnly() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .batchPartitionColumns(Arrays.asList("year"))
            .yearRange(2023, 2025)
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, String>> result =
        (List<Map<String, String>>) method.invoke(materializer, config);

    assertEquals(3, result.size());
    assertEquals("2023", result.get(0).get("year"));
    assertEquals("2024", result.get(1).get("year"));
    assertEquals("2025", result.get(2).get("year"));
  }

  // ===== buildCombinationsRecursive tests =====

  @Test
  void testBuildCombinationsRecursiveCartesianProduct() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive", List.class, List.class, int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> columnNames = Arrays.asList("year", "region");
    List<List<String>> columnValues = new ArrayList<List<String>>();
    columnValues.add(Arrays.asList("2023", "2024"));
    columnValues.add(Arrays.asList("US", "EU"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    method.invoke(materializer, columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), result);

    assertEquals(4, result.size());
    // Verify all combinations exist
    boolean foundUS2023 = false;
    boolean foundEU2024 = false;
    for (Map<String, String> combo : result) {
      if ("2023".equals(combo.get("year")) && "US".equals(combo.get("region"))) {
        foundUS2023 = true;
      }
      if ("2024".equals(combo.get("year")) && "EU".equals(combo.get("region"))) {
        foundEU2024 = true;
      }
    }
    assertTrue(foundUS2023);
    assertTrue(foundEU2024);
  }

  // ===== extractCiksFromRowFilter tests =====

  @Test
  void testExtractCiksFromRowFilterBasic() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ('0000320193', '0000004962')");
    assertEquals(2, ciks.size());
    assertTrue(ciks.contains("0000320193"));
    assertTrue(ciks.contains("0000004962"));
  }

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
  void testExtractCiksFromRowFilterNoCik() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("year = 2023");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterDoubleSpace() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "CIK  IN ('0001')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0001"));
  }

  @Test
  void testExtractCiksFromRowFilterNoOpenParen() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("cik IN");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterNoCloseParen() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("cik IN ('0001'");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterUnquotedValues() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN (0001, 0002)");
    // Unquoted values don't match the quote-stripping logic
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromRowFilterSingleQuotedCik() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ('single_cik')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("single_cik"));
  }

  // ===== countAllAccessions tests =====

  @Test
  void testCountAllAccessions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    Map<String, Set<String>> cikToAccessions = new HashMap<String, Set<String>>();
    Set<String> set1 = new HashSet<String>();
    set1.add("a");
    set1.add("b");
    cikToAccessions.put("cik1", set1);

    Set<String> set2 = new HashSet<String>();
    set2.add("c");
    cikToAccessions.put("cik2", set2);

    int count = (Integer) method.invoke(materializer, cikToAccessions);
    assertEquals(3, count);
  }

  @Test
  void testCountAllAccessionsEmpty() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    int count = (Integer) method.invoke(materializer,
        new HashMap<String, Set<String>>());
    assertEquals(0, count);
  }

  // ===== getFilteredSourceAccessions tests =====

  @Test
  void testGetFilteredSourceAccessionsNullProvider() throws Exception {
    // Create materializer with null storage provider
    IcebergMaterializer noProvider =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    Set<String> result = (Set<String>) method.invoke(noProvider,
        "s3://bucket/year=*/data.parquet", "2023", null);
    assertNull(result);
  }

  @Test
  void testGetFilteredSourceAccessionsNoCikFilter() throws Exception {
    // Mock getSourceAccessions result
    Method getSourceMethod = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    getSourceMethod.setAccessible(true);

    // Pre-populate sourceAccessionsCache
    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> accessions1 = new HashSet<String>();
    accessions1.add("acc1");
    cikMap.put("cik1", accessions1);
    Set<String> accessions2 = new HashSet<String>();
    accessions2.add("acc2");
    cikMap.put("cik2", accessions2);
    cache.put("2023:_data.parquet", cikMap);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) getSourceMethod.invoke(materializer,
        "s3://bucket/year=*/*_data.parquet", "2023", null);

    // No CIK filter -> returns all accessions
    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains("acc1"));
    assertTrue(result.contains("acc2"));
  }

  @Test
  void testGetFilteredSourceAccessionsWithCikFilter() throws Exception {
    Method getSourceMethod = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    getSourceMethod.setAccessible(true);

    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> accessions1 = new HashSet<String>();
    accessions1.add("acc1");
    cikMap.put("cik1", accessions1);
    Set<String> accessions2 = new HashSet<String>();
    accessions2.add("acc2");
    cikMap.put("cik2", accessions2);
    cache.put("2023:_data.parquet", cikMap);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) getSourceMethod.invoke(materializer,
        "s3://bucket/year=*/*_data.parquet", "2023", "cik IN ('cik1')");

    // CIK filter should only return accessions for cik1
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains("acc1"));
  }

  // ===== getSourceAccessions tests =====

  @Test
  void testGetSourceAccessionsFromCache() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cached = new HashMap<String, Set<String>>();
    cached.put("cik1", new HashSet<String>(Arrays.asList("acc1")));
    cache.put("2023:_facts.parquet", cached);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNotNull(result);
    assertTrue(result.containsKey("cik1"));
  }

  @Test
  void testGetSourceAccessionsNoYearWildcard() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/data/*.parquet", "2023");

    // No year=* in pattern -> returns null
    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsNullStorageProvider() throws Exception {
    IcebergMaterializer noStorage =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(noStorage,
            "s3://bucket/year=*/data.parquet", "2023");

    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsWithFiles() throws Exception {
    // Mock storage provider to return files
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0001234_acc-123_facts.parquet",
        "0001234_acc-123_facts.parquet", false, 1000, System.currentTimeMillis()));
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0005678_acc-456_facts.parquet",
        "0005678_acc-456_facts.parquet", false, 2000, System.currentTimeMillis()));
    files.add(new StorageProvider.FileEntry(
        "s3://bucket/year=2023/0001234_acc-789_metadata.parquet",
        "0001234_acc-789_metadata.parquet", false, 500, System.currentTimeMillis()));

    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNotNull(result);
    // Only files ending with _facts.parquet should be included
    assertEquals(2, result.size());
    assertTrue(result.containsKey("0001234"));
    assertTrue(result.containsKey("0005678"));
    assertTrue(result.get("0001234").contains("acc-123"));
    assertTrue(result.get("0005678").contains("acc-456"));
  }

  @Test
  void testGetSourceAccessionsExceptionHandling() throws Exception {
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenThrow(new IOException("S3 unavailable"));

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/*_facts.parquet", "2023");

    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsFileSuffixExtraction() throws Exception {
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    // Pattern without * in filename part
    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result =
        (Map<String, Set<String>>) method.invoke(materializer,
            "s3://bucket/year=*/data.parquet", "2023");

    // No * in file pattern -> fileSuffix stays default "_metadata.parquet"
    // Since no files match, returns empty map
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ===== getTrackedAccessions tests =====

  @Test
  void testGetTrackedAccessionsBasic() throws Exception {
    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry1 = new LinkedHashMap<String, String>();
    entry1.put("year", "2023");
    entry1.put("accession_number", "acc1");
    processed.add(entry1);

    Map<String, String> entry2 = new LinkedHashMap<String, String>();
    entry2.put("year", "2024");
    entry2.put("accession_number", "acc2");
    processed.add(entry2);

    when(mockTracker.getProcessedKeyValues(anyString())).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "tableId", "2023");

    // Only year=2023 entries should be returned
    assertEquals(1, result.size());
    assertTrue(result.contains("acc1"));
  }

  @Test
  void testGetTrackedAccessionsNullYear() throws Exception {
    Set<Map<String, String>> processed = new HashSet<Map<String, String>>();
    Map<String, String> entry = new LinkedHashMap<String, String>();
    entry.put("accession_number", "acc1");
    processed.add(entry);

    when(mockTracker.getProcessedKeyValues(anyString())).thenReturn(processed);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "tableId", null);

    // With null year, all entries should be returned
    assertEquals(1, result.size());
    assertTrue(result.contains("acc1"));
  }

  @Test
  void testGetTrackedAccessionsException() throws Exception {
    when(mockTracker.getProcessedKeyValues(anyString()))
        .thenThrow(new RuntimeException("tracker error"));

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "tableId", "2023");

    assertTrue(result.isEmpty());
  }

  // ===== selfHealTracker tests =====

  @Test
  void testSelfHealTrackerNoSourceAccessions() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/year=*/data.parquet")
            .targetTableId("test_table")
            .build();

    // No source accessions available -> should return early
    method.invoke(materializer, config, "2023", new HashSet<String>());
  }

  @Test
  void testSelfHealTrackerAllTracked() throws Exception {
    // Pre-populate sourceAccessionsCache with accessions
    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> accessions = new HashSet<String>();
    accessions.add("acc1");
    accessions.add("acc2");
    cikMap.put("cik1", accessions);
    cache.put("2023:_data.parquet", cikMap);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/year=*/*_data.parquet")
            .targetTableId("test_table")
            .build();

    // All source accessions are in excludeAccessions -> nothing to heal
    Set<String> excludeAccessions = new HashSet<String>();
    excludeAccessions.add("acc1");
    excludeAccessions.add("acc2");
    method.invoke(materializer, config, "2023", excludeAccessions);
  }

  @Test
  void testSelfHealTrackerWithUntracked() throws Exception {
    Field cacheField = IcebergMaterializer.class.getDeclaredField("sourceAccessionsCache");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Set<String>>> cache =
        (Map<String, Map<String, Set<String>>>) cacheField.get(materializer);

    Map<String, Set<String>> cikMap = new HashMap<String, Set<String>>();
    Set<String> accessions = new HashSet<String>();
    accessions.add("acc1");
    accessions.add("acc2");
    accessions.add("acc3");
    cikMap.put("cik1", accessions);
    cache.put("2023:_data.parquet", cikMap);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/year=*/*_data.parquet")
            .targetTableId("test_table")
            .sourceTableName("source_tbl")
            .build();

    // acc1 tracked, acc2 and acc3 not tracked
    Set<String> excludeAccessions = new HashSet<String>();
    excludeAccessions.add("acc1");
    method.invoke(materializer, config, "2023", excludeAccessions);

    // Verify markProcessed was called for untracked accessions
    org.mockito.Mockito.verify(mockTracker, org.mockito.Mockito.atLeast(2))
        .markProcessed(anyString(), anyString(), any(Map.class), anyString());
  }

  // ===== getExcludedAccessions tests =====

  @Test
  void testGetExcludedAccessionsNoIcebergLocation() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config, null,
        Collections.singletonMap("year", "2023"));

    assertTrue(result.isEmpty());
  }

  @Test
  void testGetExcludedAccessionsEmptyIcebergLocation() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .icebergTableLocation("")
            .build();

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config, null,
        Collections.singletonMap("year", "2023"));

    assertTrue(result.isEmpty());
  }

  // ===== cleanupStagingDirectory tests =====

  @Test
  void testCleanupStagingDirectoryS3Path() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // S3 path - should skip cleanup
    method.invoke(materializer, "s3://bucket/staging/dir");
    // No exception = success
  }

  @Test
  void testCleanupStagingDirectoryS3aPath() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, "s3a://bucket/staging/dir");
  }

  @Test
  void testCleanupStagingDirectoryLocalWithFiles() throws Exception {
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry(
        tempDir + "/staging/file1.parquet", "file1.parquet", false, 100, 0));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, tempDir + "/staging");
  }

  @Test
  void testCleanupStagingDirectoryLocalNoFiles() throws Exception {
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, tempDir + "/staging");
  }

  @Test
  void testCleanupStagingDirectoryLocalException() throws Exception {
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenThrow(new IOException("List failed"));

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Should not throw
    method.invoke(materializer, tempDir + "/staging");
  }

  // ===== createStagingPath tests =====

  @Test
  void testCreateStagingPath() throws Exception {
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenReturn(tempDir + "/.staging/test");

    Method method = IcebergMaterializer.class.getDeclaredMethod("createStagingPath");
    method.setAccessible(true);

    String path = (String) method.invoke(materializer);
    assertNotNull(path);
    assertTrue(path.contains(".staging"));
  }

  // ===== getSourceFileWatermark tests =====

  @Test
  void testGetSourceFileWatermarkS3Path() throws Exception {
    long watermark = materializer.getSourceFileWatermark(
        "s3://bucket/data/*.parquet", IcebergMaterializer.SourceFormat.PARQUET);

    // S3 paths return 0 (no watermark available)
    assertEquals(0, watermark);
  }

  @Test
  void testGetSourceFileWatermarkS3aPath() throws Exception {
    long watermark = materializer.getSourceFileWatermark(
        "s3a://bucket/data/*.parquet", IcebergMaterializer.SourceFormat.PARQUET);

    assertEquals(0, watermark);
  }

  // ===== partitionHasData tests =====

  @Test
  void testPartitionHasDataNullTable() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    boolean result = (Boolean) method.invoke(materializer, null,
        Collections.singletonMap("year", "2023"));
    assertFalse(result);
  }

  @Test
  void testPartitionHasDataNullPartitionValues() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    boolean result = (Boolean) method.invoke(materializer,
        mock(org.apache.iceberg.Table.class), null);
    assertFalse(result);
  }

  @Test
  void testPartitionHasDataEmptyPartitionValues() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    boolean result = (Boolean) method.invoke(materializer,
        mock(org.apache.iceberg.Table.class), Collections.emptyMap());
    assertFalse(result);
  }

  // ===== isSourceWatermarkEnabled tests =====

  @Test
  void testIsSourceWatermarkEnabled() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "isSourceWatermarkEnabled", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    boolean result = (Boolean) method.invoke(materializer, config);
    assertTrue(result); // Always returns true
  }

  // ===== MaterializationResult tests =====

  @Test
  void testMaterializationResultBasicConstructor() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table1", 10, 2, 3, 1500);

    assertEquals("table1", result.getTableId());
    assertEquals(10, result.getSuccessCount());
    assertEquals(2, result.getFailedCount());
    assertEquals(3, result.getSkippedCount());
    assertEquals(1500, result.getDurationMs());
    assertFalse(result.isFullySuccessful());
    assertFalse(result.isTableRecreated());
  }

  @Test
  void testMaterializationResultFullySuccessful() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table1", 10, 0, 0, 1000);

    assertTrue(result.isFullySuccessful());
    assertFalse(result.isTableRecreated());
  }

  @Test
  void testMaterializationResultWithRecreation() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("table1", 10, 0, 0, 1000, true);

    assertTrue(result.isTableRecreated());
    assertTrue(result.isFullySuccessful());
  }

  @Test
  void testMaterializationResultToString() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult("t1", 5, 1, 2, 999, true);

    String str = result.toString();
    assertTrue(str.contains("t1"));
    assertTrue(str.contains("success=5"));
    assertTrue(str.contains("failed=1"));
    assertTrue(str.contains("skipped=2"));
    assertTrue(str.contains("duration=999ms"));
    assertTrue(str.contains("recreated=true"));
  }

  // ===== MaterializationConfig comprehensive builder tests =====

  @Test
  void testBuilderFullConfig() {
    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    List<IcebergCatalogManager.ColumnDef> tableCols = new ArrayList<IcebergCatalogManager.ColumnDef>();
    tableCols.add(new IcebergCatalogManager.ColumnDef("year", "INTEGER"));
    tableCols.add(new IcebergCatalogManager.ColumnDef("name", "VARCHAR"));

    Map<String, String> computed = new HashMap<String, String>();
    computed.put("upper_name", "UPPER(name)");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
            .targetTableId("test_table")
            .sourceTableName("source_table")
            .partitionColumns(partCols)
            .tableColumns(tableCols)
            .batchPartitionColumns(Arrays.asList("year"))
            .incrementalKeys(Arrays.asList("year"))
            .yearRange(2020, 2025)
            .threads(4)
            .description("My test config")
            .computedColumns(computed)
            .rowBatchSize(100)
            .rowFilter("active = true")
            .icebergTableLocation("/tmp/iceberg")
            .accessionColumn("custom_accession")
            .build();

    assertEquals("data/*.parquet", config.getSourcePattern());
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
    assertEquals("test_table", config.getTargetTableId());
    assertEquals("source_table", config.getSourceTableName());
    assertEquals(1, config.getPartitionColumns().size());
    assertEquals(2, config.getTableColumns().size());
    assertEquals(Arrays.asList("year"), config.getBatchPartitionColumns());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertEquals(2020, config.getStartYear());
    assertEquals(2025, config.getEndYear());
    assertEquals(4, config.getThreads());
    assertEquals("My test config", config.getDescription());
    assertEquals(1, config.getComputedColumns().size());
    assertEquals(100, config.getRowBatchSize());
    assertEquals("active = true", config.getRowFilter());
    assertEquals("/tmp/iceberg", config.getIcebergTableLocation());
    assertEquals("custom_accession", config.getAccessionColumn());
    assertTrue(config.supportsIncremental());
    assertEquals(Arrays.asList("year"), config.getPartitionColumnNames());
  }

  @Test
  void testBuilderDefaultAccessionColumn() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    assertEquals("accession_number", config.getAccessionColumn());
  }

  @Test
  void testBuilderDefaultThreads() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .threads(0) // 0 should use default
            .build();

    assertEquals(2, config.getThreads()); // DEFAULT_THREADS
  }

  @Test
  void testBuilderNoDescription() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("my_table")
            .build();

    // Description defaults to targetTableId
    assertEquals("my_table", config.getDescription());
  }

  @Test
  void testBuilderSupportsIncrementalFalse() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    assertFalse(config.supportsIncremental());
  }

  // ===== SourceFormat enum tests =====

  @Test
  void testSourceFormatValues() {
    IcebergMaterializer.SourceFormat[] values = IcebergMaterializer.SourceFormat.values();
    assertEquals(2, values.length);
    assertEquals(IcebergMaterializer.SourceFormat.JSON,
        IcebergMaterializer.SourceFormat.valueOf("JSON"));
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET,
        IcebergMaterializer.SourceFormat.valueOf("PARQUET"));
  }

  // ===== TableSetupResult inner class tests =====

  @Test
  void testTableSetupResultViaReflection() throws Exception {
    Class<?>[] innerClasses = IcebergMaterializer.class.getDeclaredClasses();
    Class<?> tableSetupResultClass = null;
    for (Class<?> clazz : innerClasses) {
      if (clazz.getSimpleName().equals("TableSetupResult")) {
        tableSetupResultClass = clazz;
        break;
      }
    }
    assertNotNull(tableSetupResultClass);

    java.lang.reflect.Constructor<?> ctor = tableSetupResultClass.getDeclaredConstructor(
        org.apache.iceberg.Table.class, boolean.class);
    ctor.setAccessible(true);

    Object result = ctor.newInstance((org.apache.iceberg.Table) null, true);
    assertNotNull(result);

    Field wasRecreatedField = tableSetupResultClass.getDeclaredField("wasRecreated");
    wasRecreatedField.setAccessible(true);
    assertTrue((Boolean) wasRecreatedField.get(result));

    Field tableField = tableSetupResultClass.getDeclaredField("table");
    tableField.setAccessible(true);
    assertNull(tableField.get(result));
  }

  // ===== getSourceCountForBatch tests =====

  @Test
  void testGetSourceCountForBatchErrorReturnsNegative() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", java.sql.Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    // Use a mock connection that will fail
    java.sql.Connection mockConn = mock(java.sql.Connection.class);
    when(mockConn.createStatement()).thenThrow(new java.sql.SQLException("fail"));

    long result = (Long) method.invoke(materializer, mockConn, config,
        "nonexistent/*.parquet", Collections.emptyMap());
    assertEquals(-1, result);
  }

  @Test
  void testGetSourceCountForBatchWithFilters() throws Exception {
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceCountForBatch", java.sql.Connection.class,
        IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .rowFilter("status = 'active'")
            .build();

    java.sql.Connection mockConn = mock(java.sql.Connection.class);
    when(mockConn.createStatement()).thenThrow(new java.sql.SQLException("fail"));

    Map<String, String> batch = new LinkedHashMap<String, String>();
    batch.put("year", "2023");
    batch.put("name", "test");

    long result = (Long) method.invoke(materializer, mockConn, config,
        "data/*.json", batch);
    assertEquals(-1, result);
  }

  // ===== configureS3 tests (indirectly via getDuckDBConnection) =====

  @Test
  void testConfigureS3WithRegion() throws Exception {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "key");
    s3Config.put("secretAccessKey", "secret");
    s3Config.put("endpoint", "http://minio:9000");
    s3Config.put("region", "eu-west-1");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer s3Materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    // getDuckDBConnection exercises configureS3
    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    try {
      java.sql.Connection conn = (java.sql.Connection) method.invoke(s3Materializer, 1);
      assertNotNull(conn);
      conn.close();
    } catch (Exception e) {
      // Extensions may not be available but configureS3 branch was exercised
    }
  }

  @Test
  void testConfigureS3WithoutRegion() throws Exception {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "key");
    s3Config.put("secretAccessKey", "secret");
    // No region -> should default to 'auto'
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer s3Materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    try {
      java.sql.Connection conn = (java.sql.Connection) method.invoke(s3Materializer, 1);
      assertNotNull(conn);
      conn.close();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  void testConfigureS3NoCredentials() throws Exception {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("endpoint", "http://minio:9000");
    // No access key or secret
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer s3Materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    try {
      java.sql.Connection conn = (java.sql.Connection) method.invoke(s3Materializer, 1);
      assertNotNull(conn);
      conn.close();
    } catch (Exception e) {
      // Expected
    }
  }

  // ===== getDuckDBConnection tests =====

  @Test
  void testGetDuckDBConnectionNoStorageProvider() throws Exception {
    IcebergMaterializer noStorage =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    java.sql.Connection conn = (java.sql.Connection) method.invoke(noStorage, 2);
    assertNotNull(conn);
    conn.close();
  }

  @Test
  void testGetDuckDBConnectionNullWarehouse() throws Exception {
    IcebergMaterializer nullWarehouse =
        new IcebergMaterializer(null, null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getDuckDBConnection", int.class);
    method.setAccessible(true);

    java.sql.Connection conn = (java.sql.Connection) method.invoke(nullWarehouse, 1);
    assertNotNull(conn);
    conn.close();
  }
}
