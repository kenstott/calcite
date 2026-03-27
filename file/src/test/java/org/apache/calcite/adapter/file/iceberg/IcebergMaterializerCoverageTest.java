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
import java.lang.reflect.InvocationTargetException;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for {@link IcebergMaterializer} targeting maximum line coverage.
 *
 * <p>Uses Mockito to mock all external dependencies (StorageProvider, IncrementalTracker).
 * Tests all inner classes, builders, config objects, and utility methods via reflection.
 */
@Tag("unit")
public class IcebergMaterializerCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorageProvider;
  private IncrementalTracker mockTracker;

  @BeforeEach
  void setUp() {
    mockStorageProvider = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
  }

  // ===== Constructor Tests =====

  @Test
  void testConstructorWithNullTracker() {
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, null);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithNullStorageProvider() {
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithFullS3Config() {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    s3Config.put("secretAccessKey", "secret");
    s3Config.put("endpoint", "http://localhost:9000");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
    verify(mockStorageProvider).getS3Config();
  }

  @Test
  void testConstructorWithPartialS3Config() {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    // No secretAccessKey, no endpoint
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithEmptyS3Config() {
    Map<String, String> emptyConfig = Collections.<String, String>emptyMap();
    when(mockStorageProvider.getS3Config()).thenReturn(emptyConfig);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithNullS3Config() {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithCustomRetrySettings() {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker, 5, 2000L);
    assertNotNull(materializer);
  }

  @Test
  void testDefaultConstructor() {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithS3ConfigHavingSecretKeyOnly() {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("secretAccessKey", "secret");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  @Test
  void testConstructorWithS3ConfigHavingEndpointOnly() {
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("endpoint", "http://localhost:9000");
    when(mockStorageProvider.getS3Config()).thenReturn(s3Config);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);
    assertNotNull(materializer);
  }

  // ===== MaterializationConfig Builder Tests =====

  @Test
  void testBuilderRequiresSourcePattern() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .targetTableId("test")
            .build());
  }

  @Test
  void testBuilderRequiresTargetTableId() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .build());
  }

  @Test
  void testBuilderWithEmptySourcePattern() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("")
            .targetTableId("test")
            .build());
  }

  @Test
  void testBuilderWithEmptyTargetTableId() {
    assertThrows(IllegalArgumentException.class, () ->
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("")
            .build());
  }

  @Test
  void testBuilderMinimalConfig() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .build();

    assertEquals("data/*.parquet", config.getSourcePattern());
    assertEquals("test_table", config.getTargetTableId());
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
    assertNull(config.getSourceTableName());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertTrue(config.getTableColumns().isEmpty());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertEquals(0, config.getStartYear());
    assertEquals(0, config.getEndYear());
    assertEquals(2, config.getThreads()); // default
    assertEquals("test_table", config.getDescription()); // defaults to targetTableId
    assertTrue(config.getComputedColumns().isEmpty());
    assertEquals(0, config.getRowBatchSize());
    assertNull(config.getRowFilter());
    assertNull(config.getIcebergTableLocation());
    assertEquals("accession_number", config.getAccessionColumn());
    assertFalse(config.supportsIncremental());
    assertTrue(config.getPartitionColumnNames().isEmpty());
  }

  @Test
  void testBuilderFullConfig() {
    List<ColumnDefinition> partitionCols = new ArrayList<ColumnDefinition>();
    partitionCols.add(new ColumnDefinition("year", "INTEGER"));
    partitionCols.add(new ColumnDefinition("region", "VARCHAR"));

    List<IcebergCatalogManager.ColumnDef> tableCols =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    tableCols.add(new IcebergCatalogManager.ColumnDef("year", "INTEGER"));
    tableCols.add(new IcebergCatalogManager.ColumnDef("region", "VARCHAR"));
    tableCols.add(new IcebergCatalogManager.ColumnDef("value", "DOUBLE"));

    Map<String, String> computedCols = new HashMap<String, String>();
    computedCols.put("embedding", "embed_jina(text)::FLOAT[768]");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/data/year=*/*.parquet")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .targetTableId("schema.full_table")
            .sourceTableName("source_data")
            .partitionColumns(partitionCols)
            .tableColumns(tableCols)
            .batchPartitionColumns(Arrays.asList("year", "region"))
            .incrementalKeys(Arrays.asList("year"))
            .yearRange(2020, 2025)
            .threads(8)
            .description("Full config test")
            .computedColumns(computedCols)
            .rowBatchSize(30)
            .rowFilter("cik IN ('0001', '0002')")
            .icebergTableLocation("s3://bucket/warehouse/table")
            .accessionColumn("custom_accession")
            .build();

    assertEquals("s3://bucket/data/year=*/*.parquet", config.getSourcePattern());
    assertEquals(IcebergMaterializer.SourceFormat.JSON, config.getSourceFormat());
    assertEquals("schema.full_table", config.getTargetTableId());
    assertEquals("source_data", config.getSourceTableName());
    assertEquals(2, config.getPartitionColumns().size());
    assertEquals(3, config.getTableColumns().size());
    assertEquals(Arrays.asList("year", "region"), config.getBatchPartitionColumns());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertEquals(2020, config.getStartYear());
    assertEquals(2025, config.getEndYear());
    assertEquals(8, config.getThreads());
    assertEquals("Full config test", config.getDescription());
    assertEquals(1, config.getComputedColumns().size());
    assertEquals("embed_jina(text)::FLOAT[768]", config.getComputedColumns().get("embedding"));
    assertEquals(30, config.getRowBatchSize());
    assertEquals("cik IN ('0001', '0002')", config.getRowFilter());
    assertEquals("s3://bucket/warehouse/table", config.getIcebergTableLocation());
    assertEquals("custom_accession", config.getAccessionColumn());
    assertTrue(config.supportsIncremental());

    // Test getPartitionColumnNames
    List<String> partitionNames = config.getPartitionColumnNames();
    assertEquals(2, partitionNames.size());
    assertEquals("year", partitionNames.get(0));
    assertEquals("region", partitionNames.get(1));
  }

  @Test
  void testBuilderSourceFormatDefaultsToParquet() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
  }

  @Test
  void testBuilderDescriptionDefaultsToTargetTableId() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("my_table")
            .build();
    assertEquals("my_table", config.getDescription());
  }

  @Test
  void testBuilderAccessionColumnDefaultsToAccessionNumber() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();
    assertEquals("accession_number", config.getAccessionColumn());
  }

  @Test
  void testBuilderThreadsDefaultToTwo() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .threads(0) // zero or negative defaults to DEFAULT_THREADS=2
            .build();
    assertEquals(2, config.getThreads());
  }

  @Test
  void testBuilderThreadsNegativeDefaultsToTwo() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .threads(-1)
            .build();
    assertEquals(2, config.getThreads());
  }

  // ===== MaterializationResult Tests =====

  @Test
  void testMaterializationResultBasic() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "test_table", 10, 0, 5, 1500);

    assertEquals("test_table", result.getTableId());
    assertEquals(10, result.getSuccessCount());
    assertEquals(0, result.getFailedCount());
    assertEquals(5, result.getSkippedCount());
    assertEquals(1500, result.getDurationMs());
    assertTrue(result.isFullySuccessful());
    assertFalse(result.isTableRecreated());
  }

  @Test
  void testMaterializationResultWithFailures() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "failed_table", 8, 2, 3, 2000);

    assertFalse(result.isFullySuccessful());
    assertEquals(2, result.getFailedCount());
  }

  @Test
  void testMaterializationResultWithRecreatedFlag() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "recreated_table", 5, 0, 0, 1000, true);

    assertTrue(result.isTableRecreated());
    assertTrue(result.isFullySuccessful());
  }

  @Test
  void testMaterializationResultWithRecreatedFalse() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "normal_table", 5, 0, 0, 1000, false);

    assertFalse(result.isTableRecreated());
  }

  @Test
  void testMaterializationResultToString() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "my_table", 10, 2, 3, 5000, true);

    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("my_table"));
    assertTrue(str.contains("10"));
    assertTrue(str.contains("5000"));
    assertTrue(str.contains("true"));
  }

  @Test
  void testMaterializationResultZeroCounts() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "empty_table", 0, 0, 0, 100);

    assertEquals(0, result.getSuccessCount());
    assertEquals(0, result.getFailedCount());
    assertEquals(0, result.getSkippedCount());
    assertTrue(result.isFullySuccessful());
  }

  // ===== SourceFormat Enum Tests =====

  @Test
  void testSourceFormatValues() {
    IcebergMaterializer.SourceFormat[] formats = IcebergMaterializer.SourceFormat.values();
    assertEquals(2, formats.length);
    assertEquals(IcebergMaterializer.SourceFormat.JSON,
        IcebergMaterializer.SourceFormat.valueOf("JSON"));
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET,
        IcebergMaterializer.SourceFormat.valueOf("PARQUET"));
  }

  // ===== extractCiksFromRowFilter (package-private static method) Tests =====

  @Test
  void testExtractCiksFromNullFilter() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(null);
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromEmptyFilter() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter("");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromFilterWithNoCikClause() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("year = 2023 AND type = 'annual'");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksFromSingleCik() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test
  void testExtractCiksFromMultipleCiks() {
    Set<String> ciks = IcebergMaterializer.extractCiksFromRowFilter(
        "cik IN ('0000320193', '0000004962', '0001234567')");
    assertEquals(3, ciks.size());
    assertTrue(ciks.contains("0000320193"));
    assertTrue(ciks.contains("0000004962"));
    assertTrue(ciks.contains("0001234567"));
  }

  @Test
  void testExtractCiksFromUppercaseFilter() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test
  void testExtractCiksFromDoubleSpaceFilter() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("CIK  IN ('0000320193')");
    assertEquals(1, ciks.size());
    assertTrue(ciks.contains("0000320193"));
  }

  @Test
  void testExtractCiksWithMissingOpenParen() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksWithMissingCloseParen() {
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN ('0000320193'");
    assertTrue(ciks.isEmpty());
  }

  @Test
  void testExtractCiksWithUnquotedValues() {
    // Values without quotes should be ignored
    Set<String> ciks =
        IcebergMaterializer.extractCiksFromRowFilter("cik IN (0000320193, 0000004962)");
    assertTrue(ciks.isEmpty());
  }

  // ===== Private Method Tests via Reflection =====

  @Test
  void testCoerceValueNullValue() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertNull(method.invoke(materializer, null, "INTEGER"));
  }

  @Test
  void testCoerceValueNullType() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals("test", method.invoke(materializer, "test", null));
  }

  @Test
  void testCoerceValueInteger() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(42, method.invoke(materializer, "42", "INTEGER"));
    assertEquals(42, method.invoke(materializer, "42", "INT"));
  }

  @Test
  void testCoerceValueBigint() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(123456789L, method.invoke(materializer, "123456789", "BIGINT"));
    assertEquals(123456789L, method.invoke(materializer, "123456789", "LONG"));
  }

  @Test
  void testCoerceValueDouble() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(3.14, method.invoke(materializer, "3.14", "DOUBLE"));
  }

  @Test
  void testCoerceValueBoolean() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals(true, method.invoke(materializer, "true", "BOOLEAN"));
    assertEquals(false, method.invoke(materializer, "false", "BOOLEAN"));
  }

  @Test
  void testCoerceValueVarchar() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "coerceValue", String.class, String.class);
    method.setAccessible(true);

    assertEquals("hello", method.invoke(materializer, "hello", "VARCHAR"));
    assertEquals("hello", method.invoke(materializer, "hello", "TEXT"));
  }

  @Test
  void testFindColumnType() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

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

  @Test
  void testNeedsFilenameEmbedding() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "needsFilenameEmbedding", IcebergMaterializer.MaterializationConfig.class, Map.class);
    method.setAccessible(true);

    // Empty batch
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    assertFalse((Boolean) method.invoke(materializer, config,
        Collections.<String, String>emptyMap()));

    // Batch key that IS a partition column
    List<ColumnDefinition> partitionCols = new ArrayList<ColumnDefinition>();
    partitionCols.add(new ColumnDefinition("year", "INTEGER"));
    IcebergMaterializer.MaterializationConfig configWithPartitions =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .partitionColumns(partitionCols)
            .build();

    Map<String, String> batchInPartitions = new HashMap<String, String>();
    batchInPartitions.put("year", "2024");
    assertFalse((Boolean) method.invoke(materializer, configWithPartitions, batchInPartitions));

    // Batch key that is NOT a partition column
    Map<String, String> batchNotInPartitions = new HashMap<String, String>();
    batchNotInPartitions.put("region", "US");
    assertTrue((Boolean) method.invoke(materializer, configWithPartitions, batchNotInPartitions));
  }

  @Test
  void testBuildFilenamePattern() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildFilenamePattern", Map.class);
    method.setAccessible(true);

    // Single key
    Map<String, String> singleBatch = new LinkedHashMap<String, String>();
    singleBatch.put("year", "2024");
    String pattern = (String) method.invoke(materializer, singleBatch);
    assertEquals("year_2024_{i}", pattern);

    // Multiple keys
    Map<String, String> multiBatch = new LinkedHashMap<String, String>();
    multiBatch.put("year", "2024");
    multiBatch.put("region", "US");
    String multiPattern = (String) method.invoke(materializer, multiBatch);
    assertEquals("year_2024_region_US_{i}", multiPattern);
  }

  @Test
  void testBuildBatchCombinations() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildBatchCombinations", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    // No batch columns
    IcebergMaterializer.MaterializationConfig noBatch =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    @SuppressWarnings("unchecked")
    List<Map<String, String>> result =
        (List<Map<String, String>>) method.invoke(materializer, noBatch);
    assertTrue(result.isEmpty());

    // Year-based batch columns
    IcebergMaterializer.MaterializationConfig yearBatch =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .batchPartitionColumns(Arrays.asList("year"))
            .yearRange(2023, 2025)
            .build();

    @SuppressWarnings("unchecked")
    List<Map<String, String>> yearResult =
        (List<Map<String, String>>) method.invoke(materializer, yearBatch);
    assertEquals(3, yearResult.size());
    assertEquals("2023", yearResult.get(0).get("year"));
    assertEquals("2024", yearResult.get(1).get("year"));
    assertEquals("2025", yearResult.get(2).get("year"));
  }

  @Test
  void testGroupBatchesByIncrementalKey() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "groupBatchesByIncrementalKey", List.class, List.class);
    method.setAccessible(true);

    // Batches with incremental key
    List<Map<String, String>> batches = new ArrayList<Map<String, String>>();
    Map<String, String> batch1 = new LinkedHashMap<String, String>();
    batch1.put("year", "2023");
    batches.add(batch1);
    Map<String, String> batch2 = new LinkedHashMap<String, String>();
    batch2.put("year", "2024");
    batches.add(batch2);

    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        (Map<Map<String, String>, List<Map<String, String>>>)
            method.invoke(materializer, batches, Arrays.asList("year"));

    assertEquals(2, grouped.size());

    // With null incremental keys
    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> nullGrouped =
        (Map<Map<String, String>, List<Map<String, String>>>)
            method.invoke(materializer, batches, null);

    assertEquals(1, nullGrouped.size()); // all batches grouped under empty key

    // With empty incremental keys list
    @SuppressWarnings("unchecked")
    Map<Map<String, String>, List<Map<String, String>>> emptyGrouped =
        (Map<Map<String, String>, List<Map<String, String>>>)
            method.invoke(materializer, batches, Collections.<String>emptyList());

    assertEquals(1, emptyGrouped.size());
  }

  @Test
  void testBuildCountSqlForParquet() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config, "data/*.parquet");
    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertTrue(sql.contains("read_parquet"));
    assertTrue(sql.contains("hive_partitioning=true"));
  }

  @Test
  void testBuildCountSqlForJson() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    String sql = (String) method.invoke(materializer, config, "data/*.json");
    assertTrue(sql.contains("read_json"));
    assertTrue(sql.contains("union_by_name=true"));
  }

  @Test
  void testBuildCountSqlWithRowFilter() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCountSql", IcebergMaterializer.MaterializationConfig.class, String.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .rowFilter("cik IN ('0001')")
            .build();

    String sql = (String) method.invoke(materializer, config, "data/*.parquet");
    assertTrue(sql.contains("WHERE"));
    assertTrue(sql.contains("cik IN ('0001')"));
  }

  @Test
  void testBuildSelectSql() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    // Basic: no computed columns, no filter, no exclusions
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), null);
    assertTrue(sql.startsWith("SELECT * FROM"));
    assertTrue(sql.contains("read_parquet"));
  }

  @Test
  void testBuildSelectSqlWithComputedColumns() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    Map<String, String> computedCols = new HashMap<String, String>();
    computedCols.put("full_name", "first_name || ' ' || last_name");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .computedColumns(computedCols)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), null);
    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("AS full_name"));
  }

  @Test
  void testBuildSelectSqlJsonSource() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", Collections.<String, String>emptyMap(), null);
    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildSelectSqlWithRowFilter() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .rowFilter("year > 2020")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), null);
    assertTrue(sql.contains("WHERE year > 2020"));
  }

  @Test
  void testBuildSelectSqlWithSmallExclusionSet() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");
    exclusions.add("acc2");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), exclusions);
    assertTrue(sql.contains("NOT IN"));
    assertTrue(sql.contains("'acc1'"));
    assertTrue(sql.contains("'acc2'"));
  }

  @Test
  void testBuildSelectSqlWithLargeExclusionSet() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    // Create exclusion set > 100 items
    Set<String> exclusions = new HashSet<String>();
    for (int i = 0; i < 150; i++) {
      exclusions.add("acc_" + i);
    }

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), exclusions);
    assertTrue(sql.contains("NOT EXISTS"));
    assertTrue(sql.contains("_exclusions"));
  }

  @Test
  void testBuildSelectSqlWithFilterAndExclusions() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .rowFilter("year = 2024")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(), exclusions);
    assertTrue(sql.contains("WHERE year = 2024"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("NOT IN"));
  }

  // ===== buildSelectSqlWithPaging Tests =====

  @Test
  void testBuildSelectSqlWithPagingBasic() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(),
        100, 0, null);
    assertTrue(sql.contains("LIMIT 100"));
    assertFalse(sql.contains("OFFSET"));
  }

  @Test
  void testBuildSelectSqlWithPagingAndOffset() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(),
        50, 100, null);
    assertTrue(sql.contains("LIMIT 50"));
    assertTrue(sql.contains("OFFSET 100"));
  }

  @Test
  void testBuildSelectSqlWithPagingJsonSource() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", Collections.<String, String>emptyMap(),
        50, 0, null);
    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildSelectSqlWithPagingAndComputedCols() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    Map<String, String> computedCols = new HashMap<String, String>();
    computedCols.put("full_name", "first || last");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .computedColumns(computedCols)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(),
        50, 0, null);
    assertTrue(sql.contains("SELECT *, "));
    assertTrue(sql.contains("AS full_name"));
  }

  @Test
  void testBuildSelectSqlWithPagingAndExclusions() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildSelectSqlWithPaging", IcebergMaterializer.MaterializationConfig.class,
        String.class, Map.class, int.class, int.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .rowFilter("year = 2024")
            .build();

    Set<String> exclusions = new HashSet<String>();
    exclusions.add("acc1");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", Collections.<String, String>emptyMap(),
        50, 0, exclusions);
    assertTrue(sql.contains("WHERE year = 2024"));
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("NOT IN"));
    assertTrue(sql.contains("LIMIT 50"));
  }

  // ===== buildDuckDBSql Tests =====

  @Test
  void testBuildDuckDBSqlBasic() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/output.parquet",
        Collections.<String, String>emptyMap(), -1, -1);
    assertTrue(sql.contains("COPY ("));
    assertTrue(sql.contains("SELECT * FROM"));
    assertTrue(sql.contains("OVERWRITE_OR_IGNORE"));
    assertTrue(sql.contains("TO '/tmp/staging/output.parquet'"));
  }

  @Test
  void testBuildDuckDBSqlJsonSource() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.json")
            .targetTableId("test")
            .sourceFormat(IcebergMaterializer.SourceFormat.JSON)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.json", "/tmp/staging/output.parquet",
        Collections.<String, String>emptyMap(), -1, -1);
    assertTrue(sql.contains("read_json"));
  }

  @Test
  void testBuildDuckDBSqlWithComputedColumns() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    Map<String, String> computedCols = new LinkedHashMap<String, String>();
    computedCols.put("full_name", "first || ' ' || last");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .computedColumns(computedCols)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/output.parquet",
        Collections.<String, String>emptyMap(), -1, -1);
    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("AS full_name"));
  }

  @Test
  void testBuildDuckDBSqlWithPartitions() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .partitionColumns(partCols)
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/output.parquet",
        Collections.<String, String>emptyMap(), -1, -1);
    assertTrue(sql.contains("PARTITION_BY"));
    assertTrue(sql.contains("year"));
  }

  @Test
  void testBuildDuckDBSqlWithFilenameEmbedding() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    List<ColumnDefinition> partCols = new ArrayList<ColumnDefinition>();
    partCols.add(new ColumnDefinition("year", "INTEGER"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .partitionColumns(partCols)
            .build();

    // Batch key not in partition columns triggers filename embedding
    Map<String, String> batch = new HashMap<String, String>();
    batch.put("region", "US");

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/output.parquet",
        batch, -1, -1);
    assertTrue(sql.contains("FILENAME_PATTERN"));
  }

  @Test
  void testBuildDuckDBSqlWithRowBatching() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/batch.parquet",
        Collections.<String, String>emptyMap(), 30, 60);
    assertTrue(sql.contains("LIMIT 30"));
    assertTrue(sql.contains("OFFSET 60"));
    // When limit > 0, partition and filename embedding are skipped
    assertFalse(sql.contains("PARTITION_BY"));
    assertFalse(sql.contains("OVERWRITE_OR_IGNORE"));
  }

  @Test
  void testBuildDuckDBSqlWithRowFilter() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildDuckDBSql", IcebergMaterializer.MaterializationConfig.class,
        String.class, String.class, Map.class, int.class, int.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .rowFilter("cik IN ('0001')")
            .build();

    String sql = (String) method.invoke(materializer, config,
        "data/*.parquet", "/tmp/staging/output.parquet",
        Collections.<String, String>emptyMap(), -1, -1);
    assertTrue(sql.contains("WHERE cik IN ('0001')"));
  }

  // ===== cleanupStagingDirectory Tests =====

  @Test
  void testCleanupStagingDirectoryS3Path() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // S3 path should be skipped
    method.invoke(materializer, "s3://bucket/staging/path");
    // No calls to storageProvider since S3 cleanup is skipped
  }

  @Test
  void testCleanupStagingDirectoryS3aPath() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // S3a path should also be skipped
    method.invoke(materializer, "s3a://bucket/staging/path");
  }

  @Test
  void testCleanupStagingDirectoryLocalPath() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());
    when(mockStorageProvider.delete(anyString())).thenReturn(true);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, "/tmp/staging/local");
  }

  @Test
  void testCleanupStagingDirectoryLocalPathWithFiles() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("/tmp/staging/file1.parquet",
        "file1.parquet", false, 1024, System.currentTimeMillis()));

    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);
    when(mockStorageProvider.deleteBatch(any(List.class))).thenReturn(1);
    when(mockStorageProvider.delete(anyString())).thenReturn(true);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    method.invoke(materializer, "/tmp/staging/local");
    verify(mockStorageProvider).deleteBatch(any(List.class));
  }

  @Test
  void testCleanupStagingDirectoryLocalPathDeleteThrows() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenReturn(Collections.<StorageProvider.FileEntry>emptyList());
    when(mockStorageProvider.delete(anyString())).thenThrow(new IOException("delete failed"));

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Should not throw - catches the IOException
    method.invoke(materializer, "/tmp/staging/local");
  }

  @Test
  void testCleanupStagingDirectoryLocalPathListThrows() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenThrow(new IOException("list failed"));

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Should not throw - catches the IOException
    method.invoke(materializer, "/tmp/staging/local");
  }

  // ===== getSourceFileWatermark Tests =====

  @Test
  void testGetSourceFileWatermarkS3Path() {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    // S3 paths should return 0 (watermark not available for S3)
    long watermark = materializer.getSourceFileWatermark(
        "s3://bucket/data/*.parquet", IcebergMaterializer.SourceFormat.PARQUET);
    assertEquals(0, watermark);
  }

  @Test
  void testGetSourceFileWatermarkS3aPath() {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    long watermark = materializer.getSourceFileWatermark(
        "s3a://bucket/data/*.parquet", IcebergMaterializer.SourceFormat.PARQUET);
    assertEquals(0, watermark);
  }

  // ===== countAllAccessions Tests =====

  @Test
  void testCountAllAccessions() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    Map<String, Set<String>> cikToAccessions = new HashMap<String, Set<String>>();
    Set<String> set1 = new HashSet<String>();
    set1.add("acc1");
    set1.add("acc2");
    cikToAccessions.put("cik1", set1);
    Set<String> set2 = new HashSet<String>();
    set2.add("acc3");
    cikToAccessions.put("cik2", set2);

    int count = (Integer) method.invoke(materializer, cikToAccessions);
    assertEquals(3, count);
  }

  @Test
  void testCountAllAccessionsEmpty() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "countAllAccessions", Map.class);
    method.setAccessible(true);

    int count = (Integer) method.invoke(materializer,
        new HashMap<String, Set<String>>());
    assertEquals(0, count);
  }

  // ===== getFilteredSourceAccessions Tests =====

  @Test
  void testGetFilteredSourceAccessionsNullProvider() throws Exception {
    // null storage provider
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/year=*/*_facts.parquet", "2023", null);
    assertNull(result);
  }

  @Test
  void testGetFilteredSourceAccessionsNoCikFilter() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    // Mock storageProvider.listFiles to return some files
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc-1_facts.parquet",
        "0001_acc-1_facts.parquet", false, 100, 1000L));
    files.add(new StorageProvider.FileEntry("path/0002_acc-2_facts.parquet",
        "0002_acc-2_facts.parquet", false, 200, 2000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    // No CIK filter - should return all accessions
    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023", null);
    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains("acc-1"));
    assertTrue(result.contains("acc-2"));
  }

  @Test
  void testGetFilteredSourceAccessionsWithCikFilter() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc-1_facts.parquet",
        "0001_acc-1_facts.parquet", false, 100, 1000L));
    files.add(new StorageProvider.FileEntry("path/0002_acc-2_facts.parquet",
        "0002_acc-2_facts.parquet", false, 200, 2000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    // Filter by CIK 0001 only
    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023",
        "cik IN ('0001')");
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains("acc-1"));
  }

  @Test
  void testGetFilteredSourceAccessionsListFailure() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockStorageProvider.listFiles(anyString(), anyBoolean()))
        .thenThrow(new IOException("S3 error"));

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023", null);
    assertNull(result);
  }

  @Test
  void testGetFilteredSourceAccessionsNoYearWildcard() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    // Pattern without year=* should return null
    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/data/*.parquet", "2023", null);
    assertNull(result);
  }

  @Test
  void testGetFilteredSourceAccessionsUsesCache() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc-1_facts.parquet",
        "0001_acc-1_facts.parquet", false, 100, 1000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    // First call populates cache
    method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023", null);

    // Second call should use cache (listFiles called only once)
    method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023", null);

    // Verify listFiles was called only once (cached)
    org.mockito.Mockito.verify(mockStorageProvider, org.mockito.Mockito.times(1))
        .listFiles(anyString(), anyBoolean());
  }

  @Test
  void testGetFilteredSourceAccessionsFileSuffixExtraction() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc-1_metadata.parquet",
        "0001_acc-1_metadata.parquet", false, 100, 1000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getFilteredSourceAccessions", String.class, String.class, String.class);
    method.setAccessible(true);

    // Pattern with *_metadata.parquet suffix
    Set<String> result = (Set<String>) method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_metadata.parquet", "2023", null);
    assertNotNull(result);
  }

  // ===== getTrackedAccessions Tests =====

  @Test
  void testGetTrackedAccessionsWithMatchingYear() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    Set<Map<String, String>> processedKeys = new HashSet<Map<String, String>>();
    Map<String, String> key1 = new LinkedHashMap<String, String>();
    key1.put("year", "2023");
    key1.put("accession_number", "acc-1");
    processedKeys.add(key1);

    Map<String, String> key2 = new LinkedHashMap<String, String>();
    key2.put("year", "2024");
    key2.put("accession_number", "acc-2");
    processedKeys.add(key2);

    when(mockTracker.getProcessedKeyValues("my_table")).thenReturn(processedKeys);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "my_table", "2023");
    assertEquals(1, result.size());
    assertTrue(result.contains("acc-1"));
  }

  @Test
  void testGetTrackedAccessionsWithNullYear() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    Set<Map<String, String>> processedKeys = new HashSet<Map<String, String>>();
    Map<String, String> key1 = new LinkedHashMap<String, String>();
    key1.put("accession_number", "acc-1");
    processedKeys.add(key1);

    when(mockTracker.getProcessedKeyValues("my_table")).thenReturn(processedKeys);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "my_table", null);
    assertEquals(1, result.size());
    assertTrue(result.contains("acc-1"));
  }

  @Test
  void testGetTrackedAccessionsException() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockTracker.getProcessedKeyValues(anyString()))
        .thenThrow(new RuntimeException("tracker error"));

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getTrackedAccessions", String.class, String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, "my_table", "2023");
    assertTrue(result.isEmpty());
  }

  // ===== isSourceWatermarkEnabled Tests =====

  @Test
  void testIsSourceWatermarkEnabled() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "isSourceWatermarkEnabled", IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    // Always returns true
    assertTrue((Boolean) method.invoke(materializer, config));
  }

  // ===== createStagingPath Tests =====

  @Test
  void testCreateStagingPath() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(invocation -> {
          String base = invocation.getArgument(0);
          String rel = invocation.getArgument(1);
          return base + "/" + rel;
        });
    doNothing().when(mockStorageProvider).ensureLifecycleRule(anyString(), anyInt());
    doNothing().when(mockStorageProvider).createDirectories(anyString());

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod("createStagingPath");
    method.setAccessible(true);

    String path = (String) method.invoke(materializer);
    assertNotNull(path);
    assertTrue(path.contains(".staging/"));
    verify(mockStorageProvider).ensureLifecycleRule(".staging/", 1);
    verify(mockStorageProvider).createDirectories(anyString());
  }

  // ===== getExcludedAccessions Tests =====

  @Test
  void testGetExcludedAccessionsNoLocation() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        null, Collections.<String, String>emptyMap());
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetExcludedAccessionsEmptyLocation() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .icebergTableLocation("")
            .build();

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        null, Collections.<String, String>emptyMap());
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetExcludedAccessionsWithTrackerData() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    Set<Map<String, String>> processedKeys = new HashSet<Map<String, String>>();
    Map<String, String> key1 = new LinkedHashMap<String, String>();
    key1.put("year", "2023");
    key1.put("accession_number", "tracked-acc");
    processedKeys.add(key1);
    when(mockTracker.getProcessedKeyValues("test_table")).thenReturn(processedKeys);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getExcludedAccessions", IcebergMaterializer.MaterializationConfig.class,
        org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test_table")
            .icebergTableLocation("s3://bucket/warehouse/table")
            .build();

    Map<String, String> batch = new HashMap<String, String>();
    batch.put("year", "2023");

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(materializer, config,
        null, batch);
    assertTrue(result.contains("tracked-acc"));
  }

  // ===== selfHealTracker Tests =====

  @Test
  void testSelfHealTrackerNullSourceAccessions() throws Exception {
    // No storage provider => getFilteredSourceAccessions returns null
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/data/year=*/*_facts.parquet")
            .targetTableId("test")
            .build();

    // Should not throw
    method.invoke(materializer, config, "2023", new HashSet<String>());
  }

  @Test
  void testSelfHealTrackerAllAlreadyTracked() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);

    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc-1_facts.parquet",
        "0001_acc-1_facts.parquet", false, 100, 1000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "selfHealTracker", IcebergMaterializer.MaterializationConfig.class,
        String.class, Set.class);
    method.setAccessible(true);

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("s3://bucket/source=sec/year=*/*_facts.parquet")
            .targetTableId("test")
            .build();

    Set<String> excluded = new HashSet<String>();
    excluded.add("acc-1"); // Already tracked

    // Should not call markProcessed since all are already tracked
    method.invoke(materializer, config, "2023", excluded);
  }

  // ===== partitionHasData Tests =====

  @Test
  void testPartitionHasDataNullTable() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    assertFalse((Boolean) method.invoke(materializer, null,
        Collections.singletonMap("year", "2023")));
  }

  @Test
  void testPartitionHasDataNullPartitionValues() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    org.apache.iceberg.Table mockTable = mock(org.apache.iceberg.Table.class);
    assertFalse((Boolean) method.invoke(materializer, mockTable, null));
  }

  @Test
  void testPartitionHasDataEmptyPartitionValues() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "partitionHasData", org.apache.iceberg.Table.class, Map.class);
    method.setAccessible(true);

    org.apache.iceberg.Table mockTable = mock(org.apache.iceberg.Table.class);
    assertFalse((Boolean) method.invoke(materializer, mockTable,
        Collections.<String, String>emptyMap()));
  }

  // ===== getSourceAccessions Cache Tests =====

  @Test
  void testGetSourceAccessionsNoYearWildcard() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    // Pattern without year=*
    Object result = method.invoke(materializer, "s3://bucket/data/*.parquet", "2023");
    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsNullStorageProvider() throws Exception {
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), null, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    Object result = method.invoke(materializer,
        "s3://bucket/year=*/*.parquet", "2023");
    assertNull(result);
  }

  @Test
  void testGetSourceAccessionsWithFileSuffixExtraction() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    List<StorageProvider.FileEntry> files = new ArrayList<StorageProvider.FileEntry>();
    files.add(new StorageProvider.FileEntry("path/0001_acc1_facts.parquet",
        "0001_acc1_facts.parquet", false, 100, 1000L));
    files.add(new StorageProvider.FileEntry("path/0001_acc2_metadata.parquet",
        "0001_acc2_metadata.parquet", false, 100, 1000L));
    when(mockStorageProvider.listFiles(anyString(), anyBoolean())).thenReturn(files);

    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "getSourceAccessions", String.class, String.class);
    method.setAccessible(true);

    // Should only match *_facts.parquet files
    @SuppressWarnings("unchecked")
    Map<String, Set<String>> result = (Map<String, Set<String>>) method.invoke(materializer,
        "s3://bucket/source=sec/year=*/*_facts.parquet", "2023");
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey("0001"));
    assertTrue(result.get("0001").contains("acc1"));
  }

  // ===== buildCombinationsRecursive Tests =====

  @Test
  void testBuildCombinationsRecursiveMultipleDimensions() throws Exception {
    when(mockStorageProvider.getS3Config()).thenReturn(null);
    IcebergMaterializer materializer =
        new IcebergMaterializer(tempDir.toString(), mockStorageProvider, mockTracker);

    Method method = IcebergMaterializer.class.getDeclaredMethod(
        "buildCombinationsRecursive", List.class, List.class,
        int.class, Map.class, List.class);
    method.setAccessible(true);

    List<String> colNames = Arrays.asList("year", "region");
    List<List<String>> colValues = new ArrayList<List<String>>();
    colValues.add(Arrays.asList("2023", "2024"));
    colValues.add(Arrays.asList("US", "EU"));

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    method.invoke(materializer, colNames, colValues, 0,
        new LinkedHashMap<String, String>(), result);

    // Cartesian product: 2 years x 2 regions = 4 combinations
    assertEquals(4, result.size());
  }
}
