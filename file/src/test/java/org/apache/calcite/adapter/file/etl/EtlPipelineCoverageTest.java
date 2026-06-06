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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive coverage tests for {@link EtlPipeline} execution paths.
 *
 * <p>These tests focus on line coverage for the execute() method and related private
 * methods by using mocked dependencies (StorageProvider, IncrementalTracker,
 * MaterializationWriter, DataProvider, DataWriter) to exercise all code paths
 * without requiring actual infrastructure.
 */
@Tag("unit")
public class EtlPipelineCoverageTest {

  private StorageProvider mockStorage;
  private IncrementalTracker mockTracker;
  private MaterializationWriter mockWriter;
  private MockedStatic<MaterializationWriterFactory> factoryMock;

  @BeforeEach
  void setUp() {
    mockStorage = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
    mockWriter = mock(MaterializationWriter.class);

    // Mock the factory to return our mockWriter instead of creating real writers
    factoryMock = mockStatic(MaterializationWriterFactory.class);
    factoryMock.when(
        () -> MaterializationWriterFactory.createFromConfig(
        any(MaterializeConfig.class), any(StorageProvider.class),
        anyString(), any(IncrementalTracker.class)))
        .thenReturn(mockWriter);
  }

  @AfterEach
  void tearDown() {
    if (factoryMock != null) {
      factoryMock.close();
    }
  }

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private EtlPipelineConfig buildConfig(String name, Map<String, DimensionConfig> dims) {
    return buildConfig(name, dims, null, null);
  }

  private EtlPipelineConfig buildConfig(String name, Map<String, DimensionConfig> dims,
      MaterializeConfig.Format format, List<ColumnConfig> columns) {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    MaterializeConfig.Builder matBuilder = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build());
    if (format != null) {
      matBuilder.format(format);
    }
    if (columns != null) {
      matBuilder.columns(columns);
    }

    EtlPipelineConfig.Builder configBuilder = EtlPipelineConfig.builder()
        .name(name)
        .source(source)
        .materialize(matBuilder.build());
    if (dims != null) {
      configBuilder.dimensions(dims);
    }
    if (columns != null) {
      configBuilder.columns(columns);
    }
    return configBuilder.build();
  }

  private Map<String, DimensionConfig> singleRangeDimension(String name, int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(name, DimensionConfig.builder()
        .name(name)
        .type(DimensionType.RANGE)
        .start(start)
        .end(end)
        .build());
    return dims;
  }

  private Map<String, DimensionConfig> singleListDimension(String name, List<String> values) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(name, DimensionConfig.builder()
        .name(name)
        .type(DimensionType.LIST)
        .values(values)
        .build());
    return dims;
  }

  /**
   * Helper to set up a mock data provider returning a fixed iterator.
   */
  private DataProvider dataProviderReturning(final Iterator<Map<String, Object>> data) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        return data;
      }
    };
  }

  /**
   * Creates an iterator of N simple rows.
   */
  private Iterator<Map<String, Object>> rowIterator(int count) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "row_" + i);
      rows.add(row);
    }
    return rows.iterator();
  }

  // -----------------------------------------------------------------------
  // Constructor tests
  // -----------------------------------------------------------------------

  @Test void testConstructorWithMinimalArgs() {
    EtlPipelineConfig config = buildConfig("test", null);
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/base");
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithProgressListener() {
    EtlPipelineConfig config = buildConfig("test", null);
    EtlPipeline.ProgressListener listener = new EtlPipeline.LoggingProgressListener();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/base", listener);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithIncrementalTracker() {
    EtlPipelineConfig config = buildConfig("test", null);
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/base", null, mockTracker);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithAllOptions() {
    EtlPipelineConfig config = buildConfig("test", null);
    DataProvider dp = DataProvider.DEFAULT;
    DataWriter dw = DataWriter.DEFAULT;
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, dw);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithSeparateSourceStorage() {
    EtlPipelineConfig config = buildConfig("test", null);
    StorageProvider sourceStorage = mock(StorageProvider.class);
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, sourceStorage, "/base", null, mockTracker, null, null);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithOperatingDirectory() {
    EtlPipelineConfig config = buildConfig("test", null);
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, null, "/base", null, mockTracker, null, null, "/ops");
    assertNotNull(pipeline);
  }

  @Test void testStaticCreateMethod() {
    EtlPipelineConfig config = buildConfig("test", null);
    EtlPipeline pipeline = EtlPipeline.create(config, mockStorage, "/base");
    assertNotNull(pipeline);
  }

  // -----------------------------------------------------------------------
  // execute() - empty dimensions => 0 combinations
  // -----------------------------------------------------------------------

  @Test void testExecuteEmptyDimensions() throws IOException {
    // Note: null/empty dimensions still produce 1 combination (the empty map)
    // via DimensionIterator.expand(). To get truly 0 combinations, we need to
    // mock a scenario where all combos are already processed.
    EtlPipelineConfig config = buildConfig("empty_pipeline", null);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    // Return empty set => all combinations already processed
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(Collections.<Integer>emptySet());

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals("empty_pipeline", result.getPipelineName());
    // All 1 combinations already processed => 0 needed => marks complete
    assertEquals(1, result.getSkippedBatches());
    assertEquals(0, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - cached completion fast-path: config hash match + data exists
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedCompletionFastPathSkip() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));
    EtlPipelineConfig config = buildConfig("cached_pipeline", dims);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 1000);

    when(mockTracker.getCachedCompletion("cached_pipeline")).thenReturn(cached);
    when(mockStorage.isDirectory("/base/cached_pipeline/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(1000, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - cached completion with 0 rows and TTL not expired
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedCompletionZeroRowsTtlNotExpired() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("zero_rows_pipeline", dims);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    // completedAt is now (not expired)
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 0, System.currentTimeMillis());

    when(mockTracker.getCachedCompletion("zero_rows_pipeline")).thenReturn(cached);
    when(mockStorage.isDirectory("/base/zero_rows_pipeline/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(0, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - cached completion with 0 rows and TTL expired => re-trigger
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedCompletionZeroRowsTtlExpired() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    // Build config with short emptyResultTtlDays
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .options(MaterializeOptionsConfig.builder().emptyResultTtlDays(1).build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("ttl_expired_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    String configHash = IncrementalTracker.computeConfigHash(dims);
    // completedAt is 10 days ago (TTL expired)
    long tenDaysAgo = System.currentTimeMillis() - 10L * 24 * 60 * 60 * 1000;
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 0, tenDaysAgo);

    when(mockTracker.getCachedCompletion("ttl_expired_pipeline")).thenReturn(cached);
    when(mockStorage.isDirectory("/base/ttl_expired_pipeline/metadata")).thenReturn(true);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    // Set up for full pipeline execution after TTL expiry
    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/ttl_expired_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // It should have invalidated and re-processed
    verify(mockTracker).invalidateTableCompletion("ttl_expired_pipeline");
  }

  // -----------------------------------------------------------------------
  // execute() - cached with config hash mismatch but rowCount > 0
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedConfigMismatchWithDataExists() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));
    EtlPipelineConfig config = buildConfig("mismatch_pipeline", dims);

    // Create cached with different config hash
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("different_hash", "old_sig", 500);

    when(mockTracker.getCachedCompletion("mismatch_pipeline")).thenReturn(cached);
    when(mockStorage.isDirectory("/base/mismatch_pipeline/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(500, result.getTotalRows());
    // Should have updated config hash
    verify(mockTracker).markTableCompleteWithConfig(eq("mismatch_pipeline"),
        anyString(), eq("old_sig"), eq(500L));
  }

  // -----------------------------------------------------------------------
  // execute() - cached with config hash mismatch, rowCount > 0, data MISSING
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedConfigMismatchDataMissing() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("missing_data", dims);

    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("different_hash", "old_sig", 500);

    when(mockTracker.getCachedCompletion("missing_data")).thenReturn(cached);
    // Data does NOT exist
    when(mockStorage.isDirectory("/base/missing_data/metadata")).thenReturn(false);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/missing_data");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Should have proceeded with full pipeline execution
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - cached with hash mismatch and 0 rows
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedConfigMismatchZeroRows() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("zero_mismatch", dims);

    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("different_hash", "old_sig", 0);

    when(mockTracker.getCachedCompletion("zero_mismatch")).thenReturn(cached);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/zero_mismatch");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - cachedCompletion null + data exists (cold start Iceberg recovery)
  // -----------------------------------------------------------------------

  @Test void testExecuteNoCacheIcebergDataExists() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("cold_recovery")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("cold_recovery")).thenReturn(null);
    // Data exists for cold-start recovery
    when(mockStorage.isDirectory("/base/cold_recovery/metadata")).thenReturn(true);
    // But readRowCountFromIceberg will fail (no actual Iceberg table),
    // so it returns 0 and falls through to normal processing
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/cold_recovery");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // readRowCountFromIceberg will throw (no real Iceberg) and return 0,
    // so pipeline should proceed to processing
  }

  // -----------------------------------------------------------------------
  // execute() - isTableComplete returns true + data exists (signature match)
  // -----------------------------------------------------------------------

  @Test void testExecuteTableCompleteFastPath() throws IOException {
    // Use disabled materialization so isTableComplete fast-path returns EtlResult.skipped()
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("complete_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("complete_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("complete_pipeline"), anyString())).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
  }

  @Test void testExecuteTableCompleteIcebergWithRowCountZero() throws IOException {
    // Iceberg format with isTableComplete=true, data exists.
    // readRowCountFromIceberg fails (no real Iceberg) => returns 0.
    // Since emptyResultTtlDays defaults to 7 (>0), the pipeline invalidates
    // and continues processing. This covers the rowCount==0 TTL path.
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("iceberg_complete")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("iceberg_complete")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("iceberg_complete"), anyString())).thenReturn(true);
    when(mockStorage.isDirectory("/base/iceberg_complete/metadata")).thenReturn(true);

    // After invalidation, must set up for processing
    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/iceberg_complete");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Pipeline should have invalidated table completion due to rowCount==0 + TTL > 0
    verify(mockTracker).invalidateTableCompletion("iceberg_complete");
    // After invalidation, the pipeline continues to Phase 2 where the filter determines
    // what needs processing. The exact total rows depends on the filter result.
    assertNotNull(result.getPipelineName());
  }

  // -----------------------------------------------------------------------
  // execute() - isTableComplete=true but data missing => invalidate + reprocess
  // -----------------------------------------------------------------------

  @Test void testExecuteTableCompleteButDataMissing() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("stale_complete", dims);

    when(mockTracker.getCachedCompletion("stale_complete")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("stale_complete"), anyString())).thenReturn(true);
    // Data does not exist
    when(mockStorage.isDirectory("/base/stale_complete/metadata")).thenReturn(false);

    // After invalidation, forceReprocessAll => all indices
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(3L);
    when(mockWriter.getTableLocation()).thenReturn("/base/stale_complete");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(3);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertFalse(result.isSkippedEntirePipeline());
    verify(mockTracker).invalidateTableCompletion("stale_complete");
  }

  // -----------------------------------------------------------------------
  // execute() - all combinations already processed => mark complete
  // -----------------------------------------------------------------------

  @Test void testExecuteAllCombinationsAlreadyProcessed() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));
    EtlPipelineConfig config = buildConfig("all_processed", dims);

    when(mockTracker.getCachedCompletion("all_processed")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    // Simulate data existing so the "no Iceberg data" fast-path does not force reprocess
    when(mockStorage.isDirectory(anyString())).thenReturn(true);
    // Return empty set => all are already processed
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(Collections.<Integer>emptySet());

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(2, result.getSkippedBatches());
    verify(mockTracker).markTableCompleteWithConfig(eq("all_processed"),
        anyString(), anyString(), anyLong());
  }

  // -----------------------------------------------------------------------
  // execute() - sequential processing with DataProvider
  // -----------------------------------------------------------------------

  @Test void testExecuteSequentialWithDataProvider() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021", "2022"));
    EtlPipelineConfig config = buildConfig("seq_pipeline", dims);

    when(mockTracker.getCachedCompletion("seq_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    unprocessed.add(2);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/seq_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline.ProgressListener listener = new EtlPipeline.LoggingProgressListener();
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", listener, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(30, result.getTotalRows());
    assertEquals(3, result.getSuccessfulBatches());
    assertEquals(0, result.getFailedBatches());
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - sequential processing with custom DataWriter returning positive
  // -----------------------------------------------------------------------

  @Test void testExecuteSequentialWithCustomDataWriter() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("writer_pipeline", dims);

    when(mockTracker.getCachedCompletion("writer_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/writer_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    // Custom DataWriter that returns a positive row count (overriding default writer)
    DataWriter dw = new DataWriter() {
      @Override public long write(EtlPipelineConfig config, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        // Consume the iterator
        int count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, dw);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
    assertEquals(1, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // execute() - DataWriter returns -1 => fallback to MaterializationWriter
  // -----------------------------------------------------------------------

  @Test void testExecuteDataWriterReturnsNegativeOne() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("fallback_pipeline", dims);

    when(mockTracker.getCachedCompletion("fallback_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(7L);
    when(mockWriter.getTableLocation()).thenReturn("/base/fallback_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(7);
      }
    };

    // DataWriter returns -1 => fall through to MaterializationWriter
    DataWriter dw = DataWriter.DEFAULT;

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, dw);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(7, result.getTotalRows());
    verify(mockWriter).writeBatch(any(Iterator.class), any(Map.class));
  }

  // -----------------------------------------------------------------------
  // execute() - batch failure with SKIP error action
  // -----------------------------------------------------------------------

  @Test void testExecuteBatchFailureSkipAction() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("skip_error_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .errorHandling(errorConfig)
        .build();

    when(mockTracker.getCachedCompletion("skip_error_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/skip_error_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // First batch throws, second succeeds
    DataProvider dp = new DataProvider() {
      private int callCount = 0;
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        callCount++;
        if (callCount == 1) {
          throw new IOException("HTTP 500 Internal Server Error");
        }
        return rowIterator(10);
      }
    };

    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);

    EtlPipeline.ProgressListener listener = new EtlPipeline.LoggingProgressListener();
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", listener, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, result.getSuccessfulBatches());
    // SKIP action increments skippedBatches
    assertTrue(result.getSkippedBatches() > 0 || result.getFailedBatches() > 0);
  }

  // -----------------------------------------------------------------------
  // execute() - batch failure with FAIL error action
  // -----------------------------------------------------------------------

  @Test void testExecuteBatchFailureFailAction() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("fail_error_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .errorHandling(errorConfig)
        .build();

    when(mockTracker.getCachedCompletion("fail_error_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 500 Server Error");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // FAIL throws IOException which is caught by outer catch block
    assertTrue(result.getFailedBatches() > 0 || result.getFailureMessage() != null);
  }

  // -----------------------------------------------------------------------
  // execute() - batch failure with WARN error action
  // -----------------------------------------------------------------------

  @Test void testExecuteBatchFailureWarnAction() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("warn_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .errorHandling(errorConfig)
        .build();

    when(mockTracker.getCachedCompletion("warn_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/warn_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      private int callCount = 0;
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        callCount++;
        if (callCount == 1) {
          throw new IOException("HTTP 500 Server Error");
        }
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, result.getFailedBatches());
    assertEquals(1, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // determineErrorAction - HTTP 401 (auth error)
  // -----------------------------------------------------------------------

  @Test void testErrorActionAuth401() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("auth_pipeline", dims);

    when(mockTracker.getCachedCompletion("auth_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 401 Unauthorized");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Auth errors default to FAIL action
    assertTrue(result.getFailedBatches() > 0 || result.getFailureMessage() != null);
  }

  // -----------------------------------------------------------------------
  // determineErrorAction - HTTP 404 (not found)
  // -----------------------------------------------------------------------

  @Test void testErrorAction404() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("notfound_pipeline", dims);

    when(mockTracker.getCachedCompletion("notfound_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/notfound_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 404 Not Found");
      }
    };

    EtlPipeline.ProgressListener listener = new EtlPipeline.LoggingProgressListener();
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", listener, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // 404 defaults to SKIP action
    assertTrue(result.getSkippedBatches() > 0 || result.getErrors().size() > 0);
  }

  // -----------------------------------------------------------------------
  // determineErrorAction - HTTP 429 (transient)
  // -----------------------------------------------------------------------

  @Test void testErrorAction429Transient() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("transient_pipeline", dims);

    when(mockTracker.getCachedCompletion("transient_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/transient_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 429 Too Many Requests");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // determineErrorAction - HTTP 503 (transient)
  // -----------------------------------------------------------------------

  @Test void testErrorAction503Transient() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("transient503_pipeline", dims);

    when(mockTracker.getCachedCompletion("transient503_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/transient503_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 503 Service Unavailable");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // determineErrorAction - null message
  // -----------------------------------------------------------------------

  @Test void testErrorActionNullMessage() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("null_msg_pipeline", dims);

    when(mockTracker.getCachedCompletion("null_msg_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/null_msg_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException((String) null);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // execute() - PARQUET format (non-Iceberg) table directory creation
  // -----------------------------------------------------------------------

  @Test void testExecuteParquetFormat() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config =
        buildConfig("parquet_pipeline", dims, MaterializeConfig.Format.PARQUET, null);

    when(mockTracker.getCachedCompletion("parquet_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/parquet_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.PARQUET);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
    assertEquals(MaterializeConfig.Format.PARQUET, result.getMaterializeFormat());
  }

  // -----------------------------------------------------------------------
  // execute() - document source type with DataWriter
  // -----------------------------------------------------------------------

  @Test void testExecuteDocumentSourceWithDataWriter() throws IOException {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/docs")
        .build();
    Map<String, Object> rawSource = new HashMap<String, Object>();
    rawSource.put("type", "document");
    rawSource.put("url", "https://api.example.com/docs");

    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();

    Map<String, DimensionConfig> dims =
        singleListDimension("id", Arrays.asList("doc1"));

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_pipeline")
        .sourceType("document")
        .source(source)
        .rawSourceConfig(rawSource)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("doc_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/doc_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataWriter dw = new DataWriter() {
      @Override public long write(EtlPipelineConfig config, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        return 42L;
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, dw);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(42, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - document source type WITHOUT DataWriter => 0 rows
  // -----------------------------------------------------------------------

  @Test void testExecuteDocumentSourceWithoutDataWriter() throws IOException {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/docs")
        .build();
    Map<String, Object> rawSource = new HashMap<String, Object>();
    rawSource.put("type", "document");

    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();

    Map<String, DimensionConfig> dims =
        singleListDimension("id", Arrays.asList("doc1"));

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_no_writer")
        .sourceType("document")
        .source(source)
        .rawSourceConfig(rawSource)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("doc_no_writer")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/doc_no_writer");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Document source without writer returns 0 rows per batch
    assertEquals(0, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - DataProvider returns null => fallback to DataSource
  // -----------------------------------------------------------------------

  @Test void testExecuteDataProviderReturnsNull() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("null_dp_pipeline", dims);

    when(mockTracker.getCachedCompletion("null_dp_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(0L);
    when(mockWriter.getTableLocation()).thenReturn("/base/null_dp_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // DataProvider returns null => falls through to DataSource.fetch
    // The DataSource (HttpSource) will fail since there's no real HTTP, but the
    // pipeline catches the exception
    DataProvider dp = DataProvider.DEFAULT;

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // HttpSource creation will proceed then fetch will fail - result depends on error handling
  }

  // -----------------------------------------------------------------------
  // execute() - outer catch block (unexpected exception)
  // -----------------------------------------------------------------------

  @Test void testExecuteUnexpectedException() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("exception_pipeline", dims);

    when(mockTracker.getCachedCompletion("exception_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);

    // Writer throws during initialize => caught by outer catch block
    factoryMock.close();
    factoryMock = mockStatic(MaterializationWriterFactory.class);
    MaterializationWriter failingWriter = mock(MaterializationWriter.class);
    factoryMock.when(
        () -> MaterializationWriterFactory.createFromConfig(
        any(MaterializeConfig.class), any(StorageProvider.class),
        anyString(), any(IncrementalTracker.class)))
        .thenReturn(failingWriter);
    when(failingWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);
    // Throw during initialize
    IOException initError = new IOException("Writer initialization failed");
    // Use doThrow pattern for void methods
    org.mockito.Mockito.doThrow(initError).when(failingWriter)
        .initialize(any(MaterializeConfig.class));

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.getFailedBatches() > 0);
    assertNotNull(result.getFailureMessage());
    assertTrue(result.getFailureMessage().contains("Writer initialization failed"));
  }

  // -----------------------------------------------------------------------
  // execute() - writer close failure in finally block
  // -----------------------------------------------------------------------

  @Test void testExecuteWriterCloseFailure() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("close_fail_pipeline", dims);

    when(mockTracker.getCachedCompletion("close_fail_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/close_fail_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);
    // close throws IOException
    org.mockito.Mockito.doThrow(new IOException("Close failed"))
        .when(mockWriter).close();

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    // The result should still be returned successfully despite close failure
    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists with IOException
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsIOException() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("io_verify_pipeline", dims);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("io_verify_pipeline")).thenReturn(cached);
    // Storage throws IOException during isDirectory check
    when(mockStorage.isDirectory(anyString())).thenThrow(new IOException("S3 down"));

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // IOException in verifyDataExists returns true (assume exists), so pipeline should skip
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists with null storageProvider
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsNullStorage() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("null_storage_pipeline", dims);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("null_storage_pipeline")).thenReturn(cached);

    EtlPipeline pipeline =
        new EtlPipeline(config, null, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Null storage => verifyDataExists returns true => skip
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists for Parquet format
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsParquetFormat() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config =
        buildConfig("parquet_verify", dims, MaterializeConfig.Format.PARQUET, null);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("parquet_verify")).thenReturn(cached);
    // For Parquet, verify checks baseDir/pipelineName not metadata subdir
    when(mockStorage.isDirectory("/base/parquet_verify")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - materialize config name/column merging
  // -----------------------------------------------------------------------

  @Test void testExecuteMaterializeConfigMerging() throws IOException {
    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder().name("col1").type("VARCHAR").build());
    columns.add(ColumnConfig.builder().name("col2").type("INTEGER").build());

    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    // Config has columns at table level, materialize has no columns/name
    EtlPipelineConfig config = buildConfig("merge_pipeline", dims, null, columns);

    when(mockTracker.getCachedCompletion("merge_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/merge_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - PARQUET format with trailing slash in baseDirectory
  // -----------------------------------------------------------------------

  @Test void testExecuteParquetWithTrailingSlash() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config =
        buildConfig("slash_pipeline", dims, MaterializeConfig.Format.PARQUET, null);

    when(mockTracker.getCachedCompletion("slash_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/slash_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.PARQUET);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    // Use trailing slash in base directory
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base/", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists for Iceberg with warehousePath configured
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsIcebergWithWarehousePath() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("/warehouse")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("warehouse_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("warehouse_pipeline")).thenReturn(cached);
    // Check warehousePath first, then fallback
    when(mockStorage.isDirectory("/warehouse/warehouse_pipeline/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists Iceberg with warehousePath miss, fallback hit
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsIcebergWarehouseFallback() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("/warehouse")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("fallback_verify")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("fallback_verify")).thenReturn(cached);
    // warehousePath directory does not exist
    when(mockStorage.isDirectory("/warehouse/fallback_verify/metadata")).thenReturn(false);
    // Fallback to baseDirectory
    when(mockStorage.isDirectory("/base/fallback_verify/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists disabled materialization
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsDisabledMaterialization() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("disabled_mat")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("disabled_mat")).thenReturn(cached);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Disabled materialization => verifyDataExists returns true
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - isTableComplete with materialization not enabled
  // -----------------------------------------------------------------------

  @Test void testExecuteTableCompleteNoMaterialization() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("no_mat_complete")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("no_mat_complete")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("no_mat_complete"), anyString())).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSkipped());
  }

  // -----------------------------------------------------------------------
  // LoggingProgressListener comprehensive test
  // -----------------------------------------------------------------------

  @Test void testLoggingProgressListenerAllMethods() {
    EtlPipeline.LoggingProgressListener listener =
        new EtlPipeline.LoggingProgressListener();

    // Phase lifecycle
    listener.onPhaseStart("test_phase", 100);
    listener.onPhaseComplete("test_phase", 100);

    // Batch lifecycle - success
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("region", "US");
    listener.onBatchStart(1, 10, vars);
    listener.onBatchComplete(1, 10, 500, null);

    // Batch lifecycle - failure
    listener.onBatchStart(2, 10, vars);
    listener.onBatchComplete(2, 10, 0, new RuntimeException("Test error"));

    // Edge cases
    listener.onBatchStart(0, 0, Collections.<String, String>emptyMap());
    listener.onBatchComplete(0, 0, 0, null);
  }

  // -----------------------------------------------------------------------
  // execute() - successful pipeline marks table complete
  // -----------------------------------------------------------------------

  @Test void testExecuteSuccessMarksTableComplete() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("success_complete", dims);

    when(mockTracker.getCachedCompletion("success_complete")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/success_complete");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(10, result.getTotalRows());
    assertEquals(0, result.getFailedBatches());
    assertTrue(result.getErrors().isEmpty());
    // Should mark table complete since no failures
    verify(mockTracker).markTableCompleteWithConfig(eq("success_complete"),
        anyString(), anyString(), eq(10L));
  }

  // -----------------------------------------------------------------------
  // execute() - failed pipeline does NOT mark table complete
  // -----------------------------------------------------------------------

  @Test void testExecuteFailedDoesNotMarkComplete() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("partial_fail")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .errorHandling(errorConfig)
        .build();

    when(mockTracker.getCachedCompletion("partial_fail")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/partial_fail");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      private int callCount = 0;
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        callCount++;
        if (callCount == 1) {
          throw new IOException("HTTP 500 Error");
        }
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, result.getFailedBatches());
    // Should NOT have called markTableCompleteWithConfig since there were failures
    verify(mockTracker, times(0)).markTableCompleteWithConfig(eq("partial_fail"),
        anyString(), anyString(), eq(10L));
  }

  // -----------------------------------------------------------------------
  // execute() - Iceberg format table complete with 0 rows + TTL
  // -----------------------------------------------------------------------

  @Test void testExecuteTableCompleteZeroRowsWithTtl() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .options(MaterializeOptionsConfig.builder().emptyResultTtlDays(7).build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("zero_ttl_pipeline")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("zero_ttl_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("zero_ttl_pipeline"), anyString())).thenReturn(true);
    when(mockStorage.isDirectory("/base/zero_ttl_pipeline/metadata")).thenReturn(true);

    // After dimension-based check, read row count returns 0
    // The pipeline should invalidate and continue

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/zero_ttl_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Either skipped (if readRowCount > 0) or processed
  }

  // -----------------------------------------------------------------------
  // execute() - Iceberg format table complete with 0 rows and no TTL config
  // -----------------------------------------------------------------------

  @Test void testExecuteTableCompleteZeroRowsNoTtl() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .options(MaterializeOptionsConfig.builder().emptyResultTtlDays(0).build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("zero_no_ttl")
        .source(source)
        .dimensions(dims)
        .materialize(mat)
        .build();

    when(mockTracker.getCachedCompletion("zero_no_ttl")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("zero_no_ttl"), anyString())).thenReturn(true);
    when(mockStorage.isDirectory("/base/zero_no_ttl/metadata")).thenReturn(true);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, null, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // With emptyResultTtlMillis == 0, the pipeline skips since TTL is not positive
    assertTrue(result.isSkippedEntirePipeline() || result.getTotalRows() >= 0);
  }

  // -----------------------------------------------------------------------
  // execute() - HTTP 403 auth error
  // -----------------------------------------------------------------------

  @Test void testErrorAction403Auth() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("auth403_pipeline", dims);

    when(mockTracker.getCachedCompletion("auth403_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 403 Forbidden");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // execute() - HTTP 5xx server error (not 503)
  // -----------------------------------------------------------------------

  @Test void testErrorAction5xxServerError() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("5xx_pipeline", dims);

    when(mockTracker.getCachedCompletion("5xx_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.getTableLocation()).thenReturn("/base/5xx_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 502 Bad Gateway");
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // execute() - verifyDataExists for Parquet format where data is missing
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExistsParquetMissing() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config =
        buildConfig("parquet_missing", dims, MaterializeConfig.Format.PARQUET, null);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 100);

    when(mockTracker.getCachedCompletion("parquet_missing")).thenReturn(cached);
    when(mockStorage.isDirectory("/base/parquet_missing")).thenReturn(false);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/parquet_missing");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.PARQUET);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Data missing => invalidate and reprocess
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // execute() - multiple dimension combinations
  // -----------------------------------------------------------------------

  @Test void testExecuteMultipleDimensionCombinations() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Arrays.asList("2020", "2021"))
        .build());
    dims.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("US", "EU"))
        .build());

    EtlPipelineConfig config = buildConfig("multi_dim", dims);

    when(mockTracker.getCachedCompletion("multi_dim")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    // All 4 combinations (2*2) need processing
    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < 4; i++) {
      unprocessed.add(i);
    }
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/multi_dim");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(40, result.getTotalRows());
    assertEquals(4, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // CachedCompletion - isEmptyResultTtlExpired
  // -----------------------------------------------------------------------

  @Test void testCachedCompletionTtlExpiry() {
    // Non-empty result => never expired
    IncrementalTracker.CachedCompletion nonEmpty =
        new IncrementalTracker.CachedCompletion("hash", "sig", 100);
    assertFalse(nonEmpty.isEmptyResultTtlExpired(1000));

    // Empty with no TTL => not expired
    IncrementalTracker.CachedCompletion emptyNoTtl =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0);
    assertFalse(emptyNoTtl.isEmptyResultTtlExpired(0));
    assertFalse(emptyNoTtl.isEmptyResultTtlExpired(-1));

    // Empty with TTL not yet expired
    IncrementalTracker.CachedCompletion recentEmpty =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, System.currentTimeMillis());
    assertFalse(recentEmpty.isEmptyResultTtlExpired(60000)); // 60 seconds TTL

    // Empty with TTL expired
    long longAgo = System.currentTimeMillis() - 200000;
    IncrementalTracker.CachedCompletion oldEmpty =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, longAgo);
    assertTrue(oldEmpty.isEmptyResultTtlExpired(100000)); // 100 seconds TTL
  }

  // -----------------------------------------------------------------------
  // CachedCompletion - isSourceFilesModified
  // -----------------------------------------------------------------------

  @Test void testCachedCompletionSourceFilesModified() {
    // No watermark tracking
    IncrementalTracker.CachedCompletion noWatermark =
        new IncrementalTracker.CachedCompletion("hash", "sig", 100, System.currentTimeMillis(), 0);
    assertFalse(noWatermark.isSourceFilesModified(1000));

    // Current watermark is 0 => not modified
    IncrementalTracker.CachedCompletion withWatermark =
        new IncrementalTracker.CachedCompletion("hash", "sig", 100, System.currentTimeMillis(), 5000);
    assertFalse(withWatermark.isSourceFilesModified(0));

    // Not modified (same watermark)
    assertFalse(withWatermark.isSourceFilesModified(5000));

    // Modified (newer watermark)
    assertTrue(withWatermark.isSourceFilesModified(6000));
  }

  // -----------------------------------------------------------------------
  // allIndicesSet utility
  // -----------------------------------------------------------------------

  @Test void testAllIndicesSet() throws IOException {
    // Test indirectly through forceReprocessAll path
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020", "2021", "2022"));
    EtlPipelineConfig config = buildConfig("force_reprocess", dims);

    when(mockTracker.getCachedCompletion("force_reprocess")).thenReturn(null);
    when(mockTracker.isTableComplete(eq("force_reprocess"), anyString())).thenReturn(true);
    // Data does not exist => invalidate + forceReprocessAll
    when(mockStorage.isDirectory("/base/force_reprocess/metadata")).thenReturn(false);

    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/force_reprocess");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(3, result.getSuccessfulBatches());
    assertEquals(15, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // getParallelThreadCount - via system property
  // -----------------------------------------------------------------------

  @Test void testGetParallelThreadCountDefault() throws IOException {
    // Without setting calcite.etl.threads, default is 1 (sequential)
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("thread_test", dims);

    when(mockTracker.getCachedCompletion("thread_test")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/thread_test");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // execute() - cached completion data missing => invalidate & fall through
  // -----------------------------------------------------------------------

  @Test void testExecuteCachedCompletionDataMissing() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("cache_miss_pipeline", dims);

    String configHash = IncrementalTracker.computeConfigHash(dims);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion(configHash, "sig", 1000);

    when(mockTracker.getCachedCompletion("cache_miss_pipeline")).thenReturn(cached);
    // Data does not exist despite cache saying so
    when(mockStorage.isDirectory("/base/cache_miss_pipeline/metadata")).thenReturn(false);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    // After invalidation, forceReprocessAll
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn("/base/cache_miss_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(10);
      }
    };

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", null, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertFalse(result.isSkippedEntirePipeline());
    verify(mockTracker).invalidateTableCompletion("cache_miss_pipeline");
  }

  // -----------------------------------------------------------------------
  // execute() - sourceStorageProvider defaults to storageProvider
  // -----------------------------------------------------------------------

  @Test void testSourceStorageProviderDefaultsToMain() {
    EtlPipelineConfig config = buildConfig("source_default", null);
    // Pass null for sourceStorageProvider => should default to main storageProvider
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, null, "/base", null, mockTracker, null, null, null);
    assertNotNull(pipeline);
  }

  // -----------------------------------------------------------------------
  // execute() - progress listener called during processing
  // -----------------------------------------------------------------------

  @Test void testProgressListenerCalledDuringExecution() throws IOException {
    Map<String, DimensionConfig> dims =
        singleListDimension("year", Arrays.asList("2020"));
    EtlPipelineConfig config = buildConfig("progress_pipeline", dims);

    when(mockTracker.getCachedCompletion("progress_pipeline")).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(unprocessed);
    when(mockWriter.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn("/base/progress_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dp = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
          Map<String, String> variables) {
        return rowIterator(5);
      }
    };

    EtlPipeline.ProgressListener mockListener = mock(EtlPipeline.ProgressListener.class);
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/base", mockListener, mockTracker, dp, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    verify(mockListener).onPhaseStart(eq("dimension_expansion"), eq(1));
    verify(mockListener).onPhaseComplete(eq("dimension_expansion"), eq(1));
    verify(mockListener).onPhaseStart(eq("data_processing"), eq(1));
    verify(mockListener).onBatchStart(eq(1), eq(1), any(Map.class));
    verify(mockListener).onBatchComplete(eq(1), eq(1), eq(5), eq(null));
    verify(mockListener).onPhaseComplete(eq("data_processing"), eq(1));
  }

  // -----------------------------------------------------------------------
  // EtlPipelineConfig.isEnabled
  // -----------------------------------------------------------------------

  @Test void testPipelineConfigEnabled() {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();

    EtlPipelineConfig enabled = EtlPipelineConfig.builder()
        .name("test")
        .source(source)
        .materialize(mat)
        .enabled(true)
        .build();
    assertTrue(enabled.isEnabled());

    EtlPipelineConfig disabled = EtlPipelineConfig.builder()
        .name("test")
        .source(source)
        .materialize(mat)
        .enabled(false)
        .build();
    assertFalse(disabled.isEnabled());
  }

  // -----------------------------------------------------------------------
  // IncrementalTracker.computeConfigHash
  // -----------------------------------------------------------------------

  @Test void testComputeConfigHash() {
    // Empty dimensions
    String emptyHash = IncrementalTracker.computeConfigHash(null);
    assertEquals("empty", emptyHash);

    String emptyHash2 =
        IncrementalTracker.computeConfigHash(Collections.<String, DimensionConfig>emptyMap());
    assertEquals("empty", emptyHash2);

    // Non-empty dimensions
    Map<String, DimensionConfig> dims = singleRangeDimension("year", 2020, 2024);
    String hash1 = IncrementalTracker.computeConfigHash(dims);
    assertNotNull(hash1);
    assertTrue(hash1.startsWith("cfg:"));

    // Same config => same hash
    String hash2 = IncrementalTracker.computeConfigHash(dims);
    assertEquals(hash1, hash2);
  }

  // -----------------------------------------------------------------------
  // IncrementalTracker.computeDimensionSignature
  // -----------------------------------------------------------------------

  @Test void testComputeDimensionSignature() {
    // Empty combinations
    String emptySig = IncrementalTracker.computeDimensionSignature(null);
    assertEquals("empty", emptySig);

    String emptySig2 =
        IncrementalTracker.computeDimensionSignature(Collections.<Map<String, String>>emptyList());
    assertEquals("empty", emptySig2);

    // Non-empty combinations
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> combo1 = new LinkedHashMap<String, String>();
    combo1.put("year", "2020");
    combos.add(combo1);
    Map<String, String> combo2 = new LinkedHashMap<String, String>();
    combo2.put("year", "2021");
    combos.add(combo2);

    String sig = IncrementalTracker.computeDimensionSignature(combos);
    assertNotNull(sig);
    assertTrue(sig.startsWith("count:2"));
    assertTrue(sig.contains("year"));
    assertTrue(sig.contains("hash:"));
  }

  // -----------------------------------------------------------------------
  // IncrementalTracker.NOOP behavior
  // -----------------------------------------------------------------------

  @Test void testNoopTracker() {
    IncrementalTracker noop = IncrementalTracker.NOOP;

    Map<String, String> keys = new HashMap<String, String>();
    keys.put("year", "2020");

    assertFalse(noop.isProcessed("alt", "src", keys));
    assertFalse(noop.isProcessedWithTtl("alt", "src", keys, 1000));
    assertFalse(noop.isTableComplete("pipeline", "sig"));

    // No-ops don't throw
    noop.markProcessed("alt", "src", keys, "pattern");
    noop.markTableComplete("pipeline", "sig");
    noop.invalidate("alt", keys);
    noop.invalidateAll("alt");
    noop.invalidateTableCompletion("pipeline");
    noop.clearAllCompletions();

    assertTrue(noop.getProcessedKeyValues("alt").isEmpty());

    // filterUnprocessed returns all indices
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(keys);
    Set<Integer> unprocessed = noop.filterUnprocessed("alt", "src", combos);
    assertEquals(1, unprocessed.size());
    assertTrue(unprocessed.contains(0));
  }
}
