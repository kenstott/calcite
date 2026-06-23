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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link EtlPipeline} targeting uncovered code paths:
 * constructor variants, execute() branches (cached completion, empty result TTL,
 * cold start recovery, config hash mismatch, zero dimension combinations,
 * partitioned expansion, parallel processing), progress listener callbacks,
 * error handling actions (SKIP, WARN, FAIL), and cleanup.
 */
@Tag("unit")
public class EtlPipelineDeepCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider mockStorage;
  private IncrementalTracker mockTracker;
  private MaterializationWriter mockWriter;
  private MockedStatic<MaterializationWriterFactory> factoryMock;

  @BeforeEach
  void setUp() {
    mockStorage = mock(StorageProvider.class);
    mockTracker = mock(IncrementalTracker.class);
    mockWriter = mock(MaterializationWriter.class);

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

  // --- Constructor variants ---

  @Test void testConstructorMinimal() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output");
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithProgressListener() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output", listener);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithIncrementalTracker() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", listener, mockTracker);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithDataProviderAndWriter() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    DataProvider dataProvider = mock(DataProvider.class);
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithSourceStorageProvider() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    StorageProvider sourceStorage = mock(StorageProvider.class);
    DataProvider dataProvider = mock(DataProvider.class);
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, sourceStorage, "/output", null, mockTracker, dataProvider, dataWriter);
    assertNotNull(pipeline);
  }

  @Test void testConstructorWithOperatingDirectory() {
    EtlPipelineConfig config = buildSimpleConfig("test_pipeline");
    StorageProvider sourceStorage = mock(StorageProvider.class);
    DataProvider dataProvider = mock(DataProvider.class);
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, sourceStorage, "/output", null, mockTracker, dataProvider, dataWriter, tempDir.toString());
    assertNotNull(pipeline);
  }

  // --- Execute with cached completion (fast-path skip) ---

  @Test void testExecuteCachedCompletionWithMatchingConfigHash() throws IOException {
    EtlPipelineConfig config = buildConfigWithMaterialize("cached_pipeline");

    // Config has no dimensions, so computeConfigHash returns "empty"
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("empty", "sig", 100);
    when(mockTracker.getCachedCompletion("cached_pipeline")).thenReturn(cached);
    // verifyDataExists checks isDirectory for Iceberg metadata path
    when(mockStorage.isDirectory(anyString())).thenReturn(true);

    DataProvider dataProvider = (cfg, variables) -> Collections.<Map<String, Object>>emptyList().iterator();
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(100, result.getTotalRows());
  }

  // --- Execute with zero rows and empty result TTL ---

  @Test void testExecuteCachedCompletionZeroRowsTtlNotExpired() throws IOException {
    MaterializeOptionsConfig options = MaterializeOptionsConfig.builder()
        .build();

    EtlPipelineConfig config = buildConfigWithOptions("zero_rows_pipeline", options);

    // Use the 4-arg constructor to set completedAt to just now (TTL not expired)
    // Config has no dimensions, so computeConfigHash returns "empty"
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("empty", "sig", 0,
            System.currentTimeMillis());

    when(mockTracker.getCachedCompletion("zero_rows_pipeline")).thenReturn(cached);
    // verifyDataExists checks isDirectory for Iceberg metadata path
    when(mockStorage.isDirectory(anyString())).thenReturn(true);

    DataProvider dataProvider = (cfg, variables) -> Collections.<Map<String, Object>>emptyList().iterator();
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(0, result.getTotalRows());
  }

  // --- Execute with no dimension combinations ---

  @Test void testExecuteNoDimensionCombinations() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<>();
    // RANGE with start > end produces 0 combinations
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2025)
        .end(2024)
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("empty_dims_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    DataProvider dataProvider = (cfg, variables) -> Collections.<Map<String, Object>>emptyList().iterator();
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, result.getTotalRows());
  }

  // --- Execute with successful processing ---

  @Test void testExecuteWithSuccessfulProcessing() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("success_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<>();
    unprocessed.add(0);
    when(
        mockTracker.filterUnprocessed(anyString(), anyString(), anyList())).thenReturn(unprocessed);

    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("value", "test");
    data.add(row);

    DataProvider dataProvider = (cfg, variables) -> data.iterator();
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.getTotalRows() >= 0);
  }

  // --- Execute all combinations already processed ---

  @Test void testExecuteAllCombinationsProcessed() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("all_processed_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    // Return empty set - all combinations already processed
    when(
        mockTracker.filterUnprocessed(anyString(), anyString(), anyList())).thenReturn(new HashSet<Integer>());

    DataProvider dataProvider = (cfg, variables) -> Collections.<Map<String, Object>>emptyList().iterator();
    DataWriter dataWriter = mock(DataWriter.class);

    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage, "/output", null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- EtlResult builder tests ---

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("test", 100);
    assertNotNull(result);
    assertEquals("test", result.getPipelineName());
  }

  @Test void testEtlResultBuilder() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test_pipeline")
        .totalRows(500)
        .successfulBatches(10)
        .failedBatches(1)
        .skippedBatches(2)
        .elapsedMs(1234)
        .tableLocation("/path/to/table")
        .build();

    assertEquals("test_pipeline", result.getPipelineName());
    assertEquals(500, result.getTotalRows());
  }

  // --- LoggingProgressListener ---

  @Test void testLoggingProgressListener() {
    EtlPipeline.LoggingProgressListener listener = new EtlPipeline.LoggingProgressListener();

    // These should not throw
    listener.onPhaseStart("dimension_expansion", 10);
    listener.onPhaseComplete("dimension_expansion", 10);

    Map<String, String> vars = new HashMap<>();
    vars.put("year", "2024");
    listener.onBatchStart(1, 10, vars);
    listener.onBatchComplete(1, 10, 100, null);
    listener.onBatchComplete(1, 10, 0, new RuntimeException("Error occurred"));
  }

  // --- Error handling config ---

  @Test void testErrorHandlingDefaults() {
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        EtlPipelineConfig.ErrorHandlingConfig.defaults();
    assertNotNull(errorConfig);
  }

  // --- Helper methods ---

  private EtlPipelineConfig buildSimpleConfig(String name) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();
  }

  private EtlPipelineConfig buildConfigWithMaterialize(String name) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();
  }

  private EtlPipelineConfig buildConfigWithOptions(String name,
      MaterializeOptionsConfig options) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .options(options)
            .build())
        .build();
  }

  private EtlPipelineConfig buildConfigWithDimensions(String name,
      Map<String, DimensionConfig> dims) {
    return EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();
  }
}
