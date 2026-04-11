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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Line coverage tests for {@link EtlPipeline} targeting uncovered code paths:
 * <ul>
 *   <li>Parallel execution paths (parallelThreads > 1 with ExecutorService threading)</li>
 *   <li>DataWriter integration paths (custom DataWriter returns rows or -1)</li>
 *   <li>Error handling (SKIP, WARN, FAIL actions via determineErrorAction)</li>
 *   <li>Progress listener callbacks (onPhaseStart, onPhaseComplete, onBatchStart, onBatchComplete)</li>
 *   <li>Document source type processing</li>
 *   <li>DataProvider custom fetch</li>
 *   <li>Consecutive failure abort path</li>
 *   <li>Config hash mismatch with 0 rows path</li>
 *   <li>Empty result TTL expired path</li>
 *   <li>getParallelThreadCount via system property</li>
 * </ul>
 */
@Tag("unit")
public class EtlPipelineLineCoverageTest {

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
    factoryMock.when(() -> MaterializationWriterFactory.createFromConfig(
        any(MaterializeConfig.class), any(StorageProvider.class),
        anyString(), any(IncrementalTracker.class)))
        .thenReturn(mockWriter);
  }

  @AfterEach
  void tearDown() {
    if (factoryMock != null) {
      factoryMock.close();
    }
    // Clean up system property
    System.clearProperty("calcite.etl.threads");
    System.clearProperty("calcite.etl.maxConsecutiveFailures");
  }

  // --- Parallel execution with system property ---

  @Test
  void testParallelExecutionViaSystemProperty() throws IOException {
    System.setProperty("calcite.etl.threads", "2");

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(java.util.Arrays.asList("2023", "2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("parallel_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    // Mock writer to return some rows
    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(5L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/parallel_pipeline");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // Provide data via DataProvider
    final AtomicInteger fetchCount = new AtomicInteger(0);
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        fetchCount.incrementAndGet();
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        row.put("value", "test");
        rows.add(row);
        return rows.iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(fetchCount.get() >= 2, "Both batches should have been fetched");
  }

  // --- Parallel execution with HttpSource parallel config ---

  @Test
  void testParallelExecutionViaHttpSourceConfig() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(java.util.Arrays.asList("2023", "2024", "2025"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("parallel_http_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data?year=${year}")
            .parallel(3)
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    unprocessed.add(2);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(10L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/parallel_http");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("data", variables.get("year"));
        rows.add(row);
        return rows.iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- DataWriter integration: custom writer returns positive rows ---

  @Test
  void testCustomDataWriterReturnsPositiveRows() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("custom_writer_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/custom_writer");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    // Custom DataWriter that returns positive value (bypasses MaterializationWriter)
    DataWriter dataWriter = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig cfg, Iterator<Map<String, Object>> data,
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

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(1, result.getTotalRows());
  }

  // --- DataWriter returns -1 (falls back to MaterializationWriter) ---

  @Test
  void testCustomDataWriterReturnsNegativeOne() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("fallback_writer_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(7L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/fallback");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    // Returns -1 to indicate fallback to default writer
    DataWriter dataWriter = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig cfg, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        return -1;
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(7, result.getTotalRows());
  }

  // --- Progress listener callbacks ---

  @Test
  void testProgressListenerCallbacks() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("listener_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(3L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/listener");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        listener, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);

    // Verify progress listener was called
    verify(listener).onPhaseStart(eq("dimension_expansion"), anyInt());
    verify(listener).onPhaseComplete(eq("dimension_expansion"), anyInt());
    verify(listener).onPhaseStart(eq("data_processing"), anyInt());
    verify(listener).onBatchStart(anyInt(), anyInt(), anyMap());
    verify(listener).onBatchComplete(anyInt(), anyInt(), anyInt(), isNull());
    verify(listener).onPhaseComplete(eq("data_processing"), anyInt());
  }

  // --- Error handling: SKIP action ---

  @Test
  void testErrorHandlingSkipAction() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("skip_error_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .notFoundAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/skip");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // DataProvider that throws HTTP 404
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 404 Not Found");
      }
    };

    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        listener, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Batch was skipped due to 404 SKIP action
    assertTrue(result.getSkippedBatches() > 0 || result.getErrors().size() > 0);

    // Verify error callback
    verify(listener).onBatchComplete(anyInt(), anyInt(), eq(0), any(IOException.class));
  }

  // --- Error handling: WARN action ---

  @Test
  void testErrorHandlingWarnAction() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("warn_error_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/warn");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // DataProvider that throws a generic API error (HTTP 500)
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 500 Internal Server Error");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.getFailedBatches() > 0, "Should have failed batches for WARN action");
  }

  // --- Error handling: FAIL action ---

  @Test
  void testErrorHandlingFailAction() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("fail_error_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/fail");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // DataProvider that throws HTTP 401 (auth error)
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 401 Unauthorized");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    // Pipeline should catch the IOException from FAIL action and return failed result
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isFailed() || result.getFailedBatches() > 0);
  }

  // --- Error handling: various HTTP status codes ---

  @Test
  void testDetermineErrorActionHTTP429() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("http429_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/429");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 429 Too Many Requests");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test
  void testDetermineErrorActionHTTP403() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("http403_pipeline")
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

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/403");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 403 Forbidden");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test
  void testDetermineErrorActionNullMessage() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("nullmsg_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/nullmsg");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // IOException with null message
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException((String) null);
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- Document source type ---

  @Test
  void testDocumentSourceTypeWithDataWriter() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("ticker", DimensionConfig.builder()
        .name("ticker")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("AAPL"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("document_pipeline")
        .sourceType("document")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/doc")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(0L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/doc");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    DataWriter docWriter = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig cfg, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        return 42;
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, null, docWriter);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(42, result.getTotalRows());
  }

  @Test
  void testDocumentSourceTypeWithoutDataWriter() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("ticker", DimensionConfig.builder()
        .name("ticker")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("AAPL"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_no_writer_pipeline")
        .sourceType("document")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/doc")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(0L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/doc_none");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // No DataWriter - should log warning and return 0 rows
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, null, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, result.getTotalRows());
  }

  // --- Config hash mismatch with 0 rows ---

  @Test
  void testCachedCompletionConfigHashMismatchZeroRows() throws IOException {
    EtlPipelineConfig config = buildConfigWithMaterialize("mismatch_zero_pipeline");

    // Cached completion with different config hash and 0 rows
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("different_hash", "sig", 0);

    when(mockTracker.getCachedCompletion("mismatch_zero_pipeline")).thenReturn(cached);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    // Pipeline has no dimensions so 0 combinations
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- Empty result TTL expired ---

  @Test
  void testCachedCompletionEmptyResultTtlExpired() throws IOException {
    MaterializeOptionsConfig options = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(1)
        .build();

    EtlPipelineConfig config = buildConfigWithOptions("ttl_expired_pipeline", options);

    // Set completedAt to well in the past so TTL is expired
    long completedAt = System.currentTimeMillis() - (2L * 24L * 60L * 60L * 1000L);
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("empty", "sig", 0, completedAt);

    when(mockTracker.getCachedCompletion("ttl_expired_pipeline")).thenReturn(cached);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    when(mockStorage.isDirectory(anyString())).thenReturn(true);

    // Empty dimensions -> 0 combinations
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- Cached completion data doesn't exist (invalidate path) ---

  @Test
  void testCachedCompletionDataDoesNotExist() throws IOException {
    EtlPipelineConfig config = buildConfigWithMaterialize("no_data_pipeline");

    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("empty", "sig", 100);

    when(mockTracker.getCachedCompletion("no_data_pipeline")).thenReturn(cached);
    // verifyDataExists returns false
    when(mockStorage.isDirectory(anyString())).thenReturn(false);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Should have invalidated completion
    verify(mockTracker).invalidateTableCompletion("no_data_pipeline");
  }

  // --- Consecutive failure abort ---

  @Test
  void testConsecutiveFailureAbort() throws IOException {
    System.setProperty("calcite.etl.maxConsecutiveFailures", "1");

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(java.util.Arrays.asList("2023", "2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("abort_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/abort");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // Always fail
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("Connection refused");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    // Expect pipeline to abort after 1 consecutive failure
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isFailed() || result.getFailedBatches() > 0);
  }

  // --- Parallel execution error handling ---

  @Test
  void testParallelExecutionWithErrors() throws IOException {
    System.setProperty("calcite.etl.threads", "2");

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(java.util.Arrays.asList("2023", "2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("parallel_error_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    unprocessed.add(1);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/par_err");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);

    // All fetches fail
    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 503 Service Unavailable");
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.getErrors().size() > 0);
  }

  // --- Static factory method ---

  @Test
  void testCreateStaticFactory() {
    EtlPipelineConfig config = buildSimpleConfig("factory_test");
    EtlPipeline pipeline = EtlPipeline.create(config, mockStorage, "/output");
    assertNotNull(pipeline);
  }

  // --- Source storage provider defaults to main when null ---

  @Test
  void testSourceStorageProviderDefaultsToMain() {
    EtlPipelineConfig config = buildSimpleConfig("source_default");
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, null, "/output",
        null, mockTracker, null, null);
    assertNotNull(pipeline);
  }

  // --- Writer close error handling ---

  @Test
  void testWriterCloseErrorIsHandled() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = buildConfigWithDimensions("close_error_pipeline", dims);

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(1L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/close_err");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.ICEBERG);
    doThrow(new IOException("Close failed")).when(mockWriter).close();

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    // Should not throw despite writer close error
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // --- LoggingProgressListener ---

  @Test
  void testLoggingProgressListenerAllMethods() {
    EtlPipeline.LoggingProgressListener listener = new EtlPipeline.LoggingProgressListener();

    listener.onPhaseStart("dimension_expansion", 10);
    listener.onPhaseComplete("dimension_expansion", 10);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    listener.onBatchStart(1, 10, vars);
    listener.onBatchComplete(1, 10, 100, null);
    listener.onBatchComplete(1, 10, 0, new RuntimeException("test error"));
  }

  // --- Config hash mismatch with rows > 0 and data exists ---

  @Test
  void testCachedCompletionConfigHashMismatchWithRowsDataExists() throws IOException {
    EtlPipelineConfig config = buildConfigWithMaterialize("mismatch_rows_pipeline");

    // Cached with different hash but has rows
    IncrementalTracker.CachedCompletion cached =
        new IncrementalTracker.CachedCompletion("different_hash", "sig", 50);

    when(mockTracker.getCachedCompletion("mismatch_rows_pipeline")).thenReturn(cached);
    when(mockStorage.isDirectory(anyString())).thenReturn(true);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(50, result.getTotalRows());
    // Should have updated config hash
    verify(mockTracker).markTableCompleteWithConfig(
        eq("mismatch_rows_pipeline"), anyString(), eq("sig"), eq(50L));
  }

  // --- Parquet format table directory path ---

  @Test
  void testParquetFormatTableDirectoryPath() throws IOException {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("parquet_dir_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    when(mockTracker.getCachedCompletion(anyString())).thenReturn(null);
    when(mockTracker.isTableComplete(anyString(), anyString())).thenReturn(false);

    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(0);
    when(mockTracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        anyList(), anyLong())).thenReturn(unprocessed);

    when(mockWriter.writeBatch(any(Iterator.class), anyMap())).thenReturn(1L);
    when(mockWriter.getTableLocation()).thenReturn(tempDir.toString() + "/parquet_dir");
    when(mockWriter.getFormat()).thenReturn(MaterializeConfig.Format.PARQUET);

    DataProvider dataProvider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(EtlPipelineConfig cfg,
          Map<String, String> variables) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    EtlPipeline pipeline = new EtlPipeline(config, mockStorage, "/output",
        null, mockTracker, dataProvider, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
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
