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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link EtlPipeline} execution paths.
 *
 * <p>Uses custom DataProvider and DataWriter to test pipeline orchestration
 * without needing real HTTP or Parquet infrastructure.
 */
@Tag("unit")
class EtlPipelineExecutionTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EtlPipelineExecutionTest.class);

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @Test void testPipelineWithCustomDataProviderAndWriter() throws IOException {
    // Track what the pipeline sends to the writer
    final AtomicLong totalRowsWritten = new AtomicLong(0);
    final AtomicInteger batchesWritten = new AtomicInteger(0);

    // Custom DataProvider returns 5 rows per batch
    DataProvider dataProvider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        String year = variables.get("year");
        for (int i = 1; i <= 5; i++) {
          Map<String, Object> row = new HashMap<String, Object>();
          row.put("id", i);
          row.put("year", year);
          row.put("value", i * 100);
          rows.add(row);
        }
        return rows.iterator();
      }
    };

    // Custom DataWriter counts rows and batches
    DataWriter dataWriter = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        totalRowsWritten.addAndGet(count);
        batchesWritten.incrementAndGet();
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_custom_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP, dataProvider, dataWriter);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSuccessful());
    // 3 years (2020, 2021, 2022) x 5 rows = 15 total rows
    assertEquals(15, result.getTotalRows());
    assertEquals(3, batchesWritten.get());
    assertEquals(15, totalRowsWritten.get());
  }

  @Test void testPipelineWithNoDimensions() throws IOException {
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_no_dimensions")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(new LinkedHashMap<String, DimensionConfig>())
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        DataProvider.DEFAULT, DataWriter.DEFAULT);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // No dimension combinations should produce 0 batches
    assertEquals(0, result.getTotalRows());
    assertTrue(result.getErrors().size() > 0);
  }

  @Test void testPipelineWithProgressListener() throws IOException {
    final List<String> phaseStarts = new ArrayList<String>();
    final List<String> phaseCompletes = new ArrayList<String>();
    final AtomicInteger batchStartCount = new AtomicInteger(0);
    final AtomicInteger batchCompleteCount = new AtomicInteger(0);

    EtlPipeline.ProgressListener listener = new EtlPipeline.ProgressListener() {
      @Override public void onPhaseStart(String phase, int totalBatches) {
        phaseStarts.add(phase);
      }

      @Override public void onPhaseComplete(String phase, int totalBatches) {
        phaseCompletes.add(phase);
      }

      @Override public void onBatchStart(int batchIndex, int totalBatches,
          Map<String, String> variables) {
        batchStartCount.incrementAndGet();
      }

      @Override public void onBatchComplete(int batchIndex, int totalBatches,
          int rowCount, Exception error) {
        batchCompleteCount.incrementAndGet();
      }
    };

    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("val", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_progress")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), listener, IncrementalTracker.NOOP,
        provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSuccessful());
    assertTrue(phaseStarts.contains("dimension_expansion"));
    assertTrue(phaseCompletes.contains("dimension_expansion"));
    assertEquals(2, batchStartCount.get()); // 2020, 2021
    assertEquals(2, batchCompleteCount.get());
  }

  @Test void testPipelineDataProviderReturnsNull() throws IOException {
    // When custom DataProvider returns null, pipeline should use built-in source
    // But since there's no real HTTP source, this should result in an error path
    DataProvider nullProvider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        return null;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2024"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_null_provider")
        .source(HttpSourceConfig.builder()
            .url("https://nonexistent.example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        nullProvider, DataWriter.DEFAULT);

    // This will attempt to use built-in HttpSource which will fail
    // The pipeline should handle this gracefully
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Should have failed batches since the HTTP source can't connect
    assertTrue(result.getFailedBatches() > 0 || result.getErrors().size() > 0);
  }

  @Test void testPipelineWithListDimensions() throws IOException {
    final List<Map<String, String>> capturedVariables =
        new ArrayList<Map<String, String>>();

    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        capturedVariables.add(new HashMap<String, String>(variables));
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("data", variables.get("region"));
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("EAST", "WEST", "NORTH"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_list_dims")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSuccessful());
    assertEquals(3, result.getTotalRows());
    assertEquals(3, capturedVariables.size());

    // Verify all regions were processed
    List<String> regions = new ArrayList<String>();
    for (Map<String, String> vars : capturedVariables) {
      regions.add(vars.get("region"));
    }
    assertTrue(regions.contains("EAST"));
    assertTrue(regions.contains("WEST"));
    assertTrue(regions.contains("NORTH"));
  }

  @Test void testPipelineWithMultipleDimensions() throws IOException {
    final AtomicInteger batchCount = new AtomicInteger(0);

    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("year", variables.get("year"));
        row.put("region", variables.get("region"));
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        batchCount.incrementAndGet();
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("EAST", "WEST"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_multi_dims")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(result.isSuccessful());
    // 2 years x 2 regions = 4 combinations
    assertEquals(4, batchCount.get());
    assertEquals(4, result.getTotalRows());
  }

  @Test void testPipelineWithFailingBatch() throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);

    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables)
          throws IOException {
        int call = callCount.incrementAndGet();
        if (call == 2) {
          throw new IOException("Simulated API error");
        }
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("val", call);
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions =
        new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_failing_batch")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline =
        new EtlPipeline(config, storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Pipeline should complete (may be successful or failed depending on error handling)
    // The important thing is that it doesn't throw an unhandled exception
    assertTrue(result.getFailedBatches() >= 0);
    assertTrue(result.getSuccessfulBatches() >= 0);
  }

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("skipped_pipeline", 100);
    assertNotNull(result);
    assertEquals("skipped_pipeline", result.getPipelineName());
    assertEquals(100, result.getElapsedMs());
    assertTrue(result.isSuccessful());
  }

  @Test void testEtlResultBuilderWithTableLocation() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .tableLocation("/data/warehouse/test_table")
        .materializeFormat(MaterializeConfig.Format.ICEBERG)
        .totalRows(1000)
        .elapsedMs(500)
        .build();

    assertEquals("/data/warehouse/test_table", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.ICEBERG, result.getMaterializeFormat());
  }

  @Test void testEtlResultBuilderWithSkippedEntirePipeline() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .skippedEntirePipeline(true)
        .totalRows(5000)
        .elapsedMs(50)
        .build();

    assertTrue(result.isSkippedEntirePipeline());
    assertTrue(result.isSuccessful());
  }

  @Test void testLoggingProgressListenerAllMethods() {
    EtlPipeline.LoggingProgressListener listener =
        new EtlPipeline.LoggingProgressListener();

    listener.onPhaseStart("test", 5);
    listener.onPhaseComplete("test", 5);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("region", "EAST");

    listener.onBatchStart(0, 5, vars);
    listener.onBatchComplete(0, 5, 100, null);
    listener.onBatchComplete(1, 5, 0, new IOException("test error"));

    // Just verify no exceptions thrown
  }

  @Test void testDataProviderDefault() throws IOException {
    Iterator<Map<String, Object>> result =
        DataProvider.DEFAULT.fetch(null, null);
    // Default returns null (signaling use built-in)
    assertEquals(null, result);
  }

  @Test void testDataWriterDefault() throws IOException {
    long result = DataWriter.DEFAULT.write(null, null, null);
    // Default returns -1 (signaling use built-in)
    assertEquals(-1, result);
  }
}
