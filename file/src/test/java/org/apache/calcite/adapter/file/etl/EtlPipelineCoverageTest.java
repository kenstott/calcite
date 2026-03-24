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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for EtlPipeline, FileSource, SchemaLifecycleProcessor,
 * and related ETL classes.
 *
 * <p>Tests pipeline execution paths, error handling, progress listening,
 * file source reading, and schema lifecycle processing.
 */
@Tag("unit")
class EtlPipelineCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  // ---------------------------------------------------------------
  // EtlPipeline error handling tests
  // ---------------------------------------------------------------

  @Test void testPipelineErrorHandlingDeterminesAuthErrorAction() throws IOException {
    // Test that HTTP 401/403 errors trigger auth error action
    final List<String> errorActions = new ArrayList<String>();

    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 401 Unauthorized");
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        return 0;
      }
    };

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Arrays.asList("2020"))
        .build());

    // Default error handling uses FAIL for auth errors
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_auth_error")
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

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();
    // Auth errors with default FAIL action lead to pipeline failure
    assertNotNull(result);
    assertTrue(result.isFailed() || result.getFailedBatches() > 0);
  }

  @Test void testPipelineErrorHandlingWith404SkipsToWarn() throws IOException {
    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 404 Not Found");
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        return 0;
      }
    };

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("US"))
        .build());

    // Configure with SKIP for notFound
    Map<String, Object> errorMap = new HashMap<String, Object>();
    errorMap.put("notFound", "SKIP");
    errorMap.put("apiError", "WARN");
    errorMap.put("transientError", "SKIP");

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_404_skip")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .dimensions(dimensions)
        .errorHandling(EtlPipelineConfig.ErrorHandlingConfig.fromMap(errorMap))
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // With SKIP action, the batch is skipped and pipeline does not throw
    assertTrue(result.getSkippedBatches() > 0 || result.getErrors().size() > 0);
  }

  @Test void testPipelineErrorHandlingWith503TransientError() throws IOException {
    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        throw new IOException("HTTP 503 Service Unavailable");
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        return 0;
      }
    };

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("period", DimensionConfig.builder()
        .name("period")
        .type(DimensionType.LIST)
        .values(Arrays.asList("Q1"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_transient_error")
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

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.getErrors().size() > 0);
  }

  @Test void testPipelineWithNullErrorMessage() throws IOException {
    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        throw new IOException((String) null);
      }
    };

    DataWriter writer = DataWriter.DEFAULT;

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("id", DimensionConfig.builder()
        .name("id")
        .type(DimensionType.LIST)
        .values(Arrays.asList("1"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_null_error_msg")
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

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Null message triggers default apiError action
    assertTrue(result.getErrors().size() > 0 || result.isFailed());
  }

  @Test void testPipelineWithMultipleDimensionCombinations() throws IOException {
    final AtomicLong totalRows = new AtomicLong(0);
    final AtomicInteger batchCount = new AtomicInteger(0);

    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("year", variables.get("year"));
        row.put("region", variables.get("region"));
        row.put("amount", 100);
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        totalRows.addAndGet(count);
        batchCount.incrementAndGet();
        return count;
      }
    };

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Arrays.asList("2020", "2021"))
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("US", "EU"))
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

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSuccessful());
    assertEquals(4, batchCount.get()); // 2 years x 2 regions
    assertEquals(4, totalRows.get());
  }

  @Test void testPipelineConstructorWithSeparateSourceStorage() throws IOException {
    StorageProvider sourceStorage = new LocalFileStorageProvider();
    StorageProvider outputStorage = new LocalFileStorageProvider();

    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("val", 1);
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
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

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("id", DimensionConfig.builder()
        .name("id")
        .type(DimensionType.LIST)
        .values(Arrays.asList("1"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_dual_storage")
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

    // Use constructor with separate source storage provider
    EtlPipeline pipeline = new EtlPipeline(config, outputStorage,
        sourceStorage, tempDir.toString(), null, IncrementalTracker.NOOP,
        provider, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSuccessful());
    assertEquals(1, result.getTotalRows());
  }

  @Test void testPipelineConstructorWithOperatingDirectory() throws IOException {
    Path operatingDir = Files.createDirectories(tempDir.resolve("operating"));

    DataProvider provider = new DataProvider() {
      @Override
      public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("val", 42);
        rows.add(row);
        return rows.iterator();
      }
    };

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
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

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("id", DimensionConfig.builder()
        .name("id")
        .type(DimensionType.LIST)
        .values(Arrays.asList("1"))
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_operating_dir")
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

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        storageProvider, tempDir.toString(), null, IncrementalTracker.NOOP,
        provider, writer, operatingDir.toString());

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(result.isSuccessful());
  }

  @Test void testLoggingProgressListener() {
    EtlPipeline.LoggingProgressListener listener = new EtlPipeline.LoggingProgressListener();

    // Should not throw
    listener.onPhaseStart("test_phase", 10);
    listener.onPhaseComplete("test_phase", 10);
    listener.onBatchStart(1, 10, Collections.<String, String>emptyMap());
    listener.onBatchComplete(1, 10, 100, null);
    listener.onBatchComplete(2, 10, 0, new IOException("test error"));
  }

  @Test void testPipelineWithDocumentSourceType() throws IOException {
    final AtomicLong rowsWritten = new AtomicLong(0);

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        // Document sources pass null data iterator
        rowsWritten.addAndGet(5);
        return 5;
      }
    };

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("cik", DimensionConfig.builder()
        .name("cik")
        .type(DimensionType.LIST)
        .values(Arrays.asList("0001234567"))
        .build());

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("type", "document");

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_document_source")
        .sourceType(EtlPipelineConfig.SOURCE_TYPE_DOCUMENT)
        .rawSourceConfig(sourceMap)
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, null, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(5, result.getTotalRows());
    assertEquals(5, rowsWritten.get());
  }

  @Test void testPipelineWithDocumentSourceNoWriter() throws IOException {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("cik", DimensionConfig.builder()
        .name("cik")
        .type(DimensionType.LIST)
        .values(Arrays.asList("0001234567"))
        .build());

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("type", "document");

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_document_no_writer")
        .sourceType(EtlPipelineConfig.SOURCE_TYPE_DOCUMENT)
        .rawSourceConfig(sourceMap)
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    // No DataWriter provided - should produce 0 rows
    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, null, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Document source without writer returns 0 rows per batch
    assertEquals(0, result.getTotalRows());
  }

  @Test void testPipelineWithConstantsSourceType() throws IOException {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("id", DimensionConfig.builder()
        .name("id")
        .type(DimensionType.LIST)
        .values(Arrays.asList("1"))
        .build());

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("type", "constants");
    sourceMap.put("file", "/nonexistent.yaml");
    sourceMap.put("path", "data");
    sourceMap.put("keyColumn", "code");
    sourceMap.put("valueColumn", "name");

    DataWriter writer = new DataWriter() {
      @Override
      public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data,
          Map<String, String> variables) throws IOException {
        return 0;
      }
    };

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_constants_source")
        .sourceType(EtlPipelineConfig.SOURCE_TYPE_CONSTANTS)
        .rawSourceConfig(sourceMap)
        .dimensions(dimensions)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline = new EtlPipeline(config, storageProvider,
        tempDir.toString(), null, IncrementalTracker.NOOP, null, writer);

    // Constants source tries to load from classpath - will fail for nonexistent file
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Should have errors because the YAML file does not exist
    assertTrue(result.isFailed() || result.getErrors().size() > 0);
  }

  @Test void testPipelineConfigFromMap() {
    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("type", "file");
    sourceMap.put("path", "/data/report.xlsx");
    sourceMap.put("format", "xlsx");

    Map<String, Object> dimYearMap = new HashMap<String, Object>();
    dimYearMap.put("type", "range");
    dimYearMap.put("start", 2020);
    dimYearMap.put("end", 2023);

    Map<String, Object> dimensionsMap = new HashMap<String, Object>();
    dimensionsMap.put("year", dimYearMap);

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/output");
    outputMap.put("format", "parquet");

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    materializeMap.put("enabled", true);
    materializeMap.put("format", "PARQUET");
    materializeMap.put("output", outputMap);

    Map<String, Object> pipelineMap = new HashMap<String, Object>();
    pipelineMap.put("name", "test_from_map");
    pipelineMap.put("source", sourceMap);
    pipelineMap.put("dimensions", dimensionsMap);
    pipelineMap.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(pipelineMap);
    assertNotNull(config);
    assertEquals("test_from_map", config.getName());
    assertEquals("file", config.getSourceType());
    assertNotNull(config.getDimensions().get("year"));
    assertNotNull(config.getMaterialize());
  }

  @Test void testPipelineConfigFromMapWithEnabledFalse() {
    Map<String, Object> pipelineMap = new HashMap<String, Object>();
    pipelineMap.put("name", "disabled_pipeline");
    pipelineMap.put("enabled", false);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(pipelineMap);
    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testPipelineConfigFromNullReturnsNull() {
    assertNull(EtlPipelineConfig.fromMap(null));
  }

  // ---------------------------------------------------------------
  // FileSource tests
  // ---------------------------------------------------------------

  @Test void testFileSourceConfigBuilder() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/data/report.xlsx")
        .format("xlsx")
        .sheet("Sheet1")
        .build();

    assertEquals("/data/report.xlsx", config.getPath());
    assertEquals("xlsx", config.getFormat());
    assertEquals("Sheet1", config.getSheet());
  }

  @Test void testFileSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/report.csv");
    map.put("format", "csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertEquals("/data/report.csv", config.getPath());
    assertEquals("csv", config.getFormat());
  }

  @Test void testFileSourceConfigFromMapWithLocation() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "/data/report.json");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertEquals("/data/report.json", config.getPath());
  }

  @Test void testFileSourceConfigRequiresPath() {
    assertThrows(IllegalArgumentException.class, () -> {
      FileSourceConfig.builder().build();
    });
  }

  @Test void testFileSourceAutoDetectsFormat() throws IOException {
    // Create a test CSV file
    Path csvFile = tempDir.resolve("testdata.csv");
    Files.write(csvFile, Arrays.asList(
        "name,amount,year",
        "Alice,100,2020",
        "Bob,200,2021"
    ), StandardCharsets.UTF_8);

    FileSourceConfig config = FileSourceConfig.builder()
        .path(csvFile.toAbsolutePath().toString())
        .build();

    FileSource source = new FileSource(config, storageProvider);
    assertEquals("file", source.getType());
  }

  @Test void testFileSourceReadsCsvFile() throws IOException {
    Path csvFile = tempDir.resolve("sales.csv");
    Files.write(csvFile, Arrays.asList(
        "product,revenue,quantity",
        "Widget,500,10",
        "Gadget,300,5"
    ), StandardCharsets.UTF_8);

    FileSourceConfig config = FileSourceConfig.builder()
        .path(csvFile.toAbsolutePath().toString())
        .format("csv")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> data = source.fetch(
        Collections.<String, String>emptyMap());

    assertNotNull(data);
    assertTrue(data.hasNext());

    Map<String, Object> row1 = data.next();
    assertNotNull(row1);
    assertEquals("Widget", row1.get("product").toString());

    assertTrue(data.hasNext());
    Map<String, Object> row2 = data.next();
    assertEquals("Gadget", row2.get("product").toString());

    assertFalse(data.hasNext());
  }

  @Test void testFileSourceReadsJsonFile() throws IOException {
    Path jsonFile = tempDir.resolve("records.json");
    Files.write(jsonFile, Arrays.asList(
        "[{\"item\": \"A\", \"qty\": 10}, {\"item\": \"B\", \"qty\": 20}]"
    ), StandardCharsets.UTF_8);

    FileSourceConfig config = FileSourceConfig.builder()
        .path(jsonFile.toAbsolutePath().toString())
        .format("json")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> data = source.fetch(
        Collections.<String, String>emptyMap());

    assertNotNull(data);
    assertTrue(data.hasNext());

    Map<String, Object> row1 = data.next();
    assertEquals("A", row1.get("item").toString());

    assertTrue(data.hasNext());
    Map<String, Object> row2 = data.next();
    assertEquals("B", row2.get("item").toString());

    assertFalse(data.hasNext());
  }

  @Test void testFileSourceVariableSubstitution() throws IOException {
    Path dir2020 = Files.createDirectories(tempDir.resolve("2020"));
    Path csvFile = dir2020.resolve("data.csv");
    Files.write(csvFile, Arrays.asList(
        "metric,amount",
        "revenue,1000"
    ), StandardCharsets.UTF_8);

    FileSourceConfig config = FileSourceConfig.builder()
        .path(tempDir.toAbsolutePath().toString() + "/${year}/data.csv")
        .format("csv")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2020");

    Iterator<Map<String, Object>> data = source.fetch(variables);
    assertNotNull(data);
    assertTrue(data.hasNext());

    Map<String, Object> row = data.next();
    assertEquals("revenue", row.get("metric").toString());
  }

  @Test void testFileSourceUnsupportedFormat() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/some/file.xyz")
        .format("xyz")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    assertThrows(IOException.class, () -> {
      source.fetch(Collections.<String, String>emptyMap());
    });
  }

  // ---------------------------------------------------------------
  // SchemaLifecycleProcessor tests
  // ---------------------------------------------------------------

  @Test void testSchemaLifecycleProcessorBuilder() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    assertNotNull(processor);
  }

  @Test void testSchemaLifecycleProcessorWithEmptyTables() throws IOException {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("empty_schema")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    assertEquals("empty_schema", result.getSchemaName());
  }

  @Test void testSchemaLifecycleProcessorWithDisabledTable() throws IOException {
    EtlPipelineConfig disabledTable = EtlPipelineConfig.builder()
        .name("disabled_table")
        .enabled(false)
        .build();

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("schema_disabled")
        .tables(Collections.singletonList(disabledTable))
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    // Disabled table should be skipped
    assertNotNull(result.getTableResults());
  }

  @Test void testSchemaLifecycleProcessorWithSeparateSourceStorage() throws IOException {
    StorageProvider sourceProvider = new LocalFileStorageProvider();

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("dual_storage_schema")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .sourceStorageProvider(sourceProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
  }

  // ---------------------------------------------------------------
  // ParquetMaterializationWriter tests
  // ---------------------------------------------------------------

  @Test void testParquetMaterializationWriterInitializationFailsWhenDisabled() {
    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, tempDir.toString());

    MaterializeConfig disabledConfig = MaterializeConfig.builder()
        .enabled(false)
        .build();

    assertThrows(IOException.class, () -> {
      writer.initialize(disabledConfig);
    });
  }

  @Test void testParquetMaterializationWriterInitializationNullConfig() {
    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, tempDir.toString());

    assertThrows(IllegalArgumentException.class, () -> {
      writer.initialize(null);
    });
  }

  @Test void testParquetMaterializationWriterWriteBeforeInit() {
    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, tempDir.toString());

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("val", 1);
    rows.add(row);

    assertThrows(IllegalStateException.class, () -> {
      writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());
    });
  }

  @Test void testParquetMaterializationWriterEmptyBatch() throws IOException {
    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, tempDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    writer.initialize(config);

    long rowsWritten = writer.writeBatch(
        Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(0, rowsWritten);
  }

  @Test void testParquetMaterializationWriterNullData() throws IOException {
    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, tempDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location(tempDir.toString())
            .build())
        .build();

    writer.initialize(config);

    long rowsWritten = writer.writeBatch(null,
        Collections.<String, String>emptyMap());
    assertEquals(0, rowsWritten);
  }

  @Test void testParquetMaterializationWriterWritesBatch() throws IOException {
    Path outputDir = Files.createDirectories(tempDir.resolve("parquet_output"));
    String outputFile = outputDir.resolve("output.parquet").toString();

    ParquetMaterializationWriter writer =
        new ParquetMaterializationWriter(storageProvider, outputDir.toString());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location(outputFile)
            .build())
        .build();

    writer.initialize(config);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 3; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("item_name", "item_" + i);
      row.put("quantity", i * 10);
      rows.add(row);
    }

    long rowsWritten = writer.writeBatch(rows.iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(3, rowsWritten);

    // Commit and close
    writer.commit();
    writer.close();
  }

  // ---------------------------------------------------------------
  // EtlResult builder tests
  // ---------------------------------------------------------------

  @Test void testEtlResultSkippedFactory() {
    EtlResult result = EtlResult.skipped("test_pipeline", 100);
    assertNotNull(result);
    assertEquals("test_pipeline", result.getPipelineName());
    assertTrue(result.isSkippedEntirePipeline());
  }

  @Test void testEtlResultWithErrors() {
    EtlResult result = EtlResult.builder()
        .pipelineName("error_pipeline")
        .totalRows(0)
        .failedBatches(2)
        .errors(Arrays.asList("Error 1", "Error 2"))
        .failed(true)
        .failureMessage("Pipeline failed")
        .build();

    assertTrue(result.isFailed());
    assertEquals(2, result.getErrors().size());
    assertEquals("Pipeline failed", result.getFailureMessage());
    assertEquals(2, result.getFailedBatches());
  }

  @Test void testEtlResultWithTableLocation() {
    EtlResult result = EtlResult.builder()
        .pipelineName("located_pipeline")
        .tableLocation("/data/output/my_table")
        .materializeFormat(MaterializeConfig.Format.PARQUET)
        .build();

    assertEquals("/data/output/my_table", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.PARQUET, result.getMaterializeFormat());
  }

  // ---------------------------------------------------------------
  // ConstantsSource tests
  // ---------------------------------------------------------------

  @Test void testConstantsSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("file", "/data/constants.yaml");
    map.put("path", "codes");
    map.put("keyColumn", "code");
    map.put("valueColumn", "description");

    ConstantsSource.ConstantsSourceConfig config =
        ConstantsSource.ConstantsSourceConfig.fromMap(map);

    assertEquals("/data/constants.yaml", config.getFile());
    assertEquals("codes", config.getPath());
    assertEquals("code", config.getKeyColumn());
    assertEquals("description", config.getValueColumn());
  }

  @Test void testConstantsSourceConfigDefaults() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("file", "/data/ref.yaml");

    ConstantsSource.ConstantsSourceConfig config =
        ConstantsSource.ConstantsSourceConfig.fromMap(map);

    assertEquals("key", config.getKeyColumn());
    assertEquals("value", config.getValueColumn());
  }

  @Test void testConstantsSourceType() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("file", "/test.yaml");
    configMap.put("path", "data");

    ConstantsSource source = ConstantsSource.fromMap(configMap);
    assertEquals("constants", source.getType());
    assertEquals(-1, source.estimateRowCount());
  }

  @Test void testConstantsSourceMissingResource() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("file", "/nonexistent/resource.yaml");
    configMap.put("path", "data");

    ConstantsSource source = ConstantsSource.fromMap(configMap);
    assertThrows(IOException.class, () -> {
      source.fetch(Collections.<String, String>emptyMap());
    });
  }
}
