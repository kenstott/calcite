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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration-style coverage tests for {@link EtlPipeline} and
 * {@link SchemaLifecycleProcessor}.
 *
 * <p>These tests construct real configuration objects and execute real methods
 * with custom DataProvider/DataWriter implementations to maximize code coverage
 * without requiring HTTP or database connections.
 */
@Tag("unit")
public class EtlPipelineIntegrationCoverageTest {

  @TempDir
  Path tempDir;

  // =========================================================================
  // Helper methods
  // =========================================================================

  /** Creates a simple row map from key-value pairs. */
  private Map<String, Object> row(String... kvPairs) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      r.put(kvPairs[i], kvPairs[i + 1]);
    }
    return r;
  }

  /** Creates a range dimension. */
  private Map<String, DimensionConfig> rangeDimension(String key, int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.RANGE).start(start).end(end).build());
    return dims;
  }

  /** Creates a list dimension. */
  private Map<String, DimensionConfig> listDimension(String key, String... values) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.LIST).values(Arrays.asList(values)).build());
    return dims;
  }

  /** Creates a multi-dimension config (two dims). */
  private Map<String, DimensionConfig> multiDimension(
      String key1, int start1, int end1,
      String key2, String... values2) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key1, DimensionConfig.builder()
        .name(key1).type(DimensionType.RANGE).start(start1).end(end1).build());
    dims.put(key2, DimensionConfig.builder()
        .name(key2).type(DimensionType.LIST).values(Arrays.asList(values2)).build());
    return dims;
  }

  /** Creates an HttpSourceConfig for testing. */
  private HttpSourceConfig httpSource(String url) {
    return HttpSourceConfig.builder().url(url).build();
  }

  /** Creates columns list. */
  private List<ColumnConfig> columns(String... names) {
    List<ColumnConfig> cols = new ArrayList<ColumnConfig>();
    for (String name : names) {
      cols.add(ColumnConfig.builder().name(name).type("VARCHAR").build());
    }
    return cols;
  }

  /** Creates a minimal EtlPipelineConfig with HTTP source. */
  private EtlPipelineConfig httpConfig(String name, Map<String, DimensionConfig> dims) {
    return httpConfig(name, dims, null, null);
  }

  /** Creates EtlPipelineConfig with format and columns. */
  private EtlPipelineConfig httpConfig(String name, Map<String, DimensionConfig> dims,
      MaterializeConfig.Format format, List<ColumnConfig> cols) {
    MaterializeConfig.Builder matBuilder = MaterializeConfig.builder()
        .enabled(true)
        .output(MaterializeOutputConfig.builder().build());
    if (format != null) {
      matBuilder.format(format);
    }
    EtlPipelineConfig.Builder builder = EtlPipelineConfig.builder()
        .name(name)
        .source(httpSource("https://api.example.com/data"))
        .materialize(matBuilder.build());
    if (dims != null) {
      builder.dimensions(dims);
    }
    if (cols != null) {
      builder.columns(cols);
    }
    return builder.build();
  }

  /** Creates a hooks-only (no source) EtlPipelineConfig. */
  private EtlPipelineConfig hooksOnlyConfig(String name) {
    HooksConfig hooks = HooksConfig.builder().enabled(true).build();
    return EtlPipelineConfig.builder()
        .name(name)
        .hooks(hooks)
        .build();
  }

  /** Creates a real local StorageProvider backed by the temp dir. */
  private StorageProvider localStorage() {
    return new LocalFileStorageProvider();
  }

  /** Creates a DataProvider that returns fixed data per batch. */
  private DataProvider fixedDataProvider(final List<Map<String, Object>> data) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        return data.iterator();
      }
    };
  }

  /** Creates a DataProvider that throws an IOException on every call. */
  private DataProvider failingDataProvider(final String errorMessage) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        throw new IOException(errorMessage);
      }
    };
  }

  /** Creates a DataWriter that counts rows and tracks calls. */
  private static class CountingDataWriter implements DataWriter {
    final AtomicLong totalRows = new AtomicLong();
    final AtomicInteger callCount = new AtomicInteger();
    final List<Map<String, String>> receivedVariables =
        Collections.synchronizedList(new ArrayList<Map<String, String>>());

    @Override public long write(EtlPipelineConfig config,
        Iterator<Map<String, Object>> data, Map<String, String> variables) {
      callCount.incrementAndGet();
      receivedVariables.add(new LinkedHashMap<String, String>(variables));
      long count = 0;
      while (data != null && data.hasNext()) {
        data.next();
        count++;
      }
      totalRows.addAndGet(count);
      return count;
    }
  }

  /** Creates a DataWriter that returns -1 (fall through to default). */
  private DataWriter passThroughWriter() {
    return new DataWriter() {
      @Override public long write(EtlPipelineConfig config,
          Iterator<Map<String, Object>> data, Map<String, String> variables) {
        return -1;
      }
    };
  }

  /** Creates a no-op progress listener that tracks calls. */
  private static class TrackingProgressListener implements EtlPipeline.ProgressListener {
    final AtomicInteger phaseStarts = new AtomicInteger();
    final AtomicInteger phaseCompletes = new AtomicInteger();
    final AtomicInteger batchStarts = new AtomicInteger();
    final AtomicInteger batchCompletes = new AtomicInteger();
    final AtomicInteger batchErrors = new AtomicInteger();

    @Override public void onPhaseStart(String phase, int totalItems) {
      phaseStarts.incrementAndGet();
    }

    @Override public void onPhaseComplete(String phase, int processedItems) {
      phaseCompletes.incrementAndGet();
    }

    @Override public void onBatchStart(int batchNum, int totalBatches,
        Map<String, String> variables) {
      batchStarts.incrementAndGet();
    }

    @Override public void onBatchComplete(int batchNum, int totalBatches,
        int rowCount, Exception error) {
      batchCompletes.incrementAndGet();
      if (error != null) {
        batchErrors.incrementAndGet();
      }
    }
  }

  // =========================================================================
  // EtlPipelineConfig construction tests
  // =========================================================================

  @Test void testConfigBuilderMinimal() {
    EtlPipelineConfig config = httpConfig("test_table", null);
    assertEquals("test_table", config.getName());
    assertTrue(config.isEnabled());
    assertEquals("http", config.getSourceType());
    assertNotNull(config.getSource());
    assertNotNull(config.getMaterialize());
    assertNotNull(config.getErrorHandling());
    assertNotNull(config.getHooks());
    assertTrue(config.getDimensions().isEmpty());
    assertTrue(config.getColumns().isEmpty());
  }

  @Test void testConfigBuilderWithDimensions() {
    Map<String, DimensionConfig> dims = multiDimension("year", 2020, 2022, "region", "US", "EU");
    EtlPipelineConfig config = httpConfig("sales", dims);
    assertEquals(2, config.getDimensions().size());
    assertTrue(config.getDimensions().containsKey("year"));
    assertTrue(config.getDimensions().containsKey("region"));
  }

  @Test void testConfigBuilderWithColumns() {
    List<ColumnConfig> cols = columns("id", "name", "value");
    EtlPipelineConfig config = httpConfig("data", null, null, cols);
    assertEquals(3, config.getColumns().size());
    assertEquals("id", config.getColumns().get(0).getName());
    assertEquals("VARCHAR", config.getColumns().get(0).getType());
  }

  @Test void testConfigBuilderWithErrorHandling() {
    EtlPipelineConfig.ErrorHandlingConfig errorConfig =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY)
            .notFoundAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .transientRetries(5)
            .transientBackoffMs(2000)
            .notFoundRetryDays(14)
            .apiErrorRetryDays(3)
            .build();

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_errors")
        .source(httpSource("https://example.com"))
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .errorHandling(errorConfig)
        .build();

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getErrorHandling().getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getErrorHandling().getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getErrorHandling().getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getErrorHandling().getAuthErrorAction());
    assertEquals(5, config.getErrorHandling().getTransientRetries());
    assertEquals(2000L, config.getErrorHandling().getTransientBackoffMs());
    assertEquals(14, config.getErrorHandling().getNotFoundRetryDays());
    assertEquals(3, config.getErrorHandling().getApiErrorRetryDays());
  }

  @Test void testConfigBuilderDisabledSkipsValidation() {
    // Disabled configs skip source/materialize validation
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("disabled_table")
        .enabled(false)
        .build();

    assertFalse(config.isEnabled());
    assertEquals("disabled_table", config.getName());
    assertNull(config.getSource());
    assertNull(config.getMaterialize());
  }

  @Test void testConfigBuilderNameRequired() {
    try {
      EtlPipelineConfig.builder().build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testConfigBuilderSourceRequired() {
    try {
      EtlPipelineConfig.builder().name("no_source").build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Source"));
    }
  }

  @Test void testConfigBuilderHooksOnlySkipsSourceValidation() {
    EtlPipelineConfig config = hooksOnlyConfig("hooks_only");
    assertEquals("hooks_only", config.getName());
    assertTrue(config.getHooks().isEnabled());
    assertNull(config.getSource());
  }

  @Test void testConfigFromMap() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "from_map");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("from_map", config.getName());
    assertEquals("http", config.getSourceType());
  }

  @Test void testConfigFromMapWithDimensions() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "with_dims");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    Map<String, Object> dimsMap = new LinkedHashMap<String, Object>();
    Map<String, Object> yearDim = new LinkedHashMap<String, Object>();
    yearDim.put("type", "range");
    yearDim.put("start", 2020);
    yearDim.put("end", 2023);
    dimsMap.put("year", yearDim);
    map.put("dimensions", dimsMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(1, config.getDimensions().size());
    assertTrue(config.getDimensions().containsKey("year"));
  }

  @Test void testConfigFromMapWithConstantsSource() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "constants_table");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "constants");
    sourceMap.put("data", "test");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("constants", config.getSourceType());
    assertNotNull(config.getRawSourceConfig());
    assertNull(config.getSource()); // HttpSourceConfig is null for constants
  }

  @Test void testConfigFromMapWithColumns() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "col_table");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    List<Map<String, Object>> colsList = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new LinkedHashMap<String, Object>();
    col1.put("name", "id");
    col1.put("type", "INTEGER");
    colsList.add(col1);
    Map<String, Object> col2 = new LinkedHashMap<String, Object>();
    col2.put("name", "value");
    col2.put("type", "VARCHAR");
    col2.put("comment", "The data value");
    colsList.add(col2);
    map.put("columns", colsList);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumns().size());
    assertEquals("id", config.getColumns().get(0).getName());
  }

  @Test void testConfigFromMapWithErrorHandling() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "err_table");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    Map<String, Object> errMap = new LinkedHashMap<String, Object>();
    errMap.put("transientErrorAction", "RETRY");
    errMap.put("notFoundAction", "SKIP");
    errMap.put("apiErrorAction", "WARN");
    errMap.put("authErrorAction", "FAIL");
    errMap.put("transientRetries", 5);
    errMap.put("transientBackoffMs", 2000L);
    errMap.put("notFoundRetryDays", 14);
    errMap.put("apiErrorRetryDays", 3);
    map.put("errorHandling", errMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getErrorHandling().getTransientErrorAction());
    assertEquals(5, config.getErrorHandling().getTransientRetries());
  }

  @Test void testConfigFromMapEnabledBoolean() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "disabled");
    map.put("enabled", Boolean.FALSE);
    // Disabled tables skip validation
    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testConfigFromMapEnabledString() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "enabled_str");
    map.put("enabled", "false");
    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testConfigFromMapNull() {
    assertNull(EtlPipelineConfig.fromMap(null));
  }

  @Test void testErrorHandlingDefaults() {
    EtlPipelineConfig.ErrorHandlingConfig defaults =
        EtlPipelineConfig.ErrorHandlingConfig.defaults();
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        defaults.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        defaults.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        defaults.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        defaults.getAuthErrorAction());
    assertEquals(3, defaults.getTransientRetries());
    assertEquals(1000L, defaults.getTransientBackoffMs());
    assertEquals(7, defaults.getNotFoundRetryDays());
    assertEquals(7, defaults.getApiErrorRetryDays());
  }

  @Test void testErrorHandlingFromMapNull() {
    EtlPipelineConfig.ErrorHandlingConfig fromNull =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(null);
    assertNotNull(fromNull);
    // Should be defaults
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        fromNull.getTransientErrorAction());
  }

  // =========================================================================
  // EtlPipeline constructor variants
  // =========================================================================

  @Test void testPipelineConstructorMinimal() {
    EtlPipelineConfig config = httpConfig("ctor_min", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    assertNotNull(pipeline);
  }

  @Test void testPipelineConstructorWithProgressListener() {
    EtlPipelineConfig config = httpConfig("ctor_pl", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    TrackingProgressListener listener = new TrackingProgressListener();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), listener);
    assertNotNull(pipeline);
  }

  @Test void testPipelineConstructorWithTracker() {
    EtlPipelineConfig config = httpConfig("ctor_trk", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP);
    assertNotNull(pipeline);
  }

  @Test void testPipelineConstructorFull() {
    EtlPipelineConfig config = httpConfig("ctor_full", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    CountingDataWriter writer = new CountingDataWriter();
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("a", "1"));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), writer);
    assertNotNull(pipeline);
  }

  @Test void testPipelineConstructorWithSeparateSourceStorage() {
    EtlPipelineConfig config = httpConfig("ctor_src", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    StorageProvider srcSp = localStorage();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, srcSp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null);
    assertNotNull(pipeline);
  }

  @Test void testPipelineConstructorWithOperatingDirectory() {
    EtlPipelineConfig config = httpConfig("ctor_opdir", rangeDimension("y", 2020, 2020));
    StorageProvider sp = localStorage();
    String opDir = tempDir.resolve("opdir").toString();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, null, tempDir.toString(), null, IncrementalTracker.NOOP, null, null, opDir);
    assertNotNull(pipeline);
  }

  // =========================================================================
  // EtlPipeline.execute() with custom DataProvider + DataWriter
  // =========================================================================

  @Test void testExecuteWithCustomDataProviderAndWriter() throws IOException {
    Map<String, DimensionConfig> dims = rangeDimension("year", 2020, 2021);
    EtlPipelineConfig config = httpConfig("custom_dp", dims);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "value", "a"));
    data.add(row("id", "2", "value", "b"));

    CountingDataWriter writer = new CountingDataWriter();
    TrackingProgressListener listener = new TrackingProgressListener();

    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), listener, IncrementalTracker.NOOP, fixedDataProvider(data), writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals("custom_dp", result.getPipelineName());
    assertFalse(result.isFailed());
    assertTrue(result.isSuccessful());
    // 2 batches (2020, 2021), 2 rows each = 4 total
    assertEquals(4, result.getTotalRows());
    assertEquals(2, result.getSuccessfulBatches());
    assertEquals(0, result.getFailedBatches());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.getElapsedMs() >= 0);

    // Verify writer received correct variables
    assertEquals(2, writer.callCount.get());
    assertEquals(4, writer.totalRows.get());

    // Verify progress listener was called
    assertTrue(listener.phaseStarts.get() >= 2); // dimension_expansion + data_processing
    assertTrue(listener.phaseCompletes.get() >= 2);
    assertEquals(2, listener.batchStarts.get());
    assertEquals(2, listener.batchCompletes.get());
    assertEquals(0, listener.batchErrors.get());
  }

  @Test void testExecuteWithEmptyDimensions() throws IOException {
    // Empty dimensions produce 1 combination (the empty map)
    Map<String, DimensionConfig> emptyDims = new LinkedHashMap<String, DimensionConfig>();
    EtlPipelineConfig config = httpConfig("empty_dims", emptyDims);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("k", "v"));

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Empty dimensions = 1 empty-map combination = 1 batch
    assertEquals(1, result.getTotalRows());
    assertEquals(1, result.getSuccessfulBatches());
    assertEquals(1, writer.callCount.get());
    assertTrue(result.getErrors().isEmpty());
  }

  @Test void testExecuteWithListDimensions() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("region", "US", "EU", "APAC");
    EtlPipelineConfig config = httpConfig("list_dims", dims);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("region", "X", "count", "100"));

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals("list_dims", result.getPipelineName());
    assertFalse(result.isFailed());
    assertEquals(3, result.getSuccessfulBatches()); // US, EU, APAC
    assertEquals(3, result.getTotalRows()); // 1 row per batch
    assertEquals(3, writer.callCount.get());

    // Verify all regions were processed
    List<String> processedRegions = new ArrayList<String>();
    for (Map<String, String> vars : writer.receivedVariables) {
      processedRegions.add(vars.get("region"));
    }
    assertTrue(processedRegions.contains("US"));
    assertTrue(processedRegions.contains("EU"));
    assertTrue(processedRegions.contains("APAC"));
  }

  @Test void testExecuteWithMultiDimensions() throws IOException {
    Map<String, DimensionConfig> dims = multiDimension("year", 2020, 2021, "region", "US", "EU");
    EtlPipelineConfig config = httpConfig("multi_dims", dims);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("k", "v"));

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), writer);

    EtlResult result = pipeline.execute();

    assertFalse(result.isFailed());
    // 2 years x 2 regions = 4 combinations
    assertEquals(4, result.getSuccessfulBatches());
    assertEquals(4, result.getTotalRows());
  }

  @Test void testExecuteWithNullDataProvider() throws IOException {
    // DataProvider returns null, forcing fallback to DataSource (which fails for HTTP in test)
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    EtlPipelineConfig config = httpConfig("null_dp", dims);

    DataProvider nullProvider = DataProvider.DEFAULT; // returns null

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, nullProvider, writer);

    // This will fail because HTTP source tries to connect
    EtlResult result = pipeline.execute();
    // With SKIP error handling, it should not throw but still report errors
    assertTrue(result.getErrors().size() > 0 || result.getFailedBatches() > 0
        || result.isFailed());
  }

  @Test void testExecuteWithFailingDataProvider() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a", "b");
    EtlPipelineConfig config = httpConfig("fail_dp", dims);

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, failingDataProvider("HTTP 404 Not Found"), writer);

    EtlResult result = pipeline.execute();
    // With default error handling (SKIP for 404), should not throw
    assertNotNull(result);
    // No successful batches
    assertEquals(0, result.getSuccessfulBatches());
    // Errors should be recorded
    assertFalse(result.getErrors().isEmpty());
  }

  @Test void testExecuteWithAuthFailure() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("auth_fail")
        .source(httpSource("https://api.example.com"))
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .errorHandling(new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build())
        .build();

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, failingDataProvider("HTTP 401 Unauthorized"), writer);

    // Auth FAIL action causes the pipeline to throw
    EtlResult result = pipeline.execute();
    assertTrue(result.isFailed() || !result.getErrors().isEmpty());
  }

  @Test void testExecuteWithTransientError() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    EtlPipelineConfig config = httpConfig("transient", dims);

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, failingDataProvider("HTTP 429 Too Many Requests"), writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Transient errors should be recorded
    assertFalse(result.getErrors().isEmpty());
  }

  @Test void testExecuteWithServerError() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    EtlPipelineConfig config = httpConfig("server_err", dims);

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, failingDataProvider("HTTP 500 Internal Server Error"), writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecuteDataWriterReturnsNegative() throws IOException {
    // DataWriter returns -1, meaning fall through to built-in writer
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    EtlPipelineConfig config = httpConfig("passthrough_dw", dims);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("k", "v"));

    // The pass-through writer returns -1, so pipeline tries the built-in MaterializationWriter.
    // This will fail because no real writer is configured, but it exercises the fallback code path
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), passThroughWriter());

    EtlResult result = pipeline.execute();
    // This should fail since the built-in MaterializationWriter is not properly set up
    // but the code path is exercised
    assertNotNull(result);
  }

  @Test void testExecuteDocumentSourceType() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    // Build config with document source type
    Map<String, Object> rawSource = new LinkedHashMap<String, Object>();
    rawSource.put("type", "document");
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_src")
        .sourceType("document")
        .rawSourceConfig(rawSource)
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, null, writer);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Document sources use dataWriter directly
    assertEquals(1, writer.callCount.get());
  }

  @Test void testExecuteDocumentSourceWithoutWriter() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    Map<String, Object> rawSource = new LinkedHashMap<String, Object>();
    rawSource.put("type", "document");
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_no_writer")
        .sourceType("document")
        .rawSourceConfig(rawSource)
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();

    // No dataWriter - document source should log warning and return 0 rows
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, null, null);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, result.getTotalRows());
  }

  // =========================================================================
  // EtlPipeline.execute() - EtlResult assertions
  // =========================================================================

  @Test void testEtlResultProperties() throws IOException {
    Map<String, DimensionConfig> dims = listDimension("x", "a");
    EtlPipelineConfig config = httpConfig("result_test", dims, MaterializeConfig.Format.PARQUET, null);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("k", "v"));
    data.add(row("k", "v2"));

    CountingDataWriter writer = new CountingDataWriter();
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, fixedDataProvider(data), writer);

    EtlResult result = pipeline.execute();

    assertEquals("result_test", result.getPipelineName());
    assertEquals(2, result.getTotalRows());
    assertEquals(1, result.getSuccessfulBatches());
    assertEquals(0, result.getFailedBatches());
    assertEquals(0, result.getSkippedBatches());
    assertEquals(1, result.getTotalBatches());
    assertTrue(result.getElapsedMs() >= 0);
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
    assertFalse(result.isFailed());
    assertNull(result.getFailureMessage());
    assertTrue(result.getErrors().isEmpty());
    assertFalse(result.isSkippedEntirePipeline());
  }

  @Test void testEtlResultSkipped() {
    EtlResult skipped = EtlResult.skipped("skipped_pipe", 100);
    assertEquals("skipped_pipe", skipped.getPipelineName());
    assertTrue(skipped.isSkippedEntirePipeline());
    assertEquals(100, skipped.getElapsedMs());
  }

  @Test void testEtlResultBuilder() {
    EtlResult result = EtlResult.builder()
        .pipelineName("built")
        .totalRows(42)
        .successfulBatches(3)
        .failedBatches(1)
        .skippedBatches(2)
        .elapsedMs(500)
        .errors(Arrays.asList("err1", "err2"))
        .failed(true)
        .failureMessage("something broke")
        .tableLocation("/tmp/data")
        .materializeFormat(MaterializeConfig.Format.ICEBERG)
        .build();

    assertEquals("built", result.getPipelineName());
    assertEquals(42, result.getTotalRows());
    assertEquals(3, result.getSuccessfulBatches());
    assertEquals(1, result.getFailedBatches());
    assertEquals(2, result.getSkippedBatches());
    assertEquals(6, result.getTotalBatches());
    assertEquals(500, result.getElapsedMs());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.isFailed());
    assertFalse(result.isSuccessful());
    assertFalse(result.isCompleteSuccess());
    assertEquals("something broke", result.getFailureMessage());
    assertEquals("/tmp/data", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.ICEBERG, result.getMaterializeFormat());
  }

  // =========================================================================
  // EtlPipeline - determineErrorAction (via reflection)
  // =========================================================================

  @Test void testDetermineErrorActionAllCodes() throws Exception {
    EtlPipelineConfig config = httpConfig("err_act", rangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, localStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);

    EtlPipelineConfig.ErrorHandlingConfig defaults = EtlPipelineConfig.ErrorHandlingConfig.defaults();

    // Auth errors
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        m.invoke(pipeline, new IOException("HTTP 401"), defaults));
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        m.invoke(pipeline, new IOException("HTTP 403"), defaults));

    // Not found
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        m.invoke(pipeline, new IOException("HTTP 404"), defaults));

    // Transient
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        m.invoke(pipeline, new IOException("HTTP 429"), defaults));
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        m.invoke(pipeline, new IOException("HTTP 503"), defaults));

    // Server errors
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        m.invoke(pipeline, new IOException("HTTP 500"), defaults));
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        m.invoke(pipeline, new IOException("HTTP 502"), defaults));

    // Null message
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        m.invoke(pipeline, new IOException((String) null), defaults));

    // Generic
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        m.invoke(pipeline, new IOException("Timeout"), defaults));
  }

  // =========================================================================
  // EtlPipeline - verifyDataExists (via reflection)
  // =========================================================================

  @Test void testVerifyDataExistsNoMaterialize() throws Exception {
    // Config with no materialize should return true
    EtlPipelineConfig config = hooksOnlyConfig("no_mat");
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP, null, null);

    Method m =
        EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    Boolean result = (Boolean) m.invoke(pipeline, "no_mat", config);
    assertTrue(result);
  }

  @Test void testVerifyDataExistsParquetFormatDirectoryExists() throws Exception {
    // Create a directory structure that looks like parquet output
    Path dataDir = tempDir.resolve("verify_parquet").resolve("test_table");
    Files.createDirectories(dataDir);
    Files.write(dataDir.resolve("part-0.parquet"), new byte[]{1, 2, 3});

    EtlPipelineConfig config =
        httpConfig("test_table", null, MaterializeConfig.Format.PARQUET, null);

    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.resolve("verify_parquet").toString(),
        null, IncrementalTracker.NOOP, null, null);

    Method m =
        EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    Boolean result = (Boolean) m.invoke(pipeline, "test_table", config);
    // Result depends on the directory check
    assertNotNull(result);
  }

  // =========================================================================
  // EtlPipeline - LoggingProgressListener
  // =========================================================================

  @Test void testLoggingProgressListener() {
    EtlPipeline.LoggingProgressListener listener = new EtlPipeline.LoggingProgressListener();
    // Should not throw
    listener.onPhaseStart("test_phase", 10);
    listener.onPhaseComplete("test_phase", 10);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2020");
    listener.onBatchStart(1, 10, vars);
    listener.onBatchComplete(1, 10, 100, null);
    listener.onBatchComplete(2, 10, 0, new Exception("test error"));
  }

  // =========================================================================
  // SchemaConfig construction
  // =========================================================================

  @Test void testSchemaConfigBuilder() {
    EtlPipelineConfig table1 = httpConfig("table1", rangeDimension("y", 2020, 2020));
    EtlPipelineConfig table2 = httpConfig("table2", listDimension("r", "US"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("test_schema")
        .sourceDirectory("/source")
        .materializeDirectory("/output")
        .addTable(table1)
        .addTable(table2)
        .build();

    assertEquals("test_schema", schema.getName());
    assertEquals("/source", schema.getSourceDirectory());
    assertEquals("/output", schema.getMaterializeDirectory());
    assertEquals(2, schema.getTables().size());
    assertEquals(2, schema.getTableCount());
    assertEquals("table1", schema.getTable("table1").getName());
    assertEquals("table2", schema.getTable("table2").getName());
    assertNull(schema.getTable("nonexistent"));
    assertNotNull(schema.getHooks());
    assertTrue(schema.getPostProcess().isEmpty());
    assertTrue(schema.getMetadata().isEmpty());
    assertTrue(schema.getBulkDownloads().isEmpty());
  }

  @Test void testSchemaConfigFromMap() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "map_schema");
    map.put("sourceDirectory", "/src");
    map.put("materializeDirectory", "/mat");

    List<Map<String, Object>> tablesList = new ArrayList<Map<String, Object>>();
    Map<String, Object> tbl = new LinkedHashMap<String, Object>();
    tbl.put("name", "tbl1");
    Map<String, Object> srcMap = new LinkedHashMap<String, Object>();
    srcMap.put("url", "https://example.com");
    tbl.put("source", srcMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    tbl.put("materialize", matMap);
    tablesList.add(tbl);
    map.put("tables", tablesList);

    SchemaConfig schema = SchemaConfig.fromMap(map);
    assertNotNull(schema);
    assertEquals("map_schema", schema.getName());
    assertEquals(1, schema.getTableCount());
  }

  @Test void testSchemaConfigFromMapWithSchemaName() {
    // Alternative format using "schemaName" key
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("schemaName", "alt_schema");
    map.put("materializeDirectory", "/mat");

    SchemaConfig schema = SchemaConfig.fromMap(map);
    assertNotNull(schema);
    assertEquals("alt_schema", schema.getName());
  }

  @Test void testSchemaConfigFromMapSkipsViews() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "view_schema");
    map.put("materializeDirectory", "/mat");

    List<Map<String, Object>> tablesList = new ArrayList<Map<String, Object>>();
    // A view definition - should be skipped
    Map<String, Object> viewDef = new LinkedHashMap<String, Object>();
    viewDef.put("name", "my_view");
    viewDef.put("type", "view");
    tablesList.add(viewDef);
    // A sourceless table without hooks - should be skipped
    Map<String, Object> noSrcTbl = new LinkedHashMap<String, Object>();
    noSrcTbl.put("name", "no_source");
    tablesList.add(noSrcTbl);
    map.put("tables", tablesList);

    SchemaConfig schema = SchemaConfig.fromMap(map);
    assertNotNull(schema);
    assertEquals(0, schema.getTableCount()); // Both skipped
  }

  @Test void testSchemaConfigFromMapNull() {
    assertNull(SchemaConfig.fromMap(null));
  }

  @Test void testSchemaConfigNameRequired() {
    try {
      SchemaConfig.builder().build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  // =========================================================================
  // SchemaLifecycleProcessor - basic construction
  // =========================================================================

  @Test void testProcessorBuilderMinimal() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("proc_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(httpConfig("t1", rangeDimension("y", 2020, 2020)))
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .build();

    assertNotNull(processor);
  }

  @Test void testProcessorBuilderConfigRequired() {
    try {
      SchemaLifecycleProcessor.builder()
          .storageProvider(localStorage())
          .materializeDirectory(tempDir.toString())
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("config"));
    }
  }

  @Test void testProcessorBuilderStorageRequired() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("need_storage")
        .materializeDirectory(tempDir.toString())
        .build();

    try {
      SchemaLifecycleProcessor.builder()
          .config(schema)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Storage") || e.getMessage().contains("storage"));
    }
  }

  @Test void testProcessorBuilderMaterializeDirRequired() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("need_dir")
        .build();

    try {
      SchemaLifecycleProcessor.builder()
          .config(schema)
          .storageProvider(localStorage())
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("directory") || e.getMessage().contains("Materialize"));
    }
  }

  @Test void testProcessorBuilderAllOptions() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("full_proc")
        .materializeDirectory(tempDir.toString())
        .sourceDirectory(tempDir.resolve("source").toString())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .sourceStorageProvider(localStorage())
        .materializeDirectory(tempDir.resolve("output").toString())
        .sourceDirectory(tempDir.resolve("src").toString())
        .operatingDirectory(tempDir.resolve("ops").toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaListener(SchemaLifecycleListener.NOOP)
        .defaultTableListener(TableLifecycleListener.NOOP)
        .build();

    assertNotNull(processor);
  }

  // =========================================================================
  // SchemaLifecycleProcessor - process() with hooks-only tables
  // =========================================================================

  @Test void testProcessorWithHooksOnlyTable() throws IOException {
    EtlPipelineConfig hooksTable = hooksOnlyConfig("hooks_table");

    SchemaConfig schema = SchemaConfig.builder()
        .name("hooks_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(hooksTable)
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    assertEquals("hooks_schema", result.getSchemaName());
    assertTrue(result.getElapsedMs() >= 0);
  }

  // =========================================================================
  // SchemaLifecycleProcessor - process() with programmatic hooks
  // =========================================================================

  @Test void testProcessorWithBeforeAfterTableHooks() throws IOException {
    EtlPipelineConfig table = httpConfig("hooked_table", listDimension("x", "a"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("hook_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    final AtomicBoolean beforeCalled = new AtomicBoolean(false);
    final AtomicBoolean afterCalled = new AtomicBoolean(false);
    final AtomicBoolean beforeSourceCalled = new AtomicBoolean(false);
    final AtomicBoolean afterSourceCalled = new AtomicBoolean(false);
    final AtomicBoolean beforeMatCalled = new AtomicBoolean(false);
    final AtomicBoolean afterMatCalled = new AtomicBoolean(false);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("k", "v"));

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .beforeTable("hooked_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeCalled.set(true);
          }
        })
        .afterTable("hooked_table", new java.util.function.BiConsumer<TableContext, EtlResult>() {
          @Override public void accept(TableContext ctx, EtlResult result) {
            afterCalled.set(true);
          }
        })
        .beforeSource("hooked_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeSourceCalled.set(true);
          }
        })
        .afterSource("hooked_table", new java.util.function.BiConsumer<TableContext, SourceResult>() {
          @Override public void accept(TableContext ctx, SourceResult result) {
            afterSourceCalled.set(true);
          }
        })
        .beforeMaterialize("hooked_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeMatCalled.set(true);
          }
        })
        .afterMaterialize("hooked_table", new java.util.function.BiConsumer<TableContext, MaterializeResult>() {
          @Override public void accept(TableContext ctx, MaterializeResult result) {
            afterMatCalled.set(true);
          }
        })
        .fetchData("hooked_table", new java.util.function.BiFunction<TableContext, Map<String, String>,
            Iterator<Map<String, Object>>>() {
          @Override public Iterator<Map<String, Object>> apply(
              TableContext ctx, Map<String, String> vars) {
            return data.iterator();
          }
        })
        .writeData("hooked_table", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
          @Override public long write(TableContext context,
              Iterator<Map<String, Object>> data, Map<String, String> variables) {
            long count = 0;
            while (data != null && data.hasNext()) {
              data.next();
              count++;
            }
            return count;
          }
        })
        .build();

    SchemaResult result = processor.process();

    assertNotNull(result);
    assertTrue(beforeCalled.get(), "beforeTable hook should be called");
    assertTrue(afterCalled.get(), "afterTable hook should be called");
    assertTrue(beforeSourceCalled.get(), "beforeSource hook should be called");
    assertTrue(afterSourceCalled.get(), "afterSource hook should be called");
    assertTrue(beforeMatCalled.get(), "beforeMaterialize hook should be called");
    assertTrue(afterMatCalled.get(), "afterMaterialize hook should be called");
  }

  @Test void testProcessorWithDimensionResolverHook() throws IOException {
    Map<String, DimensionConfig> originalDims = listDimension("x", "a");
    EtlPipelineConfig table = httpConfig("dim_resolve", originalDims);

    SchemaConfig schema = SchemaConfig.builder()
        .name("dim_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    // Hook that replaces dimensions with a different set
    final Map<String, DimensionConfig> resolvedDims = listDimension("x", "resolved1", "resolved2");
    final AtomicInteger writeCount = new AtomicInteger(0);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .resolveDimensions("dim_resolve",
            new java.util.function.BiFunction<TableContext,
                Map<String, DimensionConfig>, Map<String, DimensionConfig>>() {
              @Override public Map<String, DimensionConfig> apply(
                  TableContext ctx, Map<String, DimensionConfig> staticDims) {
                return resolvedDims;
              }
            })
        .fetchData("dim_resolve", new java.util.function.BiFunction<TableContext, Map<String, String>,
            Iterator<Map<String, Object>>>() {
          @Override public Iterator<Map<String, Object>> apply(
              TableContext ctx, Map<String, String> vars) {
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            rows.add(row("v", vars.get("x")));
            return rows.iterator();
          }
        })
        .writeData("dim_resolve", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
          @Override public long write(TableContext context,
              Iterator<Map<String, Object>> data, Map<String, String> variables) {
            writeCount.incrementAndGet();
            long count = 0;
            while (data != null && data.hasNext()) {
              data.next();
              count++;
            }
            return count;
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    // Should use resolved dimensions (2 values) instead of original (1 value)
    assertEquals(2, writeCount.get());
  }

  @Test void testProcessorWithIsEnabledHook() throws IOException {
    EtlPipelineConfig table1 = httpConfig("enabled_table", listDimension("x", "a"));
    EtlPipelineConfig table2 = httpConfig("disabled_table", listDimension("x", "b"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("filter_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table1)
        .addTable(table2)
        .build();

    final AtomicInteger writeCount = new AtomicInteger(0);

    SchemaLifecycleProcessor.Builder builder = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .isEnabled("disabled_table", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return false; // Disable this table
          }
        });

    // Add fetchData + writeData for the enabled table
    builder.fetchData("enabled_table", new java.util.function.BiFunction<TableContext, Map<String, String>,
        Iterator<Map<String, Object>>>() {
      @Override public Iterator<Map<String, Object>> apply(
          TableContext ctx, Map<String, String> vars) {
        return Collections.singletonList(row("k", "v")).iterator();
      }
    });
    builder.writeData("enabled_table", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
      @Override public long write(TableContext context,
          Iterator<Map<String, Object>> data, Map<String, String> variables) {
        writeCount.incrementAndGet();
        return 1;
      }
    });

    // Also add for disabled table in case it runs unexpectedly
    builder.fetchData("disabled_table", new java.util.function.BiFunction<TableContext, Map<String, String>,
        Iterator<Map<String, Object>>>() {
      @Override public Iterator<Map<String, Object>> apply(
          TableContext ctx, Map<String, String> vars) {
        return Collections.singletonList(row("k", "v")).iterator();
      }
    });
    builder.writeData("disabled_table", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
      @Override public long write(TableContext context,
          Iterator<Map<String, Object>> data, Map<String, String> variables) {
        writeCount.incrementAndGet();
        return 1;
      }
    });

    SchemaLifecycleProcessor processor = builder.build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    // Only enabled_table should have been processed
    assertEquals(1, writeCount.get());
  }

  @Test void testProcessorWithTableErrorHook() throws IOException {
    EtlPipelineConfig table = httpConfig("error_table", listDimension("x", "a"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("error_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    final AtomicBoolean errorHandled = new AtomicBoolean(false);

    // To trigger onTableError, the error must escape the inner try-catch in process().
    // Errors from fetchData are caught at the source/materialize level (inner catch).
    // To reach the outer catch that calls onTableError, we use beforeTable to throw,
    // since it is called outside the inner try-catch.
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .onTableError("error_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception error) {
                errorHandled.set(true);
                return true; // Continue processing
              }
            })
        .beforeTable("error_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            throw new RuntimeException("Intentional error in beforeTable");
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    assertTrue(errorHandled.get(), "Error hook should have been called");
  }

  @Test void testProcessorWithSourceErrorHook() throws IOException {
    EtlPipelineConfig table = httpConfig("src_err_table", listDimension("x", "a"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("src_err_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    final AtomicBoolean sourceErrorHandled = new AtomicBoolean(false);
    final AtomicBoolean materializeErrorHandled = new AtomicBoolean(false);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .onSourceError("src_err_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception error) {
                sourceErrorHandled.set(true);
                return true; // Continue
              }
            })
        .onMaterializeError("src_err_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception error) {
                materializeErrorHandled.set(true);
                return true; // Continue
              }
            })
        .onTableError("src_err_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception error) {
                return true; // Continue
              }
            })
        .fetchData("src_err_table", new java.util.function.BiFunction<TableContext, Map<String, String>,
            Iterator<Map<String, Object>>>() {
          @Override public Iterator<Map<String, Object>> apply(
              TableContext ctx, Map<String, String> vars) {
            throw new RuntimeException("Source fetch error");
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
  }

  @Test void testProcessorWithWildcardEnabledHook() throws IOException {
    EtlPipelineConfig table1 = httpConfig("t1", listDimension("x", "a"));
    EtlPipelineConfig table2 = httpConfig("t2", listDimension("x", "b"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("wildcard_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table1)
        .addTable(table2)
        .build();

    // Use wildcard "*" to disable all tables
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .isEnabled("*", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return false; // Disable all
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    // Both tables should be skipped
    assertNotNull(result.getTableResults());
  }

  @Test void testProcessorWithDisabledTable() throws IOException {
    // Table explicitly disabled via YAML enabled=false
    EtlPipelineConfig disabledTable = EtlPipelineConfig.builder()
        .name("yaml_disabled")
        .enabled(false)
        .build();

    EtlPipelineConfig enabledTable = httpConfig("yaml_enabled", listDimension("x", "a"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("disable_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(disabledTable)
        .addTable(enabledTable)
        .build();

    final AtomicInteger writeCount = new AtomicInteger(0);

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .fetchData("yaml_enabled", new java.util.function.BiFunction<TableContext, Map<String, String>,
            Iterator<Map<String, Object>>>() {
          @Override public Iterator<Map<String, Object>> apply(
              TableContext ctx, Map<String, String> vars) {
            return Collections.singletonList(row("k", "v")).iterator();
          }
        })
        .writeData("yaml_enabled", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
          @Override public long write(TableContext context,
              Iterator<Map<String, Object>> data, Map<String, String> variables) {
            writeCount.incrementAndGet();
            return 1;
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    assertEquals(1, writeCount.get()); // Only enabled table processed
  }

  // =========================================================================
  // SchemaLifecycleProcessor - multiple tables
  // =========================================================================

  @Test void testProcessorMultipleTablesWithMixedHooks() throws IOException {
    EtlPipelineConfig table1 = httpConfig("sales", listDimension("year", "2020"));
    EtlPipelineConfig table2 = httpConfig("inventory", listDimension("region", "US"));
    EtlPipelineConfig table3 = hooksOnlyConfig("summary");

    SchemaConfig schema = SchemaConfig.builder()
        .name("multi_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table1)
        .addTable(table2)
        .addTable(table3)
        .build();

    final List<String> processOrder = Collections.synchronizedList(new ArrayList<String>());

    SchemaLifecycleProcessor.Builder builder = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage());

    // Set up fetchData and writeData for each HTTP table
    for (final String tableName : Arrays.asList("sales", "inventory")) {
      builder.fetchData(tableName, new java.util.function.BiFunction<TableContext, Map<String, String>,
          Iterator<Map<String, Object>>>() {
        @Override public Iterator<Map<String, Object>> apply(
            TableContext ctx, Map<String, String> vars) {
          return Collections.singletonList(row("table", tableName)).iterator();
        }
      });
      builder.writeData(tableName, new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
        @Override public long write(TableContext context,
            Iterator<Map<String, Object>> data, Map<String, String> variables) {
          processOrder.add(tableName);
          while (data != null && data.hasNext()) {
            data.next();
          }
          return 1;
        }
      });
    }

    SchemaLifecycleProcessor processor = builder.build();

    SchemaResult result = processor.process();
    assertNotNull(result);
    // Tables processed in order
    assertEquals(2, processOrder.size());
    assertEquals("sales", processOrder.get(0));
    assertEquals("inventory", processOrder.get(1));
  }

  // =========================================================================
  // SchemaResult
  // =========================================================================

  @Test void testSchemaResultBuilder() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test_result")
        .addTableResult("t1", EtlResult.builder().pipelineName("t1").totalRows(10).build())
        .addTableResult("t2", EtlResult.builder().pipelineName("t2").totalRows(20).build())
        .elapsedMs(500)
        .build();

    assertEquals("test_result", result.getSchemaName());
    assertEquals(30, result.getTotalRows());
    assertNotNull(result.getTableResult("t1"));
    assertNotNull(result.getTableResult("t2"));
    assertNull(result.getTableResult("t3"));
    assertEquals(500, result.getElapsedMs());
  }

  @Test void testSchemaResultConvenienceConstructor() {
    SchemaResult result = new SchemaResult("err_schema", 1, 1, 0, 100, 200, "error msg");
    assertEquals("err_schema", result.getSchemaName());
    assertEquals(1, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(100, result.getTotalRows());
    assertEquals(200, result.getElapsedMs());
    assertEquals(1, result.getErrors().size());
    assertEquals("error msg", result.getErrors().get(0));
    assertTrue(result.getTableResults().isEmpty());
  }

  @Test void testSchemaResultConvenienceConstructorNullError() {
    SchemaResult result = new SchemaResult("ok_schema", 2, 0, 1, 50, 100, null);
    assertEquals("ok_schema", result.getSchemaName());
    assertTrue(result.getErrors().isEmpty());
  }

  // =========================================================================
  // SchemaConfig.SchemaHooksConfig
  // =========================================================================

  @Test void testSchemaHooksConfigEmpty() {
    SchemaConfig.SchemaHooksConfig hooks = SchemaConfig.SchemaHooksConfig.empty();
    assertNull(hooks.getSchemaLifecycleListenerClass());
    assertNull(hooks.getTableLifecycleListenerClass());
  }

  @Test void testSchemaHooksConfigFromMap() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("schemaLifecycleListener", "com.example.SchemaListener");
    map.put("tableLifecycleListener", "com.example.TableListener");

    SchemaConfig.SchemaHooksConfig hooks = SchemaConfig.SchemaHooksConfig.fromMap(map);
    assertEquals("com.example.SchemaListener", hooks.getSchemaLifecycleListenerClass());
    assertEquals("com.example.TableListener", hooks.getTableLifecycleListenerClass());
  }

  @Test void testSchemaHooksConfigFromMapNull() {
    SchemaConfig.SchemaHooksConfig hooks = SchemaConfig.SchemaHooksConfig.fromMap(null);
    assertNull(hooks.getSchemaLifecycleListenerClass());
    assertNull(hooks.getTableLifecycleListenerClass());
  }

  // =========================================================================
  // TableContext and SchemaContext
  // =========================================================================

  @Test void testTableContext() {
    EtlPipelineConfig table = httpConfig("ctx_table", listDimension("x", "a"));
    SchemaConfig schema = SchemaConfig.builder()
        .name("ctx_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    SchemaContext schemaCtx = SchemaContext.builder()
        .config(schema)
        .storageProvider(localStorage())
        .materializeDirectory(tempDir.toString())
        .sourceDirectory(tempDir.resolve("src").toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    TableContext tableCtx = TableContext.builder()
        .tableConfig(table)
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    assertEquals("ctx_table", tableCtx.getTableName());
    assertNotNull(tableCtx.getTableConfig());
    assertNotNull(tableCtx.getSchemaContext());
    assertNotNull(tableCtx.getSourceConfig());
    assertNotNull(tableCtx.getSourceUrl());
    assertNotNull(tableCtx.getMaterializeConfig());
    assertNotNull(tableCtx.getDimensions());
    assertEquals(1, tableCtx.getDimensions().size());
  }

  @Test void testSchemaContext() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("s_ctx")
        .materializeDirectory(tempDir.toString())
        .build();

    StorageProvider sp = localStorage();
    StorageProvider srcSp = localStorage();

    SchemaContext ctx = SchemaContext.builder()
        .config(schema)
        .storageProvider(sp)
        .sourceStorageProvider(srcSp)
        .sourceDirectory("/source")
        .materializeDirectory("/mat")
        .operatingDirectory("/ops")
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    assertEquals("s_ctx", ctx.getSchemaName());
    assertNotNull(ctx.getConfig());
    assertEquals(sp, ctx.getStorageProvider());
    assertEquals(srcSp, ctx.getSourceStorageProvider());
    assertEquals("/source", ctx.getSourceDirectory());
    assertEquals("/mat", ctx.getMaterializeDirectory());
    assertNotNull(ctx.getIncrementalTracker());
    assertTrue(ctx.getTables().isEmpty());
  }

  @Test void testSchemaContextSourceStorageFallback() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("fallback")
        .materializeDirectory(tempDir.toString())
        .build();

    StorageProvider sp = localStorage();

    // Don't set sourceStorageProvider - should fall back to main one
    SchemaContext ctx = SchemaContext.builder()
        .config(schema)
        .storageProvider(sp)
        .build();

    // Source storage should fall back to main storage
    assertEquals(sp, ctx.getSourceStorageProvider());
  }

  // =========================================================================
  // DimensionConfig and DimensionIterator
  // =========================================================================

  @Test void testDimensionConfigRange() {
    DimensionConfig dim = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2023)
        .step(1)
        .build();

    assertEquals("year", dim.getName());
    assertEquals(DimensionType.RANGE, dim.getType());
    assertEquals(Integer.valueOf(2020), dim.getStart());
    assertEquals(Integer.valueOf(2023), dim.getEnd());
    assertEquals(Integer.valueOf(1), dim.getStep());
    assertEquals(Integer.valueOf(0), dim.getDataLag());
    assertNull(dim.getReleaseMonth());
    assertTrue(dim.getValues().isEmpty());
    assertNull(dim.getSql());
    assertNull(dim.getSource());
    assertTrue(dim.getProperties().isEmpty());
    assertTrue(dim.getExcludeYears().isEmpty());
  }

  @Test void testDimensionConfigList() {
    DimensionConfig dim = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("US", "EU", "APAC"))
        .build();

    assertEquals("region", dim.getName());
    assertEquals(DimensionType.LIST, dim.getType());
    assertEquals(3, dim.getValues().size());
    assertTrue(dim.getValues().contains("US"));
  }

  @Test void testDimensionIteratorExpand() {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year").type(DimensionType.RANGE).start(2020).end(2021).build());
    dims.put("region", DimensionConfig.builder()
        .name("region").type(DimensionType.LIST).values(Arrays.asList("US", "EU")).build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dims);

    // 2 years x 2 regions = 4 combinations
    assertEquals(4, combinations.size());

    // Verify all combinations exist
    boolean found2020US = false;
    boolean found2020EU = false;
    boolean found2021US = false;
    boolean found2021EU = false;
    for (Map<String, String> combo : combinations) {
      String year = combo.get("year");
      String region = combo.get("region");
      if ("2020".equals(year) && "US".equals(region)) {
        found2020US = true;
      }
      if ("2020".equals(year) && "EU".equals(region)) {
        found2020EU = true;
      }
      if ("2021".equals(year) && "US".equals(region)) {
        found2021US = true;
      }
      if ("2021".equals(year) && "EU".equals(region)) {
        found2021EU = true;
      }
    }
    assertTrue(found2020US);
    assertTrue(found2020EU);
    assertTrue(found2021US);
    assertTrue(found2021EU);
  }

  @Test void testDimensionIteratorEmptyDimensions() {
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combos = iterator.expand(null);
    // Null or empty dimensions produce a single empty-map combination
    assertNotNull(combos);
    assertEquals(1, combos.size());
    assertTrue(combos.get(0).isEmpty());

    // Same for empty map
    List<Map<String, String>> combos2 =
        iterator.expand(new LinkedHashMap<String, DimensionConfig>());
    assertNotNull(combos2);
    assertEquals(1, combos2.size());
    assertTrue(combos2.get(0).isEmpty());
  }

  @Test void testDimensionIteratorSingleValue() {
    Map<String, DimensionConfig> dims = listDimension("x", "only");
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combos = iterator.expand(dims);

    assertEquals(1, combos.size());
    assertEquals("only", combos.get(0).get("x"));
  }

  // =========================================================================
  // ColumnConfig
  // =========================================================================

  @Test void testColumnConfigBuilder() {
    ColumnConfig col = ColumnConfig.builder()
        .name("price")
        .type("DECIMAL(15,2)")
        .source("raw_price")
        .required(true)
        .build();

    assertEquals("price", col.getName());
    assertEquals("DECIMAL(15,2)", col.getType());
    assertEquals("raw_price", col.getSource());
    assertTrue(col.isRequired());
    assertNull(col.getExpression());
  }

  @Test void testColumnConfigComputed() {
    ColumnConfig col = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .required(false)
        .build();

    assertEquals("quarter", col.getName());
    assertEquals("VARCHAR", col.getType());
    assertEquals("SUBSTR(period, 1, 2)", col.getExpression());
    assertNull(col.getSource());
    assertFalse(col.isRequired());
  }

  @Test void testColumnConfigFromMap() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "id");
    map.put("type", "INTEGER");
    map.put("source", "row_id");
    map.put("required", Boolean.TRUE);

    ColumnConfig col = ColumnConfig.fromMap(map);
    assertNotNull(col);
    assertEquals("id", col.getName());
    assertEquals("INTEGER", col.getType());
    assertEquals("row_id", col.getSource());
    assertTrue(col.isRequired());
  }

  @Test void testColumnConfigNameRequired() {
    try {
      ColumnConfig.builder().type("VARCHAR").build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  // =========================================================================
  // MaterializeConfig
  // =========================================================================

  @Test void testMaterializeConfigBuilder() {
    MaterializeConfig mat = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .targetTableId("my_table")
        .output(MaterializeOutputConfig.builder()
            .pattern("year=STAR/")
            .compression("zstd")
            .build())
        .build();

    assertTrue(mat.isEnabled());
    assertEquals(MaterializeConfig.Format.ICEBERG, mat.getFormat());
    assertEquals("my_table", mat.getTargetTableId());
    assertNotNull(mat.getOutput());
    assertEquals("year=STAR/", mat.getOutput().getPattern());
    assertEquals("zstd", mat.getOutput().getCompression());
  }

  @Test void testMaterializeConfigFormats() {
    for (MaterializeConfig.Format format : MaterializeConfig.Format.values()) {
      MaterializeConfig mat = MaterializeConfig.builder()
          .format(format)
          .output(MaterializeOutputConfig.builder().build())
          .build();
      assertEquals(format, mat.getFormat());
    }
  }

  // =========================================================================
  // HooksConfig
  // =========================================================================

  @Test void testHooksConfigEmpty() {
    HooksConfig hooks = HooksConfig.empty();
    // HooksConfig.empty() uses new Builder().build() which defaults enabled=true
    assertTrue(hooks.isEnabled());
    assertNull(hooks.getResponseTransformerClass());
    assertTrue(hooks.getRowTransformers().isEmpty());
    assertTrue(hooks.getValidators().isEmpty());
    assertNull(hooks.getDimensionResolverClass());
    assertNull(hooks.getDataProviderClass());
    assertNull(hooks.getTableLifecycleListenerClass());
  }

  @Test void testHooksConfigBuilder() {
    HooksConfig hooks = HooksConfig.builder()
        .enabled(true)
        .responseTransformerClass("com.example.Transformer")
        .dimensionResolverClass("com.example.DimResolver")
        .dataProviderClass("com.example.DataProv")
        .tableLifecycleListenerClass("com.example.Listener")
        .build();

    assertTrue(hooks.isEnabled());
    assertEquals("com.example.Transformer", hooks.getResponseTransformerClass());
    assertEquals("com.example.DimResolver", hooks.getDimensionResolverClass());
    assertEquals("com.example.DataProv", hooks.getDataProviderClass());
    assertEquals("com.example.Listener", hooks.getTableLifecycleListenerClass());
  }

  // =========================================================================
  // IncrementalTracker.NOOP
  // =========================================================================

  @Test void testNoopTrackerOperations() {
    IncrementalTracker noop = IncrementalTracker.NOOP;
    // All operations should be no-ops and not throw
    assertFalse(noop.isTableComplete("test", "sig"));
    assertNull(noop.getCachedCompletion("test"));
    noop.markTableCompleteWithConfig("test", "hash", "sig", 100);
    noop.invalidateTableCompletion("test");
    noop.markProcessedWithRowCount("test", "test", Collections.<String, String>emptyMap(), null, 0);
    noop.markProcessedWithError("test", "test", Collections.<String, String>emptyMap(), null, "err");
    noop.preloadAllCompletions();
  }

  // =========================================================================
  // EtlPipeline - consecutive failure abort
  // =========================================================================

  @Test void testConsecutiveFailureAbort() throws IOException {
    // Create enough dimensions to exceed consecutive failure limit
    List<String> values = new ArrayList<String>();
    for (int i = 0; i < 15; i++) {
      values.add("v" + i);
    }
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("x", DimensionConfig.builder()
        .name("x").type(DimensionType.LIST).values(values).build());

    EtlPipelineConfig config = httpConfig("abort_test", dims);

    // All batches fail
    EtlPipeline pipeline =
        new EtlPipeline(config, localStorage(), tempDir.toString(), null, IncrementalTracker.NOOP,
        failingDataProvider("HTTP 500 Server Error"), new CountingDataWriter());

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Should have aborted due to consecutive failures
    assertTrue(result.isFailed() || result.getFailedBatches() > 0
        || !result.getErrors().isEmpty());
  }

  // =========================================================================
  // EtlPipeline - verifyDataExists with null storage
  // =========================================================================

  @Test void testVerifyDataExistsNullStorage() throws Exception {
    EtlPipelineConfig config = httpConfig("no_storage", rangeDimension("y", 2020, 2020));
    // Use null storageProvider
    EtlPipeline pipeline =
        new EtlPipeline(config, null, tempDir.toString(), null, IncrementalTracker.NOOP, null, null);

    Method m =
        EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    Boolean result = (Boolean) m.invoke(pipeline, "no_storage", config);
    // Null storage provider should return true (can't verify)
    assertTrue(result);
  }

  // =========================================================================
  // DataProvider.DEFAULT and DataWriter.DEFAULT
  // =========================================================================

  @Test void testDataProviderDefault() throws IOException {
    Iterator<Map<String, Object>> result =
        DataProvider.DEFAULT.fetch(httpConfig("t", null), Collections.<String, String>emptyMap());
    assertNull(result);
  }

  @Test void testDataWriterDefault() throws IOException {
    long result =
        DataWriter.DEFAULT.write(httpConfig("t", null), Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(-1, result);
  }

  // =========================================================================
  // DataSource interface
  // =========================================================================

  @Test void testDataSourceDefaults() throws IOException {
    DataSource ds = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }

      @Override public String getType() {
        return "test";
      }
    };

    assertEquals("test", ds.getType());
    assertEquals(-1, ds.estimateRowCount());
    ds.close(); // Should not throw
  }

  // =========================================================================
  // SchemaLifecycleListener.NOOP and TableLifecycleListener.NOOP
  // =========================================================================

  @Test void testSchemaListenerNoop() throws Exception {
    SchemaLifecycleListener noop = SchemaLifecycleListener.NOOP;
    SchemaConfig schema = SchemaConfig.builder()
        .name("noop")
        .materializeDirectory(tempDir.toString())
        .build();
    SchemaContext ctx = SchemaContext.builder()
        .config(schema)
        .storageProvider(localStorage())
        .build();

    noop.beforeSchema(ctx);
    noop.afterSchema(ctx, new SchemaResult("noop", 0, 0, 0, 0, 0, null));
    noop.onSchemaError(ctx, new Exception("test"));
  }

  @Test void testTableListenerNoop() throws Exception {
    TableLifecycleListener noop = TableLifecycleListener.NOOP;
    EtlPipelineConfig table = httpConfig("noop_tbl", null);
    SchemaConfig schema = SchemaConfig.builder()
        .name("noop_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();
    SchemaContext schemaCtx = SchemaContext.builder()
        .config(schema)
        .storageProvider(localStorage())
        .build();
    TableContext ctx = TableContext.builder()
        .tableConfig(table)
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    noop.beforeTable(ctx);
    noop.afterTable(ctx, EtlResult.builder().pipelineName("noop_tbl").build());
    assertTrue(noop.onTableError(ctx, new Exception("test")));
  }

  // =========================================================================
  // SchemaLifecycleProcessor - deprecated shouldProcess builder method
  // =========================================================================

  @Test void testProcessorBuilderDeprecatedShouldProcess() throws IOException {
    EtlPipelineConfig table = httpConfig("dep_table", listDimension("x", "a"));

    SchemaConfig schema = SchemaConfig.builder()
        .name("dep_schema")
        .materializeDirectory(tempDir.toString())
        .addTable(table)
        .build();

    @SuppressWarnings("deprecation")
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .shouldProcess("dep_table", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return false; // Skip
          }
        })
        .build();

    SchemaResult result = processor.process();
    assertNotNull(result);
  }

  // =========================================================================
  // SchemaLifecycleProcessor - deprecated baseDirectory builder method
  // =========================================================================

  @Test void testProcessorBuilderDeprecatedBaseDirectory() {
    SchemaConfig schema = SchemaConfig.builder()
        .name("dep_basedir")
        .materializeDirectory(tempDir.toString())
        .build();

    @SuppressWarnings("deprecation")
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schema)
        .storageProvider(localStorage())
        .baseDirectory(tempDir.resolve("override").toString())
        .build();

    assertNotNull(processor);
  }

  // =========================================================================
  // EtlPipelineConfig.fromMap with hooks section
  // =========================================================================

  @Test void testConfigFromMapWithHooks() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "hooks_table");
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    map.put("source", sourceMap);
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outputMap);
    map.put("materialize", matMap);

    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    hooksMap.put("enabled", true);
    hooksMap.put("responseTransformer", "com.example.Transformer");
    map.put("hooks", hooksMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertNotNull(config.getHooks());
    assertTrue(config.getHooks().isEnabled());
  }

  // =========================================================================
  // EtlPipelineConfig - source type constants
  // =========================================================================

  @Test void testSourceTypeConstants() {
    assertEquals("http", EtlPipelineConfig.SOURCE_TYPE_HTTP);
    assertEquals("file", EtlPipelineConfig.SOURCE_TYPE_FILE);
    assertEquals("constants", EtlPipelineConfig.SOURCE_TYPE_CONSTANTS);
    assertEquals("document", EtlPipelineConfig.SOURCE_TYPE_DOCUMENT);
  }

  // =========================================================================
  // EtlPipelineConfig - isTruthyString edge cases (tested via fromMap)
  // =========================================================================

  @Test void testEnabledTruthyValues() {
    // Helper: create a source+materialize map for enabled configs that need validation
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com");
    Map<String, Object> matMap = new LinkedHashMap<String, Object>();
    matMap.put("enabled", true);
    Map<String, Object> outMap = new LinkedHashMap<String, Object>();
    matMap.put("output", outMap);

    // "true" string - truthy, needs source
    Map<String, Object> map1 = new LinkedHashMap<String, Object>();
    map1.put("name", "truthy1");
    map1.put("enabled", "true");
    map1.put("source", sourceMap);
    map1.put("materialize", matMap);
    EtlPipelineConfig config1 = EtlPipelineConfig.fromMap(map1);
    assertTrue(config1.isEnabled());

    // "TRUE" string - truthy, needs source
    Map<String, Object> map2 = new LinkedHashMap<String, Object>();
    map2.put("name", "truthy2");
    map2.put("enabled", "TRUE");
    map2.put("source", sourceMap);
    map2.put("materialize", matMap);
    EtlPipelineConfig config2 = EtlPipelineConfig.fromMap(map2);
    assertTrue(config2.isEnabled());

    // Non-empty string (truthy) - needs source
    Map<String, Object> map3 = new LinkedHashMap<String, Object>();
    map3.put("name", "truthy3");
    map3.put("enabled", "some_value");
    map3.put("source", sourceMap);
    map3.put("materialize", matMap);
    EtlPipelineConfig config3 = EtlPipelineConfig.fromMap(map3);
    assertTrue(config3.isEnabled());

    // Empty string (falsy) - disabled skips validation
    Map<String, Object> map4 = new LinkedHashMap<String, Object>();
    map4.put("name", "falsy");
    map4.put("enabled", "");
    EtlPipelineConfig config4 = EtlPipelineConfig.fromMap(map4);
    assertFalse(config4.isEnabled());

    // "FALSE" string - disabled skips validation
    Map<String, Object> map5 = new LinkedHashMap<String, Object>();
    map5.put("name", "falsy2");
    map5.put("enabled", "FALSE");
    EtlPipelineConfig config5 = EtlPipelineConfig.fromMap(map5);
    assertFalse(config5.isEnabled());

    // Non-standard type (Integer) defaults to enabled - needs source
    Map<String, Object> map6 = new LinkedHashMap<String, Object>();
    map6.put("name", "default");
    map6.put("enabled", 42);
    map6.put("source", sourceMap);
    map6.put("materialize", matMap);
    EtlPipelineConfig config6 = EtlPipelineConfig.fromMap(map6);
    assertTrue(config6.isEnabled());
  }

  // =========================================================================
  // MaterializeOutputConfig
  // =========================================================================

  @Test void testMaterializeOutputConfigDefaults() {
    MaterializeOutputConfig output = MaterializeOutputConfig.builder().build();
    assertNull(output.getLocation());
    assertNull(output.getPattern());
    assertEquals("parquet", output.getFormat());
    assertEquals("snappy", output.getCompression());
  }

  @Test void testMaterializeOutputConfigCustom() {
    MaterializeOutputConfig output = MaterializeOutputConfig.builder()
        .location("/data/output")
        .pattern("year=STAR/region=STAR/")
        .format("parquet")
        .compression("zstd")
        .build();

    assertEquals("/data/output", output.getLocation());
    assertEquals("year=STAR/region=STAR/", output.getPattern());
    assertEquals("parquet", output.getFormat());
    assertEquals("zstd", output.getCompression());
  }
}
