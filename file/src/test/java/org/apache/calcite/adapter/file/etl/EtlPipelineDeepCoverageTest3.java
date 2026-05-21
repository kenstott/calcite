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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link EtlPipeline} targeting uncovered lines.
 */
@Tag("unit")
public class EtlPipelineDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions) {
    return createHttpConfig(name, dimensions, null, null, null);
  }

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions,
      MaterializeConfig.Format format,
      MaterializeOptionsConfig options,
      List<ColumnConfig> columns) {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig.Builder matBuilder = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build());
    if (format != null) {
      matBuilder.format(format);
    }
    if (options != null) {
      matBuilder.options(options);
    }
    EtlPipelineConfig.Builder configBuilder = EtlPipelineConfig.builder()
        .name(name)
        .source(source)
        .materialize(matBuilder.build());
    if (dimensions != null) {
      configBuilder.dimensions(dimensions);
    }
    if (columns != null) {
      configBuilder.columns(columns);
    }
    return configBuilder.build();
  }

  private Map<String, DimensionConfig> singleRangeDimension(String key, int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.RANGE).start(start).end(end).build());
    return dims;
  }

  private IncrementalTracker mockTracker() {
    IncrementalTracker tracker = mock(IncrementalTracker.class);
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    when(tracker.getCachedCompletion(anyString())).thenReturn(null);
    return tracker;
  }

  private StorageProvider mockStorage() {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.getStorageType()).thenReturn("local");
    return sp;
  }

  private Map<String, Object> row(String... kvPairs) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      r.put(kvPairs[i], kvPairs[i + 1]);
    }
    return r;
  }

  // ===== determineErrorAction =====

  @Test void testDetermineErrorAction_authError401() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 401 Unauthorized"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL, r);
  }

  @Test void testDetermineErrorAction_authError403() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 403 Forbidden"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL, r);
  }

  @Test void testDetermineErrorAction_notFound404() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 404 Not Found"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, r);
  }

  @Test void testDetermineErrorAction_transient429() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 429 Too Many Requests"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY, r);
  }

  @Test void testDetermineErrorAction_transient503() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 503 Service Unavailable"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY, r);
  }

  @Test void testDetermineErrorAction_server500() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("HTTP 500 Internal Server Error"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, r);
  }

  @Test void testDetermineErrorAction_nullMessage() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException((String) null), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, r);
  }

  @Test void testDetermineErrorAction_genericError() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object r =
        m.invoke(pipeline, new IOException("Connection timed out"), EtlPipelineConfig.ErrorHandlingConfig.defaults());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, r);
  }

  // ===== allIndicesSet =====

  @Test void testAllIndicesSet() throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<Integer> r = (Set<Integer>) m.invoke(null, 5);
    assertEquals(5, r.size());
    for (int i = 0; i < 5; i++) {
      assertTrue(r.contains(i));
    }
  }

  @Test void testAllIndicesSetZero() throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<Integer> r = (Set<Integer>) m.invoke(null, 0);
    assertTrue(r.isEmpty());
  }

  // ===== getParallelThreadCount =====

  @Test void testGetParallelThreadCountDefault() throws Exception {
    String orig = System.getProperty("calcite.etl.threads");
    try {
      System.clearProperty("calcite.etl.threads");
      // getParallelThreadCount is an instance method, so we need an EtlPipeline instance
      EtlPipelineConfig config = createHttpConfig("test_threads", null);
      StorageProvider sp = mock(StorageProvider.class);
      EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
      Method m = EtlPipeline.class.getDeclaredMethod("getParallelThreadCount");
      m.setAccessible(true);
      assertEquals(1, (int) m.invoke(pipeline));
    } finally {
      if (orig != null) {
        System.setProperty("calcite.etl.threads", orig);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testGetParallelThreadCountConfigured() throws Exception {
    String orig = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "4");
      // getParallelThreadCount is an instance method, so we need an EtlPipeline instance
      EtlPipelineConfig config = createHttpConfig("test_threads", null);
      StorageProvider sp = mock(StorageProvider.class);
      EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
      Method m = EtlPipeline.class.getDeclaredMethod("getParallelThreadCount");
      m.setAccessible(true);
      assertEquals(4, (int) m.invoke(pipeline));
    } finally {
      if (orig != null) {
        System.setProperty("calcite.etl.threads", orig);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testGetParallelThreadCountInvalid() throws Exception {
    String orig = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "abc");
      // getParallelThreadCount is an instance method, so we need an EtlPipeline instance
      EtlPipelineConfig config = createHttpConfig("test_threads", null);
      StorageProvider sp = mock(StorageProvider.class);
      EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
      Method m = EtlPipeline.class.getDeclaredMethod("getParallelThreadCount");
      m.setAccessible(true);
      assertEquals(1, (int) m.invoke(pipeline));
    } finally {
      if (orig != null) {
        System.setProperty("calcite.etl.threads", orig);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  // ===== verifyDataExists =====

  @Test void testVerifyDataExists_nullMaterialize() throws Exception {
    HooksConfig hooks = HooksConfig.builder().enabled(true).build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("t").source(HttpSourceConfig.builder().url("http://x").build())
        .hooks(hooks).build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_disabled() throws Exception {
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build()).enabled(false).build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("t").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mat).build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_nullStorage() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, null, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_icebergMetadataExists() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    when(sp.isDirectory(tempDir.toString() + "/t/metadata")).thenReturn(true);
    EtlPipelineConfig config =
        createHttpConfig("t", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_icebergNoMetadata() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    EtlPipelineConfig config =
        createHttpConfig("t", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertFalse((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_parquetDirExists() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(tempDir.toString() + "/t")).thenReturn(true);
    EtlPipelineConfig config =
        createHttpConfig("t", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_parquetDirNotExists() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    EtlPipelineConfig config =
        createHttpConfig("t", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertFalse((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_ioException() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenThrow(new IOException("fail"));
    EtlPipelineConfig config =
        createHttpConfig("t", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_icebergWithWarehouse() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory("/wh/t/metadata")).thenReturn(true);
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder().warehousePath("/wh").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("t")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mat).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  @Test void testVerifyDataExists_icebergWarehouseFallback() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory("/wh/t/metadata")).thenReturn(false);
    when(sp.isDirectory(tempDir.toString() + "/t/metadata")).thenReturn(true);
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder().warehousePath("/wh").build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("t")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mat).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("verifyDataExists", String.class, EtlPipelineConfig.class);
    m.setAccessible(true);
    assertTrue((boolean) m.invoke(pipeline, "t", config));
  }

  // ===== loadDimensionResolver =====

  @Test void testLoadDimensionResolver_nullHooks() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("loadDimensionResolver", HooksConfig.class);
    m.setAccessible(true);
    assertNull(m.invoke(pipeline, (HooksConfig) null));
  }

  @Test void testLoadDimensionResolver_noResolver() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("loadDimensionResolver", HooksConfig.class);
    m.setAccessible(true);
    assertNull(m.invoke(pipeline, HooksConfig.empty()));
  }

  @Test void testLoadDimensionResolver_classNotFound() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("loadDimensionResolver", HooksConfig.class);
    m.setAccessible(true);
    HooksConfig hooks = HooksConfig.builder()
        .dimensionResolverClass("com.nonexistent.FakeResolver").build();
    try {
      m.invoke(pipeline, hooks);
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("not found"));
    }
  }

  @Test void testLoadDimensionResolver_wrongInterface() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("loadDimensionResolver", HooksConfig.class);
    m.setAccessible(true);
    HooksConfig hooks = HooksConfig.builder()
        .dimensionResolverClass("java.lang.String").build();
    try {
      m.invoke(pipeline, hooks);
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("DimensionResolver"));
    }
  }

  // ===== buildIcebergCatalogConfig =====

  @Test void testBuildIcebergCatalogConfig_noIceberg() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertEquals("hadoop", r.get("catalog"));
    assertEquals(tempDir.toString(), r.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_restCatalog() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.REST)
            .restUri("http://localhost:8181").warehousePath("/wh").build())
        .build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertEquals("rest", r.get("catalog"));
    assertEquals("http://localhost:8181", r.get("uri"));
  }

  @Test void testBuildIcebergCatalogConfig_hiveCatalog() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HIVE)
            .warehousePath("/wh").build())
        .build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertEquals("hive", r.get("catalog"));
  }

  @Test void testBuildIcebergCatalogConfig_s3Path() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath("s3://bucket/wh").build())
        .build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertEquals("s3a://bucket/wh", r.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_noIcebergS3Base() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), "s3://bucket/path");
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertEquals("s3a://bucket/path", r.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_withS3Config() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, String> s3 = new HashMap<String, String>();
    s3.put("accessKeyId", "AK");
    s3.put("secretAccessKey", "SK");
    s3.put("endpoint", "http://localhost:9000");
    s3.put("region", "us-east-1");
    when(sp.getS3Config()).thenReturn(s3);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    @SuppressWarnings("unchecked")
    Map<String, String> hc = (Map<String, String>) r.get("hadoopConfig");
    assertNotNull(hc);
    assertEquals("AK", hc.get("fs.s3a.access.key"));
    assertEquals("SK", hc.get("fs.s3a.secret.key"));
    assertEquals("http://localhost:9000", hc.get("fs.s3a.endpoint"));
    assertEquals("us-east-1", hc.get("fs.s3a.endpoint.region"));
  }

  @Test void testBuildIcebergCatalogConfig_nullStorageProvider() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, null, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("buildIcebergCatalogConfig", MaterializeConfig.class);
    m.setAccessible(true);
    MaterializeConfig mat = MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build();
    @SuppressWarnings("unchecked")
    Map<String, Object> r = (Map<String, Object>) m.invoke(pipeline, mat);
    assertNull(r.get("hadoopConfig"));
  }

  // ===== processSingleBatch =====

  @Test void testProcessSingleBatch_docSourceWithWriter() throws Exception {
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(), any(Map.class))).thenReturn(42L);
    IncrementalTracker tracker = mockTracker();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("d").sourceType("document")
        .rawSourceConfig(Collections.<String, Object>singletonMap("type", "document"))
        .materialize(MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, dw);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    Map<String, String> vars = new HashMap<String, String>();
    long rows = (long) m.invoke(pipeline, config, vars, null, mock(MaterializationWriter.class), 1, 1, "d");
    assertEquals(42L, rows);
  }

  @Test void testProcessSingleBatch_docSourceNoWriter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("d").sourceType("document")
        .rawSourceConfig(Collections.<String, Object>singletonMap("type", "document"))
        .materialize(MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    long rows =
        (long) m.invoke(pipeline, config, new HashMap<String, String>(), null, mock(MaterializationWriter.class), 1, 1, "d");
    assertEquals(0L, rows);
  }

  @Test void testProcessSingleBatch_customProvider() throws Exception {
    DataProvider prov = mock(DataProvider.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class))).thenReturn(data.iterator());
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(1L);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, prov, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    long rows =
        (long) m.invoke(pipeline, config, new HashMap<String, String>(), mock(DataSource.class), w, 1, 1, "t");
    assertEquals(1L, rows);
  }

  @Test void testProcessSingleBatch_customProviderNull() throws Exception {
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class))).thenReturn(null);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(1L);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, prov, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    Map<String, String> vars = new HashMap<String, String>();
    long rows = (long) m.invoke(pipeline, config, vars, ds, w, 1, 1, "t");
    assertEquals(1L, rows);
    verify(ds).fetch(vars);
  }

  @Test void testProcessSingleBatch_customDataWriter() throws Exception {
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(Iterator.class), any(Map.class))).thenReturn(5L);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, dw);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    long rows = (long) m.invoke(pipeline, config, new HashMap<String, String>(), ds, w, 1, 1, "t");
    assertEquals(5L, rows);
  }

  @Test void testProcessSingleBatch_customDataWriterNegative() throws Exception {
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(Iterator.class), any(Map.class))).thenReturn(-1L);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(10L);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, dw);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, int.class, String.class);
    m.setAccessible(true);
    long rows = (long) m.invoke(pipeline, config, new HashMap<String, String>(), ds, w, 1, 1, "t");
    assertEquals(10L, rows);
    verify(w).writeBatch(any(Iterator.class), any(Map.class));
  }

  // ===== writeWithResponsePartitioning =====

  @Test void testWriteWithResponsePartitioning_empty() throws Exception {
    IncrementalTracker tracker = mockTracker();
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, DataWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    HttpSourceConfig.ResponsePartitioningConfig rp = HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    Map<String, String> urlVars = new HashMap<String, String>();
    long rows =
        (long) m.invoke(pipeline, Collections.emptyList().iterator(), urlVars, rp, mock(MaterializationWriter.class), null, tracker, "t");
    assertEquals(0L, rows);
  }

  @Test void testWriteWithResponsePartitioning_withData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(3L);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, DataWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    HttpSourceConfig.ResponsePartitioningConfig rp = HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    data.add(row("id", "3", "year", "2022"));
    long rows =
        (long) m.invoke(pipeline, data.iterator(), new HashMap<String, String>(), rp, w, null, tracker, "t");
    assertEquals(3L, rows);
  }

  @Test void testWriteWithResponsePartitioning_yearFilter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(1L);
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, DataWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    rpMap.put("yearField", "year");
    rpMap.put("yearStart", 2021);
    rpMap.put("yearEnd", 2022);
    HttpSourceConfig.ResponsePartitioningConfig rp = HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    data.add(row("id", "3", "year", "2023"));
    long rows =
        (long) m.invoke(pipeline, data.iterator(), new HashMap<String, String>(), rp, w, null, tracker, "t");
    assertEquals(1L, rows);
  }

  // ===== createDataSource =====

  @Test void testCreateDataSource_document() throws Exception {
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("d").sourceType("document")
        .rawSourceConfig(Collections.<String, Object>singletonMap("type", "document"))
        .materialize(MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    assertNull(m.invoke(pipeline, config));
  }

  @Test void testCreateDataSource_constants() throws Exception {
    Map<String, Object> raw = new HashMap<String, Object>();
    raw.put("type", "constants");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(row("code", "US"));
    raw.put("rows", rows);
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("c").sourceType("constants")
        .rawSourceConfig(raw)
        .materialize(MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    DataSource ds = (DataSource) m.invoke(pipeline, config);
    assertNotNull(ds);
    assertEquals("constants", ds.getType());
  }

  @Test void testCreateDataSource_httpWithRawCache() throws Exception {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("h")
        .source(source)
        .materialize(MaterializeConfig.builder().output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    assertNotNull(m.invoke(pipeline, config));
  }

  @Test void testCreateDataSource_httpDefault() throws Exception {
    EtlPipelineConfig config = createHttpConfig("h", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, mockStorage(), tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    assertNotNull(m.invoke(pipeline, config));
  }

  // ===== execute() fast-path scenarios =====

  @Test void testExecute_emptyDimProducesOneCombination() throws Exception {
    EtlPipelineConfig config =
        createHttpConfig("e", Collections.<String, DimensionConfig>emptyMap());
    IncrementalTracker tracker = mockTracker();
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    assertNotNull(r);
    assertEquals("e", r.getPipelineName());
  }

  @Test void testExecute_cachedCompletionMatch() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2020);
    String hash = IncrementalTracker.computeConfigHash(dims);
    when(tracker.getCachedCompletion("c")).thenReturn(
        new IncrementalTracker.CachedCompletion(hash, "sig", 100));
    when(sp.isDirectory(anyString())).thenReturn(true);
    EtlPipelineConfig config = createHttpConfig("c", dims, MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult r = pipeline.execute();
    assertTrue(r.isSkippedEntirePipeline());
    assertEquals(100, r.getTotalRows());
  }

  @Test void testExecute_cachedMismatchDataExists() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(tracker.getCachedCompletion("m")).thenReturn(
        new IncrementalTracker.CachedCompletion("old", "sig", 50));
    when(sp.isDirectory(anyString())).thenReturn(true);
    EtlPipelineConfig config =
        createHttpConfig("m", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult r = pipeline.execute();
    assertTrue(r.isSkippedEntirePipeline());
    assertEquals(50, r.getTotalRows());
  }

  @Test void testExecute_cachedMatchZeroTtlNotExpired() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2020);
    String hash = IncrementalTracker.computeConfigHash(dims);
    when(tracker.getCachedCompletion("z")).thenReturn(
        new IncrementalTracker.CachedCompletion(hash, "sig", 0, System.currentTimeMillis()));
    when(sp.isDirectory(anyString())).thenReturn(true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder().emptyResultTtlDays(7).build();
    EtlPipelineConfig config = createHttpConfig("z", dims, MaterializeConfig.Format.ICEBERG, opts, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult r = pipeline.execute();
    assertTrue(r.isSkippedEntirePipeline());
    assertEquals(0, r.getTotalRows());
  }

  @Test void testExecute_cachedMatchZeroTtlExpired() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2020);
    String hash = IncrementalTracker.computeConfigHash(dims);
    when(tracker.getCachedCompletion("e")).thenReturn(
        new IncrementalTracker.CachedCompletion(hash, "sig", 0, 1000L));
    when(sp.isDirectory(anyString())).thenReturn(true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder().emptyResultTtlDays(1).build();
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config = createHttpConfig("e", dims, MaterializeConfig.Format.PARQUET, opts, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    assertFalse(r.isSkippedEntirePipeline());
  }

  @Test void testExecute_cachedMatchDataNotExists() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2020);
    String hash = IncrementalTracker.computeConfigHash(dims);
    when(tracker.getCachedCompletion("s")).thenReturn(
        new IncrementalTracker.CachedCompletion(hash, "sig", 100));
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config = createHttpConfig("s", dims, MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    verify(tracker).invalidateTableCompletion("s");
  }

  @Test void testExecute_allAlreadyProcessed() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(Collections.<Integer>emptySet());
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(Collections.<Integer>emptySet());
    EtlPipelineConfig config =
        createHttpConfig("a", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult r = pipeline.execute();
    assertEquals(0, r.getTotalRows());
  }

  @Test void testExecute_tableCompleteButDataGone() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(true);
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("g", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    pipeline.execute();
    verify(tracker).invalidateTableCompletion("g");
  }

  @Test void testExecute_tableCompleteNoMaterialize() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(true);
    when(sp.isDirectory(anyString())).thenReturn(true);
    HooksConfig hooks = HooksConfig.builder().enabled(true).build();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("n")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .hooks(hooks).dimensions(singleRangeDimension("y", 2020, 2020)).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult r = pipeline.execute();
    assertTrue(r.isSkippedEntirePipeline());
  }

  @Test void testExecute_withProgressListener() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("p", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onPhaseStart("dimension_expansion", 1);
    verify(listener).onPhaseComplete("dimension_expansion", 1);
    verify(listener).onPhaseStart("data_processing", 1);
  }

  @Test void testExecute_exceptionInProcessing() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new RuntimeException("Unexpected"));
    EtlPipelineConfig config =
        createHttpConfig("x", singleRangeDimension("y", 2020, 2020), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    assertTrue(r.isFailed());
  }

  // ===== Constructor and factory tests =====

  @Test void testCreateFactory() {
    EtlPipeline p =
        EtlPipeline.create(createHttpConfig("f", singleRangeDimension("y", 2020, 2020)),
        mockStorage(), tempDir.toString());
    assertNotNull(p);
  }

  @Test void testConstructorVariations() {
    EtlPipelineConfig config = createHttpConfig("c", singleRangeDimension("y", 2020, 2020));
    StorageProvider sp = mockStorage();
    assertNotNull(new EtlPipeline(config, sp, tempDir.toString()));
    assertNotNull(new EtlPipeline(config, sp, tempDir.toString(), mock(EtlPipeline.ProgressListener.class)));
    assertNotNull(new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP));
    assertNotNull(new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null));
    assertNotNull(new EtlPipeline(config, sp, sp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null));
    assertNotNull(new EtlPipeline(config, sp, sp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null, "/op"));
  }

  @Test void testSourceStorageDefaultsToMain() throws Exception {
    EtlPipelineConfig config = createHttpConfig("s", singleRangeDimension("y", 2020, 2020));
    StorageProvider sp = mockStorage();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, null, tempDir.toString(), null, IncrementalTracker.NOOP, null, null, null);
    Field f = EtlPipeline.class.getDeclaredField("sourceStorageProvider");
    f.setAccessible(true);
    assertEquals(sp, f.get(pipeline));
  }

  @Test void testTrackerDefaultsToNoop() throws Exception {
    EtlPipelineConfig config = createHttpConfig("t", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), null, tempDir.toString(), null, null, null, null, null);
    Field f = EtlPipeline.class.getDeclaredField("incrementalTracker");
    f.setAccessible(true);
    assertNotNull(f.get(pipeline));
  }

  // ===== LoggingProgressListener =====

  @Test void testLoggingProgressListener() {
    EtlPipeline.LoggingProgressListener l = new EtlPipeline.LoggingProgressListener();
    l.onPhaseStart("p", 10);
    l.onPhaseComplete("p", 10);
    Map<String, String> v = new HashMap<String, String>();
    v.put("y", "2024");
    l.onBatchStart(1, 10, v);
    l.onBatchComplete(1, 10, 500, null);
    l.onBatchComplete(2, 10, 0, new IOException("err"));
  }

  // ===== execute() sequential error handling =====

  @Test void testExecute_skipAction() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    allIdx.add(1);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(data.iterator())
        .thenThrow(new IOException("HTTP 404 Not Found"));
    EtlPipelineConfig config =
        createHttpConfig("sk", singleRangeDimension("y", 2020, 2021), MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    assertNotNull(r);
  }

  @Test void testExecute_warnAction() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    Set<Integer> allIdx = new HashSet<Integer>();
    allIdx.add(0);
    allIdx.add(1);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(allIdx);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenThrow(new IOException("HTTP 500 Internal Server Error"));
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder().name("w")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2021))
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .format(MaterializeConfig.Format.PARQUET).build())
        .errorHandling(eh).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult r = pipeline.execute();
    assertTrue(r.getFailedBatches() > 0 || !r.getErrors().isEmpty());
  }
}
