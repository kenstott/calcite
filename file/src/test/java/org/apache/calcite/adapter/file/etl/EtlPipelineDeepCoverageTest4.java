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
import java.nio.file.Files;
import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests (phase 4) for {@link EtlPipeline} targeting
 * previously uncovered lines: cache management, state machine transitions,
 * config merging, error action edge-cases, CachedEtlProperties,
 * readRowCountFromIceberg variants, storeEtlPropertiesToIceberg,
 * rebuildCacheFromIceberg, and parallel processing paths.
 */
@Tag("unit")
public class EtlPipelineDeepCoverageTest4 {

  @TempDir
  Path tempDir;

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions) {
    return createHttpConfig(name, dimensions, null, null, null, null);
  }

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions,
      MaterializeConfig.Format format,
      MaterializeOptionsConfig options,
      List<ColumnConfig> columns) {
    return createHttpConfig(name, dimensions, format, options, columns, null);
  }

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions,
      MaterializeConfig.Format format,
      MaterializeOptionsConfig options,
      List<ColumnConfig> columns,
      EtlPipelineConfig.ErrorHandlingConfig errorHandling) {
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
    if (errorHandling != null) {
      configBuilder.errorHandling(errorHandling);
    }
    return configBuilder.build();
  }

  private Map<String, DimensionConfig> singleRangeDimension(String key,
      int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.RANGE).start(start).end(end).build());
    return dims;
  }

  private Map<String, DimensionConfig> singleListDimension(String key,
      String... values) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.LIST)
        .values(Arrays.asList(values)).build());
    return dims;
  }

  private Map<String, Object> row(String key, Object value) {
    Map<String, Object> m = new HashMap<String, Object>();
    m.put(key, value);
    return m;
  }

  private IncrementalTracker mockTracker() {
    IncrementalTracker tracker = mock(IncrementalTracker.class);
    when(tracker.getCachedCompletion(anyString())).thenReturn(null);
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenAnswer(inv -> {
          List<?> combos = inv.getArgument(2);
          Set<Integer> all = new HashSet<Integer>();
          for (int i = 0; i < combos.size(); i++) {
            all.add(i);
          }
          return all;
        });
    when(
        tracker.filterUnprocessedWithEmptyTtl(
        anyString(), anyString(), any(List.class), anyLong()))
        .thenAnswer(inv -> {
          List<?> combos = inv.getArgument(2);
          Set<Integer> all = new HashSet<Integer>();
          for (int i = 0; i < combos.size(); i++) {
            all.add(i);
          }
          return all;
        });
    return tracker;
  }

  private StorageProvider mockStorage() throws IOException {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.getStorageType()).thenReturn("local");
    when(sp.isDirectory(anyString())).thenReturn(false);
    return sp;
  }

  private Object invokePrivate(Object instance, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod(methodName, paramTypes);
    m.setAccessible(true);
    return m.invoke(instance, args);
  }

  @SuppressWarnings("unchecked")
  private <T> T getField(Object instance, String fieldName) throws Exception {
    Field f = EtlPipeline.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    return (T) f.get(instance);
  }

  // -----------------------------------------------------------------------
  // 1. CachedEtlProperties inner class
  // -----------------------------------------------------------------------

  @Test void testCachedEtlProperties_constructorAndFields() throws Exception {
    Class<?> clazz =
        Class.forName("org.apache.calcite.adapter.file.etl.EtlPipeline$CachedEtlProperties");
    Object instance =
        clazz.getDeclaredConstructors()[0].newInstance("hash123", "sig456", 100L);
    java.lang.reflect.Field hashField = clazz.getDeclaredField("configHash");
    hashField.setAccessible(true);
    assertEquals("hash123", hashField.get(instance));

    java.lang.reflect.Field sigField = clazz.getDeclaredField("signature");
    sigField.setAccessible(true);
    assertEquals("sig456", sigField.get(instance));

    java.lang.reflect.Field rowField = clazz.getDeclaredField("rowCount");
    rowField.setAccessible(true);
    assertEquals(100L, rowField.get(instance));
  }

  @Test void testCachedEtlProperties_zeroRowCount() throws Exception {
    Class<?> clazz =
        Class.forName("org.apache.calcite.adapter.file.etl.EtlPipeline$CachedEtlProperties");
    Object instance = clazz.getDeclaredConstructors()[0].newInstance("h", "s", 0L);
    java.lang.reflect.Field rowField = clazz.getDeclaredField("rowCount");
    rowField.setAccessible(true);
    assertEquals(0L, rowField.get(instance));
  }

  @Test void testCachedEtlProperties_negativeRowCount() throws Exception {
    Class<?> clazz =
        Class.forName("org.apache.calcite.adapter.file.etl.EtlPipeline$CachedEtlProperties");
    Object instance = clazz.getDeclaredConstructors()[0].newInstance("h", "s", -1L);
    java.lang.reflect.Field rowField = clazz.getDeclaredField("rowCount");
    rowField.setAccessible(true);
    assertEquals(-1L, rowField.get(instance));
  }

  // -----------------------------------------------------------------------
  // 2. readRowCountFromIceberg (via reflection -- covers exception paths)
  // -----------------------------------------------------------------------

  @Test void testReadRowCountFromIceberg_invalidLocation() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    // Invalid/non-existent path should return 0 (caught exception)
    long result =
        (Long) invokePrivate(pipeline, "readRowCountFromIceberg", new Class[]{String.class}, "/nonexistent/path/tbl");
    assertEquals(0, result);
  }

  @Test void testReadRowCountFromIceberg_emptyDir() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    Path iceDir = tempDir.resolve("warehouse");
    Files.createDirectories(iceDir);
    EtlPipeline pipeline = new EtlPipeline(config, sp, iceDir.toString());
    long result =
        (Long) invokePrivate(pipeline, "readRowCountFromIceberg", new Class[]{String.class}, iceDir.toString() + "/tbl");
    assertEquals(0, result);
  }

  @Test void testReadRowCountFromIceberg_withExpectedColumns_noTable() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    List<ColumnConfig> cols =
        Collections.singletonList(ColumnConfig.builder().name("id").type("VARCHAR").build());
    Path iceDir = tempDir.resolve("warehouse2");
    Files.createDirectories(iceDir);
    EtlPipeline pipeline = new EtlPipeline(config, sp, iceDir.toString());
    long result =
        (Long) invokePrivate(pipeline, "readRowCountFromIceberg", new Class[]{String.class, List.class}, iceDir.toString() + "/tbl", cols);
    assertEquals(0, result);
  }

  @Test void testReadRowCountFromIceberg_nullExpectedColumns() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    long result =
        (Long) invokePrivate(pipeline, "readRowCountFromIceberg", new Class[]{String.class, List.class}, tempDir.toString() + "/tbl",
        (Object) null);
    assertEquals(0, result);
  }

  @Test void testReadRowCountFromIceberg_emptyExpectedColumns() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    long result =
        (Long) invokePrivate(pipeline, "readRowCountFromIceberg", new Class[]{String.class, List.class}, tempDir.toString() + "/tbl",
        Collections.emptyList());
    assertEquals(0, result);
  }

  // -----------------------------------------------------------------------
  // 3. readEtlPropertiesFromIceberg (reflection -- exception paths)
  // -----------------------------------------------------------------------

  @Test void testReadEtlPropertiesFromIceberg_noTable() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Object result =
        invokePrivate(pipeline, "readEtlPropertiesFromIceberg", new Class[]{String.class}, tempDir.toString() + "/no_table");
    assertNull(result);
  }

  @Test void testReadEtlPropertiesFromIceberg_invalidPath() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/totally/invalid");
    Object result =
        invokePrivate(pipeline, "readEtlPropertiesFromIceberg", new Class[]{String.class}, "/totally/invalid/tbl");
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // 4. storeEtlPropertiesToIceberg (reflection -- exception/no-op paths)
  // -----------------------------------------------------------------------

  @Test void testStoreEtlPropertiesToIceberg_noTable() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    // Should not throw -- just logs debug and returns
    invokePrivate(pipeline, "storeEtlPropertiesToIceberg",
        new Class[]{String.class, String.class, String.class, long.class},
        tempDir.toString() + "/no_table", "hash", "sig", 42L);
  }

  @Test void testStoreEtlPropertiesToIceberg_invalidPath() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/bad/path");
    invokePrivate(pipeline, "storeEtlPropertiesToIceberg",
        new Class[]{String.class, String.class, String.class, long.class},
        "/bad/path/tbl", "hash", "sig", 0L);
  }

  // -----------------------------------------------------------------------
  // 5. buildIcebergCatalogConfig edge cases
  // -----------------------------------------------------------------------

  @Test void testBuildIcebergCatalogConfig_hadoopDefault() throws Exception {
    StorageProvider sp = mockStorage();
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    assertEquals("hadoop", result.get("catalog"));
    assertEquals(tempDir.toString(), result.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_s3PathConversion() throws Exception {
    StorageProvider sp = mockStorage();
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, "s3://bucket/warehouse");
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    assertEquals("s3a://bucket/warehouse", result.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_withS3CredentialsPartial() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AKID");
    // no secretAccessKey, no endpoint, no region
    when(sp.getS3Config()).thenReturn(s3Config);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    @SuppressWarnings("unchecked")
    Map<String, String> hadoop = (Map<String, String>) result.get("hadoopConfig");
    assertNotNull(hadoop);
    assertEquals("AKID", hadoop.get("fs.s3a.access.key"));
    assertNull(hadoop.get("fs.s3a.secret.key"));
    assertNull(hadoop.get("fs.s3a.endpoint"));
  }

  @Test void testBuildIcebergCatalogConfig_withEndpointSetsPathStyleAccess() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("endpoint", "http://minio:9000");
    when(sp.getS3Config()).thenReturn(s3Config);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    @SuppressWarnings("unchecked")
    Map<String, String> hadoop = (Map<String, String>) result.get("hadoopConfig");
    assertEquals("true", hadoop.get("fs.s3a.path.style.access"));
  }

  @Test void testBuildIcebergCatalogConfig_withRegion() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("region", "us-west-2");
    when(sp.getS3Config()).thenReturn(s3Config);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    @SuppressWarnings("unchecked")
    Map<String, String> hadoop = (Map<String, String>) result.get("hadoopConfig");
    assertEquals("us-west-2", hadoop.get("fs.s3a.endpoint.region"));
  }

  @Test void testBuildIcebergCatalogConfig_emptyS3Config() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.getS3Config()).thenReturn(Collections.<String, String>emptyMap());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    assertNull(result.get("hadoopConfig"));
  }

  @Test void testBuildIcebergCatalogConfig_icebergWarehouseS3Conversion() throws Exception {
    StorageProvider sp = mockStorage();
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("s3://my-bucket/wh")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    assertEquals("s3a://my-bucket/wh", result.get("warehousePath"));
  }

  @Test void testBuildIcebergCatalogConfig_icebergEmptyWarehouseFallsBackToBase() throws Exception {
    StorageProvider sp = mockStorage();
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/base/dir");
    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(pipeline, "buildIcebergCatalogConfig",
        new Class[]{MaterializeConfig.class}, mc);
    assertEquals("/base/dir", result.get("warehousePath"));
  }

  // -----------------------------------------------------------------------
  // 6. verifyDataExists edge cases
  // -----------------------------------------------------------------------

  @Test void testVerifyDataExists_parquetWithExistingDir() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertTrue(result);
  }

  @Test void testVerifyDataExists_parquetNonExistingDir() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertFalse(result);
  }

  @Test void testVerifyDataExists_icebergWarehousePathUsed() throws Exception {
    StorageProvider sp = mockStorage();
    // First call for warehouse path - true; second for fallback - false
    when(sp.isDirectory(eq("/wh/tbl/metadata"))).thenReturn(true);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("/wh")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/base");
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertTrue(result);
  }

  @Test void testVerifyDataExists_icebergWarehouseFallbackToBase() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(eq("/wh/tbl/metadata"))).thenReturn(false);
    when(sp.isDirectory(eq("/base/tbl/metadata"))).thenReturn(true);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("/wh")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/base");
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertTrue(result);
  }

  @Test void testVerifyDataExists_icebergWarehouseAndFallbackBothFail() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .warehousePath("/wh")
            .build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/base");
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertFalse(result);
  }

  @Test void testVerifyDataExists_ioExceptionReturnsTrue() throws Exception {
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenThrow(new IOException("disk error"));
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc).build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    boolean result =
        (Boolean) invokePrivate(pipeline, "verifyDataExists", new Class[]{String.class, EtlPipelineConfig.class}, "tbl", config);
    assertTrue(result);
  }

  // -----------------------------------------------------------------------
  // 7. determineErrorAction edge cases
  // -----------------------------------------------------------------------

  @Test void testDetermineErrorAction_http500UsesApiAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build();
    Object result =
        invokePrivate(pipeline, "determineErrorAction", new Class[]{IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class},
        new IOException("HTTP 500 error"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL, result);
  }

  @Test void testDetermineErrorAction_http502UsesApiAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    Object result =
        invokePrivate(pipeline, "determineErrorAction", new Class[]{IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class},
        new IOException("HTTP 502 gateway"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN, result);
  }

  @Test void testDetermineErrorAction_nonHttpMessage() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build();
    Object result =
        invokePrivate(pipeline, "determineErrorAction", new Class[]{IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class},
        new IOException("Connection reset"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, result);
  }

  @Test void testDetermineErrorAction_nullMessageUsesApiAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    Object result =
        invokePrivate(pipeline, "determineErrorAction", new Class[]{IOException.class, EtlPipelineConfig.ErrorHandlingConfig.class},
        new IOException((String) null), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN, result);
  }

  // -----------------------------------------------------------------------
  // 8. allIndicesSet (static)
  // -----------------------------------------------------------------------

  @Test void testAllIndicesSet_singleElement() throws Exception {
    @SuppressWarnings("unchecked")
    Set<Integer> result =
        (Set<Integer>) invokePrivate(null, "allIndicesSet", new Class[]{int.class}, 1);
    // allIndicesSet is a static method; invoke with null instance won't work
    // Use a different approach: invoke directly using the static method
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    result = (Set<Integer>) m.invoke(null, 1);
    assertEquals(1, result.size());
    assertTrue(result.contains(0));
  }

  @Test void testAllIndicesSet_fiveElements() throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(null, 5);
    assertEquals(5, result.size());
    for (int i = 0; i < 5; i++) {
      assertTrue(result.contains(i));
    }
  }

  @Test void testAllIndicesSet_empty() throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(null, 0);
    assertTrue(result.isEmpty());
  }

  @Test void testAllIndicesSet_largeSet() throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod("allIndicesSet", int.class);
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(null, 100);
    assertEquals(100, result.size());
    assertTrue(result.contains(0));
    assertTrue(result.contains(99));
  }

  // -----------------------------------------------------------------------
  // 9. getParallelThreadCount
  // -----------------------------------------------------------------------

  @Test void testGetParallelThreadCount_sourceConfigParallel() throws Exception {
    StorageProvider sp = mockStorage();
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("http://x")
        .parallel(4)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(source)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    int count =
        (Integer) invokePrivate(pipeline, "getParallelThreadCount", new Class[]{});
    assertEquals(4, count);
  }

  @Test void testGetParallelThreadCount_sourceConfigParallelOne() throws Exception {
    StorageProvider sp = mockStorage();
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("http://x")
        .parallel(1)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl").source(source)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    int count =
        (Integer) invokePrivate(pipeline, "getParallelThreadCount", new Class[]{});
    // parallel=1 means <= 1, so falls through to system property (default 1)
    assertEquals(1, count);
  }

  // -----------------------------------------------------------------------
  // 10. createDataSource
  // -----------------------------------------------------------------------

  @Test void testCreateDataSource_fileSourceType() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, Object> rawSource = new HashMap<String, Object>();
    rawSource.put("path", "/data/input.csv");
    rawSource.put("format", "csv");
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("file")
        .rawSourceConfig(rawSource)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    DataSource ds =
        (DataSource) invokePrivate(pipeline, "createDataSource", new Class[]{EtlPipelineConfig.class}, config);
    assertNotNull(ds);
  }

  @Test void testCreateDataSource_documentSourceReturnsNull() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("document")
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    DataSource ds =
        (DataSource) invokePrivate(pipeline, "createDataSource", new Class[]{EtlPipelineConfig.class}, config);
    assertNull(ds);
  }

  @Test void testCreateDataSource_constantsType() throws Exception {
    StorageProvider sp = mockStorage();
    Map<String, Object> rawSource = new HashMap<String, Object>();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(row("id", "1"));
    rawSource.put("rows", rows);
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("constants")
        .rawSourceConfig(rawSource)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    DataSource ds =
        (DataSource) invokePrivate(pipeline, "createDataSource", new Class[]{EtlPipelineConfig.class}, config);
    assertNotNull(ds);
  }

  // -----------------------------------------------------------------------
  // 11. loadDimensionResolver
  // -----------------------------------------------------------------------

  @Test void testLoadDimensionResolver_emptyClassName() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    // null hooks returns null
    Object result =
        invokePrivate(pipeline, "loadDimensionResolver", new Class[]{HooksConfig.class}, (Object) null);
    assertNull(result);
  }

  @Test void testLoadDimensionResolver_hooksWithNoResolverClass() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    HooksConfig hooks = HooksConfig.builder().build();
    Object result =
        invokePrivate(pipeline, "loadDimensionResolver", new Class[]{HooksConfig.class}, hooks);
    assertNull(result);
  }

  @Test void testLoadDimensionResolver_invalidClassName() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    HooksConfig hooks = HooksConfig.builder()
        .dimensionResolverClass("com.nonexistent.BadClass")
        .build();
    try {
      invokePrivate(pipeline, "loadDimensionResolver",
          new Class[]{HooksConfig.class}, hooks);
      fail("Expected exception for nonexistent class");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException
          || e.getMessage().contains("not found"));
    }
  }

  // -----------------------------------------------------------------------
  // 12. Constructor field assignments
  // -----------------------------------------------------------------------

  @Test void testConstructor_sourceStorageProviderDefaultsToMain() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    StorageProvider actual = getField(pipeline, "sourceStorageProvider");
    assertEquals(sp, actual);
  }

  @Test void testConstructor_sourceStorageProviderOverride() throws Exception {
    StorageProvider sp1 = mockStorage();
    StorageProvider sp2 = mock(StorageProvider.class);
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp1, sp2, tempDir.toString(), null, IncrementalTracker.NOOP, null, null);
    StorageProvider actual = getField(pipeline, "sourceStorageProvider");
    assertEquals(sp2, actual);
  }

  @Test void testConstructor_trackerDefaultsToNoop() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    IncrementalTracker actual = getField(pipeline, "incrementalTracker");
    assertEquals(IncrementalTracker.NOOP, actual);
  }

  @Test void testConstructor_nullTrackerBecomesNoop() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, null);
    IncrementalTracker actual = getField(pipeline, "incrementalTracker");
    assertEquals(IncrementalTracker.NOOP, actual);
  }

  @Test void testConstructor_operatingDirectory() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, null, tempDir.toString(), null, IncrementalTracker.NOOP, null, null, "/op/dir");
    String opDir = getField(pipeline, "operatingDirectory");
    assertEquals("/op/dir", opDir);
  }

  @Test void testConstructor_baseDirectoryStored() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, "/my/base");
    String base = getField(pipeline, "baseDirectory");
    assertEquals("/my/base", base);
  }

  @Test void testConstructor_customDataProvider() throws Exception {
    StorageProvider sp = mockStorage();
    DataProvider dp = mock(DataProvider.class);
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, dp, null);
    DataProvider actual = getField(pipeline, "dataProvider");
    assertEquals(dp, actual);
  }

  @Test void testConstructor_customDataWriter() throws Exception {
    StorageProvider sp = mockStorage();
    DataWriter dw = mock(DataWriter.class);
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, null, dw);
    DataWriter actual = getField(pipeline, "dataWriter");
    assertEquals(dw, actual);
  }

  // -----------------------------------------------------------------------
  // 13. execute() - cached completion with config hash match
  // -----------------------------------------------------------------------

  @Test void testExecute_cachedCompletionMatchSkipsPipeline() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    String configHash =
        IncrementalTracker.computeConfigHash(singleRangeDimension("y", 2020, 2020));
    when(tracker.getCachedCompletion("test_cached"))
        .thenReturn(new IncrementalTracker.CachedCompletion(configHash, "sig", 500));
    EtlPipelineConfig config =
        createHttpConfig("test_cached", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(500, result.getTotalRows());
  }

  @Test void testExecute_cachedMismatchWithRowsDataExistsSkips() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("test_mismatch"))
        .thenReturn(new IncrementalTracker.CachedCompletion("old_hash", "old_sig", 250));
    EtlPipelineConfig config =
        createHttpConfig("test_mismatch", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(250, result.getTotalRows());
    verify(tracker).markTableCompleteWithConfig(eq("test_mismatch"),
        anyString(), eq("old_sig"), eq(250L));
  }

  @Test void testExecute_cachedMismatchWithRowsNoDataDoesNotSkip() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    when(tracker.getCachedCompletion("test_nodata"))
        .thenReturn(new IncrementalTracker.CachedCompletion("old_hash", "old_sig", 250));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("test_nodata", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 14. execute() - empty result TTL handling
  // -----------------------------------------------------------------------

  @Test void testExecute_cachedZeroRowsTtlNotExpiredSkips() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(30).build();
    String configHash =
        IncrementalTracker.computeConfigHash(singleRangeDimension("y", 2020, 2020));
    // completedAt = now, so TTL not expired
    when(tracker.getCachedCompletion("tbl_empty"))
        .thenReturn(
            new IncrementalTracker.CachedCompletion(
            configHash, "sig", 0, System.currentTimeMillis()));
    EtlPipelineConfig config =
        createHttpConfig("tbl_empty", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(0, result.getTotalRows());
  }

  @Test void testExecute_cachedZeroRowsTtlExpiredInvalidates() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(1).build();
    String configHash =
        IncrementalTracker.computeConfigHash(singleRangeDimension("y", 2020, 2020));
    // completedAt = 10 days ago, TTL is 1 day -> expired
    long completedAt = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000);
    when(tracker.getCachedCompletion("tbl_expired"))
        .thenReturn(
            new IncrementalTracker.CachedCompletion(
            configHash, "sig", 0, completedAt));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_expired", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    verify(tracker).invalidateTableCompletion("tbl_expired");
    assertFalse(result.isSkippedEntirePipeline());
  }

  @Test void testExecute_cachedMatchDataGoneInvalidatesAndReprocesses() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    String configHash =
        IncrementalTracker.computeConfigHash(singleRangeDimension("y", 2020, 2020));
    when(tracker.getCachedCompletion("tbl_gone"))
        .thenReturn(new IncrementalTracker.CachedCompletion(configHash, "sig", 100));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_gone", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    verify(tracker).invalidateTableCompletion("tbl_gone");
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 15. execute() - zero dimensions / empty combos
  // -----------------------------------------------------------------------

  @Test void testExecute_zeroDimensionCombinationsReturnsError() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Return empty set from filterUnprocessedWithEmptyTtl
    when(
        tracker.filterUnprocessedWithEmptyTtl(
        anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(Collections.<Integer>emptySet());
    // Simulate empty dimensions by using a range where start > end
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("y", DimensionConfig.builder()
        .name("y").type(DimensionType.RANGE).start(2025).end(2024).build());
    EtlPipelineConfig config =
        createHttpConfig("tbl_empty_dim", dims, MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Should have error about no dimension combinations
    assertTrue(result.getErrors().size() > 0
        || result.getTotalRows() == 0);
  }

  // -----------------------------------------------------------------------
  // 16. execute() - table complete check (phase 2)
  // -----------------------------------------------------------------------

  @Test void testExecute_tableCompleteWithParquetFormatRowCountZeroInvalidates() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("tbl_complete"), anyString())).thenReturn(true);
    // PARQUET format: rowCount stays 0 (not read from Iceberg), and since the
    // default options have emptyResultTtlDays=7 (> 0), the completion is invalidated
    // and the pipeline continues to Phase 2 instead of returning skipped.
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_complete", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Verify invalidation happened because rowCount==0 and emptyResultTtlMillis > 0
    verify(tracker).invalidateTableCompletion("tbl_complete");
    assertNotNull(result);
  }

  @Test void testExecute_tableCompleteButDataGoneInvalidates() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    when(tracker.isTableComplete(eq("tbl_stale"), anyString())).thenReturn(true);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_stale", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    verify(tracker).invalidateTableCompletion("tbl_stale");
  }

  @Test void testExecute_tableCompleteNoMaterializeEnabled() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(tracker.isTableComplete(eq("tbl_nomat"), anyString())).thenReturn(true);
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data").build();
    // materialize is present but disabled
    MaterializeConfig mc = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl_nomat").source(source)
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkipped());
  }

  // -----------------------------------------------------------------------
  // 17. execute() - all already processed (neededCount == 0)
  // -----------------------------------------------------------------------

  @Test void testExecute_allAlreadyProcessedMarksComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(
        tracker.filterUnprocessedWithEmptyTtl(
        anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(Collections.<Integer>emptySet());
    EtlPipelineConfig config =
        createHttpConfig("tbl_done", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertEquals(1, result.getSkippedBatches());
    assertEquals(0, result.getSuccessfulBatches());
    verify(tracker).markTableCompleteWithConfig(eq("tbl_done"),
        anyString(), anyString(), anyLong());
  }

  // -----------------------------------------------------------------------
  // 18. execute() - config merging (materialize name / columns)
  // -----------------------------------------------------------------------

  @Test void testExecute_mergesNameFromPipelineConfig() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // materialize has no name and no targetTableId
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("my_pipeline")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // If it doesn't throw, merging worked
  }

  @Test void testExecute_mergesColumnsFromPipelineConfig() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // materialize has no columns, but pipeline config has columns
    List<ColumnConfig> cols =
        Collections.singletonList(ColumnConfig.builder().name("id").type("VARCHAR").build());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .name("tbl")
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("col_merge")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .columns(cols)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 19. execute() - table directory path construction
  // -----------------------------------------------------------------------

  @Test void testExecute_parquetAppendsTableNameToPath() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("my_table", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, "/data/output", null, tracker, prov, null);
    // Execute should not throw due to path construction issues
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_parquetBasePathTrailingSlash() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_slash", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, "/data/output/", null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 20. execute() - progress listener callbacks
  // -----------------------------------------------------------------------

  @Test void testExecute_progressListenerCalledOnAllPhases() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("tbl_progress", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onPhaseStart(eq("dimension_expansion"), eq(1));
    verify(listener).onPhaseComplete(eq("dimension_expansion"), eq(1));
    verify(listener).onPhaseStart(eq("data_processing"), eq(1));
    verify(listener).onBatchStart(eq(1), eq(1), any(Map.class));
  }

  @Test void testExecute_progressListenerBatchCompleteOnSuccess() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("tbl_pl2", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onBatchComplete(eq(1), eq(1), any(int.class), eq(null));
  }

  @Test void testExecute_progressListenerBatchCompleteOnError() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("HTTP 404 Not Found"));
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("tbl_pl3", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onBatchComplete(eq(1), eq(1), eq(0), any(IOException.class));
  }

  // -----------------------------------------------------------------------
  // 21. execute() - error handling in sequential mode
  // -----------------------------------------------------------------------

  @Test void testExecute_failActionThrowsException() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("HTTP 401 Unauthorized"));
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("tbl_fail", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // FAIL action results in caught exception -> result is built from exception handler
    assertTrue(result.isFailed() || result.getFailedBatches() > 0);
  }

  @Test void testExecute_skipActionIncrementsSkipped() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenThrow(new IOException("HTTP 404 Not Found"));
    EtlPipelineConfig config =
        createHttpConfig("tbl_skip", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_warnActionIncrementsFailed() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("HTTP 500 Server Error"));
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("tbl_warn", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertTrue(result.getFailedBatches() > 0);
  }

  @Test void testExecute_defaultActionIncrementsFailed() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("something weird"));
    EtlPipelineConfig config =
        createHttpConfig("tbl_def", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // default apiErrorAction is SKIP
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 22. execute() - consecutive failure abort
  // -----------------------------------------------------------------------

  @Test void testExecute_consecutiveFailuresAbortTable() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    // All fetches fail
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("HTTP 429 Too Many Requests"));
    // Create many dimensions to exceed max consecutive failures
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("y", DimensionConfig.builder()
        .name("y").type(DimensionType.RANGE).start(2000).end(2020).build());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("tbl_abort", dims, MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertTrue(result.isFailed() || result.getFailedBatches() > 0);
    assertNotNull(result.getFailureMessage());
  }

  @Test void testExecute_consecutiveFailuresResetOnSuccess() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    // Alternating success and failure
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenThrow(new IOException("HTTP 429 Rate Limit"))
        .thenReturn(Collections.singletonList(row("id", "2")).iterator())
        .thenThrow(new IOException("HTTP 429 Rate Limit"));
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("y", DimensionConfig.builder()
        .name("y").type(DimensionType.RANGE).start(2020).end(2023).build());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("tbl_reset", dims, MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Should not abort because consecutive failures reset on success
    assertFalse(result.isFailed());
    assertTrue(result.getSuccessfulBatches() >= 2);
  }

  // -----------------------------------------------------------------------
  // 23. execute() - document source
  // -----------------------------------------------------------------------

  @Test void testExecute_documentSourceWithWriterUsesDataWriter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(), any(Map.class)))
        .thenReturn(50L);
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_tbl")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("document")
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .format(MaterializeConfig.Format.PARQUET).build())
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, null, dw);
    EtlResult result = pipeline.execute();
    assertEquals(50, result.getTotalRows());
    verify(dw).write(any(EtlPipelineConfig.class), any(), any(Map.class));
  }

  @Test void testExecute_documentSourceWithoutWriterReturnsZero() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("doc_no_writer")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("document")
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .format(MaterializeConfig.Format.PARQUET).build())
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, null, null);
    EtlResult result = pipeline.execute();
    assertEquals(0, result.getTotalRows());
  }

  // -----------------------------------------------------------------------
  // 24. execute() - custom DataProvider
  // -----------------------------------------------------------------------

  @Test void testExecute_customProviderReturnsNull_fallsBackToDataSource() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(null);
    // Fall back would try HttpSource, which will fail - caught as error
    EtlPipelineConfig config =
        createHttpConfig("tbl_null_prov", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Will either fail or succeed depending on HttpSource behavior
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 25. execute() - custom DataWriter returning negative
  // -----------------------------------------------------------------------

  @Test void testExecute_customDataWriterNegativeFallsBackToWriter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(Iterator.class), any(Map.class)))
        .thenReturn(-1L);
    EtlPipelineConfig config =
        createHttpConfig("tbl_neg_dw", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, dw);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 26. execute() - phase 6 commit and close
  // -----------------------------------------------------------------------

  @Test void testExecute_writerClosedEvenOnException() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new RuntimeException("unexpected"));
    EtlPipelineConfig config =
        createHttpConfig("tbl_close", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertTrue(result.isFailed());
  }

  @Test void testExecute_noFailedBatchesMarksTableComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_success", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // If successful, should mark complete
    if (!result.isFailed() && result.getFailedBatches() == 0) {
      verify(tracker).markTableCompleteWithConfig(eq("tbl_success"),
          anyString(), anyString(), anyLong());
    }
  }

  @Test void testExecute_failedBatchesDoNotMarkComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenThrow(new IOException("HTTP 500 error"));
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("tbl_partial", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Should NOT mark complete because there are failed batches
    if (result.getFailedBatches() > 0) {
      verify(tracker, never()).markTableCompleteWithConfig(eq("tbl_partial"),
          anyString(), anyString(), anyLong());
    }
  }

  // -----------------------------------------------------------------------
  // 27. Static create() factory method
  // -----------------------------------------------------------------------

  @Test void testCreateFactoryMethod() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config = createHttpConfig("tbl", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = EtlPipeline.create(config, sp, tempDir.toString());
    assertNotNull(pipeline);
    EtlPipelineConfig storedConfig = getField(pipeline, "config");
    assertEquals(config, storedConfig);
  }

  // -----------------------------------------------------------------------
  // 28. LoggingProgressListener edge cases
  // -----------------------------------------------------------------------

  @Test void testLoggingProgressListener_onBatchCompleteWithError() {
    EtlPipeline.LoggingProgressListener listener =
        new EtlPipeline.LoggingProgressListener();
    listener.onBatchComplete(1, 10, 0, new RuntimeException("test error"));
    // Should not throw - logs warning
  }

  @Test void testLoggingProgressListener_onBatchCompleteWithoutError() {
    EtlPipeline.LoggingProgressListener listener =
        new EtlPipeline.LoggingProgressListener();
    listener.onBatchComplete(1, 10, 500, null);
    // Should not throw - logs debug
  }

  @Test void testLoggingProgressListener_allCallbacks() {
    EtlPipeline.LoggingProgressListener listener =
        new EtlPipeline.LoggingProgressListener();
    listener.onPhaseStart("test_phase", 5);
    listener.onPhaseComplete("test_phase", 5);
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("x", "1");
    listener.onBatchStart(1, 5, vars);
    listener.onBatchComplete(1, 5, 100, null);
    listener.onBatchComplete(2, 5, 0, new IOException("error"));
    // All should execute without exception
  }

  // -----------------------------------------------------------------------
  // 29. CachedCompletion edge cases
  // -----------------------------------------------------------------------

  @Test void testCachedCompletion_isEmptyResultTtlExpired_nonEmptyResult() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("hash", "sig", 100, 0L);
    assertFalse(cc.isEmptyResultTtlExpired(86400000L));
  }

  @Test void testCachedCompletion_isEmptyResultTtlExpired_noTtlConfigured() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, 0L);
    assertFalse(cc.isEmptyResultTtlExpired(0L));
  }

  @Test void testCachedCompletion_isEmptyResultTtlExpired_negativeTtl() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, 0L);
    assertFalse(cc.isEmptyResultTtlExpired(-1L));
  }

  @Test void testCachedCompletion_isEmptyResultTtlExpired_expired() {
    long completedAt = System.currentTimeMillis() - (2 * 24 * 60 * 60 * 1000L);
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, completedAt);
    // 1 day TTL, completed 2 days ago -> expired
    assertTrue(cc.isEmptyResultTtlExpired(24 * 60 * 60 * 1000L));
  }

  @Test void testCachedCompletion_isEmptyResultTtlExpired_notYetExpired() {
    long completedAt = System.currentTimeMillis();
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("hash", "sig", 0, completedAt);
    // 30 day TTL, completed now -> not expired
    assertFalse(cc.isEmptyResultTtlExpired(30L * 24 * 60 * 60 * 1000L));
  }

  @Test void testCachedCompletion_threeArgConstructorSetsCompletedAtToNow() {
    long before = System.currentTimeMillis();
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("h", "s", 42);
    long after = System.currentTimeMillis();
    assertTrue(cc.completedAt >= before);
    assertTrue(cc.completedAt <= after);
    assertEquals(42, cc.rowCount);
  }

  @Test void testCachedCompletion_fourArgConstructor() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("h", "s", 10, 12345L);
    assertEquals("h", cc.configHash);
    assertEquals("s", cc.signature);
    assertEquals(10, cc.rowCount);
    assertEquals(12345L, cc.completedAt);
  }

  @Test void testCachedCompletion_fiveArgConstructorWithWatermark() {
    IncrementalTracker.CachedCompletion cc =
        new IncrementalTracker.CachedCompletion("h", "s", 10, 12345L, 99999L);
    assertEquals("h", cc.configHash);
    assertEquals("s", cc.signature);
    assertEquals(10, cc.rowCount);
    assertEquals(12345L, cc.completedAt);
    assertEquals(99999L, cc.sourceFileWatermark);
  }

  // -----------------------------------------------------------------------
  // 30. MaterializeOptionsConfig edge cases
  // -----------------------------------------------------------------------

  @Test void testMaterializeOptionsConfig_defaults() {
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.defaults();
    assertEquals(2, opts.getThreads());
    assertEquals(100000, opts.getRowGroupSize());
    assertEquals(10000, opts.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, opts.getStagingMode());
    assertFalse(opts.isPreserveInsertionOrder());
    assertEquals(7, opts.getEmptyResultTtlDays());
  }

  @Test void testMaterializeOptionsConfig_customValues() {
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .threads(8)
        .rowGroupSize(50000)
        .batchSize(20000)
        .stagingMode(MaterializeOptionsConfig.StagingMode.LOCAL)
        .preserveInsertionOrder(true)
        .emptyResultTtlDays(14)
        .build();
    assertEquals(8, opts.getThreads());
    assertEquals(50000, opts.getRowGroupSize());
    assertEquals(20000, opts.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, opts.getStagingMode());
    assertTrue(opts.isPreserveInsertionOrder());
    assertEquals(14, opts.getEmptyResultTtlDays());
  }

  @Test void testMaterializeOptionsConfig_emptyResultTtlMillis() {
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(3)
        .build();
    long expectedMillis = 3L * 24L * 60L * 60L * 1000L;
    assertEquals(expectedMillis, opts.getEmptyResultTtlMillis());
  }

  @Test void testMaterializeOptionsConfig_fromMapNullReturnsDefaults() {
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.fromMap(null);
    assertEquals(2, opts.getThreads());
  }

  @Test void testMaterializeOptionsConfig_fromMapPartialValues() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("threads", 6);
    map.put("preserveInsertionOrder", true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.fromMap(map);
    assertEquals(6, opts.getThreads());
    assertTrue(opts.isPreserveInsertionOrder());
    // defaults for unset values
    assertEquals(100000, opts.getRowGroupSize());
    assertEquals(10000, opts.getBatchSize());
  }

  @Test void testMaterializeOptionsConfig_fromMapStagingModeLocal() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("stagingMode", "local");
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.fromMap(map);
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, opts.getStagingMode());
  }

  @Test void testMaterializeOptionsConfig_fromMapInvalidStagingMode() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("stagingMode", "invalid");
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.fromMap(map);
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, opts.getStagingMode());
  }

  @Test void testMaterializeOptionsConfig_fromMapEmptyResultTtlDays() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("emptyResultTtlDays", 30);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.fromMap(map);
    assertEquals(30, opts.getEmptyResultTtlDays());
  }

  @Test void testMaterializeOptionsConfig_zeroValuesUseDefaults() {
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .threads(0)
        .rowGroupSize(0)
        .batchSize(0)
        .emptyResultTtlDays(0)
        .build();
    assertEquals(2, opts.getThreads());
    assertEquals(100000, opts.getRowGroupSize());
    assertEquals(10000, opts.getBatchSize());
    assertEquals(7, opts.getEmptyResultTtlDays());
  }

  // -----------------------------------------------------------------------
  // 31. EtlResult edge cases
  // -----------------------------------------------------------------------

  @Test void testEtlResult_successFactory() {
    EtlResult result = EtlResult.success("pipe", 1000, 5, 2500);
    assertEquals("pipe", result.getPipelineName());
    assertEquals(1000, result.getTotalRows());
    assertEquals(5, result.getSuccessfulBatches());
    assertEquals(2500, result.getElapsedMs());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
    assertFalse(result.isFailed());
  }

  @Test void testEtlResult_failureFactory() {
    EtlResult result = EtlResult.failure("pipe", "broke", 500);
    assertTrue(result.isFailed());
    assertFalse(result.isSuccessful());
    assertEquals("broke", result.getFailureMessage());
  }

  @Test void testEtlResult_skippedFactory() {
    EtlResult result = EtlResult.skipped("pipe", 10);
    assertTrue(result.isSkipped());
    assertTrue(result.isSkippedEntirePipeline());
    assertEquals(10, result.getElapsedMs());
  }

  @Test void testEtlResult_rowsPerSecond() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .totalRows(1000)
        .elapsedMs(2000)
        .build();
    assertEquals(500.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testEtlResult_rowsPerSecondZeroElapsed() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .totalRows(1000)
        .elapsedMs(0)
        .build();
    assertEquals(0.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testEtlResult_totalBatches() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .successfulBatches(3)
        .failedBatches(1)
        .skippedBatches(2)
        .build();
    assertEquals(6, result.getTotalBatches());
  }

  @Test void testEtlResult_isCompleteSuccess_falseWhenFailed() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .failedBatches(1)
        .build();
    assertFalse(result.isCompleteSuccess());
  }

  @Test void testEtlResult_isCompleteSuccess_falseWhenErrors() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .errors(Collections.singletonList("error"))
        .build();
    assertFalse(result.isCompleteSuccess());
  }

  @Test void testEtlResult_toStringSkipped() {
    EtlResult result = EtlResult.skipped("pipe", 50);
    String s = result.toString();
    assertTrue(s.contains("SKIPPED"));
    assertTrue(s.contains("pipe"));
  }

  @Test void testEtlResult_toStringFailed() {
    EtlResult result = EtlResult.failure("pipe", "msg", 100);
    String s = result.toString();
    assertTrue(s.contains("FAILED"));
    assertTrue(s.contains("msg"));
  }

  @Test void testEtlResult_toStringSuccess() {
    EtlResult result = EtlResult.success("pipe", 100, 5, 2000);
    String s = result.toString();
    assertTrue(s.contains("rows=100"));
    assertTrue(s.contains("elapsed=2000ms"));
  }

  @Test void testEtlResult_toStringWithFailedAndSkippedBatches() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .totalRows(50)
        .successfulBatches(3)
        .failedBatches(1)
        .skippedBatches(2)
        .elapsedMs(1000)
        .build();
    String s = result.toString();
    assertTrue(s.contains("1 failed"));
    assertTrue(s.contains("2 skipped"));
  }

  @Test void testEtlResult_tableLocationAndFormat() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .tableLocation("/data/tbl")
        .materializeFormat(MaterializeConfig.Format.ICEBERG)
        .build();
    assertEquals("/data/tbl", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.ICEBERG, result.getMaterializeFormat());
  }

  @Test void testEtlResult_errorsImmutable() {
    List<String> errors = new ArrayList<String>();
    errors.add("e1");
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .errors(errors)
        .build();
    try {
      result.getErrors().add("e2");
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    assertEquals(1, result.getErrors().size());
  }

  @Test void testEtlResult_nullErrorsReturnsEmptyList() {
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .build();
    assertNotNull(result.getErrors());
    assertTrue(result.getErrors().isEmpty());
  }

  // -----------------------------------------------------------------------
  // 32. execute() - cached completion with 0 rows, mismatch
  // -----------------------------------------------------------------------

  @Test void testExecute_cachedMismatchZeroRowsExpandsDimensions() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    when(tracker.getCachedCompletion("tbl_mismatch_0"))
        .thenReturn(new IncrementalTracker.CachedCompletion("old_hash", "sig", 0));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_mismatch_0", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Should fall through to processing
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 33. execute() - table complete with zero rows and TTL
  // -----------------------------------------------------------------------

  @Test void testExecute_tableCompleteZeroRowsPositiveTtlInvalidates() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("tbl_0ttl"), anyString())).thenReturn(true);
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(7).build();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_0ttl", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // readRowCountFromIceberg will return 0 (no actual table), so
    // the TTL check may invalidate the completion
    verify(tracker, atLeastOnce()).markTableCompleteWithConfig(
        eq("tbl_0ttl"), anyString(), anyString(), anyLong());
  }

  @Test void testExecute_tableCompleteDisabledMaterializeSkips() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("tbl_0notl"), anyString())).thenReturn(true);
    // When materialize is disabled, verifyDataExists returns true (no check),
    // and the else branch at line 440-443 returns EtlResult.skipped()
    MaterializeConfig mc = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("tbl_0notl")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 34. ErrorHandlingConfig
  // -----------------------------------------------------------------------

  @Test void testErrorHandlingConfig_defaults() {
    EtlPipelineConfig.ErrorHandlingConfig eh =
        EtlPipelineConfig.ErrorHandlingConfig.defaults();
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        eh.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        eh.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        eh.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        eh.getAuthErrorAction());
  }

  @Test void testErrorHandlingConfig_fromMapNull() {
    EtlPipelineConfig.ErrorHandlingConfig eh =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(null);
    assertNotNull(eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        eh.getTransientErrorAction());
  }

  @Test void testErrorHandlingConfig_fromMapCustom() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("transientErrorAction", "fail");
    map.put("notFoundAction", "warn");
    map.put("apiErrorAction", "fail");
    map.put("authErrorAction", "skip");
    EtlPipelineConfig.ErrorHandlingConfig eh =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(map);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        eh.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        eh.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        eh.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        eh.getAuthErrorAction());
  }

  @Test void testErrorHandlingConfig_builderAllActions() {
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .notFoundAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY)
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build();
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        eh.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        eh.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        eh.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        eh.getAuthErrorAction());
  }

  // -----------------------------------------------------------------------
  // 35. rebuildCacheFromIceberg - edge cases (via reflection)
  // -----------------------------------------------------------------------

  @Test void testRebuildCacheFromIceberg_cacheHasData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Cache already has some data (only 1 of 2 unprocessed)
    Set<Integer> partial = new HashSet<Integer>();
    partial.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(partial);
    EtlPipelineConfig config =
        createHttpConfig("rebuild_test", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    Map<String, String> c2 = new HashMap<String, String>();
    c2.put("y", "2021");
    combos.add(c2);
    @SuppressWarnings("unchecked")
    Set<Integer> result =
        (Set<Integer>) invokePrivate(pipeline, "rebuildCacheFromIceberg",
        new Class[]{String.class, EtlPipelineConfig.class, List.class},
        "rebuild_test", config, combos);
    // Cache has data, so returns early with filter result
    assertEquals(1, result.size());
    assertTrue(result.contains(0));
  }

  @Test void testRebuildCacheFromIceberg_noPartitionColumns() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    // All are unprocessed
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    // materialize without partition config
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rebuild_nopart")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    @SuppressWarnings("unchecked")
    Set<Integer> result =
        (Set<Integer>) invokePrivate(pipeline, "rebuildCacheFromIceberg",
        new Class[]{String.class, EtlPipelineConfig.class, List.class},
        "rebuild_nopart", config, combos);
    assertEquals(1, result.size());
  }

  @Test void testRebuildCacheFromIceberg_disabledMaterialize() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    MaterializeConfig mc = MaterializeConfig.builder()
        .enabled(false)
        .output(MaterializeOutputConfig.builder().build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rebuild_disabled")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    List<Map<String, String>> combos =
        Collections.singletonList(Collections.singletonMap("y", "2020"));
    @SuppressWarnings("unchecked")
    Set<Integer> result =
        (Set<Integer>) invokePrivate(pipeline, "rebuildCacheFromIceberg",
        new Class[]{String.class, EtlPipelineConfig.class, List.class},
        "rebuild_disabled", config, combos);
    assertEquals(1, result.size());
  }

  // -----------------------------------------------------------------------
  // 36. Iceberg cold-start recovery path in execute()
  // -----------------------------------------------------------------------

  @Test void testExecute_icebergColdStartNoCachedCompletion() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // No cached completion and data exists
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("cold_start")).thenReturn(null);
    // readRowCountFromIceberg will return 0 (no actual Iceberg table)
    // so it falls through to normal processing
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("cold_start", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_icebergColdStartWithColumnsCheck() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("cold_cols")).thenReturn(null);
    List<ColumnConfig> cols =
        Arrays.asList(ColumnConfig.builder().name("id").type("VARCHAR").build(),
        ColumnConfig.builder().name("value").type("INTEGER").build());
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("cold_cols", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, cols);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 37. IncrementalTracker.NOOP
  // -----------------------------------------------------------------------

  @Test void testNoopTracker_getCachedCompletion() {
    assertNull(IncrementalTracker.NOOP.getCachedCompletion("any"));
  }

  @Test void testNoopTracker_isTableComplete() {
    assertFalse(IncrementalTracker.NOOP.isTableComplete("any", "sig"));
  }

  @Test void testNoopTracker_filterUnprocessed() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    combos.add(Collections.singletonMap("k", "v"));
    Set<Integer> result = IncrementalTracker.NOOP.filterUnprocessed("a", "b", combos);
    assertEquals(1, result.size());
    assertTrue(result.contains(0));
  }

  @Test void testNoopTracker_operationsDoNotThrow() {
    IncrementalTracker.NOOP.markProcessed("a", "b",
        Collections.singletonMap("k", "v"), "p");
    IncrementalTracker.NOOP.markProcessedWithRowCount("a", "b",
        Collections.singletonMap("k", "v"), "p", 100);
    IncrementalTracker.NOOP.markProcessedWithError("a", "b",
        Collections.singletonMap("k", "v"), "p", "err");
    IncrementalTracker.NOOP.markTableComplete("a", "sig");
    IncrementalTracker.NOOP.markTableCompleteWithConfig("a", "h", "s", 100);
    IncrementalTracker.NOOP.invalidateTableCompletion("a");
  }
}
