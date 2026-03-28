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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Comprehensive unit tests for {@link SchemaContext}.
 *
 * <p>Tests the builder, getters, attribute map operations, bulk download
 * path management, and default value behavior.
 */
@Tag("unit")
class SchemaContextCoverageTest {

  // ========== Builder basic tests ==========

  @Test void testBuilderCreatesValidSchemaContext() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("my_schema").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();

    assertNotNull(ctx);
    assertSame(config, ctx.getConfig());
    assertSame(sp, ctx.getStorageProvider());
  }

  @Test void testBuilderRequiresConfig() {
    StorageProvider sp = mock(StorageProvider.class);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        SchemaContext.builder()
            .storageProvider(sp)
            .build());
    assertEquals("Schema config is required", ex.getMessage());
  }

  @Test void testBuilderRequiresStorageProvider() {
    SchemaConfig config = SchemaConfig.builder().name("test").build();

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        SchemaContext.builder()
            .config(config)
            .build());
    assertEquals("Storage provider is required", ex.getMessage());
  }

  // ========== Getter tests ==========

  @Test void testGetSchemaName() {
    SchemaContext ctx = createContext("my_schema");
    assertEquals("my_schema", ctx.getSchemaName());
  }

  @Test void testGetTables() {
    EtlPipelineConfig table = EtlPipelineConfig.builder()
        .name("test_table")
        .enabled(false)
        .build();
    SchemaConfig config = SchemaConfig.builder()
        .name("tables_schema")
        .addTable(table)
        .build();

    StorageProvider sp = mock(StorageProvider.class);
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();

    List<EtlPipelineConfig> tables = ctx.getTables();
    assertNotNull(tables);
    assertEquals(1, tables.size());
    assertEquals("test_table", tables.get(0).getName());
  }

  @Test void testGetTablesEmptyByDefault() {
    SchemaContext ctx = createContext("empty_schema");
    List<EtlPipelineConfig> tables = ctx.getTables();
    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  @Test void testGetStorageProvider() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("sp_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();

    assertSame(sp, ctx.getStorageProvider());
  }

  @Test void testGetSourceStorageProviderFallsBackToMainProvider() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("fallback_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();

    // When sourceStorageProvider not set, falls back to main
    assertSame(sp, ctx.getSourceStorageProvider());
  }

  @Test void testGetSourceStorageProviderExplicitlySet() {
    StorageProvider mainSp = mock(StorageProvider.class);
    StorageProvider sourceSp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("source_sp_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(mainSp)
        .sourceStorageProvider(sourceSp)
        .build();

    assertSame(mainSp, ctx.getStorageProvider());
    assertSame(sourceSp, ctx.getSourceStorageProvider());
  }

  @Test void testGetIncrementalTrackerDefaultsToNoop() {
    SchemaContext ctx = createContext("tracker_test");
    IncrementalTracker tracker = ctx.getIncrementalTracker();
    assertNotNull(tracker);
    assertSame(IncrementalTracker.NOOP, tracker);
  }

  @Test void testGetIncrementalTrackerExplicitlySet() {
    IncrementalTracker customTracker = mock(IncrementalTracker.class);
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("tracker_explicit").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .incrementalTracker(customTracker)
        .build();

    assertSame(customTracker, ctx.getIncrementalTracker());
  }

  @Test void testGetSourceDirectory() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("dir_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .sourceDirectory("/data/source")
        .build();

    assertEquals("/data/source", ctx.getSourceDirectory());
  }

  @Test void testGetSourceDirectoryDefaultNull() {
    SchemaContext ctx = createContext("no_source_dir");
    assertNull(ctx.getSourceDirectory());
  }

  @Test void testGetMaterializeDirectory() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("mat_dir_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .materializeDirectory("/output/parquet")
        .build();

    assertEquals("/output/parquet", ctx.getMaterializeDirectory());
  }

  @Test void testGetMaterializeDirectoryDefaultNull() {
    SchemaContext ctx = createContext("no_mat_dir");
    assertNull(ctx.getMaterializeDirectory());
  }

  @Test void testGetOperatingDirectory() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("op_dir_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .operatingDirectory("/home/user/.aperio/my_schema")
        .build();

    assertEquals("/home/user/.aperio/my_schema", ctx.getOperatingDirectory());
  }

  @Test void testGetOperatingDirectoryDefaultNull() {
    SchemaContext ctx = createContext("no_op_dir");
    assertNull(ctx.getOperatingDirectory());
  }

  @SuppressWarnings("deprecation")
  @Test void testGetBaseDirectoryDeprecated() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("base_dir_test").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .materializeDirectory("/output/base")
        .build();

    // Deprecated getBaseDirectory should return materializeDirectory
    assertEquals("/output/base", ctx.getBaseDirectory());
    assertEquals(ctx.getMaterializeDirectory(), ctx.getBaseDirectory());
  }

  @SuppressWarnings("deprecation")
  @Test void testDeprecatedBaseDirectoryBuilder() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("deprecated_builder").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .baseDirectory("/legacy/path")
        .build();

    assertEquals("/legacy/path", ctx.getMaterializeDirectory());
    assertEquals("/legacy/path", ctx.getBaseDirectory());
  }

  // ========== Attribute map tests ==========

  @Test void testGetAttributesInitiallyEmpty() {
    SchemaContext ctx = createContext("attrs_test");
    Map<String, Object> attrs = ctx.getAttributes();
    assertNotNull(attrs);
    assertTrue(attrs.isEmpty());
  }

  @Test void testSetAndGetAttribute() {
    SchemaContext ctx = createContext("attr_ops");

    ctx.setAttribute("key1", "value1");
    String val = ctx.getAttribute("key1");
    assertEquals("value1", val);
  }

  @Test void testSetAttributeOverwrite() {
    SchemaContext ctx = createContext("overwrite_test");

    ctx.setAttribute("key", "original");
    assertEquals("original", ctx.getAttribute("key"));

    ctx.setAttribute("key", "updated");
    assertEquals("updated", ctx.getAttribute("key"));
  }

  @Test void testGetAttributeNonExistentReturnsNull() {
    SchemaContext ctx = createContext("no_key_test");
    Object val = ctx.getAttribute("nonexistent");
    assertNull(val);
  }

  @Test void testSetAttributeNullValue() {
    SchemaContext ctx = createContext("null_val_test");

    ctx.setAttribute("nullable", null);
    assertNull(ctx.getAttribute("nullable"));
    // Key should exist even with null value
    assertTrue(ctx.getAttributes().containsKey("nullable"));
  }

  @Test void testSetAttributeVariousTypes() {
    SchemaContext ctx = createContext("type_test");

    ctx.setAttribute("string", "hello");
    ctx.setAttribute("integer", 42);
    ctx.setAttribute("long", 100L);
    ctx.setAttribute("double", 3.14);
    ctx.setAttribute("boolean", true);
    ctx.setAttribute("list", Collections.singletonList("item"));

    String strVal = ctx.getAttribute("string");
    assertEquals("hello", strVal);

    Integer intVal = ctx.getAttribute("integer");
    assertEquals(42, intVal.intValue());

    Long longVal = ctx.getAttribute("long");
    assertEquals(100L, longVal.longValue());

    Double dblVal = ctx.getAttribute("double");
    assertEquals(3.14, dblVal, 0.001);

    Boolean boolVal = ctx.getAttribute("boolean");
    assertTrue(boolVal);

    List<String> listVal = ctx.getAttribute("list");
    assertEquals(1, listVal.size());
    assertEquals("item", listVal.get(0));
  }

  @Test void testGetAttributesMapIsMutable() {
    SchemaContext ctx = createContext("mutable_map");

    // Direct manipulation of the attributes map
    Map<String, Object> attrs = ctx.getAttributes();
    attrs.put("directKey", "directValue");

    assertEquals("directValue", ctx.getAttribute("directKey"));
  }

  @Test void testMultipleAttributeOperations() {
    SchemaContext ctx = createContext("multi_ops");

    // Set multiple attributes
    ctx.setAttribute("a", 1);
    ctx.setAttribute("b", 2);
    ctx.setAttribute("c", 3);

    assertEquals(3, ctx.getAttributes().size());
    assertEquals(1, (int) ctx.<Integer>getAttribute("a"));
    assertEquals(2, (int) ctx.<Integer>getAttribute("b"));
    assertEquals(3, (int) ctx.<Integer>getAttribute("c"));

    // Overwrite one
    ctx.setAttribute("b", 20);
    assertEquals(3, ctx.getAttributes().size());
    assertEquals(20, (int) ctx.<Integer>getAttribute("b"));
  }

  // ========== Bulk download path tests ==========

  @Test void testSetAndGetBulkDownloadPath() {
    SchemaContext ctx = createContext("bulk_test");

    ctx.setBulkDownloadPath("qcew_bulk", "year=2024", "/cache/qcew/2024.zip");
    String path = ctx.getBulkDownloadPath("qcew_bulk", "year=2024");
    assertEquals("/cache/qcew/2024.zip", path);
  }

  @Test void testGetBulkDownloadPathNotSet() {
    SchemaContext ctx = createContext("bulk_missing");
    String path = ctx.getBulkDownloadPath("nonexistent", "key=value");
    assertNull(path);
  }

  @Test void testMultipleBulkDownloadPaths() {
    SchemaContext ctx = createContext("multi_bulk");

    ctx.setBulkDownloadPath("bulk1", "year=2023", "/cache/b1/2023.zip");
    ctx.setBulkDownloadPath("bulk1", "year=2024", "/cache/b1/2024.zip");
    ctx.setBulkDownloadPath("bulk2", "default", "/cache/b2/data.csv");

    assertEquals("/cache/b1/2023.zip", ctx.getBulkDownloadPath("bulk1", "year=2023"));
    assertEquals("/cache/b1/2024.zip", ctx.getBulkDownloadPath("bulk1", "year=2024"));
    assertEquals("/cache/b2/data.csv", ctx.getBulkDownloadPath("bulk2", "default"));
  }

  @Test void testBulkDownloadPathOverwrite() {
    SchemaContext ctx = createContext("overwrite_bulk");

    ctx.setBulkDownloadPath("bulk", "key=val", "/old/path.zip");
    assertEquals("/old/path.zip", ctx.getBulkDownloadPath("bulk", "key=val"));

    ctx.setBulkDownloadPath("bulk", "key=val", "/new/path.zip");
    assertEquals("/new/path.zip", ctx.getBulkDownloadPath("bulk", "key=val"));
  }

  @Test void testBulkDownloadPathStoredInAttributes() {
    SchemaContext ctx = createContext("bulk_attrs");

    ctx.setBulkDownloadPath("myBulk", "dim=val", "/path/to/file");

    // Verify it's stored in the attributes map
    Map<String, Object> attrs = ctx.getAttributes();
    String expectedKey = "bulkDownload.path.myBulk.dim=val";
    assertTrue(attrs.containsKey(expectedKey));
    assertEquals("/path/to/file", attrs.get(expectedKey));
  }

  @Test void testBulkDownloadPathWithComplexVariableKey() {
    SchemaContext ctx = createContext("complex_key");

    String complexKey = "year=2024,frequency=annual,region=US";
    ctx.setBulkDownloadPath("detailed", complexKey, "/cache/detailed/2024_annual_US.zip");

    assertEquals("/cache/detailed/2024_annual_US.zip",
        ctx.getBulkDownloadPath("detailed", complexKey));
  }

  // ========== getBulkDownload tests ==========

  @Test void testGetBulkDownloadFromConfig() {
    BulkDownloadConfig bulkConfig = BulkDownloadConfig.builder()
        .name("test_bulk")
        .cachePattern("bulk/{year}/data.zip")
        .url("https://example.com/{year}/data.zip")
        .build();

    Map<String, BulkDownloadConfig> bulkDownloads = new HashMap<String, BulkDownloadConfig>();
    bulkDownloads.put("test_bulk", bulkConfig);

    SchemaConfig config = SchemaConfig.builder()
        .name("bulk_config_schema")
        .bulkDownloads(bulkDownloads)
        .build();

    StorageProvider sp = mock(StorageProvider.class);
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();

    BulkDownloadConfig retrieved = ctx.getBulkDownload("test_bulk");
    assertNotNull(retrieved);
    assertEquals("test_bulk", retrieved.getName());
    assertEquals("bulk/{year}/data.zip", retrieved.getCachePattern());
  }

  @Test void testGetBulkDownloadNotFound() {
    SchemaContext ctx = createContext("no_bulk");
    BulkDownloadConfig result = ctx.getBulkDownload("nonexistent");
    assertNull(result);
  }

  // ========== Builder with all fields ==========

  @Test void testBuilderWithAllFieldsSet() {
    StorageProvider mainSp = mock(StorageProvider.class);
    StorageProvider sourceSp = mock(StorageProvider.class);
    IncrementalTracker tracker = mock(IncrementalTracker.class);
    SchemaConfig config = SchemaConfig.builder().name("full_schema").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(mainSp)
        .sourceStorageProvider(sourceSp)
        .incrementalTracker(tracker)
        .sourceDirectory("/data/raw")
        .materializeDirectory("/data/output")
        .operatingDirectory("/home/.aperio/full_schema")
        .build();

    assertSame(config, ctx.getConfig());
    assertEquals("full_schema", ctx.getSchemaName());
    assertSame(mainSp, ctx.getStorageProvider());
    assertSame(sourceSp, ctx.getSourceStorageProvider());
    assertSame(tracker, ctx.getIncrementalTracker());
    assertEquals("/data/raw", ctx.getSourceDirectory());
    assertEquals("/data/output", ctx.getMaterializeDirectory());
    assertEquals("/home/.aperio/full_schema", ctx.getOperatingDirectory());
    assertNotNull(ctx.getAttributes());
    assertTrue(ctx.getAttributes().isEmpty());
  }

  // ========== Independent contexts do not share attributes ==========

  @Test void testIndependentContextsHaveSeparateAttributes() {
    SchemaContext ctx1 = createContext("schema1");
    SchemaContext ctx2 = createContext("schema2");

    ctx1.setAttribute("shared_key", "value1");
    ctx2.setAttribute("shared_key", "value2");

    assertEquals("value1", ctx1.getAttribute("shared_key"));
    assertEquals("value2", ctx2.getAttribute("shared_key"));
  }

  // ========== Null incremental tracker defaults to NOOP ==========

  @Test void testNullIncrementalTrackerDefaultsToNoop() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name("null_tracker").build();

    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .incrementalTracker(null)
        .build();

    assertNotNull(ctx.getIncrementalTracker());
    assertSame(IncrementalTracker.NOOP, ctx.getIncrementalTracker());
  }

  // ========== Helper methods ==========

  /**
   * Creates a minimal SchemaContext with only required fields.
   */
  private SchemaContext createContext(String schemaName) {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig config = SchemaConfig.builder().name(schemaName).build();

    return SchemaContext.builder()
        .config(config)
        .storageProvider(sp)
        .build();
  }
}
