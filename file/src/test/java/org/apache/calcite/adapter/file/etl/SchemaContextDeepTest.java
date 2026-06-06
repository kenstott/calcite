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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep tests for {@link SchemaContext}, {@link TableContext},
 * and {@link SchemaLifecycleListener}.
 */
@Tag("unit")
class SchemaContextDeepTest {

  private StorageProvider storageProvider = new LocalFileStorageProvider();

  private SchemaConfig createMinimalSchemaConfig(String name) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", name);
    map.put("tables", Collections.emptyList());
    return SchemaConfig.fromMap(map);
  }

  @Test void testSchemaContextBuilderFull() {
    SchemaConfig config = createMinimalSchemaConfig("test_schema");
    StorageProvider sourceProvider = new LocalFileStorageProvider();

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceStorageProvider(sourceProvider)
        .incrementalTracker(IncrementalTracker.NOOP)
        .sourceDirectory("/source")
        .materializeDirectory("/materialize")
        .operatingDirectory("/operating")
        .build();

    assertEquals("test_schema", context.getSchemaName());
    assertEquals(config, context.getConfig());
    assertEquals(storageProvider, context.getStorageProvider());
    assertEquals(sourceProvider, context.getSourceStorageProvider());
    assertEquals("/source", context.getSourceDirectory());
    assertEquals("/materialize", context.getMaterializeDirectory());
    assertEquals("/operating", context.getOperatingDirectory());
    assertNotNull(context.getIncrementalTracker());
    assertNotNull(context.getAttributes());
    assertTrue(context.getAttributes().isEmpty());
  }

  @Test void testSchemaContextSourceProviderDefault() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();

    // When no sourceStorageProvider set, should default to main storageProvider
    assertEquals(storageProvider, context.getSourceStorageProvider());
  }

  @Test void testSchemaContextAttributes() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();

    // Set and get attributes
    context.setAttribute("key1", "value1");
    context.setAttribute("key2", 42);

    assertEquals("value1", context.getAttribute("key1"));
    assertEquals(42, (int) context.<Integer>getAttribute("key2"));
    assertNull(context.getAttribute("nonexistent"));
  }

  @Test void testSchemaContextBulkDownloadPath() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();

    context.setBulkDownloadPath("census_data", "year=2024", "/cache/census_2024.json");

    assertEquals("/cache/census_2024.json",
        context.getBulkDownloadPath("census_data", "year=2024"));
    assertNull(context.getBulkDownloadPath("census_data", "year=2023"));
    assertNull(context.getBulkDownloadPath("other_data", "year=2024"));
  }

  @Test void testSchemaContextRequiresConfig() {
    assertThrows(IllegalArgumentException.class, () ->
        SchemaContext.builder()
            .storageProvider(storageProvider)
            .build());
  }

  @Test void testSchemaContextRequiresStorageProvider() {
    SchemaConfig config = createMinimalSchemaConfig("test");
    assertThrows(IllegalArgumentException.class, () ->
        SchemaContext.builder()
            .config(config)
            .build());
  }

  @SuppressWarnings("deprecation")
  @Test void testSchemaContextDeprecatedBaseDirectory() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory("/mat")
        .build();

    assertEquals("/mat", context.getBaseDirectory());
  }

  @SuppressWarnings("deprecation")
  @Test void testSchemaContextDeprecatedBuilderBaseDirectory() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .baseDirectory("/base")
        .build();

    assertEquals("/base", context.getMaterializeDirectory());
  }

  @Test void testSchemaContextIncrementalTrackerDefault() {
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();

    // Should default to NOOP tracker
    assertNotNull(context.getIncrementalTracker());
  }

  // --- TableContext tests ---

  @Test void testTableContextBuilder() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("schema");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory("/mat")
        .build();

    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("my_table")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/output")
                .build())
            .build())
        .build();

    TableContext tableContext = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(2)
        .totalTables(10)
        .build();

    assertEquals(tableConfig, tableContext.getTableConfig());
    assertEquals(schemaContext, tableContext.getSchemaContext());
    assertEquals(2, tableContext.getTableIndex());
    assertEquals(10, tableContext.getTotalTables());
    assertEquals("my_table", tableContext.getTableName());
  }

  @Test void testTableContextAttributes() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("schema");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .build();

    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("table")
        .source(HttpSourceConfig.builder()
            .url("https://example.com")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/output")
                .build())
            .build())
        .build();

    TableContext tableContext = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(1)
        .build();

    assertNotNull(tableContext.getAttributes());
    tableContext.setAttribute("count", 100);
    assertEquals(100, (int) tableContext.<Integer>getAttribute("count"));
  }

  @Test void testTableContextDetectSource() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("schema");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .build();

    // Table with HTTP source
    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("http_table")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/output")
                .build())
            .build())
        .build();

    TableContext tableContext = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(1)
        .build();

    String sourceType = tableContext.detectSource();
    assertNotNull(sourceType);
  }

  // --- SchemaLifecycleListener default implementation tests ---

  @Test void testDefaultSchemaLifecycleListener() throws Exception {
    SchemaLifecycleListener listener = SchemaLifecycleListener.NOOP;
    SchemaConfig config = createMinimalSchemaConfig("test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();

    // Default methods should be no-ops
    listener.beforeSchema(context);
    listener.afterSchema(context, new SchemaResult("test", 0, 0, 0, 0, 0, null));
    listener.onSchemaError(context, new Exception("test"));
  }

  // --- SchemaResult tests ---

  @Test void testSchemaResultBuilder() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test_schema")
        .addTableResult("table1", EtlResult.success("table1", 100, 5, 500))
        .addTableResult("table2", EtlResult.success("table2", 200, 3, 300))
        .elapsedMs(1000)
        .build();

    assertEquals("test_schema", result.getSchemaName());
    assertEquals(2, result.getTableResults().size());
    assertEquals(1000, result.getElapsedMs());
    assertNotNull(result.toString());
  }

  @Test void testSchemaResultWithFailure() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("failed_schema")
        .addTableResult("table1", EtlResult.success("table1", 100, 5, 500))
        .addTableResult("table2", EtlResult.failure("table2", "Connection error", 100))
        .elapsedMs(600)
        .build();

    assertEquals("failed_schema", result.getSchemaName());
    assertEquals(2, result.getTableResults().size());
    assertTrue(result.hasErrors());
  }

  // --- SourceResult tests ---

  @Test void testSourceResultSuccess() {
    SourceResult result = SourceResult.success(500, 1024, 1000, "http://example.com");
    assertTrue(result.isSuccess());
    assertEquals(500, result.getRecordCount());
    assertEquals(1024, result.getBytesRead());
    assertEquals(1000, result.getDurationMs());
    assertEquals("http://example.com", result.getSourceUrl());
  }

  @Test void testSourceResultSkipped() {
    SourceResult result = SourceResult.skipped("Already cached");
    assertEquals(SourceResult.Status.SKIPPED, result.getStatus());
    assertEquals("Already cached", result.getErrorMessage());
  }

  @Test void testSourceResultError() {
    SourceResult result = SourceResult.error("Connection failed", 100, "http://example.com");
    assertTrue(result.isError());
    assertEquals("Connection failed", result.getErrorMessage());
    assertEquals("http://example.com", result.getSourceUrl());
  }

  // --- ModelResult tests ---

  @Test void testModelResultConstructor() {
    List<SchemaResult> schemas = new ArrayList<SchemaResult>();
    schemas.add(new SchemaResult("schema1", 3, 0, 0, 10000, 2000, null));
    schemas.add(new SchemaResult("schema2", 2, 1, 0, 5000, 3000, "error"));

    ModelResult result = new ModelResult("test_model", schemas, 2, 0, 5000);

    assertEquals("test_model", result.getModelName());
    assertEquals(2, result.getTotalSchemas());
    assertEquals(15000, result.getTotalRows());
    assertEquals(5000, result.getElapsedMs());
    assertNotNull(result.toString());
  }
}
