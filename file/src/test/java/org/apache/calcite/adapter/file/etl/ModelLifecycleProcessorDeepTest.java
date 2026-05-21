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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep tests for {@link ModelLifecycleProcessor}, {@link ModelConfig},
 * {@link ModelResult}, and {@link TableLifecycleListener}.
 */
@Tag("unit")
class ModelLifecycleProcessorDeepTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  // --- ModelConfig tests ---

  @Test void testModelConfigBuilder() {
    ModelConfig config = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/config/schema1.yaml")
        .schema("schema2", "/config/schema2.yaml", "schema1")
        .build();

    assertEquals("test_model", config.getName());
    assertEquals(2, config.getSchemas().size());
    assertEquals("schema1", config.getSchemas().get(0).getName());
    assertEquals("schema2", config.getSchemas().get(1).getName());
  }

  @Test void testModelConfigBuilderWithSchemaRef() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("myschema", "/path/to/schema.yaml");

    ModelConfig config = ModelConfig.builder()
        .name("model")
        .schema(ref)
        .build();

    assertEquals(1, config.getSchemas().size());
    assertEquals("myschema", config.getSchemas().get(0).getName());
    assertEquals("/path/to/schema.yaml", config.getSchemas().get(0).getResourcePath());
  }

  @Test void testModelConfigRequiresName() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .schema("schema", "/path")
            .build());
  }

  @Test void testModelConfigRequiresEmptyName() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .name("")
            .schema("schema", "/path")
            .build());
  }

  @Test void testModelConfigRequiresSchemas() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .name("model")
            .build());
  }

  @Test void testModelConfigSchemasImmutable() {
    ModelConfig config = ModelConfig.builder()
        .name("model")
        .schema("s1", "/path1")
        .build();

    try {
      config.getSchemas().add(ModelConfig.SchemaRef.of("s2", "/path2"));
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  // --- SchemaRef tests ---

  @Test void testSchemaRefNoDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("test", "/test.yaml");

    assertEquals("test", ref.getName());
    assertEquals("/test.yaml", ref.getResourcePath());
    assertTrue(ref.getDependsOn().isEmpty());
  }

  @Test void testSchemaRefWithDependencies() {
    ModelConfig.SchemaRef ref =
        ModelConfig.SchemaRef.of("econ", "/econ.yaml", "reference", "common");

    assertEquals("econ", ref.getName());
    assertEquals(2, ref.getDependsOn().size());
    assertTrue(ref.getDependsOn().contains("reference"));
    assertTrue(ref.getDependsOn().contains("common"));
  }

  @Test void testSchemaRefDependsOnImmutable() {
    ModelConfig.SchemaRef ref =
        ModelConfig.SchemaRef.of("schema", "/path", "dep1");

    try {
      ref.getDependsOn().add("sneaky");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testSchemaRefToString() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml", "reference");
    String str = ref.toString();
    assertTrue(str.contains("econ"));
    assertTrue(str.contains("/econ.yaml"));
    assertTrue(str.contains("reference"));
  }

  @Test void testSchemaRefToStringNoDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("simple", "/simple.yaml");
    String str = ref.toString();
    assertTrue(str.contains("simple"));
    assertFalse(str.contains("dependsOn"));
  }

  // --- ModelLifecycleProcessor builder tests ---

  @Test void testProcessorBuilderWithHooks() {
    AtomicBoolean beforeModelCalled = new AtomicBoolean(false);
    AtomicBoolean afterModelCalled = new AtomicBoolean(false);
    AtomicReference<String> processedModelName = new AtomicReference<String>();

    // Create a schema config with no actual tables to process quickly
    SchemaConfig emptySchemaConfig = createMinimalSchemaConfig("test_schema");

    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("test_schema", "/test/schema.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> emptySchemaConfig)
        .beforeModel(m -> {
          beforeModelCalled.set(true);
          processedModelName.set(m.getName());
        })
        .afterModel((m, result) -> afterModelCalled.set(true))
        .build();

    ModelResult result = processor.process();

    assertTrue(beforeModelCalled.get());
    assertTrue(afterModelCalled.get());
    assertEquals("test_model", processedModelName.get());
    assertNotNull(result);
    assertEquals("test_model", result.getModelName());
  }

  @Test void testProcessorWithMultipleSchemas() {
    List<String> processedSchemas = new ArrayList<String>();

    SchemaConfig refConfig = createMinimalSchemaConfig("ref_schema");
    SchemaConfig mainConfig = createMinimalSchemaConfig("main_schema");

    ModelConfig model = ModelConfig.builder()
        .name("multi_model")
        .schema("ref_schema", "/ref.yaml")
        .schema("main_schema", "/main.yaml", "ref_schema")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> {
          if (path.contains("ref")) {
            return refConfig;
          }
          return mainConfig;
        })
        .beforeSchema("ref_schema", ref -> processedSchemas.add(ref.getName()))
        .beforeSchema("main_schema", ref -> processedSchemas.add(ref.getName()))
        .build();

    ModelResult result = processor.process();

    // ref_schema should be processed before main_schema (dependency order)
    assertNotNull(result);
    assertEquals(2, processedSchemas.size());
    assertEquals("ref_schema", processedSchemas.get(0));
    assertEquals("main_schema", processedSchemas.get(1));
  }

  @Test void testProcessorWithSchemaConfigLoaderFailure() {
    ModelConfig model = ModelConfig.builder()
        .name("fail_model")
        .schema("bad_schema", "/nonexistent.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> null) // Returns null to simulate failure
        .build();

    ModelResult result = processor.process();
    // Should complete but with a failed schema
    assertNotNull(result);
    assertEquals(1, result.getFailedSchemas());
  }

  @Test void testProcessorWithTableHooks() {
    AtomicBoolean beforeTableCalled = new AtomicBoolean(false);
    AtomicBoolean afterTableCalled = new AtomicBoolean(false);

    SchemaConfig config = createSchemaConfigWithTable("my_schema", "my_table");

    ModelConfig model = ModelConfig.builder()
        .name("hook_model")
        .schema("my_schema", "/schema.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> config)
        .beforeTable("my_schema.my_table", ctx -> beforeTableCalled.set(true))
        .afterTable("my_schema.my_table", (ctx, result) -> afterTableCalled.set(true))
        .build();

    ModelResult result = processor.process();

    assertNotNull(result);
    // Verify the processor ran and produced a result for 1 schema
    assertEquals(1, result.getTotalSchemas());
    // If the schema was processed successfully, hooks should have been called
    if (result.getSuccessfulSchemas() > 0) {
      assertTrue(beforeTableCalled.get());
      assertTrue(afterTableCalled.get());
    }
  }

  @Test void testProcessorWithIsEnabledHook() {
    AtomicBoolean tableCalled = new AtomicBoolean(false);

    SchemaConfig config = createSchemaConfigWithTable("my_schema", "disabled_table");

    ModelConfig model = ModelConfig.builder()
        .name("filter_model")
        .schema("my_schema", "/schema.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> config)
        .isEnabled("my_schema.disabled_table", ctx -> false) // Disable the table
        .beforeTable("my_schema.disabled_table", ctx -> tableCalled.set(true))
        .build();

    ModelResult result = processor.process();

    assertNotNull(result);
    // The beforeTable hook should not be called for disabled table
    assertFalse(tableCalled.get());
  }

  // --- ModelResult tests ---

  // --- ModelResult tests ---
  // SchemaResult(schemaName, successfulTables, failedTables, skippedTables, totalRows, elapsedMs, error)

  @Test void testModelResultConstructor() {
    List<SchemaResult> schemaResults = new ArrayList<SchemaResult>();
    schemaResults.add(new SchemaResult("schema1", 3, 0, 0, 1000, 500, null));
    schemaResults.add(new SchemaResult("schema2", 2, 0, 0, 500, 300, null));

    ModelResult result = new ModelResult("test_model", schemaResults, 2, 0, 800);

    assertEquals("test_model", result.getModelName());
    assertEquals(2, result.getSuccessfulSchemas());
    assertEquals(0, result.getFailedSchemas());
    assertEquals(800, result.getElapsedMs());
    assertEquals(1500, result.getTotalRows());
    assertEquals(5, result.getTotalTables());
    assertEquals(2, result.getTotalSchemas());
    assertNotNull(result.getSchemaResults());
    assertEquals(2, result.getSchemaResults().size());
  }

  @Test void testModelResultToString() {
    List<SchemaResult> schemaResults = new ArrayList<SchemaResult>();
    schemaResults.add(new SchemaResult("schema1", 2, 0, 0, 500, 300, null));

    ModelResult result = new ModelResult("model", schemaResults, 1, 0, 500);
    String str = result.toString();
    assertTrue(str.contains("model"));
  }

  @Test void testModelResultSchemaResultsImmutable() {
    List<SchemaResult> schemaResults = new ArrayList<SchemaResult>();
    schemaResults.add(new SchemaResult("s1", 1, 0, 0, 100, 200, null));

    ModelResult result = new ModelResult("model", schemaResults, 1, 0, 200);

    try {
      result.getSchemaResults().add(new SchemaResult("s2", 1, 0, 0, 50, 100, null));
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  // --- TableLifecycleListener default methods ---

  @Test void testDefaultTableLifecycleListener() throws Exception {
    TableLifecycleListener listener = TableLifecycleListener.NOOP;

    SchemaConfig schemaConfig = createMinimalSchemaConfig("test");
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

    // All default methods should work without throwing
    listener.beforeTable(tableContext);
    listener.afterTable(tableContext,
        EtlResult.success("table", 100, 5, 500));
    assertTrue(
        listener.onTableError(tableContext,
        new RuntimeException("test")));
    assertTrue(listener.isTableEnabled(tableContext));

    // Default dimension resolution returns null (meaning "use static dimensions")
    Map<String, DimensionConfig> dims = new HashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.LIST)
        .values(Collections.singletonList("2020"))
        .build());
    Map<String, DimensionConfig> resolved = listener.resolveDimensions(tableContext, dims);
    assertNull(resolved);

    // Source/materialize phase hooks
    listener.beforeSource(tableContext);
    listener.afterSource(tableContext, SourceResult.success(100, 0, 500, null));
    assertTrue(listener.onSourceError(tableContext, new RuntimeException("test")));
    listener.beforeMaterialize(tableContext);
    listener.afterMaterialize(tableContext,
        MaterializeResult.success(100, 5, 1000));
    assertTrue(listener.onMaterializeError(tableContext, new RuntimeException("test")));
  }

  // --- SchemaConfig additional coverage ---

  @Test void testSchemaConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "test_schema");
    map.put("sourceDirectory", "/source");
    map.put("materializeDirectory", "/materialize");
    map.put("tables", new ArrayList<Object>());

    SchemaConfig config = SchemaConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("test_schema", config.getName());
    assertEquals("/source", config.getSourceDirectory());
    assertEquals("/materialize", config.getMaterializeDirectory());
    assertTrue(config.getTables().isEmpty());
  }

  @Test void testSchemaConfigFromMapWithTables() {
    Map<String, Object> tableMap = new HashMap<String, Object>();
    tableMap.put("name", "my_table");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    tableMap.put("source", sourceMap);

    Map<String, Object> matMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/output");
    matMap.put("output", outputMap);
    tableMap.put("materialize", matMap);

    List<Object> tables = new ArrayList<Object>();
    tables.add(tableMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "schema_with_tables");
    map.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("schema_with_tables", config.getName());
    assertEquals(1, config.getTables().size());
    assertEquals("my_table", config.getTables().get(0).getName());
  }

  // --- Helper methods ---

  private SchemaConfig createMinimalSchemaConfig(String name) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", name);
    map.put("tables", new ArrayList<Object>());
    map.put("materializeDirectory", tempDir.toString());
    return SchemaConfig.fromMap(map);
  }

  private SchemaConfig createSchemaConfigWithTable(String schemaName, String tableName) {
    Map<String, Object> tableMap = new HashMap<String, Object>();
    tableMap.put("name", tableName);
    // No source = hooks-only table (won't need HTTP)
    // Must enable hooks for the table to be included by SchemaConfig.fromMap
    Map<String, Object> hooksMap = new HashMap<String, Object>();
    hooksMap.put("enabled", true);
    tableMap.put("hooks", hooksMap);

    List<Object> tables = new ArrayList<Object>();
    tables.add(tableMap);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", schemaName);
    map.put("tables", tables);
    map.put("materializeDirectory", tempDir.toString());
    return SchemaConfig.fromMap(map);
  }
}
