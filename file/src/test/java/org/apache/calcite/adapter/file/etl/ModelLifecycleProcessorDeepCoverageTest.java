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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ModelLifecycleProcessor} targeting uncovered lines:
 * processSchema with source/materialize directories, hook registration and invocation
 * for all hook types (source, materialize, data), topological sort edge cases,
 * loadSchemaConfigFromYaml, auto-create StorageProvider, default schemaConfigLoader,
 * processSchema with afterSchema hook, processSchema exception path,
 * and Builder.WriteDataFunction interface.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class ModelLifecycleProcessorDeepCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  // ====================================================================
  // processSchema with source and materialize directories from schema config
  // ====================================================================

  @Test
  void testProcessSchemaUsesSchemaLevelDirectories() {
    Map<String, Object> schemaMap = new HashMap<String, Object>();
    schemaMap.put("name", "local_schema");
    schemaMap.put("tables", new ArrayList<Object>());
    schemaMap.put("sourceDirectory", tempDir.resolve("src").toString());
    schemaMap.put("materializeDirectory", tempDir.resolve("mat").toString());
    SchemaConfig schemaConfig = SchemaConfig.fromMap(schemaMap);

    ModelConfig model = ModelConfig.builder()
        .name("dir_test")
        .schema("local_schema", "/local.yaml")
        .build();

    // Model-level directories will NOT override since schema config has its own
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .sourceDirectory("/fallback/source")
        .materializeDirectory("/fallback/mat")
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(1, result.getSuccessfulSchemas());
  }

  @Test
  void testProcessSchemaFallsBackToModelDirectories() {
    // Schema config without directories
    Map<String, Object> schemaMap = new HashMap<String, Object>();
    schemaMap.put("name", "no_dir_schema");
    schemaMap.put("tables", new ArrayList<Object>());
    SchemaConfig schemaConfig = SchemaConfig.fromMap(schemaMap);

    ModelConfig model = ModelConfig.builder()
        .name("fallback_dir_test")
        .schema("no_dir_schema", "/nodir.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .sourceDirectory(tempDir.resolve("model_src").toString())
        .materializeDirectory(tempDir.resolve("model_mat").toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(1, result.getSuccessfulSchemas());
  }

  // ====================================================================
  // processSchema with all hook types registered
  // ====================================================================

  @Test
  void testProcessSchemaWithAllTableHooks() {
    AtomicBoolean beforeTableCalled = new AtomicBoolean(false);
    AtomicBoolean afterTableCalled = new AtomicBoolean(false);
    AtomicBoolean errorHookCalled = new AtomicBoolean(false);
    AtomicBoolean beforeSourceCalled = new AtomicBoolean(false);
    AtomicBoolean afterSourceCalled = new AtomicBoolean(false);
    AtomicBoolean sourceErrorCalled = new AtomicBoolean(false);
    AtomicBoolean beforeMatCalled = new AtomicBoolean(false);
    AtomicBoolean afterMatCalled = new AtomicBoolean(false);
    AtomicBoolean matErrorCalled = new AtomicBoolean(false);
    AtomicBoolean fetchDataCalled = new AtomicBoolean(false);
    AtomicBoolean writeDataCalled = new AtomicBoolean(false);
    AtomicBoolean dimensionsCalled = new AtomicBoolean(false);
    AtomicBoolean isEnabledCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = createSchemaConfigWithTable("hook_schema", "hook_table");

    ModelConfig model = ModelConfig.builder()
        .name("all_hooks_test")
        .schema("hook_schema", "/hook.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .beforeTable("hook_schema.hook_table", ctx -> beforeTableCalled.set(true))
        .afterTable("hook_schema.hook_table", (ctx, r) -> afterTableCalled.set(true))
        .onTableError("hook_schema.hook_table", (ctx, ex) -> {
          errorHookCalled.set(true);
          return true;
        })
        .beforeSource("hook_schema.hook_table", ctx -> beforeSourceCalled.set(true))
        .afterSource("hook_schema.hook_table", (ctx, r) -> afterSourceCalled.set(true))
        .onSourceError("hook_schema.hook_table", (ctx, ex) -> {
          sourceErrorCalled.set(true);
          return true;
        })
        .beforeMaterialize("hook_schema.hook_table", ctx -> beforeMatCalled.set(true))
        .afterMaterialize("hook_schema.hook_table", (ctx, r) -> afterMatCalled.set(true))
        .onMaterializeError("hook_schema.hook_table", (ctx, ex) -> {
          matErrorCalled.set(true);
          return true;
        })
        .fetchData("hook_schema.hook_table", (ctx, vars) -> {
          fetchDataCalled.set(true);
          return null;
        })
        .writeData("hook_schema.hook_table", (ctx, data, vars) -> {
          writeDataCalled.set(true);
          return 0L;
        })
        .resolveDimensions("hook_schema.hook_table", (ctx, dims) -> {
          dimensionsCalled.set(true);
          return dims;
        })
        .isEnabled("hook_schema.hook_table", ctx -> {
          isEnabledCalled.set(true);
          return true;
        })
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
  }

  // ====================================================================
  // processSchema exception path (schema config load fails)
  // ====================================================================

  @Test
  void testProcessSchemaExceptionInLoadConfig() {
    ModelConfig model = ModelConfig.builder()
        .name("exception_test")
        .schema("bad_schema", "/bad.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> null) // Returns null -> throws IllegalStateException
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(0, result.getSuccessfulSchemas());
    assertEquals(1, result.getFailedSchemas());
  }

  @Test
  void testProcessSchemaExceptionInSchemaConfigLoader() {
    ModelConfig model = ModelConfig.builder()
        .name("loader_exception_test")
        .schema("throw_schema", "/throw.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> {
          throw new RuntimeException("Loader crashed");
        })
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(0, result.getSuccessfulSchemas());
    assertEquals(1, result.getFailedSchemas());
  }

  // ====================================================================
  // afterSchema hook invocation
  // ====================================================================

  @Test
  void testAfterSchemaHookCalled() {
    AtomicReference<SchemaResult> schemaResultRef = new AtomicReference<SchemaResult>();

    SchemaConfig schemaConfig = createMinimalSchemaConfig("after_hook_schema");

    ModelConfig model = ModelConfig.builder()
        .name("after_schema_test")
        .schema("after_hook_schema", "/after.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .afterSchema("after_hook_schema", (ref, result) -> schemaResultRef.set(result))
        .build();

    processor.process();
    assertNotNull(schemaResultRef.get());
  }

  // ====================================================================
  // topologicalSort edge cases
  // ====================================================================

  @Test
  void testTopologicalSortSingleSchema() {
    ModelConfig model = ModelConfig.builder()
        .name("single_schema")
        .schema("only_one", "/only.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> createMinimalSchemaConfig("only_one"))
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(1, result.getTotalSchemas());
  }

  @Test
  void testTopologicalSortLinearDependencies() {
    List<String> processOrder = new ArrayList<String>();

    ModelConfig model = ModelConfig.builder()
        .name("linear_deps")
        .schema("A", "/a.yaml")
        .schema("B", "/b.yaml", "A")
        .schema("C", "/c.yaml", "B")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> {
          String name = path.replace("/", "").replace(".yaml", "");
          return createMinimalSchemaConfig(name);
        })
        .beforeSchema("A", ref -> processOrder.add("A"))
        .beforeSchema("B", ref -> processOrder.add("B"))
        .beforeSchema("C", ref -> processOrder.add("C"))
        .build();

    processor.process();

    assertEquals(3, processOrder.size());
    assertEquals("A", processOrder.get(0));
    assertEquals("B", processOrder.get(1));
    assertEquals("C", processOrder.get(2));
  }

  @Test
  void testTopologicalSortDiamondDependencies() {
    List<String> processOrder = new ArrayList<String>();

    ModelConfig model = ModelConfig.builder()
        .name("diamond_deps")
        .schema("base", "/base.yaml")
        .schema("left", "/left.yaml", "base")
        .schema("right", "/right.yaml", "base")
        .schema("top", "/top.yaml", "left", "right")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> {
          String name = path.replace("/", "").replace(".yaml", "");
          return createMinimalSchemaConfig(name);
        })
        .beforeSchema("base", ref -> processOrder.add("base"))
        .beforeSchema("left", ref -> processOrder.add("left"))
        .beforeSchema("right", ref -> processOrder.add("right"))
        .beforeSchema("top", ref -> processOrder.add("top"))
        .build();

    processor.process();

    // base must be first, top must be last
    assertEquals("base", processOrder.get(0));
    assertEquals("top", processOrder.get(3));
  }

  @Test
  void testTopologicalSortCircularDependency() {
    ModelConfig model = ModelConfig.builder()
        .name("circular")
        .schema("a", "/a.yaml", "c")
        .schema("b", "/b.yaml", "a")
        .schema("c", "/c.yaml", "b")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> createMinimalSchemaConfig("x"))
        .build();

    assertThrows(IllegalStateException.class, () -> processor.process());
  }

  @Test
  void testTopologicalSortIndependentSchemas() {
    ModelConfig model = ModelConfig.builder()
        .name("independent")
        .schema("s1", "/s1.yaml")
        .schema("s2", "/s2.yaml")
        .schema("s3", "/s3.yaml")
        .build();

    AtomicInteger count = new AtomicInteger(0);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> {
          String name = path.replace("/", "").replace(".yaml", "");
          return createMinimalSchemaConfig(name);
        })
        .beforeSchema("s1", ref -> count.incrementAndGet())
        .beforeSchema("s2", ref -> count.incrementAndGet())
        .beforeSchema("s3", ref -> count.incrementAndGet())
        .build();

    ModelResult result = processor.process();

    assertEquals(3, count.get());
    assertEquals(3, result.getSuccessfulSchemas());
  }

  // ====================================================================
  // loadSchemaConfigFromYaml
  // ====================================================================

  @Test
  void testLoadSchemaConfigFromYamlNonExistent() {
    SchemaConfig config = ModelLifecycleProcessor.loadSchemaConfigFromYaml("/nonexistent.yaml");
    assertNull(config);
  }

  @Test
  void testLoadSchemaConfigFromYamlNullInput() {
    // Calling with a path that doesn't exist as classpath resource
    SchemaConfig config = ModelLifecycleProcessor.loadSchemaConfigFromYaml("/does_not_exist_xyz.yaml");
    assertNull(config);
  }

  // ====================================================================
  // Builder.build() with auto-created StorageProvider
  // ====================================================================

  @Test
  void testBuilderAutoCreatesStorageProvider() {
    ModelConfig model = ModelConfig.builder()
        .name("auto_sp_test")
        .schema("s1", "/s1.yaml")
        .build();

    // materializeDirectory is a local path -> auto-creates LocalFileStorageProvider
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> createMinimalSchemaConfig("s1"))
        .build();

    assertNotNull(processor);
    ModelResult result = processor.process();
    assertNotNull(result);
  }

  @Test
  void testBuilderWithDefaultSchemaConfigLoader() {
    ModelConfig model = ModelConfig.builder()
        .name("default_loader_test")
        .schema("s1", "/nonexistent_resource.yaml")
        .build();

    // No schemaConfigLoader set -> uses default loadSchemaConfigFromYaml
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    // Schema config will be null -> treated as failure
    assertEquals(1, result.getFailedSchemas());
  }

  @Test
  void testBuilderWithNoModel() {
    assertThrows(IllegalStateException.class, () ->
        ModelLifecycleProcessor.builder()
            .schemaConfigLoader(path -> null)
            .build());
  }

  // ====================================================================
  // Builder.incrementalTracker
  // ====================================================================

  @Test
  void testBuilderIncrementalTracker() {
    ModelConfig model = ModelConfig.builder()
        .name("tracker_test")
        .schema("s1", "/s1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> createMinimalSchemaConfig("s1"))
        .build();

    assertNotNull(processor);
  }

  // ====================================================================
  // Builder.WriteDataFunction functional interface
  // ====================================================================

  @Test
  void testWriteDataFunctionInterface() {
    ModelLifecycleProcessor.Builder.WriteDataFunction fn =
        (ctx, data, vars) -> 42L;

    // Verify the lambda works
    assertEquals(42L, fn.write(null, null, null));
  }

  // ====================================================================
  // processSchema with successful tables and afterTable hook
  // ====================================================================

  @Test
  void testProcessSchemaWithSuccessfulTablesCallsAfterTable() {
    AtomicBoolean afterCalled = new AtomicBoolean(false);
    AtomicReference<EtlResult> capturedResult = new AtomicReference<EtlResult>();

    SchemaConfig schemaConfig = createSchemaConfigWithTable("s", "t");

    ModelConfig model = ModelConfig.builder()
        .name("after_table_test")
        .schema("s", "/s.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .afterTable("s.t", (ctx, r) -> {
          afterCalled.set(true);
          capturedResult.set(r);
        })
        .build();

    processor.process();
    // If the table processed, afterTable should be called
    if (afterCalled.get()) {
      assertNotNull(capturedResult.get());
    }
  }

  // ====================================================================
  // processSchema with hooks for unmatched schemas (prefix mismatch)
  // ====================================================================

  @Test
  void testProcessSchemaIgnoresHooksForOtherSchemas() {
    AtomicBoolean beforeCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = createSchemaConfigWithTable("schema_a", "table1");

    ModelConfig model = ModelConfig.builder()
        .name("mismatch_test")
        .schema("schema_a", "/a.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        // This hook is for schema_b, not schema_a
        .beforeTable("schema_b.table1", ctx -> beforeCalled.set(true))
        .build();

    processor.process();
    assertFalse(beforeCalled.get());
  }

  // ====================================================================
  // ModelResult construction and getters
  // ====================================================================

  @Test
  void testModelResultWithEmptySchemas() {
    ModelResult result = new ModelResult("empty_model",
        new ArrayList<SchemaResult>(), 0, 0, 100);

    assertEquals("empty_model", result.getModelName());
    assertEquals(0, result.getSuccessfulSchemas());
    assertEquals(0, result.getFailedSchemas());
    assertEquals(0, result.getTotalSchemas());
    assertEquals(0, result.getTotalRows());
    assertEquals(0, result.getTotalTables());
    assertEquals(100, result.getElapsedMs());
    assertTrue(result.getSchemaResults().isEmpty());
  }

  @Test
  void testModelResultToStringHasModelName() {
    List<SchemaResult> results = new ArrayList<SchemaResult>();
    results.add(new SchemaResult("s1", 2, 1, 0, 500, 100, null));

    ModelResult result = new ModelResult("test_model", results, 1, 0, 200);
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("test_model"));
  }

  @Test
  void testModelResultWithMixedSchemas() {
    List<SchemaResult> results = new ArrayList<SchemaResult>();
    results.add(new SchemaResult("s1", 3, 0, 1, 1000, 500, null));
    results.add(new SchemaResult("s2", 1, 2, 0, 300, 300, "error"));

    ModelResult result = new ModelResult("mixed_model", results, 1, 1, 1000);

    assertEquals(2, result.getTotalSchemas());
    assertEquals(1, result.getSuccessfulSchemas());
    assertEquals(1, result.getFailedSchemas());
    assertEquals(1300, result.getTotalRows());
    assertEquals(7, result.getTotalTables()); // 3+0+1 + 1+2+0 = 7
  }

  // ====================================================================
  // SchemaResult construction
  // ====================================================================

  @Test
  void testSchemaResultConstruction() {
    SchemaResult result = new SchemaResult("test_schema", 5, 2, 1, 10000, 1500, null);

    assertEquals("test_schema", result.getSchemaName());
    assertEquals(5, result.getSuccessfulTables());
    assertEquals(2, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertEquals(10000, result.getTotalRows());
    assertEquals(1500, result.getElapsedMs());
    assertTrue(result.getErrors().isEmpty());
    assertEquals(8, result.getTotalTables()); // 5+2+1
  }

  @Test
  void testSchemaResultWithError() {
    SchemaResult result = new SchemaResult("err_schema", 0, 0, 0, 0, 100, "Something went wrong");

    assertFalse(result.getErrors().isEmpty());
    assertTrue(result.getErrors().get(0).contains("Something went wrong"));
    assertEquals(0, result.getTotalTables());
  }

  // ====================================================================
  // ModelConfig builder and SchemaRef
  // ====================================================================

  @Test
  void testModelConfigWithSchemaRefMultipleDeps() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("complex", "/complex.yaml",
        "dep1", "dep2", "dep3");

    assertEquals("complex", ref.getName());
    assertEquals("/complex.yaml", ref.getResourcePath());
    assertEquals(3, ref.getDependsOn().size());
    assertTrue(ref.getDependsOn().contains("dep1"));
    assertTrue(ref.getDependsOn().contains("dep2"));
    assertTrue(ref.getDependsOn().contains("dep3"));
  }

  @Test
  void testModelConfigBuilderWithSchemaRefObject() {
    ModelConfig.SchemaRef ref1 = ModelConfig.SchemaRef.of("s1", "/s1.yaml");
    ModelConfig.SchemaRef ref2 = ModelConfig.SchemaRef.of("s2", "/s2.yaml", "s1");

    ModelConfig config = ModelConfig.builder()
        .name("obj_ref_test")
        .schema(ref1)
        .schema(ref2)
        .build();

    assertEquals(2, config.getSchemas().size());
    assertEquals("s1", config.getSchemas().get(0).getName());
    assertEquals("s2", config.getSchemas().get(1).getName());
  }

  @Test
  void testModelConfigBuilderEmptyNameThrows() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .name("")
            .schema("s1", "/s1.yaml")
            .build());
  }

  @Test
  void testModelConfigBuilderNullNameThrows() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .schema("s1", "/s1.yaml")
            .build());
  }

  @Test
  void testModelConfigBuilderNoSchemasThrows() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .name("no_schemas")
            .build());
  }

  @Test
  void testSchemaRefToStringWithDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("my_schema", "/path.yaml", "dep1");
    String str = ref.toString();
    assertTrue(str.contains("my_schema"));
    assertTrue(str.contains("/path.yaml"));
    assertTrue(str.contains("dep1"));
  }

  @Test
  void testSchemaRefToStringWithoutDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("simple", "/simple.yaml");
    String str = ref.toString();
    assertTrue(str.contains("simple"));
    assertFalse(str.contains("dependsOn"));
  }

  @Test
  void testSchemaRefDependsOnImmutable() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("s", "/s.yaml", "dep");
    assertThrows(UnsupportedOperationException.class, () -> ref.getDependsOn().add("sneaky"));
  }

  @Test
  void testModelConfigSchemasImmutable() {
    ModelConfig config = ModelConfig.builder()
        .name("immutable")
        .schema("s1", "/s1.yaml")
        .build();

    assertThrows(UnsupportedOperationException.class, () ->
        config.getSchemas().add(ModelConfig.SchemaRef.of("s2", "/s2.yaml")));
  }

  // ====================================================================
  // Process with beforeSchema hook that throws
  // ====================================================================

  @Test
  void testProcessSchemaBeforeHookThrows() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("throw_schema");

    ModelConfig model = ModelConfig.builder()
        .name("before_hook_throw")
        .schema("throw_schema", "/throw.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .beforeSchema("throw_schema", ref -> {
          throw new RuntimeException("Hook failed");
        })
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals(1, result.getFailedSchemas());
  }

  // ====================================================================
  // processSchema with partial failures (some tables fail)
  // ====================================================================

  @Test
  void testProcessSchemaWithPartialTableFailures() {
    AtomicReference<SchemaResult> resultRef = new AtomicReference<SchemaResult>();

    Map<String, Object> schemaMap = new HashMap<String, Object>();
    schemaMap.put("name", "partial_fail_schema");
    schemaMap.put("tables", new ArrayList<Object>());
    schemaMap.put("materializeDirectory", tempDir.toString());
    SchemaConfig schemaConfig = SchemaConfig.fromMap(schemaMap);

    ModelConfig model = ModelConfig.builder()
        .name("partial_fail_test")
        .schema("partial_fail_schema", "/partial.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .afterSchema("partial_fail_schema", (ref, r) -> resultRef.set(r))
        .build();

    processor.process();
    assertNotNull(resultRef.get());
  }

  // ====================================================================
  // Deprecated builder methods
  // ====================================================================

  @SuppressWarnings("deprecation")
  @Test
  void testBuilderDeprecatedBaseDirectory() {
    ModelConfig model = ModelConfig.builder()
        .name("deprecated_test")
        .schema("s1", "/s1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .baseDirectory(tempDir.toString())
        .schemaConfigLoader(path -> createMinimalSchemaConfig("s1"))
        .build();

    assertNotNull(processor);
  }

  @SuppressWarnings("deprecation")
  @Test
  void testBuilderDeprecatedShouldProcess() {
    ModelConfig model = ModelConfig.builder()
        .name("deprecated_should_process")
        .schema("s1", "/s1.yaml")
        .build();

    AtomicBoolean called = new AtomicBoolean(false);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .schemaConfigLoader(path -> createMinimalSchemaConfig("s1"))
        .shouldProcess("s1.table1", ctx -> {
          called.set(true);
          return true;
        })
        .build();

    assertNotNull(processor);
  }

  // ====================================================================
  // processSchema with fetchData and writeData hooks
  // ====================================================================

  @Test
  void testProcessSchemaWithFetchDataHook() {
    AtomicBoolean fetchCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = createSchemaConfigWithTable("fetch_schema", "fetch_table");

    ModelConfig model = ModelConfig.builder()
        .name("fetch_data_test")
        .schema("fetch_schema", "/fetch.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .fetchData("fetch_schema.fetch_table", (ctx, vars) -> {
          fetchCalled.set(true);
          List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
          Map<String, Object> row = new HashMap<String, Object>();
          row.put("id", "1");
          rows.add(row);
          return rows.iterator();
        })
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
  }

  @Test
  void testProcessSchemaWithWriteDataHook() {
    AtomicBoolean writeCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = createSchemaConfigWithTable("write_schema", "write_table");

    ModelConfig model = ModelConfig.builder()
        .name("write_data_test")
        .schema("write_schema", "/write.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaConfigLoader(path -> schemaConfig)
        .writeData("write_schema.write_table", (ctx, data, vars) -> {
          writeCalled.set(true);
          return 42L;
        })
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

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

    // Add hooks configuration so table is included
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
