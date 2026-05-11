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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ModelLifecycleProcessor covering builder pattern,
 * topological sort, hook registration, and model processing.
 */
@Tag("unit")
public class ModelLifecycleProcessorCoverageTest {

  @Test void testBuilderRequiresModel() {
    assertThrows(IllegalStateException.class, () ->
        ModelLifecycleProcessor.builder()
            .schemaConfigLoader(path -> null)
            .build());
  }

  @Test void testBuilderBasicConstruction() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithDirectories() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .sourceDirectory("/source/path")
        .materializeDirectory("/tmp/test_output")
        .schemaConfigLoader(path -> null)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    AtomicBoolean beforeCalled = new AtomicBoolean(false);
    AtomicBoolean afterCalled = new AtomicBoolean(false);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeModel(m -> beforeCalled.set(true))
        .afterModel((m, r) -> afterCalled.set(true))
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithSchemaHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    AtomicBoolean beforeSchema = new AtomicBoolean(false);
    AtomicBoolean afterSchema = new AtomicBoolean(false);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeSchema("schema1", ref -> beforeSchema.set(true))
        .afterSchema("schema1", (ref, result) -> afterSchema.set(true))
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithTableHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeTable("schema1.table1", ctx -> {})
        .afterTable("schema1.table1", (ctx, result) -> {})
        .onTableError("schema1.table1", (ctx, ex) -> true)
        .isEnabled("schema1.table1", ctx -> true)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithSourcePhaseHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeSource("schema1.table1", ctx -> {})
        .afterSource("schema1.table1", (ctx, result) -> {})
        .onSourceError("schema1.table1", (ctx, ex) -> true)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithMaterializePhaseHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeMaterialize("schema1.table1", ctx -> {})
        .afterMaterialize("schema1.table1", (ctx, result) -> {})
        .onMaterializeError("schema1.table1", (ctx, ex) -> true)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderWithDataHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .fetchData("schema1.table1", (ctx, vars) -> null)
        .writeData("schema1.table1", (ctx, data, vars) -> 0L)
        .build();

    assertNotNull(processor);
  }

  @Test void testBuilderDeprecatedMethods() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/schema1.yaml")
        .build();

    @SuppressWarnings("deprecation")
    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .baseDirectory("/tmp/deprecated_dir")
        .shouldProcess("schema1.table1", ctx -> true)
        .build();

    assertNotNull(processor);
  }

  @Test void testProcessWithMissingSchemaConfig() {
    ModelConfig model = ModelConfig.builder()
        .name("test_model")
        .schema("schema1", "/nonexistent.yaml")
        .build();

    AtomicReference<ModelResult> resultRef = new AtomicReference<>();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .afterModel((m, r) -> resultRef.set(r))
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    // Schema loading should fail but not crash the processor
    assertEquals("test_model", result.getModelName());
  }

  @Test void testProcessCallsBeforeAndAfterModelHooks() {
    ModelConfig model = ModelConfig.builder()
        .name("hook_test_model")
        .schema("s1", "/s1.yaml")
        .build();

    AtomicBoolean beforeCalled = new AtomicBoolean(false);
    AtomicBoolean afterCalled = new AtomicBoolean(false);

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeModel(m -> beforeCalled.set(true))
        .afterModel((m, r) -> afterCalled.set(true))
        .build();

    ModelResult result = processor.process();

    assertTrue(beforeCalled.get(), "beforeModel hook should have been called");
    assertTrue(afterCalled.get(), "afterModel hook should have been called");
    assertNotNull(result);
  }

  @Test void testTopologicalSortWithDependencies() {
    // Create schemas with dependencies
    ModelConfig model = ModelConfig.builder()
        .name("dep_test")
        .schema("reference", "/ref.yaml")
        .schema("facts", "/facts.yaml", "reference")
        .schema("derived", "/derived.yaml", "facts")
        .build();

    List<String> processOrder = new ArrayList<>();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .beforeSchema("reference", ref -> processOrder.add("reference"))
        .beforeSchema("facts", ref -> processOrder.add("facts"))
        .beforeSchema("derived", ref -> processOrder.add("derived"))
        .build();

    processor.process();

    // Verify order: reference must come before facts, facts before derived
    if (processOrder.contains("reference") && processOrder.contains("facts")) {
      assertTrue(processOrder.indexOf("reference") < processOrder.indexOf("facts"));
    }
    if (processOrder.contains("facts") && processOrder.contains("derived")) {
      assertTrue(processOrder.indexOf("facts") < processOrder.indexOf("derived"));
    }
  }

  @Test void testTopologicalSortCircularDependencyDetected() {
    ModelConfig model = ModelConfig.builder()
        .name("circular_test")
        .schema("a", "/a.yaml", "b")
        .schema("b", "/b.yaml", "a")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .build();

    // Processing should throw due to circular dependency
    assertThrows(IllegalStateException.class, () -> processor.process());
  }

  @Test void testModelConfigBuilder() {
    ModelConfig model = ModelConfig.builder()
        .name("test")
        .schema("s1", "/s1.yaml")
        .schema("s2", "/s2.yaml", "s1")
        .schema(ModelConfig.SchemaRef.of("s3", "/s3.yaml"))
        .build();

    assertEquals("test", model.getName());
    assertEquals(3, model.getSchemas().size());
    assertEquals("s1", model.getSchemas().get(0).getName());
    assertEquals("/s1.yaml", model.getSchemas().get(0).getResourcePath());
    assertTrue(model.getSchemas().get(0).getDependsOn().isEmpty());
    assertEquals(1, model.getSchemas().get(1).getDependsOn().size());
    assertEquals("s1", model.getSchemas().get(1).getDependsOn().get(0));
  }

  @Test void testModelConfigBuilderRequiresName() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .schema("s1", "/s1.yaml")
            .build());
  }

  @Test void testModelConfigBuilderRequiresSchemas() {
    assertThrows(IllegalStateException.class, () ->
        ModelConfig.builder()
            .name("empty")
            .build());
  }

  @Test void testSchemaRefToString() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("s1", "/s1.yaml");
    String str = ref.toString();
    assertNotNull(str);
    assertTrue(str.contains("s1"));
    assertTrue(str.contains("/s1.yaml"));

    ModelConfig.SchemaRef refWithDeps = ModelConfig.SchemaRef.of("s2", "/s2.yaml", "s1");
    String str2 = refWithDeps.toString();
    assertTrue(str2.contains("dependsOn"));
  }

  @Test void testModelResultAggregation() {
    ModelConfig model = ModelConfig.builder()
        .name("agg_test")
        .schema("s1", "/s1.yaml")
        .schema("s2", "/s2.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .build();

    ModelResult result = processor.process();
    assertNotNull(result);
    assertEquals("agg_test", result.getModelName());
  }

  @Test void testLoadSchemaConfigFromYamlNonExistentResource() {
    SchemaConfig config = ModelLifecycleProcessor.loadSchemaConfigFromYaml("/nonexistent.yaml");
    // Should return null for missing resources
    assertTrue(config == null);
  }

  @Test void testResolveDimensionsHook() {
    ModelConfig model = ModelConfig.builder()
        .name("dim_test")
        .schema("schema1", "/s1.yaml")
        .build();

    ModelLifecycleProcessor processor = ModelLifecycleProcessor.builder()
        .model(model)
        .schemaConfigLoader(path -> null)
        .resolveDimensions("schema1.table1", (ctx, dims) -> dims)
        .build();

    assertNotNull(processor);
  }
}
