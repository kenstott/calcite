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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ModelConfig} and {@link ModelConfig.SchemaRef}.
 */
@Tag("unit")
class ModelConfigTest {

  @Test void testBuilderBasic() {
    ModelConfig config = ModelConfig.builder()
        .name("test_model")
        .schema("econ", "/econ/schema.yaml")
        .build();
    assertEquals("test_model", config.getName());
    assertEquals(1, config.getSchemas().size());
  }

  @Test void testBuilderMissingNameThrows() {
    try {
      ModelConfig.builder()
          .schema("econ", "/schema.yaml")
          .build();
      assertTrue(false, "Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testBuilderEmptyNameThrows() {
    try {
      ModelConfig.builder()
          .name("")
          .schema("econ", "/schema.yaml")
          .build();
      assertTrue(false, "Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testBuilderNoSchemasThrows() {
    try {
      ModelConfig.builder()
          .name("test")
          .build();
      assertTrue(false, "Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("schema"));
    }
  }

  @Test void testMultipleSchemas() {
    ModelConfig config = ModelConfig.builder()
        .name("govdata")
        .schema("reference", "/ref.yaml")
        .schema("econ", "/econ.yaml", "reference")
        .build();
    assertEquals(2, config.getSchemas().size());
  }

  @Test void testSchemaRefWithoutDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/schema.yaml");
    assertEquals("econ", ref.getName());
    assertEquals("/schema.yaml", ref.getResourcePath());
    assertTrue(ref.getDependsOn().isEmpty());
  }

  @Test void testSchemaRefWithDependencies() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml", "ref1", "ref2");
    assertEquals("econ", ref.getName());
    assertEquals("/econ.yaml", ref.getResourcePath());
    assertEquals(2, ref.getDependsOn().size());
    assertTrue(ref.getDependsOn().contains("ref1"));
    assertTrue(ref.getDependsOn().contains("ref2"));
  }

  @Test void testSchemaRefDependsOnImmutable() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml", "ref1");
    try {
      ref.getDependsOn().add("sneaky");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testSchemaRefToString() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml");
    String str = ref.toString();
    assertTrue(str.contains("econ"));
    assertTrue(str.contains("/econ.yaml"));
  }

  @Test void testSchemaRefToStringWithDeps() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml", "ref1");
    String str = ref.toString();
    assertTrue(str.contains("dependsOn"));
    assertTrue(str.contains("ref1"));
  }

  @Test void testSchemasAreImmutable() {
    ModelConfig config = ModelConfig.builder()
        .name("test")
        .schema("s1", "/s1.yaml")
        .build();
    try {
      config.getSchemas().add(ModelConfig.SchemaRef.of("sneaky", "/sneaky.yaml"));
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBuilderSchemaRef() {
    ModelConfig.SchemaRef ref = ModelConfig.SchemaRef.of("econ", "/econ.yaml");
    ModelConfig config = ModelConfig.builder()
        .name("test")
        .schema(ref)
        .build();
    assertEquals(1, config.getSchemas().size());
    assertEquals("econ", config.getSchemas().get(0).getName());
  }
}
