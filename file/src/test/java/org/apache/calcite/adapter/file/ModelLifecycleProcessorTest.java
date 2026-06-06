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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ModelLifecycleProcessor} builder and validation.
 */
@Tag("unit")
class ModelLifecycleProcessorTest {

  @Test void testBuilderCreation() {
    ModelLifecycleProcessor.Builder builder = ModelLifecycleProcessor.builder();
    assertNotNull(builder);
  }

  @Test void testBuildWithNoSchemasThrows() {
    ModelLifecycleProcessor.Builder builder = ModelLifecycleProcessor.builder();
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test void testBuilderFluentApi() {
    ModelLifecycleProcessor.Builder builder = ModelLifecycleProcessor.builder()
        .operatingDirectory("/tmp/test-operating")
        .sourceStorage(null)
        .materializedStorage(null);
    assertNotNull(builder);
  }

  @Test void testProcessResultGetSchema() {
    // ProcessResult.getSchema returns null for unknown schema names
    org.apache.calcite.schema.SchemaPlus rootSchema =
        org.apache.calcite.tools.Frameworks.createRootSchema(true);
    java.util.Map<String, org.apache.calcite.schema.Schema> schemas = new java.util.HashMap<>();

    // Use reflection to create ProcessResult since constructor is package-private
    ModelLifecycleProcessor.ProcessResult result =
        new ModelLifecycleProcessor.ProcessResult(rootSchema, schemas);

    assertNotNull(result.getRootSchema());
    // Unknown schema returns null
    org.junit.jupiter.api.Assertions.assertNull(result.getSchema("nonexistent"));
  }

  @Test void testProcessResultWithSchemas() {
    org.apache.calcite.schema.SchemaPlus rootSchema =
        org.apache.calcite.tools.Frameworks.createRootSchema(true);
    java.util.Map<String, org.apache.calcite.schema.Schema> schemas = new java.util.HashMap<>();
    org.apache.calcite.schema.Schema mockSchema = new org.apache.calcite.schema.impl.AbstractSchema() {};
    schemas.put("TEST", mockSchema);

    ModelLifecycleProcessor.ProcessResult result =
        new ModelLifecycleProcessor.ProcessResult(rootSchema, schemas);

    assertNotNull(result.getSchema("TEST"));
    org.junit.jupiter.api.Assertions.assertSame(mockSchema, result.getSchema("TEST"));
  }
}
