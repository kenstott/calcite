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
