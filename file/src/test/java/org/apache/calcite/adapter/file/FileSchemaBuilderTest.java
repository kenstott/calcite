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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileSchemaBuilder}.
 */
@Tag("unit")
class FileSchemaBuilderTest {

  @TempDir
  File tempDir;

  @Test void testCreateReturnsBuilder() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertNotNull(builder);
  }

  @Test void testBuilderChaining() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("baseDirectory", tempDir.getAbsolutePath());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .operand(operand);
    assertNotNull(builder);
  }

  @Test void testGetOperandRequiresConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, () -> builder.getOperand());
  }

  @Test void testGetOperandWithConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("tables", new HashMap<String, Object>());
    config.put("directory", tempDir.getAbsolutePath());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("myKey", "myValue");
    Map<String, Object> result = builder.getOperand();
    assertNotNull(result);
    assertTrue(result.containsKey("myKey"));
  }

  @Test void testSchemaConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("tables", new HashMap<String, Object>());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);
    assertNotNull(builder);
  }

  @Test void testSchemaResourceNotFound() {
    assertThrows(RuntimeException.class, () -> {
      FileSchemaBuilder.create()
          .schemaResource("/nonexistent/schema.yaml");
    });
  }

  @Test void testOperandWithNullMapThrows() {
    assertThrows(NullPointerException.class, () -> {
      FileSchemaBuilder.create()
          .operand((Map<String, Object>) null);
    });
  }

  @Test void testSchemaConfigWithEnvVarResolution() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("tables", new HashMap<String, Object>());
    config.put("name", "test_schema");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);
    assertNotNull(builder);
  }

  @Test void testMultipleOperandOverrides() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("tables", new HashMap<String, Object>());
    config.put("directory", tempDir.getAbsolutePath());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("key1", "val1")
        .operand("key2", "val2");
    Map<String, Object> result = builder.getOperand();
    assertTrue(result.containsKey("key1"));
    assertTrue(result.containsKey("key2"));
  }
}
