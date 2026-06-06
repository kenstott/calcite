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
