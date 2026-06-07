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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link PartitionInfoRegistry} singleton behavior and null handling.
 */
@Tag("unit")
public class PartitionInfoRegistryTest {

  private PartitionInfoRegistry registry;

  @BeforeEach
  void setUp() {
    registry = PartitionInfoRegistry.getInstance();
    registry.clear();
  }

  @AfterEach
  void tearDown() {
    registry.clear();
  }

  @Test void testSingletonInstance() {
    PartitionInfoRegistry instance1 = PartitionInfoRegistry.getInstance();
    PartitionInfoRegistry instance2 = PartitionInfoRegistry.getInstance();

    assertSame(instance1, instance2);
  }

  @Test void testRegisterNullParametersIgnored() {
    // These should not throw
    registry.register(null, "table", null);
    registry.register("schema", null, null);
    registry.register("schema", "table", null);
  }

  @Test void testLookupNullReturnsNull() {
    assertNull(registry.lookup(null, "table"));
    assertNull(registry.lookup("schema", null));
    assertNull(registry.lookup(null, null));
  }

  @Test void testLookupNonexistentSchemaReturnsNull() {
    assertNull(registry.lookup("nonexistent", "table"));
  }

  @Test void testGetPartitionColumnsNullReturnsNull() {
    assertNull(registry.getPartitionColumns(null, "table"));
    assertNull(registry.getPartitionColumns("schema", null));
    assertNull(registry.getPartitionColumns("schema", "nonexistent"));
  }

  @Test void testIsPartitionColumnWithNoRegistration() {
    assertFalse(registry.isPartitionColumn("schema", "table", "col"));
  }

  @Test void testGetDistinctPartitionValuesNullReturnsNull() {
    assertNull(registry.getDistinctPartitionValues("schema", "table", "col"));
  }

  @Test void testClear() {
    // Clear should not throw even on empty registry
    registry.clear();
  }

  @Test void testClearSchema() {
    // Should not throw even for nonexistent schema
    registry.clearSchema("nonexistent");
  }

  private static void assertFalse(boolean value) {
    org.junit.jupiter.api.Assertions.assertFalse(value);
  }
}
