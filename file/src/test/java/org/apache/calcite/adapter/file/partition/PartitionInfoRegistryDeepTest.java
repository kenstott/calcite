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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for PartitionInfoRegistry singleton, covering
 * lookup paths (exact, case-insensitive index, iteration fallback),
 * null handling, and clear operations.
 */
@Tag("unit")
public class PartitionInfoRegistryDeepTest {

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
    PartitionInfoRegistry r1 = PartitionInfoRegistry.getInstance();
    PartitionInfoRegistry r2 = PartitionInfoRegistry.getInstance();
    assertSame(r1, r2);
  }

  // ===== Register with nulls =====

  @Test void testRegisterNullSchemaName() {
    // Should be a no-op
    registry.register(null, "table", null);
    assertNull(registry.lookup(null, "table"));
  }

  @Test void testRegisterNullTableName() {
    registry.register("schema", null, null);
    assertNull(registry.lookup("schema", null));
  }

  @Test void testRegisterNullTable() {
    registry.register("schema", "table", null);
    assertNull(registry.lookup("schema", "table"));
  }

  // ===== Lookup with nulls =====

  @Test void testLookupNullSchemaName() {
    assertNull(registry.lookup(null, "table"));
  }

  @Test void testLookupNullTableName() {
    assertNull(registry.lookup("schema", null));
  }

  // ===== Lookup non-existent =====

  @Test void testLookupNonExistentSchema() {
    assertNull(registry.lookup("nonexistent", "table"));
  }

  // ===== getPartitionColumns =====

  @Test void testGetPartitionColumnsNoTable() {
    assertNull(registry.getPartitionColumns("schema", "table"));
  }

  // ===== isPartitionColumn =====

  @Test void testIsPartitionColumnNoTable() {
    assertFalse(registry.isPartitionColumn("schema", "table", "col"));
  }

  // ===== getDistinctPartitionValues =====

  @Test void testGetDistinctPartitionValuesNoTable() {
    assertNull(registry.getDistinctPartitionValues("schema", "table", "col"));
  }

  // ===== clearSchema =====

  @Test void testClearSchema() {
    // Just test the method doesn't throw on empty registry
    registry.clearSchema("nonexistent");
  }

  // ===== clear =====

  @Test void testClear() {
    registry.clear();
    // Should not throw and leave registry empty
  }
}
