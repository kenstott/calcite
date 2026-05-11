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
package org.apache.calcite.adapter.file.refresh;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link RefreshableTable.RefreshBehavior}.
 */
@Tag("unit")
class RefreshableTableBehaviorTest {

  @Test void testSingleFileDescription() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.SINGLE_FILE;
    assertNotNull(behavior.getDescription());
    assertEquals("Re-reads file if modified", behavior.getDescription());
  }

  @Test void testDirectoryScanDescription() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.DIRECTORY_SCAN;
    assertNotNull(behavior.getDescription());
    assertEquals("Updates existing files only, ignores new/deleted files",
        behavior.getDescription());
  }

  @Test void testPartitionedTableDescription() {
    RefreshableTable.RefreshBehavior behavior =
        RefreshableTable.RefreshBehavior.PARTITIONED_TABLE;
    assertNotNull(behavior.getDescription());
    assertEquals("Discovers new partitions and updates existing files",
        behavior.getDescription());
  }

  @Test void testMaterializedViewDescription() {
    RefreshableTable.RefreshBehavior behavior =
        RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW;
    assertNotNull(behavior.getDescription());
    assertEquals("Re-executes query if source tables changed",
        behavior.getDescription());
  }

  @Test void testEnumValues() {
    RefreshableTable.RefreshBehavior[] values = RefreshableTable.RefreshBehavior.values();
    assertEquals(4, values.length);
  }

  @Test void testEnumValueOf() {
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE,
        RefreshableTable.RefreshBehavior.valueOf("SINGLE_FILE"));
    assertEquals(RefreshableTable.RefreshBehavior.DIRECTORY_SCAN,
        RefreshableTable.RefreshBehavior.valueOf("DIRECTORY_SCAN"));
    assertEquals(RefreshableTable.RefreshBehavior.PARTITIONED_TABLE,
        RefreshableTable.RefreshBehavior.valueOf("PARTITIONED_TABLE"));
    assertEquals(RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW,
        RefreshableTable.RefreshBehavior.valueOf("MATERIALIZED_VIEW"));
  }
}
