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
