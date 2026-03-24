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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AbstractRefreshableTable} covering needsRefresh logic,
 * file modification detection, and refresh lifecycle.
 */
@Tag("unit")
class AbstractRefreshableTableTest {

  @TempDir
  File tempDir;

  /** Concrete test implementation of AbstractRefreshableTable. */
  static class TestRefreshableTable extends AbstractRefreshableTable {
    final AtomicInteger doRefreshCount = new AtomicInteger(0);

    TestRefreshableTable(String tableName, Duration refreshInterval) {
      super(tableName, refreshInterval);
    }

    @Override protected void doRefresh() {
      doRefreshCount.incrementAndGet();
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return null;
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override public RefreshBehavior getRefreshBehavior() {
      return RefreshBehavior.SINGLE_FILE;
    }
  }

  @Test void testNeedsRefreshWithNullInterval() {
    TestRefreshableTable table = new TestRefreshableTable("test", null);
    assertFalse(table.needsRefresh());
  }

  @Test void testNeedsRefreshFirstTime() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    assertTrue(table.needsRefresh());
  }

  @Test void testNeedsRefreshAfterRecentRefresh() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofHours(1));
    // Simulate a refresh
    table.refresh();
    // Should not need refresh immediately after
    assertFalse(table.needsRefresh());
  }

  @Test void testGetRefreshInterval() {
    Duration interval = Duration.ofMinutes(10);
    TestRefreshableTable table = new TestRefreshableTable("test", interval);
    assertEquals(interval, table.getRefreshInterval());
  }

  @Test void testGetRefreshIntervalNull() {
    TestRefreshableTable table = new TestRefreshableTable("test", null);
    assertNull(table.getRefreshInterval());
  }

  @Test void testGetLastRefreshTimeInitiallyNull() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    assertNull(table.getLastRefreshTime());
  }

  @Test void testGetLastRefreshTimeAfterRefresh() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    Instant before = Instant.now();
    table.refresh();
    Instant after = Instant.now();

    Instant lastRefresh = table.getLastRefreshTime();
    assertNotNull(lastRefresh);
    assertTrue(!lastRefresh.isBefore(before));
    assertTrue(!lastRefresh.isAfter(after));
  }

  @Test void testRefreshCallsDoRefresh() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMillis(1));
    table.refresh();
    assertEquals(1, table.doRefreshCount.get());
  }

  @Test void testRefreshSkipsWhenIntervalNotElapsed() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofHours(1));
    table.refresh(); // First call
    table.refresh(); // Second call - should skip
    assertEquals(1, table.doRefreshCount.get());
  }

  @Test void testRefreshWithNullIntervalNeverCalls() {
    TestRefreshableTable table = new TestRefreshableTable("test", null);
    table.refresh();
    assertEquals(0, table.doRefreshCount.get());
  }

  @Test void testIsFileModifiedNewFile() throws IOException {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File testFile = new File(tempDir, "test.csv");
    Files.write(testFile.toPath(), "data".getBytes());

    // First check should detect modification (lastModifiedTime starts at 0)
    assertTrue(table.isFileModified(testFile));
  }

  @Test void testIsFileModifiedNonExistentFile() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    File nonExistent = new File(tempDir, "does_not_exist.csv");
    assertFalse(table.isFileModified(nonExistent));
  }

  @Test void testIsFileModifiedAfterUpdate() throws IOException, InterruptedException {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File testFile = new File(tempDir, "test.csv");
    Files.write(testFile.toPath(), "initial data".getBytes());

    // Track the initial modification
    table.updateLastModified(testFile);

    // File hasn't changed since we tracked it
    assertFalse(table.isFileModified(testFile));

    // Now modify the file
    Thread.sleep(10); // Ensure timestamp changes
    Files.write(testFile.toPath(), "modified data".getBytes());
    // Force a future timestamp
    testFile.setLastModified(System.currentTimeMillis() + 1000);

    assertTrue(table.isFileModified(testFile));
  }

  @Test void testUpdateLastModifiedSetsTime() throws IOException {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File testFile = new File(tempDir, "test.csv");
    Files.write(testFile.toPath(), "data".getBytes());

    assertNull(table.getLastRefreshTime());
    table.updateLastModified(testFile);
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testUpdateLastModifiedNonExistentFile() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    File nonExistent = new File(tempDir, "nope.csv");
    // Should not throw - just sets lastRefreshTime
    table.updateLastModified(nonExistent);
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshBehaviorValues() {
    // Test all RefreshBehavior enum values and their descriptions
    for (RefreshableTable.RefreshBehavior behavior : RefreshableTable.RefreshBehavior.values()) {
      assertNotNull(behavior.getDescription());
      assertFalse(behavior.getDescription().isEmpty());
    }
  }

  @Test void testRefreshBehaviorSingleFile() {
    assertEquals("Re-reads file if modified",
        RefreshableTable.RefreshBehavior.SINGLE_FILE.getDescription());
  }

  @Test void testRefreshBehaviorDirectoryScan() {
    assertEquals("Updates existing files only, ignores new/deleted files",
        RefreshableTable.RefreshBehavior.DIRECTORY_SCAN.getDescription());
  }

  @Test void testRefreshBehaviorPartitionedTable() {
    assertEquals("Discovers new partitions and updates existing files",
        RefreshableTable.RefreshBehavior.PARTITIONED_TABLE.getDescription());
  }

  @Test void testRefreshBehaviorMaterializedView() {
    assertEquals("Re-executes query if source tables changed",
        RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW.getDescription());
  }
}
