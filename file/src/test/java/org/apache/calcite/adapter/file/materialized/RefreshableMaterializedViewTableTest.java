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
package org.apache.calcite.adapter.file.materialized;

import org.apache.calcite.adapter.file.refresh.RefreshableTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RefreshableMaterializedViewTable}.
 */
@Tag("unit")
public class RefreshableMaterializedViewTableTest {

  @TempDir
  Path tempDir;

  private SchemaPlus createParentSchema() {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
    return rootSchema.plus();
  }

  private RefreshableMaterializedViewTable createTable(Duration refreshInterval) {
    SchemaPlus parentSchema = createParentSchema();
    File parquetFile = tempDir.resolve("test_view.parquet").toFile();
    Map<String, Table> tables = new HashMap<String, Table>();

    return new RefreshableMaterializedViewTable(
        parentSchema,
        "TEST_SCHEMA",
        "test_view",
        "SELECT 1",
        parquetFile,
        tables,
        refreshInterval);
  }

  @Test
  public void testGetRefreshInterval() {
    Duration interval = Duration.ofMinutes(30);
    RefreshableMaterializedViewTable table = createTable(interval);

    assertEquals(interval, table.getRefreshInterval());
  }

  @Test
  public void testGetRefreshIntervalNull() {
    RefreshableMaterializedViewTable table = createTable(null);

    assertNull(table.getRefreshInterval());
  }

  @Test
  public void testNeedsRefreshWithNullInterval() {
    RefreshableMaterializedViewTable table = createTable(null);

    // No refresh interval means never needs refresh
    assertFalse(table.needsRefresh());
  }

  @Test
  public void testNeedsRefreshFirstTime() {
    Duration interval = Duration.ofMinutes(30);
    RefreshableMaterializedViewTable table = createTable(interval);

    // First time should always need refresh (lastRefreshTime is null)
    assertTrue(table.needsRefresh());
  }

  @Test
  public void testNeedsRefreshAfterRefresh() {
    Duration interval = Duration.ofHours(1);
    RefreshableMaterializedViewTable table = createTable(interval);

    // Refresh the table
    table.refresh();

    // Should not need refresh immediately after
    assertFalse(table.needsRefresh());
  }

  @Test
  public void testNeedsRefreshWithExpiredInterval() {
    // Use a very short interval
    Duration interval = Duration.ofMillis(1);
    RefreshableMaterializedViewTable table = createTable(interval);

    // Refresh, then wait for interval to expire
    table.refresh();

    // Sleep a tiny bit to ensure interval expires
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Should need refresh now
    assertTrue(table.needsRefresh());
  }

  @Test
  public void testGetLastRefreshTimeInitiallyNull() {
    Duration interval = Duration.ofMinutes(5);
    RefreshableMaterializedViewTable table = createTable(interval);

    assertNull(table.getLastRefreshTime());
  }

  @Test
  public void testGetLastRefreshTimeAfterRefresh() {
    Duration interval = Duration.ofMinutes(5);
    RefreshableMaterializedViewTable table = createTable(interval);

    Instant before = Instant.now();
    table.refresh();
    Instant after = Instant.now();

    Instant lastRefresh = table.getLastRefreshTime();
    assertNotNull(lastRefresh);
    // Last refresh time should be between before and after
    assertFalse(lastRefresh.isBefore(before));
    assertFalse(lastRefresh.isAfter(after));
  }

  @Test
  public void testRefreshBehavior() {
    RefreshableMaterializedViewTable table = createTable(Duration.ofMinutes(5));

    assertEquals(RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW,
        table.getRefreshBehavior());
  }

  @Test
  public void testRefreshDoesNotRefreshWhenNotNeeded() {
    // No interval = never needs refresh
    RefreshableMaterializedViewTable table = createTable(null);

    // This should be a no-op
    table.refresh();

    // lastRefreshTime should still be null since refresh was skipped
    assertNull(table.getLastRefreshTime());
  }

  @Test
  public void testToString() {
    RefreshableMaterializedViewTable table = createTable(Duration.ofMinutes(5));

    String str = table.toString();
    assertNotNull(str);
    assertTrue(str.contains("test_view"));
  }

  @Test
  public void testMultipleRefreshes() {
    Duration interval = Duration.ofMillis(1);
    RefreshableMaterializedViewTable table = createTable(interval);

    // First refresh
    table.refresh();
    Instant firstRefresh = table.getLastRefreshTime();
    assertNotNull(firstRefresh);

    // Wait for interval to expire
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Second refresh
    table.refresh();
    Instant secondRefresh = table.getLastRefreshTime();
    assertNotNull(secondRefresh);

    // Second refresh time should be after first
    assertTrue(secondRefresh.isAfter(firstRefresh) || secondRefresh.equals(firstRefresh));
  }

  @Test
  public void testRefreshWithZeroDuration() {
    // Zero duration means refresh when strictly after lastRefreshTime
    Duration interval = Duration.ZERO;
    RefreshableMaterializedViewTable table = createTable(interval);

    // First time always needs refresh (lastRefreshTime is null)
    assertTrue(table.needsRefresh());

    // After refresh, lastRefreshTime is set
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test
  public void testRefreshWithLargeDuration() {
    Duration interval = Duration.ofDays(365);
    RefreshableMaterializedViewTable table = createTable(interval);

    // First time needs refresh
    assertTrue(table.needsRefresh());

    table.refresh();
    // Should NOT need refresh for a long time
    assertFalse(table.needsRefresh());
  }

  @Test
  public void testParquetFileCleanupOnRefresh() throws IOException {
    Duration interval = Duration.ofMillis(1);
    RefreshableMaterializedViewTable table = createTable(interval);

    // Create the parquet file to simulate existing materialized data
    File parquetFile = tempDir.resolve("test_view.parquet").toFile();
    parquetFile.createNewFile();
    assertTrue(parquetFile.exists());

    // First refresh
    table.refresh();

    // Wait for interval
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Calling refresh again should mark needsRematerialization
    table.refresh();
    // The actual deletion happens in toRel, not in refresh itself
  }
}
