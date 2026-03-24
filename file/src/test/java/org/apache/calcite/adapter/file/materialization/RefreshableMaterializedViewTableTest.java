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
package org.apache.calcite.adapter.file.materialization;

import org.apache.calcite.adapter.file.materialized.MaterializedViewUtil;
import org.apache.calcite.adapter.file.materialized.RefreshableMaterializedViewTable;
import org.apache.calcite.adapter.file.refresh.RefreshableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RefreshableMaterializedViewTable}.
 * Covers refresh intervals, needsRefresh logic, and refresh behavior.
 */
@Tag("unit")
public class RefreshableMaterializedViewTableTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RefreshableMaterializedViewTableTest.class);

  @TempDir
  Path tempDir;

  private RefreshableMaterializedViewTable createView(Duration refreshInterval) {
    File parquetFile = tempDir.resolve("test_view.parquet").toFile();
    Map<String, Table> tables = new HashMap<>();

    return new RefreshableMaterializedViewTable(
        null, // parentSchema
        "test_schema",
        "test_view",
        "SELECT 1",
        parquetFile,
        tables,
        refreshInterval);
  }

  @Test
  public void testGetRefreshInterval() {
    Duration interval = Duration.ofMinutes(30);
    RefreshableMaterializedViewTable view = createView(interval);
    assertEquals(interval, view.getRefreshInterval());
  }

  @Test
  public void testGetRefreshIntervalNull() {
    RefreshableMaterializedViewTable view = createView(null);
    assertNull(view.getRefreshInterval());
  }

  @Test
  public void testNeedsRefreshFirstTime() {
    RefreshableMaterializedViewTable view = createView(Duration.ofMinutes(30));
    // First time - lastRefreshTime is null, should always need refresh
    assertTrue(view.needsRefresh());
  }

  @Test
  public void testNeedsRefreshNullInterval() {
    RefreshableMaterializedViewTable view = createView(null);
    // No refresh interval set - never needs refresh
    assertFalse(view.needsRefresh());
  }

  @Test
  public void testRefreshSetsLastRefreshTime() {
    RefreshableMaterializedViewTable view = createView(Duration.ofMinutes(30));

    assertNull(view.getLastRefreshTime());

    view.refresh();

    assertNotNull(view.getLastRefreshTime());
    assertTrue(view.getLastRefreshTime().isBefore(Instant.now().plusSeconds(1)));
  }

  @Test
  public void testRefreshWhenNotNeeded() {
    RefreshableMaterializedViewTable view = createView(null);
    // No refresh interval - refresh should be a no-op
    view.refresh();
    assertNull(view.getLastRefreshTime());
  }

  @Test
  public void testNeedsRefreshAfterRecent() {
    RefreshableMaterializedViewTable view = createView(Duration.ofHours(1));

    // First refresh
    view.refresh();

    // Should not need refresh immediately after
    assertFalse(view.needsRefresh());
  }

  @Test
  public void testGetRefreshBehavior() {
    RefreshableMaterializedViewTable view = createView(Duration.ofMinutes(30));
    assertEquals(RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW,
        view.getRefreshBehavior());
  }

  @Test
  public void testToString() {
    RefreshableMaterializedViewTable view = createView(Duration.ofMinutes(30));
    String str = view.toString();
    assertNotNull(str);
    assertTrue(str.contains("test_view"));
    assertTrue(str.contains("RefreshableMaterializedViewTable"));
  }

  // --- MaterializedViewUtil tests ---

  @Test
  public void testGetFileExtensionParquet() {
    assertEquals("parquet", MaterializedViewUtil.getFileExtension("PARQUET"));
    assertEquals("parquet", MaterializedViewUtil.getFileExtension("parquet"));
  }

  @Test
  public void testGetFileExtensionArrow() {
    assertEquals("arrow", MaterializedViewUtil.getFileExtension("ARROW"));
    assertEquals("arrow", MaterializedViewUtil.getFileExtension("arrow"));
  }

  @Test
  public void testGetFileExtensionVectorized() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("VECTORIZED"));
  }

  @Test
  public void testGetFileExtensionLinq4j() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("LINQ4J"));
  }

  @Test
  public void testGetFileExtensionDefault() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("UNKNOWN"));
  }

  @Test
  public void testGetFileExtensionNull() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension(null));
  }

  @Test
  public void testIsMaterializedViewFile() {
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("data.parquet", "PARQUET"));
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("data.csv", "LINQ4J"));
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("data.arrow", "ARROW"));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("data.csv", "PARQUET"));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("data.json", "LINQ4J"));
  }
}
