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

import org.apache.calcite.util.Source;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link RefreshableCsvTable}.
 * Tests refresh logic, staleness detection, and protocol handling.
 */
@Tag("unit")
public class RefreshableCsvTableCoverageTest {

  private Source createFileSource(File file) {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("file");
    when(source.file()).thenReturn(file);
    when(source.path()).thenReturn(file.getAbsolutePath());
    return source;
  }

  private Source createHttpSource() {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("https");
    when(source.path()).thenReturn("https://example.com/data.csv");
    return source;
  }

  // =========================================================================
  // Constructor and basic properties
  // =========================================================================

  @Test public void testConstructorWithRefreshInterval(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMinutes(5));
    assertNotNull(table);
    assertEquals(Duration.ofMinutes(5), table.getRefreshInterval());
    assertEquals("RefreshableCsvTable(test_table)", table.toString());
  }

  @Test public void testConstructorWithNullRefreshInterval(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(source, "test_table", null, null);
    assertNull(table.getRefreshInterval());
  }

  @Test public void testGetRefreshBehavior(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMinutes(5));
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  // =========================================================================
  // needsRefresh
  // =========================================================================

  @Test public void testNeedsRefreshNoInterval(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(source, "test_table", null, null);
    assertFalse(table.needsRefresh());
  }

  @Test public void testNeedsRefreshFirstTime(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMinutes(5));
    assertTrue(table.needsRefresh());
  }

  @Test public void testNeedsRefreshAfterRefresh(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofHours(1));
    assertTrue(table.needsRefresh());

    // Call refresh
    table.refresh();

    // Should not need refresh again (1 hour interval)
    assertFalse(table.needsRefresh());
  }

  @Test public void testNeedsRefreshAfterIntervalElapsed(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));

    // First refresh
    table.refresh();

    // Wait for interval to elapse
    Thread.sleep(10);

    assertTrue(table.needsRefresh());
  }

  // =========================================================================
  // refresh - file protocol
  // =========================================================================

  @Test public void testRefreshLocalFileModified(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));

    // First refresh
    table.refresh();
    assertNotNull(table.getLastRefreshTime());

    // Modify file
    Thread.sleep(10);
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n2,test2\n");
    }

    // Wait for interval
    Thread.sleep(10);

    // Second refresh should detect modification
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test public void testRefreshLocalFileNotExists(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("nonexistent.csv").toFile();
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));
    table.refresh();
    // Should not throw, just not mark as stale
    assertNotNull(table.getLastRefreshTime());
  }

  @Test public void testRefreshLocalFileNullFile(@TempDir Path tempDir) throws Exception {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("file");
    when(source.file()).thenReturn(null);
    when(source.path()).thenReturn("test.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // =========================================================================
  // refresh - remote protocols (http, https, s3, ftp)
  // =========================================================================

  @Test public void testRefreshRemoteFileProtocol(@TempDir Path tempDir) throws Exception {
    Source source = createHttpSource();

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));

    // Remote refresh will fail because RemoteFileMetadata.fetch will throw IOException
    // (no actual HTTP server), but the catch block should handle it gracefully
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test public void testRefreshS3Protocol(@TempDir Path tempDir) throws Exception {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("s3");
    when(source.path()).thenReturn("s3://bucket/data.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test public void testRefreshFtpProtocol(@TempDir Path tempDir) throws Exception {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("ftp");
    when(source.path()).thenReturn("ftp://example.com/data.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // =========================================================================
  // refresh skipped when not needed
  // =========================================================================

  @Test public void testRefreshSkippedWhenNotNeeded(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofHours(1));
    table.refresh();
    Instant firstRefresh = table.getLastRefreshTime();

    // Immediate second refresh should be skipped
    table.refresh();
    Instant secondRefresh = table.getLastRefreshTime();

    assertEquals(firstRefresh, secondRefresh, "Refresh should be skipped");
  }

  // =========================================================================
  // clearCachedRowType via reflection
  // =========================================================================

  @Test public void testClearCachedRowType(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));

    // Access private clearCachedRowType via reflection
    java.lang.reflect.Method method =
        RefreshableCsvTable.class.getDeclaredMethod("clearCachedRowType");
    method.setAccessible(true);
    // Should not throw even if fields are already null
    method.invoke(table);
  }

  // =========================================================================
  // dataStale flag via reflection
  // =========================================================================

  @Test public void testDataStaleFlag(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMillis(1));

    Field dataStaleField = RefreshableCsvTable.class.getDeclaredField("dataStale");
    dataStaleField.setAccessible(true);
    assertFalse((Boolean) dataStaleField.get(table));

    // After first refresh of a file, dataStale should become true
    table.refresh();

    // Check if dataStale was set (for local file that exists)
    // On first refresh of existing file with modified time > 0, it should be true
    boolean stale = (Boolean) dataStaleField.get(table);
    assertTrue(stale, "dataStale should be true after first refresh of existing file");
  }

  // =========================================================================
  // toString
  // =========================================================================

  @Test public void testToString(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "my_csv_table", null, Duration.ofMinutes(5));
    assertEquals("RefreshableCsvTable(my_csv_table)", table.toString());
  }

  // =========================================================================
  // getLastRefreshTime
  // =========================================================================

  @Test public void testGetLastRefreshTimeInitiallyNull(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMinutes(5));
    assertNull(table.getLastRefreshTime());
  }

  @Test public void testGetLastRefreshTimeAfterRefresh(@TempDir Path tempDir) throws Exception {
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,test\n");
    }
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "test_table", null, Duration.ofMinutes(5));
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }
}
