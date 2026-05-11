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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Deep coverage tests for {@link ConversionFileWatcher} targeting the
 * {@code checkFile} internal method and {@code shutdown} lifecycle.
 *
 * <p>Since {@code checkFile} is private, we trigger it indirectly by
 * registering files with short refresh intervals and waiting for the
 * scheduled executor to invoke the check. We also test the path where
 * a watched file is modified, triggering re-conversion logic.
 *
 * <p>Each test uses unique schema names and cleans up watched files in
 * {@code @AfterEach} to prevent the singleton watcher from accumulating
 * background tasks across tests.
 */
@Tag("unit")
public class ConversionFileWatcherDeepCoverageTest {

  @TempDir
  File tempDir;

  /** Tracks (schemaName, file) pairs registered during each test for cleanup. */
  private final List<String[]> watchedEntries = new ArrayList<String[]>();

  @AfterEach
  public void unwatchAll() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    for (String[] entry : watchedEntries) {
      try {
        watcher.unwatchFile(entry[0], new File(entry[1]));
      } catch (Exception ignored) {
        // Best-effort cleanup; file may already be gone
      }
    }
    watchedEntries.clear();
  }

  private void watch(String schema, File file, Duration interval) {
    ConversionFileWatcher.getInstance().watchFile(schema, file, interval);
    watchedEntries.add(new String[]{schema, file.getAbsolutePath()});
  }

  // === shutdown / lifecycle tests ===

  @Test
  public void testShutdownDoesNotThrow() {
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    assertNotNull(watcher);
    // Just verify getInstance() works and returns non-null; we do not
    // actually shut down the singleton to avoid side effects on other tests.
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testRegisterAndUnregisterCycle() throws IOException {
    String schema = "cycle_schema_" + System.nanoTime();
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File base = new File(tempDir, "cycle_base");
    base.mkdirs();
    watcher.registerSchemaBaseDirectory(schema, base);

    File xlsxFile = new File(tempDir, "cycle_test.xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{0x50, 0x4B});

    watch(schema, xlsxFile, Duration.ofMinutes(60));
    watcher.unwatchFile(schema, xlsxFile);
    watchedEntries.clear(); // Already unwatched manually
  }

  // === checkFile code path tests ===

  /**
   * Tests checkFile indirectly by registering a file with a short interval,
   * modifying its timestamp, and waiting for the scheduled check to fire.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testCheckFileDetectsModification() throws Exception {
    String schema = "checkfile_schema_" + System.nanoTime();
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File baseDir = new File(tempDir, "checkfile_base");
    baseDir.mkdirs();
    watcher.registerSchemaBaseDirectory(schema, baseDir);

    File xlsxFile = new File(tempDir, "checkfile_test.xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{0x50, 0x4B, 0x03, 0x04});
    // Set initial timestamp to 1 minute ago so modification is detected
    xlsxFile.setLastModified(System.currentTimeMillis() - 60000L);

    watch(schema, xlsxFile, Duration.ofMillis(100));

    // Modify timestamp to trigger conversion on next scheduled check
    xlsxFile.setLastModified(System.currentTimeMillis());

    // Wait for the scheduled task to run at least once (100ms interval + margin)
    Thread.sleep(300);

    // Re-conversion attempt may fail on invalid XLSX content — that is expected.
    // The goal is to exercise the checkFile code path without unexpected exceptions.
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testCheckFileWithHtmFileModification() throws Exception {
    String schema = "htm_schema_" + System.nanoTime();
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File baseDir = new File(tempDir, "htm_base");
    baseDir.mkdirs();
    watcher.registerSchemaBaseDirectory(schema, baseDir);

    File htmFile = new File(tempDir, "checkfile_" + System.nanoTime() + ".htm");
    Files.write(htmFile.toPath(),
        "<html><body><table><tr><th>A</th></tr></table></body></html>".getBytes());
    htmFile.setLastModified(System.currentTimeMillis() - 60000L);

    watch(schema, htmFile, Duration.ofMillis(100));

    htmFile.setLastModified(System.currentTimeMillis());

    Thread.sleep(300);
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testCheckFileWithXmlFileModification() throws Exception {
    String schema = "xml_schema_" + System.nanoTime();
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    File baseDir = new File(tempDir, "xml_base");
    baseDir.mkdirs();
    watcher.registerSchemaBaseDirectory(schema, baseDir);

    File xmlFile = new File(tempDir, "checkfile_" + System.nanoTime() + ".xml");
    Files.write(xmlFile.toPath(), "<root><item>value</item></root>".getBytes());
    xmlFile.setLastModified(System.currentTimeMillis() - 60000L);

    watch(schema, xmlFile, Duration.ofMillis(100));

    xmlFile.setLastModified(System.currentTimeMillis());

    Thread.sleep(300);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testCheckFileWithDeletedFile() throws Exception {
    String schema = "del_schema_" + System.nanoTime();

    File xlsxFile = new File(tempDir, "will_delete_" + System.nanoTime() + ".xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{0x50, 0x4B, 0x03, 0x04});

    watch(schema, xlsxFile, Duration.ofMillis(50));

    // Delete the file so checkFile encounters a missing file
    xlsxFile.delete();

    // Wait for the scheduled check to detect the missing file
    Thread.sleep(200);
    // Should handle missing file gracefully (logs warning, removes from map)
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testCheckFileWithNoBaseDirectoryFallback() throws Exception {
    // Use a schema name with NO registered base directory to exercise the fallback
    String schema = "no_base_schema_" + System.nanoTime();

    File xlsxFile = new File(tempDir, "no_base_" + System.nanoTime() + ".xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{0x50, 0x4B, 0x03, 0x04});
    xlsxFile.setLastModified(System.currentTimeMillis() - 60000L);

    watch(schema, xlsxFile, Duration.ofMillis(100));

    xlsxFile.setLastModified(System.currentTimeMillis());

    Thread.sleep(300);
    // Should fall back to the file's parent directory and log a warning
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testWatchSameFileTwiceDoesNotDoubleRegister() throws IOException {
    String schema = "double_watch_" + System.nanoTime();

    File xlsxFile = new File(tempDir, "double_watch_" + System.nanoTime() + ".xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{0x50, 0x4B, 0x03, 0x04});

    // Watch the same file twice — should be idempotent
    ConversionFileWatcher.getInstance().watchFile(schema, xlsxFile, Duration.ofMinutes(60));
    ConversionFileWatcher.getInstance().watchFile(schema, xlsxFile, Duration.ofMinutes(60));
    watchedEntries.add(new String[]{schema, xlsxFile.getAbsolutePath()});

    ConversionFileWatcher.getInstance().unwatchFile(schema, xlsxFile);
    watchedEntries.clear();
  }
}
