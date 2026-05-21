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
package org.apache.calcite.adapter.file.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link ConcurrentSpilloverManager} targeting
 * the {@code cleanupOldDirectories} method and its lambda branches.
 */
@Tag("unit")
public class ConcurrentSpilloverManagerDeepCoverageTest {

  private final List<String> connectionIds =
      Collections.synchronizedList(new ArrayList<String>());

  @AfterEach
  public void cleanup() {
    for (String id : connectionIds) {
      ConcurrentSpilloverManager.cleanupConnectionDirectory(id);
    }
    connectionIds.clear();
  }

  private String uniqueId() {
    String id = "deep_test_" + UUID.randomUUID().toString();
    connectionIds.add(id);
    return id;
  }

  /**
   * Tests that {@code cleanupOldDirectories} deletes directories whose last-modified
   * timestamp is in the past. Forces the directory's mtime to be very old, then
   * calls {@code cleanupOldDirectories(0)} so every directory qualifies.
   */
  @Test public void testCleanupOldDirectoriesDeletesOldDirs() throws IOException {
    String id = uniqueId();

    // Create a spillover directory
    Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(id);
    assertNotNull(dir);
    assertTrue(Files.exists(dir));

    // Write a file inside so the deletion lambda is exercised
    Path file = dir.resolve("testfile.tmp");
    Files.write(file, "data".getBytes());
    assertTrue(Files.exists(file));

    // Set the directory's last modified time to be very old (epoch 0)
    Files.setLastModifiedTime(dir, FileTime.fromMillis(0L));

    // Remove from cleanup list since we're testing cleanup explicitly
    connectionIds.remove(id);

    // cleanupOldDirectories(0) sets cutoff = now - 0ms = now.
    // Since dir's mtime = 0 (epoch), it is older than now, so it should be deleted.
    ConcurrentSpilloverManager.cleanupOldDirectories(0);

    // The directory should have been cleaned up
    assertFalse(Files.exists(dir),
        "Old spillover directory should be removed by cleanupOldDirectories");
  }

  /**
   * Tests that {@code cleanupOldDirectories} leaves recent directories intact.
   */
  @Test public void testCleanupOldDirectoriesKeepsRecentDirs() throws IOException {
    String id = uniqueId();

    // Create a spillover directory (recently created = recent mtime)
    Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(id);
    assertNotNull(dir);
    assertTrue(Files.exists(dir));

    // Call cleanupOldDirectories with a very large value (1 year = 8760 hours)
    // so nothing recent is deleted
    ConcurrentSpilloverManager.cleanupOldDirectories(8760);

    // Directory should still exist
    assertTrue(Files.exists(dir),
        "Recent spillover directory should NOT be removed");

    ConcurrentSpilloverManager.cleanupConnectionDirectory(id);
    connectionIds.remove(id);
  }

  /**
   * Tests that {@code cleanupOldDirectories} handles non-existent base directory
   * gracefully (no exception).
   */
  @Test public void testCleanupOldDirectoriesNonExistentBaseDir() {
    // Just call it; if no spillover dirs created yet, the base may or may not exist.
    // Either way, should not throw.
    ConcurrentSpilloverManager.cleanupOldDirectories(24);
  }

  /**
   * Tests {@code cleanupConnectionDirectory} path when directory has nested files.
   */
  @Test public void testCleanupConnectionDirectoryWithFiles() throws IOException {
    String id = uniqueId();
    Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(id);

    // Create nested file to exercise the deletion lambda
    Path nested = dir.resolve("nested_file.tmp");
    Files.write(nested, "content".getBytes());
    assertTrue(Files.exists(nested));

    // Remove from list to call manually
    connectionIds.remove(id);
    ConcurrentSpilloverManager.cleanupConnectionDirectory(id);

    assertFalse(Files.exists(dir),
        "Directory with files should be fully cleaned up");
  }

  /**
   * Tests that cleanup of an already-cleaned directory is idempotent.
   */
  @Test public void testCleanupConnectionDirectoryIdempotent() {
    String id = uniqueId();
    connectionIds.remove(id);

    // Clean up without ever creating a directory - should not throw
    ConcurrentSpilloverManager.cleanupConnectionDirectory(id);
    ConcurrentSpilloverManager.cleanupConnectionDirectory(id);
  }

  /**
   * Tests createSpilloverFile is in a directory with the connection ID in its name.
   */
  @Test public void testCreateSpilloverFileInConnectionDir() throws IOException {
    String id = uniqueId();

    java.io.File file = ConcurrentSpilloverManager.createSpilloverFile(id, "test_prefix");
    assertNotNull(file);
    assertTrue(file.getName().startsWith("test_prefix_"));
    assertTrue(file.getName().endsWith(".tmp"));

    // Parent directory should contain the connection ID
    String parentName = file.getParentFile().getName();
    assertTrue(parentName.contains(id),
        "Parent dir name '" + parentName + "' should contain connection id");
  }
}
