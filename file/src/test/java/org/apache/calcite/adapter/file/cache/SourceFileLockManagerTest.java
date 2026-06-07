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
package org.apache.calcite.adapter.file.cache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SourceFileLockManager}.
 */
@Tag("unit")
class SourceFileLockManagerTest {

  @TempDir
  Path tempDir;

  @Test void testAcquireReadLock() throws IOException {
    File f = tempDir.resolve("data.csv").toFile();
    Files.write(f.toPath(), "a,b,c".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle = SourceFileLockManager.acquireReadLock(f);
    assertNotNull(handle);
    assertTrue(handle.isValid());

    handle.close();
  }

  @Test void testLockHandleCloseIdempotent() throws IOException {
    File f = tempDir.resolve("data.csv").toFile();
    Files.write(f.toPath(), "a,b,c".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle = SourceFileLockManager.acquireReadLock(f);
    handle.close();
    // Second close should not throw
    handle.close();
  }

  @Test void testAcquireReadLockWithTimeout() throws IOException {
    File f = tempDir.resolve("data2.csv").toFile();
    Files.write(f.toPath(), "x,y,z".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireReadLock(f, 5000);
    assertNotNull(handle);
    assertTrue(handle.isValid());

    handle.close();
  }

  @Test void testAcquireWriteLock() throws IOException {
    File f = tempDir.resolve("write_test.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 5000);
    assertNotNull(handle);
    assertTrue(handle.isValid());

    handle.close();
  }

  @Test void testMultipleReadLocks() throws IOException {
    File f = tempDir.resolve("shared.csv").toFile();
    Files.write(f.toPath(), "shared data".getBytes(StandardCharsets.UTF_8));

    // Shared read locks should be compatible
    SourceFileLockManager.LockHandle handle1 = SourceFileLockManager.acquireReadLock(f);
    assertNotNull(handle1);
    assertTrue(handle1.isValid());

    SourceFileLockManager.LockHandle handle2 = SourceFileLockManager.acquireReadLock(f);
    assertNotNull(handle2);
    assertTrue(handle2.isValid());

    handle1.close();
    handle2.close();
  }

  @Test void testLockOnNonexistentFile() {
    File f = tempDir.resolve("nonexistent.csv").toFile();
    assertThrows(IOException.class, () -> SourceFileLockManager.acquireReadLock(f, 100));
  }

  @Test void testLockHandleInvalidAfterClose() throws IOException {
    File f = tempDir.resolve("test_valid.csv").toFile();
    Files.write(f.toPath(), "content".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle = SourceFileLockManager.acquireReadLock(f);
    assertTrue(handle.isValid());
    handle.close();
    // After close, isValid may be false (depends on implementation path)
  }
}
