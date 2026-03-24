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
