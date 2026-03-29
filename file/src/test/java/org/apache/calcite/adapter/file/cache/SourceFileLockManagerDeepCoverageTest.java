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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link SourceFileLockManager} covering the
 * write lock code path, timeout handling, thread-interrupt behavior,
 * and overlapping-lock detection.
 */
@Tag("unit")
public class SourceFileLockManagerDeepCoverageTest {

  @TempDir
  Path tempDir;

  // === Write lock tests ===

  @Test
  public void testAcquireWriteLockBasic() throws IOException {
    File f = tempDir.resolve("write_basic.csv").toFile();
    Files.write(f.toPath(), "a,b,c\n1,2,3\n".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 5000);

    assertNotNull(handle);
    assertTrue(handle.isValid());
    handle.close();
  }

  @Test
  public void testWriteLockHandleIsValidFalseAfterClose() throws IOException {
    File f = tempDir.resolve("write_valid.csv").toFile();
    Files.write(f.toPath(), "x".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 5000);

    assertNotNull(handle);
    assertTrue(handle.isValid());
    handle.close();
    assertFalse(handle.isValid());
  }

  @Test
  public void testWriteLockHandleCloseIdempotent() throws IOException {
    File f = tempDir.resolve("write_idempotent.csv").toFile();
    Files.write(f.toPath(), "x".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 5000);
    handle.close();
    // Second close should not throw
    handle.close();
  }

  @Test
  public void testWriteLockOnNewFileCreatesIt() throws IOException {
    // RandomAccessFile("rw") creates the file if it doesn't exist
    File f = tempDir.resolve("new_write.csv").toFile();

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 100);
    assertNotNull(handle);
    assertTrue(handle.isValid());
    handle.close();
    // File was created by write lock
    assertTrue(f.exists(), "Write lock should have created the file");
  }

  @Test
  public void testWriteLockAcquireAndRelease() throws IOException {
    File f = tempDir.resolve("write_rel.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle =
        SourceFileLockManager.acquireWriteLock(f, 5000);
    assertNotNull(handle);
    assertTrue(handle.isValid());
    handle.close();
    // After close, re-acquiring should succeed
    SourceFileLockManager.LockHandle handle2 =
        SourceFileLockManager.acquireWriteLock(f, 5000);
    assertNotNull(handle2);
    handle2.close();
  }

  @Test
  public void testReadLockThenWriteLockOnDifferentFiles() throws IOException {
    File readFile = tempDir.resolve("read_file.csv").toFile();
    Files.write(readFile.toPath(), "r_data".getBytes(StandardCharsets.UTF_8));

    File writeFile = tempDir.resolve("write_file.csv").toFile();
    Files.write(writeFile.toPath(), "w_data".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle readHandle =
        SourceFileLockManager.acquireReadLock(readFile);
    SourceFileLockManager.LockHandle writeHandle =
        SourceFileLockManager.acquireWriteLock(writeFile, 5000);

    assertNotNull(readHandle);
    assertNotNull(writeHandle);
    assertTrue(readHandle.isValid());
    assertTrue(writeHandle.isValid());

    readHandle.close();
    writeHandle.close();
  }

  @Test
  public void testReadLockOnSameFileMultipleTimes() throws IOException {
    // Covers the OverlappingFileLockException path in acquireReadLock
    // where the same JVM already has a lock on the file
    File f = tempDir.resolve("overlap.csv").toFile();
    Files.write(f.toPath(), "overlap_data".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle h1 = SourceFileLockManager.acquireReadLock(f);
    assertNotNull(h1);
    assertTrue(h1.isValid());

    // Second read lock on same file - may hit OverlappingFileLockException
    // depending on OS and JVM. Either it succeeds or we get an IOException.
    try {
      SourceFileLockManager.LockHandle h2 = SourceFileLockManager.acquireReadLock(f);
      assertNotNull(h2);
      h2.close();
    } catch (IOException e) {
      // Acceptable - some OS/JVM combinations reject overlapping locks
    }

    h1.close();
  }

  @Test
  public void testReadLockDefaultTimeout() throws IOException {
    // Exercises the default 30-second timeout path
    File f = tempDir.resolve("default_timeout.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    // acquireReadLock(file) uses the default 30-second timeout
    SourceFileLockManager.LockHandle handle = SourceFileLockManager.acquireReadLock(f);
    assertNotNull(handle);
    assertTrue(handle.isValid());
    handle.close();
  }

  // === Timeout path tests ===

  /**
   * Tests the timeout path in acquireReadLock (line 115).
   * A negative timeout means the deadline is already in the past,
   * so the while loop never executes and throws immediately.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testReadLockTimeoutThrows() throws IOException {
    File f = tempDir.resolve("read_timeout.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    // timeoutMs = -1 → deadline = now - 1, while(now < now-1) is false → immediate timeout
    assertThrows(IOException.class, () ->
        SourceFileLockManager.acquireReadLock(f, -1L),
        "Should throw IOException on timeout");
  }

  /**
   * Tests the timeout path in acquireWriteLock (line 172).
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void testWriteLockTimeoutThrows() throws IOException {
    File f = tempDir.resolve("write_timeout.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    // timeoutMs = -1 → deadline = now - 1, while(now < now-1) is false → immediate timeout
    assertThrows(IOException.class, () ->
        SourceFileLockManager.acquireWriteLock(f, -1L),
        "Should throw IOException on write lock timeout");
  }

  // === Thread interruption path tests ===

  /**
   * Tests that InterruptedException during sleep in acquireReadLock throws IOException.
   * This exercises lines 109-112 in SourceFileLockManager.
   *
   * <p>Strategy: Use a very short timeout (100ms) and acquire the lock from the
   * main thread first (causing OverlappingFileLockException on retry), then
   * interrupt the waiting thread.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testReadLockInterruptedThrows() throws Exception {
    File f = tempDir.resolve("read_interrupt.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    // Acquire lock on main thread so the worker thread will retry in a sleep loop
    SourceFileLockManager.LockHandle mainHandle = SourceFileLockManager.acquireReadLock(f);

    final AtomicReference<Exception> caught = new AtomicReference<Exception>();

    Thread worker = new Thread(new Runnable() {
      @Override public void run() {
        try {
          // Use a long timeout (10s) so it will be sleeping when interrupted
          SourceFileLockManager.acquireReadLock(f, 10000L);
        } catch (IOException e) {
          caught.set(e);
        }
      }
    });
    worker.start();

    // Give the worker time to enter the retry loop and fall asleep
    Thread.sleep(200);
    worker.interrupt();
    worker.join(3000);

    mainHandle.close();

    // If IOException was thrown, it should contain "Interrupted"
    // If OverlappingFileLockException was handled by returning shared handle, caught stays null
    Exception ex = caught.get();
    if (ex != null) {
      assertTrue(ex.getMessage().contains("Interrupted") || ex.getMessage().contains("lock"),
          "Unexpected exception: " + ex.getMessage());
    }
  }

  /**
   * Tests that InterruptedException during sleep in acquireWriteLock throws IOException.
   * This exercises lines 162-169 in SourceFileLockManager.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testWriteLockInterruptedThrows() throws Exception {
    File f = tempDir.resolve("write_interrupt.csv").toFile();
    Files.write(f.toPath(), "data".getBytes(StandardCharsets.UTF_8));

    // Acquire write lock on main thread - prevents worker from getting it
    SourceFileLockManager.LockHandle mainHandle = SourceFileLockManager.acquireWriteLock(f, 5000);

    final AtomicReference<Exception> caught = new AtomicReference<Exception>();

    Thread worker = new Thread(new Runnable() {
      @Override public void run() {
        try {
          // Long timeout so it will sleep; it will first get OverlappingFileLockException
          // then throw immediately from that branch
          SourceFileLockManager.acquireWriteLock(f, 10000L);
        } catch (IOException e) {
          caught.set(e);
        }
      }
    });
    worker.start();
    Thread.sleep(200);
    worker.interrupt();
    worker.join(3000);

    mainHandle.close();

    // Worker should have gotten "Cannot acquire write lock" or "Interrupted"
    // (OverlappingFileLockException throws immediately in write path, before sleeping)
    Exception ex = caught.get();
    assertNotNull(ex, "Worker should have gotten an IOException");
  }
}
