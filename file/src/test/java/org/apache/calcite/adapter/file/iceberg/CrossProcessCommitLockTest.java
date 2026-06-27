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
package org.apache.calcite.adapter.file.iceberg;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused tests for {@link CrossProcessCommitLock}.
 *
 * <p>Verifies the cross-process layer the parallel-CIK funnel relies on:
 * <ul>
 *   <li>two JVM processes cannot hold the same table's commit lock at once;</li>
 *   <li>different tables never block one another;</li>
 *   <li>acquisition is reentrant within a single JVM (so nested commit paths
 *       do not self-deadlock).</li>
 * </ul>
 *
 * <p>The mutual-exclusion test launches a child JVM (via {@link #main}) that
 * acquires the lock and holds it until released, proving exclusion across real
 * OS processes rather than just threads.
 */
@Tag("unit")
public class CrossProcessCommitLockTest {

  /** Child-process entry point: acquire, signal ready, hold until released. */
  public static void main(String[] args) throws Exception {
    String tableLocation = args[0];
    Path readyFile = Paths.get(args[1]);
    Path releaseFile = Paths.get(args[2]);

    CrossProcessCommitLock.Handle handle = CrossProcessCommitLock.acquire(tableLocation);
    Files.write(readyFile, "ready".getBytes(StandardCharsets.UTF_8));

    long deadline = System.currentTimeMillis() + 30_000L;
    while (!Files.exists(releaseFile) && System.currentTimeMillis() < deadline) {
      Thread.sleep(25L);
    }
    handle.close();
    System.exit(0);
  }

  @Test void heldByAnotherProcessCannotBeAcquired() throws Exception {
    String tableLocation = "s3://test-bucket/xproc/excl/" + System.nanoTime();
    Path dir = Files.createTempDirectory("xproc-lock-test");
    Path readyFile = dir.resolve("ready");
    Path releaseFile = dir.resolve("release");

    Process child = startChild(tableLocation, readyFile, releaseFile);
    try {
      awaitFile(readyFile, 20_000L);

      // The child process holds the OS lock; we must not be able to take it.
      CrossProcessCommitLock.Handle blocked = CrossProcessCommitLock.tryAcquire(tableLocation);
      assertNull(blocked, "lock must be held by the child process");

      // Release the child and let it exit, freeing the OS lock.
      Files.write(releaseFile, "go".getBytes(StandardCharsets.UTF_8));
      assertTrue(child.waitFor(20, TimeUnit.SECONDS), "child process should exit");
      assertEquals(0, child.exitValue(), "child process should exit cleanly");

      // Now the lock is free.
      CrossProcessCommitLock.Handle free = CrossProcessCommitLock.tryAcquire(tableLocation);
      assertNotNull(free, "lock must be acquirable after the child released it");
      free.close();
    } finally {
      child.destroyForcibly();
    }
  }

  @Test void differentTablesDoNotBlock() {
    String tableA = "s3://test-bucket/xproc/a/" + System.nanoTime();
    String tableB = "s3://test-bucket/xproc/b/" + System.nanoTime();

    CrossProcessCommitLock.Handle a = CrossProcessCommitLock.tryAcquire(tableA);
    CrossProcessCommitLock.Handle b = CrossProcessCommitLock.tryAcquire(tableB);
    try {
      assertNotNull(a, "table A lock should be free");
      assertNotNull(b, "table B lock should be held concurrently with A");
    } finally {
      if (a != null) {
        a.close();
      }
      if (b != null) {
        b.close();
      }
    }
  }

  @Test void acquireIsReentrantWithinJvm() {
    String tableLocation = "s3://test-bucket/xproc/re/" + System.nanoTime();

    CrossProcessCommitLock.Handle first = CrossProcessCommitLock.acquire(tableLocation);
    CrossProcessCommitLock.Handle second = CrossProcessCommitLock.acquire(tableLocation);
    assertNotNull(first);
    assertNotNull(second);

    // Releasing one of two holds must keep the lock held by this JVM.
    second.close();
    assertNull(CrossProcessCommitLock.tryAcquire(tableLocation),
        "lock must still be held after releasing only one reentrant hold");

    // Releasing the last hold frees it.
    first.close();
    CrossProcessCommitLock.Handle reacquired = CrossProcessCommitLock.tryAcquire(tableLocation);
    assertNotNull(reacquired, "lock must be free after all holds are released");
    reacquired.close();
  }

  private static Process startChild(String tableLocation, Path readyFile, Path releaseFile)
      throws IOException {
    String javaBin = System.getProperty("java.home")
        + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath,
        CrossProcessCommitLockTest.class.getName(),
        tableLocation, readyFile.toString(), releaseFile.toString());
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    return pb.start();
  }

  private static void awaitFile(Path file, long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (!Files.exists(file)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("timed out waiting for " + file);
      }
      Thread.sleep(25L);
    }
  }
}
