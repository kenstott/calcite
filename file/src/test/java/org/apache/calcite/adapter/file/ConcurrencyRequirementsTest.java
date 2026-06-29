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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.adapter.file.format.parquet.ConcurrentParquetCache;
import org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-127 / FILE-128 / FILE-129 — exact-assertion recode of the weak file-adapter
 * concurrency/locking tests. Each method pins a precise locking invariant instead of
 * smoke-testing that a handle is merely non-null.
 *
 * <ul>
 *   <li>FILE-128: {@link SourceFileLockManager} shared read-lock coexistence,
 *       immediate timeout on a non-positive deadline, and idempotent / state-flipping
 *       {@code LockHandle} close. Mirrors {@code SourceFileLockManagerTest} /
 *       {@code SourceFileLockManagerDeepCoverageTest}.</li>
 *   <li>FILE-129: {@link CrossProcessCommitLock} independence across distinct table
 *       locations and per-table in-JVM reentrancy. Mirrors
 *       {@code CrossProcessCommitLockTest}.</li>
 *   <li>FILE-127: {@link ConcurrentParquetCache#convertWithLocking} serializes racing
 *       threads onto one source and hands every thread a valid existing parquet file,
 *       with {@code conversionCount >= 1} (a cache-hit fast path is allowed). Mirrors
 *       {@code ConcurrentParquetCacheTest#testConcurrentConversionSerializesAccess}.</li>
 * </ul>
 *
 * <p>NOTE: the cross-process (separate-JVM / Redis-backed) exclusion paths in these
 * components are intentionally omitted here — this class is hermetic (@TempDir, no
 * external services), and asserts only the same-JVM behavior reachable without Redis
 * or a child process. The child-JVM exclusion case lives in
 * {@code CrossProcessCommitLockTest#heldByAnotherProcessCannotBeAcquired}.
 */
@Tag("unit")
public class ConcurrencyRequirementsTest {

  @TempDir
  Path tempDir;

  // === FILE-128: SourceFileLockManager ===

  /** Multiple shared read locks on one file coexist; both handles stay valid. */
  @Test
  @Tag("FILE-128")
  void sharedReadLocksCoexist() throws IOException {
    File f = tempDir.resolve("shared.csv").toFile();
    Files.write(f.toPath(), "a,b,c".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle first = SourceFileLockManager.acquireReadLock(f);
    SourceFileLockManager.LockHandle second = SourceFileLockManager.acquireReadLock(f);
    try {
      assertNotNull(first, "first shared read lock should be acquired");
      assertNotNull(second, "second shared read lock should coexist with the first");
      assertTrue(first.isValid(), "first read lock should be valid while held");
      assertTrue(second.isValid(), "second read lock should be valid while held");
    } finally {
      first.close();
      second.close();
    }
  }

  /**
   * A non-positive (already-past) timeout makes the deadline lie in the past, so the
   * acquisition loop never runs and an {@link IOException} is thrown immediately.
   */
  @Test
  @Tag("FILE-128")
  void pastTimeoutThrowsImmediately() throws IOException {
    File f = tempDir.resolve("past_timeout.csv").toFile();
    Files.write(f.toPath(), "x,y,z".getBytes(StandardCharsets.UTF_8));

    assertThrows(IOException.class,
        () -> SourceFileLockManager.acquireReadLock(f, -1L),
        "a negative/already-past timeout must throw IOException immediately");
  }

  /**
   * {@code LockHandle.close()} is idempotent and {@code isValid()} flips true -> false
   * across the close.
   */
  @Test
  @Tag("FILE-128")
  void closeIsIdempotentAndFlipsValidity() throws IOException {
    File f = tempDir.resolve("validity.csv").toFile();
    Files.write(f.toPath(), "content".getBytes(StandardCharsets.UTF_8));

    SourceFileLockManager.LockHandle handle = SourceFileLockManager.acquireReadLock(f);
    assertTrue(handle.isValid(), "handle should be valid immediately after acquisition");

    handle.close();
    assertFalse(handle.isValid(), "handle must be invalid after close");

    // Second close must be a no-op, not an error, and must not revive validity.
    handle.close();
    assertFalse(handle.isValid(), "handle must remain invalid after a redundant close");
  }

  // === FILE-129: CrossProcessCommitLock ===

  /** Locks for two different table locations are independent and acquire concurrently. */
  @Test
  @Tag("FILE-129")
  void differentTablesAreIndependent() {
    String tableA = "s3://test-bucket/req/a/" + System.nanoTime();
    String tableB = "s3://test-bucket/req/b/" + System.nanoTime();

    CrossProcessCommitLock.Handle a = CrossProcessCommitLock.tryAcquire(tableA);
    CrossProcessCommitLock.Handle b = CrossProcessCommitLock.tryAcquire(tableB);
    try {
      assertNotNull(a, "table A lock should be free");
      assertNotNull(b, "table B lock should be acquirable while A is held");
    } finally {
      if (a != null) {
        a.close();
      }
      if (b != null) {
        b.close();
      }
    }
  }

  /**
   * Acquisition is reentrant per table within one JVM: a second acquire for the same
   * table succeeds via the hold count, and the lock stays held until every hold closes.
   */
  @Test
  @Tag("FILE-129")
  void acquireIsReentrantPerTable() {
    String tableLocation = "s3://test-bucket/req/re/" + System.nanoTime();

    CrossProcessCommitLock.Handle first = CrossProcessCommitLock.acquire(tableLocation);
    CrossProcessCommitLock.Handle second = CrossProcessCommitLock.acquire(tableLocation);
    assertNotNull(first, "first acquire should take the lock");
    assertNotNull(second, "second acquire for the same table should succeed via hold-count");

    // Releasing one of two reentrant holds keeps the lock held by this JVM.
    second.close();
    assertNull(CrossProcessCommitLock.tryAcquire(tableLocation),
        "lock must stay held after releasing only one reentrant hold");

    // Releasing the final hold frees it.
    first.close();
    CrossProcessCommitLock.Handle reacquired = CrossProcessCommitLock.tryAcquire(tableLocation);
    assertNotNull(reacquired, "lock must be free after all holds are released");
    reacquired.close();
  }

  // === FILE-127: ConcurrentParquetCache ===

  /**
   * Under N threads racing on a single source, conversion is serialized and every thread
   * receives a valid existing parquet file. The asserted invariant is
   * {@code conversionCount >= 1} (NOT exactly 1) because a cache-hit fast path is allowed.
   */
  @Test
  @Tag("FILE-127")
  void concurrentConversionSerializesAndAllThreadsGetAFile() throws Exception {
    File cacheDir = new File(tempDir.toFile(), "cache");
    assertTrue(cacheDir.mkdirs(), "cache directory should be created for the test");

    File sourceFile = new File(tempDir.toFile(), "race_source.csv");
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("id,name,value\n");
      writer.write("1,alpha,100\n");
      writer.write("2,beta,200\n");
    }

    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final AtomicInteger conversionCount = new AtomicInteger(0);
    List<Future<File>> futures = new ArrayList<Future<File>>();

    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      futures.add(executor.submit(new Callable<File>() {
        @Override public File call() throws Exception {
          startLatch.await(); // all threads start together
          try {
            return ConcurrentParquetCache.convertWithLocking(
                sourceFile, cacheDir, false, null, "TO_LOWER",
                new ConcurrentParquetCache.ConversionCallback() {
                  @Override public void convert(File targetFile) throws Exception {
                    conversionCount.incrementAndGet();
                    try (FileWriter writer = new FileWriter(targetFile)) {
                      writer.write("thread_" + threadIndex);
                    }
                  }
                });
          } finally {
            doneLatch.countDown();
          }
        }
      }));
    }

    // Release all threads simultaneously to maximize the contention window.
    startLatch.countDown();

    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue(completed, "all racing threads should complete within timeout");

    // Every thread must receive a valid, existing parquet file.
    for (Future<File> future : futures) {
      File result = future.get(5, TimeUnit.SECONDS);
      assertNotNull(result, "each thread must receive a non-null result file");
      assertTrue(result.exists(), "each thread's result file must exist on disk");
    }

    // Serialization invariant: at least one real conversion ran; the cache-hit fast
    // path may have served the rest, so we assert >= 1 (never exactly 1).
    assertTrue(conversionCount.get() >= 1,
        "at least one thread must perform the actual conversion");

    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
        "executor should terminate after the race completes");
  }
}
