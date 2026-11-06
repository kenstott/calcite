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

import org.apache.iceberg.LockManager;
import org.apache.iceberg.util.LockManagers;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Commits to different Iceberg tables from several threads of one JVM each take and release
 * the lock manager Iceberg gives every Hadoop catalog and {@code HadoopTables} by default.
 * Releasing that lock must not fail.
 *
 * <p>In Iceberg 1.4.x it did, about once in ten thousand lock cycles on two threads: the
 * manager keeps its heartbeats in a static, unsynchronised map, an entry was lost, and
 * {@code release} threw a {@code NullPointerException} after the commit had taken effect
 * (issue 476). Iceberg 1.5.0 tolerates the lost entry. This test fails on 1.4.x.
 */
@Tag("unit")
class IcebergLockReleaseConcurrencyTest {

  private static final int THREADS = 4;
  private static final int ROUNDS = 100000;

  @Test void releasingLocksOfDifferentTablesFromSeveralThreadsNeverFails() throws Exception {
    final LockManager manager = LockManagers.defaultLockManager();
    final AtomicInteger released = new AtomicInteger();
    final AtomicInteger failed = new AtomicInteger();
    final AtomicReference<Throwable> first = new AtomicReference<Throwable>();
    final CountDownLatch start = new CountDownLatch(1);
    Thread[] committers = new Thread[THREADS];
    for (int t = 0; t < THREADS; t++) {
      final int id = t;
      committers[t] = new Thread(new Runnable() {
        @Override public void run() {
          try {
            start.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          String owner = "lock-release-test-" + id;
          for (int i = 0; i < ROUNDS; i++) {
            // An entity of this thread's own, as the metadata file of its own table is.
            String entity =
                "file:/lock-release-test/t" + id + "_" + i + "/metadata/v1.metadata.json";
            try {
              manager.acquire(entity, owner);
              manager.release(entity, owner);
              released.incrementAndGet();
            } catch (Throwable e) {
              failed.incrementAndGet();
              first.compareAndSet(null, e);
            }
          }
        }
      }, "lock-release-test-" + t);
      committers[t].start();
    }
    start.countDown();
    for (Thread committer : committers) {
      committer.join();
    }

    Throwable failure = first.get();
    assertNull(failure, () -> failed.get() + " of " + (THREADS * ROUNDS)
        + " lock releases failed; the first: " + failure);
    assertEquals(THREADS * ROUNDS, released.get());
  }
}
