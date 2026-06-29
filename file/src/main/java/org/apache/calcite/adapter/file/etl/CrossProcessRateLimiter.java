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
package org.apache.calcite.adapter.file.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generic host-local, cross-process rate limiter.
 *
 * <p>Paces requests sharing a {@code key} across every process and thread on the
 * host through a single budget, spaced {@code intervalMs} apart. A per-JVM lock
 * (or a {@code Thread.sleep}) only bounds threads within one process, so N
 * worker processes on one host can collectively exceed a per-host/per-IP limit;
 * this limiter closes that gap.
 *
 * <p>Mechanism: one monotonically advancing "next slot" timestamp per key, kept
 * in a small file under the host temp dir and guarded by an OS file lock so the
 * read-compute-write is atomic across processes. Each caller claims the next
 * free slot, releases the lock, then sleeps until its slot — the lock is held
 * only briefly and aggregate throughput is bounded to one request per interval
 * across the whole host.
 *
 * <p>This class is policy-free: callers choose the {@code key} (e.g. a host or
 * registrable domain) and {@code intervalMs}. Any domain-specific grouping
 * (e.g. "all sec.gov endpoints share one budget") belongs in the caller, not
 * here.
 */
public final class CrossProcessRateLimiter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrossProcessRateLimiter.class);

  /**
   * Per-key intra-JVM monitor. An OS {@link FileLock} is held on behalf of the entire JVM, not the
   * acquiring thread, so a second thread in the same process that calls {@code channel.lock()} on
   * the same file throws {@link java.nio.channels.OverlappingFileLockException} (a RuntimeException)
   * rather than blocking. Serializing same-JVM callers on this monitor first means only one thread
   * per process ever contends for the file lock, which is then left to do its real job: pacing
   * across processes. Together they bound throughput across all threads AND processes on the host.
   */
  private static final ConcurrentHashMap<String, Object> KEY_MONITORS =
      new ConcurrentHashMap<String, Object>();

  private CrossProcessRateLimiter() {
  }

  private static Path stateFile(String key) {
    String safe = key == null ? "default" : key.replaceAll("[^a-zA-Z0-9._-]", "_");
    return Paths.get(System.getProperty("java.io.tmpdir"), "calcite-ratelimit-" + safe + ".slot");
  }

  /**
   * Blocks until this caller's slot in the host-wide budget for {@code key},
   * then returns. No-op when {@code intervalMs <= 0}.
   *
   * @param key budget identity shared by all callers that must be paced together
   * @param intervalMs minimum spacing between successive requests on this key
   */
  public static void acquire(String key, long intervalMs) {
    if (intervalMs <= 0) {
      return;
    }
    long now = System.currentTimeMillis();
    long slot;
    Object monitor = KEY_MONITORS.computeIfAbsent(
        key == null ? "default" : key, k -> new Object());
    try {
      Path file = stateFile(key);
      Files.createDirectories(file.getParent());
      // Serialize same-JVM callers before taking the OS file lock — see KEY_MONITORS.
      synchronized (monitor) {
        try (FileChannel channel = FileChannel.open(file,
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
          try (FileLock lock = channel.lock()) {
            long last = readLong(channel);
            slot = Math.max(now, last + intervalMs);
            writeLong(channel, slot);
          }
        }
      }
    } catch (IOException e) {
      // Never let a limiter failure block ingestion; fall back to a local pause.
      LOGGER.warn("Rate limiter unavailable for key '{}' ({}), falling back to local pacing",
          key, e.getMessage());
      sleepQuietly(intervalMs);
      return;
    }
    // Recompute against current time: a thread that waited on the monitor has already burned part
    // of the gap to its slot, so sleeping (slot - now) with the pre-wait timestamp would over-pace.
    sleepQuietly(slot - System.currentTimeMillis());
  }

  private static long readLong(FileChannel channel) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
    channel.position(0);
    int read = channel.read(buf);
    if (read < Long.BYTES) {
      return 0L;
    }
    buf.flip();
    return buf.getLong();
  }

  private static void writeLong(FileChannel channel, long value) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
    buf.putLong(value);
    buf.flip();
    channel.position(0);
    channel.write(buf);
  }

  private static void sleepQuietly(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
