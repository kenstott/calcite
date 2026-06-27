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

/**
 * Host-local, cross-process rate limiter for EDGAR (sec.gov) HTTP access.
 *
 * <p>SEC enforces ~10 requests/second <em>per IP</em>, across all sec.gov
 * endpoints (www.sec.gov/Archives, data.sec.gov/submissions, full-index, ...).
 * Per-process throttles (a {@code Thread.sleep} or an in-JVM "global" lock) do
 * not bound this: N worker processes on one host (= one IP) each pace
 * themselves and collectively issue up to N×10 req/s, tripping EDGAR's limit
 * and producing 429s that can exhaust retries and drop data.
 *
 * <p>This limiter paces <em>all</em> sec.gov requests from every process and
 * thread on the host through a single shared budget. It keeps one monotonically
 * advancing "next slot" timestamp in a small file under the host temp dir,
 * guarded by an OS file lock so the read-compute-write is atomic across
 * processes. Each caller claims the next free slot (spaced {@code intervalMs}
 * apart), releases the lock, then sleeps until its slot — so the lock is held
 * only briefly and aggregate throughput is bounded to one request per interval
 * across the whole host.
 *
 * <p>Lives in the file ETL layer so both the SEC adapter downloaders (govdata)
 * and the generic {@link DocumentSource} fetch path (file) share one budget.
 * The interval is read from the model ({@code sec.edgarRateLimitMs}, default
 * 100ms = 10 req/s). Host-scoped is exactly IP-scoped under the deployment
 * model where each host has its own egress IP.
 */
public final class EdgarRateLimiter {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdgarRateLimiter.class);
  private static final long DEFAULT_INTERVAL_MS = 100L;

  private EdgarRateLimiter() {
  }

  /** Single shared pacing file for the host (one IP per host). */
  private static Path stateFile() {
    return Paths.get(System.getProperty("java.io.tmpdir"), "calcite-edgar-ratelimit.slot");
  }

  private static long intervalMs() {
    long v = ModelOperand.getLong("sec.edgarRateLimitMs", DEFAULT_INTERVAL_MS);
    return v > 0 ? v : DEFAULT_INTERVAL_MS;
  }

  /** Returns true if the URL targets a sec.gov host (so it shares the budget). */
  public static boolean isSecUrl(String url) {
    if (url == null) {
      return false;
    }
    String lower = url.toLowerCase(java.util.Locale.ROOT);
    return lower.contains("sec.gov/") || lower.contains("://sec.gov")
        || lower.contains(".sec.gov");
  }

  /**
   * Blocks until this caller's slot in the host-wide EDGAR request budget, then
   * returns. Call immediately before each sec.gov HTTP request.
   */
  public static void acquire() {
    long interval = intervalMs();
    long now = System.currentTimeMillis();
    long slot;
    try {
      Path file = stateFile();
      Files.createDirectories(file.getParent());
      try (FileChannel channel = FileChannel.open(file,
          StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        try (FileLock lock = channel.lock()) {
          long last = readLong(channel);
          slot = Math.max(now, last + interval);
          writeLong(channel, slot);
        }
      }
    } catch (IOException e) {
      // Never let a limiter failure block ingestion; fall back to a local pause.
      LOGGER.warn("EDGAR rate limiter unavailable ({}), falling back to local pacing", e.getMessage());
      sleepQuietly(interval);
      return;
    }
    sleepQuietly(slot - now);
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
