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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.ModelOperand;

/**
 * EDGAR-specific paging over the generic {@link CrossProcessRateLimiter}.
 *
 * <p>SEC enforces ~10 requests/second <em>per IP</em>, across all sec.gov
 * endpoints (www.sec.gov/Archives, data.sec.gov/submissions, full-index, ...).
 * All SEC fetchers — the govdata downloaders here and the file-layer
 * {@code DocumentSource} submissions fetch — pace through the single
 * {@code "sec.gov"} budget so parallel worker JVMs on one host share it.
 *
 * <p>The "all sec.gov is one per-IP budget" policy and the EDGAR rate value
 * ({@code sec.edgarRateLimitMs}, default 100ms) live here in the SEC adapter;
 * the file layer only provides the generic cross-process mechanism. The
 * {@code DocumentSource} side reaches the same {@code "sec.gov"} budget by
 * keying on the request's registrable domain, which collapses every sec.gov
 * host to {@code "sec.gov"}.
 */
public final class EdgarRateLimiter {

  /** Shared budget key for all sec.gov traffic (matches DocumentSource's registrable-domain key). */
  public static final String SEC_KEY = "sec.gov";
  // 125ms = 8 req/s: headroom below SEC's 10 req/s per-IP ceiling (100ms left no margin for
  // slot-claim-to-send jitter and tripped SEC's escalating IP block). Matches sec-schema.yaml's
  // edgarRateLimitMs default and the file-layer DocumentSource default so all SEC paths agree.
  private static final long DEFAULT_INTERVAL_MS = 125L;

  // Akamai blocks on this host have lasted tens of minutes; probing sooner extends them.
  private static final long DEFAULT_COOLDOWN_MS = 30L * 60L * 1000L;

  private EdgarRateLimiter() {
  }

  private static final java.util.concurrent.atomic.AtomicLong ACQUIRED =
      new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicBoolean HOOKED =
      new java.util.concurrent.atomic.AtomicBoolean();

  /** Blocks until the next slot in the host-wide sec.gov request budget. */
  public static void acquire() {
    if (HOOKED.compareAndSet(false, true)) {
      // One line per JVM saying how many sec.gov requests it made: the figure every
      // request-saving change (bulk data sets, the daily feed) is measured against.
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override public void run() {
          org.slf4j.LoggerFactory.getLogger(EdgarRateLimiter.class).info(
              "sec.gov requests paced by EdgarRateLimiter in this JVM: {}", ACQUIRED.get());
        }
      }, "edgar-request-count"));
    }
    long blockedUntil = blockedUntil();
    if (blockedUntil > System.currentTimeMillis()) {
      throw new IllegalStateException("sec.gov refused this host with HTTP 403/429; no requests "
          + "until " + java.time.Instant.ofEpochMilli(blockedUntil));
    }
    ACQUIRED.incrementAndGet();
    long interval = ModelOperand.getLong("sec.edgarRateLimitMs", DEFAULT_INTERVAL_MS);
    CrossProcessRateLimiter.acquire(SEC_KEY, interval > 0 ? interval : DEFAULT_INTERVAL_MS);
  }

  private static java.nio.file.Path blockFile() {
    return java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"),
        "calcite-ratelimit-" + SEC_KEY + ".blocked");
  }

  /** Epoch millis until which every process on this host must not call sec.gov, or 0. */
  static long blockedUntil() {
    java.nio.file.Path file = blockFile();
    if (!java.nio.file.Files.exists(file)) {
      return 0L;
    }
    try {
      return Long.parseLong(new String(java.nio.file.Files.readAllBytes(file),
          java.nio.charset.StandardCharsets.UTF_8).trim());
    } catch (java.io.IOException | NumberFormatException e) {
      throw new IllegalStateException("Cannot read sec.gov block marker " + file, e);
    }
  }

  /**
   * Records a sec.gov refusal. A 403 or 429 starts a host-wide cool-down
   * ({@code sec.edgarBlockCooldownMs}) during which {@link #acquire()} throws, so no process
   * keeps sending requests to a host that has blocked it. Other statuses are ignored.
   *
   * @param status HTTP status of the response
   * @param url requested URL, named in the log
   */
  public static void recordStatus(int status, String url) {
    if (status != 403 && status != 429) {
      return;
    }
    long cooldown = ModelOperand.getLong("sec.edgarBlockCooldownMs", DEFAULT_COOLDOWN_MS);
    long until = System.currentTimeMillis() + cooldown;
    try {
      java.nio.file.Files.write(blockFile(),
          Long.toString(until).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Cannot write sec.gov block marker " + blockFile(), e);
    }
    org.slf4j.LoggerFactory.getLogger(EdgarRateLimiter.class).error(
        "HTTP {} from {}: no sec.gov requests from this host until {}", status, url,
        java.time.Instant.ofEpochMilli(until));
  }

  /** The User-Agent (with a real contact) declared on direct sec.gov requests, from the model. */
  public static String userAgent() {
    String ua = ModelOperand.getString("sec.edgarUserAgent");
    if (ua == null || ua.isEmpty()) {
      throw new IllegalStateException(
          "sec.edgarUserAgent is not set in the sec schema operand; SEC requires a contact");
    }
    return ua;
  }

  /** Requests paced so far in this JVM. */
  public static long acquired() {
    return ACQUIRED.get();
  }
}
