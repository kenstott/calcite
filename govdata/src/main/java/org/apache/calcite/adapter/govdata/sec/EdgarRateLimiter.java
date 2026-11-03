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
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.govdata.GovDataUtils;

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

  private static final long DEFAULT_COOLDOWN_MS = CrossProcessRateLimiter.DEFAULT_COOLDOWN_MS;

  private static final long DEFAULT_RETRY_FLOOR_MS = 10_000L;
  private static final long DEFAULT_MAX_RETRIES = 5L;

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
              "sec.gov requests paced by EdgarRateLimiter in this JVM: {}; live document fetches "
                  + "through DocumentSource: {}", ACQUIRED.get(),
              org.apache.calcite.adapter.file.etl.DocumentSource.liveCalls());
        }
      }, "edgar-request-count"));
    }
    CrossProcessRateLimiter.checkNotBlocked(SEC_KEY);
    ACQUIRED.incrementAndGet();
    long interval = ModelOperand.getLong("sec.edgarRateLimitMs", DEFAULT_INTERVAL_MS);
    CrossProcessRateLimiter.acquire(SEC_KEY, interval > 0 ? interval : DEFAULT_INTERVAL_MS);
  }

  /** Epoch millis until which every process on this host must not call sec.gov, or 0. */
  static long blockedUntil() {
    return CrossProcessRateLimiter.blockedUntil(SEC_KEY);
  }

  /**
   * Records a sec.gov refusal. A 403 starts a host-wide cool-down
   * ({@code sec.edgarBlockCooldownMs}) during which {@link #acquire()} throws, so no process
   * keeps sending requests to a host that has blocked it. Other statuses are ignored; a 429 is
   * paced by {@link #connect} instead.
   *
   * @param status HTTP status of the response
   * @param url requested URL, named in the log
   */
  public static void recordStatus(int status, String url) {
    if (status != 403) {
      return;
    }
    long cooldown = ModelOperand.getLong("sec.edgarBlockCooldownMs", DEFAULT_COOLDOWN_MS);
    long until = CrossProcessRateLimiter.block(SEC_KEY, cooldown);
    org.slf4j.LoggerFactory.getLogger(EdgarRateLimiter.class).error(
        "HTTP {} from {}: no sec.gov requests from this host until {}", status, url,
        java.time.Instant.ofEpochMilli(until));
  }

  /**
   * Sends one paced request to sec.gov and returns the connection with its status read. A 403
   * starts the host-wide cool-down ({@link #recordStatus}); a 429 waits for the server's
   * {@code Retry-After} (never less than {@code sec.edgarRetryFloorMs}) and retries, at most
   * {@code sec.edgarMaxRetries} times, then throws naming the URL. Callers read the status from
   * the returned connection.
   */
  static java.net.HttpURLConnection connect(String url, String method, int connectTimeoutMs,
      int readTimeoutMs) throws java.io.IOException {
    return connect(url, method, connectTimeoutMs, readTimeoutMs,
        ModelOperand.getLong("sec.edgarRetryFloorMs", DEFAULT_RETRY_FLOOR_MS),
        (int) ModelOperand.getLong("sec.edgarMaxRetries", DEFAULT_MAX_RETRIES));
  }

  static java.net.HttpURLConnection connect(String url, String method, int connectTimeoutMs,
      int readTimeoutMs, long retryFloorMs, int maxRetries) throws java.io.IOException {
    for (int attempt = 0;; attempt++) {
      acquire();
      java.net.HttpURLConnection conn =
          (java.net.HttpURLConnection) java.net.URI.create(url).toURL().openConnection();
      conn.setRequestMethod(method);
      conn.setRequestProperty("User-Agent", userAgent());
      conn.setConnectTimeout(connectTimeoutMs);
      conn.setReadTimeout(readTimeoutMs);
      org.apache.calcite.adapter.file.etl.NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      recordStatus(status, url);
      if (status != 429) {
        return conn;
      }
      if (attempt >= maxRetries) {
        throw new java.io.IOException("HTTP 429 from " + url + " after " + (attempt + 1)
            + " attempts");
      }
      long wait = Math.max(retryFloorMs, retryAfterMs(conn));
      org.slf4j.LoggerFactory.getLogger(EdgarRateLimiter.class).warn(
          "HTTP 429 from {}: waiting {} ms before retry {}/{}", url, wait, attempt + 1,
          maxRetries);
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new java.io.IOException("Interrupted waiting to retry " + url, e);
      }
    }
  }

  /** The {@code Retry-After} delay in millis (seconds form only), or 0 when absent. */
  private static long retryAfterMs(java.net.HttpURLConnection conn) {
    String v = conn.getHeaderField("Retry-After");
    if (v == null) {
      return 0L;
    }
    try {
      return Long.parseLong(v.trim()) * 1000L;
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  /** The User-Agent (with a real contact) declared on direct sec.gov requests, from the model. */
  public static String userAgent() {
    String ua = ModelOperand.getString("sec.edgarUserAgent");
    if (ua == null) {
      // A model that does not name one (the hand-built accession reprocess model) gets the schema
      // YAML's own value, the single place it is declared.
      ua = GovDataUtils.loadSchemaSetting(SecSchemaFactory.class, "/sec/sec-schema.yaml",
          "edgarUserAgent");
      if (ua != null && ua.contains("${")) {
        ua = VariableResolver.resolveEnvVars(ua);
      }
    }
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
