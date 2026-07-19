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
package org.apache.calcite.adapter.file.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared retryable HTTP opener for {@link StreamingResponseTransformer}s, which open their own
 * connection and so bypass {@link HttpSource}'s built-in retry/rate-limit path. Centralizing the
 * logic here keeps API sources (Comtrade, ILOSTAT, FAOSTAT, …) consistent instead of each rolling
 * its own (or none), and makes the source's declared {@code rateLimit:} YAML block actually apply
 * to them.
 *
 * <p>On a retryable status (429/500/503) or a connection {@link IOException} it retries up to the
 * configured {@code maxRetries}, honoring the server's {@code Retry-After} header when present
 * (essential for 429 rate limits, which report exactly how long to wait) and otherwise backing off
 * exponentially — both capped so one rate-limited request can't stall a worker unbounded. A
 * declared {@code requestsPerSecond} is enforced proactively via a JVM-wide minimum spacing.
 * Non-retryable non-200 responses (e.g. 404/403) are surfaced as {@link HttpStatusException} so the
 * caller can apply its own semantics (e.g. treat 404 as an honest absence).
 */
public final class RetryableHttp {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryableHttp.class);

  private static final int DEFAULT_MAX_RETRIES = 6;
  private static final long DEFAULT_BASE_BACKOFF_MS = 2_000L;
  private static final long MAX_BACKOFF_MS = 60_000L;
  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 1_800_000;   // 30 min — large bulk downloads (FAO zips)
  private static final String DEFAULT_USER_AGENT = "GovData/1.0";

  // JVM-wide next-allowed time for proactive requestsPerSecond spacing. One ETL worker = one JVM.
  private static final AtomicLong NEXT_ALLOWED_MS = new AtomicLong(0L);

  private RetryableHttp() {
  }

  /** Signals a non-retryable, non-200 HTTP status so callers can apply their own handling. */
  public static final class HttpStatusException extends IOException {
    private final int status;

    public HttpStatusException(int status, String message) {
      super(message);
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  /**
   * Opens {@code url} and returns a connected {@link HttpURLConnection} at HTTP 200, retrying on
   * 429/500/503 and connection errors per {@code rateLimit}. The caller owns the returned
   * connection (read {@code getInputStream()} then {@code disconnect()}).
   *
   * @param url             fully-resolved request URL
   * @param requestProps    request headers to set (override the default User-Agent if present)
   * @param rateLimit       the source's declared rate-limit config, or null for defaults
   * @param followRedirects whether to follow HTTP redirects
   * @return a connected 200 connection
   * @throws HttpStatusException on a non-retryable non-200 status (e.g. 404)
   * @throws IOException         if retries are exhausted
   */
  public static HttpURLConnection openWithRetry(String url, Map<String, String> requestProps,
      HttpSourceConfig.RateLimitConfig rateLimit, boolean followRedirects) throws IOException {
    int maxRetries = rateLimit != null && rateLimit.getMaxRetries() > 0
        ? rateLimit.getMaxRetries() : DEFAULT_MAX_RETRIES;
    long baseBackoffMs = rateLimit != null && rateLimit.getRetryBackoffMs() > 0
        ? rateLimit.getRetryBackoffMs() : DEFAULT_BASE_BACKOFF_MS;
    int requestsPerSecond = rateLimit != null ? rateLimit.getRequestsPerSecond() : 0;

    IOException last = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      throttle(requestsPerSecond);
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setInstanceFollowRedirects(followRedirects);
      conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
      if (requestProps != null) {
        for (Map.Entry<String, String> h : requestProps.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }
      int status;
      try {
        status = conn.getResponseCode();
      } catch (IOException e) {
        conn.disconnect();
        last = e;
        if (attempt < maxRetries) {
          backoff(attempt, -1L, baseBackoffMs);
        }
        continue;
      }
      if (status == HttpURLConnection.HTTP_OK) {
        return conn;
      }
      long retryAfterMs = retryAfterMillis(conn);
      conn.disconnect();
      if (status == 429 || status == 500 || status == 503) {
        last = new HttpStatusException(status, "HTTP " + status + " from " + url);
        LOGGER.warn("HTTP {} (attempt {}/{}) for {}{}", status, attempt + 1, maxRetries + 1, url,
            retryAfterMs >= 0 ? " — Retry-After " + (retryAfterMs / 1000) + "s" : "");
        if (attempt < maxRetries) {
          backoff(attempt, retryAfterMs, baseBackoffMs);
        }
        continue;
      }
      throw new HttpStatusException(status, "HTTP " + status + " from " + url);
    }
    throw last != null ? last : new IOException("retries exhausted for " + url);
  }

  /** Enforces a minimum spacing of {@code 1000/requestsPerSecond} ms between calls, JVM-wide. */
  private static void throttle(int requestsPerSecond) {
    if (requestsPerSecond <= 0) {
      return;
    }
    long spacingMs = 1000L / requestsPerSecond;
    while (true) {
      long now = System.currentTimeMillis();
      long prevNext = NEXT_ALLOWED_MS.get();
      long myTurn = Math.max(now, prevNext);
      if (NEXT_ALLOWED_MS.compareAndSet(prevNext, myTurn + spacingMs)) {
        long waitMs = myTurn - now;
        if (waitMs > 0) {
          sleep(waitMs);
        }
        return;
      }
    }
  }

  /**
   * Waits before the next retry: honors {@code Retry-After} when {@code retryAfterMs >= 0}, else
   * exponential backoff from {@code baseBackoffMs}; both capped at {@link #MAX_BACKOFF_MS}.
   */
  private static void backoff(int attempt, long retryAfterMs, long baseBackoffMs) {
    long delayMs = retryAfterMs >= 0
        ? Math.min(retryAfterMs, MAX_BACKOFF_MS)
        : Math.min(baseBackoffMs << attempt, MAX_BACKOFF_MS);
    sleep(delayMs);
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Parses the {@code Retry-After} response header to milliseconds; -1 if absent/unparseable. */
  private static long retryAfterMillis(HttpURLConnection conn) {
    String value = conn.getHeaderField("Retry-After");
    if (value == null || value.trim().isEmpty()) {
      return -1L;
    }
    value = value.trim();
    try {
      return Long.parseLong(value) * 1000L;   // delta-seconds form
    } catch (NumberFormatException notSeconds) {
      try {                                    // HTTP-date form (RFC 1123)
        long whenMs = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
            .toInstant().toEpochMilli();
        long deltaMs = whenMs - System.currentTimeMillis();
        return deltaMs > 0 ? deltaMs : 0L;
      } catch (DateTimeParseException notDate) {
        return -1L;
      }
    }
  }
}
