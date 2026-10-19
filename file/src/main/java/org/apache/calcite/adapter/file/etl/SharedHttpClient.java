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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The one {@link HttpClient} every ETL HTTP fetch goes through ({@link RetryableHttp} and
 * {@link HttpSource}).
 *
 * <p>The client prefers HTTP/2. ALPN negotiates down to HTTP/1.1 on servers that do not speak h2;
 * that negotiation is the only fallback, and there is no per-host list and no retry on another
 * protocol. Moving off {@code HttpURLConnection} matters because CDN bot filters (Cloudflare and
 * similar) fingerprint its HTTP/1.1 client and reject it, while an h2 client is served. Cleartext
 * {@code http://} requests offer the {@code h2c} upgrade, which a server may decline.
 *
 * <p>What {@code HttpURLConnection} honoured implicitly is carried over explicitly: the JVM
 * default {@link ProxySelector}, {@link CookieHandler} and {@link Authenticator}, and the default
 * {@code SSLContext} (so {@code javax.net.ssl.trustStore*} system properties still apply).
 * Gzip is not negotiated here, exactly as before: no {@code Accept-Encoding} is added and callers
 * that declare a gzip response (the {@code compressed} setting) unwrap the stream themselves.
 *
 * <h3>Timeouts</h3>
 * <ul>
 *   <li><b>Response headers:</b> {@link Request#setReadTimeoutMs} bounds connect plus time to
 *       the response headers (the JDK {@code HttpRequest.timeout}). A per-request connect timeout
 *       does not exist in {@code java.net.http}; the client-wide connect timeout is
 *       {@link #CONNECT_TIMEOUT}.
 *   <li><b>Hard deadline:</b> the call is additionally bounded by that timeout plus
 *       {@link #DNS_DEADLINE_BUFFER_MS}, because name resolution is covered by no JDK timeout and
 *       a stalled resolver would otherwise block the thread indefinitely (kenstott/govdata-ops
 *       #236/#139/#121).
 *   <li><b>Body:</b> {@code HttpRequest.timeout} stops applying once headers arrive, so the body
 *       stream is wrapped: every blocking {@code read} is armed with a watchdog and, if it
 *       stays blocked longer than the read timeout, the exchange is cancelled and the read throws
 *       {@link SocketTimeoutException}. This is the same per-read stall semantics as
 *       {@code HttpURLConnection.setReadTimeout}, so a 30-minute bulk download that keeps
 *       making progress is never cut off.
 * </ul>
 */
public final class SharedHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedHttpClient.class);

  /** Client-wide connect timeout (the longest any caller used with HttpURLConnection). */
  static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(60);

  /** Margin on top of the response timeout for the otherwise-unbounded DNS resolution. */
  static final long DNS_DEADLINE_BUFFER_MS = 30_000L;

  private static HttpClient followingClient;
  private static HttpClient nonFollowingClient;

  private static final ScheduledExecutorService WATCHDOG = newWatchdog();

  private SharedHttpClient() {
  }

  private static ScheduledExecutorService newWatchdog() {
    ThreadFactory daemon = r -> {
      Thread t = new Thread(r, "shared-http-read-watchdog");
      t.setDaemon(true);
      return t;
    };
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemon);
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  /**
   * Returns the shared client. Redirect handling is a client-level setting in
   * {@code java.net.http}, so there is one client per mode, each built once.
   *
   * @param followRedirects {@code NORMAL} redirect policy when true (https to http is not
   *                        followed), {@code NEVER} otherwise
   */
  public static synchronized HttpClient client(boolean followRedirects) {
    if (followRedirects) {
      if (followingClient == null) {
        followingClient = build(HttpClient.Redirect.NORMAL);
      }
      return followingClient;
    }
    if (nonFollowingClient == null) {
      nonFollowingClient = build(HttpClient.Redirect.NEVER);
    }
    return nonFollowingClient;
  }

  private static HttpClient build(HttpClient.Redirect redirect) {
    HttpClient.Builder builder = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .followRedirects(redirect)
        .connectTimeout(CONNECT_TIMEOUT)
        .proxy(ProxySelector.getDefault());
    CookieHandler cookies = CookieHandler.getDefault();
    if (cookies != null) {
      builder.cookieHandler(cookies);
    }
    Authenticator authenticator = Authenticator.getDefault();
    if (authenticator != null) {
      builder.authenticator(authenticator);
    }
    return builder.build();
  }

  /**
   * Sends {@code request} and returns once the response headers have arrived.
   *
   * @throws IOException on connection failure, on a response-header timeout
   *                     ({@link SocketTimeoutException}) or when the hard deadline passes
   */
  public static Response execute(Request request) throws IOException {
    HttpRequest.Builder builder = HttpRequest.newBuilder(request.uri)
        .timeout(Duration.ofMillis(request.readTimeoutMs));
    for (Map.Entry<String, String> h : request.headers.entrySet()) {
      builder.header(h.getKey(), h.getValue());
    }
    builder.method(request.method, request.body == null
        ? HttpRequest.BodyPublishers.noBody()
        : HttpRequest.BodyPublishers.ofByteArray(request.body));

    long deadlineMs = request.readTimeoutMs + DNS_DEADLINE_BUFFER_MS;
    CompletableFuture<HttpResponse<InputStream>> future =
        client(request.followRedirects).sendAsync(builder.build(),
            HttpResponse.BodyHandlers.ofInputStream());
    try {
      return new Response(future.get(deadlineMs, TimeUnit.MILLISECONDS), request.readTimeoutMs);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new IOException("Connection deadline exceeded (" + deadlineMs + "ms) for "
          + request.uri + " - likely a stalled DNS resolution or blackholed connect, neither "
          + "bounded by the response timeout", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof java.net.http.HttpTimeoutException) {
        SocketTimeoutException timeout = new SocketTimeoutException(
            "Timed out waiting for response headers from " + request.uri);
        timeout.initCause(cause);
        throw timeout;
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Request failed: " + request.uri, cause);
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while requesting: " + request.uri, e);
    }
  }

  /** A request description; headers are case-insensitive and a repeated name replaces. */
  public static final class Request {
    private final String method;
    private final URI uri;
    private final Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private byte[] body;
    private long readTimeoutMs = 120_000L;
    private boolean followRedirects = true;

    public Request(String method, String url) {
      this.method = method;
      this.uri = URI.create(url);
    }

    public Request setRequestProperty(String name, String value) {
      headers.put(name, value);
      return this;
    }

    /** Returns the header value, or null when not set. */
    public String getRequestProperty(String name) {
      return headers.get(name);
    }

    public Request setBody(byte[] bodyBytes) {
      this.body = bodyBytes;
      return this;
    }

    /** Bounds time to response headers and each stalled body read; see class doc. */
    public Request setReadTimeoutMs(long ms) {
      this.readTimeoutMs = ms;
      return this;
    }

    public Request setFollowRedirects(boolean follow) {
      this.followRedirects = follow;
      return this;
    }
  }

  /**
   * A response whose headers have arrived. Mirrors the {@code HttpURLConnection} calls the ETL
   * code used: status, header lookups, {@link #getInputStream()} for success bodies,
   * {@link #getErrorStream()} for non-2xx bodies, and {@link #disconnect()} to abandon the
   * exchange.
   */
  public static final class Response implements Closeable {
    private final HttpResponse<InputStream> response;
    private final InputStream body;

    Response(HttpResponse<InputStream> response, long readTimeoutMs) {
      this.response = response;
      this.body = new StallGuardInputStream(response.body(), readTimeoutMs);
    }

    public int getResponseCode() {
      return response.statusCode();
    }

    /** The protocol actually negotiated for this exchange. */
    public HttpClient.Version getVersion() {
      return response.version();
    }

    /** First value of a header (case-insensitive), or null when absent. */
    public String getHeaderField(String name) {
      return response.headers().firstValue(name).orElse(null);
    }

    /** All headers; names are lower-case under HTTP/2, so look up case-insensitively. */
    public Map<String, List<String>> getHeaderFields() {
      return response.headers().map();
    }

    /** Header parsed as an HTTP-date to epoch ms, or {@code defaultMs} when absent/unparseable. */
    public long getHeaderFieldDate(String name, long defaultMs) {
      Optional<String> value = response.headers().firstValue(name);
      if (!value.isPresent()) {
        return defaultMs;
      }
      try {
        return ZonedDateTime.parse(value.get().trim(), DateTimeFormatter.RFC_1123_DATE_TIME)
            .toInstant().toEpochMilli();
      // fallback-guard: allow documented defaultMs contract, same as HttpURLConnection.getHeaderFieldDate
      } catch (DateTimeParseException e) {
        return defaultMs;
      }
    }

    /** Content-Length, or -1 when the server did not send one. */
    public long getContentLengthLong() {
      return response.headers().firstValueAsLong("Content-Length").orElse(-1L);
    }

    public String getContentType() {
      return getHeaderField("Content-Type");
    }

    public String getContentEncoding() {
      return getHeaderField("Content-Encoding");
    }

    /**
     * The body of a 2xx/3xx response.
     *
     * @throws IOException if the status is 4xx/5xx (read {@link #getErrorStream()} instead)
     */
    public InputStream getInputStream() throws IOException {
      if (response.statusCode() >= 400) {
        throw new IOException("HTTP " + response.statusCode() + " from " + response.uri());
      }
      return body;
    }

    /** The body of a 4xx/5xx response, or null when the status is not an error. */
    public InputStream getErrorStream() {
      return response.statusCode() >= 400 ? body : null;
    }

    /** Abandons the exchange; an unread body is cancelled rather than drained. */
    public void disconnect() {
      try {
        body.close();
      } catch (IOException e) {
        LOGGER.debug("Error closing response body for {}: {}", response.uri(), e.toString());
      }
    }

    @Override public void close() {
      disconnect();
    }
  }

  /** Per-read stall guard: a read blocked longer than the timeout cancels the exchange. */
  private static final class StallGuardInputStream extends InputStream {
    private final InputStream delegate;
    private final long timeoutMs;
    private volatile boolean timedOut;

    StallGuardInputStream(InputStream delegate, long timeoutMs) {
      this.delegate = delegate;
      this.timeoutMs = timeoutMs;
    }

    private ScheduledFuture<?> arm() {
      return WATCHDOG.schedule(() -> {
        timedOut = true;
        try {
          delegate.close();
        } catch (IOException e) {
          LOGGER.debug("Error cancelling stalled response: {}", e.toString());
        }
      }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private IOException translate(IOException e) {
      if (timedOut) {
        SocketTimeoutException timeout =
            new SocketTimeoutException("Read timed out after " + timeoutMs + "ms");
        timeout.initCause(e);
        return timeout;
      }
      // The JDK reports a transport fault mid-body (truncated content-length, reset) as a generic
      // IOException("closed") wrapping the real failure; surface the real failure so callers
      // classify it by what actually happened.
      if (e.getCause() instanceof IOException) {
        return (IOException) e.getCause();
      }
      return e;
    }

    @Override public int read() throws IOException {
      ScheduledFuture<?> guard = arm();
      try {
        return delegate.read();
      } catch (IOException e) {
        throw translate(e);
      } finally {
        guard.cancel(false);
      }
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      ScheduledFuture<?> guard = arm();
      try {
        return delegate.read(b, off, len);
      } catch (IOException e) {
        throw translate(e);
      } finally {
        guard.cancel(false);
      }
    }

    @Override public int available() throws IOException {
      return delegate.available();
    }

    @Override public void close() throws IOException {
      delegate.close();
    }
  }
}
