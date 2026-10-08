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
package org.apache.calcite.adapter.servicenow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * HTTP client for one ServiceNow instance's Table API.
 *
 * <p>Every call is a GET. Concurrency is capped per instance, because REST traffic shares a small
 * integration thread pool with every other integration on the customer's instance. A 429 is
 * retried only as far as the server's {@code Retry-After} allows and only within a bounded
 * budget (attempts and total wait); after that it is an error carrying the server's message, never
 * a silent wait.
 */
public class ServiceNowConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceNowConnection.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String TABLE_API = "/api/now/table/";

  /** Pauses between retries; replaced in tests so that they need not wait. */
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** A successful (HTTP 200, JSON) response. */
  public static final class Response {
    public final HttpHeaders headers;
    public final JsonNode body;

    Response(HttpHeaders headers, JsonNode body) {
      this.headers = headers;
      this.body = body;
    }
  }

  private final URI instanceUrl;
  private final ServiceNowAuth auth;
  private final HttpClient http;
  private final Duration requestTimeout;
  private final Semaphore permits;
  private final int maxRetries;
  private final long maxRetryWaitMillis;
  private final Sleeper sleeper;

  ServiceNowConnection(URI instanceUrl, ServiceNowAuth auth, int maxConcurrentRequests,
      int maxRetries, Duration maxRetryWait, Duration requestTimeout) {
    this(instanceUrl, auth, maxConcurrentRequests, maxRetries, maxRetryWait, requestTimeout,
        Thread::sleep);
  }

  ServiceNowConnection(URI instanceUrl, ServiceNowAuth auth, int maxConcurrentRequests,
      int maxRetries, Duration maxRetryWait, Duration requestTimeout, Sleeper sleeper) {
    if (maxConcurrentRequests < 1) {
      throw new IllegalArgumentException(
          "maxConcurrentRequests must be at least 1: " + maxConcurrentRequests);
    }
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries is negative: " + maxRetries);
    }
    if (maxRetryWait.isNegative()) {
      throw new IllegalArgumentException("maxRetryWait is negative: " + maxRetryWait);
    }
    this.instanceUrl = instanceUrl;
    this.auth = auth;
    // Redirects are not followed: a sleeping or misconfigured instance redirects to a login page,
    // and that must surface as an error rather than as HTML parsed as data
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
    this.requestTimeout = requestTimeout;
    this.permits = new Semaphore(maxConcurrentRequests);
    this.maxRetries = maxRetries;
    this.maxRetryWaitMillis = maxRetryWait.toMillis();
    this.sleeper = sleeper;
  }

  /** The instance this connection talks to. */
  public URI getInstanceUrl() {
    return instanceUrl;
  }

  /** Identifies the instance and user, for cache scoping. */
  String scope() {
    return instanceUrl + "|" + auth.identity();
  }

  /**
   * GETs {@code /api/now/table/<table>} with the given query parameters (names and values are
   * encoded here).
   */
  public Response getTable(String table, Map<String, String> params) {
    final StringBuilder query = new StringBuilder();
    for (Map.Entry<String, String> param : params.entrySet()) {
      if (query.length() > 0) {
        query.append('&');
      }
      query.append(encode(param.getKey())).append('=').append(encode(param.getValue()));
    }
    final String pathAndQuery = TABLE_API + table + (query.length() > 0 ? "?" + query : "");
    final URI uri = URI.create(trimTrailingSlash(instanceUrl.toString()) + pathAndQuery);
    return get(uri, "GET " + TABLE_API + table);
  }

  private Response get(URI uri, String what) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
        .timeout(requestTimeout)
        .header("Accept", "application/json")
        .GET();
    auth.authorize(builder);
    final HttpRequest request = builder.build();

    long waitedMillis = 0;
    for (int attempt = 0;; attempt++) {
      final HttpResponse<String> response = send(request, what);
      if (response.statusCode() != 429) {
        return interpret(response, what);
      }
      final String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
      final String serverMessage = errorMessage(response.body());
      if (attempt >= maxRetries) {
        throw new ServiceNowException.RateLimited(what + " was rate limited (HTTP 429) and the "
            + maxRetries + " allowed retries are used up. Server message: " + serverMessage);
      }
      final long waitMillis = parseRetryAfter(retryAfter, what, serverMessage);
      if (waitedMillis + waitMillis > maxRetryWaitMillis) {
        throw new ServiceNowException.RateLimited(what + " was rate limited (HTTP 429) and "
            + "Retry-After of " + retryAfter + "s would exceed the retry wait budget of "
            + maxRetryWaitMillis / 1000 + "s (maxRetryWaitSeconds). Server message: "
            + serverMessage);
      }
      LOGGER.warn("{} rate limited; retrying in {}s (retry {} of {})", what, retryAfter,
          attempt + 1, maxRetries);
      pause(waitMillis, what);
      waitedMillis += waitMillis;
    }
  }

  private HttpResponse<String> send(HttpRequest request, String what) {
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceNowException(what + " interrupted while waiting for a request slot", e);
    }
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new ServiceNowException(what + " failed to reach " + instanceUrl + ": " + e, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceNowException(what + " interrupted", e);
    } finally {
      permits.release();
    }
  }

  private void pause(long millis, String what) {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceNowException(what + " interrupted while waiting to retry", e);
    }
  }

  private static long parseRetryAfter(String retryAfter, String what, String serverMessage) {
    if (retryAfter == null) {
      throw new ServiceNowException.RateLimited(what + " was rate limited (HTTP 429) without a "
          + "Retry-After header, so the adapter cannot tell how long to wait. Server message: "
          + serverMessage);
    }
    try {
      final long seconds = Long.parseLong(retryAfter.trim());
      if (seconds < 0) {
        throw new NumberFormatException("negative");
      }
      return seconds * 1000;
    } catch (NumberFormatException e) {
      throw new ServiceNowException.RateLimited(what + " was rate limited (HTTP 429) with a "
          + "Retry-After value that is not a whole number of seconds: '" + retryAfter + "'");
    }
  }

  private Response interpret(HttpResponse<String> response, String what) {
    final int status = response.statusCode();
    if (status != 200) {
      throw new ServiceNowException(status, what + " failed with HTTP " + status
          + (status == 401 ? " (authentication was rejected for " + auth.identity() + ")" : "")
          + ". Server message: " + errorMessage(response.body()));
    }
    final String contentType = response.headers().firstValue("Content-Type").orElse("");
    if (!contentType.toLowerCase(Locale.ROOT).contains("application/json")) {
      // A hibernating developer instance and a login redirect page both answer 200 with HTML
      throw new ServiceNowException(status, what + " answered HTTP 200 with Content-Type '"
          + contentType + "' instead of JSON. The instance may be asleep or the URL may not be "
          + "a ServiceNow instance. Body starts: " + snippet(response.body()));
    }
    try {
      return new Response(response.headers(), MAPPER.readTree(response.body()));
    } catch (JsonProcessingException e) {
      throw new ServiceNowException(status, what + " answered HTTP 200 with a body that is not "
          + "valid JSON (" + response.body().length() + " characters; a response cut short by "
          + "a transaction quota looks like this): " + e.getOriginalMessage(), e);
    }
  }

  /** Best-effort text of an error response, for the exception message only. */
  private static String errorMessage(String body) {
    if (body == null || body.isEmpty()) {
      return "(empty body)";
    }
    try {
      final JsonNode error = MAPPER.readTree(body).path("error");
      if (error.isObject()) {
        final String message = error.path("message").asText("");
        final String detail = error.path("detail").asText("");
        if (!message.isEmpty() || !detail.isEmpty()) {
          return message + (detail.isEmpty() ? "" : " (" + detail + ")");
        }
      }
    } catch (IOException e) {
      // The body is not JSON; report it as text below
    }
    return snippet(body);
  }

  private static String snippet(String body) {
    final String flat = body.replaceAll("\\s+", " ").trim();
    return flat.length() <= 200 ? flat : flat.substring(0, 200) + "...";
  }

  private static String trimTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
