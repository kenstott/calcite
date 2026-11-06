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
package org.apache.calcite.adapter.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * HTTP client for one Jira site's REST API.
 *
 * <p>Concurrency is capped per site. A 429 is retried only as far as the server's
 * {@code Retry-After} allows and only within a bounded budget (attempts and total wait); after
 * that it is an error carrying the server's message, never a silent wait. Redirects are not
 * followed, so a login page never reaches the JSON parser.
 */
class JiraConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(JiraConnection.class);

  static final ObjectMapper MAPPER = new ObjectMapper();

  /** Pauses between retries; replaced in tests so that they need not wait. */
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final URI baseUrl;
  private final JiraFlavor flavor;
  private final HttpClient http;
  private final Duration requestTimeout;
  private final Semaphore permits;
  private final int maxRetries;
  private final long maxRetryWaitMillis;
  private final Sleeper sleeper;

  JiraConnection(URI baseUrl, JiraFlavor flavor, int maxConcurrentRequests, int maxRetries,
      Duration maxRetryWait, Duration requestTimeout, Sleeper sleeper) {
    this.baseUrl = baseUrl;
    this.flavor = flavor;
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

  JiraFlavor flavor() {
    return flavor;
  }

  URI baseUrl() {
    return baseUrl;
  }

  /** A response: its status and its JSON body, or null when it has none. */
  static final class Reply {
    final int status;
    final @Nullable JsonNode body;

    Reply(int status, @Nullable JsonNode body) {
      this.status = status;
      this.body = body;
    }
  }

  /** GETs {@code <apiBase><path>} with the query parameters (encoded here). */
  JsonNode get(String path, Map<String, String> params) {
    final StringBuilder query = new StringBuilder();
    for (Map.Entry<String, String> param : params.entrySet()) {
      query.append(query.length() == 0 ? '?' : '&')
          .append(encode(param.getKey())).append('=').append(encode(param.getValue()));
    }
    return body(execute("GET", path + query, null, 200), "GET " + path);
  }

  /** POSTs a JSON body, expecting 200, and returns the response body. */
  JsonNode post(String path, JsonNode body) {
    return body(execute("POST", path, body, 200), "POST " + path);
  }

  /**
   * POSTs a JSON body. Any of the {@code accepted} statuses is returned rather than thrown, so a
   * caller can read the body of an answer that reports some elements failed.
   */
  Reply exchange(String method, String path, @Nullable JsonNode body, int... accepted) {
    return execute(method, path, body, accepted);
  }

  /** PUTs a JSON body, expecting 204. */
  void put(String path, JsonNode body) {
    execute("PUT", path, body, 204);
  }

  /** DELETEs, expecting 204. */
  void delete(String path) {
    execute("DELETE", path, null, 204);
  }

  /** Checks the credentials with {@code GET /myself}; a rejection names the account and site. */
  JsonNode myself() {
    return get("/myself", Map.of());
  }

  private static JsonNode body(Reply reply, String what) {
    if (reply.body == null) {
      throw new JiraException(reply.status, what + " answered HTTP " + reply.status
          + " with no body");
    }
    return reply.body;
  }

  private Reply execute(String method, String pathAndQuery, @Nullable JsonNode body,
      int... accepted) {
    final String what = method + " " + flavor.apiBase + pathAndQuery.split("\\?", 2)[0];
    final URI uri = URI.create(trimTrailingSlash(baseUrl.toString()) + flavor.apiBase
        + pathAndQuery);
    final HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
        .timeout(requestTimeout)
        .header("Accept", "application/json");
    if (body == null) {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      builder.header("Content-Type", "application/json")
          .method(method,
              HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
    }
    flavor.authorize(builder);
    final HttpRequest request = builder.build();

    long waitedMillis = 0;
    for (int attempt = 0;; attempt++) {
      final HttpResponse<String> response = send(request, what);
      if (response.statusCode() != 429) {
        return interpret(response, accepted, what);
      }
      final String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
      final String serverMessage = errorMessage(response.body());
      if (attempt >= maxRetries) {
        throw new JiraException(429, what + " was rate limited (HTTP 429) and the "
            + maxRetries + " allowed retries are used up. Server message: " + serverMessage);
      }
      final long waitMillis = retryAfterMillis(retryAfter, what, serverMessage);
      if (waitedMillis + waitMillis > maxRetryWaitMillis) {
        throw new JiraException(429, what + " was rate limited (HTTP 429) and Retry-After of "
            + retryAfter + "s would exceed the retry wait budget of "
            + maxRetryWaitMillis / 1000 + "s (maxRetryWaitSeconds). Server message: "
            + serverMessage);
      }
      LOGGER.warn("{} rate limited; retrying in {}s (retry {} of {})", what, retryAfter,
          attempt + 1, maxRetries);
      try {
        sleeper.sleep(waitMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new JiraException(what + " interrupted while waiting to retry", e);
      }
      waitedMillis += waitMillis;
    }
  }

  private HttpResponse<String> send(HttpRequest request, String what) {
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new JiraException(what + " interrupted while waiting for a request slot", e);
    }
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new JiraException(what + " failed to reach " + baseUrl + ": " + e, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new JiraException(what + " interrupted", e);
    } finally {
      permits.release();
    }
  }

  private static long retryAfterMillis(@Nullable String retryAfter, String what,
      String serverMessage) {
    if (retryAfter == null) {
      throw new JiraException(429, what + " was rate limited (HTTP 429) without a Retry-After "
          + "header, so the adapter cannot tell how long to wait. Server message: "
          + serverMessage);
    }
    try {
      final long seconds = Long.parseLong(retryAfter.trim());
      if (seconds < 0) {
        throw new NumberFormatException("negative");
      }
      return seconds * 1000;
    } catch (NumberFormatException e) {
      throw new JiraException(429, what + " was rate limited (HTTP 429) with a Retry-After "
          + "value that is not a whole number of seconds: '" + retryAfter + "'");
    }
  }

  private Reply interpret(HttpResponse<String> response, int[] accepted, String what) {
    final int status = response.statusCode();
    boolean ok = false;
    for (int a : accepted) {
      ok |= a == status;
    }
    if (!ok) {
      throw new JiraException(status, what + " failed with HTTP " + status
          + (status == 401 ? " (authentication was rejected for " + flavor.identity + " at "
              + baseUrl + ")" : "")
          + (status == 403 ? " (the site refused it: " + flavor.identity
              + " lacks the permission)" : "")
          + ". Server message: " + errorMessage(response.body()));
    }
    final String text = response.body();
    if (text == null || text.isEmpty()) {
      return new Reply(status, null);
    }
    final String contentType = response.headers().firstValue("Content-Type").orElse("");
    if (!contentType.toLowerCase(Locale.ROOT).contains("application/json")) {
      throw new JiraException(status, what + " answered HTTP " + status + " with Content-Type '"
          + contentType + "' instead of JSON; the URL may not be a Jira site. Body starts: "
          + snippet(text));
    }
    try {
      return new Reply(status, MAPPER.readTree(text));
    } catch (JsonProcessingException e) {
      throw new JiraException(status, what + " answered HTTP " + status
          + " with a body that is not valid JSON: " + e.getOriginalMessage());
    }
  }

  /**
   * The text of a Jira error response ({@code errorMessages} and {@code errors}), for the
   * exception message only.
   */
  static String errorMessage(@Nullable String body) {
    if (body == null || body.isEmpty()) {
      return "(empty body)";
    }
    try {
      return errorText(MAPPER.readTree(body));
    } catch (IOException e) {
      return snippet(body);
    }
  }

  /** {@code errorMessages} and {@code errors} of a Jira error object, joined. */
  static String errorText(JsonNode response) {
    final StringBuilder text = new StringBuilder();
    for (JsonNode message : response.path("errorMessages")) {
      text.append(text.length() == 0 ? "" : "; ").append(message.asText());
    }
    final Iterator<Map.Entry<String, JsonNode>> fields = response.path("errors").fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      text.append(text.length() == 0 ? "" : "; ").append(field.getKey()).append(": ")
          .append(field.getValue().asText());
    }
    return text.length() == 0 ? snippet(response.toString()) : text.toString();
  }

  private static String snippet(String body) {
    final String flat = body.replaceAll("\\s+", " ").trim();
    return flat.length() <= 200 ? flat : flat.substring(0, 200) + "...";
  }

  private static String trimTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
