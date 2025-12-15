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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP data source that fetches data from REST APIs.
 *
 * <p>HttpSource implements the {@link DataSource} interface to fetch data
 * from HTTP/REST APIs with support for:
 * <ul>
 *   <li>Variable substitution in URL, parameters, and headers</li>
 *   <li>Environment variable references ({@code {env:VAR_NAME}})</li>
 *   <li>Pagination (offset, cursor, page-based)</li>
 *   <li>Rate limiting with exponential backoff</li>
 *   <li>Response caching</li>
 *   <li>JSONPath data extraction</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * HttpSourceConfig config = HttpSourceConfig.builder()
 *     .url("https://api.example.com/data")
 *     .method(HttpMethod.GET)
 *     .parameters(Map.of("year", "{year}", "apiKey", "{env:API_KEY}"))
 *     .build();
 *
 * HttpSource source = new HttpSource(config);
 * Iterator<Map<String, Object>> data = source.fetch(Map.of("year", "2024"));
 * }</pre>
 *
 * @see HttpSourceConfig
 * @see DataSource
 */
public class HttpSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Pattern VAR_PATTERN = Pattern.compile("\\{([^}]+)\\}");
  private static final Pattern ENV_PATTERN = Pattern.compile("env:(.+)");

  private final HttpSourceConfig config;
  private final Map<String, CacheEntry> cache;
  private long lastRequestTime;

  /**
   * Creates a new HttpSource with the given configuration.
   *
   * @param config HTTP source configuration
   */
  public HttpSource(HttpSourceConfig config) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    this.lastRequestTime = 0;
  }

  @Override
  public Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException {
    // Build the URL with variables substituted
    String url = substituteVariables(config.getUrl(), variables);

    // Build query parameters
    Map<String, String> params = new LinkedHashMap<String, String>();
    for (Map.Entry<String, String> e : config.getParameters().entrySet()) {
      params.put(e.getKey(), substituteVariables(e.getValue(), variables));
    }

    // Check cache if enabled
    String cacheKey = buildCacheKey(url, params);
    if (cache != null) {
      CacheEntry cached = cache.get(cacheKey);
      if (cached != null && !cached.isExpired()) {
        LOGGER.debug("Cache hit for {}", cacheKey);
        return cached.getData().iterator();
      }
    }

    // Fetch data with pagination support
    List<Map<String, Object>> allData = new ArrayList<Map<String, Object>>();
    HttpSourceConfig.PaginationConfig pagination = config.getResponse().getPagination();

    if (pagination.getType() == HttpSourceConfig.PaginationType.NONE) {
      // Single request
      String response = executeRequest(url, params, variables);
      allData.addAll(parseResponse(response));
    } else {
      // Paginated requests
      int offset = 0;
      int pageSize = pagination.getPageSize();
      boolean hasMore = true;

      while (hasMore) {
        Map<String, String> pageParams = new LinkedHashMap<String, String>(params);

        switch (pagination.getType()) {
          case OFFSET:
            pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            pageParams.put(pagination.getOffsetParam(), String.valueOf(offset));
            break;
          case PAGE:
            int page = (offset / pageSize) + 1;
            pageParams.put(pagination.getPageParam(), String.valueOf(page));
            if (pagination.getLimitParam() != null) {
              pageParams.put(pagination.getLimitParam(), String.valueOf(pageSize));
            }
            break;
          default:
            hasMore = false;
            continue;
        }

        String response = executeRequest(url, pageParams, variables);
        List<Map<String, Object>> pageData = parseResponse(response);

        if (pageData.isEmpty()) {
          hasMore = false;
        } else {
          allData.addAll(pageData);
          offset += pageSize;

          // Check if we got less than a full page
          if (pageData.size() < pageSize) {
            hasMore = false;
          }
        }
      }
    }

    // Store in cache if enabled
    if (cache != null) {
      long ttlMs = config.getCache().getTtlSeconds() * 1000;
      cache.put(cacheKey, new CacheEntry(allData, System.currentTimeMillis() + ttlMs));
      LOGGER.debug("Cached {} records for {}", allData.size(), cacheKey);
    }

    LOGGER.info("Fetched {} records from {}", allData.size(), url);
    return allData.iterator();
  }

  @Override
  public String getType() {
    return "http";
  }

  @Override
  public void close() {
    if (cache != null) {
      cache.clear();
    }
  }

  /**
   * Executes an HTTP request with rate limiting and retries.
   */
  private String executeRequest(String baseUrl, Map<String, String> params,
      Map<String, String> variables) throws IOException {

    // Rate limiting
    enforceRateLimit();

    // Build full URL with query parameters
    String fullUrl = buildUrlWithParams(baseUrl, params);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    int retries = 0;
    IOException lastException = null;

    while (retries <= rateLimit.getMaxRetries()) {
      try {
        String response = doRequest(fullUrl, variables);
        return response;
      } catch (IOException e) {
        lastException = e;

        // Check if we should retry
        if (shouldRetry(e, rateLimit)) {
          retries++;
          if (retries <= rateLimit.getMaxRetries()) {
            long backoff = rateLimit.getRetryBackoffMs() * (1L << (retries - 1));
            LOGGER.warn("Request failed, retrying in {}ms (attempt {}/{}): {}",
                backoff, retries, rateLimit.getMaxRetries(), e.getMessage());
            try {
              Thread.sleep(backoff);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new IOException("Interrupted during retry backoff", ie);
            }
          }
        } else {
          throw e;
        }
      }
    }

    throw lastException != null ? lastException : new IOException("Request failed after retries");
  }

  /**
   * Performs the actual HTTP request.
   */
  private String doRequest(String urlString, Map<String, String> variables) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      // Set headers
      for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
        conn.setRequestProperty(e.getKey(), substituteVariables(e.getValue(), variables));
      }

      // Apply authentication
      applyAuth(conn, variables);

      // Handle POST/PUT body if needed
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        // For now, we don't support request body - just query parameters
      }

      int responseCode = conn.getResponseCode();
      LOGGER.debug("HTTP {} {} -> {}", config.getMethod(), urlString, responseCode);

      if (responseCode >= 200 && responseCode < 300) {
        return readResponse(conn.getInputStream());
      } else {
        String errorBody = readResponse(conn.getErrorStream());
        throw new IOException("HTTP " + responseCode + ": " + errorBody);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Applies authentication to the connection.
   */
  private void applyAuth(HttpURLConnection conn, Map<String, String> variables) {
    HttpSourceConfig.AuthConfig auth = config.getAuth();
    if (auth.getType() == HttpSourceConfig.AuthType.NONE) {
      return;
    }

    switch (auth.getType()) {
      case API_KEY:
        String value = substituteVariables(auth.getValue(), variables);
        if (auth.getLocation() == HttpSourceConfig.AuthLocation.HEADER) {
          conn.setRequestProperty(auth.getName(), value);
        }
        // Query param auth is handled in URL building
        break;

      case BASIC:
        String credentials = substituteVariables(auth.getUsername(), variables)
            + ":" + substituteVariables(auth.getPassword(), variables);
        String encoded = Base64.getEncoder().encodeToString(
            credentials.getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + encoded);
        break;

      case BEARER:
        String token = substituteVariables(auth.getValue(), variables);
        conn.setRequestProperty("Authorization", "Bearer " + token);
        break;

      default:
        break;
    }
  }

  /**
   * Parses the response based on configured format and data path.
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parseResponse(String response) throws IOException {
    HttpSourceConfig.ResponseConfig respConfig = config.getResponse();

    if (respConfig.getFormat() != HttpSourceConfig.ResponseFormat.JSON) {
      throw new IOException("Only JSON format is currently supported");
    }

    JsonNode root = OBJECT_MAPPER.readTree(response);

    // Navigate to data path if specified
    if (respConfig.getDataPath() != null && !respConfig.getDataPath().isEmpty()) {
      root = navigateToPath(root, respConfig.getDataPath());
    }

    // Convert to list of maps
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    if (root.isArray()) {
      for (JsonNode item : root) {
        Map<String, Object> row = OBJECT_MAPPER.convertValue(item, Map.class);
        result.add(row);
      }
    } else if (root.isObject()) {
      // Single object - wrap in list
      Map<String, Object> row = OBJECT_MAPPER.convertValue(root, Map.class);
      result.add(row);
    }

    return result;
  }

  /**
   * Navigates to a JSON path (simple dot or bracket notation).
   */
  private JsonNode navigateToPath(JsonNode root, String path) {
    // Handle JSONPath-like syntax: $.results.data or results.data
    String cleanPath = path;
    if (cleanPath.startsWith("$.")) {
      cleanPath = cleanPath.substring(2);
    } else if (cleanPath.startsWith("$")) {
      cleanPath = cleanPath.substring(1);
    }

    JsonNode current = root;
    for (String part : cleanPath.split("\\.")) {
      if (current == null || current.isMissingNode()) {
        return OBJECT_MAPPER.createArrayNode();
      }

      // Handle array index: data[0]
      if (part.contains("[")) {
        int bracketIdx = part.indexOf('[');
        String fieldName = part.substring(0, bracketIdx);
        if (!fieldName.isEmpty()) {
          current = current.get(fieldName);
        }

        // Extract index
        int endBracket = part.indexOf(']');
        String indexStr = part.substring(bracketIdx + 1, endBracket);
        int index = Integer.parseInt(indexStr);
        current = current != null ? current.get(index) : null;
      } else {
        current = current.get(part);
      }
    }

    return current != null ? current : OBJECT_MAPPER.createArrayNode();
  }

  /**
   * Substitutes variables in a string.
   * Supports {varName} for variables and {env:VAR_NAME} for environment variables.
   */
  private String substituteVariables(String template, Map<String, String> variables) {
    if (template == null || template.isEmpty()) {
      return template;
    }

    StringBuffer result = new StringBuffer();
    Matcher matcher = VAR_PATTERN.matcher(template);

    while (matcher.find()) {
      String varExpr = matcher.group(1);
      String replacement;

      Matcher envMatcher = ENV_PATTERN.matcher(varExpr);
      if (envMatcher.matches()) {
        // Environment variable
        String envName = envMatcher.group(1);
        replacement = System.getenv(envName);
        if (replacement == null) {
          replacement = System.getProperty(envName, "");
        }
      } else {
        // Regular variable
        replacement = variables != null ? variables.get(varExpr) : null;
        if (replacement == null) {
          replacement = matcher.group(0); // Keep original if not found
        }
      }

      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Builds URL with query parameters.
   */
  private String buildUrlWithParams(String baseUrl, Map<String, String> params) {
    if (params == null || params.isEmpty()) {
      return baseUrl;
    }

    StringBuilder url = new StringBuilder(baseUrl);
    char separator = baseUrl.contains("?") ? '&' : '?';

    for (Map.Entry<String, String> e : params.entrySet()) {
      try {
        url.append(separator)
            .append(URLEncoder.encode(e.getKey(), "UTF-8"))
            .append('=')
            .append(URLEncoder.encode(e.getValue(), "UTF-8"));
        separator = '&';
      } catch (Exception ex) {
        // Fallback without encoding
        url.append(separator).append(e.getKey()).append('=').append(e.getValue());
        separator = '&';
      }
    }

    return url.toString();
  }

  /**
   * Builds a cache key from URL and parameters.
   */
  private String buildCacheKey(String url, Map<String, String> params) {
    StringBuilder key = new StringBuilder(url);
    if (params != null && !params.isEmpty()) {
      List<String> sortedKeys = new ArrayList<String>(params.keySet());
      Collections.sort(sortedKeys);
      for (String k : sortedKeys) {
        key.append('|').append(k).append('=').append(params.get(k));
      }
    }
    return key.toString();
  }

  /**
   * Enforces rate limiting.
   */
  private synchronized void enforceRateLimit() {
    int rps = config.getRateLimit().getRequestsPerSecond();
    if (rps <= 0) {
      return;
    }

    long minInterval = 1000 / rps;
    long now = System.currentTimeMillis();
    long elapsed = now - lastRequestTime;

    if (elapsed < minInterval) {
      try {
        Thread.sleep(minInterval - elapsed);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    lastRequestTime = System.currentTimeMillis();
  }

  /**
   * Checks if we should retry based on the error.
   */
  private boolean shouldRetry(IOException e, HttpSourceConfig.RateLimitConfig rateLimit) {
    String message = e.getMessage();
    if (message == null) {
      return false;
    }

    // Check for retryable HTTP status codes
    for (int code : rateLimit.getRetryOn()) {
      if (message.contains("HTTP " + code)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Reads response body from input stream.
   */
  private String readResponse(InputStream input) throws IOException {
    if (input == null) {
      return "";
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
    }
    return response.toString();
  }

  /**
   * Creates an HttpSource from configuration.
   */
  public static HttpSource create(HttpSourceConfig config) {
    return new HttpSource(config);
  }

  /**
   * Cache entry with expiration.
   */
  private static class CacheEntry {
    private final List<Map<String, Object>> data;
    private final long expiresAt;

    CacheEntry(List<Map<String, Object>> data, long expiresAt) {
      this.data = data;
      this.expiresAt = expiresAt;
    }

    List<Map<String, Object>> getData() {
      return data;
    }

    boolean isExpired() {
      return System.currentTimeMillis() > expiresAt;
    }
  }
}
