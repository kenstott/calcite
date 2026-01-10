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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
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
 *   <li>Variable substitution in URL, parameters, headers, and request body</li>
 *   <li>Environment variable references ({@code {env:VAR_NAME}})</li>
 *   <li>POST/PUT request bodies (JSON or form-urlencoded)</li>
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
  private final ResponseTransformer responseTransformer;
  private final StorageProvider storageProvider;
  private final String rawCachePath;
  private long lastRequestTime;

  /**
   * Creates a new HttpSource with the given configuration.
   *
   * @param config HTTP source configuration
   */
  public HttpSource(HttpSourceConfig config) {
    this(config, (HooksConfig) null, null, null);
  }

  /**
   * Creates a new HttpSource with configuration and hooks.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig) {
    this(config, hooksConfig, null, null);
  }

  /**
   * Creates a new HttpSource with configuration, hooks, and storage provider for raw caching.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   * @param storageProvider Storage provider for raw response caching (S3, local, etc.)
   * @param rawCachePath Base path for raw response cache (e.g., s3://bucket/.raw)
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig,
      StorageProvider storageProvider, String rawCachePath) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    this.lastRequestTime = 0;
    this.responseTransformer = loadResponseTransformer(hooksConfig);
    this.storageProvider = storageProvider;
    this.rawCachePath = rawCachePath;
  }

  /**
   * Creates a new HttpSource with configuration and explicit response transformer.
   *
   * @param config HTTP source configuration
   * @param responseTransformer Response transformer instance
   */
  public HttpSource(HttpSourceConfig config, ResponseTransformer responseTransformer) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    this.lastRequestTime = 0;
    this.responseTransformer = responseTransformer;
    this.storageProvider = null;
    this.rawCachePath = null;
  }

  /**
   * Loads a ResponseTransformer from HooksConfig.
   */
  private ResponseTransformer loadResponseTransformer(HooksConfig hooksConfig) {
    if (hooksConfig == null || hooksConfig.getResponseTransformerClass() == null) {
      return null;
    }

    String className = hooksConfig.getResponseTransformerClass();
    try {
      Class<?> clazz = Class.forName(className);
      if (!ResponseTransformer.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement ResponseTransformer");
      }
      return (ResponseTransformer) clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("ResponseTransformer class not found: " + className, e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to instantiate ResponseTransformer: " + className, e);
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException {
    // Check if batching is configured - if so, use batched fetching
    if (config.hasBatching()) {
      return fetchWithBatching(variables);
    }

    // Build the URL with variables substituted
    String url = substituteVariables(config.getUrl(), variables);

    // Build query parameters
    Map<String, String> params = new LinkedHashMap<String, String>();
    for (Map.Entry<String, String> e : config.getParameters().entrySet()) {
      params.put(e.getKey(), substituteVariables(e.getValue(), variables));
    }

    // Check raw cache first (persistent storage-based)
    String rawCacheFilePath = null;
    if (isRawCacheEnabled()) {
      rawCacheFilePath = buildRawCachePath(variables);
      if (hasValidRawCache(rawCacheFilePath)) {
        // For CSV/TSV, stream directly from raw cache to avoid OOM on large files
        HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
        if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
            || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV) {
          char delimiter = respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV ? ',' : '\t';
          LOGGER.info("Streaming CSV from raw cache: {}", rawCacheFilePath);
          return parseDelimitedResponseStreaming(rawCacheFilePath, delimiter);
        }
        // For JSON and other formats, read into memory (typically small)
        String cachedResponse = readRawCache(rawCacheFilePath);
        cachedResponse = transformResponse(cachedResponse, url, params, variables);
        List<Map<String, Object>> data = parseResponse(cachedResponse);
        LOGGER.info("Fetched {} records from raw cache", data.size());
        return data.iterator();
      }
    }

    // Check in-memory cache if enabled
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
      // Single request - response is cached in doRequest, returns cache path
      String cachePath = executeRequest(url, params, variables, rawCacheFilePath);

      // For CSV/TSV, stream directly from cache
      HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
      if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
          || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV) {
        char delimiter = respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV ? ',' : '\t';
        return parseDelimitedResponseStreaming(cachePath, delimiter);
      }

      // For JSON, read from cache, transform, and parse
      String content = readFromCache(cachePath);
      content = transformResponse(content, url, params, variables);
      allData.addAll(parseResponse(content));
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

        String response = executeRequest(url, pageParams, variables, null);  // No raw cache for pages
        response = transformResponse(response, url, pageParams, variables);
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

  @Override public String getType() {
    return "http";
  }

  @Override public void close() {
    if (cache != null) {
      cache.clear();
    }
  }

  /**
   * Fetches data using batching - loads values from a catalog and makes
   * multiple requests, one per batch.
   *
   * @param variables Dimension variables for this batch
   * @return Iterator over all records from all batches
   */
  private Iterator<Map<String, Object>> fetchWithBatching(Map<String, String> variables)
      throws IOException {
    HttpSourceConfig.BatchConfig batching = config.getBatching();
    LOGGER.info("Fetching with batching: field={}, size={}", batching.getField(), batching.getSize());

    // Load all values from the JSON catalog
    List<String> allValues = loadBatchValues(batching.getSource(), batching.getPath());
    LOGGER.info("Loaded {} values from catalog {}", allValues.size(), batching.getSource());

    // Split into batches
    List<List<String>> batches = createBatches(allValues, batching.getSize());
    LOGGER.info("Split into {} batches of up to {} items", batches.size(), batching.getSize());

    // Fetch each batch
    List<Map<String, Object>> allData = new ArrayList<Map<String, Object>>();
    String url = substituteVariables(config.getUrl(), variables);

    for (int i = 0; i < batches.size(); i++) {
      List<String> batch = batches.get(i);
      LOGGER.info("Processing batch {}/{} ({} items)", i + 1, batches.size(), batch.size());

      try {
        // Create a modified body with this batch's values
        Map<String, Object> batchBody = new LinkedHashMap<String, Object>(config.getBody());
        batchBody.put(batching.getField(), batch);

        // Build query parameters
        Map<String, String> params = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> e : config.getParameters().entrySet()) {
          params.put(e.getKey(), substituteVariables(e.getValue(), variables));
        }

        // Execute request with batch body
        String response = executeRequestWithBody(url, params, variables, batchBody);
        response = transformResponse(response, url, params, variables);
        List<Map<String, Object>> batchData = parseResponse(response);

        allData.addAll(batchData);
        LOGGER.debug("Batch {}/{} returned {} records", i + 1, batches.size(), batchData.size());

        // Rate limiting between batches
        if (i < batches.size() - 1 && batching.getDelayMs() > 0) {
          try {
            Thread.sleep(batching.getDelayMs());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during batch delay", e);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Batch {}/{} failed: {}", i + 1, batches.size(), e.getMessage());
        // Continue with remaining batches
      }
    }

    LOGGER.info("Batched fetch complete: {} total records from {} batches",
        allData.size(), batches.size());
    return allData.iterator();
  }

  /**
   * Loads batch values from a JSON catalog resource.
   */
  private List<String> loadBatchValues(String resourcePath, String path) throws IOException {
    return JsonCatalogResolver.resolve(getClass(), resourcePath, path);
  }

  /**
   * Splits a list into batches of the specified size.
   */
  private static <T> List<List<T>> createBatches(List<T> list, int batchSize) {
    List<List<T>> batches = new ArrayList<List<T>>();
    for (int i = 0; i < list.size(); i += batchSize) {
      batches.add(new ArrayList<T>(list.subList(i, Math.min(i + batchSize, list.size()))));
    }
    return batches;
  }

  /**
   * Executes an HTTP request with a specific body (for batching).
   */
  private String executeRequestWithBody(String baseUrl, Map<String, String> params,
      Map<String, String> variables, Map<String, Object> body) throws IOException {
    // Apply rate limiting
    enforceRateLimit();

    // Build URL with query parameters
    StringBuilder urlBuilder = new StringBuilder(baseUrl);
    if (!params.isEmpty()) {
      urlBuilder.append(baseUrl.contains("?") ? "&" : "?");
      boolean first = true;
      for (Map.Entry<String, String> e : params.entrySet()) {
        if (!first) {
          urlBuilder.append("&");
        }
        first = false;
        try {
          urlBuilder.append(URLEncoder.encode(e.getKey(), "UTF-8"));
          urlBuilder.append("=");
          urlBuilder.append(URLEncoder.encode(e.getValue(), "UTF-8"));
        } catch (Exception ex) {
          urlBuilder.append(e.getKey()).append("=").append(e.getValue());
        }
      }
    }

    String urlString = urlBuilder.toString();

    // Retry logic
    int maxRetries = config.getRateLimit().getMaxRetries();
    IOException lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return doRequestWithBody(urlString, variables, body);
      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries) {
          long backoff = config.getRateLimit().getRetryBackoffMs() * (1L << attempt);
          LOGGER.warn("Request failed, retrying in {}ms: {}", backoff, e.getMessage());
          try {
            Thread.sleep(backoff);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
    }

    throw lastException != null ? lastException : new IOException("Request failed after retries");
  }

  /**
   * Performs the actual HTTP request with a specific body.
   */
  private String doRequestWithBody(String urlString, Map<String, String> variables,
      Map<String, Object> body) throws IOException {
    java.net.URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      // Set default User-Agent if not specified (helps avoid bot detection by BLS, etc.)
      if (config.getHeaders().get("User-Agent") == null) {
        conn.setRequestProperty("User-Agent",
            "Apache-Calcite-DataAdapter/1.0 (https://calcite.apache.org; data-analysis-tool)");
      }

      // Set headers
      for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
        conn.setRequestProperty(e.getKey(), substituteVariables(e.getValue(), variables));
      }

      // Apply authentication
      applyAuth(conn, variables);

      // Send body
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        String bodyContent = serializeBody(body, config.getBodyFormat(), variables);
        String contentType = config.getBodyFormat() == HttpSourceConfig.BodyFormat.JSON
            ? "application/json"
            : "application/x-www-form-urlencoded";
        if (conn.getRequestProperty("Content-Type") == null) {
          conn.setRequestProperty("Content-Type", contentType);
        }
        LOGGER.debug("Sending batched body: {} bytes", bodyContent.length());
        try (OutputStream os = conn.getOutputStream()) {
          os.write(bodyContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          os.flush();
        }
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
   * Executes an HTTP request with rate limiting and retries.
   *
   * @param baseUrl Base URL for the request
   * @param params Query parameters
   * @param variables Variable substitution map
   * @param rawCachePath Optional path to write large files directly to cache (null to use temp files)
   */
  private String executeRequest(String baseUrl, Map<String, String> params,
      Map<String, String> variables, String rawCachePath) throws IOException {

    // Rate limiting
    enforceRateLimit();

    // Build full URL with query parameters
    String fullUrl = buildUrlWithParams(baseUrl, params);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    int retries = 0;
    IOException lastException = null;

    while (retries <= rateLimit.getMaxRetries()) {
      try {
        String response = doRequest(fullUrl, variables, rawCachePath);
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
   *
   * @param urlString Full URL to request
   * @param variables Variable substitution map
   * @param rawCachePath Optional path to write large files directly to cache (null to use temp files)
   */
  private String doRequest(String urlString, Map<String, String> variables,
      String rawCachePath) throws IOException {
    URL url = java.net.URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    try {
      conn.setRequestMethod(config.getMethod().name());
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);

      // Set default User-Agent if not specified (helps avoid bot detection by BLS, etc.)
      if (config.getHeaders().get("User-Agent") == null) {
        conn.setRequestProperty("User-Agent",
            "Apache-Calcite-DataAdapter/1.0 (https://calcite.apache.org; data-analysis-tool)");
      }

      // Set headers from config
      for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
        conn.setRequestProperty(e.getKey(), substituteVariables(e.getValue(), variables));
      }

      // Log headers being used for debugging
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("HTTP {} {} with {} custom headers",
            config.getMethod(), urlString, config.getHeaders().size());
        for (Map.Entry<String, String> e : config.getHeaders().entrySet()) {
          LOGGER.debug("  Header: {}={}", e.getKey(),
              e.getKey().toLowerCase().contains("key") ? "[REDACTED]" : e.getValue());
        }
      }

      // Apply authentication
      applyAuth(conn, variables);

      // Handle POST/PUT body if needed
      if (config.getMethod() == HttpSourceConfig.HttpMethod.POST
          || config.getMethod() == HttpSourceConfig.HttpMethod.PUT) {
        conn.setDoOutput(true);
        if (config.hasBody()) {
          String bodyContent = serializeBody(config.getBody(), config.getBodyFormat(), variables);
          // Set Content-Type if not already set
          String contentType = config.getBodyFormat() == HttpSourceConfig.BodyFormat.JSON
              ? "application/json"
              : "application/x-www-form-urlencoded";
          if (conn.getRequestProperty("Content-Type") == null) {
            conn.setRequestProperty("Content-Type", contentType);
          }
          LOGGER.debug("Sending body: {}", bodyContent);
          try (OutputStream os = conn.getOutputStream()) {
            os.write(bodyContent.getBytes(StandardCharsets.UTF_8));
            os.flush();
          }
        }
      }

      int responseCode = conn.getResponseCode();
      LOGGER.debug("HTTP {} {} -> {}", config.getMethod(), urlString, responseCode);

      if (responseCode >= 200 && responseCode < 300) {
        // Check if we need to extract from ZIP
        String extractPattern = config.getExtractPattern();
        if (extractPattern != null && !extractPattern.isEmpty()) {
          return extractFromZip(conn.getInputStream(), extractPattern, rawCachePath);
        }
        // Read response into memory first to check for API-level errors before caching
        String responseBody = readResponse(conn.getInputStream());

        // Check for API-level errors in JSON responses before caching
        HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
        if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.JSON) {
          String apiError = checkForApiError(responseBody, respConfig);
          if (apiError != null) {
            throw new IOException("API error (not cached): " + apiError);
          }
        }

        // No API error - cache the response
        return cacheResponseString(responseBody, rawCachePath);
      } else {
        String errorBody = readResponse(conn.getErrorStream());
        throw new IOException("HTTP " + responseCode + ": " + errorBody);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Caches HTTP response to storage provider.
   *
   * @param input Response input stream
   * @param cachePath Path to write to storage provider
   * @return The cache path
   * @throws IOException if caching fails
   */
  private String cacheResponse(InputStream input, String cachePath) throws IOException {
    String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
    storageProvider.createDirectories(parentPath);
    storageProvider.writeFile(cachePath, input);
    LOGGER.info("Cached response: {}", cachePath);
    return cachePath;
  }

  /**
   * Caches a string response to storage provider.
   *
   * @param response Response content as string
   * @param cachePath Path to write to storage provider
   * @return The cache path
   * @throws IOException if caching fails
   */
  private String cacheResponseString(String response, String cachePath) throws IOException {
    String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
    storageProvider.createDirectories(parentPath);
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    storageProvider.writeFile(cachePath, new java.io.ByteArrayInputStream(bytes));
    LOGGER.info("Cached response: {} ({} bytes)", cachePath, bytes.length);
    return cachePath;
  }

  /**
   * Checks for API-level errors in a JSON response before caching.
   * Returns the error message if an error is found, null otherwise.
   *
   * <p>This prevents caching error responses that would cause repeated failures.
   * Empty data responses (valid "no data" cases) return null and will be cached.
   *
   * @param responseBody The JSON response body
   * @param respConfig Response configuration with optional errorPath
   * @return Error message if API error found, null if response is valid (or empty data)
   */
  private String checkForApiError(String responseBody,
      HttpSourceConfig.ResponseConfig respConfig) {
    try {
      JsonNode root = OBJECT_MAPPER.readTree(responseBody);

      // Check for API errors using errorPath if configured
      if (respConfig.getErrorPath() != null && !respConfig.getErrorPath().isEmpty()) {
        JsonNode errorNode = navigateToPath(root, respConfig.getErrorPath());
        // Skip if error node is missing, null, or an empty array (common API pattern for "no error")
        boolean hasError = errorNode != null && !errorNode.isMissingNode() && !errorNode.isNull()
            && !(errorNode.isArray() && errorNode.size() == 0);
        if (hasError) {
          String errorMessage = errorNode.isTextual()
              ? errorNode.asText()
              : errorNode.toString();

          // Check for "no data" type errors that should be cached as empty results
          String errorLower = errorMessage.toLowerCase();
          if (errorLower.contains("no data") || errorLower.contains("not found")
              || errorLower.contains("parameter_empty") || errorLower.contains("unknown error")) {
            LOGGER.debug("API returned no-data message (will cache): {}", errorMessage);
            return null; // This is valid, cache it
          }

          return errorMessage; // Real API error - don't cache
        }
      }

      return null; // No error found (or no errorPath configured)
    } catch (Exception e) {
      LOGGER.debug("Could not check for API error (treating as valid): {}", e.getMessage());
      return null; // If we can't parse, assume it's valid
    }
  }

  /**
   * Reads content from cache.
   *
   * @param cachePath Path in storage provider
   * @return Content as string
   * @throws IOException if reading fails
   */
  private String readFromCache(String cachePath) throws IOException {
    try (InputStream is = storageProvider.openInputStream(cachePath);
         java.io.Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
      StringBuilder sb = new StringBuilder();
      char[] buffer = new char[8192];
      int len;
      while ((len = reader.read(buffer)) != -1) {
        sb.append(buffer, 0, len);
      }
      return sb.toString();
    }
  }

  /**
   * Extracts content from a ZIP archive and caches it to storage provider.
   *
   * @param input ZIP file input stream
   * @param pattern Glob pattern to match file names (e.g., "*.csv")
   * @param cachePath Path to write file to storage provider
   * @return The cache path
   * @throws IOException if extraction fails or no matching file found
   */
  private String extractFromZip(InputStream input, String pattern, String cachePath)
      throws IOException {
    String regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    try (ZipInputStream zis = new ZipInputStream(input)) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (name.matches(regex) || name.endsWith(pattern.replace("*", ""))) {
          LOGGER.info("Extracting from ZIP: {}", name);

          // Extract to temp file (ZIP streaming requires it)
          File tempFile = File.createTempFile("http-source-", ".tmp");
          tempFile.deleteOnExit();
          long totalBytes = 0;

          try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[65536];
            int len;
            long lastLogTime = System.currentTimeMillis();
            while ((len = zis.read(buffer)) > 0) {
              fos.write(buffer, 0, len);
              totalBytes += len;
              long now = System.currentTimeMillis();
              if (now - lastLogTime > 5000) {
                LOGGER.info("Extracting... {} MB", totalBytes / (1024 * 1024));
                lastLogTime = now;
              }
            }
          }

          // Write to cache
          try (InputStream fis = new FileInputStream(tempFile)) {
            String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
            storageProvider.createDirectories(parentPath);
            storageProvider.writeFile(cachePath, fis);
          }
          tempFile.delete();
          LOGGER.info("Cached {} MB: {}", totalBytes / (1024 * 1024), cachePath);
          return cachePath;
        }
        zis.closeEntry();
      }
    }
    throw new IOException("No file matching pattern '" + pattern + "' found in ZIP");
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
        String encoded =
            Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
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
   * Serializes the request body to a string format.
   *
   * @param body Body map from configuration
   * @param format Body format (JSON or FORM_URLENCODED)
   * @param variables Variables for substitution
   * @return Serialized body string
   */
  private String serializeBody(Map<String, Object> body, HttpSourceConfig.BodyFormat format,
      Map<String, String> variables) {
    // First, substitute variables in all body values
    Map<String, Object> resolvedBody = substituteBodyVariables(body, variables);

    if (format == HttpSourceConfig.BodyFormat.JSON) {
      try {
        return OBJECT_MAPPER.writeValueAsString(resolvedBody);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize body to JSON: " + e.getMessage(), e);
      }
    } else {
      // FORM_URLENCODED
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Map.Entry<String, Object> e : resolvedBody.entrySet()) {
        if (!first) {
          sb.append("&");
        }
        first = false;
        try {
          sb.append(URLEncoder.encode(e.getKey(), "UTF-8"));
          sb.append("=");
          sb.append(URLEncoder.encode(String.valueOf(e.getValue()), "UTF-8"));
        } catch (Exception ex) {
          sb.append(e.getKey()).append("=").append(e.getValue());
        }
      }
      return sb.toString();
    }
  }

  /**
   * Recursively substitutes variables in body values.
   *
   * @param body Original body map
   * @param variables Variables for substitution
   * @return New map with all string values substituted
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> substituteBodyVariables(Map<String, Object> body,
      Map<String, String> variables) {
    Map<String, Object> result = new LinkedHashMap<String, Object>();

    for (Map.Entry<String, Object> e : body.entrySet()) {
      Object value = e.getValue();
      if (value instanceof String) {
        result.put(e.getKey(), substituteVariables((String) value, variables));
      } else if (value instanceof Map) {
        result.put(e.getKey(), substituteBodyVariables((Map<String, Object>) value, variables));
      } else if (value instanceof List) {
        result.put(e.getKey(), substituteListVariables((List<?>) value, variables));
      } else {
        result.put(e.getKey(), value);
      }
    }

    return result;
  }

  /**
   * Substitutes variables in list values.
   */
  @SuppressWarnings("unchecked")
  private List<Object> substituteListVariables(List<?> list, Map<String, String> variables) {
    List<Object> result = new ArrayList<Object>();
    for (Object item : list) {
      if (item instanceof String) {
        result.add(substituteVariables((String) item, variables));
      } else if (item instanceof Map) {
        result.add(substituteBodyVariables((Map<String, Object>) item, variables));
      } else if (item instanceof List) {
        result.add(substituteListVariables((List<?>) item, variables));
      } else {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * Parses the response based on configured format and data path.
   * Checks for API errors using errorPath before extracting data.
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parseResponse(String response) throws IOException {
    HttpSourceConfig.ResponseConfig respConfig = config.getResponse();

    // Handle CSV format
    if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV) {
      return parseDelimitedResponse(response, ',');
    }

    // Handle TSV format
    if (respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV) {
      return parseDelimitedResponse(response, '\t');
    }

    if (respConfig.getFormat() != HttpSourceConfig.ResponseFormat.JSON) {
      throw new IOException("Unsupported response format: " + respConfig.getFormat());
    }

    JsonNode root = OBJECT_MAPPER.readTree(response);

    // Check for API errors using errorPath if configured
    if (respConfig.getErrorPath() != null && !respConfig.getErrorPath().isEmpty()) {
      JsonNode errorNode = navigateToPath(root, respConfig.getErrorPath());
      // Skip if error node is missing, null, or an empty array (common API pattern for "no error")
      boolean hasError = errorNode != null && !errorNode.isMissingNode() && !errorNode.isNull()
          && !(errorNode.isArray() && errorNode.size() == 0);
      if (hasError) {
        // API returned an error in the configured error location
        String errorMessage = errorNode.isTextual()
            ? errorNode.asText()
            : errorNode.toString();

        // Check for "no data" type errors that should return empty results
        // These indicate the parameter combination is invalid, not a real API error
        String errorLower = errorMessage.toLowerCase();
        if (errorLower.contains("no data") || errorLower.contains("not found")
            || errorLower.contains("parameter_empty") || errorLower.contains("unknown error")) {
          LOGGER.debug("API returned no-data error, returning empty result: {}", errorMessage);
          return Collections.emptyList();
        }

        LOGGER.warn("API error at {}: {}", respConfig.getErrorPath(), errorMessage);
        throw new IOException("API error: " + errorMessage);
      }
    }

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
   * Parses cached delimited response (CSV or TSV) returning a lazy iterator.
   *
   * @param cachePath Path to cached file in storage provider
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Iterator over rows, each row is a Map with column names as keys
   * @throws IOException if reading from cache fails
   */
  private Iterator<Map<String, Object>> parseDelimitedResponseStreaming(String cachePath, char delimiter)
      throws IOException {
    LOGGER.info("Streaming from cache: {}", cachePath);
    return new LazyCSVIterator(storageProvider, cachePath, delimiter, config.getRowFilter());
  }

  /**
   * Lazy iterator that reads CSV rows one at a time from storage provider.
   * Parses rows on-demand to avoid loading entire file into memory.
   */
  private class LazyCSVIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final BufferedReader reader;
    private final char delimiter;
    private final String[] headers;
    private final int filterColumnIndex;
    private final java.util.regex.Pattern filterRegex;
    private final int maxRows;

    private Map<String, Object> nextRow;
    private boolean exhausted;
    private int lineNumber;
    private int matchedRows;
    private int skippedRows;
    private long lastLogTime;

    LazyCSVIterator(StorageProvider provider, String cachePath, char delimiter,
        HttpSourceConfig.RowFilterConfig filter) throws IOException {
      this.delimiter = delimiter;
      this.reader = new BufferedReader(
          new InputStreamReader(provider.openInputStream(cachePath), StandardCharsets.UTF_8));
      this.exhausted = false;
      this.lineNumber = 0;
      this.matchedRows = 0;
      this.skippedRows = 0;
      this.lastLogTime = System.currentTimeMillis();

      // Parse header row
      String headerLine = reader.readLine();
      if (headerLine == null) {
        this.headers = new String[0];
        this.filterColumnIndex = -1;
        this.filterRegex = null;
        this.maxRows = 0;
        exhausted = true;
        return;
      }
      this.headers = parseDelimitedLine(headerLine, delimiter);
      LOGGER.debug("Parsed {} columns from header (from cache: {})", headers.length, cachePath);

      // Setup filter
      String filterColumn = filter != null ? filter.getColumn() : null;
      String filterPattern = filter != null ? filter.getPattern() : null;
      this.maxRows = filter != null ? filter.getMaxRows() : 0;
      this.filterRegex = filterPattern != null
          ? java.util.regex.Pattern.compile(filterPattern)
          : null;

      // Find filter column index
      int foundIndex = -1;
      if (filterColumn != null) {
        for (int i = 0; i < headers.length; i++) {
          if (headers[i].trim().equals(filterColumn)) {
            foundIndex = i;
            break;
          }
        }
        if (foundIndex < 0) {
          LOGGER.warn("Filter column '{}' not found in CSV headers", filterColumn);
        }
      }
      this.filterColumnIndex = foundIndex;

      if (filter != null && filter.isEnabled()) {
        LOGGER.info("CSV filter: column={}, pattern={}, maxRows={}",
            filterColumn, filterPattern, maxRows > 0 ? maxRows : "unlimited");
      }

      // Pre-fetch first matching row
      advance();
    }

    private void advance() {
      if (exhausted) {
        return;
      }

      try {
        String line;
        while ((line = reader.readLine()) != null) {
          lineNumber++;
          line = line.trim();
          if (line.isEmpty()) {
            continue;
          }

          String[] values = parseDelimitedLine(line, delimiter);

          // Apply filter if configured
          if (filterColumnIndex >= 0 && filterRegex != null) {
            if (filterColumnIndex >= values.length) {
              skippedRows++;
              continue;
            }
            String filterValue = values[filterColumnIndex].trim();
            if (filterValue.startsWith("\"") && filterValue.endsWith("\"")) {
              filterValue = filterValue.substring(1, filterValue.length() - 1);
            }
            if (!filterRegex.matcher(filterValue).matches()) {
              skippedRows++;
              continue;
            }
          }

          // Build row map
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          for (int j = 0; j < headers.length && j < values.length; j++) {
            String header = headers[j].trim();
            String value = values[j].trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }
            Object parsed = parseValue(value);
            row.put(header, parsed);
          }

          nextRow = row;
          matchedRows++;

          // Check maxRows limit
          if (maxRows > 0 && matchedRows >= maxRows) {
            LOGGER.info("Reached maxRows limit ({}), stopping lazy parse", maxRows);
            exhausted = true;
          }

          // Log progress periodically
          long now = System.currentTimeMillis();
          if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
            LOGGER.info("Lazy CSV... {} lines read, {} matched, {} skipped",
                lineNumber, matchedRows, skippedRows);
            lastLogTime = now;
          }

          return;
        }

        // End of file
        exhausted = true;
        nextRow = null;
        LOGGER.info("Lazy CSV complete: {} lines read, {} matched, {} skipped",
            lineNumber, matchedRows, skippedRows);
        close();

      } catch (IOException e) {
        LOGGER.error("Error reading CSV: {}", e.getMessage());
        exhausted = true;
        nextRow = null;
        try {
          close();
        } catch (IOException ignored) {
          // Already logging the original error
        }
      }
    }

    @Override
    public boolean hasNext() {
      return nextRow != null;
    }

    @Override
    public Map<String, Object> next() {
      if (nextRow == null) {
        throw new java.util.NoSuchElementException();
      }
      Map<String, Object> current = nextRow;
      nextRow = null;
      if (!exhausted) {
        advance();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  /**
   * Parses a delimited response (CSV or TSV) into a list of maps with streaming and optional filtering.
   *
   * <p>Uses streaming to avoid loading entire file into memory.
   * When rowFilter is configured, only matching rows are kept.
   *
   * @param response Delimited content with header row
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return List of maps, one per row, with column names as keys
   * @throws IOException if response contains error content instead of tabular data
   */
  private List<Map<String, Object>> parseDelimitedResponse(String response, char delimiter)
      throws IOException {
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

    if (response == null || response.isEmpty()) {
      LOGGER.warn("Received empty response body - returning 0 records");
      return result;
    }

    // Get reader - parse in-memory content (used for paginated responses)
    java.io.Reader sourceReader = new java.io.StringReader(response);

    // Get filter config if present
    HttpSourceConfig.RowFilterConfig filter = config.getRowFilter();
    String filterColumn = filter != null ? filter.getColumn() : null;
    String filterPattern = filter != null ? filter.getPattern() : null;
    int maxRows = filter != null ? filter.getMaxRows() : 0;
    java.util.regex.Pattern filterRegex = filterPattern != null
        ? java.util.regex.Pattern.compile(filterPattern)
        : null;

    if (filter != null && filter.isEnabled()) {
      LOGGER.info("CSV filter: column={}, pattern={}, maxRows={}",
          filterColumn, filterPattern, maxRows > 0 ? maxRows : "unlimited");
    }

    // Stream through the CSV line by line
    try (BufferedReader reader = new BufferedReader(sourceReader)) {
      // Parse header row
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }

      String[] headers = parseDelimitedLine(headerLine, delimiter);
      LOGGER.debug("Parsed {} columns from header", headers.length);

      // Find filter column index if filtering is enabled
      int filterColumnIndex = -1;
      if (filterColumn != null) {
        for (int i = 0; i < headers.length; i++) {
          if (headers[i].trim().equals(filterColumn)) {
            filterColumnIndex = i;
            break;
          }
        }
        if (filterColumnIndex < 0) {
          LOGGER.warn("Filter column '{}' not found in CSV headers", filterColumn);
        }
      }

      // Wide-to-narrow transformation setup
      HttpSourceConfig.WideToNarrowConfig wideToNarrow = config.getWideToNarrow();
      List<Integer> keyColumnIndices = new ArrayList<Integer>();
      List<Integer> valueColumnIndices = new ArrayList<Integer>();
      List<String> valueColumnNames = new ArrayList<String>();

      if (wideToNarrow != null && wideToNarrow.isEnabled()) {
        // Build index lists for key and value columns
        for (int i = 0; i < headers.length; i++) {
          String header = headers[i].trim();
          if (wideToNarrow.getKeyColumns().contains(header)) {
            keyColumnIndices.add(i);
          } else if (wideToNarrow.isValueColumn(header)) {
            valueColumnIndices.add(i);
            valueColumnNames.add(header);
          }
          // Columns not in keyColumns and not matching valueColumnPattern are skipped
        }
        LOGGER.info("Wide-to-narrow: {} key columns, {} value columns to unpivot",
            keyColumnIndices.size(), valueColumnIndices.size());
      }

      // Parse data rows with streaming
      String line;
      int lineNumber = 0;
      int matchedRows = 0;
      int skippedRows = 0;
      long lastLogTime = System.currentTimeMillis();

      while ((line = reader.readLine()) != null) {
        lineNumber++;
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }

        String[] values = parseDelimitedLine(line, delimiter);

        // Apply filter if configured
        if (filterColumnIndex >= 0 && filterRegex != null) {
          if (filterColumnIndex >= values.length) {
            skippedRows++;
            continue;
          }
          String filterValue = values[filterColumnIndex].trim();
          // Remove quotes if present
          if (filterValue.startsWith("\"") && filterValue.endsWith("\"")) {
            filterValue = filterValue.substring(1, filterValue.length() - 1);
          }
          if (!filterRegex.matcher(filterValue).matches()) {
            skippedRows++;
            continue;
          }
        }

        // Wide-to-narrow transformation: one input row -> N output rows
        if (wideToNarrow != null && wideToNarrow.isEnabled()) {
          // Build base row with key columns
          Map<String, Object> baseRow = new LinkedHashMap<String, Object>();
          for (int idx : keyColumnIndices) {
            if (idx < values.length) {
              String header = headers[idx].trim();
              String value = values[idx].trim();
              if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
              }
              baseRow.put(header, parseValue(value));
            }
          }

          // Create one output row per value column
          for (int i = 0; i < valueColumnIndices.size(); i++) {
            int idx = valueColumnIndices.get(i);
            if (idx < values.length) {
              String valueStr = values[idx].trim();
              if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                valueStr = valueStr.substring(1, valueStr.length() - 1);
              }

              // Skip null/empty values based on config
              if (wideToNarrow.shouldSkipValue(valueStr)) {
                continue;
              }

              Map<String, Object> row = new LinkedHashMap<String, Object>(baseRow);
              row.put(wideToNarrow.getKeyColumnName(), valueColumnNames.get(i));  // e.g., "2020"
              row.put(wideToNarrow.getValueColumnName(), parseValue(valueStr));   // e.g., 12345.0
              result.add(row);
              matchedRows++;

              // Check maxRows limit
              if (maxRows > 0 && matchedRows >= maxRows) {
                LOGGER.info("Reached maxRows limit ({}), stopping CSV parse", maxRows);
                break;
              }
            }
          }
          if (maxRows > 0 && matchedRows >= maxRows) {
            break;
          }
        } else {
          // Standard row parsing (no transformation)
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          for (int j = 0; j < headers.length && j < values.length; j++) {
            String header = headers[j].trim();
            String value = values[j].trim();

            // Remove surrounding quotes if present
            if (value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }

            // Try to parse as number
            Object parsed = parseValue(value);
            row.put(header, parsed);
          }

          result.add(row);
          matchedRows++;

          // Check maxRows limit
          if (maxRows > 0 && matchedRows >= maxRows) {
            LOGGER.info("Reached maxRows limit ({}), stopping CSV parse", maxRows);
            break;
          }
        }

        // Log progress every 10 seconds or 100k lines
        long now = System.currentTimeMillis();
        if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
          LOGGER.info("Parsing CSV... {} lines read, {} output rows",
              lineNumber, matchedRows);
          lastLogTime = now;
        }
      }

      LOGGER.info("CSV parse complete: {} lines read, {} output rows, {} skipped",
          lineNumber, matchedRows, skippedRows);
    }

    return result;
  }

  /**
   * Parses a single delimited line, handling quoted fields.
   *
   * @param line The line to parse
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Array of field values
   */
  private String[] parseDelimitedLine(String line, char delimiter) {
    List<String> fields = new ArrayList<String>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        // Check for escaped quote ("")
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++; // Skip next quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == delimiter && !inQuotes) {
        fields.add(current.toString());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }
    fields.add(current.toString());

    return fields.toArray(new String[0]);
  }

  /**
   * Attempts to parse a string value as a number.
   */
  private Object parseValue(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }

    // Try integer first
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      // Not an integer
    }

    // Try double
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      // Not a double
    }

    // Return as string
    return value;
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
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line).append('\n');
      }
    }
    return response.toString();
  }

  /**
   * Transforms the response using the configured ResponseTransformer.
   *
   * @param response Raw response from HTTP request
   * @param url The request URL
   * @param params The request parameters
   * @param dimensionValues The dimension values used
   * @return Transformed response, or original if no transformer configured
   */
  private String transformResponse(String response, String url, Map<String, String> params,
      Map<String, String> dimensionValues) {
    if (responseTransformer == null) {
      return response;
    }

    // Build request context for the transformer
    RequestContext context = RequestContext.builder()
        .url(url)
        .parameters(params)
        .headers(config.getHeaders())
        .dimensionValues(dimensionValues)
        .build();

    try {
      String transformed = responseTransformer.transform(response, context);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("ResponseTransformer transformed response for {}", url);
      }
      return transformed;
    } catch (RuntimeException e) {
      // ResponseTransformer threw an exception - this is how it signals API errors
      LOGGER.warn("ResponseTransformer threw exception for {}: {}", url, e.getMessage());
      throw e;
    }
  }

  /**
   * Creates an HttpSource from configuration.
   */
  public static HttpSource create(HttpSourceConfig config) {
    return new HttpSource(config);
  }

  /**
   * Creates an HttpSource from configuration with hooks.
   */
  public static HttpSource create(HttpSourceConfig config, HooksConfig hooksConfig) {
    return new HttpSource(config, hooksConfig);
  }

  // --- Raw Response Caching (StorageProvider-based) ---

  /**
   * Checks if raw cache is enabled and available.
   */
  private boolean isRawCacheEnabled() {
    return config.getRawCache().isEnabled()
        && storageProvider != null
        && rawCachePath != null;
  }

  /**
   * Builds the raw cache path for a given set of dimension variables.
   * Path format: {rawCachePath}/{partitionKey}/response.json
   * Example: s3://bucket/.raw/type=regional_income/year=2020/tablename=CAGDP2/response.json
   */
  private String buildRawCachePath(Map<String, String> variables) {
    StringBuilder path = new StringBuilder(rawCachePath);
    if (!rawCachePath.endsWith("/")) {
      path.append("/");
    }

    // Build partition key from sorted variables
    List<String> sortedKeys = new ArrayList<String>(variables.keySet());
    Collections.sort(sortedKeys);
    for (String key : sortedKeys) {
      String value = variables.get(key);
      if (value != null && !value.isEmpty()) {
        path.append(key).append("=").append(sanitizePathComponent(value)).append("/");
      }
    }

    path.append("response.json");
    return path.toString();
  }

  /**
   * Sanitizes a path component by removing or replacing invalid characters.
   */
  private String sanitizePathComponent(String value) {
    // Replace invalid path characters with underscores
    return value.replaceAll("[/\\\\:*?\"<>|]", "_");
  }

  /**
   * Checks if a raw cached response exists and is not expired.
   *
   * @param cachePath Path to the cached response
   * @return true if cache hit, false otherwise
   */
  private boolean hasValidRawCache(String cachePath) {
    try {
      // Immutable data - if cache exists, it's valid
      // Staleness is determined by IncrementalTracker, not by TTL
      if (!storageProvider.exists(cachePath)) {
        LOGGER.debug("Raw cache miss: {}", cachePath);
        return false;
      }
      LOGGER.debug("Raw cache hit: {}", cachePath);
      return true;
    } catch (IOException e) {
      LOGGER.debug("Error checking raw cache: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Reads raw cached response from storage provider.
   *
   * @param cachePath Path to the cached response
   * @return Cached response content
   * @throws IOException if read fails
   */
  private String readRawCache(String cachePath) throws IOException {
    try (InputStream is = storageProvider.openInputStream(cachePath)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int len;
      while ((len = is.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      String content = baos.toString(StandardCharsets.UTF_8.name());
      LOGGER.info("Raw cache hit: {} ({} bytes)", cachePath, content.length());
      return content;
    }
  }

  /**
   * Writes response to raw cache in storage provider.
   *
   * @param cachePath Path to write the cached response
   * @param content Response content to cache
   */
  private void writeRawCache(String cachePath, String content) {
    try {
      // Ensure parent directory exists
      String parentPath = cachePath.substring(0, cachePath.lastIndexOf('/'));
      storageProvider.createDirectories(parentPath);

      // Write content
      storageProvider.writeFile(cachePath, content.getBytes(StandardCharsets.UTF_8));
      LOGGER.info("Raw cache written: {} ({} bytes)", cachePath, content.length());
    } catch (IOException e) {
      LOGGER.warn("Failed to write raw cache: {} - {}", cachePath, e.getMessage());
    }
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
