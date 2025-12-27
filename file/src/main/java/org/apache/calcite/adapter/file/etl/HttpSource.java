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
  private long lastRequestTime;

  /**
   * Creates a new HttpSource with the given configuration.
   *
   * @param config HTTP source configuration
   */
  public HttpSource(HttpSourceConfig config) {
    this(config, (HooksConfig) null);
  }

  /**
   * Creates a new HttpSource with configuration and hooks.
   *
   * @param config HTTP source configuration
   * @param hooksConfig Optional hooks configuration for response transformation
   */
  public HttpSource(HttpSourceConfig config, HooksConfig hooksConfig) {
    this.config = config;
    this.cache = config.getCache().isEnabled()
        ? new ConcurrentHashMap<String, CacheEntry>()
        : null;
    this.lastRequestTime = 0;
    this.responseTransformer = loadResponseTransformer(hooksConfig);
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
      response = transformResponse(response, url, params, variables);

      // For CSV/TSV with large temp files, use lazy streaming iterator directly
      // This avoids loading entire file into memory
      HttpSourceConfig.ResponseConfig respConfig = config.getResponse();
      if ((respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV
          || respConfig.getFormat() == HttpSourceConfig.ResponseFormat.TSV)
          && response.startsWith(TEMP_FILE_PREFIX)) {
        char delimiter = respConfig.getFormat() == HttpSourceConfig.ResponseFormat.CSV ? ',' : '\t';
        LOGGER.info("Using lazy streaming iterator for large CSV/TSV file");
        // Note: Caching disabled for streaming - data is too large to cache in memory
        return parseDelimitedResponseStreaming(response, delimiter);
      }

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
          return extractFromZip(conn.getInputStream(), extractPattern);
        }
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
   * Marker prefix for temp file paths in response strings.
   * When a response starts with this prefix, the remainder is a temp file path.
   */
  private static final String TEMP_FILE_PREFIX = "TEMP_FILE:";

  /**
   * Threshold for using temp file vs memory (10MB).
   * Files larger than this are extracted to temp files to avoid OOM.
   */
  private static final long TEMP_FILE_THRESHOLD = 10 * 1024 * 1024;

  /**
   * Extracts content from a ZIP archive matching the given pattern.
   *
   * <p>For large files (>10MB), extracts to a temp file and returns a marker string
   * (TEMP_FILE:/path/to/file) to avoid loading entire content into memory.
   * Callers must check for this prefix and handle accordingly.
   *
   * @param input ZIP file input stream
   * @param pattern Glob pattern to match file names (e.g., "*.csv")
   * @return Content of the first matching file, or TEMP_FILE:path for large files
   * @throws IOException if extraction fails or no matching file found
   */
  private String extractFromZip(InputStream input, String pattern) throws IOException {
    LOGGER.debug("Extracting from ZIP with pattern: {}", pattern);

    // Convert glob pattern to regex
    String regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    try (ZipInputStream zis = new ZipInputStream(input)) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        LOGGER.debug("ZIP entry: {}", name);

        // Check if name matches pattern
        if (name.matches(regex) || name.endsWith(pattern.replace("*", ""))) {
          LOGGER.info("Extracting file from ZIP: {}", name);

          // First pass: extract to temp file to get size and avoid OOM
          File tempFile = File.createTempFile("http-source-", ".tmp");
          tempFile.deleteOnExit();

          byte[] buffer = new byte[65536];  // 64KB buffer for better throughput
          int len;
          long totalBytes = 0;
          long lastLogTime = System.currentTimeMillis();

          try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            while ((len = zis.read(buffer)) > 0) {
              fos.write(buffer, 0, len);
              totalBytes += len;
              // Log progress every 5 seconds
              long now = System.currentTimeMillis();
              if (now - lastLogTime > 5000) {
                LOGGER.info("Extracting... {} MB read", totalBytes / (1024 * 1024));
                lastLogTime = now;
              }
            }
          }

          LOGGER.info("Extracted {} MB from ZIP to temp file", totalBytes / (1024 * 1024));

          // For large files, return temp file path marker (caller streams from file)
          if (totalBytes > TEMP_FILE_THRESHOLD) {
            LOGGER.info("Using temp file for large content ({}MB > {}MB threshold)",
                totalBytes / (1024 * 1024), TEMP_FILE_THRESHOLD / (1024 * 1024));
            return TEMP_FILE_PREFIX + tempFile.getAbsolutePath();
          }

          // For small files, read into memory and delete temp file
          try {
            byte[] content = java.nio.file.Files.readAllBytes(tempFile.toPath());
            return new String(content, StandardCharsets.UTF_8);
          } finally {
            tempFile.delete();
          }
        }
        zis.closeEntry();
      }
    }

    throw new IOException("No file matching pattern '" + pattern + "' found in ZIP archive");
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
   * Parses a delimited response (CSV or TSV) returning a lazy iterator.
   *
   * <p>For large files (TEMP_FILE: prefix), returns a lazy iterator that reads one row
   * at a time, avoiding loading entire file into memory. For small in-memory responses,
   * falls back to the list-based implementation.
   *
   * @param response Delimited content or TEMP_FILE: marker
   * @param delimiter The delimiter character (comma for CSV, tab for TSV)
   * @return Iterator over rows, each row is a Map with column names as keys
   * @throws IOException if response contains error content
   */
  private Iterator<Map<String, Object>> parseDelimitedResponseStreaming(String response, char delimiter)
      throws IOException {

    if (response == null || response.isEmpty()) {
      LOGGER.warn("Received empty response body - returning empty iterator");
      return Collections.<Map<String, Object>>emptyList().iterator();
    }

    // For large temp files, use lazy streaming iterator
    if (response.startsWith(TEMP_FILE_PREFIX)) {
      String filePath = response.substring(TEMP_FILE_PREFIX.length());
      File tempFile = new File(filePath);
      LOGGER.info("Creating lazy streaming iterator for temp file: {} ({} MB)",
          filePath, tempFile.length() / (1024 * 1024));
      return new LazyCSVIterator(tempFile, delimiter, config.getRowFilter());
    }

    // For small in-memory responses, use existing list-based parsing
    return parseDelimitedResponse(response, delimiter).iterator();
  }

  /**
   * Lazy iterator that reads CSV rows one at a time from a file.
   * Parses rows on-demand to avoid loading entire file into memory.
   */
  private class LazyCSVIterator implements Iterator<Map<String, Object>>, java.io.Closeable {
    private final File tempFile;
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

    LazyCSVIterator(File tempFile, char delimiter, HttpSourceConfig.RowFilterConfig filter)
        throws IOException {
      this.tempFile = tempFile;
      this.delimiter = delimiter;
      this.reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(tempFile), StandardCharsets.UTF_8));
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
      LOGGER.debug("Parsed {} columns from header", headers.length);

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
      try {
        reader.close();
      } finally {
        if (tempFile != null && tempFile.exists()) {
          boolean deleted = tempFile.delete();
          if (deleted) {
            LOGGER.debug("Deleted temp file: {}", tempFile.getAbsolutePath());
          } else {
            LOGGER.warn("Failed to delete temp file: {}", tempFile.getAbsolutePath());
          }
        }
      }
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

    // Check if response is a temp file marker (for large files)
    File tempFile = null;
    java.io.Reader sourceReader;
    if (response.startsWith(TEMP_FILE_PREFIX)) {
      String filePath = response.substring(TEMP_FILE_PREFIX.length());
      tempFile = new File(filePath);
      LOGGER.info("Streaming from temp file: {} ({} MB)",
          filePath, tempFile.length() / (1024 * 1024));
      sourceReader = new InputStreamReader(new FileInputStream(tempFile), StandardCharsets.UTF_8);
    } else {
      // Check for error-like responses (HTTP 200 with error content)
      // These indicate server returned error page instead of data
      String trimmed = response.trim();

      // Log response info for debugging
      LOGGER.info("Parsing delimited response: {} bytes, first 100 chars: {}",
          response.length(),
          response.length() > 100 ? response.substring(0, 100).replace("\n", "\\n") : response.replace("\n", "\\n"));

      if (trimmed.startsWith("<") || trimmed.startsWith("<!DOCTYPE")) {
        // HTML response - likely an error page
        String preview = trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed;
        throw new IOException("Received HTML instead of tabular data (possible error page): " + preview);
      }
      if (trimmed.toLowerCase().startsWith("access denied")
          || trimmed.toLowerCase().startsWith("error")
          || trimmed.toLowerCase().startsWith("forbidden")) {
        throw new IOException("Received error response: " + trimmed);
      }

      sourceReader = new java.io.StringReader(response);
    }

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

        // Build row map
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

        // Log progress every 10 seconds or 100k lines
        long now = System.currentTimeMillis();
        if (now - lastLogTime > 10000 || lineNumber % 100000 == 0) {
          LOGGER.info("Parsing CSV... {} lines read, {} matched, {} skipped",
              lineNumber, matchedRows, skippedRows);
          lastLogTime = now;
        }
      }

      LOGGER.info("CSV parse complete: {} lines read, {} matched, {} skipped",
          lineNumber, matchedRows, skippedRows);
    } finally {
      // Clean up temp file if used
      if (tempFile != null && tempFile.exists()) {
        boolean deleted = tempFile.delete();
        if (deleted) {
          LOGGER.debug("Deleted temp file: {}", tempFile.getAbsolutePath());
        } else {
          LOGGER.warn("Failed to delete temp file: {}", tempFile.getAbsolutePath());
        }
      }
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
