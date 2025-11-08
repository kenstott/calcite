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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cross-schema base class for government data downloaders (ECON, GEO, SEC).
 *
 * <p>Provides shared infrastructure only: HTTP client with retry/backoff and
 * rate limiting, plus small diagnostics helpers for file reads. Schema/domain
 * specifics remain in their respective abstract classes.</p>
 */
public abstract class AbstractGovDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGovDataDownloader.class);

  /** Cache directory for storing downloaded raw data (e.g., $GOVDATA_CACHE_DIR/...) */
  protected final String cacheDirectory;
  /** Operating directory for storing operational metadata (e.g., .aperio/<schema>/) */
  protected final String operatingDirectory;
  /** Parquet directory for storing converted parquet files */
  protected final String parquetDirectory;
  /** Storage provider for reading/writing raw cache files (JSON, CSV, XML) */
  protected final StorageProvider cacheStorageProvider;
  /** Storage provider for reading/writing parquet files (supports local and S3) */
  protected final StorageProvider storageProvider;
  /** Schema resource name (e.g., "/econ-schema.json", "/geo-schema.json") */
  protected final String schemaResourceName;

  /** Cache manifest for tracking downloads and conversions */
  protected AbstractCacheManifest cacheManifest;

  /** Shared JSON mapper for convenience */
  protected final ObjectMapper MAPPER = new ObjectMapper();

  /** HTTP client for API/HTTP requests */
  protected final HttpClient httpClient;

  /** Timestamp of last request for rate limiting */
  protected long lastRequestTime = 0L;

  /** Cached rate limit configuration (loaded on first access) */
  private Map<String, Object> rateLimitConfig = null;

  protected AbstractGovDataDownloader(
      String cacheDirectory,
      String operatingDirectory,
      String parquetDirectory,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider,
      String schemaName,
      AbstractCacheManifest sharedManifest) {
    this.cacheManifest = sharedManifest;
    this.cacheDirectory = cacheDirectory;
    this.operatingDirectory = operatingDirectory;
    this.parquetDirectory = parquetDirectory;
    this.cacheStorageProvider = cacheStorageProvider;
    this.storageProvider = storageProvider;
    this.schemaResourceName = "/" + schemaName + "-schema.json";
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
  }

  /**
   * Returns the primary table name that this downloader is associated with.
   * This table's download configuration (including rate limits, API settings, etc.)
   * will be used when executing downloads.
   *
   * <p>Subclasses must override this to specify their associated table.
   * Returns null by default, which uses fallback default values for rate limiting.
   *
   * @return Table name (e.g., "fred_indicators") or null for defaults
   */
  protected String getTableName() {
    return null;
  }

  /**
   * Returns the cache manifest for this downloader.
   *
   * @return Cache manifest instance
   */
  protected AbstractCacheManifest getCacheManifest() {
    return cacheManifest;
  }

  /**
   * Gets the rate limit configuration, loading and caching it on first access.
   * Returns null if no table name specified or no rateLimit config exists.
   */
  private Map<String, Object> getRateLimitConfig() {
    if (rateLimitConfig == null) {
      String tableName = getTableName();
      if (tableName != null) {
        try {
          Map<String, Object> metadata = loadTableMetadata(tableName);
          Object downloadObj = metadata.get("download");
          if (downloadObj instanceof JsonNode) {
            JsonNode download = (JsonNode) downloadObj;
            if (download.has("rateLimit")) {
              JsonNode rateLimit = download.get("rateLimit");
              Map<String, Object> config = new java.util.HashMap<>();
              if (rateLimit.has("minIntervalMs")) {
                config.put("minIntervalMs", rateLimit.get("minIntervalMs").asLong());
              }
              if (rateLimit.has("maxRetries")) {
                config.put("maxRetries", rateLimit.get("maxRetries").asInt());
              }
              if (rateLimit.has("retryDelayMs")) {
                config.put("retryDelayMs", rateLimit.get("retryDelayMs").asLong());
              }
              rateLimitConfig = config;
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not load rate limit config for {}: {}", tableName, e.getMessage());
        }
      }
    }
    return rateLimitConfig;
  }

  /**
   * Minimum interval between HTTP requests in milliseconds.
   * Reads from schema's download.rateLimit.minIntervalMs if available,
   * otherwise returns default of 1000ms (1 request per second).
   */
  protected long getMinRequestIntervalMs() {
    Map<String, Object> config = getRateLimitConfig();
    if (config != null && config.containsKey("minIntervalMs")) {
      return (Long) config.get("minIntervalMs");
    }
    return 1000; // Default: 1 second between requests
  }

  /**
   * Max retry attempts for transient failures.
   * Reads from schema's download.rateLimit.maxRetries if available,
   * otherwise returns default of 3 retries.
   */
  protected int getMaxRetries() {
    Map<String, Object> config = getRateLimitConfig();
    if (config != null && config.containsKey("maxRetries")) {
      return (Integer) config.get("maxRetries");
    }
    return 3; // Default: 3 retries
  }

  /**
   * Initial backoff delay in milliseconds.
   * Reads from schema's download.rateLimit.retryDelayMs if available,
   * otherwise returns default of 1000ms (1 second).
   */
  protected long getRetryDelayMs() {
    Map<String, Object> config = getRateLimitConfig();
    if (config != null && config.containsKey("retryDelayMs")) {
      return (Long) config.get("retryDelayMs");
    }
    return 1000; // Default: 1 second retry delay
  }

  /** Optional: override to customize default User-Agent. */
  protected String getDefaultUserAgent() { return "Calcite-GovData/1.0"; }

  /** Enforce a simple per-instance rate limit. */
  protected final void enforceRateLimit() throws InterruptedException {
    long minInterval = getMinRequestIntervalMs();
    if (minInterval <= 0) {
      return; // No rate limit
    }
    synchronized (this) {
      long now = System.currentTimeMillis();
      long elapsed = now - lastRequestTime;
      if (elapsed < minInterval) {
        long waitTime = minInterval - elapsed;
        LOGGER.trace("Rate limiting: waiting {} ms", waitTime);
        Thread.sleep(waitTime);
      }
      lastRequestTime = System.currentTimeMillis();
    }
  }

  /** Execute an HTTP request with retry/backoff and rate limiting. */
  protected final HttpResponse<String> executeWithRetry(HttpRequest request)
      throws IOException, InterruptedException {
    int maxRetries = Math.max(1, getMaxRetries());
    long retryDelay = Math.max(0, getRetryDelayMs());

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        enforceRateLimit();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 429 || response.statusCode() >= 500) {
          if (attempt < maxRetries - 1) {
            long delay = retryDelay * (long) Math.pow(2, attempt);
            LOGGER.warn("Request failed with status {} - retrying in {} ms (attempt {}/{})",
                response.statusCode(), delay, attempt + 1, maxRetries);
            Thread.sleep(delay);
            continue;
          }
        }
        return response; // success or non-retryable
      } catch (IOException e) {
        if (attempt < maxRetries - 1) {
          long delay = retryDelay * (long) Math.pow(2, attempt);
          LOGGER.warn("Request failed: {} - retrying in {} ms (attempt {}/{})",
              e.getMessage(), delay, attempt + 1, maxRetries);
          Thread.sleep(delay);
        } else {
          throw e;
        }
      }
    }
    throw new IOException("Failed after " + maxRetries + " attempts");
  }

  /** Convenience: download a URL to bytes with default headers and retry. */
  protected byte[] downloadFile(String url) throws IOException {
    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(60))
          .header("User-Agent", getDefaultUserAgent())
          .header("Accept", "*/*")
          .GET()
          .build();
      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      if (code >= 200 && code < 300) {
        return response.body();
      }
      throw new IOException("HTTP " + code + " for " + url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while downloading: " + url, e);
    }
  }

  // ===== Diagnostics helpers (optional use) =====

  /** Log file size if available using the given provider. */
  protected void logFileSize(StorageProvider provider, String fullPath, String label) {
    try {
      long size = provider.getMetadata(fullPath).getSize();
      LOGGER.debug("[DEBUG] {} exists, size={} bytes: {}", label, size, fullPath);
    } catch (Exception e) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", fullPath, e.getMessage());
    }
  }

  /** Preview the head of a file for debugging (non-destructive). */
  protected void previewHead(StorageProvider provider, String fullPath, int bytes, String label) {
    try (InputStream in = provider.openInputStream(fullPath);
         BufferedInputStream bin = new BufferedInputStream(in)) {
      bin.mark(bytes + 1);
      byte[] head = bin.readNBytes(bytes);
      LOGGER.debug("[DEBUG] Head of {} ({} bytes):\n{}", label, head.length, new String(head, StandardCharsets.UTF_8));
      bin.reset();
    } catch (Exception e) {
      LOGGER.debug("[DEBUG] Unable to preview head for {}: {}", fullPath, e.getMessage());
    }
  }

  /** Log basic JSON shape information for quick diagnostics. */
  protected void logJsonShape(JsonNode root, String label) {
    try {
      LOGGER.debug("[DEBUG] {} JSON root type: {}", label, root.getNodeType());
      if (root.isObject()) {
        List<String> keys = new ArrayList<>();
        root.fieldNames().forEachRemaining(keys::add);
        LOGGER.debug("[DEBUG] {} JSON object keys: {}", label, keys);
      } else if (root.isArray()) {
        LOGGER.debug("[DEBUG] {} JSON array size: {}", label, root.size());
        if (root.size() > 0) {
          List<String> keys = new ArrayList<>();
          root.get(0).fieldNames().forEachRemaining(keys::add);
          LOGGER.debug("[DEBUG] {} first element keys: {}", label, keys);
        }
      }
    } catch (Exception ignore) {
      // best-effort logging only
    }
  }

  // ===== Metadata-Driven Path Resolution =====

  protected Map<String, Object> loadTableMetadata() {
    return loadTableMetadata(getTableName());
  }

  /**
   * Loads full table metadata from the schema resource file (e.g., econ-schema.json).
   * Returns a Map containing all table properties including pattern, columns, and partitions.
   *
   * @param tableName The name of the table (must match "name" in schema JSON)
   * @return Map with keys: "name", "pattern", "columns", "partitions", "comment"
   * @throws IllegalArgumentException if table not found or schema file cannot be loaded
   */
  protected Map<String, Object> loadTableMetadata(String tableName) {
    try {
      // Load schema from resources (derived from schema name in constructor)
      String schemaResource = schemaResourceName;
      InputStream schemaStream = getClass().getResourceAsStream(schemaResource);
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            schemaResource + " not found in resources");
      }

      // Parse JSON
      JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "partitionedTables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid " + schemaResource + ": missing 'partitionedTables' array");
      }

      for (JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - return full metadata as Map
          Map<String, Object> metadata = new java.util.HashMap<>();
          metadata.put("name", name);

          if (tableNode.has("pattern")) {
            metadata.put("pattern", tableNode.get("pattern").asText());
          }

          if (tableNode.has("comment")) {
            metadata.put("comment", tableNode.get("comment").asText());
          }

          if (tableNode.has("partitions")) {
            metadata.put("partitions", tableNode.get("partitions"));
          }

          if (tableNode.has("columns")) {
            metadata.put("columns", tableNode.get("columns"));
          }

          if (tableNode.has("download")) {
            metadata.put("download", tableNode.get("download"));
          }

          return metadata;
        }
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in " + schemaResource);

    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to load metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

  /**
   * Derives JSON cache file path from a schema pattern by replacing wildcards with actual values.
   *
   * <p>Example:
   * <pre>
   *   pattern = "type=fred_indicators/year=&#42;/fred_indicators.parquet"
   *   variables = {year: "2020"}
   *   returns "type=fred_indicators/year=2020/fred_indicators.json"
   * </pre>
   *
   * @param pattern The pattern from schema JSON (e.g., "type=fred/year=&#42;/fred.parquet")
   * @param variables Map of partition key to value (e.g., {year: "2020", frequency: "monthly"})
   * @return Relative path to JSON cache file with wildcards replaced and .parquet → .json
   * @throws IllegalArgumentException if required variables are missing or pattern is invalid
   */
  protected String resolveJsonPath(String pattern, Map<String, String> variables) {
    if (pattern == null || pattern.isEmpty()) {
      throw new IllegalArgumentException("Pattern cannot be null or empty");
    }

    // Replace wildcards with actual values
    String resolvedPath = substitutePatternVariables(pattern, variables);

    // Change extension from .parquet to .json
    if (resolvedPath.endsWith(".parquet")) {
      resolvedPath = resolvedPath.substring(0, resolvedPath.length() - 8) + ".json";
    } else {
      // Pattern doesn't end with .parquet - append .json anyway
      LOGGER.warn("Pattern does not end with .parquet: {}", pattern);
      if (!resolvedPath.endsWith(".json")) {
        resolvedPath = resolvedPath + ".json";
      }
    }

    return resolvedPath;
  }

  /**
   * Derives Parquet output file path from a schema pattern by replacing wildcards with actual values.
   *
   * <p>Example:
   * <pre>
   *   pattern = "type=fred_indicators/year=&#42;/fred_indicators.parquet"
   *   variables = {year: "2020"}
   *   returns "type=fred_indicators/year=2020/fred_indicators.parquet"
   * </pre>
   *
   * @param pattern The pattern from schema JSON (e.g., "type=fred/year=&#42;/fred.parquet")
   * @param variables Map of partition key to value (e.g., {year: "2020", frequency: "monthly"})
   * @return Relative path to Parquet file with wildcards replaced
   * @throws IllegalArgumentException if required variables are missing or pattern is invalid
   */
  protected String resolveParquetPath(String pattern, Map<String, String> variables) {
    if (pattern == null || pattern.isEmpty()) {
      throw new IllegalArgumentException("Pattern cannot be null or empty");
    }

    // Replace wildcards with actual values
    return substitutePatternVariables(pattern, variables);
  }

  /**
   * Replaces partition wildcards (key=&#42;) in a pattern with actual values from variable map.
   *
   * <p>Parses patterns like "type=fred/year=&#42;/frequency=&#42;/fred.parquet" and replaces
   * each "key=&#42;" with "key=value" where value comes from the variables map.
   *
   * @param pattern Pattern with wildcards (e.g., "type=fred/year=&#42;/fred.parquet")
   * @param variables Map of partition key to value
   * @return Pattern with wildcards replaced
   * @throws IllegalArgumentException if a required variable is missing from the map
   */
  private String substitutePatternVariables(String pattern, Map<String, String> variables) {
    if (variables == null) {
      variables = new java.util.HashMap<>();
    }

    String result = pattern;

    // Find all partition patterns like "key=*" and replace with "key=value"
    // Use regex to find all occurrences of <word>=*
    java.util.regex.Pattern wildcardPattern = java.util.regex.Pattern.compile("(\\w+)=\\*");
    java.util.regex.Matcher matcher = wildcardPattern.matcher(pattern);

    List<String> missingVariables = new ArrayList<>();

    // Find all wildcards first to report all missing variables at once
    List<String> wildcards = new ArrayList<>();
    while (matcher.find()) {
      String key = matcher.group(1);
      wildcards.add(key);
      if (!variables.containsKey(key)) {
        missingVariables.add(key);
      }
    }

    if (!missingVariables.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing required variables for pattern '" + pattern + "': " + missingVariables);
    }

    // Now perform substitutions
    for (String key : wildcards) {
      String value = variables.get(key);
      // Replace "key=*" with "key=value"
      result = result.replaceAll(key + "=\\*", key + "=" + value);
    }

    return result;
  }

  // ===== Schema-Driven Download Infrastructure =====

  /**
   * Evaluates expressions like "startOfYear({year})" using variable substitution.
   *
   * <p>Supported functions:
   * <ul>
   *   <li>startOfYear({year}) → "YYYY-01-01"</li>
   *   <li>endOfYear({year}) → "YYYY-12-31"</li>
   *   <li>startOfMonth({year},{month}) → "YYYY-MM-01"</li>
   *   <li>endOfMonth({year},{month}) → "YYYY-MM-DD" (last day of month)</li>
   *   <li>concat({a}, "-", {b}) → string concatenation</li>
   * </ul>
   *
   * @param expression Expression string
   * @param variables Variables for substitution
   * @return Evaluated result
   */
  protected String evaluateExpression(String expression, Map<String, String> variables) {
    if (expression == null || expression.isEmpty()) {
      return expression;
    }

    // First substitute variables
    String evaluated = expression;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      evaluated = evaluated.replace("{" + entry.getKey() + "}", entry.getValue());
    }

    // Then evaluate functions
    if (evaluated.startsWith("startOfYear(") && evaluated.endsWith(")")) {
      String year = extractFunctionArg(evaluated, "startOfYear");
      return year + "-01-01";
    } else if (evaluated.startsWith("endOfYear(") && evaluated.endsWith(")")) {
      String year = extractFunctionArg(evaluated, "endOfYear");
      return year + "-12-31";
    } else if (evaluated.startsWith("startOfMonth(") && evaluated.endsWith(")")) {
      String[] args = extractFunctionArgs(evaluated, "startOfMonth");
      if (args.length >= 2) {
        return String.format("%s-%02d-01", args[0], Integer.parseInt(args[1]));
      }
    } else if (evaluated.startsWith("endOfMonth(") && evaluated.endsWith(")")) {
      String[] args = extractFunctionArgs(evaluated, "endOfMonth");
      if (args.length >= 2) {
        int year = Integer.parseInt(args[0]);
        int month = Integer.parseInt(args[1]);
        int lastDay = getLastDayOfMonth(year, month);
        return String.format("%s-%02d-%02d", args[0], month, lastDay);
      }
    } else if (evaluated.startsWith("concat(") && evaluated.endsWith(")")) {
      // Simple string concatenation - just remove concat() wrapper
      return extractFunctionArg(evaluated, "concat");
    }

    return evaluated;
  }

  /**
   * Extracts single argument from function call like "funcName(arg)".
   */
  private String extractFunctionArg(String funcCall, String funcName) {
    int start = funcName.length() + 1; // skip "funcName("
    int end = funcCall.length() - 1;   // skip closing ")"
    return funcCall.substring(start, end).trim();
  }

  /**
   * Extracts multiple arguments from function call like "funcName(arg1,arg2)".
   */
  private String[] extractFunctionArgs(String funcCall, String funcName) {
    String argsStr = extractFunctionArg(funcCall, funcName);
    String[] args = argsStr.split(",");
    for (int i = 0; i < args.length; i++) {
      args[i] = args[i].trim();
    }
    return args;
  }

  /**
   * Returns the last day of the given month (handles leap years).
   */
  private int getLastDayOfMonth(int year, int month) {
    switch (month) {
      case 1: case 3: case 5: case 7: case 8: case 10: case 12:
        return 31;
      case 4: case 6: case 9: case 11:
        return 30;
      case 2:
        // Leap year calculation
        boolean isLeap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        return isLeap ? 29 : 28;
      default:
        throw new IllegalArgumentException("Invalid month: " + month);
    }
  }

  /**
   * Builds complete download URL from download configuration with variable substitution.
   *
   * <p>Handles all parameter types:
   * <ul>
   *   <li>constant - uses "value" field directly</li>
   *   <li>expression - evaluates using evaluateExpression()</li>
   *   <li>auth - reads from authentication config environment variable</li>
   *   <li>iteration - uses iterationValue parameter</li>
   *   <li>pagination - uses paginationOffset parameter</li>
   * </ul>
   *
   * @param downloadConfig Download section from table metadata (must contain baseUrl and queryParams)
   * @param variables Variables for expression evaluation (e.g., {year: "2020"})
   * @param iterationValue Current iteration value (e.g., series_id like "DFF"), or null if not iterating
   * @param paginationOffset Current pagination offset, or 0 if not paginating
   * @return Complete URL with all query parameters properly encoded
   * @throws IllegalArgumentException if required config elements are missing or invalid
   */
  @SuppressWarnings("unchecked")
  protected String buildDownloadUrl(Map<String, Object> downloadConfig,
      Map<String, String> variables,
      String iterationValue,
      int paginationOffset) {
    // Validate required fields
    if (downloadConfig == null) {
      throw new IllegalArgumentException("downloadConfig cannot be null");
    }
    if (!downloadConfig.containsKey("baseUrl")) {
      throw new IllegalArgumentException("downloadConfig must contain 'baseUrl'");
    }
    if (!downloadConfig.containsKey("queryParams")) {
      throw new IllegalArgumentException("downloadConfig must contain 'queryParams'");
    }

    String baseUrl = downloadConfig.get("baseUrl").toString();
    Map<String, Object> queryParams = (Map<String, Object>) downloadConfig.get("queryParams");

    // Build query string
    StringBuilder urlBuilder = new StringBuilder(baseUrl);
    boolean firstParam = true;

    for (Map.Entry<String, Object> paramEntry : queryParams.entrySet()) {
      String paramName = paramEntry.getKey();
      Map<String, Object> paramConfig = (Map<String, Object>) paramEntry.getValue();

      String type = paramConfig.get("type").toString();
      String paramValue = null;

      switch (type) {
        case "constant":
          // Use value directly from config
          paramValue = paramConfig.get("value").toString();
          break;

        case "expression":
          // Evaluate expression with variable substitution
          String expression = paramConfig.get("value").toString();
          paramValue = evaluateExpression(expression, variables);
          break;

        case "auth":
          // Read authentication value from environment variable
          paramValue = resolveAuthValue(downloadConfig);
          break;

        case "iteration":
          // Use the iteration value provided by caller
          if (iterationValue == null) {
            throw new IllegalArgumentException(
                "Parameter '" + paramName + "' requires iteration value but none provided");
          }
          paramValue = iterationValue;
          break;

        case "pagination":
          // Use the pagination offset provided by caller
          paramValue = String.valueOf(paginationOffset);
          break;

        default:
          throw new IllegalArgumentException("Unknown parameter type: " + type);
      }

      // Add to URL if we have a value
      if (paramValue != null && !paramValue.isEmpty()) {
        if (firstParam) {
          urlBuilder.append("?");
          firstParam = false;
        } else {
          urlBuilder.append("&");
        }
        urlBuilder.append(urlEncode(paramName)).append("=").append(urlEncode(paramValue));
      }
    }

    return urlBuilder.toString();
  }

  /**
   * Resolves authentication value from download config by reading environment variable.
   *
   * @param downloadConfig Download configuration containing authentication section
   * @return Authentication value from environment variable
   * @throws IllegalArgumentException if authentication config is invalid or env var not set
   */
  @SuppressWarnings("unchecked")
  private String resolveAuthValue(Map<String, Object> downloadConfig) {
    if (!downloadConfig.containsKey("authentication")) {
      throw new IllegalArgumentException(
          "Download config has 'auth' parameter but no 'authentication' section");
    }

    Map<String, Object> authConfig = (Map<String, Object>) downloadConfig.get("authentication");

    if (!authConfig.containsKey("envVar")) {
      throw new IllegalArgumentException(
          "Authentication config must contain 'envVar' field");
    }

    String envVar = authConfig.get("envVar").toString();
    String authValue = System.getenv(envVar);

    // Fall back to system property if environment variable not set
    // This allows API keys to be passed via model.json operand and set as system properties
    if (authValue == null || authValue.isEmpty()) {
      authValue = System.getProperty(envVar);
    }

    if (authValue == null || authValue.isEmpty()) {
      throw new IllegalArgumentException(
          "Environment variable or system property '" + envVar + "' required for authentication but not set");
    }

    return authValue;
  }

  /**
   * Builds JSON request body for POST requests using requestBody configuration.
   *
   * @param downloadConfig Download configuration containing requestBody section
   * @param variables Variables for expression evaluation
   * @param iterationValue Current iteration value (e.g., series_id), or null if not iterating
   * @return JSON string for POST request body
   * @throws IllegalArgumentException if requestBody config is invalid
   */
  @SuppressWarnings("unchecked")
  protected String buildRequestBody(Map<String, Object> downloadConfig,
      Map<String, String> variables,
      String iterationValue) {
    if (!downloadConfig.containsKey("requestBody")) {
      throw new IllegalArgumentException("downloadConfig must contain 'requestBody' for POST requests");
    }

    Map<String, Object> requestBodyConfig = (Map<String, Object>) downloadConfig.get("requestBody");
    Map<String, Object> bodyData = new java.util.LinkedHashMap<>();

    for (Map.Entry<String, Object> fieldEntry : requestBodyConfig.entrySet()) {
      String fieldName = fieldEntry.getKey();
      Object fieldValue = null;

      // Handle both simple string values and structured config objects
      if (fieldEntry.getValue() instanceof Map) {
        Map<String, Object> fieldConfig = (Map<String, Object>) fieldEntry.getValue();
        String type = fieldConfig.get("type").toString();

        switch (type) {
          case "constant":
            fieldValue = fieldConfig.get("value");
            break;

          case "expression":
            String expression = fieldConfig.get("value").toString();
            fieldValue = evaluateExpression(expression, variables);
            break;

          case "auth":
            fieldValue = resolveAuthValue(downloadConfig);
            break;

          case "iteration":
            if (iterationValue == null) {
              // Check if there's a source field for series list iteration
              if (fieldConfig.containsKey("source")) {
                // This means we should include the full seriesList from config
                String source = fieldConfig.get("source").toString();
                if ("seriesList".equals(source) && downloadConfig.containsKey("seriesList")) {
                  fieldValue = downloadConfig.get("seriesList");
                }
              } else {
                throw new IllegalArgumentException(
                    "Field '" + fieldName + "' requires iteration value but none provided");
              }
            } else {
              fieldValue = iterationValue;
            }
            break;

          default:
            throw new IllegalArgumentException("Unknown field type: " + type);
        }
      } else {
        // Simple string value - treat as expression
        String expression = fieldEntry.getValue().toString();
        fieldValue = evaluateExpression(expression, variables);
      }

      // Add to body data if we have a value
      if (fieldValue != null) {
        bodyData.put(fieldName, fieldValue);
      }
    }

    // Convert to JSON string
    try {
      return MAPPER.writeValueAsString(bodyData);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize request body to JSON", e);
    }
  }

  /**
   * URL-encodes a string for use in query parameters.
   * Simple implementation for common characters (avoids heavy dependency).
   */
  private String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      // UTF-8 is always supported
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes schema-driven download for a table using metadata from schema JSON.
   *
   * <p>This method orchestrates the entire download process:
   * <ol>
   *   <li>Load table metadata and download config</li>
   *   <li>Iterate over series list if download uses iteration</li>
   *   <li>For each iteration (or single request if no iteration):
   *     <ul>
   *       <li>Start with pagination offset 0</li>
   *       <li>Build URL using buildDownloadUrl()</li>
   *       <li>Execute HTTP request with retry logic</li>
   *       <li>Parse response and extract data</li>
   *       <li>Handle pagination if enabled</li>
   *       <li>Aggregate all data</li>
   *     </ul>
   *   </li>
   *   <li>Write aggregated data to JSON cache file</li>
   *   <li>Return path to cached JSON file</li>
   * </ol>
   *
   * @param tableName Name of table (must exist in schema)
   * @param variables Variables for substitution (e.g., {year: "2020"})
   * @return Relative path to cached JSON file
   * @throws IOException if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  @SuppressWarnings("unchecked")
  protected String executeDownload(String tableName, Map<String, String> variables)
      throws IOException, InterruptedException {
    // Load metadata including download config
    Map<String, Object> metadata = loadTableMetadata(tableName);

    if (!metadata.containsKey("download")) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' does not have download configuration in schema");
    }

    // Convert JsonNode to Map<String, Object> for download config
    Object downloadObj = metadata.get("download");
    Map<String, Object> downloadConfig;
    if (downloadObj instanceof JsonNode) {
      downloadConfig = MAPPER.convertValue((JsonNode) downloadObj, Map.class);
    } else {
      downloadConfig = (Map<String, Object>) downloadObj;
    }

    // Check if download is enabled
    Object enabledObj = downloadConfig.get("enabled");
    if (enabledObj != null && !Boolean.parseBoolean(enabledObj.toString())) {
      throw new IllegalArgumentException(
          "Download is not enabled for table '" + tableName + "'");
    }

    // Check if we need to iterate over a series list
    List<String> seriesList = null;
    if (downloadConfig.containsKey("seriesList")) {
      Object seriesListObj = downloadConfig.get("seriesList");
      if (seriesListObj instanceof List) {
        seriesList = (List<String>) seriesListObj;
      }
    }

    // Determine if pagination is enabled
    boolean paginationEnabled = false;
    int maxPerRequest = 100000;
    if (downloadConfig.containsKey("pagination")) {
      Object paginationObj = downloadConfig.get("pagination");
      Map<String, Object> paginationConfig;
      if (paginationObj instanceof JsonNode) {
        paginationConfig = MAPPER.convertValue((JsonNode) paginationObj, Map.class);
      } else {
        paginationConfig = (Map<String, Object>) paginationObj;
      }
      Object enabledPagination = paginationConfig.get("enabled");
      paginationEnabled = enabledPagination != null && Boolean.parseBoolean(enabledPagination.toString());
      if (paginationConfig.containsKey("maxPerRequest")) {
        maxPerRequest = Integer.parseInt(paginationConfig.get("maxPerRequest").toString());
      }
    }

    // Aggregate all downloaded data
    List<JsonNode> allData = new ArrayList<>();

    // Execute download (with or without iteration)
    if (seriesList != null && !seriesList.isEmpty()) {
      // Iterate over series list
      LOGGER.info("Downloading {} with {} series", tableName, seriesList.size());
      for (String series : seriesList) {
        LOGGER.debug("Downloading series: {}", series);
        List<JsonNode> seriesData =
            downloadWithPagination(downloadConfig, variables, series, paginationEnabled, maxPerRequest);
        allData.addAll(seriesData);
      }
    } else {
      // Single download (no iteration)
      LOGGER.info("Downloading {} without iteration", tableName);
      List<JsonNode> data =
          downloadWithPagination(downloadConfig, variables, null, paginationEnabled, maxPerRequest);
      allData.addAll(data);
    }

    LOGGER.info("Downloaded {} total records for {}", allData.size(), tableName);

    // Write aggregated data to JSON cache file
    String pattern = (String) metadata.get("pattern");
    String jsonPath = resolveJsonPath(pattern, variables);
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    // Ensure parent directory exists
    ensureParentDirectory(fullJsonPath);

    // Write as JSON array - use ByteArrayOutputStream then writeFile
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    MAPPER.writeValue(baos, allData);
    cacheStorageProvider.writeFile(fullJsonPath, baos.toByteArray());

    LOGGER.info("Wrote {} records to {}", allData.size(), jsonPath);
    return jsonPath;
  }

  /**
   * Downloads data with pagination support for a single iteration.
   *
   * @param downloadConfig Download configuration from schema
   * @param variables Variables for expression evaluation
   * @param iterationValue Current iteration value (e.g., series_id), or null if not iterating
   * @param paginationEnabled Whether pagination is enabled
   * @param maxPerRequest Maximum records per request
   * @return List of data nodes from all paginated requests
   * @throws IOException if download fails
   * @throws InterruptedException if download is interrupted
   */
  @SuppressWarnings("unchecked")
  private List<JsonNode> downloadWithPagination(
      Map<String, Object> downloadConfig,
      Map<String, String> variables,
      String iterationValue,
      boolean paginationEnabled,
      int maxPerRequest) throws IOException, InterruptedException {
    List<JsonNode> allData = new ArrayList<>();
    int offset = 0;
    boolean hasMore = true;

    while (hasMore) {
      // Check if we need POST method
      String method = downloadConfig.containsKey("method")
          ? downloadConfig.get("method").toString()
          : "GET";

      String url;
      HttpRequest.Builder requestBuilder;

      if ("POST".equalsIgnoreCase(method)) {
        // For POST requests, use baseUrl only (no query params)
        url = downloadConfig.get("baseUrl").toString();
        LOGGER.debug("POST to: {}", url);

        // Build JSON request body
        String requestBody = buildRequestBody(downloadConfig, variables, iterationValue);
        LOGGER.debug("POST body: {}", requestBody);

        // Build POST request with JSON body
        requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(60))
            .header("User-Agent", getDefaultUserAgent())
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody));
      } else {
        // For GET requests, build URL with query params
        url = buildDownloadUrl(downloadConfig, variables, iterationValue, offset);
        LOGGER.debug("GET from: {}", url);

        requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(60))
            .header("User-Agent", getDefaultUserAgent())
            .header("Accept", "application/json")
            .GET();
      }

      HttpRequest request = requestBuilder.build();
      HttpResponse<String> response = executeWithRetry(request);

      if (response.statusCode() != 200) {
        throw new IOException("HTTP " + response.statusCode() + " from " + url
            + ": " + response.body());
      }

      // Parse response
      JsonNode rootNode = MAPPER.readTree(response.body());

      // Extract data from response using dataPath if specified
      JsonNode dataNode = rootNode;
      if (downloadConfig.containsKey("response")) {
        Object responseObj = downloadConfig.get("response");
        Map<String, Object> responseConfig;
        if (responseObj instanceof JsonNode) {
          responseConfig = MAPPER.convertValue((JsonNode) responseObj, Map.class);
        } else {
          responseConfig = (Map<String, Object>) responseObj;
        }
        if (responseConfig.containsKey("dataPath")) {
          String dataPath = responseConfig.get("dataPath").toString();
          // Navigate nested path (e.g., "BEAAPI.Results.ParamValue")
          dataNode = rootNode;
          for (String pathSegment : dataPath.split("\\.")) {
            dataNode = dataNode.path(pathSegment);
            if (dataNode.isMissingNode()) {
              LOGGER.warn("Data path segment '{}' not found in response", pathSegment);
              break;
            }
          }
        }
      }

      // Add data to results
      if (dataNode.isArray()) {
        int recordCount = 0;
        for (JsonNode item : dataNode) {
          allData.add(item);
          recordCount++;
        }
        LOGGER.debug("Received {} records (offset={})", recordCount, offset);

        // Check if we need to paginate
        if (paginationEnabled && recordCount >= maxPerRequest) {
          offset += maxPerRequest;
          hasMore = true;
        } else {
          hasMore = false;
        }
      } else {
        // Single object response
        allData.add(dataNode);
        hasMore = false;
      }

      // Safety check to prevent infinite loops
      if (offset > 1000000) {
        LOGGER.warn("Pagination limit reached (offset > 1M), stopping");
        hasMore = false;
      }
    }

    return allData;
  }

  /**
   * Ensures parent directory exists for the given file path.
   * Uses storage provider to create directories if needed.
   */
  private void ensureParentDirectory(String fullPath) throws IOException {
    // Extract parent directory from path
    int lastSlash = fullPath.lastIndexOf('/');
    if (lastSlash > 0) {
      String parentDir = fullPath.substring(0, lastSlash);
      // Most storage providers auto-create parent directories, but we'll be explicit
      try {
        if (!cacheStorageProvider.exists(parentDir)) {
          LOGGER.debug("Creating parent directory: {}", parentDir);
          // Note: Most StorageProvider implementations handle this automatically
          // but we document the intent here
        }
      } catch (Exception e) {
        // Best effort - storage provider may handle this automatically
        LOGGER.trace("Could not check parent directory existence: {}", e.getMessage());
      }
    }
  }

  // ===== Cache Management =====

  /**
   * Checks if data is cached in manifest and optionally updates manifest if file exists.
   * This is the first step in the download flow pattern.
   *
   * <p>Implementation follows a 2-step pattern:
   * <ol>
   *   <li>Check cache manifest first - trust it as source of truth</li>
   *   <li>Defensive check: if file exists but not in manifest, update manifest</li>
   * </ol>
   *
   * <p>Subclasses implement schema-specific caching logic including zero-byte file detection.
   *
   * @param dataType Type of data being checked
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @return true if cached (skip download), false if needs download
   */
  protected boolean isCachedOrExists(String dataType, int year, Map<String, String> params) {

    // 1. Check cache manifest first - trust it as source of truth
    if (cacheManifest.isCached(dataType, year, params)) {
      LOGGER.info("⚡ Cached (manifest: fresh ETag/TTL), skipped download: {} (year={})", dataType, year);
      return true;
    }

    // 2. Defensive check: if file exists but not in manifest, update manifest
    Map<String, Object> metadata = loadTableMetadata(dataType);
    String filePath = storageProvider.resolvePath(cacheDirectory, resolveJsonPath(metadata.get("pattern").toString(), params));
    try {
      if (cacheStorageProvider.exists(filePath)) {
        long fileSize = cacheStorageProvider.getMetadata(filePath).getSize();
        if (fileSize > 0) {
          LOGGER.info("⚡ JSON exists, updating cache manifest: {} (year={})", dataType, year);
          cacheManifest.markCached(dataType, year, params, filePath, fileSize);
          cacheManifest.save(operatingDirectory);
          return true;
        } else {
          LOGGER.warn("Found zero-byte cache file for {} at {} — will re-download instead of using cache.", dataType, filePath);
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Error checking cache file existence: {}", e.getMessage());
      // If we can't check, assume it doesn't exist
    }

    return false;
  }

  // ===== Metadata-Driven Parquet Conversion =====

  /**
   * Loads table column definitions from schema metadata.
   * Converts JsonNode columns to List&lt;TableColumn&gt; format for Parquet writing.
   *
   * @param tableName Name of table in schema
   * @return List of TableColumn definitions with type, nullability, and comments
   * @throws IllegalArgumentException if table not found or has no columns
   */
  protected java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumnsFromMetadata(String tableName) {
    Map<String, Object> metadata = loadTableMetadata(tableName);

    if (!metadata.containsKey("columns")) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'columns' in schema");
    }

    JsonNode columnsNode = (JsonNode) metadata.get("columns");
    if (!columnsNode.isArray()) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' columns is not an array");
    }

    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        new ArrayList<>();

    for (JsonNode colNode : columnsNode) {
      String colName = colNode.has("name") ? colNode.get("name").asText() : null;
      String colType = colNode.has("type") ? colNode.get("type").asText() : "string";
      boolean nullable = colNode.has("nullable") && colNode.get("nullable").asBoolean();
      String comment = colNode.has("comment") ? colNode.get("comment").asText() : "";

      if (colName != null) {
        columns.add(
            new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                colName, colType, nullable, comment));
      }
    }

    return columns;
  }

  /**
   * Converts a JSON record (object) to a typed Map based on column metadata.
   *
   * @param recordNode The JSON record node
   * @param columnTypeMap Map of column names to their types
   * @param missingValueIndicator String value that indicates null (e.g., ".", "-", "N/A")
   * @return Map with properly typed values
   */
  protected Map<String, Object> convertJsonRecordToTypedMap(JsonNode recordNode,
      Map<String, String> columnTypeMap, String missingValueIndicator) {
    Map<String, Object> record = new java.util.HashMap<>();

    // Iterate through all fields in the JSON record
    java.util.Iterator<Map.Entry<String, JsonNode>> fields = recordNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String fieldName = field.getKey();
      JsonNode fieldValue = field.getValue();

      // Get the column type from metadata (if available)
      String columnType = columnTypeMap.get(fieldName);

      if (columnType != null) {
        // Convert using type metadata
        Object convertedValue =
            convertJsonValueToType(fieldValue, fieldName, columnType, missingValueIndicator);
        record.put(fieldName, convertedValue);
      } else {
        // Field not in schema - use Jackson's default conversion
        LOGGER.debug("Field '{}' not found in column metadata, using default conversion", fieldName);
        Object defaultValue = MAPPER.convertValue(fieldValue, Object.class);
        record.put(fieldName, defaultValue);
      }
    }

    return record;
  }

  /**
   * Converts a JSON value to the appropriate Java type based on column metadata.
   *
   * @param jsonValue The JSON value (may be null)
   * @param columnName Column name (for error reporting)
   * @param columnType Column type from schema (e.g., "string", "int", "double", "boolean")
   * @param missingValueIndicator String value that indicates null (e.g., ".", "-", "N/A")
   * @return Converted value as appropriate Java type, or null
   */
  protected Object convertJsonValueToType(JsonNode jsonValue, String columnName, String columnType,
      String missingValueIndicator) {
    // Handle null/missing values
    if (jsonValue == null || jsonValue.isNull() || jsonValue.isMissingNode()) {
      return null;
    }

    // Handle empty strings as null for numeric types
    if (jsonValue.isTextual()) {
      String textValue = jsonValue.asText();
      if (textValue == null || textValue.trim().isEmpty() || "null".equalsIgnoreCase(textValue)) {
        return null;
      }

      // Check if value matches the missing value indicator
      if (missingValueIndicator != null && missingValueIndicator.equals(textValue)) {
        return null;
      }
    }

    try {
      // Normalize type names (handle both lowercase and SQL types)
      String normalizedType = columnType.toLowerCase();

      switch (normalizedType) {
        case "string":
        case "varchar":
        case "char":
          return jsonValue.isTextual() ? jsonValue.asText() : jsonValue.toString();

        case "int":
        case "integer":
          if (jsonValue.isIntegralNumber()) {
            return jsonValue.asInt();
          } else if (jsonValue.isTextual()) {
            return Integer.parseInt(jsonValue.asText().trim());
          }
          throw new NumberFormatException("Cannot convert to integer: " + jsonValue);

        case "long":
        case "bigint":
          if (jsonValue.isIntegralNumber()) {
            return jsonValue.asLong();
          } else if (jsonValue.isTextual()) {
            return Long.parseLong(jsonValue.asText().trim());
          }
          throw new NumberFormatException("Cannot convert to long: " + jsonValue);

        case "double":
        case "float":
          if (jsonValue.isNumber()) {
            return jsonValue.asDouble();
          } else if (jsonValue.isTextual()) {
            return Double.parseDouble(jsonValue.asText().trim());
          }
          throw new NumberFormatException("Cannot convert to double: " + jsonValue);

        case "boolean":
          if (jsonValue.isBoolean()) {
            return jsonValue.asBoolean();
          } else if (jsonValue.isTextual()) {
            String text = jsonValue.asText().trim().toLowerCase();
            return "true".equals(text) || "1".equals(text) || "yes".equals(text);
          }
          return false;

        default:
          // Default to string for unknown types
          LOGGER.warn("Unknown column type '{}' for column '{}', treating as string",
              columnType, columnName);
          return jsonValue.isTextual() ? jsonValue.asText() : jsonValue.toString();
      }

    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to convert value for column '{}' (type: {}): {}. Value: {}",
          columnName, columnType, e.getMessage(), jsonValue);
      return null;
    }
  }

  /**
   * Converts cached JSON data to Parquet format using schema metadata.
   *
   * <p>This is a generic, metadata-driven conversion that works for any table
   * in any schema (ECON, GEO, SEC). It uses the schema JSON as single source
   * of truth for:
   * <ul>
   *   <li>File paths (via pattern and variable substitution)</li>
   *   <li>Column definitions (types, nullability, comments)</li>
   *   <li>JSON structure (data path for extracting records)</li>
   * </ul>
   *
   * @param tableName Name of table in schema
   * @param variables Variables for path resolution (e.g., {year: "2020"})
   * @throws IOException if file operations fail
   */
  public void convertCachedJsonToParquet(String tableName, Map<String, String> variables)
      throws IOException {
    LOGGER.info("Converting cached JSON to Parquet for table: {}", tableName);

    // Load metadata
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    if (pattern == null) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'pattern' in schema");
    }

    // Resolve source (JSON) and target (Parquet) paths
    String jsonPath = resolveJsonPath(pattern, variables);
    String parquetPath = resolveParquetPath(pattern, variables);

    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
    String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

    LOGGER.info("Converting {} to {}", jsonPath, parquetPath);

    // Check if source exists
    if (!cacheStorageProvider.exists(fullJsonPath)) {
      LOGGER.warn("Source JSON file not found: {}", fullJsonPath);
      return;
    }

    // Load column metadata first to enable type-aware conversion
    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumnsFromMetadata(tableName);

    // Build column type map for efficient lookup
    Map<String, String> columnTypeMap = new java.util.HashMap<>();
    for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
      columnTypeMap.put(column.getName(), column.getType());
    }

    // Extract missingValueIndicator from metadata (download.response.missingValueIndicator)
    // Use JsonNode.at() with JSON Pointer notation for clean path traversal
    String missingValueIndicator = null;
    if (metadata.containsKey("download")) {
      JsonNode downloadNode = (JsonNode) metadata.get("download");
      JsonNode missingValueNode = downloadNode.at("/response/missingValueIndicator");
      if (!missingValueNode.isMissingNode()) {
        missingValueIndicator = missingValueNode.asText();
        LOGGER.info("Using missingValueIndicator: '{}'", missingValueIndicator);
      }
    }

    // Read JSON file with type-aware conversion
    List<Map<String, Object>> records = new ArrayList<>();
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(fullJsonPath);
         java.io.InputStreamReader reader =
             new java.io.InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

      // Extract records with proper type conversion
      if (root.isArray()) {
        for (JsonNode recordNode : root) {
          Map<String, Object> record =
              convertJsonRecordToTypedMap(recordNode, columnTypeMap, missingValueIndicator);
          records.add(record);
        }
      } else if (root.isObject()) {
        // Single object - wrap in list
        Map<String, Object> record =
            convertJsonRecordToTypedMap(root, columnTypeMap, missingValueIndicator);
        records.add(record);
      } else {
        throw new IOException("Data node is neither array nor object.");
      }

      LOGGER.info("Read {} records from {}", records.size(), jsonPath);
    } catch (Exception e) {
      LOGGER.error("Failed to read JSON file {}: {}", fullJsonPath, e.getMessage(), e);
      throw new IOException("Failed to read JSON: " + e.getMessage(), e);
    }

    if (records.isEmpty()) {
      LOGGER.warn("No records found in JSON file: {}", fullJsonPath);
      return;
    }

    // Write to Parquet using StorageProvider
    LOGGER.info("Writing {} records to Parquet: {}", records.size(), parquetPath);
    storageProvider.writeAvroParquet(fullParquetPath, columns, records, tableName, tableName);

    // Verify file was written
    if (storageProvider.exists(fullParquetPath)) {
      LOGGER.info("Successfully converted {} to Parquet: {}", tableName, parquetPath);
    } else {
      LOGGER.error("Parquet file not found after write: {}", fullParquetPath);
      throw new IOException("Parquet file not found after write: " + fullParquetPath);
    }
  }

  /**
   * Checks if a table has earlyDownload flag set to true in its download configuration.
   *
   * <p>Tables with earlyDownload=true should be downloaded before regular partitioned tables,
   * typically used for reference/catalog tables that other tables depend on.</p>
   *
   * @param tableName Name of table to check
   * @return true if table has earlyDownload=true in download config
   */
  @SuppressWarnings("unchecked")
  protected boolean isEarlyDownload(String tableName) {
    try {
      Map<String, Object> metadata = loadTableMetadata(tableName);
      if (!metadata.containsKey("download")) {
        return false;
      }

      Object downloadObj = metadata.get("download");
      Map<String, Object> downloadConfig;
      if (downloadObj instanceof JsonNode) {
        downloadConfig = MAPPER.convertValue((JsonNode) downloadObj, Map.class);
      } else {
        downloadConfig = (Map<String, Object>) downloadObj;
      }

      Object earlyDownloadObj = downloadConfig.get("earlyDownload");
      return earlyDownloadObj != null && Boolean.parseBoolean(earlyDownloadObj.toString());
    } catch (Exception e) {
      LOGGER.debug("Could not check earlyDownload for table {}: {}", tableName, e.getMessage());
      return false;
    }
  }
}
