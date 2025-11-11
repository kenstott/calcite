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

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

  /**
   * Functional interface for transforming records during JSON to Parquet conversion.
   *
   * <p>Allows per-record transformations such as:
   * <ul>
   *   <li>Adding calculated/derived fields</li>
   *   <li>Modifying field values</li>
   *   <li>Field name mapping</li>
   * </ul>
   */
  @FunctionalInterface
  public interface RecordTransformer {
    /**
     * Transforms a record.
     *
     * @param record Input record from JSON
     * @return Transformed record (can be same instance if modified in-place, or new instance)
     */
    Map<String, Object> transform(Map<String, Object> record);
  }

  /**
   * Functional interface for table operations (download or convert).
   * Executes an operation given a map of variables.
   */
  @FunctionalInterface
  public interface TableOperation {
    void execute(int year, Map<String, String> variables) throws Exception;
  }

  /**
   * Functional interface for checking if an operation is cached.
   */
  @FunctionalInterface
  public interface CacheChecker {
    boolean isCached(int year, Map<String, String> variables);
  }

  /**
   * Represents a single dimension of iteration with variable name and values.
   * Used for multi-dimensional iteration over download/conversion operations.
   */
  public static class IterationDimension {
    final String variableName;
    final List<String> values;

    public IterationDimension(String variableName, java.util.Collection<String> values) {
      this.variableName = variableName;
      this.values = new ArrayList<>(values);
    }

    public static IterationDimension fromYearRange(int startYear, int endYear) {
      List<String> years = new ArrayList<>();
      for (int year = startYear; year <= endYear; year++) {
        years.add(String.valueOf(year));
      }
      return new IterationDimension("year", years);
    }
  }

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
   * Downloads all reference tables specific to this data source.
   *
   * <p>Reference tables are catalog/lookup/metadata tables that are not year-specific
   * and typically have low update frequency. Examples include:
   * <ul>
   *   <li>FRED: reference_fred_series catalog</li>
   *   <li>BLS: JOLTS industries/states reference tables</li>
   *   <li>BEA: Regional line codes for various tables</li>
   * </ul>
   *
   * <p>Implementations should:
   * <ul>
   *   <li>Check cache manifest before downloading (respect TTL/refreshAfter)</li>
   *   <li>Use year=0 as sentinel value for reference tables</li>
   *   <li>Mark downloaded and converted in cache manifest</li>
   *   <li>Handle API-specific logic (rate limiting, pagination, etc.)</li>
   * </ul>
   *
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  public abstract void downloadReferenceData() throws IOException, InterruptedException;

  /**
   * Downloads all data for this source for the specified year range.
   *
   * <p>This is the main entry point for downloading time-series data. Implementations
   * should download all configured tables/series/indicators for the given time period.
   *
   * <p>Configuration (which tables to download, which series, etc.) should be passed to
   * the downloader via constructor parameters. This method focuses solely on the time range.
   *
   * <p>Implementations should:
   * <ul>
   *   <li>Check cache manifest before downloading (skip if already cached)</li>
   *   <li>Handle API-specific logic (rate limiting, pagination, authentication)</li>
   *   <li>Mark successfully downloaded data in cache manifest</li>
   *   <li>Log progress (downloaded/skipped counts)</li>
   * </ul>
   *
   * @param startYear First year to download (inclusive)
   * @param endYear Last year to download (inclusive)
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  public abstract void downloadAll(int startYear, int endYear)
      throws IOException, InterruptedException;

  /**
   * Converts all downloaded data to Parquet format for the specified year range.
   *
   * <p>This method should convert all raw data (JSON, CSV, XML, etc.) that was downloaded
   * via {@link #downloadAll(int, int)} into Parquet format.
   *
   * <p>Implementations should:
   * <ul>
   *   <li>Check if conversion already done (skip if parquet files exist and are up-to-date)</li>
   *   <li>Apply any transformations, enrichments, or schema mappings</li>
   *   <li>Mark successfully converted data in cache manifest</li>
   *   <li>Log progress (converted/skipped counts)</li>
   * </ul>
   *
   * @param startYear First year to convert (inclusive)
   * @param endYear Last year to convert (inclusive)
   * @throws IOException If conversion or file I/O fails
   */
  public abstract void convertAll(int startYear, int endYear) throws IOException;

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
   * Extracts an API parameter list from table metadata.
   *
   * <p>Looks for a JSON array in the download configuration at the specified key.
   *
   * @param tableName Name of the table in schema
   * @param listKey Key of the list in download config (e.g., "lineCodesList", "nipaTablesList")
   * @return List of string values, or empty list if not found
   */
  protected List<String> extractApiList(String tableName, String listKey) {
    try {
      Map<String, Object> metadata = loadTableMetadata(tableName);
      Object downloadObj = metadata.get("download");

      if (downloadObj instanceof JsonNode) {
        JsonNode download = (JsonNode) downloadObj;
        if (download.has(listKey)) {
          JsonNode listNode = download.get(listKey);
          if (listNode != null && listNode.isArray()) {
            List<String> result = new ArrayList<>();
            for (JsonNode item : listNode) {
              result.add(item.asText());
            }
            LOGGER.debug("Extracted {} items from {} for table {}", result.size(), listKey,
                tableName);
            return result;
          }
        }
      }

      LOGGER.warn("API list '{}' not found for table '{}', returning empty list", listKey,
          tableName);
      return Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Error extracting API list '{}' for table '{}': {}", listKey, tableName,
          e.getMessage());
      return Collections.emptyList();
    }
  }

  /**
   * Extracts an API parameter set (object) from table metadata.
   *
   * <p>Looks for a JSON object in the download configuration at the specified key
   * and returns it as a Map with string keys and object values.
   *
   * @param tableName Name of the table in schema
   * @param objectKey Key of the object in download config (e.g., "tableNamesSet", "geoFipsSet")
   * @return Map of key-value pairs, or empty map if not found
   */
  protected Map<String, Object> extractApiSet(String tableName, String objectKey) {
    try {
      Map<String, Object> metadata = loadTableMetadata(tableName);
      Object downloadObj = metadata.get("download");

      if (downloadObj instanceof JsonNode) {
        JsonNode download = (JsonNode) downloadObj;
        if (download.has(objectKey)) {
          JsonNode objectNode = download.get(objectKey);
          if (objectNode != null && objectNode.isObject()) {
            Map<String, Object> result = new LinkedHashMap<>();
            objectNode.fields().forEachRemaining(entry -> {
              result.put(entry.getKey(), entry.getValue());
            });
            LOGGER.debug("Extracted {} entries from {} for table {}", result.size(), objectKey,
                tableName);
            return result;
          }
        }
      }

      LOGGER.warn("API set '{}' not found for table '{}', returning empty map", objectKey,
          tableName);
      return Collections.emptyMap();
    } catch (Exception e) {
      LOGGER.error("Error extracting API set '{}' for table '{}': {}", objectKey, tableName,
          e.getMessage());
      return Collections.emptyMap();
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
   * This version supports embedding generation for computed columns.
   *
   * @param recordNode The JSON record node
   * @param columns Full column metadata including embedding config
   * @param missingValueIndicator String value that indicates null (e.g., ".", "-", "N/A")
   * @return Map with properly typed values and generated embeddings
   */
  protected Map<String, Object> convertJsonRecordToTypedMap(
      JsonNode recordNode,
      List<PartitionedTableConfig.TableColumn> columns,
      String missingValueIndicator) {

    Map<String, Object> typedRecord = new LinkedHashMap<>();

    // Build type map for conversion
    Map<String, String> columnTypeMap = new HashMap<>();
    for (PartitionedTableConfig.TableColumn col : columns) {
      columnTypeMap.put(col.getName(), col.getType());
    }

    // Existing type conversion for all fields
    Iterator<Map.Entry<String, JsonNode>> fields = recordNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String fieldName = field.getKey();

      // Skip computed columns in source data
      if (isComputedColumn(columns, fieldName)) {
        continue;
      }

      String columnType = columnTypeMap.get(fieldName);
      if (columnType != null) {
        Object convertedValue = convertJsonValueToType(
            field.getValue(), fieldName, columnType, missingValueIndicator);
        typedRecord.put(fieldName, convertedValue);
      } else {
        // Field not in schema - use Jackson's default conversion but it will be DROPPED later
        LOGGER.debug("Field '{}' not found in column metadata - will be DROPPED in Parquet output",
            fieldName);
        Object defaultValue = MAPPER.convertValue(field.getValue(), Object.class);
        typedRecord.put(fieldName, defaultValue);
      }
    }

    return typedRecord;
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
   * Builds a DuckDB SQL query for converting JSON to Parquet with type casting and null handling.
   *
   * <p>Generates a SELECT statement that:
   * <ul>
   *   <li>Casts each column to the appropriate SQL type</li>
   *   <li>Handles missing value indicators (e.g., "." → NULL)</li>
   *   <li>Preserves column order from schema</li>
   * </ul>
   *
   * @param columns Column definitions from schema
   * @param missingValueIndicator String that represents NULL (e.g., ".", "-", or null if none)
   * @param jsonPath Input JSON file path
   * @param parquetPath Output Parquet file path
   * @return Complete DuckDB SQL statement ready for execution
   */
  public static String buildDuckDBConversionSql(
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      String missingValueIndicator,
      String jsonPath,
      String parquetPath) {
    StringBuilder sql = new StringBuilder();

    // Start COPY statement
    sql.append("COPY (\n  SELECT\n");

    // Build column expressions
    boolean firstColumn = true;
    for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
      if (!firstColumn) {
        sql.append(",\n");
      }
      firstColumn = false;

      String columnName = column.getName();
      sql.append("    ");

      // Check if this is a computed column with an expression
      if (column.hasExpression()) {
        // Expression column: use SQL expression directly
        sql.append("(");
        sql.append(column.getExpression());
        sql.append(") AS ");
        sql.append(quoteIdentifier(columnName));
      } else {
        // Regular column: CAST from JSON with type conversion
        String sqlType = javaToDuckDbType(column.getType());

        // Handle missing value indicator with CASE expression
        if (missingValueIndicator != null && !missingValueIndicator.isEmpty()) {
          sql.append("CAST(CASE WHEN ");
          sql.append(quoteIdentifier(columnName));
          sql.append(" = ");
          sql.append(quoteLiteral(missingValueIndicator));
          sql.append(" THEN NULL ELSE ");
          sql.append(quoteIdentifier(columnName));
          sql.append(" END AS ");
          sql.append(sqlType);
          sql.append(") AS ");
          sql.append(quoteIdentifier(columnName));
        } else {
          // Simple CAST without null handling
          sql.append("CAST(");
          sql.append(quoteIdentifier(columnName));
          sql.append(" AS ");
          sql.append(sqlType);
          sql.append(") AS ");
          sql.append(quoteIdentifier(columnName));
        }
      }
    }

    // FROM clause with JSON reader
    sql.append("\n  FROM read_json_auto(");
    sql.append(quoteLiteral(jsonPath));
    sql.append(")\n) TO ");
    sql.append(quoteLiteral(parquetPath));
    sql.append(" (FORMAT PARQUET);");

    return sql.toString();
  }

  /**
   * Maps Java/Calcite type names to DuckDB SQL types.
   */
  private static String javaToDuckDbType(String javaType) {
    String normalized = javaType.toLowerCase();
    switch (normalized) {
      case "string":
      case "varchar":
      case "char":
        return "VARCHAR";
      case "int":
      case "integer":
        return "INTEGER";
      case "long":
      case "bigint":
        return "BIGINT";
      case "double":
      case "float":
        return "DOUBLE";
      case "boolean":
        return "BOOLEAN";
      case "date":
        return "DATE";
      case "timestamp":
        return "TIMESTAMP";
      default:
        LOGGER.warn("Unknown type '{}', defaulting to VARCHAR", javaType);
        return "VARCHAR";
    }
  }

  /**
   * Quotes a SQL identifier (column/table name) for DuckDB.
   */
  private static String quoteIdentifier(String identifier) {
    // DuckDB uses double quotes for identifiers
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  /**
   * Quotes a SQL string literal for DuckDB.
   */
  private static String quoteLiteral(String literal) {
    // DuckDB uses single quotes for string literals
    return "'" + literal.replace("'", "''") + "'";
  }

  /**
   * Creates a DuckDB connection with conversion-time extensions loaded.
   *
   * <p>Extensions are loaded with graceful degradation - failures are logged as warnings
   * but don't prevent connection creation. This allows the system to work even if some
   * extensions are unavailable.</p>
   *
   * @return DuckDB connection with extensions loaded
   * @throws java.sql.SQLException if connection creation fails
   */
  private Connection getDuckDBConnection() throws java.sql.SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    loadConversionExtensions(conn);
    return conn;
  }

  /**
   * Loads DuckDB extensions needed for data conversion operations.
   *
   * <p>Extensions loaded:
   * <ul>
   *   <li><b>quackformers</b> - Embedding generation via embed() function</li>
   *   <li><b>spatial</b> - GIS operations (ST_Read, ST_Area, etc.)</li>
   *   <li><b>h3</b> - Geospatial hexagonal indexing</li>
   *   <li><b>excel</b> - Excel file reading support</li>
   *   <li><b>fts</b> - Full-text search indexing</li>
   * </ul>
   *
   * <p>Failures are logged as warnings and do not prevent conversion. This allows
   * the system to work with basic functionality even if some extensions are missing.</p>
   *
   * @param conn DuckDB connection to load extensions into
   */
  private void loadConversionExtensions(Connection conn) {
    String[][] extensions = {
        {"quackformers", "FROM community"},  // Embedding generation
        {"spatial", ""},                      // GIS operations
        {"h3", "FROM community"},             // Geospatial hex indexing
        {"excel", ""},                        // Excel file support
        {"fts", ""}                           // Full-text indexing
    };

    for (String[] ext : extensions) {
      try {
        LOGGER.debug("Loading conversion extension: {}", ext[0]);
        conn.createStatement().execute("INSTALL " + ext[0] + " " + ext[1]);
        conn.createStatement().execute("LOAD " + ext[0]);
        LOGGER.debug("Successfully loaded extension: {}", ext[0]);
      } catch (java.sql.SQLException e) {
        // Special fallback for quackformers: try loading from GitHub
        if ("quackformers".equals(ext[0])) {
          try {
            LOGGER.info("Retrying quackformers from GitHub repository...");
            // Try simple GitHub URL first (DuckDB auto-discovers platform/version)
            conn.createStatement().execute("LOAD quackformers FROM 'https://github.com/martin-conur/quackformers'");
            LOGGER.info("Successfully loaded quackformers from GitHub");
            continue;
          } catch (java.sql.SQLException e2) {
            // If that fails, try platform-specific URL as fallback
            try {
              String githubUrl = buildQuackformersGitHubUrl(conn);
              LOGGER.info("Retrying with platform-specific URL: {}", githubUrl);
              conn.createStatement().execute("LOAD quackformers FROM '" + githubUrl + "'");
              LOGGER.info("Successfully loaded quackformers from platform-specific GitHub URL");
              continue;
            } catch (java.sql.SQLException e3) {
              LOGGER.warn("Failed to load quackformers from GitHub: {}", e3.getMessage());
            }
          }
        }
        LOGGER.warn("Failed to load extension '{}' (continuing): {}", ext[0], e.getMessage());
      }
    }
  }

  /**
   * Builds the GitHub URL for the quackformers extension binary.
   *
   * <p>Constructs a platform-specific URL like:
   * <a href="https://github.com/martin-conur/quackformers/raw/main/builds/v1.4.1/osx_arm64/quackformers.duckdb_extension">...</a>
   *
   * @param conn DuckDB connection to detect version
   * @return Full GitHub URL to the quackformers binary
   */
  private String buildQuackformersGitHubUrl(Connection conn) throws java.sql.SQLException {
    // Detect DuckDB version
    String duckdbVersion = detectDuckDBVersion(conn);

    // Detect platform
    String platform = detectPlatform();

    // Build URL
    return "https://github.com/martin-conur/quackformers/raw/main/builds/v"
        + duckdbVersion + "/" + platform + "/quackformers.duckdb_extension";
  }

  /**
   * Detects the DuckDB version from the connection.
   *
   * @param conn DuckDB connection
   * @return Version string (e.g., "1.4.1")
   */
  private String detectDuckDBVersion(Connection conn) throws java.sql.SQLException {
    try (java.sql.ResultSet rs = conn.createStatement().executeQuery("SELECT library_version FROM pragma_version()")) {
      if (rs.next()) {
        String fullVersion = rs.getString(1);
        // Extract major.minor.patch (e.g., "1.4.1" from "v1.4.1")
        if (fullVersion.startsWith("v")) {
          fullVersion = fullVersion.substring(1);
        }
        return fullVersion;
      }
    }
    // Fallback to metadata
    return conn.getMetaData().getDatabaseProductVersion();
  }

  /**
   * Detects the current platform (OS + architecture).
   *
   * @return Platform string (e.g., "osx_arm64", "linux_amd64", "windows_amd64")
   */
  private String detectPlatform() {
    String osName = System.getProperty("os.name").toLowerCase();
    String osArch = System.getProperty("os.arch").toLowerCase();

    String os;
    if (osName.contains("mac") || osName.contains("darwin")) {
      os = "osx";
    } else if (osName.contains("linux")) {
      os = "linux";
    } else if (osName.contains("windows")) {
      os = "windows";
    } else {
      os = "unknown";
    }

    String arch;
    if (osArch.equals("aarch64") || osArch.equals("arm64")) {
      arch = "arm64";
    } else if (osArch.contains("amd64") || osArch.contains("x86_64")) {
      arch = "amd64";
    } else {
      arch = "unknown";
    }

    return os + "_" + arch;
  }

  /**
   * Converts in-memory records to Parquet using DuckDB.
   *
   * <p>This method writes the in-memory records to a temporary JSON file, then uses
   * DuckDB to convert it to Parquet. The temp JSON file is written to the cache location
   * for debugging purposes and consistency with other conversions.
   *
   * @param tableName Name of table in schema
   * @param columns Column definitions from schema
   * @param records In-memory records to convert
   * @param fullParquetPath Absolute path to output Parquet file
   * @throws IOException if conversion fails
   */
  protected void convertInMemoryToParquetViaDuckDB(
      String tableName,
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> records,
      String fullParquetPath) throws IOException {

    // Write records to temporary JSON file in cache
    String tempJsonPath = fullParquetPath.replace(".parquet", "_temp.json");
    String fullTempJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, tempJsonPath);

    try {
      // Write JSON
      writeJsonRecords(fullTempJsonPath, records);

      // Convert using DuckDB
      convertCachedJsonToParquetViaDuckDB(tableName, columns, null, fullTempJsonPath, fullParquetPath);

      // Clean up temp file
      cacheStorageProvider.delete(fullTempJsonPath);

    } catch (IOException e) {
      // Clean up temp file on error
      try {
        cacheStorageProvider.delete(fullTempJsonPath);
      } catch (IOException cleanupError) {
        LOGGER.warn("Failed to clean up temp JSON file: {}", fullTempJsonPath, cleanupError);
      }
      throw e;
    }
  }

  /**
   * Writes in-memory records to a JSON file.
   *
   * @param fullJsonPath Absolute path to write JSON file
   * @param records Records to write
   * @throws IOException if write fails
   */
  private void writeJsonRecords(String fullJsonPath, List<Map<String, Object>> records) throws IOException {
    ensureParentDirectory(fullJsonPath);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    MAPPER.writeValue(baos, records);
    cacheStorageProvider.writeFile(fullJsonPath, baos.toByteArray());

    LOGGER.debug("Wrote {} records to temporary JSON: {}", records.size(), fullJsonPath);
  }

  /**
   * Converts cached JSON to Parquet using DuckDB's native SQL pipeline.
   *
   * <p>This method uses DuckDB to perform the entire conversion in a single SQL statement,
   * which is significantly faster and more memory-efficient than Java-based conversion.
   *
   * @param tableName Name of table in schema
   * @param columns Column definitions from schema
   * @param missingValueIndicator String that represents NULL (e.g., ".")
   * @param fullJsonPath Absolute path to input JSON file
   * @param fullParquetPath Absolute path to output Parquet file
   * @throws IOException if conversion fails
   */
  protected void convertCachedJsonToParquetViaDuckDB(
      String tableName,
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      String missingValueIndicator,
      String fullJsonPath,
      String fullParquetPath) throws IOException {

    // Build the SQL statement
    String sql = buildDuckDBConversionSql(columns, missingValueIndicator, fullJsonPath, fullParquetPath);

    LOGGER.debug("DuckDB conversion SQL:\n{}", sql);

    // Execute using in-memory DuckDB connection with extensions loaded
    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      // Execute the COPY statement
      stmt.execute(sql);

      LOGGER.info("Successfully converted {} to Parquet using DuckDB", tableName);

    } catch (java.sql.SQLException e) {
      // Wrap SQLException as IOException for consistent error handling
      String errorMsg = String.format(
          "DuckDB conversion failed for table '%s': %s",
          tableName,
          e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
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
    convertCachedJsonToParquet(tableName, variables, null);
  }

  /**
   * Converts cached JSON data to Parquet format using schema metadata with optional record
   * transformation.
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
   * <p><b>Note:</b> The transformer parameter is deprecated as of Phase 2. All transformations
   * should now be specified via expression columns in the schema. The parameter is retained
   * for backward compatibility but is ignored.</p>
   *
   * @param tableName Name of table in schema
   * @param variables Variables for path resolution (e.g., {year: "2020"})
   * @param transformer Deprecated - use expression columns in schema instead
   * @throws IOException if file operations fail
   * @deprecated Use expression columns in schema instead of runtime transformations
   */
  @Deprecated
  public void convertCachedJsonToParquet(String tableName, Map<String, String> variables,
      RecordTransformer transformer) throws IOException {
    LOGGER.info("Converting cached JSON to Parquet for table: {}", tableName);

    // Load metadata
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    if (pattern == null) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'pattern' in schema");
    }

    // Resolve source (JSON) and target (Parquet) paths
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));
    String fullParquetPath = storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

    LOGGER.info("Converting {} to {}", fullJsonPath, fullParquetPath);

    // Check if source exists
    if (!cacheStorageProvider.exists(fullJsonPath)) {
      LOGGER.warn("Source JSON file not found: {}", fullJsonPath);
      return;
    }

    // Load column metadata first to enable type-aware conversion
    List<PartitionedTableConfig.TableColumn> columns =
        loadTableColumnsFromMetadata(tableName);

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

    // Use DuckDB for JSON→Parquet conversion with expression columns
    LOGGER.info("Using DuckDB for JSON to Parquet conversion with expression columns");
    convertCachedJsonToParquetViaDuckDB(tableName, columns, missingValueIndicator,
        fullJsonPath, fullParquetPath);

    // Verify file was written
    if (storageProvider.exists(fullParquetPath)) {
      LOGGER.info("Successfully converted {} to Parquet: {}", tableName, fullParquetPath);
    } else {
      LOGGER.error("Parquet file not found after DuckDB conversion: {}", fullParquetPath);
      throw new IOException("Parquet file not found after write: " + fullParquetPath);
    }
  }

  /**
   * Checks if a column is marked as computed in the schema.
   */
  private boolean isComputedColumn(List<PartitionedTableConfig.TableColumn> columns,
      String columnName) {
    return columns.stream()
        .anyMatch(c -> c.getName().equals(columnName) && c.isComputed());
  }

  // ===== Trend Consolidation =====

  /**
   * Represents a trend pattern configuration from schema.
   * Trend patterns consolidate year-partitioned data into single files for faster querying.
   */
  protected static class TrendPattern {
    final String sourceTableName;
    final String sourcePattern;
    final String trendName;
    final String trendPattern;

    TrendPattern(String sourceTableName, String sourcePattern, String trendName, String trendPattern) {
      this.sourceTableName = sourceTableName;
      this.sourcePattern = sourcePattern;
      this.trendName = trendName;
      this.trendPattern = trendPattern;
    }
  }

  /**
   * Consolidates all tables with trend_patterns into consolidated parquet files.
   *
   * <p>This method:
   * <ul>
   *   <li>Scans schema for tables with trend_patterns</li>
   *   <li>For each trend pattern, uses DuckDB to consolidate all year partitions</li>
   *   <li>Writes consolidated files to parquet directory</li>
   * </ul>
   *
   * <p>Example: Consolidates employment_statistics from:
   * <pre>
   *   type=employment_statistics/frequency=A/year=2020/employment_statistics.parquet
   *   type=employment_statistics/frequency=A/year=2021/employment_statistics.parquet
   *   ...
   * </pre>
   * into:
   * <pre>
   *   type=employment_statistics/frequency=A/employment_statistics.parquet
   * </pre>
   *
   * @throws IOException if consolidation fails
   */
  public void consolidateAll() throws IOException {
    LOGGER.info("Starting trend consolidation for all tables");

    List<TrendPattern> trendPatterns = getTrendPatterns();

    if (trendPatterns.isEmpty()) {
      LOGGER.info("No tables with trend_patterns found in schema");
      return;
    }

    LOGGER.info("Found {} trend patterns to consolidate", trendPatterns.size());

    int consolidatedCount = 0;
    int skippedCount = 0;

    for (TrendPattern trend : trendPatterns) {
      try {
        consolidateTrendTable(trend);
        consolidatedCount++;
      } catch (Exception e) {
        LOGGER.error("Failed to consolidate trend '{}': {}", trend.trendName, e.getMessage(), e);
        skippedCount++;
      }
    }

    LOGGER.info("Trend consolidation complete: {} consolidated, {} failed",
        consolidatedCount, skippedCount);
  }

  /**
   * Extracts all trend patterns from schema JSON.
   *
   * @return List of trend patterns from all tables
   */
  protected List<TrendPattern> getTrendPatterns() {
    List<TrendPattern> trendPatterns = new ArrayList<>();

    try {
      // Load schema from resources
      InputStream schemaStream = getClass().getResourceAsStream(schemaResourceName);
      if (schemaStream == null) {
        LOGGER.warn("Schema resource not found: {}", schemaResourceName);
        return trendPatterns;
      }

      JsonNode root = MAPPER.readTree(schemaStream);

      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        LOGGER.warn("Schema has no partitionedTables array");
        return trendPatterns;
      }

      // Scan all tables for trend_patterns
      for (JsonNode tableNode : root.get("partitionedTables")) {
        String tableName = tableNode.has("name") ? tableNode.get("name").asText() : null;
        String sourcePattern = tableNode.has("pattern") ? tableNode.get("pattern").asText() : null;

        if (tableName == null || sourcePattern == null) {
          continue;
        }

        // Check if table has trend_patterns
        if (tableNode.has("trend_patterns") && tableNode.get("trend_patterns").isArray()) {
          for (JsonNode trendNode : tableNode.get("trend_patterns")) {
            String trendName = trendNode.has("name") ? trendNode.get("name").asText() : null;
            String trendPattern = trendNode.has("pattern") ? trendNode.get("pattern").asText() : null;

            if (trendName != null && trendPattern != null) {
              trendPatterns.add(new TrendPattern(tableName, sourcePattern, trendName, trendPattern));
              LOGGER.debug("Found trend pattern: {} -> {}", tableName, trendName);
            }
          }
        }
      }

    } catch (IOException e) {
      LOGGER.error("Failed to load trend patterns from schema: {}", e.getMessage());
    }

    return trendPatterns;
  }

  /**
   * Consolidates a single trend table using DuckDB.
   *
   * <p>Generates iteration over all non-year variables in the source pattern,
   * then for each combination, consolidates all years into a single file.
   *
   * @param trend Trend pattern configuration
   * @throws IOException if consolidation fails
   */
  protected void consolidateTrendTable(TrendPattern trend) throws IOException {
    LOGGER.info("Consolidating trend: {} from {}", trend.trendName, trend.sourceTableName);

    // Extract variables from both patterns
    // Source: type=employment_statistics/frequency={frequency}/year={year}/employment_statistics.parquet
    // Trend:  type=employment_statistics/frequency={frequency}/employment_statistics.parquet

    // Find variables in source pattern (e.g., {frequency}, {year})
    java.util.regex.Pattern varPattern = java.util.regex.Pattern.compile("\\{(\\w+)\\}");
    java.util.regex.Matcher sourceMatcher = varPattern.matcher(trend.sourcePattern);

    List<String> sourceVars = new ArrayList<>();
    while (sourceMatcher.find()) {
      sourceVars.add(sourceMatcher.group(1));
    }

    // Find variables in trend pattern (e.g., {frequency})
    java.util.regex.Matcher trendMatcher = varPattern.matcher(trend.trendPattern);
    List<String> trendVars = new ArrayList<>();
    while (trendMatcher.find()) {
      trendVars.add(trendMatcher.group(1));
    }

    // Variables to iterate over = trendVars (non-year dimensions)
    // For employment_statistics: just {frequency}
    LOGGER.debug("Trend variables to iterate: {}", trendVars);

    // For now, handle simple case: single non-year variable (frequency)
    // TODO: Extend to handle multiple non-year variables with Cartesian product

    if (trendVars.size() == 1) {
      String varName = trendVars.get(0);

      // Extract possible values from table metadata if available
      // For frequency: typically ["A", "M", "Q"]
      // For now, use hardcoded common values - subclasses can override

      List<String> values = getVariableValues(trend.sourceTableName, varName);

      LOGGER.info("Consolidating {} with {} values for {}: {}",
          trend.trendName, values.size(), varName, values);

      for (String value : values) {
        Map<String, String> variables = new HashMap<>();
        variables.put(varName, value);

        consolidateTrendForVariables(trend, variables);
      }

    } else if (trendVars.isEmpty()) {
      // No variables - consolidate everything
      consolidateTrendForVariables(trend, new HashMap<>());

    } else {
      LOGGER.warn("Multi-variable trend patterns not yet implemented: {}", trendVars);
      // TODO: Implement Cartesian product for multiple variables
    }
  }

  /**
   * Gets possible values for a variable (like frequency).
   * Subclasses can override to provide schema-specific values.
   *
   * @param tableName Table name
   * @param varName Variable name (e.g., "frequency")
   * @return List of possible values
   */
  protected List<String> getVariableValues(String tableName, String varName) {
    // Default values for common variables
    if ("frequency".equalsIgnoreCase(varName)) {
      return java.util.Arrays.asList("A", "M", "Q");
    }
    return Collections.emptyList();
  }

  /**
   * Consolidates trend data for a specific set of variables using DuckDB.
   *
   * @param trend Trend pattern
   * @param variables Variables to substitute (e.g., {frequency: "A"})
   * @throws IOException if consolidation fails
   */
  protected void consolidateTrendForVariables(TrendPattern trend, Map<String, String> variables)
      throws IOException {

    // Build source glob pattern (replaces year=* and other vars)
    String sourceGlob = buildSourceGlob(trend.sourcePattern, variables);

    // Build target path (substitutes variables, no year)
    String targetPath = substituteVariables(trend.trendPattern, variables);

    // Resolve full paths
    String fullSourceGlob = storageProvider.resolvePath(parquetDirectory, sourceGlob);
    String fullTargetPath = storageProvider.resolvePath(parquetDirectory, targetPath);

    LOGGER.info("Consolidating:\n  FROM: {}\n  TO:   {}", fullSourceGlob, fullTargetPath);

    // Build DuckDB SQL
    String sql = buildTrendConsolidationSql(fullSourceGlob, fullTargetPath);

    LOGGER.debug("Consolidation SQL:\n{}", sql);

    // Execute using DuckDB
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {

      stmt.execute(sql);
      LOGGER.info("Successfully consolidated trend: {}", trend.trendName);

    } catch (java.sql.SQLException e) {
      String errorMsg = String.format(
          "DuckDB consolidation failed for trend '%s': %s",
          trend.trendName,
          e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Builds source glob pattern by replacing variables and using * for year.
   *
   * @param sourcePattern Source pattern from schema
   * @param variables Variables to substitute
   * @return Glob pattern for reading source files
   */
  private String buildSourceGlob(String sourcePattern, Map<String, String> variables) {
    String result = sourcePattern;

    // Replace all {var} with values from variables map
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("{" + entry.getKey() + "}", entry.getValue());
    }

    // Replace {year} with * (wildcard)
    result = result.replace("{year}", "*");

    // Also handle year= patterns
    result = result.replaceAll("year=\\{year\\}", "year=*");

    return result;
  }

  /**
   * Substitutes variables in a pattern (no wildcards).
   *
   * @param pattern Pattern with {var} placeholders
   * @param variables Variable values
   * @return Pattern with variables substituted
   */
  private String substituteVariables(String pattern, Map<String, String> variables) {
    String result = pattern;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  /**
   * Builds DuckDB SQL for consolidating year partitions into a single file.
   *
   * @param sourceGlob Glob pattern for source files (with year=*)
   * @param targetPath Target consolidated file path
   * @return SQL COPY statement
   */
  private String buildTrendConsolidationSql(String sourceGlob, String targetPath) {
    return "COPY (\n" +
        "  SELECT * FROM read_parquet(" + quoteLiteral(sourceGlob) + ")\n" +
        "  ORDER BY year\n" +
        ") TO " + quoteLiteral(targetPath) + " (FORMAT PARQUET);";
  }

  /**
   * Generic method to iterate over table operations with arbitrary nesting levels (1-4 loops).
   * Handles variable map generation, cache checking, operation execution, progress tracking,
   * and manifest saving.
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensions List of iteration dimensions (1-4 dimensions supported)
   * @param cacheChecker Lambda to check if operation is cached
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationDescription Description for logging (e.g., "download", "conversion")
   */
  protected void iterateTableOperations(
      String tableName,
      List<IterationDimension> dimensions,
      CacheChecker cacheChecker,
      TableOperation operation,
      String operationDescription) {

    if (dimensions == null || dimensions.isEmpty()) {
      LOGGER.warn("No dimensions provided for {} operations on {}", operationDescription, tableName);
      return;
    }

    // Calculate total operations for progress tracking
    int totalOperations = 1;
    for (IterationDimension dim : dimensions) {
      totalOperations *= dim.values.size();
    }

    LOGGER.info("Starting {} operations for {} ({} total combinations)",
        operationDescription, tableName, totalOperations);

    // Track progress
    int[] counters = new int[2]; // [0]=executed, [1]=skipped

    // Generate and execute all combinations using recursive iteration
    iterateDimensionsRecursive(dimensions, 0, new HashMap<String, String>(),
        tableName, cacheChecker, operation, operationDescription, counters, totalOperations);

    // Save manifest after all operations complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest for {}: {}", tableName, e.getMessage());
    }

    LOGGER.info("{} {} complete: executed {} operations, skipped {} (cached)",
        tableName, operationDescription, counters[0], counters[1]);
  }

  /**
   * Recursive helper to iterate over all combinations of dimension values.
   * Builds up the variables map as it recurses through dimensions.
   *
   * @param dimensions All iteration dimensions
   * @param dimensionIndex Current dimension being iterated
   * @param variables Variables map built so far
   * @param tableName Table name for logging
   * @param cacheChecker Cache checking lambda
   * @param operation Operation execution lambda
   * @param operationDescription Description for logging
   * @param counters Progress counters [executed, skipped]
   * @param totalOperations Total operations for progress logging
   */
  private void iterateDimensionsRecursive(
      List<IterationDimension> dimensions,
      int dimensionIndex,
      Map<String, String> variables,
      String tableName,
      CacheChecker cacheChecker,
      TableOperation operation,
      String operationDescription,
      int[] counters,
      int totalOperations) {

    // Base case: all dimensions iterated, execute operation
    if (dimensionIndex >= dimensions.size()) {
      // Extract year from variables (default to 0 if not present)
      int year = 0;
      if (variables.containsKey("year")) {
        try {
          year = Integer.parseInt(variables.get("year"));
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid year value in variables: {}", variables.get("year"));
        }
      }

      // Check cache
      if (cacheChecker.isCached(year, variables)) {
        counters[1]++; // skipped
        return;
      }

      // Execute operation
      try {
        operation.execute(year, variables);
        counters[0]++; // executed

        // Log progress every 10 operations
        if (counters[0] % 10 == 0) {
          LOGGER.info("{} {}/{} operations (skipped {} cached)",
              operationDescription, counters[0], totalOperations, counters[1]);
        }
      } catch (Exception e) {
        LOGGER.error("Error during {} for {} with variables {}: {}",
            operationDescription, tableName, variables, e.getMessage());
      }
      return;
    }

    // Recursive case: iterate over current dimension
    IterationDimension currentDim = dimensions.get(dimensionIndex);
    for (String value : currentDim.values) {
      // Add current dimension's variable to map
      Map<String, String> nextVariables = new HashMap<>(variables);
      nextVariables.put(currentDim.variableName, value);

      // Recurse to next dimension
      iterateDimensionsRecursive(dimensions, dimensionIndex + 1, nextVariables,
          tableName, cacheChecker, operation, operationDescription, counters, totalOperations);
    }
  }
}
