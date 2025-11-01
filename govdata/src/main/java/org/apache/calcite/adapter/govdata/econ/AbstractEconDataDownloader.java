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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * Abstract base class for ECON data downloaders providing common infrastructure
 * for cache management, rate limiting, and download/conversion flow patterns.
 *
 * <p>Implements the Template Method pattern to enforce consistent cache manifest
 * usage across all economic data sources while allowing API-specific customization.
 *
 * <h3>Standard Flow Pattern</h3>
 * <pre>
 * Download Flow:
 * 1. Check manifest.isCached() → if true, skip download
 * 2. Check local file exists (defensive) → update manifest if found
 * 3. Download data via API-specific implementation
 * 4. Save to local cache directory
 * 5. Mark as cached in manifest
 *
 * Conversion Flow:
 * 1. Check storageProvider.exists(targetPath) → if true, skip conversion
 * 2. Convert JSON to Parquet via API-specific implementation
 * 3. FileSchema's conversion registry automatically tracks the conversion
 * 4. No need to mark as converted - FileSchema handles this
 * </pre>
 *
 * <h3>Subclass Responsibilities</h3>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getMinRequestIntervalMs()} - API-specific rate limit</li>
 *   <li>{@link #getMaxRetries()} - Retry policy for failed requests</li>
 *   <li>{@link #getRetryDelayMs()} - Initial delay for retry backoff</li>
 *   <li>API-specific download methods using provided helper methods</li>
 *   <li>API-specific conversion methods using provided helper methods</li>
 * </ul>
 *
 * @see CacheManifest
 * @see StorageProvider
 */
public abstract class AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEconDataDownloader.class);

  /** Shared ObjectMapper for JSON serialization */
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Data frequency for partitioning strategy (Phase 4).
   * Enables partition pruning for time-range queries.
   */
  public enum DataFrequency {
    DAILY("daily", "D"),
    MONTHLY("monthly", "M"),
    QUARTERLY("quarterly", "Q"),
    ANNUAL("annual", "A");

    private final String partitionName;
    private final String shortCode;

    DataFrequency(String partitionName, String shortCode) {
      this.partitionName = partitionName;
      this.shortCode = shortCode;
    }

    public String getPartitionName() {
      return partitionName;
    }

    public String getShortCode() {
      return shortCode;
    }

    /**
     * Parse frequency from short code (D, M, Q, A).
     *
     * @param code Short frequency code
     * @return Corresponding DataFrequency
     * @throws IllegalArgumentException if code is unknown
     */
    public static DataFrequency fromShortCode(String code) {
      for (DataFrequency freq : values()) {
        if (freq.shortCode.equals(code)) {
          return freq;
        }
      }
      throw new IllegalArgumentException("Unknown frequency code: " + code);
    }
  }

  /** Cache directory for storing downloaded raw data (e.g., $GOVDATA_CACHE_DIR/econ/) */
  protected final String cacheDirectory;

  /** Operating directory for storing operational metadata (e.g., .aperio/econ/) */
  protected final String operatingDirectory;

  /** Parquet directory for storing converted parquet files (e.g., $GOVDATA_PARQUET_DIR or s3://govdata-parquet) */
  protected final String parquetDirectory;

  /** Storage provider for reading/writing raw cache files (JSON, XML) */
  protected final StorageProvider cacheStorageProvider;

  /** Storage provider for reading/writing parquet files (supports local and S3) */
  protected final StorageProvider storageProvider;

  /** Cache manifest for tracking downloads and conversions */
  protected final CacheManifest cacheManifest;

  /** HTTP client for API requests */
  protected final HttpClient httpClient;

  /** Timestamp of last API request for rate limiting */
  protected long lastRequestTime = 0;

  /**
   * Constructs base downloader with required infrastructure.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   */
  protected AbstractEconDataDownloader(String cacheDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDirectory, cacheDirectory, cacheDirectory, cacheStorageProvider, storageProvider, null);
  }

  /**
   * Constructs base downloader with separate raw cache and operating directories and shared cache manifest.
   * This constructor should be used when multiple downloaders share the same manifest to avoid stale cache issues.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param operatingDirectory Directory for storing operational metadata (.aperio/<schema>/)
   * @param parquetDirectory Directory for storing parquet files (e.g., s3://govdata-parquet)
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   * @param sharedManifest Shared cache manifest (if null, will load from operatingDirectory)
   */
  protected AbstractEconDataDownloader(String cacheDirectory, String operatingDirectory, String parquetDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest) {
    this.cacheDirectory = cacheDirectory;
    this.operatingDirectory = operatingDirectory;
    this.parquetDirectory = parquetDirectory;
    this.cacheStorageProvider = cacheStorageProvider;
    this.storageProvider = storageProvider;
    this.cacheManifest = sharedManifest != null ? sharedManifest : CacheManifest.load(operatingDirectory);
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }

  /**
   * Returns the minimum interval between API requests in milliseconds.
   * Different APIs have different rate limits (e.g., FRED: 500ms, BLS: 1100ms).
   *
   * @return Minimum milliseconds between requests, or 0 if no rate limit
   */
  protected abstract long getMinRequestIntervalMs();

  /**
   * Returns the maximum number of retry attempts for failed requests.
   *
   * @return Maximum retry attempts
   */
  protected abstract int getMaxRetries();

  /**
   * Returns the initial delay for retry backoff in milliseconds.
   *
   * @return Initial retry delay in milliseconds
   */
  protected abstract long getRetryDelayMs();

  /**
   * Checks if data is cached in manifest and optionally updates manifest if file exists.
   * This is the first step in the download flow pattern.
   *
   * @param dataType Type of data being checked
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param relativePath Relative path to check (for defensive file existence check)
   * @return true if cached (skip download), false if needs download
   */
  protected final boolean isCachedOrExists(String dataType, int year,
      Map<String, String> params, String relativePath) {

    // 1. Check cache manifest first - trust it as source of truth
    if (cacheManifest.isCached(dataType, year, params)) {
      LOGGER.info("⚡ Cached (manifest: fresh ETag/TTL), skipped download: {} (year={})", dataType, year);
      return true;
    }

    // 2. Defensive check: if file exists but not in manifest, update manifest
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    try {
      if (cacheStorageProvider.exists(filePath)) {
        LOGGER.info("⚡ JSON exists, updating cache manifest: {} (year={})", dataType, year);
        long fileSize = cacheStorageProvider.getMetadata(filePath).getSize();
        cacheManifest.markCached(dataType, year, params, relativePath, fileSize);
        cacheManifest.save(operatingDirectory);
        return true;
      }
    } catch (IOException e) {
      LOGGER.debug("Error checking cache file existence: {}", e.getMessage());
      // If we can't check, assume it doesn't exist
    }

    return false;
  }

  /**
   * Saves downloaded JSON content to cache and updates manifest.
   * This is the final step in the download flow pattern.
   *
   * @param dataType Type of data being cached
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param relativePath Relative path within cache directory
   * @param jsonContent JSON content to save
   * @throws IOException If file write fails
   */
  protected final void saveToCache(String dataType, int year, Map<String, String> params,
      String relativePath, String jsonContent) throws IOException {

    // Save raw JSON data via cache storage provider
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    // Mark as cached in manifest (operating metadata stays in .aperio via File API)
    cacheManifest.markCached(dataType, year, params, relativePath, jsonContent.length());
    cacheManifest.save(operatingDirectory);

    LOGGER.info("{} data saved to: {} ({} bytes)", dataType, relativePath, jsonContent.length());
  }

  /**
   * Build partition path with frequency dimension (Phase 4).
   * Format: source=econ/type=X/frequency=Y/year=YYYY/
   *
   * @param dataType Data type (e.g., "state_wages", "treasury_yields")
   * @param frequency Data frequency
   * @param year Year
   * @return Partition path
   */
  protected String buildPartitionPath(String dataType, DataFrequency frequency, int year) {
    return buildPartitionPath(dataType, frequency, year, null);
  }

  /**
   * Build partition path with frequency and optional month (for daily data).
   * Format: source=econ/type=X/frequency=Y/year=YYYY/month=MM/
   *
   * @param dataType Data type (e.g., "treasury_yields")
   * @param frequency Data frequency
   * @param year Year
   * @param month Optional month (1-12) for daily data
   * @return Partition path
   */
  protected String buildPartitionPath(String dataType, DataFrequency frequency, int year, Integer month) {
    StringBuilder path = new StringBuilder();
    path.append("source=econ");
    path.append("/type=").append(dataType);
    path.append("/frequency=").append(frequency.getPartitionName());
    path.append("/year=").append(year);

    if (month != null && frequency == DataFrequency.DAILY) {
      path.append("/month=").append(String.format("%02d", month));
    }

    return path.toString();
  }

  // REMOVED: isParquetConverted() and markParquetConverted()
  // Parquet conversion tracking is now handled by FileSchema's conversion registry.
  // Downloaders should check file existence using storageProvider.exists() and
  // let FileSchema's conversion metadata track the conversions centrally.

  /**
   * Enforces rate limiting by ensuring minimum interval between API requests.
   * Uses synchronized block to handle concurrent access safely.
   *
   * @throws InterruptedException If thread is interrupted while waiting
   */
  protected final void enforceRateLimit() throws InterruptedException {
    long minInterval = getMinRequestIntervalMs();
    if (minInterval <= 0) {
      return; // No rate limit
    }

    synchronized (this) {
      long now = System.currentTimeMillis();
      long timeSinceLastRequest = now - lastRequestTime;
      if (timeSinceLastRequest < minInterval) {
        long waitTime = minInterval - timeSinceLastRequest;
        LOGGER.trace("Rate limiting: waiting {} ms", waitTime);
        Thread.sleep(waitTime);
      }
      lastRequestTime = System.currentTimeMillis();
    }
  }

  /**
   * Executes HTTP request with retry logic and exponential backoff.
   * Handles rate limiting and transient failures automatically.
   *
   * @param request HTTP request to execute
   * @return HTTP response
   * @throws IOException If all retry attempts fail
   * @throws InterruptedException If thread is interrupted
   */
  protected final HttpResponse<String> executeWithRetry(HttpRequest request)
      throws IOException, InterruptedException {

    int maxRetries = getMaxRetries();
    long retryDelay = getRetryDelayMs();

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // Enforce rate limiting before request
        enforceRateLimit();

        // Execute request
        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        // Check for rate limit response (429) or server error (5xx)
        if (response.statusCode() == 429 || response.statusCode() >= 500) {
          if (attempt < maxRetries - 1) {
            long delay = retryDelay * (long) Math.pow(2, attempt);
            LOGGER.warn("Request failed with status {} - retrying in {} ms (attempt {}/{})",
                response.statusCode(), delay, attempt + 1, maxRetries);
            Thread.sleep(delay);
            continue;
          }
        }

        // Success or non-retryable error
        return response;

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

  /**
   * Extracts all Hive-style partition parameters from path.
   * For example, from "type=custom/year=2020/maturity=10Y/file.parquet"
   * extracts {"maturity": "10Y"} (year is handled separately).
   *
   * @param path File path with Hive partitions
   * @return Map of partition key-value pairs (excluding year, type, and source)
   */
  protected final Map<String, String> extractPartitionParams(String path) {
    Map<String, String> params = new java.util.HashMap<>();

    // Match all key=value patterns in the path
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([^/=]+)=([^/]+)");
    java.util.regex.Matcher matcher = pattern.matcher(path);

    while (matcher.find()) {
      String key = matcher.group(1);
      String value = matcher.group(2);

      // Skip organizational partitions: year (handled separately), type (added by caller), source (organizational only)
      if (!"year".equals(key) && !"type".equals(key) && !"source".equals(key)) {
        params.put(key, value);
      }
    }

    return params;
  }

  /**
   * Extracts year from Hive-style partitioned path (e.g., "year=2020").
   *
   * @param path File path containing year partition
   * @return Extracted year
   */
  protected final int extractYearFromPath(String path) {
    // Look for year=YYYY pattern in path
    int yearIndex = path.indexOf("year=");
    if (yearIndex != -1) {
      String yearStr = path.substring(yearIndex + 5);
      // Extract 4-digit year
      int endIndex = yearStr.indexOf('/');
      if (endIndex != -1) {
        yearStr = yearStr.substring(0, endIndex);
      } else {
        // Check for file extension
        endIndex = yearStr.indexOf('.');
        if (endIndex != -1) {
          yearStr = yearStr.substring(0, endIndex);
        }
      }
      return Integer.parseInt(yearStr);
    }
    throw new IllegalArgumentException("Path does not contain year partition: " + path);
  }

  /**
   * Enriches an Avro Schema with documentation from a PartitionedTableConfig.
   * This method takes a base schema (with types defined) and adds .doc() annotations
   * for each field that has a corresponding entry in the config's columnComments.
   *
   * <p>Implementation note: Avro Schema is immutable, so this method creates a new
   * schema by manipulating the schema's JSON representation, then parsing it back.
   *
   * @param baseSchema The base Avro schema with field names and types defined
   * @param config The partitioned table config containing columnComments
   * @return A new Schema identical to baseSchema but with doc annotations from config
   */
  protected static org.apache.avro.Schema enrichSchemaWithComments(
      org.apache.avro.Schema baseSchema,
      org.apache.calcite.adapter.file.partition.PartitionedTableConfig config) {
    // If no column comments, return original schema unchanged
    if (config == null || config.getColumnComments() == null
        || config.getColumnComments().isEmpty()) {
      return baseSchema;
    }

    try {
      // Convert schema to JSON for manipulation
      com.fasterxml.jackson.databind.JsonNode schemaJson =
          MAPPER.readTree(baseSchema.toString());

      // Get column comments map
      java.util.Map<String, String> columnComments = config.getColumnComments();

      // Schema JSON is an object with a "fields" array
      if (schemaJson.has("fields") && schemaJson.get("fields").isArray()) {
        com.fasterxml.jackson.databind.node.ArrayNode fields =
            (com.fasterxml.jackson.databind.node.ArrayNode) schemaJson.get("fields");

        // Iterate through fields and add doc if available
        for (com.fasterxml.jackson.databind.JsonNode field : fields) {
          if (field.has("name")) {
            String fieldName = field.get("name").asText();
            String comment = columnComments.get(fieldName);

            if (comment != null && !comment.isEmpty()) {
              // Add doc field to this field object
              ((com.fasterxml.jackson.databind.node.ObjectNode) field).put("doc", comment);
            }
          }
        }
      }

      // Convert back to Schema
      return new org.apache.avro.Schema.Parser().parse(schemaJson.toString());
    } catch (Exception e) {
      // If enrichment fails, log warning and return original schema
      LOGGER.warn("Failed to enrich schema with comments from config for table {}: {}",
          config.getName(), e.getMessage());
      return baseSchema;
    }
  }

  /**
   * Loads table column metadata from the econ-schema.json resource file.
   * This enables metadata-driven schema generation, eliminating hardcoded type definitions.
   *
   * @param tableName The name of the table (must match "name" in econ-schema.json)
   * @return List of TableColumn definitions with type, nullability, and comments
   * @throws IllegalArgumentException if table not found or schema file cannot be loaded
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try {
      // Load econ-schema.json from resources
      java.io.InputStream schemaStream =
          AbstractEconDataDownloader.class.getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "econ-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "tables" array
      if (!root.has("tables") || !root.get("tables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid econ-schema.json: missing 'tables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("tables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in econ-schema.json");
          }

          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
              columns = new java.util.ArrayList<>();

          for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("columns")) {
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
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in econ-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

}
