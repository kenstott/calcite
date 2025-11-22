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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * Default retry interval (in days) for API errors (HTTP 200 with error content).
   * Prevents expensive retries on every restart while allowing periodic retry attempts.
   */
  protected static final int DEFAULT_API_ERROR_RETRY_DAYS = 7;

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
   * Result of a download operation containing path and file size.
   */
  public static class DownloadResult {
    public final String jsonPath;  // Relative path
    public final long fileSize;

    public DownloadResult(String jsonPath, long fileSize) {
      this.jsonPath = jsonPath;
      this.fileSize = fileSize;
    }
  }

  /**
   * Functional interface for table operations (download or convert).
   * Executes an operation with resolved paths and parameters.
   *
   */
  @FunctionalInterface
  public interface TableOperation {
    void execute(CacheKey cacheKey, Map<String, String> vars, String jsonPath, String parquetPath,
                 PrefetchHelper prefetchHelper) throws Exception;
  }

  /**
   * Functional interface for providing dimension values from table metadata or runtime data.
   * Called for each dimension found in the table's partition pattern.
   *
   */
  @FunctionalInterface
  public interface DimensionProvider {
    List<String> getValues(String dimensionName);
  }

  /**
   * Functional interface for prefetch callbacks that enable batching optimizations.
   * Called at the start of each dimension segment during iteration.
   * Allows bulk fetching of data (e.g., 20 years in one API call) and caching
   * in DuckDB for subsequent TableOperation executions.
   */
  @FunctionalInterface
  public interface PrefetchCallback {
    void prefetch(PrefetchContext context, PrefetchHelper helper) throws Exception;
  }

  /**
   * Context provided to prefetch callbacks containing segment information.
   * Allows prefetch logic to understand the current iteration state and make
   * batch decisions based on dimension values and ancestor context.
   */
  public static class PrefetchContext {
    public final String tableName;
    public final String segmentDimensionName;              // Current dimension being iterated
    public final Map<String, String> ancestorValues;        // Chosen parent dimension values
    public final Map<String, List<String>> allDimensionValues;  // ALL dimension values (mutable)

    public PrefetchContext(String tableName, String segmentDimensionName,
                           Map<String, String> ancestorValues,
                           Map<String, List<String>> allDimensionValues) {
      this.tableName = tableName;
      this.segmentDimensionName = segmentDimensionName;
      this.ancestorValues = Map.copyOf(ancestorValues);
      this.allDimensionValues = allDimensionValues;  // Intentionally mutable for dynamic updates
    }
  }

  /**
   * Helper class for prefetch callbacks and table operations to cache/retrieve data
   * using in-memory DuckDB. Auto-manages schema based on table metadata.
   * Supports JSON strings, CSV strings, and structured records.
   */
  public static class PrefetchHelper {
    private final Connection duckdbConn;
    private final String cacheTableName;
    private final List<String> partitionKeys;

    public PrefetchHelper(Connection duckdbConn, String cacheTableName, List<String> partitionKeys) {
      this.duckdbConn = duckdbConn;
      this.cacheTableName = cacheTableName;
      this.partitionKeys = partitionKeys;
    }

    /**
     * Insert JSON data with partition keys. Stores raw JSON in _raw_json column.
     */
    public void insertJsonBatch(List<Map<String, String>> partitionVars, List<String> jsonStrings)
        throws SQLException {
      // Create temp tables for batch insert
      try (Statement stmt = duckdbConn.createStatement()) {
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS json_raw (idx INTEGER, data VARCHAR)");
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS partition_lookup ("
            + String.join(" VARCHAR, ", partitionKeys) + " VARCHAR, json_idx INTEGER)");
      }

      // Insert JSON strings
      String insertJson = "INSERT INTO json_raw VALUES (?, ?)";
      try (PreparedStatement ps = duckdbConn.prepareStatement(insertJson)) {
        for (int i = 0; i < jsonStrings.size(); i++) {
          ps.setInt(1, i);
          ps.setString(2, jsonStrings.get(i));
          ps.addBatch();
        }
        ps.executeBatch();
      }

      // Insert partition keys
      insertPartitionLookup("partition_lookup", partitionVars, "json_idx");

      // JOIN and insert into cache table
      StringBuilder joinInsert = new StringBuilder("INSERT INTO ");
      joinInsert.append(cacheTableName).append(" (");
      joinInsert.append(String.join(", ", partitionKeys));
      joinInsert.append(", _raw_json) SELECT ");
      for (String key : partitionKeys) {
        joinInsert.append("p.").append(key).append(", ");
      }
      joinInsert.append("j.data FROM partition_lookup p JOIN json_raw j ON p.json_idx = j.idx");

      try (Statement stmt = duckdbConn.createStatement()) {
        stmt.execute(joinInsert.toString());
        stmt.execute("DROP TABLE json_raw");
        stmt.execute("DROP TABLE partition_lookup");
      }
    }

    /**
     * Insert CSV data with partition keys. Stores raw CSV in _raw_csv column.
     */
    public void insertCsvBatch(List<Map<String, String>> partitionVars, List<String> csvStrings)
        throws SQLException {
      // Create temp tables
      try (Statement stmt = duckdbConn.createStatement()) {
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS csv_raw (idx INTEGER, data VARCHAR)");
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS partition_lookup_csv ("
            + String.join(" VARCHAR, ", partitionKeys) + " VARCHAR, csv_idx INTEGER)");
      }

      // Insert CSV strings
      String insertCsv = "INSERT INTO csv_raw VALUES (?, ?)";
      try (PreparedStatement ps = duckdbConn.prepareStatement(insertCsv)) {
        for (int i = 0; i < csvStrings.size(); i++) {
          ps.setInt(1, i);
          ps.setString(2, csvStrings.get(i));
          ps.addBatch();
        }
        ps.executeBatch();
      }

      // Insert partition keys
      insertPartitionLookup("partition_lookup_csv", partitionVars, "csv_idx");

      // JOIN and insert into cache table
      StringBuilder joinInsert = new StringBuilder("INSERT INTO ");
      joinInsert.append(cacheTableName).append(" (");
      joinInsert.append(String.join(", ", partitionKeys));
      joinInsert.append(", _raw_csv) SELECT ");
      for (String key : partitionKeys) {
        joinInsert.append("p.").append(key).append(", ");
      }
      joinInsert.append("c.data FROM partition_lookup_csv p JOIN csv_raw c ON p.csv_idx = c.idx");

      try (Statement stmt = duckdbConn.createStatement()) {
        stmt.execute(joinInsert.toString());
        stmt.execute("DROP TABLE csv_raw");
        stmt.execute("DROP TABLE partition_lookup_csv");
      }
    }

    /**
     * Insert structured records with partition keys.
     */
    public void insertRecords(List<Map<String, String>> partitionVars,
                              List<Map<String, Object>> records) throws SQLException {
      if (records.isEmpty()) {
        return;
      }

      // Build INSERT statement with all columns from first record
      Set<String> dataColumns = records.get(0).keySet();
      StringBuilder insertSql = new StringBuilder("INSERT INTO ");
      insertSql.append(cacheTableName).append(" (");
      insertSql.append(String.join(", ", partitionKeys));
      for (String col : dataColumns) {
        insertSql.append(", ").append(col);
      }
      insertSql.append(") VALUES (");
      for (int i = 0; i < partitionKeys.size() + dataColumns.size(); i++) {
        if (i > 0) {
          insertSql.append(", ");
        }
        insertSql.append("?");
      }
      insertSql.append(")");

      try (PreparedStatement ps = duckdbConn.prepareStatement(insertSql.toString())) {
        for (int i = 0; i < partitionVars.size(); i++) {
          Map<String, String> pVars = partitionVars.get(i);
          Map<String, Object> record = records.get(i);

          int paramIndex = 1;
          for (String key : partitionKeys) {
            ps.setString(paramIndex++, pVars.get(key));
          }
          for (String col : dataColumns) {
            ps.setObject(paramIndex++, record.get(col));
          }
          ps.addBatch();
        }
        ps.executeBatch();
      }
    }

    /**
     * Retrieve JSON data for given partition keys.
     */
    public String getJson(Map<String, String> partitionVars) throws SQLException {
      String sql = "SELECT _raw_json FROM " + cacheTableName + " WHERE " + buildWhereClause();

      try (PreparedStatement ps = duckdbConn.prepareStatement(sql)) {
        bindPartitionKeys(ps, partitionVars);
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next() ? rs.getString(1) : null;
        }
      }
    }

    /**
     * Retrieve CSV data for given partition keys.
     */
    public String getCsv(Map<String, String> partitionVars) throws SQLException {
      String sql = "SELECT _raw_csv FROM " + cacheTableName + " WHERE " + buildWhereClause();

      try (PreparedStatement ps = duckdbConn.prepareStatement(sql)) {
        bindPartitionKeys(ps, partitionVars);
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next() ? rs.getString(1) : null;
        }
      }
    }

    /**
     * Retrieve structured record for given partition keys.
     */
    public Map<String, Object> getRecord(Map<String, String> partitionVars) throws SQLException {
      String sql = "SELECT * FROM " + cacheTableName + " WHERE " + buildWhereClause();

      try (PreparedStatement ps = duckdbConn.prepareStatement(sql)) {
        bindPartitionKeys(ps, partitionVars);

        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return null;
          }

          Map<String, Object> record = new HashMap<>();
          ResultSetMetaData meta = rs.getMetaData();
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            String colName = meta.getColumnName(i);
            record.put(colName, rs.getObject(i));
          }
          return record;
        }
      }
    }

    /**
     * Helper: Build WHERE clause with partition key equality conditions.
     */
    private String buildWhereClause() {
      StringBuilder where = new StringBuilder();
      boolean first = true;
      for (String key : partitionKeys) {
        if (!first) {
          where.append(" AND ");
        }
        where.append(key).append(" = ?");
        first = false;
      }
      return where.toString();
    }

    /**
     * Helper: Bind partition key values to prepared statement parameters.
     */
    private void bindPartitionKeys(PreparedStatement ps, Map<String, String> partitionVars)
        throws SQLException {
      int paramIndex = 1;
      for (String key : partitionKeys) {
        ps.setString(paramIndex++, partitionVars.get(key));
      }
    }

    /**
     * Helper: Insert partition lookup data into temp table.
     * Shared by insertJsonBatch and insertCsvBatch.
     */
    private void insertPartitionLookup(String tableName, List<Map<String, String>> partitionVars,
                                       String idxColumnName) throws SQLException {
      String insertSql = "INSERT INTO " + tableName + " VALUES (" +
          "?, ".repeat(partitionKeys.size()) +
          "?)";

      try (PreparedStatement ps = duckdbConn.prepareStatement(insertSql)) {
        for (int i = 0; i < partitionVars.size(); i++) {
          Map<String, String> pVars = partitionVars.get(i);
          int paramIndex = 1;
          for (String key : partitionKeys) {
            ps.setString(paramIndex++, pVars.get(key));
          }
          ps.setInt(paramIndex, i);
          ps.addBatch();
        }
        ps.executeBatch();
      }
    }
  }

  /**
   * Helper method to generate a list of year strings from start to end (inclusive).
   * Useful for providing year dimension values via DimensionProvider.
   *
   * @param startYear First year (inclusive)
   * @param endYear Last year (inclusive)
   * @return List of year strings
   */
  protected static List<String> yearRange(int startYear, int endYear) {
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    return years;
  }

  /**
   * Represents a single dimension of iteration with variable name and values.
   * Used for multidimensional iteration over download/conversion operations.
   */
  public static class IterationDimension {
    final String variableName;
    final List<String> values;

    public IterationDimension(String variableName, java.util.Collection<String> values) {
      this.variableName = variableName;
      this.values = new ArrayList<>(values);
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
  /** Schema resource name (e.g., "/econ/econ-schema.json", "/geo/geo-schema.json") */
  protected final String schemaResourceName;

  /** Cache manifest for tracking downloads and conversions */
  protected AbstractCacheManifest cacheManifest;

  /** Shared mapper for JSON/YAML (initialized in constructor based on schema format) */
  protected final ObjectMapper MAPPER;

  /** JSON mapper for writing cache files (always uses JSON format regardless of schema format) */
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /** HTTP client for API/HTTP requests */
  protected final HttpClient httpClient;

  /** Timestamp of last request for rate limiting */
  protected long lastRequestTime = 0L;

  /** Cached rate limit configuration (loaded on first access) */
  private Map<String, Object> rateLimitConfig = null;

  /** Cache for SQL resource files (loaded once, reused across instances) */
  private static final Map<String, String> SQL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

  /** Start year for data downloads (from model operands) */
  protected final int startYear;

  /** End year for data downloads (from model operands) */
  protected final int endYear;

  protected AbstractGovDataDownloader(
      String cacheDirectory,
      String operatingDirectory,
      String parquetDirectory,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider,
      String schemaName,
      AbstractCacheManifest sharedManifest,
      int startYear,
      int endYear) {
    this.cacheManifest = sharedManifest;
    this.cacheDirectory = cacheDirectory;
    this.operatingDirectory = operatingDirectory;
    this.parquetDirectory = parquetDirectory;
    this.cacheStorageProvider = cacheStorageProvider;
    this.storageProvider = storageProvider;
    this.startYear = startYear;
    this.endYear = endYear;
    // Determine schema file extension: econ uses YAML, others still use JSON
    String schemaExtension = "econ".equals(schemaName) ? ".yaml" : ".json";
    this.schemaResourceName = "/" + schemaName + "/" + schemaName + "-schema" + schemaExtension;
    // Initialize JSON-only mapper (YAML parsing uses YamlUtils for proper anchor resolution)
    this.MAPPER = new ObjectMapper();
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
   * Converts an object to a Map, handling both JsonNode and Map types.
   * This is a common pattern when dealing with Jackson-parsed configuration objects.
   *
   * @param obj Object to convert (JsonNode or Map)
   * @return Map representation of the object
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> convertToMap(Object obj) {
    if (obj instanceof JsonNode) {
      return MAPPER.convertValue(obj, Map.class);
    }
    return (Map<String, Object>) obj;
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
    return 1000; // Default: 1-second retry delay
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

  // ===== Metadata-Driven Path Resolution =====

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

      // Parse YAML/JSON with proper anchor resolution
      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaResource);

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

          if (tableNode.has("dimensions")) {
            metadata.put("dimensions", tableNode.get("dimensions"));
          }

          if (tableNode.has("sourcePaths")) {
            metadata.put("sourcePaths", tableNode.get("sourcePaths"));
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
   * Loads bulk download configurations from the schema resource file.
   * Bulk downloads are shared download specifications that multiple tables can reference.
   *
   * <p>Schema structure:
   * <pre>
   * {
   *   "bulkDownloads": {
   *     "qcew_annual_bulk": {
   *       "cachePattern": "type=qcew_bulk/year={year}/frequency={frequency}/qcew.zip",
   *       "url": "https://data.bls.gov/cew/data/files/{year}/csv/{year}_{frequency}_singlefile.zip",
   *       "variables": ["year", "frequency"],
   *       "comment": "QCEW bulk CSV download shared by multiple tables"
   *     }
   *   }
   * }
   * </pre>
   *
   * @return Map of bulk download name to BulkDownloadConfig, or empty map if no bulkDownloads section
   * @throws RuntimeException if schema file cannot be loaded or parsed
   */
  protected Map<String, BulkDownloadConfig> loadBulkDownloads() {
    try {
      InputStream schemaStream = getClass().getResourceAsStream(schemaResourceName);
      if (schemaStream == null) {
        throw new IllegalStateException(
            "Could not find " + schemaResourceName + " resource file");
      }

      // Parse YAML/JSON with proper anchor resolution
      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaResourceName);

      if (!root.has("bulkDownloads") || root.get("bulkDownloads").isNull()) {
        LOGGER.debug("No 'bulkDownloads' section found in schema, returning empty map");
        return new HashMap<>();
      }

      JsonNode bulkDownloadsNode = root.get("bulkDownloads");
      Map<String, BulkDownloadConfig> bulkDownloads = new HashMap<>();

      bulkDownloadsNode.fields().forEachRemaining(entry -> {
        String name = entry.getKey();
        JsonNode config = entry.getValue();

        String cachePattern = config.has("cachePattern") ? config.get("cachePattern").asText() : null;
        String url = config.has("url") ? config.get("url").asText() : null;

        List<String> variables = new ArrayList<>();
        if (config.has("variables") && config.get("variables").isArray()) {
          config.get("variables").forEach(v -> variables.add(v.asText()));
        }

        String comment = config.has("comment") ? config.get("comment").asText() : null;

        BulkDownloadConfig bulkDownload = new BulkDownloadConfig(name, cachePattern, url, variables, comment);
        bulkDownloads.put(name, bulkDownload);

        LOGGER.debug("Loaded bulk download config: {}", bulkDownload);
      });

      LOGGER.info("Loaded {} bulk download configurations from {}", bulkDownloads.size(), schemaResourceName);
      return bulkDownloads;

    } catch (IOException e) {
      throw new RuntimeException("Error loading bulk downloads from " + schemaResourceName, e);
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
            objectNode.fields().forEachRemaining(entry -> result.put(entry.getKey(), entry.getValue()));
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
      // Pattern doesn't end with .parquet - expected for reference tables with cachePattern
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
  protected String substitutePatternVariables(String pattern, Map<String, String> variables) {
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
      String paramValue;

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
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
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
  protected DownloadResult executeDownload(String tableName, Map<String, String> variables)
      throws IOException, InterruptedException {
    // Load metadata including download config
    Map<String, Object> metadata = loadTableMetadata(tableName);

    if (!metadata.containsKey("download")) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' does not have download configuration in schema");
    }

    // Convert JsonNode to Map<String, Object> for download config
    Object downloadObj = metadata.get("download");
    Map<String, Object> downloadConfig = convertToMap(downloadObj);

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
      Map<String, Object> paginationConfig = convertToMap(paginationObj);
      Object enabledPagination = paginationConfig.get("enabled");
      paginationEnabled = enabledPagination != null && Boolean.parseBoolean(enabledPagination.toString());
      if (paginationConfig.containsKey("maxPerRequest")) {
        maxPerRequest = Integer.parseInt(paginationConfig.get("maxPerRequest").toString());
      }
    }

    // Check if cached JSON already exists (skip re-downloading reference data)
    // Use cachePattern from download config if available, otherwise use table pattern
    String pattern;
    if (downloadConfig.containsKey("cachePattern")) {
      pattern = (String) downloadConfig.get("cachePattern");
    } else {
      pattern = (String) metadata.get("pattern");
    }
    String jsonPath = resolveJsonPath(pattern, variables);
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    // Check if already cached - skip download if so
    CacheKey cacheKey = new CacheKey(tableName, variables);
    if (cacheManifest.isCached(cacheKey)) {
      LOGGER.debug("Using cached {} data", tableName);
      // Return the cached file path without re-downloading
      try {
        long fileSize = cacheStorageProvider.getMetadata(fullJsonPath).getSize();
        return new DownloadResult(jsonPath, fileSize);
      } catch (Exception e) {
        LOGGER.warn("Cached file {} exists in manifest but cannot read metadata: {}, will re-download",
            jsonPath, e.getMessage());
        // Continue with download if we can't read the cached file
      }
    }

    // Self-healing: Check if file exists but isn't in manifest
    // If it does, add it to manifest and skip download (avoids re-downloading existing files)
    if (cacheStorageProvider.exists(fullJsonPath)) {
      LOGGER.info("Self-healing: Found existing file {} not in manifest, adding to manifest", jsonPath);
      try {
        long fileSize = cacheStorageProvider.getMetadata(fullJsonPath).getSize();
        // Mark as cached with immutable policy for reference data (very long TTL)
        cacheManifest.markCached(cacheKey, jsonPath, fileSize, Long.MAX_VALUE, "reference_immutable");
        cacheManifest.save(operatingDirectory);
        LOGGER.debug("Self-healed cache manifest for {}", jsonPath);

        // Return the existing file path without re-downloading
        return new DownloadResult(jsonPath, fileSize);
      } catch (Exception e) {
        LOGGER.warn("Failed to self-heal manifest for {}: {}, will re-download", jsonPath, e.getMessage());
        // Continue with download if self-healing fails
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

    // Filter out null entries - treat [null] as [] (empty array)
    // Some APIs return [null] to indicate "no data available" which should be normalized to empty
    List<JsonNode> filteredData = new ArrayList<>();
    for (JsonNode node : allData) {
      if (node != null && !node.isNull()) {
        filteredData.add(node);
      }
    }

    if (filteredData.size() < allData.size()) {
      LOGGER.info("Filtered out {} null entries, keeping {} valid records",
          allData.size() - filteredData.size(), filteredData.size());
    }

    // Write aggregated data to JSON cache file
    // (pattern, jsonPath, fullJsonPath already resolved earlier for cache check)

    // Write as JSON array - use ByteArrayOutputStream then writeFile
    // Always use JSON_MAPPER (not MAPPER) to ensure cache files are JSON regardless of schema format
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    JSON_MAPPER.writeValue(baos, filteredData);
    byte[] data = baos.toByteArray();
    cacheStorageProvider.writeFile(fullJsonPath, data);

    LOGGER.info("Wrote {} records ({} bytes) to {}", filteredData.size(), data.length, jsonPath);
    return new DownloadResult(jsonPath, data.length);
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

      // Parse response - Always use JSON_MAPPER since API responses are always JSON
      // (regardless of schema format which might be YAML)
      JsonNode rootNode = JSON_MAPPER.readTree(response.body());

      // Check for error response (HTTP 200 with error content) before extracting data
      if (downloadConfig.containsKey("response")) {
        Object responseObj = downloadConfig.get("response");
        Map<String, Object> responseConfig = convertToMap(responseObj);

        // Check for errorPath first
        if (responseConfig.containsKey("errorPath")) {
          String errorPath = responseConfig.get("errorPath").toString();
          JsonNode errorNode = navigateJsonPath(rootNode, errorPath);

          if (!errorNode.isMissingNode()) {
            // API returned an error in the response body (even though HTTP status was 200)
            String errorMsg = errorNode.toString();
            LOGGER.error("API returned error response (HTTP 200 with error content): {}", errorMsg);
            throw new IOException("API returned error (HTTP 200 with error content): " + errorMsg);
          }
        }
      }

      // Extract data from response using dataPath if specified
      JsonNode dataNode = rootNode;
      if (downloadConfig.containsKey("response")) {
        Object responseObj = downloadConfig.get("response");
        Map<String, Object> responseConfig = convertToMap(responseObj);
        if (responseConfig.containsKey("dataPath")) {
          String dataPath = responseConfig.get("dataPath").toString();
          // Navigate nested path (e.g., "BEAAPI.Results.ParamValue")
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
   * Navigates a JSON path string (e.g., "BEAAPI.Results.Error") through a JSON node tree.
   * Returns the node at the specified path, or a MissingNode if any segment is not found.
   *
   * @param root The root JSON node to navigate from
   * @param path The dot-separated path string (e.g., "BEAAPI.Results.Data")
   * @return The JSON node at the specified path, or MissingNode if not found
   */
  private JsonNode navigateJsonPath(JsonNode root, String path) {
    JsonNode current = root;
    for (String segment : path.split("\\.")) {
      current = current.path(segment);
      if (current.isMissingNode()) {
        return current;
      }
    }
    return current;
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
   * @param cacheKey The cache key identifying the data
   * @return true if cached (skip download), false if it needs download
   */
  protected boolean isCachedOrExists(CacheKey cacheKey) {

    // 1. Check cache manifest first - trust it as source of truth
    if (cacheManifest.isCached(cacheKey)) {
      LOGGER.info("⚡ Cached (manifest: fresh ETag/TTL), skipped download: {}", cacheKey.asString());
      return true;
    }

    // 2. Defensive check: if file exists but not in manifest, update manifest
    String tableName = cacheKey.getTableName();
    Map<String, String> params = cacheKey.getParameters();
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String filePath = storageProvider.resolvePath(cacheDirectory, resolveJsonPath(metadata.get("pattern").toString(), params));
    try {
      if (cacheStorageProvider.exists(filePath)) {
        long fileSize = cacheStorageProvider.getMetadata(filePath).getSize();
        if (fileSize > 0) {
          LOGGER.info("⚡ JSON exists, updating cache manifest: {}", cacheKey.asString());
          // Calculate reasonable default refresh time (same logic as CacheManifest.markCached)
          int currentYear = java.time.LocalDate.now().getYear();
          String yearStr = params.get("year");
          int year = yearStr != null ? Integer.parseInt(yearStr) : currentYear;
          long refreshAfter;
          String refreshReason;
          if (year == currentYear) {
            refreshAfter = System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24);
            refreshReason = "current_year_daily";
          } else {
            refreshAfter = Long.MAX_VALUE;
            refreshReason = "historical_immutable";
          }
          cacheManifest.markCached(cacheKey, filePath, fileSize, refreshAfter, refreshReason);
          cacheManifest.save(operatingDirectory);
          return true;
        } else {
          LOGGER.warn("Found zero-byte cache file for {} at {} — will re-download instead of using cache.", tableName, filePath);
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
      String expression = colNode.has("expression") ? colNode.get("expression").asText() : null;

      if (colName != null) {
        columns.add(
            new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                colName, colType, nullable, comment, expression));
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
        Object convertedValue =
            convertJsonValueToType(field.getValue(), fieldName, columnType, missingValueIndicator);
        typedRecord.put(fieldName, convertedValue);
      } else {
        // Field not in schema - use Jackson's default conversion, but it will be DROPPED later
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
      String parquetPath,
      String dataPath) {
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
        // If dataPath is provided, prefix field references with row_data.
        String expression = column.getExpression();
        if (dataPath != null && !dataPath.isEmpty()) {
          expression = prefixFieldReferences(expression, "row_data");
        }
        sql.append("(");
        sql.append(expression);
        sql.append(") AS ");
        sql.append(quoteIdentifier(columnName));
      } else {
        // Regular column: CAST from JSON with type conversion
        String sqlType = javaToDuckDbType(column.getType());

        // Add row_data prefix if using UNNEST
        String columnRef = (dataPath != null && !dataPath.isEmpty())
            ? "row_data." + quoteIdentifier(columnName)
            : quoteIdentifier(columnName);

        // Handle missing value indicator with CASE expression
        if (missingValueIndicator != null && !missingValueIndicator.isEmpty()) {
          sql.append("CAST(CASE WHEN ");
          sql.append(columnRef);
          sql.append(" = ");
          sql.append(quoteLiteral(missingValueIndicator));
          sql.append(" THEN NULL ELSE ");
          sql.append(columnRef);
          sql.append(" END AS ");
          sql.append(sqlType);
          sql.append(") AS ");
          sql.append(quoteIdentifier(columnName));
        } else {
          // Simple CAST without null handling
          sql.append("CAST(");
          sql.append(columnRef);
          sql.append(" AS ");
          sql.append(sqlType);
          sql.append(") AS ");
          sql.append(quoteIdentifier(columnName));
        }
      }
    }

    // Build column type specification for read_json() to prevent type inference
    // This forces DuckDB to read all columns as VARCHAR, preventing DATE inference issues
    // Also collect fields referenced in expressions (e.g., "calculations" from "calculations.net_changes['1']")
    StringBuilder columnSpec = new StringBuilder();
    boolean firstColSpec = true;
    java.util.Set<String> referencedFields = new java.util.HashSet<>();

    // First pass: add regular columns and collect referenced fields from expressions
    for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
      if (column.hasExpression()) {
        // Extract field references from expression (e.g., "calculations" from "calculations.net_changes['1']")
        extractFieldReferences(column.getExpression(), referencedFields);
        continue; // Skip computed columns in the JSON reader output
      }
      if (!firstColSpec) {
        columnSpec.append(", ");
      }
      firstColSpec = false;
      columnSpec.append(quoteIdentifier(column.getName()));
      columnSpec.append(": 'VARCHAR'");
    }

    // Second pass: add referenced fields that aren't already in regular columns
    // These are source fields used in expressions but not output as regular columns
    // Read these as JSON type so DuckDB preserves nested structure
    for (String fieldName : referencedFields) {
      if (!firstColSpec) {
        columnSpec.append(", ");
      }
      firstColSpec = false;
      columnSpec.append(quoteIdentifier(fieldName));
      columnSpec.append(": 'JSON'"); // JSON type preserves nested object structure
    }

    // FROM clause with read_json()
    // When dataPath is specified (nested JSON), use auto-detection to preserve full structure
    // Otherwise, use explicit columns to prevent type inference issues
    sql.append("\n  FROM read_json(");
    sql.append(quoteLiteral(jsonPath));
    if (dataPath == null || dataPath.isEmpty()) {
      // No nesting - use explicit column types to prevent type inference issues
      sql.append(", columns={");
      sql.append(columnSpec);
      sql.append("}");
    }
    // When dataPath exists, let DuckDB auto-detect so nested paths are accessible
    sql.append(")");

    // Add UNNEST clauses if dataPath is specified (for nested array structures)
    if (dataPath != null && !dataPath.isEmpty()) {
      sql.append(buildUnnestClauses(dataPath));
    }

    sql.append("\n) TO ");
    sql.append(quoteLiteral(parquetPath));
    sql.append(" (FORMAT PARQUET);");

    return sql.toString();
  }

  /**
   * Maps Java/Calcite type names to DuckDB SQL types.
   */
  private static String javaToDuckDbType(String javaType) {
    String normalized = javaType.toLowerCase();

    // Handle array types (e.g., "array<double>", "array<varchar>")
    if (normalized.startsWith("array<") && normalized.endsWith(">")) {
      String elementType = normalized.substring(6, normalized.length() - 1);
      String duckDbElementType = javaToDuckDbType(elementType); // Recursive call for element type
      return duckDbElementType + "[]";
    }

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
   * Builds UNNEST clauses for nested JSON arrays based on dataPath.
   *
   * <p>For BLS data with dataPath "Results.series", generates:
   * <pre>
   * CROSS JOIN UNNEST(Results.series) AS series_item
   * CROSS JOIN UNNEST(series_item.data) AS row_data
   * </pre>
   *
   * @param dataPath JSON path to nested data (e.g., "Results.series")
   * @return SQL UNNEST clauses
   */
  private static String buildUnnestClauses(String dataPath) {
    StringBuilder unnest = new StringBuilder();

    // Parse the dataPath (e.g., "Results.series")
    String[] pathSegments = dataPath.split("\\.");

    if (pathSegments.length == 0) {
      return "";
    }

    // For BLS-specific pattern: Results.series[].data[]
    // First UNNEST the dataPath array (e.g., Results.series)
    unnest.append("\n  CROSS JOIN UNNEST(");
    unnest.append(dataPath);
    unnest.append(") AS series_item");

    // Then UNNEST the nested 'data' array within each series
    // This is BLS-specific but common pattern
    unnest.append("\n  CROSS JOIN UNNEST(series_item.data) AS row_data");

    return unnest.toString();
  }

  /**
   * Prefixes field references in an expression with a table alias.
   * For example, transforms "calculations.net_changes['1']" to "row_data.calculations.net_changes['1']".
   *
   * @param expression SQL expression
   * @param prefix Table alias to prepend (e.g., "row_data")
   * @return Expression with prefixed field references
   */
  private static String prefixFieldReferences(String expression, String prefix) {
    if (expression == null || expression.isEmpty()) {
      return expression;
    }

    // Pattern: identifier at word boundary not preceded by a dot
    // This catches: "calculations.net_changes" but not "row_data.calculations" (already prefixed)
    // Also avoids matching after CAST, CASE, etc.
    java.util.regex.Pattern pattern =
        java.util.regex.Pattern.compile("(?<![a-zA-Z0-9_.])([a-zA-Z_][a-zA-Z0-9_]*)\\.");
    java.util.regex.Matcher matcher = pattern.matcher(expression);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String fieldName = matcher.group(1);
      // Skip SQL keywords and already-prefixed references
      if (!isSqlKeyword(fieldName) && !fieldName.equals(prefix)) {
        matcher.appendReplacement(result, prefix + "." + fieldName + ".");
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Extracts field references from a SQL expression.
   * For example, extracts "calculations" from "CAST(calculations.net_changes['1'] AS DOUBLE)".
   *
   * @param expression SQL expression
   * @param referencedFields Set to add discovered field names to
   */
  private static void extractFieldReferences(String expression, java.util.Set<String> referencedFields) {
    if (expression == null || expression.isEmpty()) {
      return;
    }

    // Simple pattern: find identifiers followed by a dot
    // This catches: "calculations.net_changes" -> "calculations"
    // Pattern: word character followed by dot
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.");
    java.util.regex.Matcher matcher = pattern.matcher(expression);

    while (matcher.find()) {
      String fieldName = matcher.group(1);
      // Filter out SQL keywords and functions
      if (!isSqlKeyword(fieldName)) {
        referencedFields.add(fieldName);
      }
    }
  }

  /**
   * Checks if a string is a common SQL keyword or function name.
   */
  private static boolean isSqlKeyword(String word) {
    String upper = word.toUpperCase();
    return upper.equals("CAST") || upper.equals("CASE") || upper.equals("NULL") || upper.equals("WHEN")
        || upper.equals("THEN") || upper.equals("ELSE") || upper.equals("END");
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
   * Creates a DuckDB connection with all conversion extensions preloaded.
   * This is a shared utility method that can be used by any govdata downloader.
   *
   * <p>Extensions are loaded with graceful degradation - failures are logged as warnings
   * but don't prevent connection creation. This allows the system to work even if some
   * extensions are unavailable.</p>
   *
   * @return DuckDB connection with extensions loaded
   * @throws java.sql.SQLException if connection or extension loading fails
   */
  public static Connection getDuckDBConnection() throws java.sql.SQLException {
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
  private static void loadConversionExtensions(Connection conn) {
    String[][] extensions = {
        {"quackformers", "FROM community"},  // Embedding generation
        {"spatial", ""},                      // GIS operations
        {"h3", "FROM community"},             // Geospatial hex indexing
        {"excel", ""},                        // Excel file support
        {"fts", ""},                          // Full-text indexing
        {"zipfs", "FROM community"}           // ZIP file reading support
    };

    for (String[] ext : extensions) {
      try {
        conn.createStatement().execute("INSTALL " + ext[0] + " " + ext[1]);
        conn.createStatement().execute("LOAD " + ext[0]);
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
  private static String buildQuackformersGitHubUrl(Connection conn) throws java.sql.SQLException {
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
  private static String detectDuckDBVersion(Connection conn) throws java.sql.SQLException {
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
  private static String detectPlatform() {
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
   * Converts in-memory records directly to Parquet using DuckDB without temporary files.
   * This is an optimized version that inserts data directly into DuckDB.
   *
   * @param tableName Name of table (for logging)
   * @param columns Column definitions from schema
   * @param records List of data rows as maps
   * @param fullParquetPath Absolute path to output Parquet file
   * @throws IOException if conversion fails
   */
  protected void convertInMemoryToParquetViaDuckDB(
      String tableName,
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> records,
      String fullParquetPath) throws IOException {

    if (records == null || records.isEmpty()) {
      LOGGER.warn("No records to convert for table {}", tableName);
      return;
    }

    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      // Build CREATE TABLE statement from schema columns
      StringBuilder createTable = new StringBuilder("CREATE TEMPORARY TABLE temp_data (");
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) createTable.append(", ");
        org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn col = columns.get(i);
        createTable.append(col.getName()).append(" ").append(mapToDuckDBType(col.getType()));
      }
      createTable.append(")");
      stmt.execute(createTable.toString());

      // Insert data in batches using prepared statement
      String insertSql = buildInsertStatement(columns);
      try (java.sql.PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        int batchCount = 0;
        for (Map<String, Object> row : records) {
          for (int i = 0; i < columns.size(); i++) {
            Object value = row.get(columns.get(i).getName());
            pstmt.setObject(i + 1, value);
          }
          pstmt.addBatch();
          batchCount++;

          // Execute batch every 1000 rows for memory efficiency
          if (batchCount >= 1000) {
            pstmt.executeBatch();
            batchCount = 0;
          }
        }
        // Execute remaining batch
        if (batchCount > 0) {
          pstmt.executeBatch();
        }
      }

      // Export to Parquet
      String exportSql =
          String.format("COPY (SELECT * FROM temp_data) TO '%s' (FORMAT PARQUET, COMPRESSION 'SNAPPY')",
          fullParquetPath.replace("'", "''"));
      stmt.execute(exportSql);

      LOGGER.info("Converted {} records to Parquet for table {}: {}", records.size(), tableName, fullParquetPath);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("Failed to convert in-memory data to Parquet for table '%s': %s", tableName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
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
      String fullParquetPath,
      String dataPath) throws IOException {

    // Build the SQL statement
    String sql = buildDuckDBConversionSql(columns, missingValueIndicator, fullJsonPath, fullParquetPath, dataPath);

    LOGGER.debug("DuckDB conversion SQL:\n{}", sql);

    // Execute using in-memory DuckDB connection with extensions loaded
    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      // Execute the COPY statement
      stmt.execute(sql);

      LOGGER.info("Successfully converted {} to Parquet using DuckDB", tableName);

    } catch (java.sql.SQLException e) {
      // Wrap SQLException as IOException for consistent error handling
      String errorMsg =
          String.format("DuckDB conversion failed for table '%s': %s",
          tableName,
          e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Converts a list of maps directly to Parquet using DuckDB without temporary files.
   * Inserts data directly into DuckDB and exports to Parquet.
   *
   * <p>This is useful for reference tables or any data already in memory as a List of Maps.
   * Unlike the temp-file approach, this method:
   * <ul>
   *   <li>Does not create intermediate JSON files</li>
   *   <li>Loads data directly into DuckDB via batch INSERT</li>
   *   <li>Exports to Parquet in a single operation</li>
   * </ul>
   *
   * @param rows List of data rows as maps
   * @param parquetPath Full path to output Parquet file
   * @param tableName Table name for loading column metadata
   * @throws IOException if conversion fails or column metadata is missing
   */
  protected void convertListToParquet(List<Map<String, Object>> rows, String parquetPath, String tableName)
      throws IOException {

    if (rows == null || rows.isEmpty()) {
      LOGGER.warn("No data to convert for table {}", tableName);
      return;
    }

    // Load column metadata from schema
    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumnsFromMetadata(tableName);

    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      // Build CREATE TABLE statement from schema columns
      StringBuilder createTable = new StringBuilder("CREATE TEMPORARY TABLE temp_data (");
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) createTable.append(", ");
        org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn col = columns.get(i);
        createTable.append(col.getName()).append(" ").append(mapToDuckDBType(col.getType()));
      }
      createTable.append(")");
      stmt.execute(createTable.toString());

      // Insert data in batches using prepared statement
      String insertSql = buildInsertStatement(columns);
      try (java.sql.PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        int batchCount = 0;
        for (Map<String, Object> row : rows) {
          for (int i = 0; i < columns.size(); i++) {
            Object value = row.get(columns.get(i).getName());
            pstmt.setObject(i + 1, value);
          }
          pstmt.addBatch();
          batchCount++;

          // Execute batch every 1000 rows for memory efficiency
          if (batchCount >= 1000) {
            pstmt.executeBatch();
            batchCount = 0;
          }
        }
        // Execute remaining batch
        if (batchCount > 0) {
          pstmt.executeBatch();
        }
      }

      // Export to Parquet
      String exportSql =
          String.format("COPY (SELECT * FROM temp_data) TO '%s' (FORMAT PARQUET, COMPRESSION 'SNAPPY')",
          parquetPath.replace("'", "''"));
      stmt.execute(exportSql);

      LOGGER.info("Converted {} rows to Parquet: {}", rows.size(), parquetPath);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("Failed to convert list to Parquet for table '%s': %s", tableName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Maps schema type to DuckDB type.
   */
  private String mapToDuckDBType(String schemaType) {
    String upperType = schemaType.toUpperCase();
    if (upperType.startsWith("VARCHAR") || upperType.equals("STRING")) {
      return "VARCHAR";
    } else if (upperType.equals("INTEGER") || upperType.equals("INT")) {
      return "INTEGER";
    } else if (upperType.equals("BIGINT") || upperType.equals("LONG")) {
      return "BIGINT";
    } else if (upperType.equals("DOUBLE") || upperType.equals("FLOAT")) {
      return "DOUBLE";
    } else if (upperType.equals("BOOLEAN")) {
      return "BOOLEAN";
    } else if (upperType.equals("DATE")) {
      return "DATE";
    } else if (upperType.equals("TIMESTAMP")) {
      return "TIMESTAMP";
    }
    return "VARCHAR"; // Default fallback
  }

  /**
   * Builds INSERT statement for batch insertion.
   */
  private String buildInsertStatement(
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns) {
    StringBuilder sb = new StringBuilder("INSERT INTO temp_data (");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(columns.get(i).getName());
    }
    sb.append(") VALUES (");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append("?");
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Executes arbitrary DuckDB SQL statement with extensions loaded.
   *
   * <p>This is a generic helper for executing DuckDB operations including:
   * <ul>
   *   <li>CSV→Parquet conversions via COPY (SELECT ... FROM read_csv()) TO ... </li>
   *   <li>Custom data transformations with SQL expressions</li>
   *   <li>Multi-source JOINs with reference data</li>
   * </ul>
   *
   * @param sql DuckDB SQL statement to execute
   * @param operationDescription Brief description for logging (e.g., "CSV to Parquet conversion")
   * @throws IOException if SQL execution fails
   */
  protected void executeDuckDBSql(String sql, String operationDescription) throws IOException {
    LOGGER.info("{} - Executing DuckDB SQL:\n{}", operationDescription, sql);

    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      // Execute the SQL statement
      stmt.execute(sql);

      LOGGER.info("{} completed successfully", operationDescription);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("%s failed: %s (SQL State: %s, Error Code: %d)",
          operationDescription,
          e.getMessage(),
          e.getSQLState(),
          e.getErrorCode());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Converts CSV data (potentially inside ZIP archives) to Parquet format using metadata-driven configuration.
   *
   * <p>This method supports direct CSV→Parquet conversion without intermediate JSON files,
   * leveraging DuckDB's zipfs extension to read compressed files and apply SQL filters.
   *
   * <h3>Metadata Configuration</h3>
   * <p>The table's download section should contain a csvConversion object:
   * <pre>
   * "download": {
   *   "csvConversion": {
   *     "sourcePattern": "type=qcew_bulk/year={year}/{frequency}_singlefile.zip",
   *     "csvPath": "{year}_{frequency}_singlefile.csv",
   *     "filterConditions": [
   *       "agglvl_code = '80'",
   *       "own_code = '0'",
   *       "industry_code = '10'",
   *       "area_fips IN ('C1206', 'C3100', ...)"
   *     ],
   *     "columnMappings": {
   *       "area_fips": "metro_code",
   *       "year": "year",
   *       "qtr": "qtr",
   *       "avg_wkly_wage": "value"
   *     },
   *     "computedColumns": {
   *       "metro_name": "CASE area_fips WHEN 'C1206' THEN 'Atlanta' ... END"
   *     }
   *   }
   * }
   * </pre>
   *
   * @param tableName Name of table in schema
   * @param variables Variables for path resolution (e.g., {year: "2020", frequency: "quarterly"})
   * @throws IOException if file operations fail or metadata is invalid
   */
  public void convertCsvToParquet(String tableName, Map<String, String> variables)
      throws IOException {
    LOGGER.info("Converting CSV to Parquet for table: {}", tableName);

    // Load metadata
    Map<String, Object> metadata = loadTableMetadata(tableName);

    // Extract csvConversion configuration
    if (!metadata.containsKey("download")) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'download' section in schema");
    }

    JsonNode downloadNode = (JsonNode) metadata.get("download");
    JsonNode csvConversionNode = downloadNode.get("csvConversion");

    if (csvConversionNode == null || csvConversionNode.isNull()) {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'csvConversion' configuration in download section");
    }

    // Extract configuration values
    String sourcePattern;

    // Check if this table uses a bulkDownload reference
    if (downloadNode.has("bulkDownload")) {
      String bulkDownloadName = downloadNode.get("bulkDownload").asText();
      LOGGER.debug("Resolving bulkDownload reference: {}", bulkDownloadName);

      // Load bulk downloads from schema
      Map<String, BulkDownloadConfig> bulkDownloads = loadBulkDownloads();
      BulkDownloadConfig bulkConfig = bulkDownloads.get(bulkDownloadName);

      if (bulkConfig == null) {
        throw new IllegalArgumentException(
            "Table '" + tableName + "' references unknown bulkDownload: '" + bulkDownloadName + "'");
      }

      sourcePattern = bulkConfig.getCachePattern();
      LOGGER.debug("Resolved sourcePattern from bulkDownload '{}': {}", bulkDownloadName, sourcePattern);
    } else if (csvConversionNode.has("sourcePattern")) {
      // Fall back to explicit sourcePattern in csvConversion
      sourcePattern = csvConversionNode.get("sourcePattern").asText();
    } else {
      throw new IllegalArgumentException(
          "Table '" + tableName + "' has no 'sourcePattern' in csvConversion and no 'bulkDownload' reference");
    }

    String csvPath = csvConversionNode.has("csvPath") ? csvConversionNode.get("csvPath").asText() : null;

    // Resolve source ZIP path and target Parquet path
    String resolvedSourcePattern = substitutePatternVariables(sourcePattern, variables);
    String fullSourcePath = cacheStorageProvider.resolvePath(cacheDirectory, resolvedSourcePattern);

    String pattern = (String) metadata.get("pattern");
    String fullParquetPath = storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

    LOGGER.info("Converting {} to {}", fullSourcePath, fullParquetPath);

    // Check if source exists
    if (!cacheStorageProvider.exists(fullSourcePath)) {
      LOGGER.warn("Source file not found: {}", fullSourcePath);
      return;
    }

    // Load column metadata
    List<PartitionedTableConfig.TableColumn> columns = loadTableColumnsFromMetadata(tableName);

    // Build SQL for CSV→Parquet conversion
    String sql =
        buildCsvToParquetSql(tableName,
        fullSourcePath,
        csvPath != null ? substitutePatternVariables(csvPath, variables) : null,
        fullParquetPath,
        columns,
        csvConversionNode);

    // Execute conversion
    executeDuckDBSql(sql, "CSV to Parquet conversion for " + tableName);

    // Verify file was written
    if (storageProvider.exists(fullParquetPath)) {
      LOGGER.info("Successfully converted {} to Parquet: {}", tableName, fullParquetPath);
    } else {
      LOGGER.error("Parquet file not found after conversion: {}", fullParquetPath);
      throw new IOException("Parquet file not found after write: " + fullParquetPath);
    }
  }

  /**
   * Builds DuckDB SQL for CSV→Parquet conversion with filters and column mappings.
   *
   * <p>Column handling:
   * <ul>
   *   <li>If column has {@code expression} field: use the expression as-is</li>
   *   <li>If column has {@code csvColumn} field: read from that CSV column name with type casting</li>
   *   <li>Otherwise: read from CSV column with same name as output column</li>
   * </ul>
   */
  private String buildCsvToParquetSql(
      String tableName,
      String sourcePath,
      String csvPath,
      String targetPath,
      List<PartitionedTableConfig.TableColumn> columns,
      JsonNode csvConversionNode) {

    StringBuilder sql = new StringBuilder("COPY (\n  SELECT\n");

    // Build SELECT clause from column definitions
    boolean first = true;
    for (PartitionedTableConfig.TableColumn col : columns) {
      if (col.isComputed()) {
        continue; // Skip partition columns marked as computed
      }

      if (!first) {
        sql.append(",\n");
      }
      first = false;

      String colName = col.getName();

      // Check if this column has an expression
      if (col.getExpression() != null && !col.getExpression().isEmpty()) {
        // Use the expression directly (already contains type handling)
        sql.append("    (").append(col.getExpression()).append(") AS ").append(colName);
      }
      // Check if this column has a csvColumn attribute (different CSV name)
      else if (col.getCsvColumn() != null && !col.getCsvColumn().isEmpty()) {
        // Map from CSV column name with type casting
        String castExpr = buildTypeCastExpression(col.getCsvColumn(), col.getType());
        sql.append("    ").append(castExpr).append(" AS ").append(colName);
      } else {
        // Direct mapping: CSV column name == output column name
        String castExpr = buildTypeCastExpression(colName, col.getType());
        sql.append("    ").append(castExpr).append(" AS ").append(colName);
      }
    }

    // Add FROM clause with zipfs path or direct CSV path
    sql.append("\n  FROM read_csv('");

    if (csvPath != null && sourcePath.toLowerCase().endsWith(".zip")) {
      // Use zipfs extension to read CSV inside ZIP
      sql.append("zipfs://").append(sourcePath).append("/").append(csvPath);
    } else {
      // Direct CSV file
      sql.append(sourcePath);
    }

    sql.append("', AUTO_DETECT=TRUE, HEADER=TRUE)");

    // Add WHERE clause with filter conditions
    if (csvConversionNode.has("filterConditions")) {
      JsonNode filtersNode = csvConversionNode.get("filterConditions");
      if (filtersNode.isArray() && !filtersNode.isEmpty()) {
        sql.append("\n  WHERE ");
        boolean firstFilter = true;
        for (JsonNode filterNode : filtersNode) {
          if (!firstFilter) {
            sql.append("\n    AND ");
          }
          firstFilter = false;
          sql.append(filterNode.asText());
        }
      }
    }

    sql.append("\n) TO '").append(targetPath).append("' (FORMAT PARQUET);");

    return sql.toString();
  }

  /**
   * Builds a type cast expression for a CSV column.
   */
  private String buildTypeCastExpression(String columnName, String targetType) {
    // Map Calcite types to DuckDB types
    String duckdbType = targetType.toUpperCase();

    if (duckdbType.startsWith("VARCHAR") || duckdbType.equals("STRING")) {
      return columnName; // No cast needed for strings
    } else if (duckdbType.equals("INTEGER") || duckdbType.equals("INT")) {
      return "CAST(" + columnName + " AS INTEGER)";
    } else if (duckdbType.equals("BIGINT") || duckdbType.equals("LONG")) {
      return "CAST(" + columnName + " AS BIGINT)";
    } else if (duckdbType.equals("DOUBLE") || duckdbType.equals("FLOAT")) {
      return "CAST(" + columnName + " AS DOUBLE)";
    } else if (duckdbType.equals("DATE")) {
      return "CAST(" + columnName + " AS DATE)";
    } else if (duckdbType.equals("TIMESTAMP")) {
      return "CAST(" + columnName + " AS TIMESTAMP)";
    } else {
      // Default: try to cast to the target type
      return "CAST(" + columnName + " AS " + duckdbType + ")";
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
    String dataPath = null;
    if (metadata.containsKey("download")) {
      JsonNode downloadNode = (JsonNode) metadata.get("download");
      JsonNode missingValueNode = downloadNode.at("/response/missingValueIndicator");
      if (!missingValueNode.isMissingNode()) {
        missingValueIndicator = missingValueNode.asText();
        LOGGER.info("Using missingValueIndicator: '{}'", missingValueIndicator);
      }

      // NOTE: dataPath is NOT used during conversion because the cached JSON
      // is already flattened - dataPath extraction happened during download.
      // The dataPath in response config is for extracting from API response,
      // not for reading from cache.
    }

    // Use DuckDB for JSON→Parquet conversion with expression columns
    LOGGER.info("Using DuckDB for JSON to Parquet conversion with expression columns");
    convertCachedJsonToParquetViaDuckDB(tableName, columns, missingValueIndicator,
        fullJsonPath, fullParquetPath, dataPath);

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
   */
  public void consolidateAll() {
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

      // Parse YAML/JSON with proper anchor resolution
      JsonNode root = YamlUtils.parseYamlOrJson(schemaStream, schemaResourceName);

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
    java.util.regex.Pattern varPattern = java.util.regex.Pattern.compile("\\{(\\w+)}");

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
    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {

      stmt.execute(sql);
      LOGGER.info("Successfully consolidated trend: {}", trend.trendName);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("DuckDB consolidation failed for trend '%s': %s",
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
    result = result.replaceAll("year=\\{year}", "year=*");

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
    return "COPY (\n"
  +
        "  SELECT * FROM read_parquet(" + quoteLiteral(sourceGlob) + ")\n"
  +
        "  ORDER BY year\n"
  +
        ") TO " + quoteLiteral(targetPath) + " (FORMAT PARQUET);";
  }

  /**
   * Extracts fixed dimension values from a partition pattern.
   * Fixed dimensions are those with concrete values (not wildcards).
   *
   * <p>Example: Given pattern "type=foo/frequency=WILDCARD/year=WILDCARD",
   * returns map {type: ["foo"]}.
   * Wildcards are skipped.
   *
   * @param pattern Partition pattern to parse
   * @return Map of dimension name to single-value list for each fixed dimension
   */
  private Map<String, List<String>> extractFixedDimensionsFromPattern(String pattern) {
    Map<String, List<String>> fixedDimensions = new HashMap<>();

    if (pattern == null || pattern.isEmpty()) {
      return fixedDimensions;
    }

    String[] parts = pattern.split("/");
    for (String part : parts) {
      if (part.contains("=") && !part.contains("=*")) {
        // Fixed value, not wildcard
        String dimName = part.substring(0, part.indexOf("="));
        String dimValue = part.substring(part.indexOf("=") + 1);

        // Remove file extension if present (e.g., "file.parquet" -> "file")
        if (dimValue.contains(".")) {
          dimValue = dimValue.substring(0, dimValue.indexOf("."));
        }

        fixedDimensions.put(dimName, Collections.singletonList(dimValue));
      }
    }

    return fixedDimensions;
  }

  /**
   * Creates a metadata-aware dimension provider that checks table metadata first,
   * then falls back to the provided lambda provider.
   *
   * <p>This enables dimension values to be defined in YAML using:
   * <ul>
   *   <li>Inline lists: dimensions.type: [value1, value2]</li>
   *   <li>YAML anchors: dimensions.frequency: *monthly_frequency</li>
   *   <li>Special yearRange type: dimensions.year: {type: yearRange, minYear: 1990}</li>
   * </ul>
   *
   * @param tableName Table name to load metadata for
   * @param dimensionProvider Fallback provider for dimensions not in metadata
   * @param startYear Start year for yearRange type (from schema config)
   * @param endYear End year for yearRange type (from schema config)
   * @return Dimension provider that checks metadata first
   */
  protected DimensionProvider createMetadataDimensionProvider(
      String tableName,
      DimensionProvider dimensionProvider,
      int startYear,
      int endYear) {

    // Load table metadata once
    Map<String, Object> tableMetadata = loadTableMetadata(tableName);

    // Debug: Log what keys are in the table metadata
    LOGGER.debug("Table metadata keys for {}: {}", tableName, tableMetadata.keySet());

    // Convert dimensions JsonNode to Map
    Object dimensionsObj = tableMetadata.get("dimensions");
    Map<String, Object> dimensionsMetadata = dimensionsObj != null ? convertToMap(dimensionsObj) : null;

    // Debug: Log what was loaded
    if (dimensionsMetadata != null) {
      LOGGER.debug("Loaded dimension metadata for table {}: {}", tableName, dimensionsMetadata.keySet());
    } else {
      LOGGER.warn("No dimension metadata found for table {} in schema. Available keys: {}",
          tableName, tableMetadata.keySet());
    }

    // Extract fixed dimensions from pattern (auto-generation)
    String pattern = (String) tableMetadata.get("pattern");
    Map<String, List<String>> fixedDimensions = extractFixedDimensionsFromPattern(pattern);
    return (dimensionName) -> {
      // Priority 1: Check explicit metadata dimensions first
      if (dimensionsMetadata != null && dimensionsMetadata.containsKey(dimensionName)) {
        Object dimValue = dimensionsMetadata.get(dimensionName);

        // Convert JsonNode to appropriate Java type if needed
        if (dimValue instanceof com.fasterxml.jackson.databind.node.ArrayNode) {
          dimValue = MAPPER.convertValue(dimValue, List.class);
        } else if (dimValue instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
          dimValue = MAPPER.convertValue(dimValue, Map.class);
        }

        // Case 1: List of string values (inline or YAML alias)
        if (dimValue instanceof List) {
          List<String> values = new ArrayList<>();
          for (Object item : (List<?>) dimValue) {
            values.add(String.valueOf(item));
          }
          LOGGER.debug("Dimension '{}' for table {} loaded from metadata: {} values",
              dimensionName, tableName, values.size());
          return values;
        }

        // Case 1b: String value (Jackson simplified single-element array)
        if (dimValue instanceof String) {
          List<String> values = new ArrayList<>();
          values.add((String) dimValue);
          LOGGER.debug("Dimension '{}' for table {} loaded from metadata (scalar): 1 value",
              dimensionName, tableName);
          return values;
        }

        // Case 2: Map with special type (e.g., yearRange, apiSet, apiList)
        if (dimValue instanceof Map) {
          Map<String, Object> dimConfig = (Map<String, Object>) dimValue;
          String type = (String) dimConfig.get("type");

          if ("yearRange".equals(type)) {
            // Apply minYear/maxYear constraints if specified
            int effectiveStartYear = startYear;
            int effectiveEndYear = endYear;

            if (dimConfig.containsKey("minYear")) {
              int minYear = ((Number) dimConfig.get("minYear")).intValue();
              effectiveStartYear = Math.max(startYear, minYear);
            }

            if (dimConfig.containsKey("maxYear")) {
              int maxYear = ((Number) dimConfig.get("maxYear")).intValue();
              effectiveEndYear = Math.min(endYear, maxYear);
            }

            List<String> years = yearRange(effectiveStartYear, effectiveEndYear);
            LOGGER.debug("Dimension '{}' for table {} computed as yearRange: {}-{} ({} values)",
                dimensionName, tableName, effectiveStartYear, effectiveEndYear, years.size());
            return years;
          }

          if ("apiSet".equals(type)) {
            // Reference an API set from download config (returns keys as values)
            String source = (String) dimConfig.get("source");
            if (source != null) {
              Map<String, Object> apiSet = extractApiSet(tableName, source);
              List<String> values = new ArrayList<>(apiSet.keySet());
              LOGGER.debug("Dimension '{}' for table {} loaded from API set '{}': {} values",
                  dimensionName, tableName, source, values.size());
              return values;
            }
          }

          if ("apiList".equals(type)) {
            // Reference an API list from download config
            String source = (String) dimConfig.get("source");
            if (source != null) {
              List<String> values = extractApiList(tableName, source);
              LOGGER.debug("Dimension '{}' for table {} loaded from API list '{}': {} values",
                  dimensionName, tableName, source, values.size());
              return values;
            }
          }

          LOGGER.warn("Unknown dimension type '{}' for dimension '{}' in table {}",
              type, dimensionName, tableName);
        }
        else {
          LOGGER.debug("Dimension '{}' for table {} has unknown dimension type: {}",
              dimensionName, tableName, dimValue.getClass().getName());
        }
      }

      // Priority 2: Check auto-generated fixed dimensions from pattern
      else if (fixedDimensions.containsKey(dimensionName)) {
        List<String> values = fixedDimensions.get(dimensionName);
        LOGGER.debug("Dimension '{}' for table {} auto-generated from pattern: {}",
            dimensionName, tableName, values);
        return values;
      }

      // Priority 3: Fall back to provided dimension provider
      List<String> values = dimensionProvider.getValues(dimensionName);
      if (values != null) {
        LOGGER.debug("Dimension '{}' for table {} loaded from provider: {} values",
            dimensionName, tableName, values.size());
        return values;
      }

      // Priority 4: No values found - throw error with helpful context
      Map<String, String> allAvailableDimensions = new HashMap<>();
      if (dimensionsMetadata != null) {
        for (String key : dimensionsMetadata.keySet()) {
          allAvailableDimensions.put(key, "metadata");
        }
      }
      for (String key : fixedDimensions.keySet()) {
        allAvailableDimensions.put(key, "pattern");
      }

      throw new IllegalArgumentException(
          "No values provided for dimension '" + dimensionName
          + "' in table '" + tableName + "'. "
          + "Dimension not found in metadata, pattern, or provider. "
          + "Available dimensions: " + allAvailableDimensions.keySet()
          + " (sources: " + allAvailableDimensions + ")");
    };
  }

  /**
   * Builds iteration dimensions from a table's partition pattern.
   * Extracts wildcard variables (e.g., frequency=*, year=*, tablename=*) and gets their values.
   *
   * @param tableName Table name to load pattern from
   * @param dimensionProvider Lambda to provide values for all dimensions (including year)
   * @param startYear Start year for yearRange type (from schema config)
   * @param endYear End year for yearRange type (from schema config)
   * @return List of IterationDimension objects in pattern order
   */
  private List<IterationDimension> buildDimensionsFromPattern(
      String tableName,
      DimensionProvider dimensionProvider,
      int startYear,
      int endYear) {

    // Load table metadata to get pattern
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    if (pattern == null || pattern.isEmpty()) {
      LOGGER.warn("No pattern found for table {}", tableName);
      return new ArrayList<>();
    }

    // Wrap dimensionProvider with metadata-aware provider that checks YAML dimensions first
    DimensionProvider metadataAwareProvider =
        createMetadataDimensionProvider(tableName, dimensionProvider, startYear, endYear);

    // Extract dimension names from pattern (variables with wildcards)
    // Pattern format: "type=xxx/frequency=*/year=*/tablename=*/file.parquet"
    List<IterationDimension> dimensions = new ArrayList<>();
    String[] parts = pattern.split("/");

    for (String part : parts) {
      if (part.contains("=")) {
        String dimName = part.substring(0, part.indexOf("="));

        // Use metadata-aware provider (checks YAML dimensions first, then falls back)
        List<String> values = metadataAwareProvider.getValues(dimName);
        if (values == null || values.isEmpty()) {
          throw new IllegalArgumentException(
              "No values provided for dimension '" + dimName + "' in table '" + tableName + "'. "
              + "Pattern requires this dimension but dimensionProvider returned null/empty. "
              + "Check that dimension values are configured in metadata or provided by downloader.");
        }

        dimensions.add(new IterationDimension(dimName, values));
        LOGGER.debug("Added dimension '{}' with {} values for table {}",
            dimName, values.size(), tableName);
      }
    }

    return dimensions;
  }

  /**
   * Optimized version of table iteration using DuckDB for cache filtering.
   * Delegates to overload with no prefetch callback.
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensionProvider Lambda that provides values for ALL dimensions (including year)
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationDescription Description for logging (e.g., "download", "conversion")
   */
  protected void iterateTableOperationsOptimized(
      String tableName,
      DimensionProvider dimensionProvider,
      TableOperation operation,
      OperationType operationType) {
    iterateTableOperationsOptimized(tableName, dimensionProvider, null, operation, operationType);
  }

  /**
   * Optimized version of table iteration using DuckDB for cache filtering with prefetch support.
   * Replaces row-by-row cache checking with single SQL query (10-20x faster for large sets).
   *
   * <p>Dimensions are automatically extracted from the table's partition pattern.
   * The dimensionProvider lambda is called for each dimension to get its values.
   * Use yearRange(start, end) helper for year dimensions.
   *
   * <p>Prefetch callback is called at the start of each dimension segment (including root),
   * enabling API batching optimizations (e.g., fetch 20 years in one API call instead of 20 calls).
   *
   * <p>Performance comparison for 25,000 combinations against 16,000 cached entries:
   * <ul>
   *   <li>Traditional: ~1-2 seconds (25,000 HashMap lookups)</li>
   *   <li>DuckDB: ~50-100ms (single hash join operation)</li>
   * </ul>
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensionProvider Lambda that provides values for ALL dimensions (including year)
   * @param prefetchCallback Optional callback for batching API calls (may be null)
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationType Type of operation (DOWNLOAD, CONVERSION, or DOWNLOAD_AND_CONVERT)
   */
  protected void iterateTableOperationsOptimized(
      String tableName,
      DimensionProvider dimensionProvider,
      PrefetchCallback prefetchCallback,
      TableOperation operation,
      OperationType operationType) {
    iterateTableOperationsOptimized(tableName, dimensionProvider, prefetchCallback, operation,
        operationType, null, null);
  }

  /**
   * Optimized version of table iteration with metadata-driven dimension support.
   * Wraps the dimension provider to check table metadata first (YAML dimensions section),
   * then falls back to the provided lambda.
   *
   * <p>This enables dimension values to be defined in YAML metadata:
   * <pre>
   * dimensions:
   *   type: [employment_statistics]
   *   frequency: *monthly_frequency  # YAML anchor reference
   *   year:
   *     type: yearRange
   *     minYear: 1990
   * </pre>
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensionProvider Fallback provider for dimensions not in metadata
   * @param prefetchCallback Optional callback for batching API calls
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationType Type of operation (DOWNLOAD, CONVERSION, or DOWNLOAD_AND_CONVERT)
   * @param startYear Start year for yearRange dimensions
   * @param endYear End year for yearRange dimensions
   */
  protected void iterateTableOperationsOptimized(
      String tableName,
      DimensionProvider dimensionProvider,
      PrefetchCallback prefetchCallback,
      TableOperation operation,
      OperationType operationType,
      int startYear,
      int endYear) {

    // Wrap provider with metadata-aware version
    DimensionProvider metadataProvider =
        createMetadataDimensionProvider(tableName, dimensionProvider, startYear, endYear);

    // Delegate to main method
    iterateTableOperationsOptimized(tableName, metadataProvider, prefetchCallback, operation,
        operationType, null, null);
  }

  /**
   * Optimized version of table iteration with explicit prefetch connection lifecycle management.
   * This overload accepts an existing prefetch connection and helper, allowing the caller to
   * manage the lifecycle (e.g., to share prefetch cache between download and conversion stages).
   *
   * <p>When prefetch connection/helper are provided:
   * <ul>
   *   <li>The provided prefetch infrastructure is used instead of creating a new one</li>
   *   <li>The connection is NOT closed when this method completes (caller manages lifecycle)</li>
   *   <li>The prefetchCallback parameter is ignored (prefetch table already exists)</li>
   * </ul>
   *
   * <p>This is used when you want to share the prefetch cache across multiple operations:
   * <pre>
   * Connection prefetchDb = getDuckDBConnection();
   * PrefetchHelper helper = new PrefetchHelper(prefetchDb, ...);
   * try {
   *   // Download stage - populate prefetch cache
   *   iterateTableOperationsOptimized(..., "download", prefetchDb, helper);
   *   // Conversion stage - use same prefetch cache
   *   iterateTableOperationsOptimized(..., "conversion", prefetchDb, helper);
   * } finally {
   *   prefetchDb.close();
   * }
   * </pre>
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensionProvider Lambda that provides values for ALL dimensions (including year)
   * @param prefetchCallback Optional callback for batching API calls (ignored if prefetch provided)
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationType Type of operation (DOWNLOAD, CONVERSION, or DOWNLOAD_AND_CONVERT)
   * @param prefetchDb Existing prefetch DuckDB connection (null to create new)
   * @param prefetchHelper Existing prefetch helper (null to create new)
   */
  protected void iterateTableOperationsOptimized(
      String tableName,
      DimensionProvider dimensionProvider,
      PrefetchCallback prefetchCallback,
      TableOperation operation,
      OperationType operationType,
      Connection prefetchDb,
      PrefetchHelper prefetchHelper) {
    // Delegate to overload with explicit year range using reasonable defaults
    int defaultStartYear = 1900;
    int defaultEndYear = java.time.LocalDate.now().getYear();
    iterateTableOperationsOptimized(tableName, dimensionProvider, prefetchCallback, operation,
        operationType, defaultStartYear, defaultEndYear, prefetchDb, prefetchHelper);
  }

  /**
   * Optimized version of table iteration with explicit year range AND prefetch lifecycle management.
   * This is the main implementation that all other overloads delegate to.
   *
   * @param tableName Table name for logging and manifest operations
   * @param dimensionProvider Lambda that provides values for dimensions not in YAML metadata
   * @param prefetchCallback Optional callback for batching API calls
   * @param operation Lambda to execute the operation (download or convert)
   * @param operationType Type of operation (DOWNLOAD, CONVERSION, or DOWNLOAD_AND_CONVERT)
   * @param startYear Start year for yearRange dimensions
   * @param endYear End year for yearRange dimensions
   * @param prefetchDb Existing prefetch DuckDB connection (null to create new)
   * @param prefetchHelper Existing prefetch helper (null to create new)
   */
  protected void iterateTableOperationsOptimized(
      String tableName,
      DimensionProvider dimensionProvider,
      PrefetchCallback prefetchCallback,
      TableOperation operation,
      OperationType operationType,
      int startYear,
      int endYear,
      Connection prefetchDb,
      PrefetchHelper prefetchHelper) {

    // Build dimensions from table pattern
    List<IterationDimension> dimensions =
        buildDimensionsFromPattern(tableName, dimensionProvider, startYear, endYear);

    if (dimensions.isEmpty()) {
      LOGGER.warn("No dimensions extracted from pattern for {} operations on {}",
          operationType.getValue(), tableName);
      return;
    }

    // Calculate total operations for progress tracking
    int totalOperations = 1;
    for (IterationDimension dim : dimensions) {
      totalOperations *= dim.values.size();
    }

    LOGGER.info("Starting {} operations for {} ({} total combinations, using DuckDB optimization)",
        operationType.getValue(), tableName, totalOperations);

    // Build allDimensionValues map unconditionally - needed for prefetch context and other uses
    Map<String, List<String>> allDimensionValues = new HashMap<>();
    for (IterationDimension dim : dimensions) {
      allDimensionValues.put(dim.variableName, new ArrayList<>(dim.values));
    }

    // Setup prefetch infrastructure
    // Use provided connection/helper if available, otherwise create new ones if callback provided
    Connection localPrefetchDb = prefetchDb;  // Track local vs provided
    PrefetchHelper localPrefetchHelper = prefetchHelper;
    boolean shouldClosePrefetch = false;  // Only close if we created it

    if (localPrefetchHelper == null && prefetchCallback != null) {
      // No prefetch provided but callback exists - create new prefetch infrastructure
      try {
        // Create in-memory DuckDB connection
        localPrefetchDb = getDuckDBConnection();
        shouldClosePrefetch = true;  // We created it, we close it

        // Load table metadata and extract partition keys
        Map<String, Object> metadata = loadTableMetadata(tableName);
        String pattern = (String) metadata.get("pattern");
        List<String> partitionKeys = extractPartitionKeysFromPattern(pattern);

        // Auto-create prefetch cache table
        createPrefetchCacheTable(localPrefetchDb, tableName, partitionKeys, metadata);

        // Create helper
        localPrefetchHelper = new PrefetchHelper(localPrefetchDb, tableName + "_prefetch", partitionKeys);

        LOGGER.info("Prefetch enabled for {} with {} partition keys", tableName, partitionKeys.size());

      } catch (Exception e) {
        LOGGER.warn("Failed to initialize prefetch for {}: {}", tableName, e.getMessage());
        if (localPrefetchDb != null && shouldClosePrefetch) {
          try {
            localPrefetchDb.close();
          } catch (Exception closeEx) {
            // Ignore
          }
        }
        localPrefetchDb = null;
        localPrefetchHelper = null;
        shouldClosePrefetch = false;
      }
    } else if (localPrefetchHelper != null) {
      // Using provided prefetch infrastructure
      LOGGER.info("Using provided prefetch infrastructure for {}", tableName);
    }

    final Connection finalPrefetchDb = localPrefetchDb;
    final PrefetchHelper finalPrefetchHelper = localPrefetchHelper;
    final boolean finalShouldClosePrefetch = shouldClosePrefetch;

    try {
      // 1. Generate all possible download combinations upfront
      List<CacheManifestQueryHelper.DownloadRequest> allRequests = new ArrayList<>();
      generateCombinationsRecursive(tableName, dimensions, 0, new HashMap<>(), allRequests,
          prefetchCallback, finalPrefetchHelper, allDimensionValues);

    LOGGER.debug("Generated {} download request combinations", allRequests.size());

    // 2. Filter using DuckDB SQL query (FAST!)
    List<CacheManifestQueryHelper.DownloadRequest> needed;
    try {
      long startMs = System.currentTimeMillis();
      String manifestPath = operatingDirectory + "/cache_manifest.json";
      needed = CacheManifestQueryHelper.filterUncachedRequestsOptimal(manifestPath, allRequests, operationType);
      long elapsedMs = System.currentTimeMillis() - startMs;

      LOGGER.info("Cache manifest filtering: {} uncached out of {} total ({}ms, {}% reduction)",
          needed.size(), allRequests.size(), elapsedMs,
          (int) ((1.0 - (double) needed.size() / allRequests.size()) * 100));

    } catch (Exception e) {
      LOGGER.error("DuckDB cache filtering failed for {} {}: {}",
          tableName, operationType.getValue(), e.getMessage());
      throw new RuntimeException("Failed to filter cached operations for " + tableName, e);
    }

    // 3. Execute only the needed operations
    int executed = 0;
    int skipped = allRequests.size() - needed.size();

    // Load table metadata once for path resolution
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (CacheManifestQueryHelper.DownloadRequest req : needed) {
      // Create cache key from table name and partition parameters
      // Ensure year is in parameters map (DownloadRequest stores it separately for legacy reasons)
      CacheKey cacheKey = new CacheKey(tableName, req.parameters);

      // Resolve paths using pattern (fully resolved, not relative)
      String relativeJsonPath = resolveJsonPath(pattern, req.parameters);
      String relativeParquetPath = resolveParquetPath(pattern, req.parameters);
      String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, relativeJsonPath);
      String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);

      // Self-healing: Before executing, check if source files already exist
      // This only runs for files the manifest said were needed, avoiding bulk scanning
      // Check for the appropriate source file based on table's acquisition method:
      // 1. Bulk download tables (ZIP files) - check for ZIP
      // 2. FTP-based reference tables - check for FTP file
      // 3. API-based tables (normal) - check for JSON
      try {
        String sourceFilePath = null;
        String sourceFileType = null;

        // Determine source file type and path
        JsonNode downloadNode = metadata.containsKey("download")
            ? (JsonNode) metadata.get("download")
            : null;

        // Check for bulk download (ZIP workflow)
        if (downloadNode != null && downloadNode.has("bulkDownload")) {
          String bulkDownloadName = downloadNode.get("bulkDownload").asText();
          Map<String, BulkDownloadConfig> bulkDownloads = loadBulkDownloads();
          BulkDownloadConfig bulkConfig = bulkDownloads.get(bulkDownloadName);

          if (bulkConfig != null) {
            String zipCachePath = bulkConfig.resolveCachePath(req.parameters);
            sourceFilePath = cacheStorageProvider.resolvePath(cacheDirectory, zipCachePath);
            sourceFileType = "ZIP";
          }
        }
        // Check for FTP-based reference table
        else if (metadata.containsKey("sourcePaths")) {
          JsonNode sourcePathsNode = (JsonNode) metadata.get("sourcePaths");
          if (sourcePathsNode != null && sourcePathsNode.has("ftpFiles")
              && sourcePathsNode.get("ftpFiles").isArray()
              && sourcePathsNode.get("ftpFiles").size() > 0) {
            JsonNode ftpFileNode = sourcePathsNode.get("ftpFiles").get(0);
            if (ftpFileNode.has("cachePath")) {
              String ftpCachePath = ftpFileNode.get("cachePath").asText();
              // Resolve variables in the cache path
              String resolvedPath = substitutePatternVariables(ftpCachePath, req.parameters);
              sourceFilePath = cacheStorageProvider.resolvePath(cacheDirectory, resolvedPath);
              sourceFileType = "FTP";
            }
          }
        }
        // Default: API-based workflow with JSON intermediate
        else {
          sourceFilePath = fullJsonPath;
          sourceFileType = "JSON";
        }

        // Check if source file exists (only for conversion operations)
        if (sourceFilePath != null && OperationType.CONVERSION.equals(operationType)
            && cacheStorageProvider.exists(sourceFilePath)) {

          LOGGER.info("Self-healing: Found existing {} source file for conversion: {}",
              sourceFileType, sourceFilePath);

          // For JSON files, add to prefetch table and mark in manifest
          if ("JSON".equals(sourceFileType)) {
            long fileSize = cacheStorageProvider.getMetadata(fullJsonPath).getSize();
            cacheManifest.markCached(cacheKey, relativeJsonPath, fileSize, Long.MAX_VALUE, "self_healed");

            // Read JSON and add to prefetch table so conversion can find it
            if (finalPrefetchHelper != null) {
              try (InputStream inputStream = cacheStorageProvider.openInputStream(fullJsonPath)) {
                byte[] bytes = new byte[inputStream.available()];
                inputStream.read(bytes);
                String jsonContent = new String(bytes, StandardCharsets.UTF_8);

                finalPrefetchHelper.insertJsonBatch(
                    Collections.singletonList(req.parameters),
                    Collections.singletonList(jsonContent));
                LOGGER.debug("Added self-healed JSON to prefetch table: {}", relativeJsonPath);
              } catch (Exception prefetchEx) {
                LOGGER.warn("Failed to add self-healed JSON to prefetch table: {}", prefetchEx.getMessage());
              }
            }

            // Save manifest immediately to persist self-healing discovery
            try {
              cacheManifest.save(operatingDirectory);
              LOGGER.debug("Saved manifest after self-healing {}", relativeJsonPath);
            } catch (Exception saveEx) {
              LOGGER.warn("Failed to save manifest after self-healing: {}", saveEx.getMessage());
            }
          }
          // For ZIP and FTP files, the specific downloaders handle their own caching
          // We just log that the file exists
        }
      } catch (Exception e) {
        // If self-healing check fails, just continue with normal operation
        LOGGER.debug("Self-healing check failed for {}: {}", cacheKey.asString(), e.getMessage());
      }

      try {
        operation.execute(cacheKey, req.parameters, fullJsonPath, fullParquetPath, finalPrefetchHelper);
        executed++;

        // Mark parquet as converted after successful operation
        if (OperationType.CONVERSION.equals(operationType)) {
          cacheManifest.markParquetConverted(cacheKey, relativeParquetPath);
        }

        if (executed % 10 == 0) {
          LOGGER.info("{} {}/{} operations (skipped {} cached)",
              operationType.getValue(), executed, needed.size(), skipped);
        }

      } catch (Exception e) {
        LOGGER.error("Error during {} for {} with cache key {}: {}",
            operationType.getValue(), tableName, cacheKey.asString(), e.getMessage());

        // Handle API errors (same as traditional method)
        if (e instanceof IOException && e.getMessage() != null
            && e.getMessage().contains("API returned error (HTTP 200 with error content)")) {
          String errorMessage = e.getMessage();
          if (errorMessage.startsWith("API returned error (HTTP 200 with error content): ")) {
            errorMessage = errorMessage.substring("API returned error (HTTP 200 with error content): ".length());
          }

          if (cacheManifest != null) {
            try {
              cacheManifest.markApiError(cacheKey, errorMessage, DEFAULT_API_ERROR_RETRY_DAYS);
              cacheManifest.save(operatingDirectory);
              LOGGER.info("Marked {} as API error - will retry in {} days",
                  cacheKey.asString(), DEFAULT_API_ERROR_RETRY_DAYS);
            } catch (Exception manifestError) {
              LOGGER.warn("Could not mark API error in cache manifest: {}",
                  manifestError.getMessage());
            }
          }
        }
      }
    }

      // 4. Save manifest after all operations complete (only if modified)
      try {
        assert cacheManifest != null;
        if (cacheManifest.isDirty()) {
          LOGGER.info("Saving cache manifest to {} after {} operations", operatingDirectory, executed);
          cacheManifest.save(operatingDirectory);
          LOGGER.info("Successfully saved cache manifest for {}", tableName);
        } else {
          LOGGER.debug("Cache manifest unchanged, skipping save for {}", tableName);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to save cache manifest for {}: {}", tableName, e.getMessage(), e);
      }

      LOGGER.info("{} {} complete: executed {} operations, skipped {} (cached)",
          tableName, operationType.getValue(), executed, skipped);

    } finally {
      // Cleanup prefetch DuckDB connection only if we created it
      if (finalPrefetchDb != null && finalShouldClosePrefetch) {
        try {
          finalPrefetchDb.close();
          LOGGER.debug("Closed prefetch DuckDB connection for {}", tableName);
        } catch (Exception e) {
          LOGGER.warn("Error closing prefetch connection: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Generates all download combinations from iteration dimensions.
   * Used by optimized cache filtering to build the complete request list upfront.
   *
   * @param tableName Table name for the requests
   * @param dimensions All iteration dimensions
   * @param dimensionIndex Current dimension being iterated
   * @param variables Variables map built so far
   * @param results Output list to collect all combinations
   */
  private void generateCombinationsRecursive(
      String tableName,
      List<IterationDimension> dimensions,
      int dimensionIndex,
      Map<String, String> variables,
      List<CacheManifestQueryHelper.DownloadRequest> results) {

    if (dimensionIndex >= dimensions.size()) {
      // Base case: add to results
      int year = 0;
      if (variables.containsKey("year")) {
        try {
          year = Integer.parseInt(variables.get("year"));
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid year value in variables: {}", variables.get("year"));
        }
      }
      results.add(new CacheManifestQueryHelper.DownloadRequest(tableName, variables));
      return;
    }

    // Recursive case
    IterationDimension dim = dimensions.get(dimensionIndex);
    for (String value : dim.values) {
      Map<String, String> next = new HashMap<>(variables);
      next.put(dim.variableName, value);
      generateCombinationsRecursive(tableName, dimensions, dimensionIndex + 1, next, results);
    }
  }

  /**
   * Generates all download combinations with prefetch callback support.
   * Calls prefetch callback at the start of each dimension segment.
   *
   * @param tableName Table name for the requests
   * @param dimensions All iteration dimensions
   * @param dimensionIndex Current dimension being iterated
   * @param variables Variables map built so far
   * @param results Output list to collect all combinations
   * @param prefetchCallback Optional prefetch callback
   * @param prefetchHelper Optional prefetch helper
   * @param allDimensionValues All dimension values for prefetch context
   */
  private void generateCombinationsRecursive(
      String tableName,
      List<IterationDimension> dimensions,
      int dimensionIndex,
      Map<String, String> variables,
      List<CacheManifestQueryHelper.DownloadRequest> results,
      PrefetchCallback prefetchCallback,
      PrefetchHelper prefetchHelper,
      Map<String, List<String>> allDimensionValues) {

    if (dimensionIndex >= dimensions.size()) {
      results.add(new CacheManifestQueryHelper.DownloadRequest(tableName, variables));
      return;
    }

    // PREFETCH CALLBACK - Called at start of each segment
    if (prefetchCallback != null && prefetchHelper != null) {
      IterationDimension currentDim = dimensions.get(dimensionIndex);
      PrefetchContext context =
          new PrefetchContext(tableName,
          currentDim.variableName,
          new HashMap<>(variables),  // Ancestor values chosen so far
          allDimensionValues);       // All dimension values (mutable)

      try {
        prefetchCallback.prefetch(context, prefetchHelper);
      } catch (Exception e) {
        LOGGER.warn("Prefetch failed for {} at segment {}: {}",
            tableName, currentDim.variableName, e.getMessage());
      }
    }

    // Recursive case - iterate current dimension
    IterationDimension dim = dimensions.get(dimensionIndex);
    for (String value : dim.values) {
      Map<String, String> next = new HashMap<>(variables);
      next.put(dim.variableName, value);
      generateCombinationsRecursive(tableName, dimensions, dimensionIndex + 1, next, results,
          prefetchCallback, prefetchHelper, allDimensionValues);
    }
  }

  /**
   * Extracts partition key names from a table pattern.
   * <p>Example: type=employment/frequency=STAR/year=STAR becomes [type, frequency, year]
   *
   * @param pattern The partition pattern from schema
   * @return List of partition key names in order
   */
  protected List<String> extractPartitionKeysFromPattern(String pattern) {
    List<String> keys = new ArrayList<>();
    if (pattern == null || pattern.isEmpty()) {
      return keys;
    }

    // Split by / and extract key names from key=value patterns
    // Include ALL keys (both fixed literals and wildcards) as they're all part of partitioning
    String[] parts = pattern.split("/");
    for (String part : parts) {
      int equalsIndex = part.indexOf('=');
      if (equalsIndex > 0) {
        String key = part.substring(0, equalsIndex);
        keys.add(key);
      }
    }
    return keys;
  }

  /**
   * Creates prefetch cache table in DuckDB with partition keys and schema columns.
   * Auto-generates schema from table metadata.
   *
   * @param connection DuckDB connection
   * @param tableName Table name
   * @param partitionKeys Partition key names
   * @param metadata Table metadata containing column definitions
   */
  protected void createPrefetchCacheTable(Connection connection, String tableName,
                                          List<String> partitionKeys,
                                          Map<String, Object> metadata) throws Exception {
    // Load column metadata
    List<PartitionedTableConfig.TableColumn> columns = loadTableColumnsFromMetadata(tableName);

    StringBuilder sql = new StringBuilder("CREATE TABLE ");
    sql.append(tableName).append("_prefetch (");

    // Add partition key columns
    for (String key : partitionKeys) {
      sql.append(key).append(" VARCHAR, ");
    }

    // Add schema data columns (excluding partition keys)
    for (PartitionedTableConfig.TableColumn col : columns) {
      if (!partitionKeys.contains(col.getName())) {
        String duckdbType = javaToDuckDbType(col.getType());
        sql.append(col.getName()).append(" ").append(duckdbType).append(", ");
      }
    }

    // Add raw format columns
    sql.append("_raw_json VARCHAR, ");
    sql.append("_raw_csv VARCHAR");
    sql.append(")");

    try (java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute(sql.toString());
      LOGGER.debug("Created prefetch cache table: {}", sql);
    }
  }

  // ===== SQL RESOURCE LOADING =====

  /**
   * Loads a SQL query from classpath resources.
   * Results are cached in memory for reuse across multiple calls.
   *
   * <p>SQL files use named parameter syntax: {@code {paramName}}
   * which can be substituted using {@link #substituteSqlParameters}.
   *
   * @param resourcePath Path to SQL file (e.g., "/sql/cache/load_manifest.sql")
   * @return SQL query text
   * @throws IllegalStateException if resource not found
   * @throws RuntimeException if I/O error occurs
   */
  protected String loadSqlResource(String resourcePath) {
    return SQL_CACHE.computeIfAbsent(resourcePath, path -> {
      try (InputStream is = getClass().getResourceAsStream(path)) {
        if (is == null) {
          throw new IllegalStateException("SQL resource not found: " + path);
        }
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load SQL resource: " + path, e);
      }
    });
  }

  /**
   * Substitutes named parameters in SQL template.
   * Replaces {@code {paramName}} with actual values from params map.
   *
   * <p>Example:
   * <pre>
   * String sql = loadSqlResource("/sql/cache/load_manifest.sql");
   * String resolved = substituteSqlParameters(sql,
   *     ImmutableMap.of("manifestPath", "/path/to/manifest.json"));
   * </pre>
   *
   * @param sqlTemplate SQL template with {@code {paramName}} placeholders
   * @param params Map of parameter names to values
   * @return SQL with parameters substituted
   */
  protected String substituteSqlParameters(String sqlTemplate, Map<String, String> params) {
    String result = sqlTemplate;
    for (Map.Entry<String, String> entry : params.entrySet()) {
      result = result.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  /**
   * Executes a SQL statement with named parameters using PreparedStatement.
   *
   * <p>SQL template should use {@code {paramName}} syntax which gets converted to ? placeholders.
   * Parameters are bound in the order they appear in the SQL template.
   *
   * <p>This method provides automatic SQL escaping and protection against SQL injection
   * by using JDBC PreparedStatement instead of string concatenation.
   *
   * @param conn Database connection
   * @param sqlTemplate SQL with {@code {paramName}} placeholders
   * @param params Map of parameter names to values
   * @return true if the first result is a ResultSet object; false if it is an update count or there are no results
   * @throws java.sql.SQLException if a database access error occurs
   */
  protected boolean executeWithParams(java.sql.Connection conn, String sqlTemplate, Map<String, String> params)
      throws java.sql.SQLException {

    // Track parameter order as we replace placeholders
    List<String> paramOrder = new ArrayList<>();

    // Replace {paramName} with ? and track order
    String sql = sqlTemplate;
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String placeholder = "\\{" + entry.getKey() + "}";
      if (sql.contains("{" + entry.getKey() + "}")) {
        sql = sql.replaceFirst(placeholder, "?");
        paramOrder.add(entry.getKey());
      }
    }

    // Execute with PreparedStatement
    try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < paramOrder.size(); i++) {
        pstmt.setString(i + 1, params.get(paramOrder.get(i)));
      }
      return pstmt.execute();
    }
  }

  /**
   * Executes a query with named parameters using PreparedStatement and returns ResultSet.
   *
   * <p>SQL template should use {@code {paramName}} syntax which gets converted to ? placeholders.
   * Parameters are bound in the order they appear in the SQL template.
   *
   * <p>This method provides automatic SQL escaping and protection against SQL injection
   * by using JDBC PreparedStatement instead of string concatenation.
   *
   * <p><strong>IMPORTANT:</strong> The caller is responsible for closing the returned ResultSet
   * and the underlying PreparedStatement. Consider using try-with-resources.
   *
   * @param conn Database connection
   * @param sqlTemplate SQL with {@code {paramName}} placeholders
   * @param params Map of parameter names to values
   * @return ResultSet containing query results (caller must close)
   * @throws java.sql.SQLException if a database access error occurs
   */
  protected java.sql.ResultSet queryWithParams(java.sql.Connection conn, String sqlTemplate, Map<String, String> params)
      throws java.sql.SQLException {

    // Track parameter order as we replace placeholders
    List<String> paramOrder = new ArrayList<>();

    // Replace {paramName} with ? and track order
    String sql = sqlTemplate;
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String placeholder = "\\{" + entry.getKey() + "}";
      if (sql.contains("{" + entry.getKey() + "}")) {
        sql = sql.replaceFirst(placeholder, "?");
        paramOrder.add(entry.getKey());
      }
    }

    // Execute with PreparedStatement
    java.sql.PreparedStatement pstmt = conn.prepareStatement(sql);
    for (int i = 0; i < paramOrder.size(); i++) {
      pstmt.setString(i + 1, params.get(paramOrder.get(i)));
    }
    return pstmt.executeQuery();
  }

  /**
   * Executes DuckDB SQL with named parameters using PreparedStatement.
   *
   * <p>SQL template should use {@code {paramName}} syntax which gets converted to ? placeholders.
   * This method creates a DuckDB connection, executes the SQL, and handles cleanup automatically.
   *
   * <p>This method provides automatic SQL escaping and protection against SQL injection
   * by using JDBC PreparedStatement instead of string concatenation.
   *
   * @param sqlTemplate SQL with {@code {paramName}} placeholders
   * @param params Map of parameter names to values
   * @param operationDescription Description of the operation for logging
   * @throws IOException if SQL execution fails
   */
  protected void executeDuckDBSqlWithParams(String sqlTemplate, Map<String, String> params, String operationDescription)
      throws IOException {
    LOGGER.debug("{} - DuckDB SQL template (pre-substitution):\n{}", operationDescription, sqlTemplate);

    try (java.sql.Connection conn = getDuckDBConnection()) {
      executeWithParams(conn, sqlTemplate, params);
      LOGGER.info("{} completed successfully", operationDescription);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("%s failed: %s (SQL State: %s, Error Code: %d)",
          operationDescription,
          e.getMessage(),
          e.getSQLState(),
          e.getErrorCode());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  // ===== CACHE EXPIRY HELPERS =====

  /**
   * Calculates cache expiry timestamp based on whether year is current year.
   * Current year data expires after 24 hours, historical data never expires.
   *
   * @param year The data year
   * @return Expiry timestamp in milliseconds (Long.MAX_VALUE for no expiry)
   */
  protected long getCacheExpiryForYear(int year) {
    int currentYear = java.time.LocalDate.now().getYear();
    if (year == currentYear) {
      return System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24);
    }
    return Long.MAX_VALUE;
  }

  /**
   * Returns cache policy tag based on whether year is current year.
   * Used for cache manifest categorization and debugging.
   *
   * @param year The data year
   * @return "current_year_daily" for current year, "historical_immutable" otherwise
   */
  protected String getCachePolicyForYear(int year) {
    int currentYear = java.time.LocalDate.now().getYear();
    return (year == currentYear) ? "current_year_daily" : "historical_immutable";
  }

}
