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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DuckDB-based cache manifest query utility for efficient download list generation.
 *
 * <p>Replaces row-by-row Java iteration with SQL set operations, reducing cache
 * checking overhead from O(n) to O(1) for large manifest files.
 *
 * <p>Performance: For 16,000+ manifest entries with 163,000 possible combinations,
 * reduces cache checking time from ~1-2 seconds to ~50-100ms.
 *
 * <p>Usage Example:
 * <pre>{@code
 * // Generate all possible download combinations
 * List<DownloadRequest> allRequests = new ArrayList<>();
 * for (String series : seriesList) {
 *   for (int year = 2000; year <= 2024; year++) {
 *     Map<String, String> params = new HashMap<>();
 *     params.put("series", series);
 *     allRequests.add(new DownloadRequest("fred_indicators", year, params));
 *   }
 * }
 *
 * // Filter using DuckDB (single SQL query)
 * List<DownloadRequest> needed = CacheManifestQueryHelper.filterUncachedRequests(
 *     "/path/to/cache_manifest.json",
 *     allRequests
 *);
 *
 * // Execute only uncached downloads
 * for (DownloadRequest req : needed) {
 *   download(req);
 * }
 * }</pre>
 */
public class CacheManifestQueryHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifestQueryHelper.class);

  /** Cache for SQL resource files */
  private static final Map<String, String> SQL_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Represents a download candidate (table + year + parameters).
   *
   * <p>This class encapsulates all information needed to identify a specific
   * download operation and build a cache key that matches the format used by
   * AbstractCacheManifest implementations.
   */
  public static class DownloadRequest {
    public final String dataType;
    public final Map<String, String> parameters;

    /**
     * Creates a download request.
     *
     * @param dataType Table/dataset name (e.g., "fred_indicators", "bea_regional_income")
     * @param year Year dimension for the data
     * @param parameters Additional dimension parameters (e.g., series, state, line_code)
     */
    public DownloadRequest(String dataType, Map<String, String> parameters) {
      this.dataType = dataType;
      this.parameters = new HashMap<>(parameters != null ? parameters : new HashMap<>());
    }

    /**
     * Builds cache key matching AbstractCacheManifest key format.
     * Format: dataType:year[:key1=value1][:key2=value2]...
     * Parameters are sorted alphabetically by key for consistency.
     *
     * @return Cache key string
     */
    public String buildKey() {
      StringBuilder key = new StringBuilder();
      key.append(dataType);
      if (!parameters.isEmpty()) {
        parameters.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> key.append(":").append(e.getKey())
                .append("=").append(e.getValue()));
      }

      return key.toString();
    }

    @Override public String toString() {
      return buildKey();
    }
  }

  /**
   * Filters download requests using DuckDB SQL query against cache manifest.
   *
   * <p>This method replaces row-by-row Java iteration with a single SQL query:
   * <pre>{@code
   * SELECT needed_key FROM needed_downloads
   * WHERE NOT EXISTS (
   *   SELECT 1 FROM cached_files
   *   WHERE cached_files.key = needed_downloads.key
   *     AND cached_files.refresh_after > NOW
   *     AND (cached_files.download_retry = 0 OR cached_files.download_retry < NOW)
   * )
   * }</pre>
   *
   * <p>Performance comparison for 25,000 combinations against 16,000 cached entries:
   * <ul>
   *   <li>Java iteration: ~1-2 seconds (25,000 HashMap lookups)</li>
   *   <li>DuckDB SQL: ~50-100ms (single hash join operation)</li>
   * </ul>
   *
   * @param manifestPath Path to cache_manifest.json file
   * @param allRequests All possible download combinations to check
   * @return Filtered list of requests that need downloading (not cached or expired)
   * @throws SQLException If DuckDB query fails
   */
  public static List<DownloadRequest> filterUncachedRequests(
      String manifestPath,
      List<DownloadRequest> allRequests,
      OperationType operationType) throws SQLException {

    if (allRequests == null || allRequests.isEmpty()) {
      LOGGER.debug("No download requests to filter");
      return new ArrayList<>();
    }

    // If manifest doesn't exist, all requests are uncached
    if (!new java.io.File(manifestPath).exists()) {
      LOGGER.debug("Manifest file {} does not exist - treating all {} requests as uncached",
          manifestPath, allRequests.size());
      return new ArrayList<>(allRequests);
    }

    boolean isConversion = OperationType.CONVERSION.equals(operationType);

    LOGGER.info("Filtering {} download requests using DuckDB SQL query against cache manifest: {}",
        allRequests.size(), manifestPath);

    // Use in-memory manifest filtering
    return filterUsingManifest(manifestPath, allRequests, operationType);

    /* DISABLED - causes hangs
    long startMs = System.currentTimeMillis();

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {

      // 1. Load cache manifest into DuckDB temp table
      // Uses DuckDB's read_json with custom format settings to handle nested JSON structure
      String loadManifestSql =
          substituteSqlParameters(loadSqlResource("/sql/cache/load_manifest.sql"),
          java.util.Collections.singletonMap("manifestPath", manifestPath));

      try (Statement stmt = duckdb.createStatement()) {
        stmt.execute(loadManifestSql);
        LOGGER.debug("Loaded cache manifest into DuckDB temp table");
      }

      // 2. Create temp table for all needed downloads
      try (Statement stmt = duckdb.createStatement()) {
        stmt.execute("CREATE TEMP TABLE needed_downloads (" +
            "  cache_key VARCHAR" +
            ")");
        LOGGER.debug("Created temp table for needed downloads");
      }

      // 3. Insert all requested downloads (batch insert for performance)
      try (PreparedStatement insert =
          duckdb.prepareStatement("INSERT INTO needed_downloads VALUES (?)")) {
        int batchCount = 0;
        for (DownloadRequest req : allRequests) {
          insert.setString(1, req.buildKey());
          insert.addBatch();
          batchCount++;

          // Execute batch every 1000 rows for memory efficiency
          if (batchCount % 1000 == 0) {
            insert.executeBatch();
          }
        }
        // Execute remaining batch
        if (batchCount % 1000 != 0) {
          insert.executeBatch();
        }
        LOGGER.debug("Inserted {} needed downloads into temp table", allRequests.size());
      }

      // 4. Filter: needed downloads NOT IN cached (or expired/retry-eligible)
      // This is the core optimization - single SQL query replaces thousands of HashMap lookups
      // For conversions, also check if parquet has been converted
      long now = System.currentTimeMillis();

      // Load SQL from resource file
      String parquetCheck = isConversion
          ? " OR cf.parquet_converted_at IS NULL OR cf.parquet_converted_at = 0"
          : "";

      Map<String, String> sqlParams = new HashMap<>();
      sqlParams.put("nowTimestamp", "?");
      sqlParams.put("includeParquetCheck", parquetCheck);

      String filterSql =
          substituteSqlParameters(loadSqlResource("/sql/cache/filter_uncached_temp_tables.sql"),
          sqlParams);

      List<String> uncachedKeys = new ArrayList<>();
      try (PreparedStatement query = duckdb.prepareStatement(filterSql)) {
        query.setLong(1, now);
        query.setLong(2, now);

        try (ResultSet rs = query.executeQuery()) {
          while (rs.next()) {
            uncachedKeys.add(rs.getString(1));
          }
        }
      }

      LOGGER.debug("DuckDB query found {} uncached entries", uncachedKeys.size());

      // 5. Build result list from uncached keys
      Set<String> uncachedSet = new HashSet<>(uncachedKeys);
      List<DownloadRequest> result = new ArrayList<>();

      for (DownloadRequest req : allRequests) {
        if (uncachedSet.contains(req.buildKey())) {
          result.add(req);
        }
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.info("DuckDB cache filtering complete: {} uncached out of {} total ({}ms)",
          result.size(), allRequests.size(), elapsedMs);

      return result;
    }
    */ // End DISABLED block
  }

  /**
   * Alternative implementation using direct SQL query without temp tables.
   *
   * <p>Faster for small lists (&lt;1000 entries) where temp table overhead is significant.
   * Uses DuckDB's LIST type to pass all cache keys in a single query parameter.
   *
   * @param manifestPath Path to cache_manifest.json file
   * @param allRequests All possible download combinations to check
   * @return Filtered list of requests that need downloading
   * @throws SQLException If DuckDB query fails
   */
  public static List<DownloadRequest> filterUncachedRequestsDirect(
      String manifestPath,
      List<DownloadRequest> allRequests,
      OperationType operationType) throws SQLException {

    if (allRequests == null || allRequests.isEmpty()) {
      LOGGER.debug("No download requests to filter");
      return new ArrayList<>();
    }

    // If manifest doesn't exist, all requests are uncached
    if (!new java.io.File(manifestPath).exists()) {
      LOGGER.debug("Manifest file {} does not exist - treating all {} requests as uncached",
          manifestPath, allRequests.size());
      return new ArrayList<>(allRequests);
    }


    LOGGER.info("Filtering {} download requests using direct DuckDB query", allRequests.size());

    // Use in-memory manifest filtering
    return filterUsingManifest(manifestPath, allRequests, operationType);

    /* DISABLED - causes hangs
    long startMs = System.currentTimeMillis();

    // Build list of cache keys to check
    List<String> keys = new ArrayList<>();
    for (DownloadRequest req : allRequests) {
      keys.add(req.buildKey());
    }

    // Build array literal for DuckDB (setObject with String[] doesn't work reliably)
    StringBuilder arrayLiteral = new StringBuilder("[");
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) arrayLiteral.append(", ");
      // Escape single quotes in the key
      String escapedKey = keys.get(i).replace("'", "''");
      arrayLiteral.append("'").append(escapedKey).append("'");
    }
    arrayLiteral.append("]");

    // Escape single quotes in manifestPath for SQL string literal
    String escapedManifestPath = manifestPath.replace("'", "''");

    // Build parquet conversion check clause if needed
    String parquetCheck = isConversion
        ? " OR m.parquet_converted_at IS NULL OR m.parquet_converted_at = 0"
        : "";

    // Load SQL from resource file and substitute parameters
    Map<String, String> sqlParams = new HashMap<>();
    sqlParams.put("manifestPath", escapedManifestPath);
    sqlParams.put("keysArray", arrayLiteral.toString());
    sqlParams.put("nowTimestamp", "?");
    sqlParams.put("includeParquetCheck", parquetCheck);

    String sql =
        substituteSqlParameters(loadSqlResource("/sql/cache/filter_uncached_direct.sql"),
        sqlParams);

    LOGGER.debug("Executing DuckDB direct query with {} keys", keys.size());

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:");
         PreparedStatement query = duckdb.prepareStatement(sql)) {

      // Set query timeout to 30 seconds to prevent indefinite hangs
      query.setQueryTimeout(30);

      query.setLong(1, System.currentTimeMillis());
      query.setLong(2, System.currentTimeMillis());

      LOGGER.debug("Starting DuckDB query execution...");
      List<String> uncachedKeys = new ArrayList<>();
      try (ResultSet rs = query.executeQuery()) {
        LOGGER.debug("DuckDB query completed, processing results...");
        while (rs.next()) {
          uncachedKeys.add(rs.getString(1));
        }
      } catch (SQLException e) {
        LOGGER.error("DuckDB query failed during execution. Manifest path: {}", manifestPath);
        LOGGER.error("SQL query (first 500 chars): {}", sql.substring(0, Math.min(500, sql.length())));
        throw e;
      }

      // Build result
      Set<String> uncachedSet = new HashSet<>(uncachedKeys);
      List<DownloadRequest> result = new ArrayList<>();

      for (DownloadRequest req : allRequests) {
        if (uncachedSet.contains(req.buildKey())) {
          result.add(req);
        }
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.info("Direct DuckDB filtering complete: {} uncached out of {} total ({}ms)",
          result.size(), allRequests.size(), elapsedMs);

      return result;

    } catch (Exception e) {
      // Fallback: If DuckDB query fails or times out, use in-memory manifest checking
      LOGGER.warn("DuckDB query failed or timed out, falling back to in-memory manifest check: {}",
          e.getMessage());
      return fallbackFilterUsingManifest(manifestPath, allRequests, operationType);
    }
    */ // End DISABLED block
  }

  /**
   * Filter uncached requests using in-memory manifest checking.
   * Loads the manifest JSON via Jackson and checks each request.
   */
  private static List<DownloadRequest> filterUsingManifest(
      String manifestPath,
      List<DownloadRequest> allRequests,
      OperationType operationType) {

    long startMs = System.currentTimeMillis();

    try {
      // Load manifest using Jackson (same way CacheManifest does it)
      com.fasterxml.jackson.databind.ObjectMapper mapper =
          new com.fasterxml.jackson.databind.ObjectMapper();
      java.util.Map<String, Object> manifestData =
          mapper.readValue(new java.io.File(manifestPath), java.util.Map.class);

      @SuppressWarnings("unchecked")
      java.util.Map<String, java.util.Map<String, Object>> entries =
          (java.util.Map<String, java.util.Map<String, Object>>) manifestData.get("entries");

      if (entries == null) {
        LOGGER.warn("No entries found in manifest, treating all requests as uncached");
        return new ArrayList<>(allRequests);
      }

      boolean isConversion = OperationType.CONVERSION.equals(operationType);
      long now = System.currentTimeMillis();
      List<DownloadRequest> result = new ArrayList<>();

      for (DownloadRequest req : allRequests) {
        String key = req.buildKey();
        java.util.Map<String, Object> entry = entries.get(key);

        if (entry == null) {
          // Not in manifest
          result.add(req);
          continue;
        }

        // Check if expired
        Object refreshAfterObj = entry.get("refreshAfter");
        if (refreshAfterObj != null) {
          long refreshAfter = ((Number) refreshAfterObj).longValue();
          if (now >= refreshAfter) {
            result.add(req);
            continue;
          }
        }

        // Check retry window
        Object downloadRetryObj = entry.get("downloadRetry");
        if (downloadRetryObj != null) {
          long downloadRetry = ((Number) downloadRetryObj).longValue();
          if (downloadRetry > 0 && now >= downloadRetry) {
            result.add(req);
            continue;
          }
        }

        // For conversions, check if parquet is converted
        if (isConversion) {
          Object parquetConvertedAtObj = entry.get("parquetConvertedAt");
          if (parquetConvertedAtObj == null || ((Number) parquetConvertedAtObj).longValue() == 0) {
            result.add(req);
            continue;
          }
        }

        // Entry is cached and valid
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.info("Fallback filtering complete: {} uncached out of {} total ({}ms)",
          result.size(), allRequests.size(), elapsedMs);

      return result;

    } catch (Exception e) {
      LOGGER.error("Fallback filtering failed: {}, returning all requests as uncached", e.getMessage());
      return new ArrayList<>(allRequests);
    }
  }

  /**
   * Chooses the optimal filtering strategy based on request count.
   *
   * <p>Uses direct query for small lists (&lt;1000) to avoid temp table overhead,
   * and temp table approach for large lists where it provides better performance.
   *
   * @param manifestPath Path to cache_manifest.json file
   * @param allRequests All possible download combinations to check
   * @return Filtered list of requests that need downloading
   * @throws SQLException If DuckDB query fails
   */
  public static List<DownloadRequest> filterUncachedRequestsOptimal(
      String manifestPath,
      List<DownloadRequest> allRequests,
      OperationType operationType) throws SQLException {

    if (allRequests == null || allRequests.isEmpty()) {
      return new ArrayList<>();
    }

    LOGGER.debug("Using direct query strategy for {} requests", allRequests.size());
    return filterUncachedRequestsDirect(manifestPath, allRequests, operationType);

  }

  // ===== SQL RESOURCE LOADING =====

  /**
   * Loads a SQL query from classpath resources with caching.
   *
   * @param resourcePath Path to SQL file (e.g., "/sql/cache/load_manifest.sql")
   * @return SQL query text
   */
  private static String loadSqlResource(String resourcePath) {
    return SQL_CACHE.computeIfAbsent(resourcePath, path -> {
      try (InputStream is = CacheManifestQueryHelper.class.getResourceAsStream(path)) {
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
   *
   * @param sqlTemplate SQL with {@code {paramName}} placeholders
   * @param params Parameter values
   * @return SQL with parameters substituted
   */
  private static String substituteSqlParameters(String sqlTemplate, Map<String, String> params) {
    String result = sqlTemplate;
    for (Map.Entry<String, String> entry : params.entrySet()) {
      result = result.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }
}
