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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DuckDB-based cache store for govdata manifests.
 * Provides thread-safe, persistent caching with proper ACID guarantees.
 *
 * <p>Each schema (ECON, GEO, SEC) gets its own DuckDB file in its cache directory.
 * The store handles connection pooling, schema creation, and provides
 * common CRUD operations for cache entries.
 */
public class DuckDBCacheStore implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBCacheStore.class);

  /** Schema name for this cache store (econ, geo, sec). */
  private final String schemaName;

  /** Path to the DuckDB database file. */
  private final String dbPath;

  /** Shared connection - DuckDB handles concurrent access internally. */
  private Connection connection;

  /** Lock for connection management. */
  private final Object connectionLock = new Object();

  /** Static map of open stores to allow reuse within same JVM. */
  private static final Map<String, DuckDBCacheStore> OPEN_STORES = new ConcurrentHashMap<>();

  /** SQL resource path prefix. */
  private static final String SQL_RESOURCE_PATH = "org/apache/calcite/adapter/govdata/cache/";

  private DuckDBCacheStore(String schemaName, String cacheDir) {
    this.schemaName = schemaName;
    this.dbPath = new File(cacheDir, "cache.duckdb").getAbsolutePath();
  }

  /**
   * Get or create a cache store for the given schema and cache directory.
   * Stores are cached by path to allow reuse within the same JVM.
   *
   * @param schemaName Schema name (econ, geo, sec)
   * @param cacheDir Cache directory path
   * @return DuckDBCacheStore instance
   */
  public static DuckDBCacheStore getInstance(String schemaName, String cacheDir) {
    String key = new File(cacheDir, "cache.duckdb").getAbsolutePath();
    return OPEN_STORES.computeIfAbsent(key, k -> {
      DuckDBCacheStore store = new DuckDBCacheStore(schemaName, cacheDir);
      store.initialize();
      return store;
    });
  }

  /**
   * Initialize the store by ensuring directory exists and creating tables.
   */
  private void initialize() {
    // Ensure cache directory exists
    File dbFile = new File(dbPath);
    File parentDir = dbFile.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      boolean created = parentDir.mkdirs();
      if (!created && !parentDir.exists()) {
        LOGGER.error("Failed to create cache directory: {}", parentDir.getAbsolutePath());
        throw new RuntimeException("Failed to create cache directory: " + parentDir);
      }
    }

    try {
      getConnection(); // This will create the connection
      createTables();
      LOGGER.info("Initialized DuckDB cache store for {} at {}", schemaName, dbPath);
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize DuckDB cache store at {}: {}", dbPath, e.getMessage());
      throw new RuntimeException("Failed to initialize cache store", e);
    }
  }

  /**
   * Get or create the database connection.
   * DuckDB supports concurrent access from a single connection.
   */
  private Connection getConnection() throws SQLException {
    synchronized (connectionLock) {
      if (connection == null || connection.isClosed()) {
        String jdbcUrl = "jdbc:duckdb:" + dbPath;
        connection = DriverManager.getConnection(jdbcUrl);
        LOGGER.debug("Opened DuckDB connection to {}", dbPath);
      }
      return connection;
    }
  }

  /**
   * Execute SQL with automatic retry on busy database.
   */
  private void executeWithRetry(String sql) throws SQLException {
    int maxRetries = 3;
    int retryDelayMs = 100;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        try (Statement stmt = getConnection().createStatement()) {
          stmt.execute(sql);
        }
        return;
      } catch (SQLException e) {
        if (attempt == maxRetries) {
          throw e;
        }
        LOGGER.debug("Database busy, retrying ({}/{}): {}", attempt, maxRetries, e.getMessage());
        try {
          Thread.sleep(retryDelayMs * attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  /**
   * Load SQL from a resource file.
   *
   * @param resourceName Name of the SQL file (without path prefix)
   * @return SQL content
   */
  private String loadSqlResource(String resourceName) {
    String resourcePath = SQL_RESOURCE_PATH + resourceName;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new RuntimeException("SQL resource not found: " + resourcePath);
      }
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load SQL resource: " + resourcePath, e);
    }
  }

  /**
   * Execute SQL statements from a resource file.
   * Handles files with multiple statements separated by semicolons.
   */
  private void executeSqlResource(String resourceName) throws SQLException {
    String sql = loadSqlResource(resourceName);
    LOGGER.debug("Executing SQL resource {}: {}", resourceName, sql);
    // Split by semicolon to handle multiple statements (e.g., CREATE TABLE + CREATE INDEX)
    for (String statement : sql.split(";")) {
      String trimmed = statement.trim();
      // Skip empty lines and comment-only lines
      if (!trimmed.isEmpty()) {
        // Remove leading comment lines but keep the actual SQL
        String[] lines = trimmed.split("\n");
        StringBuilder sqlBuilder = new StringBuilder();
        for (String line : lines) {
          String trimmedLine = line.trim();
          if (!trimmedLine.startsWith("--")) {
            sqlBuilder.append(line).append("\n");
          }
        }
        String cleanSql = sqlBuilder.toString().trim();
        if (!cleanSql.isEmpty()) {
          LOGGER.debug("Executing statement: {}", cleanSql);
          executeWithRetry(cleanSql);
        }
      }
    }
  }

  /**
   * Create schema-specific tables.
   */
  private void createTables() throws SQLException {
    // Root metadata table - all schemas
    executeSqlResource("create_manifest_metadata.sql");

    // Initialize version if not set
    initializeMetadata();

    // All schemas use cache_entries
    executeSqlResource("create_cache_entries.sql");

    // Schema-specific tables
    if ("econ".equals(schemaName)) {
      executeSqlResource("create_catalog_series_cache.sql");
    } else if ("sec".equals(schemaName)) {
      executeSqlResource("create_sec_filings.sql");
    }
  }

  /**
   * Initialize metadata with default values if not present.
   */
  private void initializeMetadata() throws SQLException {
    String sql = "INSERT INTO manifest_metadata (key, value) VALUES (?, ?) "
        + "ON CONFLICT (key) DO NOTHING";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      // Version
      stmt.setString(1, "version");
      stmt.setString(2, "3.0");  // New DuckDB-based version
      stmt.executeUpdate();

      // Schema name
      stmt.setString(1, "schema_name");
      stmt.setString(2, schemaName);
      stmt.executeUpdate();

      // Created timestamp
      stmt.setString(1, "created_at");
      stmt.setString(2, String.valueOf(System.currentTimeMillis()));
      stmt.executeUpdate();
    }
  }

  /**
   * Get metadata value.
   *
   * @param key Metadata key
   * @return Value or null if not found
   */
  public String getMetadata(String key) {
    String sql = "SELECT value FROM manifest_metadata WHERE key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, key);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("value");
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting metadata {}: {}", key, e.getMessage());
    }
    return null;
  }

  /**
   * Set metadata value.
   *
   * @param key Metadata key
   * @param value Metadata value
   */
  public void setMetadata(String key, String value) {
    String sql = "INSERT INTO manifest_metadata (key, value) VALUES (?, ?) "
        + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, key);
      stmt.setString(2, value);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Error setting metadata {}: {}", key, e.getMessage());
    }
  }

  /**
   * Update the last_updated timestamp in metadata.
   */
  public void touchLastUpdated() {
    setMetadata("last_updated", String.valueOf(System.currentTimeMillis()));
  }

  // ===== Cache Entry Operations =====

  /**
   * Check if an entry exists and is fresh.
   *
   * @param cacheKey Cache key to check
   * @return true if entry exists and refresh_after > now
   */
  public boolean isCached(String cacheKey) {
    String sql = "SELECT refresh_after, download_retry, etag FROM cache_entries WHERE cache_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return false;
        }

        long now = System.currentTimeMillis();
        long downloadRetry = rs.getLong("download_retry");
        String etag = rs.getString("etag");
        long refreshAfter = rs.getLong("refresh_after");

        // Check API error retry restriction
        if (downloadRetry > 0 && now < downloadRetry) {
          return true; // Still in retry restriction period
        }

        // If retry period has passed, remove entry and return false
        if (downloadRetry > 0 && now >= downloadRetry) {
          deleteEntry(cacheKey);
          return false;
        }

        // ETag-based entries are always valid until server says otherwise
        if (etag != null && !etag.isEmpty()) {
          return true;
        }

        // Check time-based expiration
        if (now >= refreshAfter) {
          deleteEntry(cacheKey);
          return false;
        }

        return true;
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking cache for {}: {}", cacheKey, e.getMessage());
      return false;
    }
  }

  /**
   * Get an entry's ETag value.
   *
   * @param cacheKey Cache key
   * @return ETag string or null
   */
  public String getETag(String cacheKey) {
    String sql = "SELECT etag FROM cache_entries WHERE cache_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("etag");
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting ETag for {}: {}", cacheKey, e.getMessage());
    }
    return null;
  }

  /**
   * Insert or update a cache entry.
   *
   * @param cacheKey Cache key
   * @param dataType Data type / table name
   * @param parameters JSON-encoded parameters
   * @param filePath Path to cached file
   * @param fileSize File size in bytes
   * @param refreshAfter Timestamp when entry should be refreshed
   * @param refreshReason Human-readable reason
   */
  public void upsertEntry(String cacheKey, String dataType, String parameters,
      String filePath, long fileSize, long refreshAfter, String refreshReason) {
    String sql = ""
        + "INSERT INTO cache_entries "
        + "(cache_key, data_type, parameters, file_path, file_size, cached_at, refresh_after, refresh_reason) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (cache_key) DO UPDATE SET "
        + "data_type = EXCLUDED.data_type, "
        + "parameters = EXCLUDED.parameters, "
        + "file_path = EXCLUDED.file_path, "
        + "file_size = EXCLUDED.file_size, "
        + "cached_at = EXCLUDED.cached_at, "
        + "refresh_after = EXCLUDED.refresh_after, "
        + "refresh_reason = EXCLUDED.refresh_reason, "
        + "last_error = NULL, "
        + "error_count = 0, "
        + "download_retry = 0";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      stmt.setString(2, dataType);
      stmt.setString(3, parameters);
      stmt.setString(4, filePath);
      stmt.setLong(5, fileSize);
      stmt.setLong(6, System.currentTimeMillis());
      stmt.setLong(7, refreshAfter);
      stmt.setString(8, refreshReason);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Error upserting cache entry {}: {}", cacheKey, e.getMessage());
    }
  }

  /**
   * Check if parquet conversion has been completed.
   *
   * @param cacheKey Cache key
   * @return true if parquet_converted_at > 0 and parquet is up-to-date
   */
  public boolean isParquetConverted(String cacheKey) {
    String sql = "SELECT parquet_converted_at, cached_at FROM cache_entries WHERE cache_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return false;
        }
        long parquetConvertedAt = rs.getLong("parquet_converted_at");
        long cachedAt = rs.getLong("cached_at");

        // Not converted if timestamp is 0
        if (parquetConvertedAt == 0) {
          return false;
        }

        // Raw file updated after conversion - needs reconversion
        if (cachedAt > parquetConvertedAt) {
          return false;
        }

        return true;
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking parquet status for {}: {}", cacheKey, e.getMessage());
      return false;
    }
  }

  /**
   * Mark parquet as converted.
   *
   * @param cacheKey Cache key
   * @param parquetPath Path to parquet file
   */
  public void markParquetConverted(String cacheKey, String parquetPath) {
    String sql = ""
        + "UPDATE cache_entries SET parquet_path = ?, parquet_converted_at = ? "
        + "WHERE cache_key = ?";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, parquetPath);
      stmt.setLong(2, System.currentTimeMillis());
      stmt.setString(3, cacheKey);
      int updated = stmt.executeUpdate();

      // If entry doesn't exist, create it
      if (updated == 0) {
        upsertEntry(cacheKey, "unknown", null, null, 0, Long.MAX_VALUE, "parquet_only");
        stmt.setString(1, parquetPath);
        stmt.setLong(2, System.currentTimeMillis());
        stmt.setString(3, cacheKey);
        stmt.executeUpdate();
      }
    } catch (SQLException e) {
      LOGGER.error("Error marking parquet converted for {}: {}", cacheKey, e.getMessage());
    }
  }

  /**
   * Mark entry as having API error with retry delay.
   *
   * @param cacheKey Cache key
   * @param errorMessage Error message
   * @param retryAfterDays Days until retry should be attempted
   */
  public void markApiError(String cacheKey, String dataType, String errorMessage, int retryAfterDays) {
    long now = System.currentTimeMillis();
    long retryAfter = now + (retryAfterDays * 24L * 60L * 60L * 1000L);

    // First try to update existing entry
    String updateSql = ""
        + "UPDATE cache_entries SET "
        + "last_error = ?, "
        + "error_count = error_count + 1, "
        + "last_attempt_at = ?, "
        + "download_retry = ?, "
        + "refresh_after = ? "
        + "WHERE cache_key = ?";

    try (PreparedStatement stmt = getConnection().prepareStatement(updateSql)) {
      stmt.setString(1, errorMessage);
      stmt.setLong(2, now);
      stmt.setLong(3, retryAfter);
      stmt.setLong(4, retryAfter);
      stmt.setString(5, cacheKey);
      int updated = stmt.executeUpdate();

      if (updated == 0) {
        // Insert new entry
        String insertSql = ""
            + "INSERT INTO cache_entries "
            + "(cache_key, data_type, cached_at, refresh_after, last_error, error_count, "
            + "last_attempt_at, download_retry, refresh_reason) "
            + "VALUES (?, ?, ?, ?, ?, 1, ?, ?, 'api_error_retry')";
        try (PreparedStatement insertStmt = getConnection().prepareStatement(insertSql)) {
          insertStmt.setString(1, cacheKey);
          insertStmt.setString(2, dataType);
          insertStmt.setLong(3, now);
          insertStmt.setLong(4, retryAfter);
          insertStmt.setString(5, errorMessage);
          insertStmt.setLong(6, now);
          insertStmt.setLong(7, retryAfter);
          insertStmt.executeUpdate();
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Error marking API error for {}: {}", cacheKey, e.getMessage());
    }
  }

  /**
   * Delete a cache entry.
   *
   * @param cacheKey Cache key to delete
   */
  public void deleteEntry(String cacheKey) {
    String sql = "DELETE FROM cache_entries WHERE cache_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.warn("Error deleting cache entry {}: {}", cacheKey, e.getMessage());
    }
  }

  /**
   * Remove expired entries based on refresh_after timestamp.
   *
   * @return Number of entries removed
   */
  public int cleanupExpiredEntries() {
    String sql = "DELETE FROM cache_entries WHERE refresh_after < ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setLong(1, System.currentTimeMillis());
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.info("Cleaned up {} expired cache entries", deleted);
      }
      return deleted;
    } catch (SQLException e) {
      LOGGER.warn("Error cleaning up expired entries: {}", e.getMessage());
      return 0;
    }
  }

  /**
   * Get cache statistics.
   *
   * @return Array of [totalEntries, freshEntries, expiredEntries]
   */
  public int[] getStats() {
    long now = System.currentTimeMillis();
    int total = 0;
    int fresh = 0;

    String sql = "SELECT COUNT(*) as total, "
        + "SUM(CASE WHEN refresh_after > ? THEN 1 ELSE 0 END) as fresh "
        + "FROM cache_entries";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setLong(1, now);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          total = rs.getInt("total");
          fresh = rs.getInt("fresh");
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting cache stats: {}", e.getMessage());
    }

    return new int[]{total, fresh, total - fresh};
  }

  // ===== ECON-specific: Catalog Series Cache =====

  /**
   * Get cached catalog series IDs for given popularity threshold.
   *
   * @param minPopularity Minimum popularity
   * @return Comma-separated series IDs, or null if not cached/expired
   */
  public String getCachedCatalogSeries(int minPopularity) {
    String cacheKey = "catalog_series:popularity=" + minPopularity;
    String sql = "SELECT series_ids, refresh_after FROM catalog_series_cache WHERE cache_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        long refreshAfter = rs.getLong("refresh_after");
        if (System.currentTimeMillis() >= refreshAfter) {
          // Expired - delete and return null
          deleteCatalogSeriesCache(minPopularity);
          return null;
        }
        return rs.getString("series_ids");
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting catalog series cache: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Cache catalog series IDs.
   *
   * @param minPopularity Popularity threshold
   * @param seriesIds Comma-separated series IDs
   * @param ttlDays Time-to-live in days
   */
  public void cacheCatalogSeries(int minPopularity, String seriesIds, int ttlDays) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      return;
    }

    String cacheKey = "catalog_series:popularity=" + minPopularity;
    long now = System.currentTimeMillis();
    long refreshAfter = now + (ttlDays * 24L * 60L * 60L * 1000L);

    String sql = ""
        + "INSERT INTO catalog_series_cache "
        + "(cache_key, min_popularity, series_ids, cached_at, refresh_after, refresh_reason) "
        + "VALUES (?, ?, ?, ?, ?, 'catalog_annual_refresh') "
        + "ON CONFLICT (cache_key) DO UPDATE SET "
        + "series_ids = EXCLUDED.series_ids, "
        + "cached_at = EXCLUDED.cached_at, "
        + "refresh_after = EXCLUDED.refresh_after";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, cacheKey);
      stmt.setInt(2, minPopularity);
      stmt.setString(3, seriesIds);
      stmt.setLong(4, now);
      stmt.setLong(5, refreshAfter);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Error caching catalog series: {}", e.getMessage());
    }
  }

  /**
   * Delete catalog series cache for a specific popularity threshold.
   */
  public void deleteCatalogSeriesCache(int minPopularity) {
    String sql = "DELETE FROM catalog_series_cache WHERE min_popularity = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setInt(1, minPopularity);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.warn("Error deleting catalog series cache: {}", e.getMessage());
    }
  }

  /**
   * Delete all catalog series caches.
   */
  public void deleteAllCatalogSeriesCache() {
    try {
      executeWithRetry("DELETE FROM catalog_series_cache");
    } catch (SQLException e) {
      LOGGER.warn("Error deleting all catalog series cache: {}", e.getMessage());
    }
  }

  // ===== SEC-specific: Filing Operations =====

  /**
   * Check if a filing is in a specific state.
   *
   * @param cik CIK
   * @param accession Accession number
   * @param fileName File name
   * @param state Expected state
   * @return true if filing exists and is in the specified state
   */
  public boolean isFilingInState(String cik, String accession, String fileName, String state) {
    String filingKey = cik + "/" + accession + "/" + fileName;
    String sql = "SELECT state FROM sec_filings WHERE filing_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, filingKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return state.equals(rs.getString("state"));
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking filing state: {}", e.getMessage());
    }
    return false;
  }

  /**
   * Mark a filing with a state.
   *
   * @param cik CIK
   * @param accession Accession number
   * @param fileName File name
   * @param state State to set
   * @param reason Reason for state
   */
  public void markFiling(String cik, String accession, String fileName, String state, String reason) {
    String filingKey = cik + "/" + accession + "/" + fileName;
    String sql = ""
        + "INSERT INTO sec_filings (filing_key, cik, accession, file_name, state, reason, checked_at) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (filing_key) DO UPDATE SET "
        + "state = EXCLUDED.state, "
        + "reason = EXCLUDED.reason, "
        + "checked_at = EXCLUDED.checked_at";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, filingKey);
      stmt.setString(2, cik);
      stmt.setString(3, accession);
      stmt.setString(4, fileName);
      stmt.setString(5, state);
      stmt.setString(6, reason);
      stmt.setLong(7, System.currentTimeMillis());
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Error marking filing {}: {}", filingKey, e.getMessage());
    }
  }

  /**
   * Get cached XBRL filename for a filing.
   */
  public String getCachedXbrlFilename(String cik, String accession) {
    String filingKey = cik + "/" + accession + "/xbrl_filename";
    String sql = "SELECT reason FROM sec_filings WHERE filing_key = ? AND state = 'xbrl_filename_cached'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, filingKey);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("reason"); // XBRL filename stored in reason field
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting cached XBRL filename: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Remove a filing entry.
   */
  public void removeFiling(String cik, String accession, String fileName) {
    String filingKey = cik + "/" + accession + "/" + fileName;
    String sql = "DELETE FROM sec_filings WHERE filing_key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, filingKey);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.warn("Error removing filing {}: {}", filingKey, e.getMessage());
    }
  }

  @Override
  public void close() {
    synchronized (connectionLock) {
      if (connection != null) {
        try {
          connection.close();
          LOGGER.debug("Closed DuckDB connection to {}", dbPath);
        } catch (SQLException e) {
          LOGGER.warn("Error closing DuckDB connection: {}", e.getMessage());
        }
        connection = null;
      }
    }
    OPEN_STORES.remove(dbPath);
  }

  /**
   * Get the database path.
   */
  public String getDbPath() {
    return dbPath;
  }

  /**
   * Get the schema name.
   */
  public String getSchemaName() {
    return schemaName;
  }
}
