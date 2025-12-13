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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    // Use schema-specific filename to avoid DuckDB database name conflicts
    // DuckDB tracks databases by filename, so "cache.duckdb" in different directories
    // would conflict. Using "cache_econ.duckdb" etc. avoids this.
    this.dbPath = new File(cacheDir, "cache_" + schemaName + ".duckdb").getAbsolutePath();
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
    // Use schema-specific filename to match constructor
    String key = new File(cacheDir, "cache_" + schemaName + ".duckdb").getAbsolutePath();
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
      executeSqlResource("create_table_year_availability.sql");
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
    String sql = "SELECT refresh_after, download_retry, etag, file_size FROM cache_entries WHERE cache_key = ?";
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
        long fileSize = rs.getLong("file_size");

        // Invalid entry (file_size=0 means corrupted/incomplete) - needs re-download
        if (fileSize <= 0) {
          return false;
        }

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
    // Note: Don't check file_size here. This method answers "is parquet converted?"
    // not "is source JSON valid?". Parquet-only entries (from self-healing) have
    // file_size=0 but are legitimately converted.
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

  // ===== Table-Level Completion Tracking =====

  /**
   * Check if a table iteration is complete (all dimension combinations cached).
   * This allows skipping the entire dimension iteration loop for subsequent runs.
   *
   * @param tableName The table name (e.g., "national_accounts", "regional_income")
   * @param dimensionSignature Hash of dimension names and value counts
   * @return true if table was fully cached with same dimension signature
   */
  public boolean isTableComplete(String tableName, String dimensionSignature) {
    String key = "table_complete:" + tableName;
    String stored = getMetadata(key);
    if (stored == null) {
      return false;
    }
    // Format: "signature|timestamp"
    String[] parts = stored.split("\\|");
    if (parts.length < 2) {
      return false;
    }
    return parts[0].equals(dimensionSignature);
  }

  /**
   * Mark a table as complete after all dimension combinations were processed.
   *
   * @param tableName The table name
   * @param dimensionSignature Hash of dimension names and value counts
   */
  public void markTableComplete(String tableName, String dimensionSignature) {
    String key = "table_complete:" + tableName;
    String value = dimensionSignature + "|" + System.currentTimeMillis();
    setMetadata(key, value);
    LOGGER.debug("Marked table {} complete with signature {}", tableName, dimensionSignature);
  }

  /**
   * Invalidate table completion when cache entries change.
   *
   * @param tableName The table name to invalidate
   */
  public void invalidateTableCompletion(String tableName) {
    String key = "table_complete:" + tableName;
    String sql = "DELETE FROM manifest_metadata WHERE key = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, key);
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.debug("Invalidated table completion for {}", tableName);
      }
    } catch (SQLException e) {
      LOGGER.warn("Error invalidating table completion for {}: {}", tableName, e.getMessage());
    }
  }

  /**
   * Check if all entries for a table are fresh (not expired).
   * Used in conjunction with isTableComplete to validate cached state.
   *
   * @param tableName The table name prefix to check
   * @return true if all entries for this table have refresh_after > now
   */
  public boolean areAllEntriesFresh(String tableName) {
    String sql = "SELECT COUNT(*) FROM cache_entries "
        + "WHERE cache_key LIKE ? AND refresh_after <= ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName + ":%");
      stmt.setLong(2, System.currentTimeMillis());
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1) == 0;  // No expired entries
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking entry freshness for {}: {}", tableName, e.getMessage());
    }
    return false;  // Assume not fresh on error
  }

  /**
   * Check if all entries for a table have been converted to parquet.
   * This is a faster check than areAllEntriesFresh for conversion operations -
   * if parquet exists, we don't need to re-convert regardless of refresh_after.
   *
   * <p>Also checks for invalid entries (file_size=0 from failed downloads, but NOT
   * parquet_only entries which are legitimate). Returns false if any invalid entries exist.
   *
   * @param tableName The table name prefix to check
   * @return true if all entries for this table have parquet_converted_at > 0 and no invalid entries
   */
  public boolean areAllParquetConverted(String tableName) {
    // Single query to get counts efficiently
    // An entry is considered "invalid" (needs re-download) only if ALL of:
    // 1. file_size=0 (no source JSON)
    // 2. Not a parquet_only entry (those are legitimate - parquet found via self-healing)
    // 3. parquet_converted_at=0 (no parquet exists either)
    // If parquet exists (parquet_converted_at > 0), entry is usable regardless of file_size
    String sql = "SELECT "
        + "COUNT(*) as total, "
        + "SUM(CASE WHEN parquet_converted_at IS NULL OR parquet_converted_at = 0 THEN 1 ELSE 0 END) as unconverted, "
        + "SUM(CASE WHEN (file_size IS NULL OR file_size = 0) "
        + "AND (refresh_reason IS NULL OR refresh_reason != 'parquet_only') "
        + "AND (parquet_converted_at IS NULL OR parquet_converted_at = 0) THEN 1 ELSE 0 END) as invalid_entries "
        + "FROM cache_entries WHERE cache_key LIKE ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName + ":%");
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int total = rs.getInt("total");
          int unconverted = rs.getInt("unconverted");
          int invalidEntries = rs.getInt("invalid_entries");

          if (total == 0) {
            return false;  // No entries at all
          }
          if (invalidEntries > 0) {
            LOGGER.debug("Table {} has {} invalid entries (no source and no parquet), cannot skip",
                tableName, invalidEntries);
            return false;  // Invalid entries need re-download
          }
          return unconverted == 0;  // All converted if no unconverted entries
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking parquet conversion for {}: {}", tableName, e.getMessage());
    }
    return false;  // Assume not converted on error
  }

  /**
   * Fast database-level check to determine if a table can be skipped entirely.
   * This combines multiple checks into a single efficient database query.
   *
   * <p>Returns true if:
   * <ul>
   *   <li>We have at least expectedCount entries for this table</li>
   *   <li>All entries have been converted to parquet (parquet_converted_at > 0)</li>
   *   <li>No entries need raw file refresh (all have cachedAt > 0 and no raw refresh needed)</li>
   * </ul>
   *
   * @param tableName The table name prefix
   * @param expectedCount The expected number of dimension combinations
   * @return true if table can be skipped entirely
   */
  public boolean isTableFullyCached(String tableName, int expectedCount) {
    // Single query to get all relevant counts efficiently
    // An entry is considered "invalid" (needs re-download) only if ALL of:
    // 1. file_size=0 (no source JSON)
    // 2. Not a parquet_only entry (those are legitimate - parquet found via self-healing)
    // 3. parquet_converted_at=0 (no parquet exists either)
    // If parquet exists (parquet_converted_at > 0), entry is usable regardless of file_size
    String sql = "SELECT "
        + "COUNT(*) as total, "
        + "SUM(CASE WHEN parquet_converted_at IS NULL OR parquet_converted_at = 0 THEN 1 ELSE 0 END) as unconverted, "
        + "SUM(CASE WHEN cached_at > parquet_converted_at THEN 1 ELSE 0 END) as needs_reconversion, "
        + "SUM(CASE WHEN (file_size IS NULL OR file_size = 0) "
        + "AND (refresh_reason IS NULL OR refresh_reason != 'parquet_only') "
        + "AND (parquet_converted_at IS NULL OR parquet_converted_at = 0) THEN 1 ELSE 0 END) as invalid_entries "
        + "FROM cache_entries WHERE cache_key LIKE ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName + ":%");
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int total = rs.getInt("total");
          int unconverted = rs.getInt("unconverted");
          int needsReconversion = rs.getInt("needs_reconversion");
          int invalidEntries = rs.getInt("invalid_entries");

          // Must have at least expected entries
          if (total < expectedCount) {
            LOGGER.debug("Table {} has {} entries but expected {}, needs processing",
                tableName, total, expectedCount);
            return false;
          }

          // No entries should be invalid (no source JSON AND no parquet)
          if (invalidEntries > 0) {
            LOGGER.debug("Table {} has {} invalid entries (no source and no parquet), needs re-download",
                tableName, invalidEntries);
            return false;
          }

          // All entries must be converted to parquet
          if (unconverted > 0) {
            LOGGER.debug("Table {} has {} unconverted entries, needs processing",
                tableName, unconverted);
            return false;
          }

          // No entries should need reconversion (raw file newer than parquet)
          if (needsReconversion > 0) {
            LOGGER.debug("Table {} has {} entries needing reconversion",
                tableName, needsReconversion);
            return false;
          }

          LOGGER.debug("Table {} is fully cached with {} entries (expected {})",
              tableName, total, expectedCount);
          return true;
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking full cache status for {}: {}", tableName, e.getMessage());
    }
    return false;  // Assume not fully cached on error
  }

  /**
   * Check if any entries for a table have actionable errors.
   *
   * <p>Only returns true if there are entries with errors whose retry period has expired.
   * Entries with errors that are still within their retry window (e.g., "retry in 7 days"
   * for unreleased data) are considered handled and don't block table-level caching.
   *
   * @param tableName The table name prefix to check
   * @return true if any entries have errors that need retry (refresh_after expired)
   */
  public boolean hasTableErrors(String tableName) {
    // Only consider errors where the retry period has expired
    // If refresh_after > now, the error is being handled (waiting for retry)
    String sql = "SELECT COUNT(*) FROM cache_entries "
        + "WHERE cache_key LIKE ? AND error_count > 0 AND refresh_after <= ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName + ":%");
      stmt.setLong(2, System.currentTimeMillis());
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1) > 0;
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking table errors for {}: {}", tableName, e.getMessage());
    }
    return true;  // Assume has errors on error
  }

  /**
   * Delete invalid cache entries for a table.
   * Entries are considered invalid if they have file_size=0 or file_size IS NULL,
   * which indicates a failed or incomplete download.
   *
   * <p>This is used to clean up stale entries from previous runs that attempted
   * to download data for invalid combinations (e.g., years not available for a table).
   *
   * @param tableName The table name prefix to clean
   * @return Number of entries deleted
   */
  public int deleteInvalidCacheEntries(String tableName) {
    String sql = "DELETE FROM cache_entries WHERE cache_key LIKE ? AND (file_size IS NULL OR file_size = 0)";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName + ":%");
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.info("Deleted {} invalid cache entries (file_size=0) for table {}", deleted, tableName);
      }
      return deleted;
    } catch (SQLException e) {
      LOGGER.warn("Error deleting invalid cache entries for {}: {}", tableName, e.getMessage());
      return 0;
    }
  }

  // ========== Table Year Availability Methods ==========

  /**
   * Get available years for a table from the cache.
   * Returns empty set if not cached or expired.
   *
   * @param dataSource The data source (e.g., "bea_regional")
   * @param tableName The table name (e.g., "SAINC1")
   * @return Set of available years, or empty set if not cached/expired
   */
  public Set<Integer> getAvailableYearsForTable(String dataSource, String tableName) {
    String sql = "SELECT available_years FROM table_year_availability "
        + "WHERE data_source = ? AND table_name = ? AND refresh_after > ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, dataSource);
      stmt.setString(2, tableName);
      stmt.setLong(3, System.currentTimeMillis());
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String yearsJson = rs.getString("available_years");
          return parseYearsJson(yearsJson);
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting available years for {}/{}: {}", dataSource, tableName, e.getMessage());
    }
    return Collections.emptySet();
  }

  /**
   * Store available years for a table in the cache.
   *
   * @param dataSource The data source (e.g., "bea_regional")
   * @param tableName The table name (e.g., "SAINC1")
   * @param years Set of available years
   * @param ttlDays TTL in days before refresh
   */
  public void setAvailableYearsForTable(String dataSource, String tableName,
      Set<Integer> years, int ttlDays) {
    String yearsJson = formatYearsJson(years);
    int maxYear = years.stream().mapToInt(Integer::intValue).max().orElse(0);
    int minYear = years.stream().mapToInt(Integer::intValue).min().orElse(0);
    long now = System.currentTimeMillis();
    long refreshAfter = now + (ttlDays * 24L * 60L * 60L * 1000L);

    String sql = "INSERT INTO table_year_availability "
        + "(data_source, table_name, available_years, max_year, min_year, cached_at, refresh_after) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (data_source, table_name) DO UPDATE SET "
        + "available_years = EXCLUDED.available_years, "
        + "max_year = EXCLUDED.max_year, "
        + "min_year = EXCLUDED.min_year, "
        + "cached_at = EXCLUDED.cached_at, "
        + "refresh_after = EXCLUDED.refresh_after";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, dataSource);
      stmt.setString(2, tableName);
      stmt.setString(3, yearsJson);
      stmt.setInt(4, maxYear);
      stmt.setInt(5, minYear);
      stmt.setLong(6, now);
      stmt.setLong(7, refreshAfter);
      stmt.executeUpdate();
      LOGGER.debug("Cached year availability for {}/{}: {} years (min={}, max={})",
          dataSource, tableName, years.size(), minYear, maxYear);
    } catch (SQLException e) {
      LOGGER.warn("Error caching available years for {}/{}: {}", dataSource, tableName, e.getMessage());
    }
  }

  /**
   * Get max available year for a table from the cache (quick lookup).
   *
   * @param dataSource The data source
   * @param tableName The table name
   * @return Max year, or -1 if not cached/expired
   */
  public int getMaxYearForTable(String dataSource, String tableName) {
    String sql = "SELECT max_year FROM table_year_availability "
        + "WHERE data_source = ? AND table_name = ? AND refresh_after > ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, dataSource);
      stmt.setString(2, tableName);
      stmt.setLong(3, System.currentTimeMillis());
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt("max_year");
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting max year for {}/{}: {}", dataSource, tableName, e.getMessage());
    }
    return -1;
  }

  /**
   * Parse JSON array of years into a Set.
   */
  private Set<Integer> parseYearsJson(String json) {
    Set<Integer> years = new HashSet<>();
    if (json == null || json.isEmpty()) {
      return years;
    }
    // Simple JSON array parsing: "[2010,2011,2012]"
    String cleaned = json.replace("[", "").replace("]", "").trim();
    if (cleaned.isEmpty()) {
      return years;
    }
    for (String yearStr : cleaned.split(",")) {
      try {
        years.add(Integer.parseInt(yearStr.trim()));
      } catch (NumberFormatException e) {
        // Skip invalid year
      }
    }
    return years;
  }

  /**
   * Format a Set of years as JSON array.
   */
  private String formatYearsJson(Set<Integer> years) {
    List<Integer> sorted = new ArrayList<>(years);
    Collections.sort(sorted);
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < sorted.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(sorted.get(i));
    }
    sb.append("]");
    return sb.toString();
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
