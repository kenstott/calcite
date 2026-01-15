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
package org.apache.calcite.adapter.file.partition;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DuckDB-based partition status store for tracking incremental processing
 * of alternate partitions in the file adapter.
 *
 * <p>Each base directory gets its own DuckDB file at .partition_status.duckdb.
 * This decouples the file adapter from govdata's DuckDBCacheStore.
 */
public class DuckDBPartitionStatusStore implements IncrementalTracker, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBPartitionStatusStore.class);

  /** SQL resource path prefix. */
  private static final String SQL_RESOURCE_PATH =
      "org/apache/calcite/adapter/file/partition/";

  /** Static map of open stores by path for reuse within same JVM. */
  private static final Map<String, DuckDBPartitionStatusStore> OPEN_STORES =
      new ConcurrentHashMap<>();

  /** Path to the DuckDB database file. */
  private final String dbPath;

  /** Base directory this store is associated with. */
  private final String baseDirectory;

  /** Shared connection - DuckDB handles concurrent access internally. */
  private Connection connection;

  /** Lock for connection management. */
  private final Object connectionLock = new Object();

  private DuckDBPartitionStatusStore(String baseDirectory) {
    this.baseDirectory = baseDirectory;
    this.dbPath = new File(baseDirectory, ".partition_status.duckdb").getAbsolutePath();
  }

  /**
   * Get or create a partition status store for the given base directory.
   *
   * @param baseDirectory Base directory for parquet files
   * @return DuckDBPartitionStatusStore instance
   */
  public static DuckDBPartitionStatusStore getInstance(String baseDirectory) {
    return OPEN_STORES.computeIfAbsent(baseDirectory, dir -> {
      DuckDBPartitionStatusStore store = new DuckDBPartitionStatusStore(dir);
      store.initialize();
      return store;
    });
  }

  /**
   * Initialize the store by ensuring directory exists and creating tables.
   */
  private void initialize() {
    File dbFile = new File(dbPath);
    File parentDir = dbFile.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      boolean created = parentDir.mkdirs();
      if (!created && !parentDir.exists()) {
        LOGGER.error("Failed to create directory for partition status store: {}",
            parentDir.getAbsolutePath());
        throw new RuntimeException("Failed to create directory: " + parentDir);
      }
    }

    try {
      getConnection();
      createTables();
      LOGGER.info("Initialized DuckDB partition status store at {}", dbPath);
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize partition status store at {}: {}",
          dbPath, e.getMessage());
      throw new RuntimeException("Failed to initialize partition status store", e);
    }
  }

  /**
   * Get or create the database connection.
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
   */
  private String loadSqlResource(String resourceName) {
    String resourcePath = SQL_RESOURCE_PATH + resourceName;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new RuntimeException("SQL resource not found: " + resourcePath);
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
        return sb.toString();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load SQL resource: " + resourcePath, e);
    }
  }

  /**
   * Execute SQL statements from a resource file.
   */
  private void executeSqlResource(String resourceName) throws SQLException {
    String sql = loadSqlResource(resourceName);
    LOGGER.debug("Executing SQL resource {}", resourceName);
    for (String statement : sql.split(";")) {
      String trimmed = statement.trim();
      if (!trimmed.isEmpty()) {
        // Remove comment lines first, then check if anything remains
        StringBuilder sqlBuilder = new StringBuilder();
        for (String line : trimmed.split("\n")) {
          String trimmedLine = line.trim();
          if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("--")) {
            sqlBuilder.append(line).append("\n");
          }
        }
        String cleanSql = sqlBuilder.toString().trim();
        if (!cleanSql.isEmpty()) {
          executeWithRetry(cleanSql);
        }
      }
    }
  }

  /**
   * Create the partition_status table.
   */
  private void createTables() throws SQLException {
    executeSqlResource("create_partition_status.sql");
  }

  // ===== IncrementalTracker Implementation =====

  @Override public boolean isProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    String keyValuesJson = mapToJson(keyValues);
    String sql = "SELECT processed_at FROM partition_status "
        + "WHERE alternate_name = ? AND incremental_key_values = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          long processedAt = rs.getLong("processed_at");
          return processedAt > 0;
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking partition status for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
    return false;
  }

  @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long ttlMillis) {
    String keyValuesJson = mapToJson(keyValues);
    String sql = "SELECT processed_at FROM partition_status "
        + "WHERE alternate_name = ? AND incremental_key_values = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          long processedAt = rs.getLong("processed_at");
          if (processedAt <= 0) {
            return false;
          }
          // Check if TTL has expired
          long now = System.currentTimeMillis();
          boolean withinTtl = (now - processedAt) < ttlMillis;
          if (!withinTtl) {
            LOGGER.debug("TTL expired for {}/{}: processed {}ms ago, TTL is {}ms",
                alternateName, keyValues, now - processedAt, ttlMillis);
          }
          return withinTtl;
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking partition status with TTL for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
    return false;
  }

  @Override public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    // Legacy method: mark with unknown row count (NULL)
    markProcessedWithRowCount(alternateName, sourceTable, keyValues, targetPattern, -1);
  }

  @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    long now = System.currentTimeMillis();
    String keyValuesJson = mapToJson(keyValues);

    String sql = "INSERT INTO partition_status "
        + "(alternate_name, incremental_key_values, source_table, target_pattern, processed_at, row_count, error_status, error_message) "
        + "VALUES (?, ?, ?, ?, ?, ?, FALSE, NULL) "
        + "ON CONFLICT (alternate_name, incremental_key_values) DO UPDATE SET "
        + "source_table = EXCLUDED.source_table, "
        + "target_pattern = EXCLUDED.target_pattern, "
        + "processed_at = EXCLUDED.processed_at, "
        + "row_count = EXCLUDED.row_count, "
        + "error_status = FALSE, "
        + "error_message = NULL";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      stmt.setString(3, sourceTable);
      stmt.setString(4, targetPattern);
      stmt.setLong(5, now);
      if (rowCount >= 0) {
        stmt.setInt(6, (int) rowCount);
      } else {
        stmt.setNull(6, java.sql.Types.INTEGER);
      }
      stmt.executeUpdate();
      LOGGER.debug("Marked {} with key {} as processed at {} (rows={})",
          alternateName, keyValues, now, rowCount >= 0 ? rowCount : "unknown");
    } catch (SQLException e) {
      LOGGER.error("Error marking partition status for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
  }

  @Override public void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    long now = System.currentTimeMillis();
    String keyValuesJson = mapToJson(keyValues);

    String sql = "INSERT INTO partition_status "
        + "(alternate_name, incremental_key_values, source_table, target_pattern, processed_at, row_count, error_status, error_message) "
        + "VALUES (?, ?, ?, ?, ?, 0, TRUE, ?) "
        + "ON CONFLICT (alternate_name, incremental_key_values) DO UPDATE SET "
        + "source_table = EXCLUDED.source_table, "
        + "target_pattern = EXCLUDED.target_pattern, "
        + "processed_at = EXCLUDED.processed_at, "
        + "row_count = 0, "
        + "error_status = TRUE, "
        + "error_message = EXCLUDED.error_message";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      stmt.setString(3, sourceTable);
      stmt.setString(4, targetPattern);
      stmt.setLong(5, now);
      stmt.setString(6, errorMessage != null ? errorMessage.substring(0, Math.min(1000, errorMessage.length())) : null);
      stmt.executeUpdate();
      LOGGER.info("Marked {} with key {} as ERROR at {} (message={})",
          alternateName, keyValues, now, errorMessage != null ? errorMessage.substring(0, Math.min(100, errorMessage.length())) : "null");
    } catch (SQLException e) {
      LOGGER.error("Error marking error status for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
  }

  @Override public boolean isProcessedWithEmptyTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long emptyResultTtlMillis) {
    String keyValuesJson = mapToJson(keyValues);
    String sql = "SELECT processed_at, row_count FROM partition_status "
        + "WHERE alternate_name = ? AND incremental_key_values = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          long processedAt = rs.getLong("processed_at");
          if (processedAt <= 0) {
            return false;
          }

          // Check row_count - if 0 (empty result), apply TTL
          int rowCount = rs.getInt("row_count");
          boolean wasEmpty = !rs.wasNull() && rowCount == 0;

          if (wasEmpty) {
            // Empty result - check if TTL has expired
            long now = System.currentTimeMillis();
            boolean withinTtl = (now - processedAt) < emptyResultTtlMillis;
            if (!withinTtl) {
              LOGGER.debug("Empty result TTL expired for {}/{}: processed {}ms ago, TTL is {}ms",
                  alternateName, keyValues, now - processedAt, emptyResultTtlMillis);
            }
            return withinTtl;
          }

          // Non-empty or legacy entry (row_count NULL) - treat as permanently processed
          return true;
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error checking partition status with empty TTL for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
    return false;
  }

  @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
    Set<Map<String, String>> result = new HashSet<>();
    String sql = "SELECT incremental_key_values FROM partition_status "
        + "WHERE alternate_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String json = rs.getString("incremental_key_values");
          Map<String, String> keyValues = jsonToMap(json);
          if (keyValues != null) {
            result.add(keyValues);
          }
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Error getting processed keys for {}: {}", alternateName, e.getMessage());
    }
    return result;
  }

  @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
    String keyValuesJson = mapToJson(keyValues);
    String sql = "DELETE FROM partition_status "
        + "WHERE alternate_name = ? AND incremental_key_values = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.info("Invalidated partition status for {} with key {}",
            alternateName, keyValues);
      }
    } catch (SQLException e) {
      LOGGER.warn("Error invalidating partition status for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
  }

  @Override public void invalidateAll(String alternateName) {
    String sql = "DELETE FROM partition_status WHERE alternate_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.info("Invalidated all {} partition status entries for {}",
            deleted, alternateName);
      }
    } catch (SQLException e) {
      LOGGER.warn("Error invalidating all partition status for {}: {}",
          alternateName, e.getMessage());
    }
  }

  // ===== JSON Utilities =====

  /**
   * Convert a map to JSON string for storage.
   * Simple implementation without external dependencies.
   */
  private String mapToJson(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      sb.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
          .append(escapeJson(entry.getValue())).append("\"");
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Escape special characters for JSON.
   */
  private String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  /**
   * Parse JSON string back to map.
   * Simple implementation for the specific format we generate.
   */
  private Map<String, String> jsonToMap(String json) {
    if (json == null || json.equals("{}") || json.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new LinkedHashMap<>();
    // Remove outer braces
    String content = json.substring(1, json.length() - 1).trim();
    if (content.isEmpty()) {
      return result;
    }
    // Simple parsing - assumes no nested objects or arrays
    int pos = 0;
    while (pos < content.length()) {
      // Find key
      int keyStart = content.indexOf('"', pos);
      if (keyStart < 0) {
        break;
      }
      int keyEnd = content.indexOf('"', keyStart + 1);
      if (keyEnd < 0) {
        break;
      }
      String key = content.substring(keyStart + 1, keyEnd);

      // Find colon
      int colonPos = content.indexOf(':', keyEnd);
      if (colonPos < 0) {
        break;
      }

      // Find value
      int valueStart = content.indexOf('"', colonPos);
      if (valueStart < 0) {
        break;
      }
      int valueEnd = findClosingQuote(content, valueStart + 1);
      if (valueEnd < 0) {
        break;
      }
      String value = unescapeJson(content.substring(valueStart + 1, valueEnd));

      result.put(key, value);

      // Move to next pair
      pos = valueEnd + 1;
      int commaPos = content.indexOf(',', pos);
      if (commaPos < 0) {
        break;
      }
      pos = commaPos + 1;
    }
    return result;
  }

  /**
   * Find the closing quote, handling escaped quotes.
   */
  private int findClosingQuote(String s, int start) {
    for (int i = start; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"') {
        return i;
      }
      if (c == '\\' && i + 1 < s.length()) {
        i++; // Skip escaped character
      }
    }
    return -1;
  }

  /**
   * Unescape JSON string.
   */
  private String unescapeJson(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\\"", "\"")
        .replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t");
  }

  // ===== Bulk Filtering Implementation =====

  @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }

    long startMs = System.currentTimeMillis();

    // Build a temporary table with all combinations and their indices
    // Then use a single SQL query to filter out processed ones
    try {
      Connection conn = getConnection();

      // Create temp table for combinations
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS temp_combinations "
            + "(idx INTEGER, key_values VARCHAR)");
        stmt.execute("DELETE FROM temp_combinations");
      }

      // Insert all combinations with their indices
      String insertSql = "INSERT INTO temp_combinations (idx, key_values) VALUES (?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
        for (int i = 0; i < allCombinations.size(); i++) {
          stmt.setInt(1, i);
          stmt.setString(2, mapToJson(allCombinations.get(i)));
          stmt.addBatch();
          // Execute in batches of 1000 to avoid memory issues
          if ((i + 1) % 1000 == 0) {
            stmt.executeBatch();
          }
        }
        stmt.executeBatch();
      }

      // Query for unprocessed combinations using LEFT ANTI JOIN pattern
      Set<Integer> unprocessedIndices = new HashSet<>();
      String filterSql = "SELECT t.idx FROM temp_combinations t "
          + "LEFT JOIN partition_status p ON p.alternate_name = ? "
          + "AND p.incremental_key_values = t.key_values "
          + "WHERE p.processed_at IS NULL";

      try (PreparedStatement stmt = conn.prepareStatement(filterSql)) {
        stmt.setString(1, alternateName);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            unprocessedIndices.add(rs.getInt("idx"));
          }
        }
      }

      // Cleanup temp table
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS temp_combinations");
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.debug("Bulk filtering for {}: {} unprocessed of {} total ({}ms)",
          alternateName, unprocessedIndices.size(), allCombinations.size(), elapsedMs);

      return unprocessedIndices;

    } catch (SQLException e) {
      LOGGER.warn("Bulk filtering failed for {}, falling back to per-item check: {}",
          alternateName, e.getMessage());
      // Fallback: return all indices as unprocessed
      Set<Integer> all = new HashSet<>();
      for (int i = 0; i < allCombinations.size(); i++) {
        all.add(i);
      }
      return all;
    }
  }

  @Override public Set<Integer> filterUnprocessedWithEmptyTtl(String alternateName,
      String sourceTable, List<Map<String, String>> allCombinations, long emptyResultTtlMillis) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }

    long startMs = System.currentTimeMillis();
    long now = System.currentTimeMillis();
    long ttlCutoff = now - emptyResultTtlMillis;

    try {
      Connection conn = getConnection();

      // Create temp table for combinations
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS temp_combinations "
            + "(idx INTEGER, key_values VARCHAR)");
        stmt.execute("DELETE FROM temp_combinations");
      }

      // Insert all combinations with their indices
      String insertSql = "INSERT INTO temp_combinations (idx, key_values) VALUES (?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
        for (int i = 0; i < allCombinations.size(); i++) {
          stmt.setInt(1, i);
          stmt.setString(2, mapToJson(allCombinations.get(i)));
          stmt.addBatch();
          if ((i + 1) % 1000 == 0) {
            stmt.executeBatch();
          }
        }
        stmt.executeBatch();
      }

      // Query for combinations that need processing:
      // 1. Never processed (p.processed_at IS NULL), OR
      // 2. Processed with 0 rows AND TTL expired (row_count = 0 AND processed_at < cutoff)
      Set<Integer> unprocessedIndices = new HashSet<>();
      String filterSql = "SELECT t.idx FROM temp_combinations t "
          + "LEFT JOIN partition_status p ON p.alternate_name = ? "
          + "AND p.incremental_key_values = t.key_values "
          + "WHERE p.processed_at IS NULL "
          + "   OR (p.row_count = 0 AND p.processed_at < ?)";

      try (PreparedStatement stmt = conn.prepareStatement(filterSql)) {
        stmt.setString(1, alternateName);
        stmt.setLong(2, ttlCutoff);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            unprocessedIndices.add(rs.getInt("idx"));
          }
        }
      }

      // Cleanup temp table
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS temp_combinations");
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.debug("Bulk filtering with TTL for {}: {} need processing of {} total ({}ms)",
          alternateName, unprocessedIndices.size(), allCombinations.size(), elapsedMs);

      return unprocessedIndices;

    } catch (SQLException e) {
      LOGGER.warn("Bulk filtering with TTL failed for {}, falling back to per-item check: {}",
          alternateName, e.getMessage());
      // Fallback: return all indices as needing processing
      Set<Integer> all = new HashSet<>();
      for (int i = 0; i < allCombinations.size(); i++) {
        all.add(i);
      }
      return all;
    }
  }

  @Override public Set<Integer> filterUnprocessedWithTtl(String alternateName,
      String sourceTable, List<Map<String, String>> allCombinations,
      long emptyResultTtlMillis, long errorTtlMillis) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }

    long startMs = System.currentTimeMillis();
    long now = System.currentTimeMillis();
    long emptyTtlCutoff = now - emptyResultTtlMillis;
    long errorTtlCutoff = now - errorTtlMillis;

    try {
      Connection conn = getConnection();

      // Create temp table for combinations
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TEMP TABLE IF NOT EXISTS temp_combinations "
            + "(idx INTEGER, key_values VARCHAR)");
        stmt.execute("DELETE FROM temp_combinations");
      }

      // Insert all combinations with their indices
      String insertSql = "INSERT INTO temp_combinations (idx, key_values) VALUES (?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
        for (int i = 0; i < allCombinations.size(); i++) {
          stmt.setInt(1, i);
          stmt.setString(2, mapToJson(allCombinations.get(i)));
          stmt.addBatch();
          if ((i + 1) % 1000 == 0) {
            stmt.executeBatch();
          }
        }
        stmt.executeBatch();
      }

      // Query for combinations that need processing:
      // 1. Never processed (p.processed_at IS NULL), OR
      // 2. Error status AND error TTL expired, OR
      // 3. Empty result (row_count = 0, not error) AND empty TTL expired
      Set<Integer> unprocessedIndices = new HashSet<>();
      String filterSql = "SELECT t.idx FROM temp_combinations t "
          + "LEFT JOIN partition_status p ON p.alternate_name = ? "
          + "AND p.incremental_key_values = t.key_values "
          + "WHERE p.processed_at IS NULL "
          + "   OR (COALESCE(p.error_status, FALSE) = TRUE AND p.processed_at < ?) "
          + "   OR (COALESCE(p.error_status, FALSE) = FALSE AND p.row_count = 0 AND p.processed_at < ?)";

      try (PreparedStatement stmt = conn.prepareStatement(filterSql)) {
        stmt.setString(1, alternateName);
        stmt.setLong(2, errorTtlCutoff);
        stmt.setLong(3, emptyTtlCutoff);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            unprocessedIndices.add(rs.getInt("idx"));
          }
        }
      }

      // Cleanup temp table
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS temp_combinations");
      }

      long elapsedMs = System.currentTimeMillis() - startMs;
      LOGGER.debug("Bulk filtering with TTL for {}: {} need processing of {} total ({}ms, emptyTTL={}ms, errorTTL={}ms)",
          alternateName, unprocessedIndices.size(), allCombinations.size(), elapsedMs,
          emptyResultTtlMillis, errorTtlMillis);

      return unprocessedIndices;

    } catch (SQLException e) {
      LOGGER.warn("Bulk filtering with TTL failed for {}, falling back to all needing processing: {}",
          alternateName, e.getMessage());
      // Fallback: return all indices as needing processing
      Set<Integer> all = new HashSet<>();
      for (int i = 0; i < allCombinations.size(); i++) {
        all.add(i);
      }
      return all;
    }
  }

  // ===== Table Completion Tracking Implementation =====

  @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
    String sql = "SELECT signature, completed_at FROM table_completion "
        + "WHERE pipeline_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String storedSignature = rs.getString("signature");
          return dimensionSignature.equals(storedSignature);
        }
      }
    } catch (SQLException e) {
      // Table might not exist yet, treat as not complete
      LOGGER.debug("Error checking table completion for {}: {}", pipelineName, e.getMessage());
    }
    return false;
  }

  @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
    // Ensure the table_completion table exists
    try {
      ensureTableCompletionTableExists();
    } catch (SQLException e) {
      LOGGER.error("Failed to create table_completion table: {}", e.getMessage());
      return;
    }

    long now = System.currentTimeMillis();
    String sql = "INSERT INTO table_completion (pipeline_name, signature, completed_at) "
        + "VALUES (?, ?, ?) "
        + "ON CONFLICT (pipeline_name) DO UPDATE SET "
        + "signature = EXCLUDED.signature, "
        + "completed_at = EXCLUDED.completed_at";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      stmt.setString(2, dimensionSignature);
      stmt.setLong(3, now);
      stmt.executeUpdate();
      LOGGER.debug("Marked pipeline {} as complete with signature {}", pipelineName, dimensionSignature);
    } catch (SQLException e) {
      LOGGER.error("Error marking table completion for {}: {}", pipelineName, e.getMessage());
    }
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    // Ensure the table_completion table exists
    try {
      ensureTableCompletionTableExists();
    } catch (SQLException e) {
      LOGGER.error("Failed to create table_completion table: {}", e.getMessage());
      return;
    }

    long now = System.currentTimeMillis();
    String sql = "INSERT INTO table_completion (pipeline_name, signature, config_hash, row_count, completed_at) "
        + "VALUES (?, ?, ?, ?, ?) "
        + "ON CONFLICT (pipeline_name) DO UPDATE SET "
        + "signature = EXCLUDED.signature, "
        + "config_hash = EXCLUDED.config_hash, "
        + "row_count = EXCLUDED.row_count, "
        + "completed_at = EXCLUDED.completed_at";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      stmt.setString(2, dimensionSignature);
      stmt.setString(3, configHash);
      stmt.setLong(4, rowCount);
      stmt.setLong(5, now);
      stmt.executeUpdate();
      LOGGER.debug("Marked pipeline {} as complete with configHash={}, signature={}, rows={}",
          pipelineName, configHash, dimensionSignature, rowCount);
    } catch (SQLException e) {
      LOGGER.error("Error marking table completion for {}: {}", pipelineName, e.getMessage());
    }
  }

  @Override public CachedCompletion getCachedCompletion(String pipelineName) {
    String sql = "SELECT config_hash, signature, row_count FROM table_completion "
        + "WHERE pipeline_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String configHash = rs.getString("config_hash");
          String signature = rs.getString("signature");
          long rowCount = rs.getLong("row_count");
          if (configHash != null) {
            return new CachedCompletion(configHash, signature, rowCount);
          }
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting cached completion for {}: {}", pipelineName, e.getMessage());
    }
    return null;
  }

  @Override public void invalidateTableCompletion(String pipelineName) {
    String sql = "DELETE FROM table_completion WHERE pipeline_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      int deleted = stmt.executeUpdate();
      if (deleted > 0) {
        LOGGER.info("Invalidated table completion for {}", pipelineName);
      }
    } catch (SQLException e) {
      LOGGER.debug("Error invalidating table completion for {}: {}", pipelineName, e.getMessage());
    }
  }

  @Override public void clearAllCompletions() {
    LOGGER.info("Clearing ALL completion tracking state (freshStart)");
    try {
      // Clear partition_status table
      try (Statement stmt = getConnection().createStatement()) {
        int partitionDeleted = stmt.executeUpdate("DELETE FROM partition_status");
        LOGGER.info("Cleared {} partition_status entries", partitionDeleted);
      }

      // Clear table_completion table
      try (Statement stmt = getConnection().createStatement()) {
        int completionDeleted = stmt.executeUpdate("DELETE FROM table_completion");
        LOGGER.info("Cleared {} table_completion entries", completionDeleted);
      }

      LOGGER.info("Fresh start: all completion tracking state cleared");
    } catch (SQLException e) {
      LOGGER.error("Error clearing completion tracking state: {}", e.getMessage());
    }
  }

  /**
   * Ensures the table_completion table exists with config hash support.
   */
  private void ensureTableCompletionTableExists() throws SQLException {
    String sql = "CREATE TABLE IF NOT EXISTS table_completion ("
        + "pipeline_name VARCHAR PRIMARY KEY, "
        + "signature VARCHAR NOT NULL, "
        + "config_hash VARCHAR, "
        + "row_count BIGINT DEFAULT 0, "
        + "completed_at BIGINT NOT NULL)";
    try (Statement stmt = getConnection().createStatement()) {
      stmt.execute(sql);
      // Add columns if they don't exist (for existing databases)
      try {
        stmt.execute("ALTER TABLE table_completion ADD COLUMN IF NOT EXISTS config_hash VARCHAR");
        stmt.execute("ALTER TABLE table_completion ADD COLUMN IF NOT EXISTS row_count BIGINT DEFAULT 0");
      } catch (SQLException e) {
        // Ignore if columns already exist or syntax not supported
        LOGGER.debug("Column migration: {}", e.getMessage());
      }
    }
  }

  @Override public void close() {
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
    OPEN_STORES.remove(baseDirectory);
  }

  /**
   * Get the database path.
   */
  public String getDbPath() {
    return dbPath;
  }

  /**
   * Get the base directory.
   */
  public String getBaseDirectory() {
    return baseDirectory;
  }
}
