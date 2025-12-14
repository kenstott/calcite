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
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(is, StandardCharsets.UTF_8))) {
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

  @Override
  public boolean isProcessed(String alternateName, String sourceTable,
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

  @Override
  public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    long now = System.currentTimeMillis();
    String keyValuesJson = mapToJson(keyValues);

    String sql = "INSERT INTO partition_status "
        + "(alternate_name, incremental_key_values, source_table, target_pattern, processed_at) "
        + "VALUES (?, ?, ?, ?, ?) "
        + "ON CONFLICT (alternate_name, incremental_key_values) DO UPDATE SET "
        + "source_table = EXCLUDED.source_table, "
        + "target_pattern = EXCLUDED.target_pattern, "
        + "processed_at = EXCLUDED.processed_at";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.setString(2, keyValuesJson);
      stmt.setString(3, sourceTable);
      stmt.setString(4, targetPattern);
      stmt.setLong(5, now);
      stmt.executeUpdate();
      LOGGER.debug("Marked {} with key {} as processed at {}",
          alternateName, keyValues, now);
    } catch (SQLException e) {
      LOGGER.error("Error marking partition status for {}/{}: {}",
          alternateName, keyValues, e.getMessage());
    }
  }

  @Override
  public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
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

  @Override
  public void invalidate(String alternateName, Map<String, String> keyValues) {
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

  @Override
  public void invalidateAll(String alternateName) {
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
