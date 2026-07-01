/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL-backed PipelineTracker for shared, multi-worker pipeline state.
 *
 * <p>Uses a PostgreSQL table to track pipeline state, enabling concurrent
 * ETL workers on different machines to share state without file locking.
 *
 * <p>Schema:
 * <pre>
 * CREATE TABLE pipeline_tracker (
 *   source_key   VARCHAR NOT NULL,
 *   table_name   VARCHAR NOT NULL,
 *   phase        VARCHAR NOT NULL,
 *   state        VARCHAR NOT NULL,
 *   row_count    BIGINT DEFAULT -1,
 *   config_hash  VARCHAR,
 *   signature    VARCHAR,
 *   error_message VARCHAR,
 *   as_of        BIGINT NOT NULL,
 *   PRIMARY KEY (source_key, table_name, phase)
 *);
 * </pre>
 *
 * <p>Also maintains a {@code table_completion} table for pipeline-level tracking
 * compatible with IncrementalTracker.
 */
public class PGPipelineTracker implements PipelineTracker, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PGPipelineTracker.class);

  /** Marker state for a genuine empty fetch (HTTP 200, zero rows), re-evaluated against the
   *  table high-water mark on read. See {@link S3HivePipelineTracker} for the rationale. */
  private static final String STATE_EMPTY = "empty";

  /** Years back an empty period stays pending when no later period has data. Override with the
   *  {@code govdata.tracker.emptyRecencyHorizonYears} system property. */
  private static final int EMPTY_RECENCY_HORIZON_YEARS =
      Integer.getInteger("govdata.tracker.emptyRecencyHorizonYears", 3);

  private final String jdbcUrl;
  private final String user;
  private final String password;
  private Connection connection;
  private final Object connectionLock = new Object();
  private boolean initialized;

  public PGPipelineTracker(String jdbcUrl, String user, String password) {
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
  }

  private Connection getConnection() throws SQLException {
    synchronized (connectionLock) {
      if (connection == null || connection.isClosed()) {
        if (user != null) {
          connection = DriverManager.getConnection(jdbcUrl, user, password);
        } else {
          connection = DriverManager.getConnection(jdbcUrl);
        }
        connection.setAutoCommit(true);
        LOGGER.debug("Opened PostgreSQL connection for pipeline tracker");
      }
      if (!initialized) {
        initializeSchema();
        initialized = true;
      }
      return connection;
    }
  }

  private void initializeSchema() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS pipeline_tracker ("
          + "  source_key VARCHAR NOT NULL,"
          + "  table_name VARCHAR NOT NULL,"
          + "  phase VARCHAR NOT NULL,"
          + "  state VARCHAR NOT NULL,"
          + "  row_count BIGINT DEFAULT -1,"
          + "  config_hash VARCHAR,"
          + "  signature VARCHAR,"
          + "  error_message VARCHAR,"
          + "  as_of BIGINT NOT NULL,"
          + "  PRIMARY KEY (source_key, table_name, phase)"
          + ")");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS table_completion ("
          + "  pipeline_name VARCHAR PRIMARY KEY,"
          + "  signature VARCHAR NOT NULL,"
          + "  config_hash VARCHAR,"
          + "  row_count BIGINT DEFAULT 0,"
          + "  source_file_watermark BIGINT DEFAULT 0,"
          + "  completed_at BIGINT NOT NULL"
          + ")");

      // Index for phase-based lookups
      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_pipeline_tracker_phase "
          + "ON pipeline_tracker(phase, state)");

      LOGGER.info("Initialized PostgreSQL pipeline tracker schema");
    }
  }

  // ===== PipelineTracker Implementation =====

  @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
    String sql = "SELECT state FROM pipeline_tracker "
        + "WHERE source_key = ? AND table_name = ? AND phase = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, tableName);
      stmt.setString(3, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return "complete".equals(rs.getString("state"));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error checking completion for {}/{}/{}: {}",
          sourceKey, tableName, phase, e.getMessage());
    }
    return false;
  }

  @Override public void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    upsertState(sourceKey, tableName, phase, "complete", rowCount, null, null, null);
  }

  @Override public void markError(String sourceKey, String tableName, String phase,
      String error) {
    upsertState(sourceKey, tableName, phase, "error", 0, null, null, error);
  }

  @Override public void markCleared(String sourceKey, String tableName, String phase) {
    String sql = "DELETE FROM pipeline_tracker "
        + "WHERE source_key = ? AND table_name = ? AND phase = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, tableName);
      stmt.setString(3, phase);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (markCleared) for "
          + sourceKey + "/" + tableName + "/" + phase + ": " + e.getMessage(), e);
    }
  }

  @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
    Set<String> tables = new LinkedHashSet<>();
    String sql = "SELECT table_name FROM pipeline_tracker "
        + "WHERE source_key = ? AND phase = ? AND state = 'complete'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString("table_name"));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting completed tables for {}/{}: {}",
          sourceKey, phase, e.getMessage());
    }
    return tables;
  }

  @Override public Set<String> getSourceKeysForPhase(String phase) {
    Set<String> keys = new LinkedHashSet<>();
    String sql = "SELECT DISTINCT source_key FROM pipeline_tracker "
        + "WHERE phase = ? AND state = 'complete'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          keys.add(rs.getString("source_key"));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting source keys for phase {}: {}", phase, e.getMessage());
    }
    return keys;
  }

  private void upsertState(String sourceKey, String tableName, String phase,
      String state, long rowCount, String configHash, String signature,
      String errorMessage) {
    long now = System.currentTimeMillis();
    String sql = "INSERT INTO pipeline_tracker "
        + "(source_key, table_name, phase, state, row_count, config_hash, "
        + "signature, error_message, as_of) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (source_key, table_name, phase) DO UPDATE SET "
        + "state = EXCLUDED.state, row_count = EXCLUDED.row_count, "
        + "config_hash = EXCLUDED.config_hash, signature = EXCLUDED.signature, "
        + "error_message = EXCLUDED.error_message, as_of = EXCLUDED.as_of";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, tableName);
      stmt.setString(3, phase);
      stmt.setString(4, state);
      stmt.setLong(5, rowCount);
      stmt.setString(6, configHash);
      stmt.setString(7, signature);
      stmt.setString(8, errorMessage != null
          ? errorMessage.substring(0, Math.min(1000, errorMessage.length())) : null);
      stmt.setLong(9, now);
      stmt.executeUpdate();
    } catch (SQLException e) {
      // Fail loud: a tracker write that does not persist means ETL would write data without
      // recording completion state (silent idempotence loss / endless reprocessing). Refuse.
      throw new RuntimeException("PG tracker write failed (upsertState) for "
          + sourceKey + "/" + tableName + "/" + phase + ": " + e.getMessage(), e);
    }
  }

  // ===== IncrementalTracker Bridge =====

  @Override public boolean isProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    return isComplete(flattenKeyValues(keyValues), alternateName, "incremental");
  }

  @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long ttlMillis) {
    String sourceKey = flattenKeyValues(keyValues);
    String sql = "SELECT as_of FROM pipeline_tracker "
        + "WHERE source_key = ? AND table_name = ? AND phase = 'incremental' "
        + "AND state = 'complete'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          long processedAt = rs.getLong("as_of");
          return (System.currentTimeMillis() - processedAt) < ttlMillis;
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error checking TTL for {}: {}", alternateName, e.getMessage());
    }
    return false;
  }

  @Override public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    markProcessedWithRowCount(alternateName, sourceTable, keyValues, targetPattern, -1);
  }

  @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    upsertState(flattenKeyValues(keyValues), alternateName, "incremental",
        "complete", rowCount, null, null, null);
  }

  @Override public void markProcessedEmpty(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    // Non-'complete' state: filterUnprocessed re-evaluates it against the table high-water
    // mark and promotes it to 'complete' only once it is settled.
    upsertState(flattenKeyValues(keyValues), alternateName, "incremental",
        STATE_EMPTY, 0, null, null, null);
  }

  /** Empty (zero-row) markers for a table, as unflattened key maps. */
  private Set<Map<String, String>> getEmptyKeyValues(String alternateName) {
    Set<Map<String, String>> result = new HashSet<>();
    String sql = "SELECT source_key FROM pipeline_tracker "
        + "WHERE table_name = ? AND phase = 'incremental' AND state = '" + STATE_EMPTY + "'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          result.add(unflattenKeyValues(rs.getString("source_key")));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting empty keys for {}: {}", alternateName, e.getMessage());
    }
    return result;
  }

  /** Year carried by a combo's {@code year} slot, or 0 when absent/non-numeric. */
  private static int yearOf(Map<String, String> keyValues) {
    String y = keyValues != null ? keyValues.get("year") : null;
    if (y == null || y.isEmpty()) {
      return 0;
    }
    try {
      return Integer.parseInt(y);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override public void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    upsertState(flattenKeyValues(keyValues), alternateName, "incremental",
        "error", 0, null, null, errorMessage);
  }

  @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
    Set<Map<String, String>> result = new HashSet<>();
    String sql = "SELECT source_key FROM pipeline_tracker "
        + "WHERE table_name = ? AND phase = 'incremental' AND state = 'complete'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          result.add(unflattenKeyValues(rs.getString("source_key")));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting processed keys for {}: {}", alternateName, e.getMessage());
    }
    return result;
  }

  @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
    markCleared(flattenKeyValues(keyValues), alternateName, "incremental");
  }

  @Override public void invalidateAll(String alternateName) {
    String sql = "DELETE FROM pipeline_tracker "
        + "WHERE table_name = ? AND phase = 'incremental'";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (invalidateAll) for "
          + alternateName + ": " + e.getMessage(), e);
    }
  }

  @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Map<String, String>> processed = getProcessedKeyValues(alternateName);
    // Settle/promote empty markers against the high-water-mark year (newest period with data).
    // An empty period at/below the HWM, aged past the recency horizon, or with no period is
    // genuinely empty → processed (and promoted to 'complete'); an empty period above the HWM
    // is still pending → left unprocessed so the source is re-fetched.
    Set<Map<String, String>> empties = getEmptyKeyValues(alternateName);
    if (!empties.isEmpty()) {
      int hwm = 0;
      for (Map<String, String> c : processed) {
        int y = yearOf(c);
        if (y > hwm) {
          hwm = y;
        }
      }
      int currentYear = java.time.Year.now(java.time.ZoneOffset.UTC).getValue();
      for (Map<String, String> e : empties) {
        int y = yearOf(e);
        boolean settled = y <= 0
            || (hwm > 0 && y <= hwm)
            || (currentYear - y > EMPTY_RECENCY_HORIZON_YEARS);
        if (settled) {
          processed.add(e);
          upsertState(flattenKeyValues(e), alternateName, "incremental",
              "complete", 0, null, null, null);
        }
      }
    }
    Set<Integer> unprocessed = new HashSet<>();
    for (int i = 0; i < allCombinations.size(); i++) {
      if (!processed.contains(allCombinations.get(i))) {
        unprocessed.add(i);
      }
    }
    return unprocessed;
  }

  // ===== Table Completion =====

  @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
    String sql = "SELECT signature FROM table_completion WHERE pipeline_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return dimensionSignature.equals(rs.getString("signature"));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error checking table completion for {}: {}", pipelineName, e.getMessage());
    }
    return false;
  }

  @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
    long now = System.currentTimeMillis();
    String sql = "INSERT INTO table_completion (pipeline_name, signature, completed_at) "
        + "VALUES (?, ?, ?) "
        + "ON CONFLICT (pipeline_name) DO UPDATE SET "
        + "signature = EXCLUDED.signature, completed_at = EXCLUDED.completed_at";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      stmt.setString(2, dimensionSignature);
      stmt.setLong(3, now);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (markTableComplete) for "
          + pipelineName + ": " + e.getMessage(), e);
    }
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    long now = System.currentTimeMillis();
    String sql = "INSERT INTO table_completion "
        + "(pipeline_name, signature, config_hash, row_count, completed_at) "
        + "VALUES (?, ?, ?, ?, ?) "
        + "ON CONFLICT (pipeline_name) DO UPDATE SET "
        + "signature = EXCLUDED.signature, config_hash = EXCLUDED.config_hash, "
        + "row_count = EXCLUDED.row_count, completed_at = EXCLUDED.completed_at";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      stmt.setString(2, dimensionSignature);
      stmt.setString(3, configHash);
      stmt.setLong(4, rowCount);
      stmt.setLong(5, now);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (markTableComplete) for "
          + pipelineName + ": " + e.getMessage(), e);
    }
  }

  @Override public void markTableCompleteWithSourceWatermark(String pipelineName,
      String configHash, String dimensionSignature, long rowCount,
      long sourceFileWatermark) {
    long now = System.currentTimeMillis();
    String sql = "INSERT INTO table_completion "
        + "(pipeline_name, signature, config_hash, row_count, source_file_watermark, completed_at) "
        + "VALUES (?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (pipeline_name) DO UPDATE SET "
        + "signature = EXCLUDED.signature, config_hash = EXCLUDED.config_hash, "
        + "row_count = EXCLUDED.row_count, "
        + "source_file_watermark = EXCLUDED.source_file_watermark, "
        + "completed_at = EXCLUDED.completed_at";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      stmt.setString(2, dimensionSignature);
      stmt.setString(3, configHash);
      stmt.setLong(4, rowCount);
      stmt.setLong(5, sourceFileWatermark);
      stmt.setLong(6, now);
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (markTableComplete) for "
          + pipelineName + ": " + e.getMessage(), e);
    }
  }

  @Override public CachedCompletion getCachedCompletion(String pipelineName) {
    String sql = "SELECT config_hash, signature, row_count, completed_at, "
        + "COALESCE(source_file_watermark, 0) AS source_file_watermark "
        + "FROM table_completion WHERE pipeline_name = ?";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String configHash = rs.getString("config_hash");
          if (configHash != null) {
            return new CachedCompletion(
                configHash,
                rs.getString("signature"),
                rs.getLong("row_count"),
                rs.getLong("completed_at"),
                rs.getLong("source_file_watermark"));
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
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (invalidateTableCompletion) for "
          + pipelineName + ": " + e.getMessage(), e);
    }
  }

  @Override public void clearAllCompletions() {
    LOGGER.info("Clearing ALL pipeline tracker state (freshStart)");
    try {
      try (Statement stmt = getConnection().createStatement()) {
        int trackerDeleted = stmt.executeUpdate("DELETE FROM pipeline_tracker");
        LOGGER.info("Cleared {} pipeline_tracker entries", trackerDeleted);
      }
      try (Statement stmt = getConnection().createStatement()) {
        int completionDeleted = stmt.executeUpdate("DELETE FROM table_completion");
        LOGGER.info("Cleared {} table_completion entries", completionDeleted);
      }
    } catch (SQLException e) {
      throw new RuntimeException("PG tracker write failed (clearAllCompletions): "
          + e.getMessage(), e);
    }
  }

  // ===== Utility Methods =====

  private String flattenKeyValues(Map<String, String> keyValues) {
    if (keyValues == null || keyValues.isEmpty()) {
      return "_empty";
    }
    if (keyValues.size() == 1) {
      return keyValues.values().iterator().next();
    }
    StringBuilder sb = new StringBuilder();
    List<String> keys = new ArrayList<>(keyValues.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      if (sb.length() > 0) {
        sb.append("__");
      }
      sb.append(key).append("=").append(keyValues.get(key));
    }
    return sb.toString();
  }

  private Map<String, String> unflattenKeyValues(String sourceKey) {
    if ("_empty".equals(sourceKey)) {
      return new LinkedHashMap<String, String>();
    }
    Map<String, String> result = new LinkedHashMap<>();
    if (sourceKey.contains("=") && sourceKey.contains("__")) {
      for (String part : sourceKey.split("__")) {
        int eq = part.indexOf('=');
        if (eq > 0) {
          result.put(part.substring(0, eq), part.substring(eq + 1));
        }
      }
    }
    if (result.isEmpty()) {
      result.put("source_key", sourceKey);
    }
    return result;
  }

  @Override public void close() {
    synchronized (connectionLock) {
      if (connection != null) {
        try {
          connection.close();
          LOGGER.debug("Closed PostgreSQL pipeline tracker connection");
        } catch (SQLException e) {
          LOGGER.warn("Error closing PG tracker connection: {}", e.getMessage());
        }
        connection = null;
        initialized = false;
      }
    }
  }
}
