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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * S3 hive-partitioned append-only PipelineTracker.
 *
 * <p>Uses an in-memory DuckDB instance with the {@code httpfs} extension to read/write
 * hive-partitioned parquet files on S3. Since the DuckDB instance is in-memory,
 * there are no file locks, enabling safe concurrent access from multiple workers.
 *
 * <p>State model: append-only parquet files in hive layout:
 * <pre>
 *   s3://bucket/tracker/year={YYYY}/source_key={key}/{uuid}.parquet
 * </pre>
 *
 * <p>Read: {@code GROUP BY (source_key, table_name, phase), take MAX(as_of)} gives
 * the latest state for each combination.
 *
 * <p>Write: Each state change appends a new parquet file. No deletes, no updates.
 */
public class S3HivePipelineTracker implements PipelineTracker, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3HivePipelineTracker.class);

  private final String bucketPath;
  private final String endpoint;
  private final Map<String, String> config;
  private Connection connection;
  private final Object connectionLock = new Object();
  private boolean initialized;
  /** Cached result of probing for any tracker data; null = not yet checked. */
  private Boolean hasAnyTrackerData;

  /**
   * Create an S3-backed pipeline tracker.
   *
   * @param bucketPath S3 path for tracker data (e.g. "s3://bucket/tracker")
   * @param endpoint   Optional S3 endpoint override (for MinIO, R2, etc.)
   */
  public S3HivePipelineTracker(String bucketPath, String endpoint) {
    this(bucketPath, endpoint, Collections.<String, String>emptyMap());
  }

  /**
   * Create an S3-backed pipeline tracker with full configuration.
   *
   * @param bucketPath S3 path for tracker data (e.g. "s3://bucket/tracker")
   * @param endpoint   Optional S3 endpoint override (for MinIO, R2, etc.)
   * @param config     Configuration map with accessKeyId, secretAccessKey, region
   */
  public S3HivePipelineTracker(String bucketPath, String endpoint,
      Map<String, String> config) {
    this.bucketPath = bucketPath.endsWith("/") ? bucketPath.substring(0, bucketPath.length() - 1)
        : bucketPath;
    this.endpoint = endpoint;
    this.config = config != null ? config : Collections.<String, String>emptyMap();
  }

  private Connection getConnection() throws SQLException {
    synchronized (connectionLock) {
      if (connection == null || connection.isClosed()) {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        LOGGER.debug("Opened in-memory DuckDB connection for S3 tracker");
      }
      if (!initialized) {
        initializeExtensions();
        initialized = true;
      }
      return connection;
    }
  }

  private void initializeExtensions() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");

      // Configure S3 - credentials must come from config (model.json operand)
      String region = config.get("region");
      if (region != null && !region.isEmpty()) {
        stmt.execute("SET s3_region = '" + region + "'");
      }

      String accessKey = config.get("accessKeyId");
      String secretKey = config.get("secretAccessKey");
      if (accessKey != null && !accessKey.isEmpty()
          && secretKey != null && !secretKey.isEmpty()) {
        stmt.execute("SET s3_access_key_id = '" + accessKey + "'");
        stmt.execute("SET s3_secret_access_key = '" + secretKey + "'");
      } else {
        LOGGER.warn("S3 tracker missing accessKeyId/secretAccessKey in config. "
            + "Provide credentials via model.json operand. Available keys: {}",
            config.keySet());
      }

      String sessionToken = config.get("sessionToken");
      if (sessionToken != null && !sessionToken.isEmpty()) {
        stmt.execute("SET s3_session_token = '" + sessionToken + "'");
      }

      if (endpoint != null && !endpoint.isEmpty()) {
        stmt.execute("SET s3_endpoint = '" + endpoint.replaceAll("https?://", "") + "'");
        stmt.execute("SET s3_url_style = 'path'");
        if (endpoint.startsWith("http://")) {
          stmt.execute("SET s3_use_ssl = false");
        }
      }

      LOGGER.info("Initialized S3 httpfs extension for tracker at {} (endpoint={})",
          bucketPath, endpoint);
    }
  }

  /**
   * Bulk-retrieve completed tables for multiple source keys in a single DuckDB query.
   *
   * <p>Builds a single {@code read_parquet([glob1, glob2, ...])} call with one
   * targeted glob per source key, so DuckDB resolves all of them in one S3 round-trip
   * instead of N sequential queries.
   */
  @Override public Map<String, Set<String>> bulkGetCompletedTables(
      java.util.Collection<String> sourceKeys, String phase) {
    if (sourceKeys.isEmpty()) {
      return Collections.emptyMap();
    }

    // Build per-sourceKey globs with targeted year from accession number
    // e.g. s3://bucket/year=2026/source_key={safe}/*.parquet
    // Falls back to year=* only when year can't be extracted
    StringBuilder globList = new StringBuilder();
    globList.append('[');
    boolean first = true;
    for (String sk : sourceKeys) {
      if (!first) {
        globList.append(',');
      }
      String year = extractYear(sk, System.currentTimeMillis());
      globList.append('\'');
      globList.append(bucketPath).append("/year=").append(year)
          .append("/source_key=")
          .append(sanitizeHiveValue(sk)).append("/*.parquet");
      globList.append('\'');
      first = false;
    }
    globList.append(']');

    String sql = "SELECT source_key, table_name FROM ("
        + "  SELECT source_key, table_name, state, "
        + "    ROW_NUMBER() OVER (PARTITION BY source_key, table_name ORDER BY as_of DESC) AS rn"
        + "  FROM read_parquet(" + globList + ", "
        + "hive_partitioning=false, union_by_name=true)"
        + "  WHERE phase = ?"
        + ") WHERE rn = 1 AND state = 'complete'";

    Map<String, Set<String>> result = new HashMap<String, Set<String>>();
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String sourceKey = rs.getString("source_key");
          String tableName = rs.getString("table_name");
          Set<String> tables = result.get(sourceKey);
          if (tables == null) {
            tables = new LinkedHashSet<String>();
            result.put(sourceKey, tables);
          }
          tables.add(tableName);
        }
      }
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        // No tracker data for any of these source keys
        return Collections.emptyMap();
      }
      LOGGER.warn("Bulk tracker query failed for {} source keys: {}",
          sourceKeys.size(), msg);
    }
    return result;
  }

  /**
   * Write a state row to S3 as an append-only parquet file.
   */
  private void writeState(String sourceKey, String tableName, String phase,
      String state, long rowCount, String configHash, String signature,
      String errorMessage) {
    long asOf = System.currentTimeMillis();
    String year = extractYear(sourceKey, asOf);
    String safeSourceKey = sanitizeHiveValue(sourceKey);
    String fileName = UUID.randomUUID().toString() + ".parquet";
    String path = bucketPath + "/year=" + year + "/source_key=" + safeSourceKey
        + "/" + fileName;

    String sql = "COPY ("
        + "SELECT ? AS source_key, ? AS table_name, ? AS phase, ? AS state, "
        + "CAST(? AS BIGINT) AS row_count, ? AS config_hash, ? AS signature, "
        + "? AS error_message, CAST(? AS BIGINT) AS as_of"
        + ") TO '" + path + "' (FORMAT PARQUET)";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, tableName);
      stmt.setString(3, phase);
      stmt.setString(4, state);
      stmt.setLong(5, rowCount);
      stmt.setString(6, configHash);
      stmt.setString(7, signature);
      stmt.setString(8, errorMessage);
      stmt.setLong(9, asOf);
      stmt.executeUpdate();
      LOGGER.debug("Wrote tracker state to {}", path);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to write tracker state to " + path + ": " + e.getMessage(), e);
    }
  }

  /**
   * Read the latest state for a (source_key, table_name, phase) combination.
   */
  private String readLatestState(String sourceKey, String tableName, String phase) {
    String year = extractYear(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";
    String sql = "SELECT state FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE source_key = ? AND table_name = ? AND phase = ? "
        + "ORDER BY as_of DESC LIMIT 1";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, tableName);
      stmt.setString(3, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("state");
        }
      }
    } catch (SQLException e) {
      // Glob may match no files, treat as not found
      LOGGER.debug("No tracker state found for {}/{}/{}: {}",
          sourceKey, tableName, phase, e.getMessage());
    }
    return null;
  }

  // ===== PipelineTracker Implementation =====

  @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
    String state = readLatestState(sourceKey, tableName, phase);
    return "complete".equals(state);
  }

  @Override public void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    writeState(sourceKey, tableName, phase, "complete", rowCount, null, null, null);
  }

  @Override public void markError(String sourceKey, String tableName, String phase,
      String error) {
    writeState(sourceKey, tableName, phase, "error", 0, null, null, error);
  }

  @Override public void markCleared(String sourceKey, String tableName, String phase) {
    writeState(sourceKey, tableName, phase, "cleared", 0, null, null, null);
  }

  @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
    Set<String> tables = new LinkedHashSet<String>();
    String year = extractYear(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";

    String sql = "SELECT table_name, state FROM ("
        + "  SELECT table_name, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY table_name ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "  WHERE source_key = ? AND phase = ?"
        + ") WHERE rn = 1 AND state = 'complete'";

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

  // ===== IncrementalTracker Bridge Implementation =====

  @Override public boolean isProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    String sourceKey = flattenKeyValues(keyValues);
    String state = readLatestState(sourceKey, alternateName, "incremental");
    return "complete".equals(state);
  }

  @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long ttlMillis) {
    String sourceKey = flattenKeyValues(keyValues);
    String year = extractYear(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";
    String sql = "SELECT as_of FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE source_key = ? AND table_name = ? AND phase = 'incremental' "
        + "AND state = 'complete' "
        + "ORDER BY as_of DESC LIMIT 1";

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
      LOGGER.debug("TTL check failed for {}: {}", alternateName, e.getMessage());
    }
    return false;
  }

  @Override public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    markProcessedWithRowCount(alternateName, sourceTable, keyValues, targetPattern, -1);
  }

  @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "complete", rowCount,
        null, null, null);
  }

  @Override public void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "error", 0,
        null, null, errorMessage);
  }

  @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
    // S3 tracker: scan all partitions for this alternate name
    Set<Map<String, String>> result = new HashSet<>();
    String glob = bucketPath + "/year=*/source_key=*/*.parquet";

    String sql = "SELECT source_key FROM ("
        + "  SELECT source_key, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "  WHERE table_name = ? AND phase = 'incremental'"
        + ") WHERE rn = 1 AND state = 'complete'";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String sourceKey = rs.getString("source_key");
          result.add(unflattenKeyValues(sourceKey));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting processed keys for {}: {}", alternateName, e.getMessage());
    }
    return result;
  }

  @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "cleared", 0,
        null, null, null);
  }

  @Override public void invalidateAll(String alternateName) {
    // Append-only: read all completed source_keys and write "cleared" markers
    // Use hive_partitioning=false to avoid DuckDB Hive partition mismatch errors
    // when source_key values have different formats across schemas (e.g. SEC vs ETL).
    // The source_key column is stored inside each parquet file, so we read it from there.
    String glob = bucketPath + "/year=*/source_key=*/*.parquet";
    String sql = "SELECT source_key FROM ("
        + "  SELECT source_key, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + glob + "', "
        + "hive_partitioning=false, union_by_name=true) "
        + "  WHERE table_name = ? AND phase = 'incremental'"
        + ") WHERE rn = 1 AND state = 'complete'";

    Set<String> completedKeys = new LinkedHashSet<String>();
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          completedKeys.add(rs.getString("source_key"));
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Failed to read completed keys for invalidateAll({}): {}",
          alternateName, e.getMessage());
      return;
    }

    if (completedKeys.isEmpty()) {
      LOGGER.info("invalidateAll({}): no completed partition keys found", alternateName);
      return;
    }

    LOGGER.info("invalidateAll({}): clearing {} completed partition keys",
        alternateName, completedKeys.size());
    for (String sourceKey : completedKeys) {
      writeState(sourceKey, alternateName, "incremental", "cleared", 0,
          null, null, null);
    }
  }

  /** Maximum number of glob paths per DuckDB query to avoid OOM with large dimension spaces. */
  private static final int FILTER_CHUNK_SIZE = 50_000;

  @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }

    // Note: we intentionally do NOT cache "no data" across tables.
    // A "No files found" for one table does not mean other tables lack tracker data.

    // Build targeted globs from the known combinations instead of scanning source_key=*
    // Use extractYear to target specific year partitions instead of year=*
    // Pre-compute flattened keys for all combinations (reused for matching later)
    String[] flatKeys = new String[allCombinations.size()];
    List<String> sourceKeyPaths = new ArrayList<String>();
    for (int i = 0; i < allCombinations.size(); i++) {
      String flat = flattenKeyValues(allCombinations.get(i));
      flatKeys[i] = flat;
      String year = extractYear(flat, System.currentTimeMillis());
      sourceKeyPaths.add(bucketPath + "/year=" + year + "/source_key="
          + sanitizeHiveValue(flat) + "/*.parquet");
    }

    // Deduplicate paths (multiple combinations may map to the same glob)
    List<String> uniquePaths = new ArrayList<String>(new LinkedHashSet<String>(sourceKeyPaths));

    // Query in chunks to avoid OOM with large dimension spaces (e.g., 3M+ combinations)
    Set<String> processedKeys = new HashSet<String>();
    boolean anyNoFiles = false;

    for (int offset = 0; offset < uniquePaths.size(); offset += FILTER_CHUNK_SIZE) {
      int end = Math.min(offset + FILTER_CHUNK_SIZE, uniquePaths.size());
      List<String> chunk = uniquePaths.subList(offset, end);

      if (uniquePaths.size() > FILTER_CHUNK_SIZE) {
        LOGGER.info("Filtering chunk {}-{} of {} unique paths for {}",
            offset, end, uniquePaths.size(), alternateName);
      }

      // Build path list for this chunk
      StringBuilder pathList = new StringBuilder();
      pathList.append("[");
      boolean first = true;
      for (String p : chunk) {
        if (!first) {
          pathList.append(", ");
        }
        pathList.append("'").append(p).append("'");
        first = false;
      }
      pathList.append("]");

      String sql = "SELECT source_key FROM ("
          + "  SELECT source_key, state, ROW_NUMBER() OVER "
          + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
          + "  FROM read_parquet(" + pathList + ", "
          + "hive_partitioning=true, union_by_name=true) "
          + "  WHERE table_name = ? AND phase = 'incremental'"
          + ") WHERE rn = 1 AND state = 'complete'";

      try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
        stmt.setString(1, alternateName);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            processedKeys.add(rs.getString("source_key"));
          }
        }
      } catch (SQLException e) {
        String msg = e.getMessage();
        if (msg != null && (msg.contains("No files found")
            || msg.contains("Could not find")
            || msg.contains("HTTP 404"))) {
          anyNoFiles = true;
          // Continue to next chunk — other chunks may have data
          continue;
        }
        LOGGER.debug("Error filtering unprocessed for {} (chunk {}-{}): {}",
            alternateName, offset, end, msg);
      }
    }

    // Cache positive result only — presence of data is safe to cache
    if (!processedKeys.isEmpty()) {
      hasAnyTrackerData = true;
    }

    // If ALL chunks returned "no files" and nothing was found, return all as unprocessed
    if (processedKeys.isEmpty() && anyNoFiles) {
      LOGGER.info("No tracker data found for {} — all {} combinations unprocessed",
          alternateName, allCombinations.size());
      return allIndices(allCombinations.size());
    }

    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < allCombinations.size(); i++) {
      if (!processedKeys.contains(flatKeys[i])) {
        unprocessed.add(i);
      }
    }
    return unprocessed;
  }

  private Set<Integer> allIndices(int size) {
    Set<Integer> all = new HashSet<>();
    for (int i = 0; i < size; i++) {
      all.add(i);
    }
    return all;
  }

  // ===== Table Completion =====

  @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
    String state = readLatestState("_table_complete", pipelineName, "table_completion");
    if (!"complete".equals(state)) {
      return false;
    }
    // Check signature match
    String glob = bucketPath + "/year=*/source_key=_table_complete/*.parquet";
    String sql = "SELECT signature FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE table_name = ? AND phase = 'table_completion' AND state = 'complete' "
        + "ORDER BY as_of DESC LIMIT 1";
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
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        0, null, dimensionSignature, null);
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configHash, dimensionSignature, null);
  }

  @Override public void markTableCompleteWithSourceWatermark(String pipelineName,
      String configHash, String dimensionSignature, long rowCount,
      long sourceFileWatermark) {
    // Store watermark in config_hash field for simplicity
    String configWithWatermark = configHash + ":wm=" + sourceFileWatermark;
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configWithWatermark, dimensionSignature, null);
  }

  @Override public CachedCompletion getCachedCompletion(String pipelineName) {
    String glob = bucketPath + "/year=*/source_key=_table_complete/*.parquet";
    String sql = "SELECT config_hash, signature, row_count, as_of "
        + "FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "WHERE table_name = ? AND phase = 'table_completion' AND state = 'complete' "
        + "ORDER BY as_of DESC LIMIT 1";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String configHash = rs.getString("config_hash");
          String signature = rs.getString("signature");
          long rowCount = rs.getLong("row_count");
          long completedAt = rs.getLong("as_of");

          // Parse watermark from config hash if present
          long watermark = 0;
          if (configHash != null && configHash.contains(":wm=")) {
            int wmIdx = configHash.indexOf(":wm=");
            try {
              watermark = Long.parseLong(configHash.substring(wmIdx + 4));
              configHash = configHash.substring(0, wmIdx);
            } catch (NumberFormatException e) {
              // ignore
            }
          }
          return new CachedCompletion(configHash, signature, rowCount, completedAt, watermark);
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting cached completion for {}: {}", pipelineName, e.getMessage());
    }
    return null;
  }

  @Override public void invalidateTableCompletion(String pipelineName) {
    writeState("_table_complete", pipelineName, "table_completion", "cleared",
        0, null, null, null);
  }

  @Override public void clearAllCompletions() {
    LOGGER.warn("clearAllCompletions on S3 tracker writes 'cleared' markers. "
        + "Old parquet files remain but are superseded by the cleared state.");
    // Write cleared markers for table completion
    writeState("_table_complete", "_all", "table_completion", "cleared",
        0, null, null, null);
  }

  // ===== Utility Methods =====

  private String flattenKeyValues(Map<String, String> keyValues) {
    if (keyValues == null || keyValues.isEmpty()) {
      return "_empty";
    }
    if (keyValues.size() == 1) {
      return keyValues.values().iterator().next();
    }
    // Multi-key: sort and join
    StringBuilder sb = new StringBuilder();
    java.util.List<String> keys = new java.util.ArrayList<>(keyValues.keySet());
    java.util.Collections.sort(keys);
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
      return Collections.emptyMap();
    }
    // Try to parse key=value pairs
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
      // Single value, use generic key
      result.put("source_key", sourceKey);
    }
    return result;
  }

  private String sanitizeHiveValue(String value) {
    // Replace characters that are problematic in hive partition paths
    return value.replace("/", "_").replace(" ", "_").replace(":", "_");
  }

  private String extractYear(String sourceKey, long asOf) {
    if (sourceKey != null) {
      // 1. Flattened dimension key: "geography=state__type=acs__year=2023"
      int yearIdx = sourceKey.indexOf("year=");
      if (yearIdx >= 0) {
        int start = yearIdx + 5; // length of "year="
        int end = start;
        while (end < sourceKey.length() && Character.isDigit(sourceKey.charAt(end))) {
          end++;
        }
        if (end - start == 4) {
          return sourceKey.substring(start, end);
        }
      }

      // 2. SEC accession format: 0000123456-YY-012345
      if (sourceKey.length() >= 15 && sourceKey.charAt(10) == '-') {
        try {
          int yy = Integer.parseInt(sourceKey.substring(11, 13));
          return String.valueOf(yy >= 90 ? 1900 + yy : 2000 + yy);
        } catch (NumberFormatException e) {
          // Fall through
        }
      }

      // 3. Bare 4-digit year (single-dimension key like "2023")
      if (sourceKey.length() == 4) {
        try {
          int y = Integer.parseInt(sourceKey);
          if (y >= 1900 && y <= 2100) {
            return sourceKey;
          }
        } catch (NumberFormatException e) {
          // Fall through
        }
      }
    }
    // Fall back to current year from timestamp
    java.util.Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(asOf);
    return String.valueOf(cal.get(java.util.Calendar.YEAR));
  }

  @Override public void close() {
    synchronized (connectionLock) {
      if (connection != null) {
        try {
          connection.close();
          LOGGER.debug("Closed S3 tracker in-memory DuckDB connection");
        } catch (SQLException e) {
          LOGGER.warn("Error closing S3 tracker connection: {}", e.getMessage());
        }
        connection = null;
        initialized = false;
        hasAnyTrackerData = null;
      }
    }
  }
}
