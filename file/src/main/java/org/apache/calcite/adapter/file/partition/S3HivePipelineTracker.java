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
import java.util.concurrent.ConcurrentHashMap;


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

  /** Fixed year partition for _table_complete markers. Avoids year=* wildcard scans. */
  private static final String COMPLETION_YEAR = "0";

  private final String bucketPath;
  private final String endpoint;
  private final Map<String, String> config;
  private Connection connection;
  private final Object connectionLock = new Object();
  private boolean initialized;
  /** Cached result of probing for any tracker data; null = not yet checked. */
  private Boolean hasAnyTrackerData;
  /** In-memory cache of table completions for the duration of this tracker instance. */
  private final Map<String, CachedCompletion> completionCache =
      new ConcurrentHashMap<String, CachedCompletion>();
  /**
   * In-memory cache of completed tables per (sourceKey, phase).
   * Key format: "sourceKey\0phase" → Set of completed table names.
   * Populated by {@link #bulkGetCompletedTables} and {@link #getCompletedTables},
   * so subsequent {@link #isComplete} calls are O(1) memory lookups.
   */
  private final Map<String, Set<String>> stageCache =
      new ConcurrentHashMap<String, Set<String>>();
  /**
   * Tracks which year+phase combinations have been attempted (prevents retries).
   * Key format: "year\0phase".
   */
  private final Set<String> scannedYears =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  /**
   * Tracks which year+phase combinations were fully scanned with complete data.
   * Only when a year is fully scanned is it safe to cache empty sets for missing
   * source keys (meaning they genuinely have no tracker data).
   */
  private final Set<String> fullyScannedYears =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

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
   * Bulk-retrieve completed tables for multiple source keys.
   *
   * <p>Groups source keys by their extracted year and performs a full year-level
   * scan for each year. This avoids the DuckDB limitation where
   * {@code read_parquet([glob1, glob2, ...])} fails with "No files found" if
   * even one glob pattern matches no files — which silently discards valid data
   * for all other patterns in the list.
   *
   * <p>Year-level scanning reads all tracker files for a year partition in one
   * query, caches every source key found, then compacts the files into a single
   * parquet file so future startups read one file instead of thousands.
   */
  @Override public Map<String, Set<String>> bulkGetCompletedTables(
      java.util.Collection<String> sourceKeys, String phase) {
    if (sourceKeys.isEmpty()) {
      return Collections.emptyMap();
    }

    // Return cached results for source keys already in stageCache, only query uncached ones
    Map<String, Set<String>> cachedResults = new HashMap<String, Set<String>>();
    List<String> uncachedKeys = new ArrayList<String>();
    for (String sk : sourceKeys) {
      String cacheKey = sk + "\0" + phase;
      Set<String> cached = stageCache.get(cacheKey);
      if (cached != null) {
        if (!cached.isEmpty()) {
          cachedResults.put(sk, cached);
        }
      } else {
        uncachedKeys.add(sk);
      }
    }
    if (uncachedKeys.isEmpty()) {
      return cachedResults;
    }

    long bulkStart = System.currentTimeMillis();
    LOGGER.info("Bulk loading completed tables for {} source keys (phase={}, {} already cached)",
        uncachedKeys.size(), phase, cachedResults.size());

    // Group uncached keys by extracted year for year-level scanning
    Map<String, List<String>> keysByYear = new HashMap<String, List<String>>();
    for (String sk : uncachedKeys) {
      String year = "_table_complete".equals(sk)
          ? COMPLETION_YEAR : extractYear(sk, System.currentTimeMillis());
      List<String> list = keysByYear.get(year);
      if (list == null) {
        list = new ArrayList<String>();
        keysByYear.put(year, list);
      }
      list.add(sk);
    }

    // Scan each year's tracker data (once per year, cached for subsequent calls)
    Map<String, Set<String>> result = new HashMap<String, Set<String>>();
    for (Map.Entry<String, List<String>> yearEntry : keysByYear.entrySet()) {
      String year = yearEntry.getKey();
      List<String> yearKeys = yearEntry.getValue();
      String scanKey = year + "\0" + phase;

      if (!scannedYears.contains(scanKey)) {
        boolean fullData = scanAndCacheYear(year, phase);
        scannedYears.add(scanKey);
        if (fullData) {
          fullyScannedYears.add(scanKey);
        } else {
          LOGGER.info("Year {} scan incomplete — individual queries will occur on demand", year);
        }
      }

      // Collect results from cache for the requested keys
      for (String sk : yearKeys) {
        String cacheKey = sk + "\0" + phase;
        Set<String> cached = stageCache.get(cacheKey);
        if (cached != null && !cached.isEmpty()) {
          result.put(sk, cached);
        }
      }
    }

    // Cache empty sets for uncached keys that weren't found in the scan.
    // Only safe when the year was fully scanned (compacted or full scan succeeded).
    for (String sk : uncachedKeys) {
      String cacheKey = sk + "\0" + phase;
      if (!stageCache.containsKey(cacheKey)) {
        String year = "_table_complete".equals(sk)
            ? COMPLETION_YEAR : extractYear(sk, System.currentTimeMillis());
        String scanKey = year + "\0" + phase;
        // Only cache empty if year was fully scanned (not just attempted)
        if (fullyScannedYears.contains(scanKey)) {
          stageCache.put(cacheKey, new LinkedHashSet<String>());
        }
      }
    }

    long bulkElapsed = System.currentTimeMillis() - bulkStart;
    LOGGER.info("Bulk loaded completed tables: {} keys queried, {} with data, {}ms",
        uncachedKeys.size(), result.size(), bulkElapsed);
    cachedResults.putAll(result);
    return cachedResults;
  }

  /**
   * Scan and compact tracker data for a range of years.
   *
   * <p>For each year in the range, reads all tracker files (or the existing
   * compacted file), caches the data, and writes a compacted file. This is
   * intended for standalone compaction runs ({@code --compact-only}) to prepare
   * tracker data for fast reads on subsequent ETL runs.
   *
   * <p>Also compacts year=0 (table completion markers).
   */
  public void compactYearRange(int startYear, int endYear) {
    long start = System.currentTimeMillis();
    LOGGER.info("Compacting tracker years {}-{} (plus completion markers)...",
        startYear, endYear);

    // Compact table completion markers (year=0)
    scanAndCacheYear(COMPLETION_YEAR, "table_completion");

    // Compact each data year for both staging and incremental phases
    for (int year = startYear; year <= endYear; year++) {
      scanAndCacheYear(String.valueOf(year), "staging");
      scanAndCacheYear(String.valueOf(year), "incremental");
    }

    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Tracker compaction complete: years {}-{} in {}ms",
        startYear, endYear, elapsed);
  }

  /**
   * Scan tracker data for a year partition and cache results.
   *
   * <p>Two-phase approach:
   * <ol>
   * <li><b>Fast path</b>: Read {@code _compacted/*.parquet} (single file, instant).
   *     Available after a previous run compacted the tracker data.</li>
   * <li><b>Slow path</b>: Read {@code source_key=*&#47;*.parquet} (all individual files).
   *     Only needed on the first run for each year. After scanning, writes a
   *     compacted file from the in-memory cache so future runs use the fast path.</li>
   * </ol>
   *
   * @return true if data was successfully loaded (from compacted or full scan)
   */
  private boolean scanAndCacheYear(String year, String phase) {
    long start = System.currentTimeMillis();

    // Fast path: try compacted file first (O(1) file read)
    String compactedGlob = bucketPath + "/year=" + year + "/_compacted/*.parquet";
    LOGGER.info("Scanning tracker year={} (phase={}) — checking for compacted file...",
        year, phase);
    int[] counts = readTrackerGlob(compactedGlob, phase);
    if (counts != null) {
      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Scanned tracker year={} from compacted file: "
          + "{} source keys, {} completed tables, {}ms",
          year, counts[0], counts[1], elapsed);
      return true;
    }

    // Slow path: full scan of individual tracker files
    // Uses source_key=*/*.parquet (non-recursive) to avoid re-reading _compacted/
    String fullGlob = bucketPath + "/year=" + year + "/source_key=*/*.parquet";
    LOGGER.info("Scanning full tracker year={} (phase={}) — "
        + "first scan, may take several minutes on remote storage...", year, phase);
    counts = readTrackerGlob(fullGlob, phase);
    if (counts != null) {
      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Scanned tracker year={}: {} source keys, {} completed tables, {}ms",
          year, counts[0], counts[1], elapsed);
      compactFromCache(year);
      return true;
    }

    // Both compacted and full scan returned null — either the year is truly empty
    // or the scan failed (timeout/error). Return true for "no files found" (safe to
    // cache empty sets) but the logging from readTrackerGlob will distinguish errors.
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("No tracker data found for year={} ({}ms)", year, elapsed);
    return true;
  }

  /**
   * Read tracker data from a glob pattern and populate the stageCache.
   *
   * @return int[]{sourceKeyCount, tableCount} on success, null on "no files found"
   */
  private int[] readTrackerGlob(String glob, String phase) {
    String sql = "SELECT source_key, table_name FROM ("
        + "  SELECT source_key, table_name, state, "
        + "    ROW_NUMBER() OVER (PARTITION BY source_key, table_name ORDER BY as_of DESC) AS rn"
        + "  FROM read_parquet('" + glob + "', "
        + "hive_partitioning=false, union_by_name=true)"
        + "  WHERE phase = ?"
        + ") WHERE rn = 1 AND state = 'complete'";

    int sourceKeyCount = 0;
    int tableCount = 0;
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, phase);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String sourceKey = rs.getString("source_key");
          String tableName = rs.getString("table_name");
          String cacheKey = sourceKey + "\0" + phase;
          Set<String> tables = stageCache.get(cacheKey);
          if (tables == null) {
            tables = new LinkedHashSet<String>();
            stageCache.put(cacheKey, tables);
            sourceKeyCount++;
          }
          tables.add(tableName);
          tableCount++;
        }
      }
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        return null;
      }
      LOGGER.warn("Failed to read tracker from {}: {}", glob, msg);
      return null;
    }
    return new int[]{sourceKeyCount, tableCount};
  }

  /**
   * Write a compacted tracker file from the in-memory stageCache.
   *
   * <p>Collects all cached (source_key, table_name) pairs for the given year,
   * writes them to a single parquet file in {@code _compacted/}. This avoids
   * re-reading thousands of small files from S3 — the data is already in memory.
   *
   * <p>Future {@link #scanAndCacheYear} calls read this single file instead of
   * scanning all individual tracker files.
   */
  private void compactFromCache(String year) {
    long start = System.currentTimeMillis();
    long asOf = System.currentTimeMillis();
    String compactedPath = bucketPath + "/year=" + year
        + "/_compacted/" + UUID.randomUUID().toString() + ".parquet";

    // Collect all cached entries for this year
    List<String[]> rows = new ArrayList<String[]>();
    for (Map.Entry<String, Set<String>> entry : stageCache.entrySet()) {
      String key = entry.getKey();
      int sep = key.indexOf('\0');
      if (sep < 0) {
        continue;
      }
      String sourceKey = key.substring(0, sep);
      String phase = key.substring(sep + 1);
      String skYear = "_table_complete".equals(sourceKey)
          ? COMPLETION_YEAR : extractYear(sourceKey, asOf);
      if (!year.equals(skYear)) {
        continue;
      }
      for (String tableName : entry.getValue()) {
        rows.add(new String[]{sourceKey, tableName, phase});
      }
    }

    if (rows.isEmpty()) {
      LOGGER.info("No cached tracker data to compact for year={}", year);
      return;
    }

    // Write via DuckDB temp table (handles large row counts efficiently)
    String tableName = "_compact_" + year.replace("-", "_");
    try {
      Connection conn = getConnection();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
        stmt.execute("CREATE TABLE " + tableName + " ("
            + "source_key VARCHAR, table_name VARCHAR, phase VARCHAR, "
            + "state VARCHAR, row_count BIGINT, config_hash VARCHAR, "
            + "signature VARCHAR, error_message VARCHAR, as_of BIGINT)");
      }

      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO " + tableName
              + " VALUES (?, ?, ?, 'complete', 0, NULL, NULL, NULL, ?)")) {
        for (String[] row : rows) {
          ps.setString(1, row[0]);
          ps.setString(2, row[1]);
          ps.setString(3, row[2]);
          ps.setLong(4, asOf);
          ps.addBatch();
        }
        ps.executeBatch();
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("COPY " + tableName
            + " TO '" + compactedPath + "' (FORMAT PARQUET)");
        stmt.execute("DROP TABLE " + tableName);
      }

      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Compacted tracker year={}: {} rows written to S3 in {}ms",
          year, rows.size(), elapsed);
    } catch (SQLException e) {
      LOGGER.warn("Failed to compact tracker year={}: {}", year, e.getMessage());
      try (Statement stmt = getConnection().createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
      } catch (SQLException e2) {
        // ignore cleanup failure
      }
    }
  }

  /**
   * Write a state row to S3 as an append-only parquet file.
   */
  private void writeState(String sourceKey, String tableName, String phase,
      String state, long rowCount, String configHash, String signature,
      String errorMessage) {
    long asOf = System.currentTimeMillis();
    String year = "_table_complete".equals(sourceKey)
        ? COMPLETION_YEAR : extractYear(sourceKey, asOf);
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
    String year = "_table_complete".equals(sourceKey)
        ? COMPLETION_YEAR : extractYear(sourceKey, System.currentTimeMillis());
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
    // Check stage cache first (populated by bulkGetCompletedTables / getCompletedTables)
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> cached = stageCache.get(cacheKey);
    if (cached != null) {
      return cached.contains(tableName);
    }
    // Cache miss — use getCompletedTables to populate full set (avoids separate S3 query)
    Set<String> tables = getCompletedTables(sourceKey, phase);
    return tables.contains(tableName);
  }

  @Override public void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    writeState(sourceKey, tableName, phase, "complete", rowCount, null, null, null);
    // Update stage cache
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> tables = stageCache.get(cacheKey);
    if (tables != null) {
      tables.add(tableName);
    } else {
      Set<String> newSet = Collections.newSetFromMap(
          new ConcurrentHashMap<String, Boolean>());
      newSet.add(tableName);
      stageCache.put(cacheKey, newSet);
    }
  }

  @Override public void markError(String sourceKey, String tableName, String phase,
      String error) {
    writeState(sourceKey, tableName, phase, "error", 0, null, null, error);
  }

  @Override public void markCleared(String sourceKey, String tableName, String phase) {
    writeState(sourceKey, tableName, phase, "cleared", 0, null, null, null);
    // Update stage cache — remove the cleared table
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> tables = stageCache.get(cacheKey);
    if (tables != null) {
      tables.remove(tableName);
    }
  }

  @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
    // Check stage cache first
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> cached = stageCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }

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
    // Cache the result (even if empty — prevents repeated S3 queries for missing data)
    stageCache.put(cacheKey, tables);
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
    // Use in-memory cache first, then fall back to getCachedCompletion
    CachedCompletion cached = completionCache.get(pipelineName);
    if (cached == null) {
      cached = getCachedCompletion(pipelineName);
    }
    if (cached == null) {
      return false;
    }
    return dimensionSignature.equals(cached.signature);
  }

  @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        0, null, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(null, dimensionSignature, 0, System.currentTimeMillis(), 0));
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configHash, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(configHash, dimensionSignature, rowCount,
            System.currentTimeMillis(), 0));
  }

  @Override public void markTableCompleteWithSourceWatermark(String pipelineName,
      String configHash, String dimensionSignature, long rowCount,
      long sourceFileWatermark) {
    // Store watermark in config_hash field for simplicity
    String configWithWatermark = configHash + ":wm=" + sourceFileWatermark;
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configWithWatermark, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(configHash, dimensionSignature, rowCount,
            System.currentTimeMillis(), sourceFileWatermark));
  }

  @Override public CachedCompletion getCachedCompletion(String pipelineName) {
    // Check in-memory cache first
    CachedCompletion memoryCached = completionCache.get(pipelineName);
    if (memoryCached != null) {
      return memoryCached;
    }

    long queryStart = System.currentTimeMillis();
    String glob = bucketPath + "/year=" + COMPLETION_YEAR
        + "/source_key=_table_complete/*.parquet";
    String sql = "SELECT config_hash, signature, row_count, as_of "
        + "FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "WHERE table_name = ? AND phase = 'table_completion' AND state = 'complete' "
        + "ORDER BY as_of DESC LIMIT 1";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        long queryElapsed = System.currentTimeMillis() - queryStart;
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
          CachedCompletion result =
              new CachedCompletion(configHash, signature, rowCount, completedAt, watermark);
          completionCache.put(pipelineName, result);
          LOGGER.info("getCachedCompletion({}) hit S3 in {}ms — found ({} rows)",
              pipelineName, queryElapsed, rowCount);
          return result;
        } else {
          LOGGER.info("getCachedCompletion({}) hit S3 in {}ms — not found",
              pipelineName, queryElapsed);
        }
      }
    } catch (SQLException e) {
      long queryElapsed = System.currentTimeMillis() - queryStart;
      LOGGER.info("getCachedCompletion({}) S3 query failed after {}ms: {}",
          pipelineName, queryElapsed, e.getMessage());
    }
    return null;
  }

  @Override public void preloadAllCompletions() {
    long start = System.currentTimeMillis();
    LOGGER.info("Preloading all table completion markers from S3 (year={})...", COMPLETION_YEAR);
    String glob = bucketPath + "/year=" + COMPLETION_YEAR
        + "/source_key=_table_complete/*.parquet";
    String sql = "SELECT table_name, config_hash, signature, row_count, as_of "
        + "FROM ("
        + "  SELECT table_name, config_hash, signature, row_count, as_of, "
        + "    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY as_of DESC) AS rn"
        + "  FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true)"
        + "  WHERE phase = 'table_completion' AND state = 'complete'"
        + ") WHERE rn = 1";
    int count = 0;
    try (Statement stmt = getConnection().createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String tableName = rs.getString("table_name");
        String configHash = rs.getString("config_hash");
        String signature = rs.getString("signature");
        long rowCount = rs.getLong("row_count");
        long completedAt = rs.getLong("as_of");

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
        completionCache.put(tableName,
            new CachedCompletion(configHash, signature, rowCount, completedAt, watermark));
        count++;
      }
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        LOGGER.info("No table completion markers found ({}ms)", System.currentTimeMillis() - start);
        return;
      }
      LOGGER.warn("Failed to preload table completions: {}", msg);
      return;
    }
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Preloaded {} table completion markers in {}ms", count, elapsed);
  }

  @Override public void invalidateTableCompletion(String pipelineName) {
    completionCache.remove(pipelineName);
    writeState("_table_complete", pipelineName, "table_completion", "cleared",
        0, null, null, null);
  }

  @Override public void clearAllCompletions() {
    LOGGER.warn("clearAllCompletions on S3 tracker writes 'cleared' markers. "
        + "Old parquet files remain but are superseded by the cleared state.");
    completionCache.clear();
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
        completionCache.clear();
        stageCache.clear();
        scannedYears.clear();
        fullyScannedYears.clear();
      }
    }
  }
}
