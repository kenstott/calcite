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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.govdata.AbstractCacheManifest;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cache manifest for tracking downloaded geographic data to improve startup performance.
 * Maintains metadata about cached files to avoid redundant downloads and S3 existence checks.
 *
 * <p>Uses DuckDB for persistent storage, providing proper ACID guarantees and
 * thread-safe concurrent access. Replaces the previous JSON-based implementation.
 *
 * <p>Extends {@link AbstractCacheManifest} to benefit from common caching infrastructure
 * including ETag support, TTL-based expiration, and materialization tracking.
 */
public class GeoCacheManifest extends AbstractCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoCacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LEGACY_MANIFEST_FILENAME = "cache_manifest.json";

  /** DuckDB-based cache store. */
  private final DuckDBCacheStore store;

  /** Cache directory path. */
  private final String cacheDir;

  /**
   * Private constructor - use {@link #load(String)} to get an instance.
   */
  private GeoCacheManifest(String cacheDir) {
    this.cacheDir = cacheDir;
    this.store = DuckDBCacheStore.getInstance("geo", cacheDir);
  }

  /**
   * Check if data is cached and fresh for the given cache key.
   * Uses ETag-based caching when available, falling back to time-based TTL.
   */
  @Override public boolean isCached(CacheKey cacheKey) {
    return store.isCached(cacheKey.asString());
  }

  /**
   * Check if data is cached and fresh for the given parameters.
   * Uses ETag-based caching when available, falling back to time-based TTL.
   * @deprecated Use {@link #isCached(CacheKey)} instead
   */
  @Deprecated
  public boolean isCached(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    return store.isCached(key);
  }

  /**
   * Get the ETag for a cached entry.
   *
   * @param dataType The type of data
   * @param year The year
   * @param parameters Additional parameters
   * @return The ETag string, or null if not cached or no ETag available
   */
  public String getETag(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    return store.getETag(key);
  }

  /**
   * Mark data as cached with metadata and explicit refresh timestamp.
   *
   * @param cacheKey The cache key identifying the data
   * @param filePath Path to the cached file
   * @param fileSize Size of the cached file
   * @param refreshAfter Timestamp (millis since epoch) when this entry should be refreshed
   * @param refreshReason Human-readable reason for the refresh policy
   */
  @Override public void markCached(CacheKey cacheKey, String filePath, long fileSize,
      long refreshAfter, String refreshReason) {
    String parametersJson = null;
    try {
      parametersJson = MAPPER.writeValueAsString(cacheKey.getParameters());
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize parameters for {}: {}", cacheKey.asString(), e.getMessage());
    }

    store.upsertEntry(
        cacheKey.asString(),
        cacheKey.getTableName(),
        parametersJson,
        filePath,
        fileSize,
        refreshAfter,
        refreshReason);

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - System.currentTimeMillis());
    LOGGER.debug("Marked as cached: {} (size={}, refresh in {} hours, policy: {})",
        cacheKey.asString(), fileSize, hoursUntilRefresh, refreshReason);
  }

  /**
   * Mark data as cached with metadata and explicit refresh timestamp.
   * @deprecated Use {@link #markCached(CacheKey, String, long, long, String)} instead
   */
  @Deprecated
  public void markCached(String dataType, int year, Map<String, String> parameters,
                        String filePath, long fileSize, long refreshAfter, String refreshReason) {
    String key = buildKey(dataType, year, parameters);
    String parametersJson = null;
    try {
      Map<String, String> allParams = new HashMap<>();
      if (parameters != null) {
        allParams.putAll(parameters);
      }
      allParams.put("year", String.valueOf(year));
      parametersJson = MAPPER.writeValueAsString(allParams);
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize parameters: {}", e.getMessage());
    }

    store.upsertEntry(key, dataType, parametersJson, filePath, fileSize, refreshAfter, refreshReason);

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - System.currentTimeMillis());
    LOGGER.debug("Marked as cached: {} (year={}, size={}, refresh in {} hours, policy: {})",
        dataType, year, fileSize, hoursUntilRefresh, refreshReason);
  }

  /**
   * Mark data as cached with default refresh for TIGER boundaries (immutable).
   */
  public void markCached(String dataType, int year, Map<String, String> parameters,
                        String filePath, long fileSize) {
    long refreshAfter = Long.MAX_VALUE;
    String refreshReason = "geographic_boundary_immutable";
    markCached(dataType, year, parameters, filePath, fileSize, refreshAfter, refreshReason);
  }

  /**
   * Check if materialization is complete for the given data.
   */
  @Override public boolean isMaterialized(CacheKey cacheKey) {
    boolean converted = store.isMaterialized(cacheKey.asString());
    // Use TRACE for per-item logging to avoid flooding DEBUG logs in high-cardinality loops
    if (converted && LOGGER.isTraceEnabled()) {
      LOGGER.trace("Parquet already converted for {}", cacheKey.asString());
    }
    return converted;
  }

  /**
   * Check if materialization is complete for the given data.
   * @deprecated Use {@link #isMaterialized(CacheKey)} instead
   */
  @Deprecated
  public boolean isMaterialized(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    return store.isMaterialized(key);
  }

  /**
   * Mark materialization as complete for cached data.
   */
  @Override public void markMaterialized(CacheKey cacheKey, String outputPath) {
    store.markMaterialized(cacheKey.asString(), outputPath);
    LOGGER.debug("Marked parquet converted: {} -> {}", cacheKey.asString(), outputPath);
  }

  /**
   * Mark materialization as complete for cached data.
   * @deprecated Use {@link #markMaterialized(CacheKey, String)} instead
   */
  @Deprecated
  public void markMaterialized(String dataType, int year, Map<String, String> parameters,
      String outputPath) {
    String key = buildKey(dataType, year, parameters);
    store.markMaterialized(key, outputPath);
    LOGGER.debug("Marked parquet converted: {} year={} -> {}", dataType, year, outputPath);
  }

  /**
   * Check if parquet is converted with self-healing fallback.
   * First checks manifest, then falls back to file existence check.
   * If file exists but not in manifest, updates manifest automatically.
   *
   * @param cacheKey Cache key to check
   * @param outputPath Path to output file
   * @param rawFilePath Path to raw source file (for timestamp comparison), or null
   * @param fileChecker Function to check file existence and get modification time
   * @return true if parquet is converted (either in manifest or self-healed)
   */
  public boolean isMaterializedWithSelfHealing(CacheKey cacheKey, String outputPath,
      String rawFilePath, DuckDBCacheStore.FileChecker fileChecker) {
    return store.isMaterializedWithSelfHealing(
        cacheKey.asString(), outputPath, rawFilePath, fileChecker);
  }

  /**
   * Mark data as having API error with configurable retry cadence.
   * Prevents repeated failed requests while allowing automatic retry once TTL expires.
   *
   * @param cacheKey The cache key identifying the data
   * @param errorMessage Full error message from API
   * @param retryAfterDays Number of days before retrying
   */
  @Override public void markApiError(CacheKey cacheKey, String errorMessage, int retryAfterDays) {
    store.markApiError(cacheKey.asString(), cacheKey.getTableName(), errorMessage, retryAfterDays);
    String truncatedMsg = errorMessage.length() > 100
        ? errorMessage.substring(0, 100) + "..."
        : errorMessage;
    LOGGER.info("Marked {} as API error (retry in {} days): {}",
        cacheKey.asString(), retryAfterDays, truncatedMsg);
  }

  /**
   * Mark data as unavailable (404 or similar) with TTL for retry.
   * Prevents repeated failed requests while allowing automatic retry once TTL expires.
   *
   * @param cacheKey The cache key identifying the data
   * @param retryAfterDays Number of days before retrying
   * @param reason Description of why unavailable
   */
  public void markUnavailable(CacheKey cacheKey, int retryAfterDays, String reason) {
    long refreshAfter = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(retryAfterDays);
    store.upsertEntry(
        cacheKey.asString(),
        cacheKey.getTableName(),
        null,  // No parameters needed for unavailable entries
        null,  // No file path
        0,     // No file size
        refreshAfter,
        reason);
    LOGGER.info("Marked {} as unavailable (retry in {} days): {}",
        cacheKey.asString(), retryAfterDays, reason);
  }

  /**
   * Check if data is marked as unavailable and still within retry window.
   *
   * @param cacheKey The cache key to check
   * @return true if data is unavailable and should not be retried yet
   */
  public boolean isUnavailable(CacheKey cacheKey) {
    return store.isUnavailable(cacheKey.asString());
  }

  /**
   * Remove expired entries from the manifest based on refreshAfter timestamps.
   */
  public int cleanupExpiredEntries() {
    return store.cleanupExpiredEntries();
  }

  /**
   * Load manifest from DuckDB cache store.
   * Automatically migrates from JSON if a legacy manifest exists.
   */
  public static GeoCacheManifest load(String cacheDir) {
    GeoCacheManifest manifest = new GeoCacheManifest(cacheDir);

    // Check for legacy JSON manifest and migrate if present
    File legacyFile = new File(cacheDir, LEGACY_MANIFEST_FILENAME);
    if (legacyFile.exists()) {
      manifest.migrateFromJson(legacyFile);
    }

    int[] stats = manifest.store.getStats();
    LOGGER.info("Loaded GEO cache manifest from DuckDB with {} entries ({} fresh, {} expired)",
        stats[0], stats[1], stats[2]);

    return manifest;
  }

  /**
   * Migrate data from legacy JSON manifest to DuckDB.
   */
  private void migrateFromJson(File legacyFile) {
    LOGGER.info("Found legacy JSON manifest at {}, migrating to DuckDB...", legacyFile.getAbsolutePath());

    try {
      LegacyManifest legacy = MAPPER.readValue(legacyFile, LegacyManifest.class);

      int migratedEntries = 0;
      if (legacy.entries != null) {
        for (java.util.Map.Entry<String, LegacyCacheEntry> entry : legacy.entries.entrySet()) {
          String cacheKey = entry.getKey();
          LegacyCacheEntry cacheEntry = entry.getValue();

          String parametersJson = null;
          if (cacheEntry.parameters != null) {
            parametersJson = MAPPER.writeValueAsString(cacheEntry.parameters);
          }

          store.upsertEntry(
              cacheKey,
              cacheEntry.dataType != null ? cacheEntry.dataType : "unknown",
              parametersJson,
              cacheEntry.filePath,
              cacheEntry.fileSize,
              cacheEntry.refreshAfter,
              cacheEntry.refreshReason);

          if (cacheEntry.materializedAt > 0) {
            store.markMaterialized(cacheKey, cacheEntry.outputPath);
          }

          migratedEntries++;
        }
      }

      LOGGER.info("Migrated {} cache entries from JSON to DuckDB", migratedEntries);

      File backupFile = new File(legacyFile.getParent(), LEGACY_MANIFEST_FILENAME + ".migrated");
      if (legacyFile.renameTo(backupFile)) {
        LOGGER.info("Renamed legacy manifest to {}", backupFile.getName());
      }

    } catch (IOException e) {
      LOGGER.error("Failed to migrate legacy JSON manifest: {}", e.getMessage());
    }
  }

  /**
   * Save manifest to DuckDB.
   * This is now a no-op as DuckDB persists changes immediately.
   */
  @Override public void save(String directory) {
    store.touchLastUpdated();
    LOGGER.debug("Cache manifest changes persisted to DuckDB");
  }

  /**
   * Build cache key from parameters.
   */
  private String buildKey(String dataType, int year, Map<String, String> parameters) {
    StringBuilder key = new StringBuilder();
    key.append(dataType).append(":").append(year);

    if (parameters != null && !parameters.isEmpty()) {
      parameters.entrySet().stream()
          .sorted(java.util.Map.Entry.comparingByKey())
          .forEach(entry -> key.append(":").append(entry.getKey()).append("=").append(entry.getValue()));
    }

    return key.toString();
  }

  // ===== Legacy JSON classes for migration =====

  private static class LegacyManifest {
    public java.util.Map<String, LegacyCacheEntry> entries;
    public String version;
    public long lastUpdated;
  }

  private static class LegacyCacheEntry {
    public String dataType;
    public int year;
    public java.util.Map<String, String> parameters;
    public String filePath;
    public long fileSize;
    public long cachedAt;
    public long refreshAfter;
    public String refreshReason;
    public String etag;
    public String outputPath;
    public long materializedAt;
  }
}
