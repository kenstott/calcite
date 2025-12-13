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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.AbstractCacheManifest;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Cache manifest for tracking downloaded economic data to improve startup performance.
 * Maintains metadata about cached files to avoid redundant downloads.
 *
 * <p>Uses DuckDB for persistent storage, providing proper ACID guarantees and
 * thread-safe concurrent access. Replaces the previous JSON-based implementation.
 *
 * <p>Extends {@link AbstractCacheManifest} to benefit from common caching infrastructure
 * including ETag support, TTL-based expiration, and parquet conversion tracking.
 */
public class CacheManifest extends AbstractCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LEGACY_MANIFEST_FILENAME = "cache_manifest.json";

  /** DuckDB-based cache store. */
  private final DuckDBCacheStore store;

  /** Cache directory path. */
  private final String cacheDir;

  /**
   * Get the underlying DuckDB cache store for direct access.
   * Used for year availability caching and other advanced operations.
   *
   * @return The DuckDB cache store
   */
  public DuckDBCacheStore getStore() {
    return store;
  }

  /**
   * Private constructor - use {@link #load(String)} to get an instance.
   */
  private CacheManifest(String cacheDir) {
    this.cacheDir = cacheDir;
    this.store = DuckDBCacheStore.getInstance("econ", cacheDir);
  }

  /**
   * Check if data is cached and fresh for the given cache key.
   * Uses ETag-based caching when available, falling back to time-based TTL.
   * Also handles API error retry cadence to prevent expensive retries.
   *
   * @param cacheKey The cache key identifying the data
   * @return true if cached and fresh, false otherwise
   */
  @Override
  public boolean isCached(CacheKey cacheKey) {
    return store.isCached(cacheKey.asString());
  }

  /**
   * Mark data as cached with metadata and explicit refresh timestamp.
   *
   * @param cacheKey The cache key identifying the data
   * @param filePath Path to the cached file
   * @param fileSize Size of the cached file
   * @param refreshAfter Timestamp (millis since epoch) when this entry should be refreshed
   * @param refreshReason Human-readable reason for the refresh policy (e.g., "daily", "immutable")
   */
  @Override
  public void markCached(CacheKey cacheKey,
                        String filePath, long fileSize, long refreshAfter, String refreshReason) {
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
        refreshReason
    );

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - System.currentTimeMillis());
    LOGGER.debug("Marked as cached: {} (size={}, refresh in {} hours, policy: {})",
        cacheKey.asString(), fileSize, hoursUntilRefresh, refreshReason);
  }

  /**
   * Check if parquet file has been converted for the given cache key.
   * This avoids expensive S3 exists checks on every run by tracking conversions in the manifest.
   *
   * @param cacheKey The cache key identifying the data
   * @return true if parquet file exists and is up-to-date, false otherwise
   */
  @Override
  public boolean isParquetConverted(CacheKey cacheKey) {
    boolean converted = store.isParquetConverted(cacheKey.asString());
    if (converted) {
      LOGGER.debug("Cached parquet, skipped conversion: {}", cacheKey.asString());
    }
    return converted;
  }

  /**
   * Mark parquet file as converted for the given cache key.
   * This is called after successful parquet conversion to avoid redundant conversions.
   *
   * @param cacheKey The cache key identifying the data
   * @param parquetPath Path to the converted parquet file
   */
  @Override
  public void markParquetConverted(CacheKey cacheKey, String parquetPath) {
    store.markParquetConverted(cacheKey.asString(), parquetPath);
    LOGGER.debug("Marked parquet as converted: {} (path={})", cacheKey.asString(), parquetPath);
  }

  /**
   * Mark data as unavailable (404 or similar) with TTL for retry.
   * Prevents repeated failed requests while allowing automatic retry once TTL expires.
   *
   * @param cacheKey The cache key identifying the data
   * @param retryAfterDays Number of days before retrying (default: 7 for unreleased data)
   * @param reason Description of why unavailable (e.g., "404_not_released", "400_invalid_variables")
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
        reason
    );
    LOGGER.info("Marked {} as unavailable (retry in {} days): {}",
        cacheKey.asString(), retryAfterDays, reason);
  }

  /**
   * Mark data as having API error (HTTP 200 with error content) with configurable retry cadence.
   *
   * @param cacheKey The cache key identifying the data
   * @param errorMessage Full error message from API (e.g., JSON error object)
   * @param retryAfterDays Number of days before retrying (default: 7 for weekly retry)
   */
  @Override
  public void markApiError(CacheKey cacheKey, String errorMessage, int retryAfterDays) {
    store.markApiError(cacheKey.asString(), cacheKey.getTableName(), errorMessage, retryAfterDays);
    String truncatedMsg = errorMessage.length() > 100
        ? errorMessage.substring(0, 100) + "..."
        : errorMessage;
    LOGGER.info("Marked {} as API error (retry in {} days): {}",
        cacheKey.asString(), retryAfterDays, truncatedMsg);
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
   *
   * @param cacheDir Cache directory path
   * @return CacheManifest instance backed by DuckDB
   */
  public static CacheManifest load(String cacheDir) {
    CacheManifest manifest = new CacheManifest(cacheDir);

    // Check for legacy JSON manifest and migrate if present
    File legacyFile = new File(cacheDir, LEGACY_MANIFEST_FILENAME);
    if (legacyFile.exists()) {
      manifest.migrateFromJson(legacyFile);
    }

    int[] stats = manifest.store.getStats();
    LOGGER.info("Loaded ECON cache manifest from DuckDB with {} entries ({} fresh, {} expired)",
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

      // Migrate cache entries
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
              cacheEntry.refreshReason
          );

          // Migrate parquet conversion status
          if (cacheEntry.parquetConvertedAt > 0) {
            store.markParquetConverted(cacheKey, cacheEntry.parquetPath);
          }

          migratedEntries++;
        }
      }

      // Migrate catalog series cache
      int migratedSeries = 0;
      if (legacy.catalogSeriesCache != null) {
        for (java.util.Map.Entry<String, LegacyCatalogSeriesCache> entry : legacy.catalogSeriesCache.entrySet()) {
          LegacyCatalogSeriesCache seriesCache = entry.getValue();
          if (seriesCache.seriesIds != null && !seriesCache.seriesIds.isEmpty()) {
            String seriesIdsStr = String.join(",", seriesCache.seriesIds);
            int ttlDays = (int) TimeUnit.MILLISECONDS.toDays(
                seriesCache.refreshAfter - seriesCache.cachedAt);
            store.cacheCatalogSeries(seriesCache.minPopularity, seriesIdsStr, ttlDays);
            migratedSeries++;
          }
        }
      }

      LOGGER.info("Migrated {} cache entries and {} catalog series caches from JSON to DuckDB",
          migratedEntries, migratedSeries);

      // Rename legacy file to indicate migration completed
      File backupFile = new File(legacyFile.getParent(), LEGACY_MANIFEST_FILENAME + ".migrated");
      if (legacyFile.renameTo(backupFile)) {
        LOGGER.info("Renamed legacy manifest to {}", backupFile.getName());
      } else {
        LOGGER.warn("Failed to rename legacy manifest file");
      }

    } catch (IOException e) {
      LOGGER.error("Failed to migrate legacy JSON manifest: {}", e.getMessage());
      // Don't delete the file if migration failed - leave it for manual inspection
    }
  }

  /**
   * Save manifest to DuckDB.
   * This is now a no-op as DuckDB persists changes immediately.
   * Kept for API compatibility.
   */
  @Override
  public void save(String directory) {
    // DuckDB persists changes immediately, no explicit save needed
    store.touchLastUpdated();
    LOGGER.debug("Cache manifest changes persisted to DuckDB");
  }

  /**
   * Get cache statistics.
   */
  public CacheStats getStats() {
    int[] stats = store.getStats();
    CacheStats cacheStats = new CacheStats();
    cacheStats.totalEntries = stats[0];
    cacheStats.freshEntries = stats[1];
    cacheStats.expiredEntries = stats[2];
    return cacheStats;
  }

  /**
   * Get the ETag for a cached entry.
   *
   * @param cacheKey The cache key identifying the data
   * @return The ETag string, or null if not cached or no ETag available
   */
  public String getETag(CacheKey cacheKey) {
    return store.getETag(cacheKey.asString());
  }

  // ===== FRED Catalog Series Cache Methods =====

  /**
   * Get cached FRED catalog series list filtered by popularity threshold.
   * Returns null if not cached or cache has expired.
   *
   * @param minPopularity Minimum popularity threshold used for filtering
   * @return List of series IDs, or null if not cached/expired
   */
  public List<String> getCachedCatalogSeries(int minPopularity) {
    String seriesIdsStr = store.getCachedCatalogSeries(minPopularity);
    if (seriesIdsStr == null || seriesIdsStr.isEmpty()) {
      return null;
    }
    return new ArrayList<>(Arrays.asList(seriesIdsStr.split(",")));
  }

  /**
   * Cache FRED catalog series extraction results with TTL.
   *
   * @param minPopularity Popularity threshold used for filtering
   * @param seriesIds List of series IDs that met the threshold
   * @param ttlDays Time-to-live in days (typically 365 for annual refresh)
   */
  public void cacheCatalogSeries(int minPopularity, List<String> seriesIds, int ttlDays) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.debug("Skipping cache of empty series list for popularity={}", minPopularity);
      return;
    }
    String seriesIdsStr = String.join(",", seriesIds);
    store.cacheCatalogSeries(minPopularity, seriesIdsStr, ttlDays);
    LOGGER.info("Cached {} catalog series (threshold: {}, TTL: {} days)",
        seriesIds.size(), minPopularity, ttlDays);
  }

  /**
   * Check if catalog series cache exists and is valid for the given threshold.
   *
   * @param minPopularity Popularity threshold
   * @return true if cache exists and has not expired
   */
  public boolean isCatalogSeriesCached(int minPopularity) {
    String cached = store.getCachedCatalogSeries(minPopularity);
    return cached != null && !cached.isEmpty();
  }

  /**
   * Invalidate cached catalog series for a specific popularity threshold.
   * Forces re-extraction on next access.
   *
   * @param minPopularity Popularity threshold to invalidate
   */
  public void invalidateCatalogSeriesCache(int minPopularity) {
    store.deleteCatalogSeriesCache(minPopularity);
    LOGGER.info("Invalidated catalog series cache (threshold: {})", minPopularity);
  }

  /**
   * Invalidate all cached catalog series for all popularity thresholds.
   * Forces re-extraction for all thresholds on next access.
   */
  public void invalidateAllCatalogSeriesCache() {
    store.deleteAllCatalogSeriesCache();
    LOGGER.info("Invalidated all catalog series caches");
  }

  // ===== Table-Level Completion Tracking =====

  /**
   * Check if a table iteration was fully completed with the given dimension signature.
   * This allows skipping the entire dimension iteration loop for subsequent runs
   * when nothing has changed.
   *
   * @param tableName The table name (e.g., "national_accounts", "regional_income")
   * @param dimensionSignature Hash of dimension names and value counts
   * @return true if table was fully cached with same dimension signature
   */
  public boolean isTableComplete(String tableName, String dimensionSignature) {
    return store.isTableComplete(tableName, dimensionSignature);
  }

  /**
   * Mark a table as complete after all dimension combinations were processed.
   *
   * @param tableName The table name
   * @param dimensionSignature Hash of dimension names and value counts
   */
  public void markTableComplete(String tableName, String dimensionSignature) {
    store.markTableComplete(tableName, dimensionSignature);
  }

  /**
   * Invalidate table completion when cache entries change.
   *
   * @param tableName The table name to invalidate
   */
  public void invalidateTableCompletion(String tableName) {
    store.invalidateTableCompletion(tableName);
  }

  /**
   * Check if all entries for a table are fresh (not expired).
   *
   * @param tableName The table name prefix to check
   * @return true if all entries for this table have refresh_after > now
   */
  public boolean areAllEntriesFresh(String tableName) {
    return store.areAllEntriesFresh(tableName);
  }

  /**
   * Check if all entries for a table have been converted to parquet.
   * This is faster than areAllEntriesFresh for conversion operations -
   * if parquet exists, we don't need to re-convert regardless of refresh_after.
   *
   * @param tableName The table name prefix to check
   * @return true if all entries for this table have parquet_converted_at > 0
   */
  public boolean areAllParquetConverted(String tableName) {
    return store.areAllParquetConverted(tableName);
  }

  /**
   * Fast database-level check to determine if a table can be skipped entirely.
   * This is the primary method for meta-level caching - it uses a single efficient
   * database query instead of iterating through all combinations.
   *
   * <p>Returns true if:
   * <ul>
   *   <li>We have at least expectedCount entries for this table</li>
   *   <li>All entries have been converted to parquet</li>
   *   <li>No entries need raw file refresh (raw file newer than parquet)</li>
   * </ul>
   *
   * @param tableName The table name prefix
   * @param expectedCount The expected number of dimension combinations
   * @return true if table can be skipped entirely
   */
  public boolean isTableFullyCached(String tableName, int expectedCount) {
    return store.isTableFullyCached(tableName, expectedCount);
  }

  /**
   * Check if any entries for a table have errors.
   *
   * @param tableName The table name prefix to check
   * @return true if any entries have error_count > 0
   */
  public boolean hasTableErrors(String tableName) {
    return store.hasTableErrors(tableName);
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int totalEntries;
    public int freshEntries;
    public int expiredEntries;

    @Override public String toString() {
      return String.format("Cache stats: %d total, %d fresh, %d expired",
                          totalEntries, freshEntries, expiredEntries);
    }
  }

  // ===== Legacy JSON classes for migration =====

  /** Legacy JSON manifest structure for migration. */
  private static class LegacyManifest {
    public java.util.Map<String, LegacyCacheEntry> entries;
    public java.util.Map<String, LegacyCatalogSeriesCache> catalogSeriesCache;
    public String version;
    public long lastUpdated;
  }

  /** Legacy cache entry for migration. */
  private static class LegacyCacheEntry {
    public String dataType;
    public java.util.Map<String, String> parameters;
    public String filePath;
    public long fileSize;
    public long cachedAt;
    public long refreshAfter;
    public String refreshReason;
    public String etag;
    public String parquetPath;
    public long parquetConvertedAt;
    public String lastError;
    public int errorCount;
    public long lastAttemptAt;
    public long downloadRetry;
  }

  /** Legacy catalog series cache for migration. */
  private static class LegacyCatalogSeriesCache {
    public int minPopularity;
    public List<String> seriesIds;
    public long cachedAt;
    public long refreshAfter;
    public String refreshReason;
  }
}
