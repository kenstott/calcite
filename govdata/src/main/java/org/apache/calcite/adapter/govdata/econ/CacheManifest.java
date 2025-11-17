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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cache manifest for tracking downloaded economic data to improve startup performance.
 * Maintains metadata about cached files to avoid redundant downloads.
 *
 * <p>Extends {@link AbstractCacheManifest} to benefit from common caching infrastructure
 * including ETag support, TTL-based expiration, and parquet conversion tracking.
 *
 * <p>Uses explicit refresh timestamps stored in each entry, allowing different refresh
 * policies per data type (e.g., current year vs historical, daily vs immutable).
 */
public class CacheManifest extends AbstractCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MANIFEST_FILENAME = "cache_manifest.json";

  @JsonProperty("entries")
  private Map<String, CacheEntry> entries = new HashMap<>();

  @JsonProperty("catalogSeriesCache")
  private Map<String, CatalogSeriesCache> catalogSeriesCache = new HashMap<>();

  @JsonProperty("version")
  private String version = "2.0";  // Bumped for refreshAfter field addition

  @JsonProperty("lastUpdated")
  private long lastUpdated = System.currentTimeMillis();

  @JsonIgnore
  private String cacheDir;  // Cache directory for resolving relative paths

  /**
   * Check if data is cached and fresh for the given cache key.
   * Uses ETag-based caching when available, falling back to time-based TTL.
   * Also handles API error retry cadence to prevent expensive retries.
   *
   * @param cacheKey The cache key identifying the data
   * @return true if cached and fresh, false otherwise
   */
  public boolean isCached(CacheKey cacheKey) {
    String key = cacheKey.asString();
    CacheEntry entry = entries.get(key);

    if (entry == null) {
      return false;
    }

    long now = System.currentTimeMillis();

    // Check if this is an API error entry with pending retry restriction
    if (entry.downloadRetry > 0 && now < entry.downloadRetry) {
      // Still within retry restriction period - skip download attempt
      long hoursUntilRetry = TimeUnit.MILLISECONDS.toHours(entry.downloadRetry - now);
      LOGGER.debug("Skipping {} due to API error retry restriction (retry in {} hours, error count: {})",
          cacheKey.asString(), hoursUntilRetry, entry.errorCount);
      return true;  // Return true to skip download attempt
    }

    // If retry period has passed for an API error, allow retry by removing the entry
    if (entry.downloadRetry > 0 && now >= entry.downloadRetry) {
      LOGGER.info("API error retry period expired for {} (error count: {}), allowing retry",
          cacheKey.asString(), entry.errorCount);
      entries.remove(key);
      return false;
    }

    // If we have an ETag, cache is always valid until server says otherwise (304 vs 200)
    if (entry.etag != null && !entry.etag.isEmpty()) {
      LOGGER.debug("Using cached {} data (ETag: {})", cacheKey.asString(), entry.etag);
      return true;
    }

    // Fallback: check if refresh time has passed for entries without ETags
    if (now >= entry.refreshAfter) {
      long ageHours = TimeUnit.MILLISECONDS.toHours(now - entry.cachedAt);
      LOGGER.info("Cache entry expired for {} (age: {} hours, refresh policy: {})",
          cacheKey.asString(), ageHours, entry.refreshReason != null ? entry.refreshReason : "unknown");
      entries.remove(key);
      return false;
    }

    // Log cache hit with time until refresh
    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(entry.refreshAfter - now);
    LOGGER.debug("Using cached {} data (refresh in {} hours, policy: {})",
        cacheKey.asString(), hoursUntilRefresh, entry.refreshReason != null ? entry.refreshReason : "unknown");

    return true;
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
  public void markCached(CacheKey cacheKey,
                        String filePath, long fileSize, long refreshAfter, String refreshReason) {
    String key = cacheKey.asString();
    CacheEntry entry = new CacheEntry();
    entry.dataType = cacheKey.getTableName();
    entry.parameters = new HashMap<>(cacheKey.getParameters());
    entry.filePath = filePath;
    entry.fileSize = fileSize;
    entry.cachedAt = System.currentTimeMillis();
    entry.refreshAfter = refreshAfter;
    entry.refreshReason = refreshReason;

    entries.put(key, entry);
    lastUpdated = System.currentTimeMillis();

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - entry.cachedAt);
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
  public boolean isParquetConverted(CacheKey cacheKey) {
    String key = cacheKey.asString();
    CacheEntry entry = entries.get(key);

    if (entry == null || entry.parquetPath == null) {
      return false;
    }

    // Check if raw file was updated AFTER parquet conversion (e.g., via ETag change detection)
    // If cachedAt > parquetConvertedAt, the raw file is newer and needs reconversion
    if (entry.cachedAt > entry.parquetConvertedAt) {
      LOGGER.info("Raw file updated after parquet conversion - reconversion needed: {}",
          cacheKey.asString());
      return false;
    }

    // Parquet is up-to-date with raw file
    LOGGER.info("⚡ Cached parquet, skipped conversion: {}", cacheKey.asString());
    return true;
  }


  /**
   * Mark parquet file as converted for the given cache key.
   * This is called after successful parquet conversion to avoid redundant conversions.
   *
   * @param cacheKey The cache key identifying the data
   * @param parquetPath Path to the converted parquet file
   */
  public void markParquetConverted(CacheKey cacheKey, String parquetPath) {
    String key = cacheKey.asString();
    CacheEntry entry = entries.get(key);

    if (entry == null) {
      // Create new entry if it doesn't exist (shouldn't happen normally)
      entry = new CacheEntry();
      entry.dataType = cacheKey.getTableName();
      entry.parameters = new HashMap<>(cacheKey.getParameters());
      entry.refreshAfter = Long.MAX_VALUE;  // Parquet files are immutable
      entry.refreshReason = "parquet_immutable";
      entries.put(key, entry);
    }

    entry.parquetPath = parquetPath;
    entry.parquetConvertedAt = System.currentTimeMillis();
    lastUpdated = System.currentTimeMillis();

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
  public void markUnavailable(CacheKey cacheKey,
                              int retryAfterDays, String reason) {
    String key = cacheKey.asString();
    CacheEntry entry = new CacheEntry();
    entry.dataType = cacheKey.getTableName();
    entry.parameters = new HashMap<>(cacheKey.getParameters());
    entry.filePath = null;  // No file - unavailable
    entry.fileSize = 0;
    entry.cachedAt = System.currentTimeMillis();
    entry.refreshAfter = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(retryAfterDays);
    entry.refreshReason = reason;

    entries.put(key, entry);
    lastUpdated = System.currentTimeMillis();

    LOGGER.info("Marked {} as unavailable (retry in {} days): {}",
        cacheKey.asString(), retryAfterDays, reason);
  }


  /**
   * Mark data as having API error (HTTP 200 with error content) with configurable retry cadence.
   * Prevents expensive retries on every restart while tracking error details for debugging.
   *
   * <p>This handles cases where the API returns HTTP 200 OK but includes error information
   * in the response body (e.g., BEA APIErrorCode 101 "Unknown error"). Unlike HTTP errors
   * (404, 500, etc.) which are handled elsewhere, these are successful HTTP responses that
   * contain API-level errors.
   *
   * @param cacheKey The cache key identifying the data
   * @param errorMessage Full error message from API (e.g., JSON error object)
   * @param retryAfterDays Number of days before retrying (default: 7 for weekly retry)
   */
  public void markApiError(CacheKey cacheKey,
                          String errorMessage, int retryAfterDays) {
    String key = cacheKey.asString();
    CacheEntry entry = entries.get(key);

    // Create new entry or update existing one
    if (entry == null) {
      entry = new CacheEntry();
      entry.dataType = cacheKey.getTableName();
      entry.parameters = new HashMap<>(cacheKey.getParameters());
      entry.errorCount = 0;
    }

    // Update error tracking fields
    entry.filePath = null;  // No file - API error
    entry.fileSize = 0;
    entry.lastError = errorMessage;
    entry.errorCount++;
    entry.lastAttemptAt = System.currentTimeMillis();
    entry.downloadRetry = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(retryAfterDays);
    entry.refreshAfter = entry.downloadRetry;  // Use downloadRetry as refresh time
    entry.refreshReason = "api_error_retry";

    entries.put(key, entry);
    lastUpdated = System.currentTimeMillis();

    LOGGER.info("Marked {} as API error (retry in {} days, error count: {}): {}",
        cacheKey.asString(), retryAfterDays, entry.errorCount,
        errorMessage.length() > 100 ? errorMessage.substring(0, 100) + "..." : errorMessage);
  }



  /**
   * Remove expired entries from the manifest based on refreshAfter timestamps.
   */
  public int cleanupExpiredEntries() {
    long now = System.currentTimeMillis();
    int[] removed = {0};

    entries.entrySet().removeIf(entry -> {
      CacheEntry cacheEntry = entry.getValue();

      // NOTE: File existence check removed - would fail for S3 cache URIs.
      // Callers (AbstractEconDataDownloader) handle file existence checks using StorageProvider.
      // Manifest focuses on time-based refresh policies only.

      // Remove if refresh time has passed
      if (now >= cacheEntry.refreshAfter) {
        long ageHours = TimeUnit.MILLISECONDS.toHours(now - cacheEntry.cachedAt);
        LOGGER.debug("Removing expired cache entry: {} (age: {} hours, policy: {})",
            cacheEntry.dataType, ageHours, cacheEntry.refreshReason);
        removed[0]++;
        return true;
      }

      return false;
    });

    if (removed[0] > 0) {
      lastUpdated = System.currentTimeMillis();
      LOGGER.info("Cleaned up {} expired cache entries", removed[0]);
    }

    return removed[0];
  }

  /**
   * Load manifest from file with automatic migration from old format.
   */
  public static CacheManifest load(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);

    if (!manifestFile.exists()) {
      LOGGER.warn("No cache manifest found at {}, creating new one - this will trigger full cache rebuild",
          manifestFile.getAbsolutePath());
      CacheManifest manifest = new CacheManifest();
      manifest.cacheDir = cacheDir;
      return manifest;
    }

    try {
      LOGGER.info("Loading cache manifest from {}", manifestFile.getAbsolutePath());
      CacheManifest manifest = MAPPER.readValue(manifestFile, CacheManifest.class);
      manifest.cacheDir = cacheDir;  // Set cache directory for path resolution
      LOGGER.info("Loaded cache manifest version {} with {} entries from {}",
          manifest.version, manifest.entries.size(), manifestFile.getAbsolutePath());


      return manifest;
    } catch (IOException e) {
      LOGGER.warn("Failed to load cache manifest, creating new one: {}", e.getMessage());
      return new CacheManifest();
    }
  }

  /**
   * Save manifest to file.
   */
  public void save(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);

    try {
      // Ensure directory exists
      manifestFile.getParentFile().mkdirs();

      // Clean up expired entries before saving
      int removed = cleanupExpiredEntries();

      LOGGER.info("Saving cache manifest to {} with {} entries (removed {} expired)",
          manifestFile.getAbsolutePath(), entries.size(), removed);

      // Write to temp file first, then atomic rename to ensure consistency
      File tempFile = new File(manifestFile.getParentFile(), MANIFEST_FILENAME + ".tmp");
      try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile);
           java.io.BufferedWriter writer = new java.io.BufferedWriter(
               new java.io.OutputStreamWriter(fos, java.nio.charset.StandardCharsets.UTF_8))) {

        // Write JSON
        String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        writer.write(json);
        writer.flush();

        // Force OS to flush buffers to disk
        fos.getFD().sync();
      }

      // Atomic rename
      if (!tempFile.renameTo(manifestFile)) {
        throw new IOException("Failed to rename temp file to " + manifestFile);
      }

      long fileSize = manifestFile.length();
      long lastModified = manifestFile.lastModified();
      LOGGER.info("Successfully wrote and synced cache manifest to {} (size: {} bytes, modified: {})",
          manifestFile.getAbsolutePath(), fileSize, new java.util.Date(lastModified));
    } catch (IOException e) {
      LOGGER.error("Failed to save cache manifest to {}: {}", manifestFile.getAbsolutePath(),
          e.getMessage(), e);
    }
  }


  /**
   * Get cache statistics.
   */
  @JsonIgnore
  public CacheStats getStats() {
    long now = System.currentTimeMillis();
    CacheStats stats = new CacheStats();
    stats.totalEntries = entries.size();
    stats.freshEntries = (int) entries.values().stream()
        .filter(entry -> now < entry.refreshAfter)
        .count();
    stats.expiredEntries = stats.totalEntries - stats.freshEntries;

    return stats;
  }

  /**
   * Get the ETag for a cached entry.
   *
   * @param cacheKey The cache key identifying the data
   * @return The ETag string, or null if not cached or no ETag available
   */
  public String getETag(CacheKey cacheKey) {
    String key = cacheKey.asString();
    CacheEntry entry = entries.get(key);
    return (entry != null) ? entry.etag : null;
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
    String key = "catalog_series:popularity=" + minPopularity;
    CatalogSeriesCache entry = catalogSeriesCache.get(key);

    if (entry == null) {
      return null;
    }

    // Treat empty results as cache miss (ignore TTL) - catalog extraction likely failed
    if (entry.seriesIds == null || entry.seriesIds.isEmpty()) {
      LOGGER.debug("Ignoring empty cached series list for popularity={} - treating as cache miss",
          minPopularity);
      catalogSeriesCache.remove(key);
      return null;
    }

    // Check if cache has expired
    long now = System.currentTimeMillis();
    if (now >= entry.refreshAfter) {
      long ageDays = TimeUnit.MILLISECONDS.toDays(now - entry.cachedAt);
      LOGGER.info("Catalog series cache expired (age: {} days, threshold: {})",
          ageDays, minPopularity);
      catalogSeriesCache.remove(key);
      return null;
    }

    long ageDays = TimeUnit.MILLISECONDS.toDays(now - entry.cachedAt);
    LOGGER.debug("Using cached catalog series (count: {}, threshold: {}, age: {} days)",
        entry.seriesIds.size(), minPopularity, ageDays);

    return new ArrayList<>(entry.seriesIds);
  }

  /**
   * Cache FRED catalog series extraction results with TTL.
   *
   * @param minPopularity Popularity threshold used for filtering
   * @param seriesIds List of series IDs that met the threshold
   * @param ttlDays Time-to-live in days (typically 365 for annual refresh)
   */
  public void cacheCatalogSeries(int minPopularity, List<String> seriesIds, int ttlDays) {
    // Don't cache empty results - they indicate catalog not yet downloaded or extraction failed
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.debug("Skipping cache of empty series list for popularity={} - " +
          "catalog may not be downloaded yet", minPopularity);
      return;
    }

    String key = "catalog_series:popularity=" + minPopularity;
    CatalogSeriesCache entry = new CatalogSeriesCache();
    entry.minPopularity = minPopularity;
    entry.seriesIds = new ArrayList<>(seriesIds);
    entry.cachedAt = System.currentTimeMillis();
    entry.refreshAfter = entry.cachedAt + TimeUnit.DAYS.toMillis(ttlDays);
    entry.refreshReason = "catalog_annual_refresh";

    catalogSeriesCache.put(key, entry);
    lastUpdated = System.currentTimeMillis();

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
    String key = "catalog_series:popularity=" + minPopularity;
    CatalogSeriesCache entry = catalogSeriesCache.get(key);

    if (entry == null) {
      return false;
    }

    long now = System.currentTimeMillis();
    return now < entry.refreshAfter;
  }

  /**
   * Invalidate cached catalog series for a specific popularity threshold.
   * Forces re-extraction on next access.
   *
   * @param minPopularity Popularity threshold to invalidate
   */
  public void invalidateCatalogSeriesCache(int minPopularity) {
    String key = "catalog_series:popularity=" + minPopularity;
    CatalogSeriesCache removed = catalogSeriesCache.remove(key);
    if (removed != null) {
      LOGGER.info("Invalidated catalog series cache (threshold: {}, had {} series)",
          minPopularity, removed.seriesIds.size());
      lastUpdated = System.currentTimeMillis();
    }
  }

  /**
   * Invalidate all cached catalog series for all popularity thresholds.
   * Forces re-extraction for all thresholds on next access.
   */
  public void invalidateAllCatalogSeriesCache() {
    int count = catalogSeriesCache.size();
    catalogSeriesCache.clear();
    if (count > 0) {
      LOGGER.info("Invalidated all catalog series caches ({} thresholds)", count);
      lastUpdated = System.currentTimeMillis();
    }
  }

  /**
   * Cache entry metadata with explicit refresh timestamp.
   * Extends {@link AbstractCacheManifest.BaseCacheEntry} to add ECON-specific fields.
   */
  public static class CacheEntry extends BaseCacheEntry {
    @JsonProperty("dataType")
    public String dataType;

    @JsonProperty("parameters")
    public Map<String, String> parameters = new HashMap<>();
  }

  /**
   * Cache entry for extracted FRED catalog series lists.
   * Caches expensive catalog extraction results (listing/parsing JSON files)
   * with configurable TTL since catalog changes slowly.
   * Keyed by minPopularity threshold since different thresholds yield different results.
   */
  public static class CatalogSeriesCache {
    @JsonProperty("minPopularity")
    public int minPopularity;

    @JsonProperty("seriesIds")
    public List<String> seriesIds;

    @JsonProperty("cachedAt")
    public long cachedAt;

    @JsonProperty("refreshAfter")
    public long refreshAfter;

    @JsonProperty("refreshReason")
    public String refreshReason;
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
}
