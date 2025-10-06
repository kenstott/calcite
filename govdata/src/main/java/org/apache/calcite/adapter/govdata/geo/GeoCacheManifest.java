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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
 * Uses explicit refresh timestamps stored in each entry, allowing different refresh
 * policies per data type (e.g., current year vs historical, TIGER boundaries vs Census API data).
 */
public class GeoCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoCacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MANIFEST_FILENAME = "geo_cache_manifest.json";

  @JsonProperty("entries")
  private Map<String, CacheEntry> entries = new HashMap<>();

  @JsonProperty("version")
  private String version = "1.0";

  @JsonProperty("lastUpdated")
  private long lastUpdated = System.currentTimeMillis();

  @JsonIgnore
  private String cacheDir;  // Cache directory for resolving relative paths

  /**
   * Check if data is cached and fresh for the given parameters.
   * Uses explicit refreshAfter timestamp stored in each entry.
   */
  public boolean isCached(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = entries.get(key);

    if (entry == null) {
      return false;
    }

    // Check if file still exists (skip if filePath is null for parquet-only entries)
    if (entry.filePath != null) {
      File file = cacheDir != null ? new File(cacheDir, entry.filePath) : new File(entry.filePath);
      if (!file.exists()) {
        LOGGER.debug("Cache entry removed - file no longer exists: {}", entry.filePath);
        entries.remove(key);
        return false;
      }
    }

    // Check if refresh time has passed
    long now = System.currentTimeMillis();
    if (now >= entry.refreshAfter) {
      long ageHours = TimeUnit.MILLISECONDS.toHours(now - entry.cachedAt);
      LOGGER.debug("Cache entry expired for {} year={} (age: {} hours, policy: {})",
          dataType, year, ageHours, entry.refreshReason != null ? entry.refreshReason : "unknown");
      entries.remove(key);
      return false;
    }

    // Log cache hit with time until refresh
    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(entry.refreshAfter - now);
    LOGGER.debug("Using cached {} data for year {} (refresh in {} hours, policy: {})",
        dataType, year, hoursUntilRefresh, entry.refreshReason != null ? entry.refreshReason : "unknown");

    return true;
  }

  /**
   * Mark data as cached with metadata and explicit refresh timestamp.
   *
   * @param dataType The type of data being cached
   * @param year The year of the data
   * @param parameters Additional parameters for the cache key
   * @param filePath Path to the cached file
   * @param fileSize Size of the cached file
   * @param refreshAfter Timestamp (millis since epoch) when this entry should be refreshed
   * @param refreshReason Human-readable reason for the refresh policy
   */
  public void markCached(String dataType, int year, Map<String, String> parameters,
                        String filePath, long fileSize, long refreshAfter, String refreshReason) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = new CacheEntry();
    entry.dataType = dataType;
    entry.year = year;
    entry.parameters = new HashMap<>(parameters != null ? parameters : new HashMap<>());
    entry.filePath = filePath;
    entry.fileSize = fileSize;
    entry.cachedAt = System.currentTimeMillis();
    entry.refreshAfter = refreshAfter;
    entry.refreshReason = refreshReason;

    entries.put(key, entry);
    lastUpdated = System.currentTimeMillis();

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - entry.cachedAt);
    LOGGER.debug("Marked as cached: {} (year={}, size={}, refresh in {} hours, policy: {})",
        dataType, year, fileSize, hoursUntilRefresh, refreshReason);
  }

  /**
   * Check if parquet conversion is complete for the given data.
   * Respects TTL - returns false if entry has expired.
   */
  public boolean isParquetConverted(String dataType, int year, Map<String, String> parameters) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = entries.get(key);

    if (entry == null || entry.parquetPath == null || entry.parquetConvertedAt == 0) {
      return false;
    }

    // Check if entry has expired based on TTL
    long now = System.currentTimeMillis();
    if (now >= entry.refreshAfter) {
      long ageHours = TimeUnit.MILLISECONDS.toHours(now - entry.cachedAt);
      LOGGER.debug("Parquet entry expired for {} year={} (age: {} hours, policy: {})",
          dataType, year, ageHours, entry.refreshReason != null ? entry.refreshReason : "unknown");
      entries.remove(key);
      return false;
    }

    LOGGER.debug("Parquet already converted for {} year={}: {}", dataType, year, entry.parquetPath);
    return true;
  }

  /**
   * Mark parquet conversion as complete for cached data.
   * Creates a stub cache entry if no raw download entry exists (handles legacy data).
   */
  public void markParquetConverted(String dataType, int year, Map<String, String> parameters, String parquetPath) {
    String key = buildKey(dataType, year, parameters);
    CacheEntry entry = entries.get(key);

    if (entry == null) {
      // Create stub entry for parquet-only data (no raw download tracked)
      LOGGER.debug("Creating stub cache entry for parquet-only data: {} year={}", dataType, year);
      entry = new CacheEntry();
      entry.dataType = dataType;
      entry.year = year;
      entry.parameters = new HashMap<>(parameters != null ? parameters : new HashMap<>());
      entry.filePath = null;  // No raw download
      entry.fileSize = 0;
      entry.cachedAt = System.currentTimeMillis();
      entry.refreshAfter = Long.MAX_VALUE;  // Never refresh (historical data)
      entry.refreshReason = "parquet_only";
      entries.put(key, entry);
    }

    entry.parquetPath = parquetPath;
    entry.parquetConvertedAt = System.currentTimeMillis();
    lastUpdated = System.currentTimeMillis();

    LOGGER.debug("Marked parquet converted: {} year={} -> {}", dataType, year, parquetPath);
  }

  /**
   * Mark data as cached with default refresh for TIGER boundaries (immutable) and Census API data (annual refresh).
   */
  public void markCached(String dataType, int year, Map<String, String> parameters,
                        String filePath, long fileSize) {
    // TIGER boundary data is immutable once published
    // Census API demographic data refreshes annually
    long refreshAfter = Long.MAX_VALUE;
    String refreshReason = "geographic_boundary_immutable";

    markCached(dataType, year, parameters, filePath, fileSize, refreshAfter, refreshReason);
  }

  /**
   * Remove expired entries from the manifest based on refreshAfter timestamps.
   */
  public int cleanupExpiredEntries() {
    long now = System.currentTimeMillis();
    int[] removed = {0};

    entries.entrySet().removeIf(entry -> {
      CacheEntry cacheEntry = entry.getValue();

      // Skip file existence check if filePath is null (parquet-only entries)
      if (cacheEntry.filePath != null) {
        // Remove if file doesn't exist (resolve relative path using cache directory)
        File file = cacheDir != null ? new File(cacheDir, cacheEntry.filePath) : new File(cacheEntry.filePath);
        if (!file.exists()) {
          LOGGER.debug("Removing cache entry for missing file: {}", cacheEntry.filePath);
          removed[0]++;
          return true;
        }
      }

      // Remove if refresh time has passed
      if (now >= cacheEntry.refreshAfter) {
        long ageHours = TimeUnit.MILLISECONDS.toHours(now - cacheEntry.cachedAt);
        LOGGER.debug("Removing expired cache entry: {} year={} (age: {} hours, policy: {})",
            cacheEntry.dataType, cacheEntry.year, ageHours, cacheEntry.refreshReason);
        removed[0]++;
        return true;
      }

      return false;
    });

    if (removed[0] > 0) {
      lastUpdated = System.currentTimeMillis();
      LOGGER.debug("Cleaned up {} expired cache entries", removed[0]);
    }

    return removed[0];
  }

  /**
   * Load manifest from file.
   */
  public static GeoCacheManifest load(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);

    if (!manifestFile.exists()) {
      LOGGER.debug("No geo cache manifest found, creating new one");
      GeoCacheManifest manifest = new GeoCacheManifest();
      manifest.cacheDir = cacheDir;
      return manifest;
    }

    try {
      GeoCacheManifest manifest = MAPPER.readValue(manifestFile, GeoCacheManifest.class);
      manifest.cacheDir = cacheDir;  // Set cache directory for path resolution
      LOGGER.debug("Loaded geo cache manifest version {} with {} entries", manifest.version, manifest.entries.size());
      return manifest;
    } catch (IOException e) {
      LOGGER.warn("Failed to load geo cache manifest, creating new one: {}", e.getMessage());
      GeoCacheManifest manifest = new GeoCacheManifest();
      manifest.cacheDir = cacheDir;
      return manifest;
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
      cleanupExpiredEntries();

      MAPPER.writerWithDefaultPrettyPrinter().writeValue(manifestFile, this);
      LOGGER.debug("Saved geo cache manifest with {} entries", entries.size());
    } catch (IOException e) {
      LOGGER.warn("Failed to save geo cache manifest: {}", e.getMessage());
    }
  }

  /**
   * Build cache key from parameters.
   */
  private String buildKey(String dataType, int year, Map<String, String> parameters) {
    StringBuilder key = new StringBuilder();
    key.append(dataType).append(":").append(year);

    if (parameters != null && !parameters.isEmpty()) {
      parameters.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(entry -> key.append(":").append(entry.getKey()).append("=").append(entry.getValue()));
    }

    return key.toString();
  }

  /**
   * Cache entry metadata with explicit refresh timestamp.
   */
  public static class CacheEntry {
    @JsonProperty("dataType")
    public String dataType;

    @JsonProperty("year")
    public int year;

    @JsonProperty("parameters")
    public Map<String, String> parameters = new HashMap<>();

    @JsonProperty("filePath")
    public String filePath;

    @JsonProperty("fileSize")
    public long fileSize;

    @JsonProperty("cachedAt")
    public long cachedAt;

    @JsonProperty("refreshAfter")
    public long refreshAfter = Long.MAX_VALUE;

    @JsonProperty("refreshReason")
    public String refreshReason;

    @JsonProperty("parquetPath")
    public String parquetPath;

    @JsonProperty("parquetConvertedAt")
    public long parquetConvertedAt;
  }
}
