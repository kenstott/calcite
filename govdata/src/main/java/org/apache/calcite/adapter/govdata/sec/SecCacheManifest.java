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
package org.apache.calcite.adapter.govdata.sec;

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
 * Cache manifest for tracking SEC submissions.json files with ETag support.
 * Enables efficient conditional GET requests to avoid redundant downloads.
 *
 * <p>Uses HTTP ETags to detect changes on the SEC server, falling back to
 * time-based expiration when ETags are not available.
 */
public class SecCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecCacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MANIFEST_FILENAME = "sec_cache_manifest.json";

  @JsonProperty("entries")
  private Map<String, SubmissionCacheEntry> entries = new HashMap<>();

  @JsonProperty("version")
  private String version = "1.0";

  @JsonProperty("lastUpdated")
  private long lastUpdated = System.currentTimeMillis();

  /**
   * Check if submissions.json is cached and still fresh for the given CIK.
   *
   * @param cik The CIK to check
   * @return true if cached and fresh, false otherwise
   */
  public boolean isCached(String cik) {
    SubmissionCacheEntry entry = entries.get(cik);

    if (entry == null) {
      return false;
    }

    // Check if file still exists
    if (!new File(entry.filePath).exists()) {
      LOGGER.debug("Cache entry removed - file no longer exists: {}", entry.filePath);
      entries.remove(cik);
      return false;
    }

    // If we have an ETag, cache is always valid until server says otherwise (304 vs 200)
    if (entry.etag != null && !entry.etag.isEmpty()) {
      LOGGER.debug("Using cached submissions for CIK {} (ETag: {})", cik, entry.etag);
      return true;
    }

    // Fallback: check if refresh time has passed for entries without ETags
    long now = System.currentTimeMillis();
    if (now >= entry.refreshAfter) {
      long ageHours = TimeUnit.MILLISECONDS.toHours(now - entry.downloadedAt);
      LOGGER.info("Cache entry expired for CIK {} (age: {} hours, no ETag available)",
          cik, ageHours);
      entries.remove(cik);
      return false;
    }

    long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(entry.refreshAfter - now);
    LOGGER.debug("Using cached submissions for CIK {} (refresh in {} hours, no ETag)",
        cik, hoursUntilRefresh);

    return true;
  }

  /**
   * Get the ETag for a cached CIK's submissions.json file.
   *
   * @param cik The CIK to get the ETag for
   * @return The ETag string, or null if not cached or no ETag available
   */
  public String getETag(String cik) {
    SubmissionCacheEntry entry = entries.get(cik);
    return (entry != null) ? entry.etag : null;
  }

  /**
   * Get the file path for a cached CIK's submissions.json file.
   *
   * @param cik The CIK to get the file path for
   * @return The file path, or null if not cached
   */
  public String getFilePath(String cik) {
    SubmissionCacheEntry entry = entries.get(cik);
    return (entry != null) ? entry.filePath : null;
  }

  /**
   * Mark submissions.json as cached for a CIK with ETag support.
   *
   * @param cik The CIK
   * @param filePath Path to the cached submissions.json file
   * @param etag ETag from SEC response header (null if not available)
   * @param fileSize Size of the cached file
   * @param refreshAfter Timestamp when this entry should be refreshed (fallback if no ETag)
   * @param refreshReason Human-readable reason for the refresh policy
   */
  public void markCached(String cik, String filePath, String etag, long fileSize,
                        long refreshAfter, String refreshReason) {
    SubmissionCacheEntry entry = new SubmissionCacheEntry();
    entry.cik = cik;
    entry.filePath = filePath;
    entry.etag = etag;
    entry.fileSize = fileSize;
    entry.downloadedAt = System.currentTimeMillis();
    entry.refreshAfter = refreshAfter;
    entry.refreshReason = refreshReason;

    entries.put(cik, entry);
    lastUpdated = System.currentTimeMillis();

    if (etag != null && !etag.isEmpty()) {
      LOGGER.debug("Marked as cached: CIK {} (size={}, ETag={})", cik, fileSize, etag);
    } else {
      long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - entry.downloadedAt);
      LOGGER.debug("Marked as cached: CIK {} (size={}, refresh in {} hours, policy: {})",
          cik, fileSize, hoursUntilRefresh, refreshReason);
    }
  }

  /**
   * Mark submissions.json as cached with default 24-hour refresh (no ETag).
   * Consider using {@link #markCached(String, String, String, long, long, String)}
   * with explicit ETag for better caching efficiency.
   *
   * @param cik The CIK
   * @param filePath Path to the cached submissions.json file
   * @param fileSize Size of the cached file
   */
  public void markCached(String cik, String filePath, long fileSize) {
    long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    markCached(cik, filePath, null, fileSize, refreshAfter, "daily_fallback_no_etag");
  }

  /**
   * Remove expired entries from the manifest.
   * Note: Entries with valid ETags are not removed based on time.
   *
   * @return Number of entries removed
   */
  public int cleanupExpiredEntries() {
    long now = System.currentTimeMillis();
    int[] removed = {0};

    entries.entrySet().removeIf(entry -> {
      SubmissionCacheEntry cacheEntry = entry.getValue();

      // Remove if file doesn't exist
      if (!new File(cacheEntry.filePath).exists()) {
        LOGGER.debug("Removing cache entry for missing file: {}", cacheEntry.filePath);
        removed[0]++;
        return true;
      }

      // Don't remove entries with ETags based on time - let server decide via 304/200
      if (cacheEntry.etag != null && !cacheEntry.etag.isEmpty()) {
        return false;
      }

      // Remove if refresh time has passed (no ETag available)
      if (now >= cacheEntry.refreshAfter) {
        long ageHours = TimeUnit.MILLISECONDS.toHours(now - cacheEntry.downloadedAt);
        LOGGER.debug("Removing expired cache entry: CIK {} (age: {} hours, no ETag)",
            cacheEntry.cik, ageHours);
        removed[0]++;
        return true;
      }

      return false;
    });

    if (removed[0] > 0) {
      lastUpdated = System.currentTimeMillis();
      LOGGER.info("Cleaned up {} expired SEC cache entries", removed[0]);
    }

    return removed[0];
  }

  /**
   * Load manifest from file.
   *
   * @param cacheDir The cache directory
   * @return Loaded manifest or new empty manifest if file doesn't exist
   */
  public static SecCacheManifest load(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);

    if (!manifestFile.exists()) {
      LOGGER.debug("No SEC cache manifest found, creating new one");
      return new SecCacheManifest();
    }

    try {
      SecCacheManifest manifest = MAPPER.readValue(manifestFile, SecCacheManifest.class);
      LOGGER.debug("Loaded SEC cache manifest version {} with {} entries",
          manifest.version, manifest.entries.size());
      return manifest;
    } catch (IOException e) {
      LOGGER.warn("Failed to load SEC cache manifest, creating new one: {}", e.getMessage());
      return new SecCacheManifest();
    }
  }

  /**
   * Save manifest to file.
   *
   * @param cacheDir The cache directory
   */
  public void save(String cacheDir) {
    File manifestFile = new File(cacheDir, MANIFEST_FILENAME);

    try {
      // Ensure directory exists
      manifestFile.getParentFile().mkdirs();

      // Clean up expired entries before saving
      cleanupExpiredEntries();

      MAPPER.writerWithDefaultPrettyPrinter().writeValue(manifestFile, this);
      LOGGER.debug("Saved SEC cache manifest with {} entries", entries.size());
    } catch (IOException e) {
      LOGGER.warn("Failed to save SEC cache manifest: {}", e.getMessage());
    }
  }

  /**
   * Get cache statistics.
   *
   * @return Cache statistics
   */
  @JsonIgnore
  public CacheStats getStats() {
    long now = System.currentTimeMillis();
    CacheStats stats = new CacheStats();
    stats.totalEntries = entries.size();
    stats.entriesWithETag = (int) entries.values().stream()
        .filter(entry -> entry.etag != null && !entry.etag.isEmpty())
        .count();
    stats.entriesWithoutETag = stats.totalEntries - stats.entriesWithETag;
    stats.expiredEntries = (int) entries.values().stream()
        .filter(entry -> (entry.etag == null || entry.etag.isEmpty()) && now >= entry.refreshAfter)
        .count();

    return stats;
  }

  /**
   * Cache entry metadata for SEC submissions.json files.
   */
  public static class SubmissionCacheEntry {
    @JsonProperty("cik")
    public String cik;

    @JsonProperty("filePath")
    public String filePath;

    @JsonProperty("etag")
    public String etag;  // ETag from SEC response header

    @JsonProperty("fileSize")
    public long fileSize;

    @JsonProperty("downloadedAt")
    public long downloadedAt;

    @JsonProperty("refreshAfter")
    public long refreshAfter = Long.MAX_VALUE;  // Fallback if no ETag

    @JsonProperty("refreshReason")
    public String refreshReason;  // e.g., "etag_based", "daily_fallback_no_etag"
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int totalEntries;
    public int entriesWithETag;
    public int entriesWithoutETag;
    public int expiredEntries;

    @Override
    public String toString() {
      return String.format("SEC Cache stats: %d total, %d with ETag, %d without ETag, %d expired",
                          totalEntries, entriesWithETag, entriesWithoutETag, expiredEntries);
    }
  }
}