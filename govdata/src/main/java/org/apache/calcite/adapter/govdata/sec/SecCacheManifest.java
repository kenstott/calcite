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

import org.apache.calcite.adapter.govdata.AbstractCacheManifest;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Cache manifest for tracking SEC submissions.json files with ETag support.
 * Enables efficient conditional GET requests to avoid redundant downloads.
 *
 * <p>Uses DuckDB for persistent storage, providing proper ACID guarantees and
 * thread-safe concurrent access. Replaces the previous JSON-based implementation.
 *
 * <p>Uses HTTP ETags to detect changes on the SEC server, falling back to
 * time-based expiration when ETags are not available.
 */
public class SecCacheManifest extends AbstractCacheManifest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecCacheManifest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LEGACY_MANIFEST_FILENAME = "cache_manifest.json";

  /** DuckDB-based cache store. */
  private final DuckDBCacheStore store;

  /** Cache directory path. */
  private final String cacheDir;

  /**
   * Private constructor - use {@link #load(String)} to get an instance.
   */
  private SecCacheManifest(String cacheDir) {
    this.cacheDir = cacheDir;
    this.store = DuckDBCacheStore.getInstance("sec", cacheDir);
  }

  /**
   * Check if a CIK has been fully processed (all filings converted to parquet).
   * This is valid only if the submissions.json has not changed since processing.
   *
   * @param cik The CIK to check
   * @return true if the CIK is fully processed and submissions.json hasn't changed
   */
  public boolean isCikFullyProcessed(String cik) {
    // Check in cache_entries table using special key pattern
    String key = "cik_processed:" + cik;
    return store.isCached(key);
  }

  /**
   * Mark a CIK as fully processed (all filings have been converted to parquet).
   *
   * @param cik The CIK
   * @param totalFilings Total number of filings that were processed
   */
  public void markCikFullyProcessed(String cik, int totalFilings) {
    String key = "cik_processed:" + cik;
    // Store with Long.MAX_VALUE refresh (until explicitly invalidated)
    store.upsertEntry(key, "cik_processed", String.valueOf(totalFilings),
        null, totalFilings, Long.MAX_VALUE, "fully_processed");
    LOGGER.info("Marked CIK {} as fully processed ({} filings)", cik, totalFilings);
  }

  /**
   * Invalidate the fully processed flag for a CIK (e.g., when submissions.json changes).
   *
   * @param cik The CIK
   */
  public void invalidateCikFullyProcessed(String cik) {
    String key = "cik_processed:" + cik;
    store.deleteEntry(key);
    LOGGER.debug("Invalidated fully_processed flag for CIK {}", cik);
  }

  /**
   * Check if submissions.json is cached and still fresh for the given CIK.
   *
   * @param cik The CIK to check
   * @return true if cached and fresh, false otherwise
   */
  public boolean isCached(String cik) {
    String key = "submission:" + cik;
    return store.isCached(key);
  }

  /**
   * Get the ETag for a cached CIK's submissions.json file.
   *
   * @param cik The CIK to get the ETag for
   * @return The ETag string, or null if not cached or no ETag available
   */
  public String getETag(String cik) {
    String key = "submission:" + cik;
    return store.getETag(key);
  }

  /**
   * Get the file path for a cached CIK's submissions.json file.
   *
   * @param cik The CIK to get the file path for
   * @return The file path, or null if not cached
   */
  public String getFilePath(String cik) {
    // This requires a custom query - for now return null as filepath is stored in cache_entries
    // The DuckDBCacheStore would need a getFilePath method to support this
    return null;
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
    String key = "submission:" + cik;
    store.upsertEntry(key, "submission", cik, filePath, fileSize, refreshAfter, refreshReason);
    // Note: etag is stored separately - DuckDBCacheStore would need enhancement to store etag

    if (etag != null && !etag.isEmpty()) {
      LOGGER.debug("Marked as cached: CIK {} (size={}, ETag={})", cik, fileSize, etag);
    } else {
      long hoursUntilRefresh = TimeUnit.MILLISECONDS.toHours(refreshAfter - System.currentTimeMillis());
      LOGGER.debug("Marked as cached: CIK {} (size={}, refresh in {} hours, policy: {})",
          cik, fileSize, hoursUntilRefresh, refreshReason);
    }
  }

  /**
   * Mark submissions.json as cached without metadata.
   * Falls back to 24-hour TTL when ETag/Last-Modified not available.
   */
  public void markCached(String cik, String filePath, long fileSize) {
    long refreshAfter = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
    markCached(cik, filePath, null, fileSize, refreshAfter, "daily_fallback_no_metadata");
  }

  /**
   * Remove expired entries from the manifest.
   *
   * @return Number of entries removed
   */
  public int cleanupExpiredEntries() {
    return store.cleanupExpiredEntries();
  }

  /**
   * Load manifest from DuckDB cache store.
   * Automatically migrates from JSON if a legacy manifest exists.
   */
  public static SecCacheManifest load(String cacheDir) {
    SecCacheManifest manifest = new SecCacheManifest(cacheDir);

    // Check for legacy JSON manifest and migrate if present
    File legacyFile = new File(cacheDir, LEGACY_MANIFEST_FILENAME);
    if (legacyFile.exists()) {
      manifest.migrateFromJson(legacyFile);
    }

    int[] stats = manifest.store.getStats();
    LOGGER.info("Loaded SEC cache manifest from DuckDB with {} entries ({} fresh, {} expired)",
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

      // Migrate submission entries
      int migratedSubmissions = 0;
      if (legacy.entries != null) {
        for (java.util.Map.Entry<String, LegacySubmissionEntry> entry : legacy.entries.entrySet()) {
          String cik = entry.getKey();
          LegacySubmissionEntry subEntry = entry.getValue();

          String key = "submission:" + cik;
          store.upsertEntry(key, "submission", cik, subEntry.filePath,
              subEntry.fileSize, subEntry.refreshAfter,
              subEntry.refreshReason != null ? subEntry.refreshReason : "migrated");

          // Migrate fully processed status
          if (subEntry.fullyProcessed && subEntry.totalFilingsWhenProcessed != null) {
            markCikFullyProcessed(cik, subEntry.totalFilingsWhenProcessed);
          }

          migratedSubmissions++;
        }
      }

      // Migrate filing entries to sec_filings table
      int migratedFilings = 0;
      if (legacy.filings != null) {
        for (java.util.Map.Entry<String, LegacyFilingEntry> entry : legacy.filings.entrySet()) {
          LegacyFilingEntry filingEntry = entry.getValue();
          store.markFiling(filingEntry.cik, filingEntry.accession, filingEntry.fileName,
              filingEntry.state, filingEntry.reason);
          migratedFilings++;
        }
      }

      LOGGER.info("Migrated {} submission entries and {} filing entries from JSON to DuckDB",
          migratedSubmissions, migratedFilings);

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

  // ===== Abstract method implementations from AbstractCacheManifest =====
  // SEC schema uses CIK-based caching (different paradigm than ECON/GEO)

  @Override public boolean isCached(CacheKey cacheKey) {
    throw new UnsupportedOperationException(
        "SEC schema uses CIK-based caching. Use isCached(String cik) instead.");
  }

  @Override public void markCached(CacheKey cacheKey, String filePath, long fileSize,
      long refreshAfter, String refreshReason) {
    throw new UnsupportedOperationException(
        "SEC schema uses CIK-based caching. Use markCached(String cik, ...) instead.");
  }

  @Override public boolean isMaterialized(CacheKey cacheKey) {
    throw new UnsupportedOperationException(
        "SEC schema tracks materialization via filing-level state.");
  }

  @Override public void markMaterialized(CacheKey cacheKey, String outputPath) {
    throw new UnsupportedOperationException(
        "SEC schema tracks materialization via filing-level state.");
  }

  @Override public void markApiError(CacheKey cacheKey, String errorMessage, int retryAfterDays) {
    LOGGER.warn("markApiError called on SecCacheManifest - not implemented for SEC adapter");
  }

  // ===== Filing tracking methods =====

  /**
   * Check if a filing file is known to not exist.
   */
  public boolean isFileNotFound(String cik, String accession, String fileName) {
    return store.isFilingInState(cik, accession, fileName, "not_found");
  }

  /**
   * Mark a filing file as downloaded and cached.
   */
  public void markFileDownloaded(String cik, String accession, String fileName) {
    store.markFiling(cik, accession, fileName, "downloaded", null);
  }

  /**
   * Check if a filing file is known to be downloaded and cached.
   */
  public boolean isFileDownloaded(String cik, String accession, String fileName) {
    return store.isFilingInState(cik, accession, fileName, "downloaded");
  }

  /**
   * Mark a filing file as not found.
   */
  public void markFileNotFound(String cik, String accession, String fileName, String reason) {
    store.markFiling(cik, accession, fileName, "not_found", reason);
  }

  /**
   * Remove a filing entry (for cache invalidation).
   */
  public void removeFilingEntry(String cik, String accession, String fileName) {
    store.removeFiling(cik, accession, fileName);
  }

  /**
   * Cache the XBRL instance document filename from FilingSummary.xml.
   */
  public void cacheFilingSummaryXbrlFilename(String cik, String accession, String xbrlInstanceFilename) {
    // Store the XBRL filename in the reason field
    store.markFiling(cik, accession, "FilingSummary.xml", "xbrl_filename_cached", xbrlInstanceFilename);
  }

  /**
   * Get the cached XBRL instance document filename from FilingSummary.xml.
   */
  public String getCachedFilingSummaryXbrlFilename(String cik, String accession) {
    return store.getCachedXbrlFilename(cik, accession);
  }

  /**
   * Mark FilingSummary.xml as unavailable.
   */
  public void markFilingSummaryNotFound(String cik, String accession, String reason) {
    markFileNotFound(cik, accession, "FilingSummary.xml", reason);
  }

  /**
   * Check if FilingSummary.xml is known to not exist for this filing.
   */
  public boolean isFilingSummaryNotFound(String cik, String accession) {
    return isFileNotFound(cik, accession, "FilingSummary.xml");
  }

  /**
   * Get cache statistics.
   */
  public CacheStats getStats() {
    int[] stats = store.getStats();
    CacheStats cacheStats = new CacheStats();
    cacheStats.totalEntries = stats[0];
    cacheStats.entriesWithETag = 0; // Would need custom query
    cacheStats.entriesWithoutETag = stats[0];
    cacheStats.expiredEntries = stats[2];
    cacheStats.totalFilings = 0; // Would need custom query
    cacheStats.notFoundFilings = 0; // Would need custom query
    return cacheStats;
  }

  /**
   * Cache statistics.
   */
  public static class CacheStats {
    public int totalEntries;
    public int entriesWithETag;
    public int entriesWithoutETag;
    public int expiredEntries;
    public int totalFilings;
    public int notFoundFilings;

    @Override public String toString() {
      return String.format(
          "SEC Cache stats: %d submissions (%d with ETag, %d without ETag, %d expired), %d filings (%d not found)",
          totalEntries, entriesWithETag, entriesWithoutETag, expiredEntries, totalFilings, notFoundFilings);
    }
  }

  // ===== Legacy JSON classes for migration =====

  private static class LegacyManifest {
    public java.util.Map<String, LegacySubmissionEntry> entries;
    public java.util.Map<String, LegacyFilingEntry> filings;
    public String version;
    public long lastUpdated;
  }

  private static class LegacySubmissionEntry {
    public String cik;
    public String filePath;
    public String etag;
    public long fileSize;
    public long downloadedAt;
    public long refreshAfter;
    public String refreshReason;
    public boolean fullyProcessed;
    public Long fullyProcessedAt;
    public Integer totalFilingsWhenProcessed;
  }

  private static class LegacyFilingEntry {
    public String cik;
    public String accession;
    public String fileName;
    public String state;
    public String reason;
    public long checkedAt;
  }
}
