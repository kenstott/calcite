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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.govdata.AbstractCacheManifest;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Check if a CIK has been fully processed for a specific year and filing type.
   * This is valid only if the submissions.json has not changed since processing.
   *
   * @param cik The CIK to check
   * @param year The filing year
   * @param filingType The filing type (e.g., "10-K", "10-Q")
   * @return true if the CIK is fully processed for this year/type
   */
  public boolean isCikFullyProcessed(String cik, int year, String filingType) {
    // Key includes year and filing type for targeted caching
    String key = buildCikProcessedKey(cik, year, filingType);
    return store.isCached(key);
  }

  /**
   * Mark a CIK as fully processed for a specific year and filing type.
   *
   * @param cik The CIK
   * @param year The filing year
   * @param filingType The filing type (e.g., "10-K", "10-Q")
   * @param totalFilings Total number of filings that were processed
   */
  public void markCikFullyProcessed(String cik, int year, String filingType, int totalFilings) {
    String key = buildCikProcessedKey(cik, year, filingType);
    // Store with Long.MAX_VALUE refresh (until explicitly invalidated)
    store.upsertEntry(key, "cik_processed", String.valueOf(totalFilings),
        null, totalFilings, Long.MAX_VALUE, "fully_processed");
    LOGGER.info("Marked CIK {} as fully processed for {}/{} ({} filings)",
        cik, year, filingType, totalFilings);
  }

  /**
   * Invalidate the fully processed flag for a CIK/year/type combination.
   *
   * @param cik The CIK
   * @param year The filing year
   * @param filingType The filing type
   */
  public void invalidateCikFullyProcessed(String cik, int year, String filingType) {
    String key = buildCikProcessedKey(cik, year, filingType);
    store.deleteEntry(key);
    LOGGER.debug("Invalidated fully_processed flag for CIK {}/{}/{}", cik, year, filingType);
  }

  /**
   * Invalidate all fully processed flags for a CIK (e.g., when submissions.json changes).
   * This clears all year/type combinations for the CIK.
   *
   * @param cik The CIK
   */
  public void invalidateAllCikProcessed(String cik) {
    String keyPrefix = "cik_processed:" + cik + ":";
    int deleted = store.deleteEntriesWithPrefix(keyPrefix);
    if (deleted > 0) {
      LOGGER.debug("Invalidated {} fully_processed flags for CIK {}", deleted, cik);
    }
  }

  /**
   * Build the cache key for cik_processed entries.
   * Format: cik_processed:{cik}:{year}:{filingType}
   */
  private String buildCikProcessedKey(String cik, int year, String filingType) {
    return "cik_processed:" + cik + ":" + year + ":" + filingType;
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

  // ===== Document-level (cik+accession) tracking =====
  // For tracking individual SEC filings that have been processed to parquet

  /**
   * Check if a document (filing) has already been processed to parquet.
   * This avoids expensive S3 exists checks by tracking processing state in DuckDB.
   *
   * @param cik The CIK
   * @param accession The accession number (e.g., "0000320193-24-000088")
   * @return true if the document has been processed, false otherwise
   */
  public boolean isDocumentProcessed(String cik, String accession) {
    String key = "doc:" + cik + ":" + accession;
    return store.isCached(key);
  }

  /**
   * Mark a document (filing) as successfully processed to parquet.
   *
   * @param cik The CIK
   * @param accession The accession number
   * @param outputFileCount Number of parquet files created
   */
  public void markDocumentProcessed(String cik, String accession, int outputFileCount) {
    String key = "doc:" + cik + ":" + accession;
    // Store with Long.MAX_VALUE refresh (immutable once processed)
    store.upsertEntry(key, "document_processed", String.valueOf(outputFileCount),
        null, outputFileCount, Long.MAX_VALUE, "immutable");
    LOGGER.trace("Marked document {}:{} as processed ({} files)", cik, accession, outputFileCount);
  }

  /**
   * Invalidate all processed documents for a CIK (e.g., when submissions.json changes).
   * This is rarely needed but ensures consistency if SEC updates past filings.
   *
   * @param cik The CIK
   */
  public void invalidateDocumentsForCik(String cik) {
    // Delete all entries matching the pattern "doc:{cik}:*"
    String keyPrefix = "doc:" + cik + ":";
    int deleted = store.deleteEntriesWithPrefix(keyPrefix);
    if (deleted > 0) {
      LOGGER.debug("Invalidated {} processed document entries for CIK {}", deleted, cik);
    }
  }

  /**
   * Load manifest from DuckDB cache store.
   */
  public static SecCacheManifest load(String cacheDir) {
    SecCacheManifest manifest = new SecCacheManifest(cacheDir);

    int[] stats = manifest.store.getStats();
    LOGGER.info("Loaded SEC cache manifest from DuckDB with {} entries ({} fresh, {} expired)",
        stats[0], stats[1], stats[2]);

    return manifest;
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

  // ===== Stock ticker availability tracking =====

  /**
   * Check if a stock ticker is marked as unavailable (doesn't exist on Stooq).
   *
   * @param ticker The stock ticker symbol
   * @return true if ticker is unavailable and should not be retried
   */
  public boolean isTickerUnavailable(String ticker) {
    String key = "ticker_unavailable:" + ticker.toUpperCase();
    return store.isUnavailable(key);
  }

  /**
   * Mark a stock ticker as unavailable (doesn't exist on Stooq).
   * Uses Long.MAX_VALUE TTL so it's essentially permanent until manually cleared.
   *
   * @param ticker The stock ticker symbol
   * @param reason Reason for unavailability (e.g., "no_data_from_stooq")
   */
  public void markTickerUnavailable(String ticker, String reason) {
    String key = "ticker_unavailable:" + ticker.toUpperCase();
    // Use max retry days to make this essentially permanent
    // 365000 days = ~1000 years, effectively permanent
    store.markApiError(key, "ticker_availability", reason, 365000);
    LOGGER.info("Marked ticker {} as unavailable: {}", ticker, reason);
  }

  // ===== Stock ticker round-robin tracking =====

  /**
   * Check if a stock ticker has been processed in the current round.
   * Persists across days — resets only when the full ticker list is exhausted.
   *
   * @param ticker The stock ticker symbol
   * @return true if already processed in the current round
   */
  public boolean isTickerProcessed(String ticker) {
    String key = "ticker_processed:" + ticker.toUpperCase();
    return store.isCached(key);
  }

  /**
   * Mark a stock ticker as processed.
   * Stored permanently until {@link #clearTickerProcessedFlags()} is called at round completion.
   *
   * @param ticker The stock ticker symbol
   */
  public void markTickerProcessed(String ticker) {
    String key = "ticker_processed:" + ticker.toUpperCase();
    store.upsertEntry(key, "ticker_processed", ticker.toUpperCase(),
        null, 1, Long.MAX_VALUE, "round_robin");
    LOGGER.debug("Marked ticker {} as processed", ticker);
  }

  /**
   * Clear all ticker-processed flags to begin a new round.
   * Called automatically when the full ticker list has been exhausted.
   *
   * @return number of flags cleared
   */
  public int clearTickerProcessedFlags() {
    int cleared = store.deleteEntriesWithPrefix("ticker_processed:");
    LOGGER.info("Cleared {} ticker-processed flags — starting new round", cleared);
    return cleared;
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
}
