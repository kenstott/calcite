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
package org.apache.calcite.adapter.govdata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for all govdata cache manifest implementations.
 * Provides common cache entry structure and utilities shared across schemas.
 *
 * <p>All govdata schemas (SEC, ECON, GEO) extend this class to benefit from:
 * <ul>
 *   <li>HTTP ETag conditional GET support</li>
 *   <li>Time-based TTL with explicit refresh policies</li>
 *   <li>Materialization tracking to avoid redundant S3 exists checks</li>
 * </ul>
 *
 * <p>Subclasses must provide schema-specific key building logic and
 * may add additional fields to their cache entry classes.
 */
public abstract class AbstractCacheManifest {

  /**
   * Tracks whether the manifest has been modified since last save.
   * Subclasses must set this to true when any mutation occurs.
   */
  @com.fasterxml.jackson.annotation.JsonIgnore
  protected transient boolean dirty = false;

  /**
   * Check if the manifest has been modified since last save.
   *
   * @return true if any mutation has occurred since last save
   */
  public boolean isDirty() {
    return dirty;
  }

  /**
   * Reset the dirty flag after successful save.
   * Should be called by subclass save() implementations after writing to disk.
   */
  protected void resetDirty() {
    dirty = false;
  }

  /**
   * Mark the manifest as modified.
   * Should be called by subclass methods when any mutation occurs.
   */
  protected void markDirty() {
    dirty = true;
  }

  /**
   * Base cache entry class with fields common to all govdata schemas.
   * Subclasses extend this to add schema-specific fields.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class BaseCacheEntry {
    @JsonProperty("filePath")
    public String filePath;

    @JsonProperty("fileSize")
    public long fileSize;

    @JsonProperty("cachedAt")
    public long cachedAt;

    @JsonProperty("refreshAfter")
    public long refreshAfter = Long.MAX_VALUE;  // Default: never refresh

    @JsonProperty("refreshReason")
    public String refreshReason;  // e.g., "etag_based", "current_year_daily", "historical_immutable"

    @JsonProperty("etag")
    public String etag;  // HTTP ETag for conditional GET requests

    @JsonProperty("outputPath")
    public String outputPath;  // Path to materialized output file (avoids S3 exists checks)

    @JsonProperty("materializedAt")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long materializedAt;  // Timestamp when data was materialized to output format

    // API error tracking fields (for HTTP 200 responses with error content)
    @JsonProperty("lastError")
    public String lastError;  // Last API error code/message (e.g., "APIErrorCode 101: Unknown error")

    @JsonProperty("errorCount")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int errorCount;  // Number of consecutive API errors

    @JsonProperty("lastAttemptAt")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long lastAttemptAt;  // Timestamp of last download attempt

    @JsonProperty("downloadRetry")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long downloadRetry;  // Timestamp when retry should occur (0 = no retry restriction)
  }

  /**
   * Utility method to safely copy parameters map.
   *
   * @param parameters Parameters to copy
   * @return New HashMap with copied parameters
   */
  protected static Map<String, String> copyParameters(Map<String, String> parameters) {
    return new HashMap<>(parameters != null ? parameters : new HashMap<>());
  }

  // ===== Abstract methods that all cache manifests must implement =====

  /**
   * Check if data is cached and fresh for the given cache key.
   *
   * @param cacheKey The cache key identifying the data
   * @return true if cached and fresh, false otherwise
   */
  public abstract boolean isCached(CacheKey cacheKey);

  /**
   * Mark data as cached in the manifest with explicit refresh policy.
   *
   * @param cacheKey The cache key identifying the data
   * @param filePath Path to cached file
   * @param fileSize Size of cached file in bytes
   * @param refreshAfter Timestamp when this entry should be refreshed
   * @param refreshReason Human-readable reason for the refresh policy
   */
  public abstract void markCached(CacheKey cacheKey, String filePath, long fileSize,
      long refreshAfter, String refreshReason);

  /**
   * Save manifest to disk.
   *
   * @param directory Directory to save manifest in
   */
  public abstract void save(String directory);

  /**
   * Check if materialization has been completed for the given cache key.
   * This is the format-agnostic check for whether data has been written
   * to the target output format (Iceberg, Parquet, Delta, etc.).
   *
   * @param cacheKey The cache key identifying the data
   * @return true if materialized output exists and is current, false otherwise
   */
  public abstract boolean isMaterialized(CacheKey cacheKey);

  /**
   * Mark data as materialized in the manifest.
   *
   * @param cacheKey The cache key identifying the data
   * @param outputPath Path to materialized output file/table
   */
  public abstract void markMaterialized(CacheKey cacheKey, String outputPath);

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
  public abstract void markApiError(CacheKey cacheKey, String errorMessage, int retryAfterDays);
}
