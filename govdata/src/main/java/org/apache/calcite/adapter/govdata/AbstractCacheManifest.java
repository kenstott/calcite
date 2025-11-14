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
package org.apache.calcite.adapter.govdata;

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
 *   <li>Parquet conversion tracking to avoid redundant S3 exists checks</li>
 * </ul>
 *
 * <p>Subclasses must provide schema-specific key building logic and
 * may add additional fields to their cache entry classes.
 */
public abstract class AbstractCacheManifest {

  /**
   * Base cache entry class with fields common to all govdata schemas.
   * Subclasses extend this to add schema-specific fields.
   */
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

    @JsonProperty("parquetPath")
    public String parquetPath;  // Path to converted parquet file (avoids S3 exists checks)

    @JsonProperty("parquetConvertedAt")
    public long parquetConvertedAt;  // Timestamp when parquet was created

    // API error tracking fields (for HTTP 200 responses with error content)
    @JsonProperty("lastError")
    public String lastError;  // Last API error code/message (e.g., "APIErrorCode 101: Unknown error")

    @JsonProperty("errorCount")
    public int errorCount;  // Number of consecutive API errors

    @JsonProperty("lastAttemptAt")
    public long lastAttemptAt;  // Timestamp of last download attempt

    @JsonProperty("downloadRetry")
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
   * Check if parquet conversion has been completed for the given cache key.
   *
   * @param cacheKey The cache key identifying the data
   * @return true if parquet exists and is current, false otherwise
   */
  public abstract boolean isParquetConverted(CacheKey cacheKey);

  /**
   * Mark parquet file as converted in the manifest.
   *
   * @param cacheKey The cache key identifying the data
   * @param parquetPath Path to parquet file
   */
  public abstract void markParquetConverted(CacheKey cacheKey, String parquetPath);

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
