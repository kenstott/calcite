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
   * Check if data is cached and fresh for the given parameters.
   *
   * @param dataType Type of data
   * @param year Year of data
   * @param parameters Additional parameters
   * @return true if cached and fresh, false otherwise
   */
  public abstract boolean isCached(String dataType, int year, Map<String, String> parameters);

  /**
   * Mark data as cached in the manifest.
   *
   * @param dataType Type of data
   * @param year Year of data
   * @param params Additional parameters
   * @param relativePath Relative path to cached file
   * @param fileSize Size of cached file in bytes
   */
  public abstract void markCached(String dataType, int year, Map<String, String> params,
      String relativePath, long fileSize);

  /**
   * Save manifest to disk.
   *
   * @param directory Directory to save manifest in
   */
  public abstract void save(String directory);

  /**
   * Check if parquet conversion has been completed for the given parameters.
   *
   * @param dataType Type of data
   * @param year Year of data
   * @param params Additional parameters
   * @return true if parquet exists and is current, false otherwise
   */
  public abstract boolean isParquetConverted(String dataType, int year, Map<String, String> params);

  /**
   * Mark parquet file as converted in the manifest.
   *
   * @param dataType Type of data
   * @param year Year of data
   * @param params Additional parameters
   * @param parquetPath Path to parquet file
   */
  public abstract void markParquetConverted(String dataType, int year, Map<String, String> params,
      String parquetPath);
}
