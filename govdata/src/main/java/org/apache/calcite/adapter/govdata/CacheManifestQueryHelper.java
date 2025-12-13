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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache manifest query utility for efficient download list filtering.
 *
 * <p>Uses the DuckDB-backed AbstractCacheManifest to filter download requests,
 * returning only those that need to be downloaded or converted.
 *
 * <p>Usage Example:
 * <pre>{@code
 * // Generate all possible download combinations
 * List<DownloadRequest> allRequests = new ArrayList<>();
 * for (String series : seriesList) {
 *   for (int year = 2000; year <= 2024; year++) {
 *     Map<String, String> params = new HashMap<>();
 *     params.put("series", series);
 *     allRequests.add(new DownloadRequest("fred_indicators", params));
 *   }
 * }
 *
 * // Filter using cache manifest
 * List<DownloadRequest> needed = CacheManifestQueryHelper.filterUncachedRequestsOptimal(
 *     cacheManifest,
 *     allRequests,
 *     OperationType.DOWNLOAD
 * );
 *
 * // Execute only uncached downloads
 * for (DownloadRequest req : needed) {
 *   download(req);
 * }
 * }</pre>
 */
public class CacheManifestQueryHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheManifestQueryHelper.class);

  /**
   * Represents a download candidate (table + parameters).
   *
   * <p>This class encapsulates all information needed to identify a specific
   * download operation and build a cache key that matches the format used by
   * AbstractCacheManifest implementations.
   */
  public static class DownloadRequest {
    public final String dataType;
    public final Map<String, String> parameters;

    /**
     * Creates a download request.
     *
     * @param dataType Table/dataset name (e.g., "fred_indicators", "bea_regional_income")
     * @param parameters Dimension parameters (e.g., year, series, state, line_code)
     */
    public DownloadRequest(String dataType, Map<String, String> parameters) {
      this.dataType = dataType;
      this.parameters = new HashMap<>(parameters != null ? parameters : new HashMap<>());
    }

    /**
     * Builds cache key matching AbstractCacheManifest key format.
     * Format: dataType[:key1=value1][:key2=value2]...
     * Parameters are sorted alphabetically by key for consistency.
     *
     * @return Cache key string
     */
    public String buildKey() {
      StringBuilder key = new StringBuilder();
      key.append(dataType);
      if (!parameters.isEmpty()) {
        parameters.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> key.append(":").append(e.getKey())
                .append("=").append(e.getValue()));
      }

      return key.toString();
    }

    @Override public String toString() {
      return buildKey();
    }
  }

  /**
   * Filters download requests using the DuckDB-backed cache manifest.
   *
   * @param cacheManifest The cache manifest to check against
   * @param allRequests All possible download combinations to check
   * @param operationType Whether this is a download or conversion operation
   * @return Filtered list of requests that need downloading (not cached or expired)
   */
  public static List<DownloadRequest> filterUncachedRequestsOptimal(
      AbstractCacheManifest cacheManifest,
      List<DownloadRequest> allRequests,
      OperationType operationType) {

    if (allRequests == null || allRequests.isEmpty()) {
      LOGGER.debug("No download requests to filter");
      return new ArrayList<>();
    }

    if (cacheManifest == null) {
      LOGGER.debug("No cache manifest provided - treating all {} requests as uncached",
          allRequests.size());
      return new ArrayList<>(allRequests);
    }

    long startMs = System.currentTimeMillis();
    boolean isConversion = OperationType.CONVERSION.equals(operationType);
    List<DownloadRequest> result = new ArrayList<>();

    for (DownloadRequest req : allRequests) {
      CacheKey cacheKey = new CacheKey(req.dataType, req.parameters);

      // For conversions, check parquet conversion status
      if (isConversion) {
        // Only convert if source is cached AND parquet hasn't been converted
        // This prevents conversion attempts for entries that were never downloaded
        if (cacheManifest.isCached(cacheKey) && !cacheManifest.isParquetConverted(cacheKey)) {
          result.add(req);
        }
      } else {
        // For downloads, check if data is cached
        if (!cacheManifest.isCached(cacheKey)) {
          result.add(req);
        }
      }
    }

    long elapsedMs = System.currentTimeMillis() - startMs;
    LOGGER.debug("Cache manifest filtering: {} uncached out of {} total ({}ms)",
        result.size(), allRequests.size(), elapsedMs);

    return result;
  }
}