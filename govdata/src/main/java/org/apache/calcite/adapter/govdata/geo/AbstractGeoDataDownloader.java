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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract base class for GEO data downloaders providing common infrastructure
 * for cache management, rate limiting, and download/conversion flow patterns.
 *
 * <p>Implements the Template Method pattern to enforce consistent cache manifest
 * usage across all geographic data sources while allowing API-specific customization.
 *
 * <h3>Standard Flow Pattern</h3>
 * <pre>
 * Download Flow:
 * 1. Check manifest.isCached() → if true, skip download
 * 2. Check local file exists (defensive) → update manifest if found
 * 3. Download data via API-specific implementation
 * 4. Save to local cache directory
 * 5. Mark as cached in manifest
 *
 * Conversion Flow:
 * 1. Check storageProvider.exists(targetPath) → if true, skip conversion
 * 2. Convert shapefile to Parquet via API-specific implementation
 * 3. FileSchema's conversion registry automatically tracks the conversion
 * 4. No need to mark as converted - FileSchema handles this
 * </pre>
 *
 * <h3>Subclass Responsibilities</h3>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getMinRequestIntervalMs()} - API-specific rate limit</li>
 *   <li>{@link #getMaxRetries()} - Retry policy for failed requests</li>
 *   <li>{@link #getRetryDelayMs()} - Initial delay for retry backoff</li>
 *   <li>API-specific download methods using provided helper methods</li>
 *   <li>API-specific conversion methods using provided helper methods</li>
 * </ul>
 *
 * @see GeoCacheManifest
 * @see StorageProvider
 */
public abstract class AbstractGeoDataDownloader extends AbstractGovDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoDataDownloader.class);

  /** Shared ObjectMapper for JSON serialization in static methods */
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Constructs base downloader with required infrastructure.
   *
   * @param cacheDirectory Local directory for caching raw data
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   */
  protected AbstractGeoDataDownloader(String cacheDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDirectory, cacheDirectory, cacheDirectory, cacheStorageProvider, storageProvider, null);
  }

  /**
   * Constructs base downloader with separate raw cache and operating directories and shared cache manifest.
   * This constructor should be used when multiple downloaders share the same manifest to avoid stale cache issues.
   *
   * @param cacheDirectory Local directory for caching raw data
   * @param operatingDirectory Directory for storing operational metadata (.aperio/<schema>/)
   * @param parquetDirectory Directory for storing parquet files (e.g., s3://govdata-parquet)
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   * @param sharedManifest Shared cache manifest (if null, will load from operatingDirectory)
   */
  protected AbstractGeoDataDownloader(String cacheDirectory, String operatingDirectory, String parquetDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider, GeoCacheManifest sharedManifest) {
    super(cacheDirectory, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, "geo", sharedManifest);
  }

  /**
   * Default implementation does nothing.
   * Concrete downloaders should override if they have reference tables to download
   * (e.g., TIGER/Line metadata, Census geography hierarchies).
   *
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadReferenceData() throws IOException, InterruptedException {
    // Default: no reference data to download
  }

  /**
   * Default implementation does nothing.
   * Concrete GEO downloaders should override if they need to download time-series data.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadAll(int startYear, int endYear)
      throws IOException, InterruptedException {
    // Default: no time-series data to download
  }

  /**
   * Default implementation does nothing.
   * Concrete GEO downloaders should override if they need to convert time-series data.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @throws IOException If conversion or file I/O fails
   */
  @Override public void convertAll(int startYear, int endYear) throws IOException {
    // Default: no time-series data to convert
  }

  // Rate limiting methods - subclasses can override to provide API-specific limits
  // If not overridden, defaults from AbstractGovDataDownloader are used

  @Override protected long getMinRequestIntervalMs() {
    return 500; // Default for GEO APIs: 500ms between requests
  }

  @Override protected int getMaxRetries() {
    return 3; // Default: 3 retry attempts
  }

  @Override protected long getRetryDelayMs() {
    return 1000; // Default: 1 second initial retry delay
  }

  /**
   * Saves downloaded content to cache and updates manifest.
   * This is the final step in the download flow pattern.
   *
   * @param dataType Type of data being cached
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param relativePath Relative path within cache directory
   * @param content Content to save (raw bytes)
   * @throws IOException If file write fails
   */
  protected final void saveToCache(String dataType, int year, Map<String, String> params,
      String relativePath, byte[] content) throws IOException {

    // Save raw data via cache storage provider
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, content);

    // Mark as cached in manifest (operating metadata stays in .aperio via File API)
    cacheManifest.markCached(dataType, year, params, relativePath, content.length);
    cacheManifest.save(operatingDirectory);

    LOGGER.info("{} data saved to: {} ({} bytes)", dataType, relativePath, content.length);
  }

  /**
   * Extracts all Hive-style partition parameters from path.
   * For example, from "type=boundaries/year=2020/state=CA/file.parquet"
   * extracts {"state": "CA"} (year is handled separately).
   *
   * @param path File path with Hive partitions
   * @return Map of partition key-value pairs (excluding year, type, and source)
   */
  protected final Map<String, String> extractPartitionParams(String path) {
    Map<String, String> params = new java.util.HashMap<>();

    // Match all key=value patterns in the path
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([^/=]+)=([^/]+)");
    java.util.regex.Matcher matcher = pattern.matcher(path);

    while (matcher.find()) {
      String key = matcher.group(1);
      String value = matcher.group(2);

      // Skip organizational partitions: year (handled separately), type (added by caller), source (organizational only)
      if (!"year".equals(key) && !"type".equals(key) && !"source".equals(key)) {
        params.put(key, value);
      }
    }

    return params;
  }

  /**
   * Extracts year from Hive-style partitioned path (e.g., "year=2020").
   *
   * @param path File path containing year partition
   * @return Extracted year
   */
  protected final int extractYearFromPath(String path) {
    // Look for year=YYYY pattern in path
    int yearIndex = path.indexOf("year=");
    if (yearIndex != -1) {
      String yearStr = path.substring(yearIndex + 5);
      // Extract 4-digit year
      int endIndex = yearStr.indexOf('/');
      if (endIndex != -1) {
        yearStr = yearStr.substring(0, endIndex);
      } else {
        // Check for file extension
        endIndex = yearStr.indexOf('.');
        if (endIndex != -1) {
          yearStr = yearStr.substring(0, endIndex);
        }
      }
      return Integer.parseInt(yearStr);
    }
    throw new IllegalArgumentException("Path does not contain year partition: " + path);
  }

  /**
   * Loads column metadata for a table from geo-schema.json.
   *
   * @param tableName The name of the table to load column metadata for
   * @return List of table column definitions
   * @throws IllegalArgumentException if the table is not found or schema is invalid
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try {
      // Load geo-schema.json from resources
      java.io.InputStream schemaStream =
          AbstractGeoDataDownloader.class.getResourceAsStream("/geo-schema.json");
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "geo-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "partitionedTables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid geo-schema.json: missing 'partitionedTables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in geo-schema.json");
          }

          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
              columns = new java.util.ArrayList<>();

          for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("columns")) {
            String colName = colNode.has("name") ? colNode.get("name").asText() : null;
            String colType = colNode.has("type") ? colNode.get("type").asText() : "string";
            boolean nullable = colNode.has("nullable") && colNode.get("nullable").asBoolean();
            String comment = colNode.has("comment") ? colNode.get("comment").asText() : "";

            if (colName != null) {
              columns.add(
                  new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                  colName, colType, nullable, comment));
            }
          }

          return columns;
        }
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in geo-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

}
