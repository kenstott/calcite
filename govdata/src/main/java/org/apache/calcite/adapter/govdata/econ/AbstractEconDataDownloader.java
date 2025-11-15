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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;
import org.apache.calcite.adapter.govdata.CacheKey;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for ECON data downloaders providing common infrastructure
 * for cache management, rate limiting, and download/conversion flow patterns.
 *
 * <p>Implements the Template Method pattern to enforce consistent cache manifest
 * usage across all economic data sources while allowing API-specific customization.
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
 * 2. Convert JSON to Parquet via API-specific implementation
 * 3. FileSchema's conversion registry automatically tracks the conversion
 * 4. No need to mark as converted - FileSchema handles this
 * </pre>
 *
 * <h3>Subclass Responsibilities</h3>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getTableName()} - Specify the primary table this downloader is associated with</li>
 *   <li>API-specific download methods using provided helper methods</li>
 *   <li>API-specific conversion methods using provided helper methods</li>
 * </ul>
 *
 * <p>Rate limiting is configured via schema metadata (download.rateLimit).
 * Subclasses must override {@link #getTableName()} to specify which table this
 * downloader is associated with. The table's download configuration contains
 * rate limit settings. If not specified, default values are used
 * (1000ms interval, 3 retries, 1000ms retry delay).</p>
 *
 * @see CacheManifest
 * @see StorageProvider
 */
public abstract class AbstractEconDataDownloader extends AbstractGovDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEconDataDownloader.class);

  /** Shared ObjectMapper for JSON serialization */
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Constructs base downloader with required infrastructure.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   */
  protected AbstractEconDataDownloader(String cacheDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDirectory, cacheDirectory, cacheDirectory, cacheStorageProvider, storageProvider, null);
  }

  /**
   * Constructs base downloader with separate raw cache and operating directories and shared cache manifest.
   * This constructor should be used when multiple downloaders share the same manifest to avoid stale cache issues.
   *
   * @param cacheDirectory Local directory for caching raw JSON data
   * @param operatingDirectory Directory for storing operational metadata (.aperio/<schema>/)
   * @param parquetDirectory Directory for storing parquet files (e.g., s3://govdata-parquet)
   * @param cacheStorageProvider Provider for raw cache file operations
   * @param storageProvider Provider for parquet file operations
   * @param sharedManifest Shared cache manifest (if null, will load from operatingDirectory)
   */
  public AbstractEconDataDownloader(String cacheDirectory, String operatingDirectory, String parquetDirectory,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider,
      CacheManifest sharedManifest) {
    super(cacheDirectory, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, "econ", sharedManifest);
  }

  /**
   * Default implementation does nothing.
   * Concrete downloaders should override if they have reference tables to download
   * (e.g., BLS JOLTS reference tables, BEA regional line codes, FRED catalog).
   *
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadReferenceData() throws IOException, InterruptedException {
    // Default: no reference data to download
  }

  /**
   * Default implementation does nothing.
   * Concrete downloaders must override to download their time-series data.
   *
   * @param startYear First year to download (inclusive)
   * @param endYear Last year to download (inclusive)
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadAll(int startYear, int endYear)
      throws IOException, InterruptedException {
    // Default: no data to download
  }

  /**
   * Default implementation does nothing.
   * Concrete downloaders must override to convert their data to Parquet.
   *
   * @param startYear First year to convert (inclusive)
   * @param endYear Last year to convert (inclusive)
   * @throws IOException If conversion or file I/O fails
   */
  @Override public void convertAll(int startYear, int endYear) throws IOException {
    // Default: no data to convert
  }

  /**
   * Saves downloaded JSON content to cache and updates manifest.
   * This is the final step in the download flow pattern.
   *
   * @param dataType Type of data being cached
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param relativePath Relative path within cache directory
   * @param jsonContent JSON content to save
   * @throws IOException If file write fails
   */
  protected final void saveToCache(String dataType, int year, Map<String, String> params,
      String relativePath, String jsonContent) throws IOException {

    // Save raw JSON data via cache storage provider
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    // Create cache key with year as a partition parameter
    Map<String, String> allParams = new HashMap<>(params != null ? params : new HashMap<>());
    allParams.put("year", String.valueOf(year));
    CacheKey cacheKey = new CacheKey(dataType, allParams);

    // Calculate reasonable default refresh time (same logic as CacheManifest.markCached)
    int currentYear = java.time.LocalDate.now().getYear();
    long refreshAfter;
    String refreshReason;
    if (year == currentYear) {
      refreshAfter = System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24);
      refreshReason = "current_year_daily";
    } else {
      refreshAfter = Long.MAX_VALUE;
      refreshReason = "historical_immutable";
    }

    // Mark as cached in manifest (operating metadata stays in .aperio via File API)
    cacheManifest.markCached(cacheKey, relativePath, jsonContent.length(), refreshAfter, refreshReason);
    cacheManifest.save(operatingDirectory);

    LOGGER.info("{} data saved to: {} ({} bytes)", dataType, relativePath, jsonContent.length());
  }

  /**
   * Loads table column metadata from the econ-schema.json resource file.
   * This enables metadata-driven schema generation, eliminating hardcoded type definitions.
   *
   * <p>NOTE: Consider using {@link AbstractGovDataDownloader#loadTableColumnsFromMetadata(String)} instead
   * for new code. That method is schema-agnostic and works for ECON, GEO, and SEC schemas
   * using the schemaResourceName from the instance.
   *
   * @param tableName The name of the table (must match "name" in econ-schema.json)
   * @return List of TableColumn definitions with type, nullability, and comments
   * @throws IllegalArgumentException if table not found or schema file cannot be loaded
   */
  protected static java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadTableColumns(String tableName) {
    try (java.io.InputStream schemaStream =
        AbstractEconDataDownloader.class.getResourceAsStream("/econ-schema.json")) {
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "econ-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "tables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid econ-schema.json: missing 'tables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Found the table - extract columns
          if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
            throw new IllegalArgumentException(
                "Table '" + tableName + "' has no 'columns' array in econ-schema.json");
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
          "Table '" + tableName + "' not found in econ-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

  /**
   * Check if parquet file has been converted, with defensive fallback to file existence and timestamp check.
   * This prevents unnecessary reconversion when the manifest is deleted but parquet files still exist.
   *
   * <p>This method supports optional partition parameters for tables with additional partitioning
   * beyond year (e.g., FRED indicators partitioned by series ID).
   *
   * @param rawFilePath Full path to raw source file (JSON)
   * @param parquetPath Full path to parquet file
   * @return true if parquet exists and is newer than raw file, false if conversion needed
   */
  protected final boolean isParquetConvertedOrExists(CacheKey cacheKey, String rawFilePath, String parquetPath) {

    // 1. Check manifest first - trust it as source of truth
    if (cacheManifest.isParquetConverted(cacheKey)) {
      return true;
    }

    // 2. Defensive check: if a parquet file exists but not in manifest, verify it's up-to-date
    try {
      if (storageProvider.exists(parquetPath)) {
        // Get timestamps for both files
        long parquetModTime = storageProvider.getMetadata(parquetPath).getLastModified();

        // Check if a raw file exists and compare timestamps
        if (cacheStorageProvider.exists(rawFilePath)) {
          long rawModTime = cacheStorageProvider.getMetadata(rawFilePath).getLastModified();

          if (parquetModTime > rawModTime) {
            // Parquet is newer than raw file - update manifest and skip conversion
            LOGGER.info("⚡ Parquet exists and is up-to-date, updating cache manifest: {}",
                cacheKey.asString());
            cacheManifest.markParquetConverted(cacheKey, parquetPath);
            cacheManifest.save(operatingDirectory);
            return true;
          } else {
            // Raw file is newer - needs reconversion
            LOGGER.info("Raw file is newer than parquet, will reconvert: {}", cacheKey.asString());
            return false;
          }
        } else {
          // No raw file exists - parquet is valid
          LOGGER.info("⚡ Parquet exists and raw file not found, updating cache manifest: {}",
              cacheKey.asString());
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
          cacheManifest.save(operatingDirectory);
          return true;
        }
      }
    } catch (java.io.IOException e) {
      LOGGER.debug("Error checking parquet file existence: {}", e.getMessage());
      // If we can't check, assume it doesn't exist
    }

    return false;
  }


  /**
   * Extracts an iteration list from econ-schema.json metadata for a given table.
   * This allows downloaders to configure themselves based on schema metadata.
   *
   * @param tableName Name of the table in econ-schema.json
   * @param listKey Key of the iteration list (e.g., "nipaTablesList", "lineCodesList", "keyIndustriesList")
   * @return List of iteration values, or empty list if not found
   */
  protected java.util.List<String> extractIterationList(String tableName, String listKey) {
    try {
      java.io.InputStream schemaStream = getClass().getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        LOGGER.warn("econ-schema.json not found, returning empty iteration list");
        return java.util.Collections.emptyList();
      }

      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the partitionedTables array (not tables array which contains views)
      com.fasterxml.jackson.databind.JsonNode partitionedTables = root.get("partitionedTables");
      if (partitionedTables != null && partitionedTables.isArray()) {
        for (com.fasterxml.jackson.databind.JsonNode table : partitionedTables) {
          com.fasterxml.jackson.databind.JsonNode nameNode = table.get("name");
          if (nameNode != null && tableName.equals(nameNode.asText())) {
            // Found the table, extract the download config
            com.fasterxml.jackson.databind.JsonNode download = table.get("download");
            if (download != null) {
              com.fasterxml.jackson.databind.JsonNode listNode = download.get(listKey);
              if (listNode != null && listNode.isArray()) {
                java.util.List<String> result = new java.util.ArrayList<>();
                for (com.fasterxml.jackson.databind.JsonNode item : listNode) {
                  result.add(item.asText());
                }
                LOGGER.debug("Extracted {} items from {} for table {}", result.size(), listKey, tableName);
                return result;
              }
            }
          }
        }
      }

      LOGGER.warn("Iteration list '{}' not found for table '{}', returning empty list", listKey, tableName);
      return java.util.Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Error extracting iteration list '{}' for table '{}': {}", listKey, tableName, e.getMessage());
      return java.util.Collections.emptyList();
    }
  }

}
