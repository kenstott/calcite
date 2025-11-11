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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Downloads and converts Federal Reserve Economic Data (FRED) to Parquet format.
 * Provides access to thousands of economic time series including interest rates,
 * GDP, monetary aggregates, and economic indicators.
 *
 * <p>Requires a FRED API key from
 * <a href="https://fred.stlouisfed.org/docs/api/api_key.html">...</a>
 */
public class FredDataDownloader extends AbstractEconDataDownloader {

  private static final ObjectMapper objectMapper =
      new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(FredDataDownloader.class);

  private final String fredApiKey;
  private final List<String> seriesIds;
  private final int fredMinPopularity;
  private final boolean fredCatalogForceRefresh;

  /**
   * Main constructor for FRED downloader.
   * Handles all FRED-specific initialization including loading catalog and building series list.
   *
   * @param fredApiKey              FRED API key
   * @param cacheDir                Cache directory for raw data
   * @param operatingDirectory      Operating directory for metadata
   * @param parquetDir              Parquet directory
   * @param cacheStorageProvider    Cache storage provider
   * @param storageProvider         Parquet storage provider
   * @param sharedManifest          Shared cache manifest
   * @param customFredSeries        Optional list of custom FRED series to add
   * @param fredMinPopularity       Minimum popularity threshold for catalog-based series
   * @param fredCatalogForceRefresh Whether to force refresh of catalog
   * @param catalogCacheTtlDays     Cache TTL for catalog series list (null for default 365 days)
   */
  public FredDataDownloader(String fredApiKey, String cacheDir, String operatingDirectory,
      String parquetDir, StorageProvider cacheStorageProvider,
      StorageProvider storageProvider,
      CacheManifest sharedManifest, List<String> customFredSeries,
      int fredMinPopularity, boolean fredCatalogForceRefresh, Integer catalogCacheTtlDays) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
    this.fredApiKey = fredApiKey;
    this.fredMinPopularity = fredMinPopularity;
    this.fredCatalogForceRefresh = fredCatalogForceRefresh;

    // Build series list from catalog + custom series
    this.seriesIds =
        buildSeriesList(customFredSeries, fredMinPopularity, catalogCacheTtlDays != null ?
            catalogCacheTtlDays : 365);
  }

  @Override protected String getTableName() {
    return "fred_indicators";
  }

  /**
   * Downloads FRED reference data (catalog of popular series).
   *
   * <p>Instantiates FredCatalogDownloader to download the reference_fred_series
   * catalog with cache checking and force refresh support.
   *
   * @throws java.io.IOException  If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadReferenceData() throws IOException, InterruptedException {
    if (fredApiKey == null || fredApiKey.isEmpty()) {
      LOGGER.warn("Skipping FRED catalog download - API key not available");
      return;
    }

    FredCatalogDownloader catalogDownloader =
        new FredCatalogDownloader(fredApiKey, cacheDirectory, parquetDirectory,
            storageProvider, (CacheManifest) cacheManifest);
    catalogDownloader.downloadCatalogIfNeeded(fredMinPopularity, fredCatalogForceRefresh);
  }

  /**
   * Downloads all FRED indicators data for the specified year range.
   * Uses the seriesIds list passed to the constructor.
   *
   * @param startYear First year to download
   * @param endYear   Last year to download
   * @throws java.io.IOException  if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  @Override public void downloadAll(int startYear, int endYear)
      throws IOException, InterruptedException {
    downloadAllSeries(startYear, endYear, this.seriesIds);
  }

  /**
   * Downloads FRED indicators data for the specified year range and series list.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to download
   * @param endYear   Last year to download
   * @param seriesIds List of FRED series IDs to download
   */
  private void downloadAllSeries(int startYear, int endYear, List<String> seriesIds) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for download");
      return;
    }

    LOGGER.info("Downloading {} FRED series for years {}-{}", seriesIds.size(), startYear, endYear);
    assert cacheManifest != null;

    String tableName = getTableName();

    // Build iteration dimensions: series x year
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("series", seriesIds));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Use iterateTableOperations() for automatic progress tracking and manifest management
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> isCachedOrExists(tableName, year, vars),
        (year, vars) -> {
          String cachedPath =
              cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, vars));
          StorageProvider.FileMetadata metadata =
              cacheStorageProvider.getMetadata(cachedPath);
          cacheManifest.markCached(tableName, year, vars,
              cachedPath, metadata.getSize());
        },
        "download");
  }

  /**
   * Converts all downloaded FRED indicator JSON files to Parquet format.
   * Uses the seriesIds list passed to the constructor.
   *
   * @param startYear First year to convert
   * @param endYear   Last year to convert
   */
  @Override public void convertAll(int startYear, int endYear) {
    convertAllSeries(startYear, endYear, this.seriesIds);
  }

  /**
   * Converts all downloaded FRED indicator JSON files to Parquet format.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to convert
   * @param endYear   Last year to convert
   * @param seriesIds List of FRED series IDs to convert
   */
  private void convertAllSeries(int startYear, int endYear, List<String> seriesIds) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for conversion");
      return;
    }

    LOGGER.info("Converting {} FRED series to Parquet for years {}-{}", seriesIds.size(),
        startYear, endYear);

    String tableName = getTableName();

    // Build iteration dimensions: series x year
    List<IterationDimension> dimensions = new java.util.ArrayList<>();
    dimensions.add(new IterationDimension("series", seriesIds));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Use iterateTableOperations() for automatic progress tracking and manifest management
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          Map<String, Object> metadata = loadTableMetadata();
          String pattern = (String) metadata.get("pattern");
          String parquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, vars));
          String rawPath =
              cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, vars));
          return isParquetConvertedOrExists(tableName, year, vars, rawPath, parquetPath);
        },
        (year, vars) -> {
          convertCachedJsonToParquet(tableName, vars);
          Map<String, Object> metadata = loadTableMetadata();
          String pattern = (String) metadata.get("pattern");
          String parquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, vars));
          cacheManifest.markParquetConverted(tableName, year, vars, parquetPath);
        },
        "conversion");
  }

  /**
   * Builds the complete FRED series list from catalog + custom series.
   * Handles catalog loading, caching, and merging with custom series.
   *
   * @param customFredSeries    Optional list of custom series to add
   * @param minPopularity       Minimum popularity threshold for catalog-based series
   * @param catalogCacheTtlDays Cache TTL in days
   * @return Complete list of series IDs to download
   */
  private List<String> buildSeriesList(List<String> customFredSeries,
      int minPopularity, int catalogCacheTtlDays) {
    LOGGER.info("Building FRED indicators series list from catalog (active series with popularity" +
            " >= {})",
        minPopularity);

    // Try to get from cache first (expensive catalog parsing operation)
    CacheManifest econManifest = (CacheManifest) cacheManifest;
    List<String> catalogSeries = econManifest.getCachedCatalogSeries(minPopularity);

    if (catalogSeries == null) {
      // Cache miss or expired - extract from catalog files
      LOGGER.info("Catalog series cache miss/expired - extracting from catalog files");
      catalogSeries = extractActivePopularSeriesFromCatalog(minPopularity);

      // Cache the results
      econManifest.cacheCatalogSeries(minPopularity, catalogSeries, catalogCacheTtlDays);
      LOGGER.info("Cached {} series IDs for {} days", catalogSeries.size(), catalogCacheTtlDays);
    }

    LOGGER.info("Found {} active popular series in FRED catalog", catalogSeries.size());

    // Merge with customFredSeries if provided
    Set<String> allSeriesIds = new LinkedHashSet<>(catalogSeries);
    if (customFredSeries != null && !customFredSeries.isEmpty()) {
      allSeriesIds.addAll(customFredSeries);
      LOGGER.info("Added {} custom FRED series, total series count: {}",
          customFredSeries.size(), allSeriesIds.size());
    }

    return new ArrayList<>(allSeriesIds);
  }

  /**
   * Extract series IDs from FRED catalog JSON files filtered by active status and popularity.
   * Reads all catalog JSON files under status=active partitions and filters by popularity
   * threshold.
   *
   * @param minPopularity Minimum popularity threshold (series with popularity >= this value are
   *                      included)
   * @return List of series IDs that are active and meet the popularity threshold
   */
  private List<String> extractActivePopularSeriesFromCatalog(int minPopularity) {
    Set<String> seriesIds = new LinkedHashSet<>();

    try {
      // Find all catalog JSON files under status=active partitions
      // Pattern: type=reference/category=*/frequency=*/source=*/status=active
      // /reference_fred_series.json
      String catalogBasePath = cacheStorageProvider.resolvePath(cacheDirectory, "type=reference");

      if (!cacheStorageProvider.isDirectory(catalogBasePath)) {
        LOGGER.warn("FRED catalog cache not found at: {}", catalogBasePath);
        return new ArrayList<>(seriesIds);
      }

      // Recursively find all reference_fred_series.json files under status=active directories
      List<String> catalogFiles = findCatalogFilesInActivePartitions(catalogBasePath);

      LOGGER.info("Found {} catalog JSON files to process", catalogFiles.size());

      // Process each catalog file
      int totalSeriesProcessed = 0;
      int seriesPassingFilter = 0;

      for (String catalogFile : catalogFiles) {
        try {
          // Read JSON file as array of series objects using InputStream
          List<Map<String, Object>> seriesList;
          try (InputStream inputStream = cacheStorageProvider.openInputStream(catalogFile);
               InputStreamReader reader =
                   new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            TypeReference<List<Map<String, Object>>> typeRef =
                new TypeReference<List<Map<String, Object>>>() {
                };
            seriesList = objectMapper.readValue(reader, typeRef);
          }

          // Filter by popularity and extract series IDs
          for (java.util.Map<String, Object> series : seriesList) {
            totalSeriesProcessed++;

            // Extract series_id (maybe under "id" or "series_id" key)
            String seriesId = (String) series.get("id");
            if (seriesId == null) {
              seriesId = (String) series.get("series_id");
            }

            // Extract popularity (default to 0 if not present)
            Object popularityObj = series.get("popularity");
            int popularity = 0;
            if (popularityObj != null) {
              if (popularityObj instanceof Integer) {
                popularity = (Integer) popularityObj;
              } else if (popularityObj instanceof Number) {
                popularity = ((Number) popularityObj).intValue();
              }
            }

            // Filter by popularity threshold
            if (seriesId != null && popularity >= minPopularity) {
              seriesIds.add(seriesId);
              seriesPassingFilter++;
            }
          }

        } catch (Exception e) {
          LOGGER.warn("Error processing catalog file {}: {}", catalogFile, e.getMessage());
        }
      }

      LOGGER.info("Extracted {} series IDs from catalog (processed {} total series, {} passed " +
              "filter with popularity >= {})",
          seriesIds.size(), totalSeriesProcessed, seriesPassingFilter, minPopularity);

    } catch (Exception e) {
      LOGGER.error("Error extracting series from FRED catalog", e);
    }

    return new java.util.ArrayList<>(seriesIds);
  }

  /**
   * Recursively find all reference_fred_series.json files under status=active partitions.
   *
   * @param basePath Base path to search from (type=reference directory)
   * @return List of full paths to catalog JSON files in active partitions
   */
  private List<String> findCatalogFilesInActivePartitions(String basePath) {
    List<String> catalogFiles = new ArrayList<>();

    try {
      // Use a simple recursive search for directories containing "status=active"
      // and collect all reference_fred_series.json files within them
      findCatalogFilesRecursive(basePath, catalogFiles);
    } catch (Exception e) {
      LOGGER.error("Error finding catalog files in active partitions", e);
    }

    return catalogFiles;
  }

  /**
   * Recursive helper to find catalog JSON files under status=active directories.
   */
  private void findCatalogFilesRecursive(String currentPath, List<String> catalogFiles)
      throws IOException {

    List<StorageProvider.FileEntry> entries =
        cacheStorageProvider.listFiles(currentPath, false);

    for (StorageProvider.FileEntry entry : entries) {
      String fullPath = entry.getPath();

      if (entry.isDirectory()) {
        // Extract directory name from path, handling trailing slash
        String pathForParsing = fullPath.endsWith("/") ?
            fullPath.substring(0, fullPath.length() - 1) : fullPath;
        String entryName = pathForParsing.substring(pathForParsing.lastIndexOf('/') + 1);

        // If this is a status=active directory, look for catalog JSON file
        if (entryName.equals("status=active")) {
          String catalogFile = cacheStorageProvider.resolvePath(fullPath, "reference_fred_series" +
              ".json");
          if (cacheStorageProvider.exists(catalogFile)) {
            catalogFiles.add(catalogFile);
            LOGGER.debug("Found catalog file: {}", catalogFile);
          }
        } else {
          // Recurse into subdirectories
          findCatalogFilesRecursive(fullPath, catalogFiles);
        }
      }
    }
  }
}
