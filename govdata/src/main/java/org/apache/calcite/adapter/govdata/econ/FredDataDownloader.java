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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Downloads and converts Federal Reserve Economic Data (FRED) to Parquet format.
 * Provides access to thousands of economic time series including interest rates,
 * GDP, monetary aggregates, and economic indicators.
 *
 * <p>Requires a FRED API key from <a href="https://fred.stlouisfed.org/docs/api/api_key.html">...</a>
 */
public class FredDataDownloader extends AbstractEconDataDownloader {

  private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FredDataDownloader.class);

  private final String fredApiKey;
  private final java.util.List<String> seriesIds;
  private final int fredMinPopularity;
  private final boolean fredCatalogForceRefresh;

  /**
   * Main constructor for FRED downloader.
   * Handles all FRED-specific initialization including loading catalog and building series list.
   *
   * @param fredApiKey FRED API key
   * @param cacheDir Cache directory for raw data
   * @param operatingDirectory Operating directory for metadata
   * @param parquetDir Parquet directory
   * @param cacheStorageProvider Cache storage provider
   * @param storageProvider Parquet storage provider
   * @param sharedManifest Shared cache manifest
   * @param customFredSeries Optional list of custom FRED series to add
   * @param fredMinPopularity Minimum popularity threshold for catalog-based series
   * @param fredCatalogForceRefresh Whether to force refresh of catalog
   * @param catalogCacheTtlDays Cache TTL for catalog series list (null for default 365 days)
   */
  public FredDataDownloader(String fredApiKey, String cacheDir, String operatingDirectory,
      String parquetDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider,
      CacheManifest sharedManifest, java.util.List<String> customFredSeries,
      int fredMinPopularity, boolean fredCatalogForceRefresh, Integer catalogCacheTtlDays) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
    this.fredApiKey = fredApiKey;
    this.fredMinPopularity = fredMinPopularity;
    this.fredCatalogForceRefresh = fredCatalogForceRefresh;

    // Build series list from catalog + custom series
    this.seriesIds = buildSeriesList(customFredSeries, fredMinPopularity,
        catalogCacheTtlDays != null ? catalogCacheTtlDays : 365);
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
   * @throws java.io.IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadReferenceData() throws java.io.IOException, InterruptedException {
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
   * @param endYear Last year to download
   * @throws java.io.IOException if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  @Override
  public void downloadAll(int startYear, int endYear)
      throws java.io.IOException, InterruptedException {
    downloadAllSeries(startYear, endYear, this.seriesIds);
  }

  /**
   * Downloads FRED indicators data for the specified year range and series list.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param seriesIds List of FRED series IDs to download
   */
  private void downloadAllSeries(int startYear, int endYear, java.util.List<String> seriesIds) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for download");
      return;
    }

    LOGGER.info("Downloading {} FRED series for years {}-{}", seriesIds.size(), startYear, endYear);
    assert cacheManifest != null;

    // Download each series for each year using metadata-driven approach
    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = getTableName();

    for (String seriesId : seriesIds) {
      for (int year = startYear; year <= endYear; year++) {

        Map<String, String> variables =
            ImmutableMap.of("year", String.valueOf(year),
            "series", seriesId);

        if (isCachedOrExists("fred_indicators", year, variables)) {
          skippedCount++;
          continue;
        }

        // Download via metadata-driven executeDownload()
        try {
          String cachedPath = cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, variables));
          StorageProvider.FileMetadata metadata = cacheStorageProvider.getMetadata(cachedPath);

          // Mark as downloaded in cache manifest using the actual path returned
          cacheManifest.markCached("fred_indicators", year, variables, cachedPath, metadata.getSize());
          cacheManifest.save(operatingDirectory);

          downloadedCount++;

          if (downloadedCount % 100 == 0) {
            LOGGER.info("Downloaded {}/{} series (skipped {} cached)", downloadedCount, seriesIds.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading FRED series {} for year {}: {}", seriesId, year, e.getMessage());
          // Continue with next series
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("FRED download complete: downloaded {} series-years, skipped {} (cached)", downloadedCount, skippedCount);
  }

  /**
   * Converts all downloaded FRED indicator JSON files to Parquet format.
   * Uses the seriesIds list passed to the constructor.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  @Override
  public void convertAll(int startYear, int endYear) {
    convertAllSeries(startYear, endYear, this.seriesIds);
  }

  /**
   * Converts all downloaded FRED indicator JSON files to Parquet format.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @param seriesIds List of FRED series IDs to convert
   */
  private void convertAllSeries(int startYear, int endYear, java.util.List<String> seriesIds) {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for conversion");
      return;
    }

    LOGGER.info("Converting {} FRED series to Parquet for years {}-{}", seriesIds.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = getTableName();
    Map<String, Object> metadata = loadTableMetadata();
    String pattern = (String) metadata.get("pattern");

    for (String seriesId : seriesIds) {
      for (int year = startYear; year <= endYear; year++) {
        // Build paths for this series/year
        Map<String, String> variables =
            ImmutableMap.of("year", String.valueOf(year),
            "series", seriesId);
        String parquetPath = storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));

        if (isParquetConvertedOrExists(tableName, year, variables, rawPath, parquetPath)) {
          skippedCount++;
          continue;
        }

        // Convert via metadata-driven approach
        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, variables, parquetPath);
          cacheManifest.save(operatingDirectory);
          convertedCount++;

          if (convertedCount % 100 == 0) {
            LOGGER.info("Converted {}/{} series (skipped {} up-to-date)", convertedCount, seriesIds.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error converting FRED series {} for year {}: {}", seriesId, year, e.getMessage());
          // Continue with next series
        }
      }
    }

    LOGGER.info("FRED conversion complete: converted {} series-years, skipped {} (up-to-date)", convertedCount, skippedCount);
  }

  /**
   * Builds the complete FRED series list from catalog + custom series.
   * Handles catalog loading, caching, and merging with custom series.
   *
   * @param customFredSeries Optional list of custom series to add
   * @param minPopularity Minimum popularity threshold for catalog-based series
   * @param catalogCacheTtlDays Cache TTL in days
   * @return Complete list of series IDs to download
   */
  private java.util.List<String> buildSeriesList(java.util.List<String> customFredSeries,
      int minPopularity, int catalogCacheTtlDays) {
    LOGGER.info("Building FRED indicators series list from catalog (active series with popularity >= {})",
        minPopularity);

    // Try to get from cache first (expensive catalog parsing operation)
    CacheManifest econManifest = (CacheManifest) cacheManifest;
    java.util.List<String> catalogSeries = econManifest.getCachedCatalogSeries(minPopularity);

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
    java.util.Set<String> allSeriesIds = new java.util.LinkedHashSet<>(catalogSeries);
    if (customFredSeries != null && !customFredSeries.isEmpty()) {
      allSeriesIds.addAll(customFredSeries);
      LOGGER.info("Added {} custom FRED series, total series count: {}",
          customFredSeries.size(), allSeriesIds.size());
    }

    return new java.util.ArrayList<>(allSeriesIds);
  }

  /**
   * Extract series IDs from FRED catalog JSON files filtered by active status and popularity.
   * Reads all catalog JSON files under status=active partitions and filters by popularity threshold.
   *
   * @param minPopularity Minimum popularity threshold (series with popularity >= this value are included)
   * @return List of series IDs that are active and meet the popularity threshold
   */
  private java.util.List<String> extractActivePopularSeriesFromCatalog(int minPopularity) {
    java.util.Set<String> seriesIds = new java.util.LinkedHashSet<>();

    try {
      // Find all catalog JSON files under status=active partitions
      // Pattern: type=reference/category=*/frequency=*/source=*/status=active/reference_fred_series.json
      String catalogBasePath = cacheStorageProvider.resolvePath(cacheDirectory, "type=reference");

      if (!cacheStorageProvider.isDirectory(catalogBasePath)) {
        LOGGER.warn("FRED catalog cache not found at: {}", catalogBasePath);
        return new java.util.ArrayList<>(seriesIds);
      }

      // Recursively find all reference_fred_series.json files under status=active directories
      java.util.List<String> catalogFiles = findCatalogFilesInActivePartitions(catalogBasePath);

      LOGGER.info("Found {} catalog JSON files to process", catalogFiles.size());

      // Process each catalog file
      int totalSeriesProcessed = 0;
      int seriesPassingFilter = 0;

      for (String catalogFile : catalogFiles) {
        try {
          // Read JSON file as array of series objects using InputStream
          java.util.List<java.util.Map<String, Object>> seriesList;
          try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(catalogFile);
               java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream,
                   java.nio.charset.StandardCharsets.UTF_8)) {
            com.fasterxml.jackson.core.type.TypeReference<java.util.List<java.util.Map<String, Object>>> typeRef =
                new com.fasterxml.jackson.core.type.TypeReference<java.util.List<java.util.Map<String, Object>>>() {};
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

      LOGGER.info("Extracted {} series IDs from catalog (processed {} total series, {} passed filter with popularity >= {})",
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
  private java.util.List<String> findCatalogFilesInActivePartitions(String basePath) {
    java.util.List<String> catalogFiles = new java.util.ArrayList<>();

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
  private void findCatalogFilesRecursive(String currentPath, java.util.List<String> catalogFiles)
      throws java.io.IOException {

    java.util.List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> entries =
        cacheStorageProvider.listFiles(currentPath, false);

    for (org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry entry : entries) {
      String fullPath = entry.getPath();

      if (entry.isDirectory()) {
        // Extract directory name from path, handling trailing slash
        String pathForParsing = fullPath.endsWith("/") ?
            fullPath.substring(0, fullPath.length() - 1) : fullPath;
        String entryName = pathForParsing.substring(pathForParsing.lastIndexOf('/') + 1);

        // If this is a status=active directory, look for catalog JSON file
        if (entryName.equals("status=active")) {
          String catalogFile = cacheStorageProvider.resolvePath(fullPath, "reference_fred_series.json");
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
