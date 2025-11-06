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

/**
 * Downloads and converts Federal Reserve Economic Data (FRED) to Parquet format.
 * Provides access to thousands of economic time series including interest rates,
 * GDP, monetary aggregates, and economic indicators.
 *
 * <p>Requires a FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html
 */
public class FredDataDownloader extends AbstractEconDataDownloader {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FredDataDownloader.class);

  public FredDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
  }

  @Override protected String getTableName() {
    return "fred_indicators";
  }

  /**
   * Downloads FRED indicators data for the specified year range and series list.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param seriesIds List of FRED series IDs to download
   * @throws java.io.IOException if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  public void downloadAll(int startYear, int endYear, java.util.List<String> seriesIds)
      throws java.io.IOException, InterruptedException {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for download");
      return;
    }

    LOGGER.info("Downloading {} FRED series for years {}-{}", seriesIds.size(), startYear, endYear);

    // Download each series for each year using metadata-driven approach
    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = getTableName();

    for (String seriesId : seriesIds) {
      for (int year = startYear; year <= endYear; year++) {
        // Build relative path for this series/year combination
        String relativePath = buildPartitionPath(tableName, year) + "/series=" + seriesId + "/" + tableName + ".json";

        // Check if already cached
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("series", seriesId);

        if (isCachedOrExists(tableName, year, params, relativePath)) {
          skippedCount++;
          continue;
        }

        // Download via metadata-driven executeDownload()
        try {
          java.util.Map<String, String> variables = new java.util.HashMap<>();
          variables.put("year", String.valueOf(year));
          variables.put("series_id", seriesId);

          String cachedPath = executeDownload(tableName, variables);
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

    LOGGER.info("FRED download complete: downloaded {} series-years, skipped {} (cached)", downloadedCount, skippedCount);
  }

  /**
   * Converts all downloaded FRED indicator JSON files to Parquet format.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @param seriesIds List of FRED series IDs to convert
   * @throws java.io.IOException if conversion or file operations fail
   */
  public void convertAll(int startYear, int endYear, java.util.List<String> seriesIds)
      throws java.io.IOException {
    if (seriesIds == null || seriesIds.isEmpty()) {
      LOGGER.warn("No FRED series IDs provided for conversion");
      return;
    }

    LOGGER.info("Converting {} FRED series to Parquet for years {}-{}", seriesIds.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = getTableName();

    for (String seriesId : seriesIds) {
      for (int year = startYear; year <= endYear; year++) {
        // Build paths for this series/year
        String seriesPartition = "type=" + tableName + "/series=" + seriesId + "/year=" + year;
        String parquetPath = storageProvider.resolvePath(parquetDirectory, seriesPartition + "/" + tableName + ".parquet");
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, seriesPartition + "/" + tableName + ".json");

        // Check if conversion needed
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("series", seriesId);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, parquetPath)) {
          skippedCount++;
          continue;
        }

        // Convert via metadata-driven approach
        try {
          java.util.Map<String, String> variables = new java.util.HashMap<>();
          variables.put("year", String.valueOf(year));
          variables.put("series_id", seriesId);
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, parquetPath);
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
   * Check if parquet file has been converted, with defensive fallback to file existence and timestamp check.
   * This prevents unnecessary reconversion when the manifest is deleted but parquet files still exist.
   *
   * @param dataType Type of data (e.g., "fred_indicators")
   * @param year Year of data
   * @param params Additional partition parameters (e.g., series ID)
   * @param rawFilePath Full path to raw source file (JSON)
   * @param parquetPath Full path to parquet file
   * @return true if parquet exists and is newer than raw file, false if conversion needed
   */
  private boolean isParquetConvertedOrExists(String dataType, int year,
      java.util.Map<String, String> params, String rawFilePath, String parquetPath) {

    // 1. Check manifest first - trust it as source of truth
    if (cacheManifest.isParquetConverted(dataType, year, params)) {
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
            LOGGER.info("⚡ Parquet exists and is up-to-date, updating cache manifest: {} (year={}, params={})",
                dataType, year, params);
            cacheManifest.markParquetConverted(dataType, year, params, parquetPath);
            cacheManifest.save(operatingDirectory);
            return true;
          } else {
            // Raw file is newer - needs reconversion
            LOGGER.info("Raw file is newer than parquet, will reconvert: {} (year={})", dataType, year);
            return false;
          }
        } else {
          // No raw file exists - parquet is valid
          LOGGER.info("⚡ Parquet exists and raw file not found, updating cache manifest: {} (year={})",
              dataType, year);
          cacheManifest.markParquetConverted(dataType, year, params, parquetPath);
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

}
