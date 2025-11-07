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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 *
 * <p>Requires a BEA API key from <a href="https://apps.bea.gov/api/signup/">...</a>
 */
public class BeaDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);
  private static final String BEA_API_BASE = "https://apps.bea.gov/api/data";

  private final String parquetDir;
  private final String apiKey;

  public BeaDataDownloader(String cacheDir, String parquetDir, String apiKey, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDir, cacheDir, parquetDir, apiKey, cacheStorageProvider, storageProvider, null);
  }

  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, String apiKey, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
    this.parquetDir = parquetDir;
    this.apiKey = apiKey;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir, String apiKey) {
    super(cacheDir, org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir), org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir));
    this.parquetDir = cacheDir; // For compatibility, use same dir
    this.apiKey = apiKey;
  }

  @Override protected String getTableName() {
    return "gdp_components";
  }

  /**
   * Gets the default start year from environment variables.
   */
  public static int getDefaultStartYear() {
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return Integer.parseInt(econStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }

    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return Integer.parseInt(govdataStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }

    return LocalDate.now().getYear() - 5;
  }

  /**
   * Gets the default end year from environment variables.
   */
  public static int getDefaultEndYear() {
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return Integer.parseInt(econEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }

    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return Integer.parseInt(govdataEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }

    return LocalDate.now().getYear();
  }

  // ===== METADATA-DRIVEN DOWNLOAD/CONVERSION METHODS =====
  // These methods use the download configurations from econ-schema.json
  // and the executeDownload() infrastructure from AbstractGovDataDownloader

  /**
   * Downloads GDP components data using metadata-driven pattern.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param nipaTablesList List of NIPA table IDs to download
   * @throws IOException if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  public void downloadGdpComponentsMetadata(int startYear, int endYear, java.util.List<String> nipaTablesList)
      throws IOException, InterruptedException {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for download");
      return;
    }

    LOGGER.info("Downloading {} NIPA tables for years {}-{}", nipaTablesList.size(), startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "gdp_components";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String nipaTable : nipaTablesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("TableName", nipaTable);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        // Check if already cached
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("TableName", nipaTable);

        if (isCachedOrExists(tableName, year, params, relativePath)) {
          skippedCount++;
          continue;
        }

        // Download via metadata-driven executeDownload()
        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} NIPA tables (skipped {} cached)", downloadedCount,
                nipaTablesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading NIPA table {} for year {}: {}", nipaTable, year, e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("GDP components download complete: downloaded {} table-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts GDP components data using metadata-driven pattern.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @param nipaTablesList List of NIPA table IDs to convert
   * @throws IOException if conversion or file operations fail
   */
  public void convertGdpComponentsMetadata(int startYear, int endYear, java.util.List<String> nipaTablesList)
      throws IOException {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for conversion");
      return;
    }

    LOGGER.info("Converting {} NIPA tables to Parquet for years {}-{}", nipaTablesList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "gdp_components";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String nipaTable : nipaTablesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("TableName", nipaTable);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        // Check if conversion needed
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("TableName", nipaTable);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        // Convert via metadata-driven approach
        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;

          if (convertedCount % 10 == 0) {
            LOGGER.info("Converted {}/{} tables (skipped {} up-to-date)", convertedCount,
                nipaTablesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error converting NIPA table {} for year {}: {}", nipaTable, year, e.getMessage());
        }
      }
    }

    LOGGER.info("GDP components conversion complete: converted {} table-years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads regional income data using metadata-driven pattern.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param lineCodesList List of line codes to download
   * @throws IOException if download or file operations fail
   * @throws InterruptedException if download is interrupted
   */
  public void downloadRegionalIncomeMetadata(int startYear, int endYear, java.util.List<String> lineCodesList)
      throws IOException, InterruptedException {
    if (lineCodesList == null || lineCodesList.isEmpty()) {
      LOGGER.warn("No line codes provided for download");
      return;
    }

    LOGGER.info("Downloading {} line codes for years {}-{}", lineCodesList.size(), startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "regional_income";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String lineCode : lineCodesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("LineCode", lineCode);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        // Check if already cached
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("LineCode", lineCode);

        if (isCachedOrExists(tableName, year, params, relativePath)) {
          skippedCount++;
          continue;
        }

        // Download via metadata-driven executeDownload()
        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} line codes (skipped {} cached)", downloadedCount,
                lineCodesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading line code {} for year {}: {}", lineCode, year, e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Regional income download complete: downloaded {} line-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts regional income data using metadata-driven pattern.
   */
  public void convertRegionalIncomeMetadata(int startYear, int endYear, java.util.List<String> lineCodesList)
      throws IOException {
    if (lineCodesList == null || lineCodesList.isEmpty()) {
      LOGGER.warn("No line codes provided for conversion");
      return;
    }

    LOGGER.info("Converting {} line codes to Parquet for years {}-{}", lineCodesList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "regional_income";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String lineCode : lineCodesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("LineCode", lineCode);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("LineCode", lineCode);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting line code {} for year {}: {}", lineCode, year, e.getMessage());
        }
      }
    }

    LOGGER.info("Regional income conversion complete: converted {} line-years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads trade statistics data using metadata-driven pattern.
   * This table has no iteration (single table T40205B).
   */
  public void downloadTradeStatisticsMetadata(int startYear, int endYear)
      throws IOException, InterruptedException {
    LOGGER.info("Downloading trade statistics for years {}-{}", startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      java.util.Map<String, String> variables = new java.util.HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "A");

      // Resolve path using pattern
      String relativePath = resolveJsonPath(pattern, variables);

      if (isCachedOrExists(tableName, year, new java.util.HashMap<>(), relativePath)) {
        skippedCount++;
        continue;
      }

      try {
        String cachedPath = executeDownload(tableName, variables);

        // Mark as downloaded in cache manifest
        try {
          String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
          long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
          cacheManifest.markCached(tableName, year, new java.util.HashMap<>(), relativePath, fileSize);
        } catch (Exception ex) {
          LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
        }

        downloadedCount++;
      } catch (Exception e) {
        LOGGER.error("Error downloading trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Trade statistics download complete: downloaded {} years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts trade statistics data using metadata-driven pattern.
   */
  public void convertTradeStatisticsMetadata(int startYear, int endYear) throws IOException {
    LOGGER.info("Converting trade statistics to Parquet for years {}-{}", startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      java.util.Map<String, String> variables = new java.util.HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "A");

      // Resolve paths using pattern
      String jsonPath = resolveJsonPath(pattern, variables);
      String parquetPath = resolveParquetPath(pattern, variables);
      String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
      String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

      if (isParquetConvertedOrExists(tableName, year, new java.util.HashMap<>(), rawPath, fullParquetPath)) {
        skippedCount++;
        continue;
      }

      try {
        convertCachedJsonToParquet(tableName, variables);
        cacheManifest.markParquetConverted(tableName, year, new java.util.HashMap<>(), fullParquetPath);
        convertedCount++;
      } catch (Exception e) {
        LOGGER.error("Error converting trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("Trade statistics conversion complete: converted {} years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads ITA data using metadata-driven pattern.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param itaIndicatorsList List of ITA indicator codes to download
   */
  public void downloadItaDataMetadata(int startYear, int endYear, java.util.List<String> itaIndicatorsList)
      throws IOException, InterruptedException {
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for download");
      return;
    }

    LOGGER.info("Downloading {} ITA indicators for years {}-{}", itaIndicatorsList.size(), startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Indicator", indicator);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("Indicator", indicator);

        if (isCachedOrExists(tableName, year, params, relativePath)) {
          skippedCount++;
          continue;
        }

        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} ITA indicators (skipped {} cached)", downloadedCount,
                itaIndicatorsList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading ITA indicator {} for year {}: {}", indicator, year, e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("ITA data download complete: downloaded {} indicator-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts ITA data using metadata-driven pattern.
   */
  public void convertItaDataMetadata(int startYear, int endYear, java.util.List<String> itaIndicatorsList)
      throws IOException {
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for conversion");
      return;
    }

    LOGGER.info("Converting {} ITA indicators to Parquet for years {}-{}", itaIndicatorsList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Indicator", indicator);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("Indicator", indicator);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting ITA indicator {} for year {}: {}", indicator, year, e.getMessage());
        }
      }
    }

    LOGGER.info("ITA data conversion complete: converted {} indicator-years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads industry GDP data using metadata-driven pattern.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param keyIndustriesList List of industry codes to download
   */
  public void downloadIndustryGdpMetadata(int startYear, int endYear, java.util.List<String> keyIndustriesList)
      throws IOException, InterruptedException {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for download");
      return;
    }

    LOGGER.info("Downloading {} industries for years {}-{}", keyIndustriesList.size(), startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("Industry", industry);

        if (isCachedOrExists(tableName, year, params, relativePath)) {
          skippedCount++;
          continue;
        }

        try {
          String cachedPath = executeDownload(tableName, variables);

          // Mark as downloaded in cache manifest
          try {
            String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
            long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
            cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
          } catch (Exception ex) {
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} industries (skipped {} cached)", downloadedCount,
                keyIndustriesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading industry {} for year {}: {}", industry, year, e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Industry GDP download complete: downloaded {} industry-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts industry GDP data using metadata-driven pattern.
   */
  public void convertIndustryGdpMetadata(int startYear, int endYear, java.util.List<String> keyIndustriesList)
      throws IOException {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for conversion");
      return;
    }

    LOGGER.info("Converting {} industries to Parquet for years {}-{}", keyIndustriesList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    java.util.Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        java.util.Map<String, String> variables = new java.util.HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("Industry", industry);

        if (isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        try {
          convertCachedJsonToParquet(tableName, variables);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
          convertedCount++;
        } catch (Exception e) {
          LOGGER.error("Error converting industry {} for year {}: {}", industry, year, e.getMessage());
        }
      }
    }

    LOGGER.info("Industry GDP conversion complete: converted {} industry-years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

}
