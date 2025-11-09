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
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 *
 * <p>Requires a BEA API key from <a href="https://apps.bea.gov/api/signup/">...</a>
 */
public class BeaDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);

  private final String parquetDir;
  private final List<String> nipaTablesList;
  private final Map<String, java.util.Set<String>> tableFrequencies;
  private final List<String> keyIndustriesList;

  /**
   * Simple constructor without shared manifest (creates one from operatingDirectory).
   */
  public BeaDataDownloader(String cacheDir, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDir, cacheDir, parquetDir, cacheStorageProvider, storageProvider, null);
  }

  /**
   * Main constructor for BEA downloader.
   * Handles all BEA-specific initialization including loading catalogs and patching table lists.
   */
  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
    this.parquetDir = parquetDir;

    // Extract iteration lists from schema metadata
    List<String> nipaTablesListFromSchema = extractIterationList("national_accounts", "nipaTablesList");
    List<String> keyIndustriesListFromSchema = extractIterationList("industry_gdp", "keyIndustriesList");

    // PATCH: Load active NIPA tables from catalog and use for downloads
    Map<String, java.util.Set<String>> loadedTableFrequencies = java.util.Collections.emptyMap();
    List<String> finalNipaTablesList = nipaTablesListFromSchema;

    try {
      loadedTableFrequencies = loadNipaTableFrequencies();
      if (!loadedTableFrequencies.isEmpty()) {
        // Extract just the table names
        finalNipaTablesList = new java.util.ArrayList<>(loadedTableFrequencies.keySet());
        LOGGER.info("Patching nipaTablesList with {} active tables from catalog (was: {})",
            finalNipaTablesList.size(), nipaTablesListFromSchema.size());
      } else {
        LOGGER.warn("No active NIPA tables found in catalog, using default nipaTablesList");
      }
    } catch (Exception e) {
      LOGGER.warn("Could not load active NIPA tables from catalog: {}. Using default nipaTablesList.", e.getMessage());
    }

    this.nipaTablesList = finalNipaTablesList;
    this.tableFrequencies = loadedTableFrequencies;
    this.keyIndustriesList = keyIndustriesListFromSchema;
  }

  /**
   * Internal constructor for passing pre-computed configuration (used by legacy code and tests).
   */
  private BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest, List<String> nipaTablesList,
      Map<String, java.util.Set<String>> tableFrequencies, List<String> keyIndustriesList) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
    this.parquetDir = parquetDir;
    this.nipaTablesList = nipaTablesList;
    this.tableFrequencies = tableFrequencies;
    this.keyIndustriesList = keyIndustriesList;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir) {
    super(cacheDir,
        org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir),
        org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir));
    this.parquetDir = cacheDir; // For compatibility, use same dir
    this.nipaTablesList = null;
    this.tableFrequencies = null;
    this.keyIndustriesList = null;
  }

  @Override protected String getTableName() {
    return "national_accounts";
  }

  /**
   * Downloads BEA reference tables (NIPA tables catalog and regional line codes).
   *
   * <p>Downloads and converts:
   * <ul>
   *   <li>reference_nipa_tables - Catalog of available NIPA tables with frequencies</li>
   *   <li>reference_regional_linecodes - Line code catalogs for all BEA Regional tables</li>
   * </ul>
   *
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadReferenceData() throws IOException, InterruptedException {
    LOGGER.info("Downloading BEA reference tables");

    // Download and convert reference_nipa_tables
    try {
      String refTablePath = downloadReferenceNipaTables();
      LOGGER.info("Downloaded reference_nipa_tables to: {}", refTablePath);

      convertReferenceNipaTablesWithFrequencies();
      LOGGER.info("Converted reference_nipa_tables to parquet with frequency columns");
    } catch (Exception e) {
      LOGGER.warn("Could not download reference_nipa_tables catalog: {}. "
          + "Will use default nipaTablesList.", e.getMessage());
    }

    // Download and convert reference_regional_linecodes
    try {
      downloadRegionalLineCodeCatalog();
      LOGGER.info("Downloaded reference_regional_linecodes catalog for all BEA Regional tables");

      convertRegionalLineCodeCatalog();
      LOGGER.info("Converted reference_regional_linecodes to parquet with enrichment");
    } catch (Exception e) {
      LOGGER.warn("Could not download reference_regional_linecodes catalog: {}. "
          + "Regional income downloads may fail.", e.getMessage());
    }

    LOGGER.info("Completed BEA reference tables download");
  }

  /**
   * Downloads all BEA data for the specified year range.
   * Consolidates all download*Metadata() calls using configuration passed to constructor.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading all BEA data for years {}-{}", startYear, endYear);

    if (nipaTablesList != null) {
      downloadNationalAccountsMetadata(startYear, endYear, nipaTablesList, tableFrequencies);
    }
    downloadRegionalIncomeMetadata(startYear, endYear);
    downloadTradeStatisticsMetadata(startYear, endYear);
    downloadItaDataMetadata(startYear, endYear);
    if (keyIndustriesList != null) {
      downloadIndustryGdpMetadata(startYear, endYear, keyIndustriesList);
    }

    LOGGER.info("Completed BEA data download");
  }

  /**
   * Converts all downloaded BEA data to Parquet format for the specified year range.
   * Consolidates all convert*Metadata() calls using configuration passed to constructor.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @throws IOException If conversion or file I/O fails
   */
  @Override
  public void convertAll(int startYear, int endYear) throws IOException {
    LOGGER.info("Converting all BEA data for years {}-{}", startYear, endYear);

    if (nipaTablesList != null) {
      convertNationalAccountsMetadata(startYear, endYear, nipaTablesList, tableFrequencies);
    }
    convertRegionalIncomeMetadata(startYear, endYear);
    convertTradeStatisticsMetadata(startYear, endYear);
    convertItaDataMetadata(startYear, endYear);
    if (keyIndustriesList != null) {
      convertIndustryGdpMetadata(startYear, endYear, keyIndustriesList);
    }

    LOGGER.info("Completed BEA data conversion");
  }

  // ===== METADATA-DRIVEN DOWNLOAD/CONVERSION METHODS =====
  // These methods use the download configurations from econ-schema.json
  // and the executeDownload() infrastructure from AbstractGovDataDownloader

  /**
   * Downloads national accounts (NIPA) data using metadata-driven pattern.
   *
   * @param startYear        First year to download
   * @param endYear          Last year to download
   * @param nipaTablesList   List of NIPA table IDs to download
   * @param tableFrequencies Map of table names to their available frequencies (A=Annual,
   *                         Q=Quarterly)
   */
  public void downloadNationalAccountsMetadata(int startYear, int endYear,
      List<String> nipaTablesList, Map<String, Set<String>> tableFrequencies) {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for download");
      return;
    }

    LOGGER.info("Downloading {} NIPA tables for years {}-{}", nipaTablesList.size(), startYear,
        endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "national_accounts";

    for (String nipaTable : nipaTablesList) {
      // Get frequencies for this table (default to Annual if not found)
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));

      for (String frequency : frequencies) {
        for (int year = startYear; year <= endYear; year++) {
          // Build variables map with frequency
          Map<String, String> variables =
              ImmutableMap.of(
                  "year", String.valueOf(year),
                  "tablename", nipaTable,
                  "frequency", frequency);

          if (isCachedOrExists(tableName, year, variables)) {
            skippedCount++;
            continue;
          }

          // Download via metadata-driven executeDownload()
          try {
            String cachedPath =
                cacheStorageProvider.resolvePath(
                    cacheDirectory, executeDownload(tableName,
                    variables));

            // Mark as downloaded in cache manifest
            try {
              long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
              cacheManifest.markCached(tableName, year, variables, cachedPath, fileSize);
            } catch (Exception ex) {
              LOGGER.warn("Failed to mark NIPA data set {} as cached in manifest: {}",
                  cachedPath, ex.getMessage());
            }

            downloadedCount++;

            if (downloadedCount % 10 == 0) {
              LOGGER.info("Downloaded {}/{} NIPA table-frequency-years (skipped {} cached)",
                  downloadedCount,
                  nipaTablesList.size() * frequencies.size() * (endYear - startYear + 1),
                  skippedCount);
            }
          } catch (Exception e) {
            LOGGER.error("Error downloading NIPA table {} frequency {} for year {}: {}",
                nipaTable, frequency, year, e.getMessage());
          }
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("National accounts download complete: downloaded {} table-frequency-years, "
            + "skipped "
            + "{} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts national accounts (NIPA) data using metadata-driven pattern.
   *
   * @param startYear        First year to convert
   * @param endYear          Last year to convert
   * @param nipaTablesList   List of NIPA table IDs to convert
   * @param tableFrequencies Map of table names to their available frequencies (A=Annual,
   *                         Q=Quarterly)
   */
  public void convertNationalAccountsMetadata(int startYear, int endYear,
      List<String> nipaTablesList, Map<String, Set<String>> tableFrequencies) {
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for conversion");
      return;
    }

    LOGGER.info("Converting {} NIPA tables to Parquet for years {}-{}", nipaTablesList.size(),
        startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "national_accounts";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String nipaTable : nipaTablesList) {
      // Get frequencies for this table (default to Annual if not found)
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));

      for (String frequency : frequencies) {
        for (int year = startYear; year <= endYear; year++) {
          // Build variables map
          Map<String, String> variables = new HashMap<>();
          variables.put("year", String.valueOf(year));
          variables.put("frequency", frequency);
          variables.put("TableName", nipaTable);

          // Resolve paths using pattern
          String jsonPath = resolveJsonPath(pattern, variables);
          String parquetPath = resolveParquetPath(pattern, variables);
          String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
          String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

          // Check if conversion needed
          Map<String, String> params = new HashMap<>();
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
              LOGGER.info("Converted {}/{} table-frequency-years (skipped {} up-to-date)",
                  convertedCount,
                  nipaTablesList.size() * frequencies.size() * (endYear - startYear + 1),
                  skippedCount);
            }
          } catch (Exception e) {
            LOGGER.error("Error converting NIPA table {} frequency {} for year {}: {}", nipaTable
                , frequency, year, e.getMessage());
          }
        }
      }
    }

    LOGGER.info("National accounts conversion complete: converted {} table-frequency-years, " +
            "skipped " +
            "{} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads regional income data using metadata-driven pattern.
   * Loads valid LineCodes from the reference_regional_linecodes catalog.
   *
   * @param startYear     First year to download
   * @param endYear       Last year to download
   */
  public void downloadRegionalIncomeMetadata(int startYear, int endYear) {

    String tableName = "regional_income";

    // Load LineCodes from reference_regional_linecodes catalog
    Map<String, Set<String>> lineCodeCatalog;
    try {
      lineCodeCatalog = loadRegionalLineCodeCatalog();
      if (lineCodeCatalog.isEmpty()) {
        LOGGER.error("Regional LineCode catalog is empty. Cannot download regional_income data. " +
            "Ensure reference_regional_linecodes has been downloaded and converted first.");
        return;
      }
    } catch (IOException e) {
      LOGGER.error("Failed to load Regional LineCode catalog: {}. Cannot download regional_income data.",
          e.getMessage());
      return;
    }

    Map<String, Object> tablenames = extractApiSet(tableName, "tableNamesSet");
    Map<String, Object> geoFips = extractApiSet(tableName, "geoFipsSet");
    int downloadedCount = 0;
    int skippedCount = 0;

    int totalOperations = 0;
    for (String tablename : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tablename);
      if (lineCodesForTable != null) {
        totalOperations += lineCodesForTable.size() * geoFips.size() * (endYear - startYear + 1);
      }
    }

    LOGGER.info("Downloading regional income data for years {}-{} ({} table-linecode-geo-year combinations)",
        startYear, endYear, totalOperations);

    for (String tablename : tablenames.keySet()) {
      // Get valid LineCodes for this table from catalog
      Set<String> lineCodesForTable = lineCodeCatalog.get(tablename);

      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("No LineCodes found in catalog for table {}, skipping", tablename);
        continue;
      }

      LOGGER.debug("Processing table {} with {} LineCodes", tablename, lineCodesForTable.size());

      for (String line_code : lineCodesForTable) {
        for (String geo_fips : geoFips.keySet()) {
          for (int year = startYear; year <= endYear; year++) {
            // Build variables map
            Map<String, String> variables =
                ImmutableMap.of("year", String.valueOf(year), "line_code", line_code, "tablename", tablename, "geo_fips_set", geo_fips);

            if (isCachedOrExists(tableName, year, variables)) {
              skippedCount++;
              continue;
            }

            // Download via metadata-driven executeDownload()
            try {
              String cachedPath =
                  cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, variables));

              // Mark as downloaded in cache manifest
              try {
                long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
                cacheManifest.markCached(tableName, year, variables, cachedPath, fileSize);
              } catch (Exception ex) {
                LOGGER.warn("Failed to mark regional income dataset {} as cached in manifest: {}",
                    cachedPath,
                    ex.getMessage());
              }

              downloadedCount++;

              if (downloadedCount % 10 == 0) {
                LOGGER.info("Downloaded {}/{} table-linecode-geo-years (skipped {} cached)",
                    downloadedCount, totalOperations, skippedCount);
              }
            } catch (Exception e) {
              LOGGER.error("Error downloading table {} line code {} geo {} for year {}: {}",
                  tablename, line_code, geo_fips, year, e.getMessage());
            }
          }
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save regional income cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Regional income download complete: downloaded {} table-linecode-geo-years, skipped {} " +
            "(cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts regional income data using metadata-driven pattern.
   * Loads valid LineCodes from the reference_regional_linecodes catalog.
   */
  public void convertRegionalIncomeMetadata(int startYear, int endYear) {
    String tableName = "regional_income";

    // Load LineCodes from reference_regional_linecodes catalog
    Map<String, Set<String>> lineCodeCatalog;
    try {
      lineCodeCatalog = loadRegionalLineCodeCatalog();
      if (lineCodeCatalog.isEmpty()) {
        LOGGER.error("Regional LineCode catalog is empty. Cannot convert regional_income data. " +
            "Ensure reference_regional_linecodes has been downloaded and converted first.");
        return;
      }
    } catch (IOException e) {
      LOGGER.error("Failed to load Regional LineCode catalog: {}. Cannot convert regional_income data.",
          e.getMessage());
      return;
    }

    Map<String, Object> tablenames = extractApiSet(tableName, "tableNamesSet");
    Map<String, Object> geoFips = extractApiSet(tableName, "geoFipsSet");
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");
    int convertedCount = 0;
    int skippedCount = 0;

    int totalOperations = 0;
    for (String tablename : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tablename);
      if (lineCodesForTable != null) {
        totalOperations += lineCodesForTable.size() * geoFips.size() * (endYear - startYear + 1);
      }
    }

    LOGGER.info("Converting regional income data to Parquet for years {}-{} ({} table-linecode-geo-year combinations)",
        startYear, endYear, totalOperations);

    for (String tablename : tablenames.keySet()) {
      // Get valid LineCodes for this table from catalog
      Set<String> lineCodesForTable = lineCodeCatalog.get(tablename);

      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("No LineCodes found in catalog for table {}, skipping", tablename);
        continue;
      }

      LOGGER.debug("Converting table {} with {} LineCodes", tablename, lineCodesForTable.size());

      for (String line_code : lineCodesForTable) {
        for (String geo_fips_set : geoFips.keySet()) {
          for (int year = startYear; year <= endYear; year++) {

            Map<String, String> variables =
                ImmutableMap.of("year", String.valueOf(year), "line_code", line_code, "tablename", tablename, "geo_fips_set", geo_fips_set);

            String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));
            String fullParquetPath = storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

            if (isParquetConvertedOrExists(tableName, year, variables, rawPath, fullParquetPath)) {
              skippedCount++;
              continue;
            }

            try {
              convertCachedJsonToParquet(tableName, variables, null);
              cacheManifest.markParquetConverted(tableName, year, variables, fullParquetPath);
              convertedCount++;

              if (convertedCount % 10 == 0) {
                LOGGER.info("Converted {}/{} table-linecode-geo-years (skipped {} up-to-date)",
                    convertedCount, totalOperations, skippedCount);
              }
            } catch (Exception e) {
              LOGGER.error("Error converting table {} line code {} geo {} for year {}: {}",
                  tablename, line_code, geo_fips_set, year, e.getMessage());
            }
          }
        }
      }
    }

    LOGGER.info("Regional income conversion complete: converted {} table-linecode-geo-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads trade statistics data using metadata-driven pattern.
   * This table has no iteration (single table T40205B).
   */
  public void downloadTradeStatisticsMetadata(int startYear, int endYear) {
    LOGGER.info("Downloading trade statistics for years {}-{}", startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      Map<String, String> variables = ImmutableMap.of("year", String.valueOf(year));

      if (isCachedOrExists(tableName, year, variables)) {
        skippedCount++;
        continue;
      }

      try {
        String cachedPath =
            cacheStorageProvider.resolvePath(
                cacheDirectory, executeDownload(tableName,
                variables));

        // Mark as downloaded in cache manifest
        try {
          long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
          cacheManifest.markCached(tableName, year, variables, cachedPath, fileSize);
        } catch (Exception ex) {
          LOGGER.warn("Failed to mark {} as cached in manifest: {}", cachedPath, ex.getMessage());
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
  public void convertTradeStatisticsMetadata(int startYear, int endYear) {
    LOGGER.info("Converting trade statistics to Parquet for years {}-{}", startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (int year = startYear; year <= endYear; year++) {
      Map<String, String> variables = ImmutableMap.of("year", String.valueOf(year));

      // Resolve paths using pattern
      String jsonPath =
          cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));
      String parquetPath =
          storageProvider.resolvePath(parquetDir, resolveParquetPath(pattern, variables));

      if (isParquetConvertedOrExists(tableName, year, variables, jsonPath, parquetPath)) {
        skippedCount++;
        continue;
      }

      try {
        convertCachedJsonToParquet(tableName, variables);
        cacheManifest.markParquetConverted(tableName, year, variables, parquetPath);
        convertedCount++;
      } catch (Exception e) {
        LOGGER.error("Error converting trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("Trade statistics conversion complete: converted {} years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads ITA data using metadata-driven pattern.
   *
   * @param startYear         First year to download
   * @param endYear           Last year to download
   */
  public void downloadItaDataMetadata(int startYear, int endYear) {

    List<String> itaIndicatorsList = extractApiList("ita_data", "itaIndicatorsList");
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for download");
      return;
    }
    List<String> frequencies = extractApiList("ita_data", "frequencyList");
    LOGGER.info("Downloading {} ITA indicators for years {}-{}", itaIndicatorsList.size(),
        startYear, endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {
        for (String frequency : frequencies) {

          Map<String, String> params =
              ImmutableMap.of("year", String.valueOf(year), "indicator", indicator, "frequency", frequency);

          if (isCachedOrExists(tableName, year, params)) {
            skippedCount++;
            continue;
          }

          try {
            String cachedPath =
                cacheStorageProvider.resolvePath(
                    cacheDirectory, executeDownload(tableName,
                    params));

            // Mark as downloaded in cache manifest
            try {
              long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
              cacheManifest.markCached(tableName, year, params, cachedPath, fileSize);
            } catch (Exception ex) {
              LOGGER.warn("Failed to mark ITA data set {} as cached in manifest: {}", cachedPath,
                  ex.getMessage());
            }

            downloadedCount++;

            if (downloadedCount % 10 == 0) {
              LOGGER.info("Downloaded {}/{} ITA indicators (skipped {} cached)", downloadedCount,
                  itaIndicatorsList.size() * (endYear - startYear + 1), skippedCount);
            }
          } catch (Exception e) {
            LOGGER.error("Error downloading ITA indicator {} for year {}: {}", indicator, year,
                e.getMessage());
          }
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save ITA data cache manifest: {}", e.getMessage());
    }

    LOGGER.info("ITA data download complete: downloaded {} indicator-years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts ITA data using metadata-driven pattern.
   */
  public void convertItaDataMetadata(int startYear, int endYear) {
    List<String> itaIndicatorsList = extractApiList("ita_data", "itaIndicatorsList");
    if (itaIndicatorsList == null || itaIndicatorsList.isEmpty()) {
      LOGGER.warn("No ITA indicators provided for conversion");
      return;
    }
    List<String> frequencies = extractApiList("ita_data", "frequencyList");

    LOGGER.info("Converting {} ITA indicators to Parquet for years {}-{}",
        itaIndicatorsList.size(), startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "ita_data";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String indicator : itaIndicatorsList) {
      for (int year = startYear; year <= endYear; year++) {
        for (String frequency : frequencies) {
          Map<String, String> variables =
              ImmutableMap.of("year", String.valueOf(year), "indicator", indicator, "frequency", frequency);

          // Resolve paths using pattern
          String rawPath =
              cacheStorageProvider.resolvePath(
                  cacheDirectory, resolveJsonPath(pattern
                      , variables));
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

          if (isParquetConvertedOrExists(tableName, year, variables, rawPath, fullParquetPath)) {
            skippedCount++;
            continue;
          }

          try {
            convertCachedJsonToParquet(tableName, variables);
            cacheManifest.markParquetConverted(tableName, year, variables, fullParquetPath);
            convertedCount++;
          } catch (Exception e) {
            LOGGER.error("Error converting ITA indicator {} for year {}: {}", indicator, year,
                e.getMessage());
          }
        }
      }
    }

    LOGGER.info("ITA data conversion complete: converted {} indicator-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads industry GDP data using metadata-driven pattern.
   *
   * @param startYear         First year to download
   * @param endYear           Last year to download
   * @param keyIndustriesList List of industry codes to download
   */
  public void downloadIndustryGdpMetadata(int startYear, int endYear,
      List<String> keyIndustriesList) {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for download");
      return;
    }

    LOGGER.info("Downloading {} industries for years {}-{}", keyIndustriesList.size(), startYear,
        endYear);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve path using pattern
        String relativePath = resolveJsonPath(pattern, variables);

        Map<String, String> params = new HashMap<>();
        params.put("Industry", industry);

        if (isCachedOrExists(tableName, year, params)) {
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
            LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath,
                ex.getMessage());
          }

          downloadedCount++;

          if (downloadedCount % 10 == 0) {
            LOGGER.info("Downloaded {}/{} industries (skipped {} cached)", downloadedCount,
                keyIndustriesList.size() * (endYear - startYear + 1), skippedCount);
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading industry {} for year {}: {}", industry, year,
              e.getMessage());
        }
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Industry GDP download complete: downloaded {} industry-years, skipped {} " +
            "(cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts industry GDP data using metadata-driven pattern.
   */
  public void convertIndustryGdpMetadata(int startYear, int endYear,
      List<String> keyIndustriesList) {
    if (keyIndustriesList == null || keyIndustriesList.isEmpty()) {
      LOGGER.warn("No industries provided for conversion");
      return;
    }

    LOGGER.info("Converting {} industries to Parquet for years {}-{}", keyIndustriesList.size(),
        startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (String industry : keyIndustriesList) {
      for (int year = startYear; year <= endYear; year++) {
        // Build variables map
        Map<String, String> variables = new HashMap<>();
        variables.put("year", String.valueOf(year));
        variables.put("frequency", "A");
        variables.put("Industry", industry);

        // Resolve paths using pattern
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        Map<String, String> params = new HashMap<>();
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
          LOGGER.error("Error converting industry {} for year {}: {}", industry, year,
              e.getMessage());
        }
      }
    }

    LOGGER.info("Industry GDP conversion complete: converted {} industry-years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Public wrapper to download reference_nipa_tables using metadata-driven pattern.
   *
   * @return Path to downloaded JSON cache file
   * @throws IOException          if download fails
   * @throws InterruptedException if download is interrupted
   */
  public String downloadReferenceNipaTables() throws IOException, InterruptedException {
    LOGGER.info("Downloading reference_nipa_tables catalog");
    return executeDownload("reference_nipa_tables", new HashMap<>());
  }

  /**
   * Loads NIPA tables from reference_nipa_tables catalog and extracts frequency information.
   *
   * <p>Parses the Description field using regex to determine which frequencies (Annual,
   * Quarterly)
   * are available for each table based on (A) and (Q) indicators.</p>
   *
   * @return Map of table name to Set of available frequency codes ("A", "Q")
   * @throws IOException if cache file cannot be read or parsed
   */
  public Map<String, Set<String>> loadNipaTableFrequencies() throws IOException {
    LOGGER.info("Loading NIPA table frequencies from reference_nipa_tables catalog");

    // Resolve path to cached JSON file
    String jsonPath = "type=reference/nipa_tables.json";
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    if (!cacheStorageProvider.exists(fullJsonPath)) {
      LOGGER.warn("reference_nipa_tables.json not found at {}, returning empty map",
          fullJsonPath);
      return new HashMap<>();
    }

    try {
      // Read JSON file
      com.fasterxml.jackson.databind.JsonNode root;
      try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
        root = MAPPER.readTree(is);
      }

      if (!root.isArray()) {
        LOGGER.warn("reference_nipa_tables.json is not an array, returning empty map");
        return new HashMap<>();
      }

      // Extract table codes and frequencies from ParamValue array
      // BEA API response format: [{Key: "T10101", Desc: "Table 1.1.1... (A) (Q)"}, ...]
      Map<String, Set<String>> tableFrequencies = new HashMap<>();

      // Regex pattern to match frequency indicators: (A) or (Q)
      Pattern frequencyPattern = Pattern.compile("\\(([AQ])\\)");

      for (JsonNode item : root) {
        JsonNode keyNode = item.get("TableName");
        JsonNode descNode = item.get("Description");

        if (keyNode != null && !keyNode.isNull() && descNode != null && !descNode.isNull()) {
          String tableName = keyNode.asText();
          String description = descNode.asText();

          // Extract frequency indicators using regex
          Set<String> frequencies = new HashSet<>();
          Matcher matcher = frequencyPattern.matcher(description);
          while (matcher.find()) {
            String freq = matcher.group(1); // Captures "A" or "Q"
            frequencies.add(freq);
          }

          // If no frequency indicators found, default to Annual
          if (frequencies.isEmpty()) {
            frequencies.add("A");
            LOGGER.debug("No frequency indicators found for table {}, defaulting to Annual",
                tableName);
          }

          tableFrequencies.put(tableName, frequencies);
        }
      }

      LOGGER.info("Loaded {} NIPA tables with frequency information from catalog",
          tableFrequencies.size());

      // Log summary of frequency distribution
      long annualOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("A")).count();
      long quarterlyOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("Q")).count();
      long both = tableFrequencies.values().stream()
          .filter(f -> f.size() == 2).count();
      LOGGER.info("Frequency distribution: {} annual-only, {} quarterly-only, {} both",
          annualOnly, quarterlyOnly, both);

      return tableFrequencies;
    } catch (Exception e) {
      LOGGER.error("Failed to load NIPA table frequencies from catalog: {}", e.getMessage(), e);
      throw new IOException("Failed to load NIPA table frequencies: " + e.getMessage(), e);
    }
  }

  /**
   * Maps NIPA section number to descriptive section name.
   *
   * <p>NIPA tables are organized into 8 sections based on the first digit after 'T':
   * <ul>
   *   <li>1 = Domestic Product and Income (GDP, national income, value added)</li>
   *   <li>2 = Personal Income and Outlays (wages, consumer spending, disposable income)</li>
   *   <li>3 = Government (current receipts, expenditures, benefits)</li>
   *   <li>4 = Foreign Transactions (exports, imports, international transactions)</li>
   *   <li>5 = Saving and Investment (fixed investment, inventories, capital formation)</li>
   *   <li>6 = Income and Employment by Industry (compensation, profits by sector)</li>
   *   <li>7 = Supplemental Tables (per capita, pension plans, additional details)</li>
   *   <li>8 = Not Seasonally Adjusted (unadjusted versions of other tables)</li>
   * </ul>
   *
   * @param section Section number as string (1-8)
   * @return Descriptive section name (e.g., "domestic_product_income")
   */
  private static String getSectionName(String section) {
    switch (section) {
    case "1":
      return "domestic_product_income";
    case "2":
      return "personal_income_outlays";
    case "3":
      return "government";
    case "4":
      return "foreign_transactions";
    case "5":
      return "saving_investment";
    case "6":
      return "industry";
    case "7":
      return "supplemental";
    case "8":
      return "not_seasonally_adjusted";
    default:
      return "unknown";
    }
  }

  /**
   * Converts reference_nipa_tables JSON to Parquet with dynamic frequency columns and section
   * categorization.
   * Parses Description field to add annual and quarterly boolean flags.
   * Parses TableName to extract section, family, metric, and table_number.
   *
   * @throws IOException if conversion fails
   */
  public void convertReferenceNipaTablesWithFrequencies() throws IOException {
    LOGGER.info("Converting reference_nipa_tables to parquet with frequency columns");

    String jsonPath = "type=reference/nipa_tables.json";
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    // Read JSON
    com.fasterxml.jackson.databind.JsonNode root;
    try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
      root = MAPPER.readTree(is);
    }

    if (!root.isArray()) {
      throw new IOException("reference_nipa_tables.json is not an array");
    }

    // Regex pattern to match frequency indicators
    Pattern frequencyPattern = Pattern.compile("\\(([AQ])\\)");

    List<Map<String, Object>> enrichedRecords = new ArrayList<>();
    for (com.fasterxml.jackson.databind.JsonNode item : root) {
      com.fasterxml.jackson.databind.JsonNode keyNode = item.get("TableName");
      com.fasterxml.jackson.databind.JsonNode descNode = item.get("Description");

      if (keyNode != null && descNode != null) {
        String tableName = keyNode.asText();
        String description = descNode.asText();

        // Parse frequency indicators from description
        boolean hasAnnual = description.contains("(A)");
        boolean hasQuarterly = description.contains("(Q)");

        // Parse TableName structure: TXYYZZ where X=section, YY=family, ZZ=metric
        String section = null;
        String sectionName = null;
        String family = null;
        String metric = null;
        String tableNumber = null;

        if (tableName != null && tableName.startsWith("T") && tableName.length() >= 4) {
          try {
            section = tableName.substring(1, 2);           // Character at position 1
            family = tableName.substring(2, 4);            // Characters at positions 2-3
            metric = tableName.substring(4);               // Remaining characters (may include
            // letters like 'B')
            sectionName = getSectionName(section);
            tableNumber = section + "." + family + "." + metric;  // e.g., "1.01.05" or "4.02.05B"
          } catch (Exception e) {
            LOGGER.warn("Failed to parse TableName '{}': {}", tableName, e.getMessage());
          }
        }

        Map<String, Object> record = new HashMap<>();
        record.put("TableName", tableName);
        record.put("Description", description);
        record.put("section", section);
        record.put("section_name", sectionName);
        record.put("family", family);
        record.put("metric", metric);
        record.put("table_number", tableNumber);
        record.put("annual", hasAnnual);
        record.put("quarterly", hasQuarterly);

        enrichedRecords.add(record);
      }
    }

    LOGGER.info("Enriched {} NIPA tables with frequency and section columns",
        enrichedRecords.size());

    // Group records by section for partitioned writing
    Map<String, List<Map<String, Object>>> recordsBySection = new HashMap<>();
    for (Map<String, Object> record : enrichedRecords) {
      String section = (String) record.get("section");
      if (section == null) {
        section = "unknown";
      }
      recordsBySection.computeIfAbsent(section, k -> new ArrayList<>()).add(record);
    }

    // Write each section partition separately
    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumnsFromMetadata("reference_nipa_tables");

    for (Map.Entry<String, List<Map<String, Object>>> entry : recordsBySection.entrySet()) {
      String section = entry.getKey();
      List<Map<String, Object>> sectionRecords = entry.getValue();

      String parquetPath = "type=reference/section=" + section + "/nipa_tables.parquet";
      String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

      storageProvider.writeAvroParquet(fullParquetPath, columns, sectionRecords,
          "reference_nipa_tables", "reference_nipa_tables");

      LOGGER.info("Wrote {} tables for section {} to {}", sectionRecords.size(), section,
          parquetPath);
    }

    LOGGER.info("Converted reference_nipa_tables to parquet with frequency and section columns " +
            "across {} sections",
        recordsBySection.size());
  }

  /**
   * Download BEA Regional LineCode catalog using GetParameterValuesFiltered API.
   * Downloads valid LineCodes for all 54 BEA Regional tables listed in the schema.
   */
  public void downloadRegionalLineCodeCatalog() throws IOException, InterruptedException {
    LOGGER.info("Downloading BEA Regional LineCode catalog");

    String tableName = "reference_regional_linecodes";
    List<String> tableNamesList = extractApiList(tableName, "tableNamesList");

    if (tableNamesList == null || tableNamesList.isEmpty()) {
      LOGGER.error("No tableNamesList found in schema for reference_regional_linecodes");
      return;
    }

    LOGGER.info("Downloading LineCodes for {} BEA Regional tables", tableNamesList.size());

    int downloadedCount = 0;
    int skippedCount = 0;
    int year = 0;  // Sentinel value for reference tables without year dimension

    for (String regionalTableName : tableNamesList) {
      Map<String, String> variables = ImmutableMap.of("tablename", regionalTableName);

      // Check cache manifest before downloading
      if (isCachedOrExists(tableName, year, variables)) {
        skippedCount++;
        continue;
      }

      try {
        String cachedPath = executeDownload(tableName, variables);

        // Mark as cached in manifest
        try {
          String fullCachedPath = cacheStorageProvider.resolvePath(cacheDirectory, cachedPath);
          long fileSize = cacheStorageProvider.getMetadata(fullCachedPath).getSize();
          cacheManifest.markCached(tableName, year, variables, fullCachedPath, fileSize);
        } catch (Exception ex) {
          LOGGER.warn("Failed to mark LineCodes for {} as cached in manifest: {}",
              regionalTableName, ex.getMessage());
        }

        downloadedCount++;

        if (downloadedCount % 10 == 0) {
          LOGGER.info("Downloaded LineCodes for {}/{} tables (skipped {} cached)",
              downloadedCount, tableNamesList.size(), skippedCount);
        }
      } catch (Exception e) {
        LOGGER.error("Error downloading LineCodes for table {}: {}", regionalTableName, e.getMessage());
      }
    }

    // Save manifest after downloads complete
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Regional LineCode catalog download complete: downloaded {} tables, skipped {} cached",
        downloadedCount, skippedCount);
  }

  /**
   * Convert BEA Regional LineCode catalog JSON to Parquet with enrichment.
   * Enriches data with parsed metadata (table_prefix, data_category, geography_level, frequency).
   */
  public void convertRegionalLineCodeCatalog() throws IOException {
    LOGGER.info("Converting BEA Regional LineCode catalog to Parquet");

    String tableName = "reference_regional_linecodes";
    List<String> tableNamesList = extractApiList(tableName, "tableNamesList");

    if (tableNamesList == null || tableNamesList.isEmpty()) {
      LOGGER.error("No tableNamesList found in schema for reference_regional_linecodes");
      return;
    }

    // Load metadata to get the pattern
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    int convertedCount = 0;
    int skippedCount = 0;
    int year = 0;  // Sentinel value for reference tables without year dimension

    for (String regionalTableName : tableNamesList) {
      Map<String, String> variables = ImmutableMap.of("tablename", regionalTableName);

      try {
        // Resolve paths
        String jsonPath = resolveJsonPath(pattern, variables);
        String parquetPath = resolveParquetPath(pattern, variables);
        String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        // Check if already converted
        if (isParquetConvertedOrExists(tableName, year, variables, fullJsonPath, fullParquetPath)) {
          skippedCount++;
          continue;
        }

        // Read JSON array (pre-extracted by dataPath during download)
        com.fasterxml.jackson.databind.JsonNode dataNode;
        try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
          dataNode = MAPPER.readTree(is);
        }

        if (!dataNode.isArray() || dataNode.size() == 0) {
          LOGGER.warn("Invalid or empty LineCode array for table {} in {}", regionalTableName, fullJsonPath);
          continue;
        }

        // Parse table structure for enrichment
        String tablePrefix = parseTablePrefix(regionalTableName);  // SA, SQ, CA, etc.
        String dataCategory = parseDataCategory(regionalTableName);  // INC, GDP, etc.
        String geographyLevel = parseGeographyLevel(regionalTableName);  // state, county, msa
        String frequency = parseFrequency(regionalTableName);  // annual, quarterly

        List<Map<String, Object>> enrichedRecords = new ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode item : dataNode) {
          String lineCode = item.get("Key").asText();
          String description = item.get("Desc").asText();

          Map<String, Object> record = new HashMap<>();
          record.put("TableName", regionalTableName);
          record.put("LineCode", lineCode);
          record.put("Description", description);
          record.put("table_prefix", tablePrefix);
          record.put("data_category", dataCategory);
          record.put("geography_level", geographyLevel);
          record.put("frequency", frequency);

          enrichedRecords.add(record);
        }

        // Write parquet partition
        List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
            loadTableColumns(tableName);
        storageProvider.writeAvroParquet(fullParquetPath, columns, enrichedRecords,
            "RegionalLineCodes", "RegionalLineCodes");

        // Mark as converted in manifest
        cacheManifest.markParquetConverted(tableName, year, variables, fullParquetPath);

        convertedCount++;
        LOGGER.debug("Converted LineCodes for table {} ({} line codes)", regionalTableName, enrichedRecords.size());

      } catch (Exception e) {
        LOGGER.error("Error converting LineCodes for table {}: {}", regionalTableName, e.getMessage());
      }
    }

    // Save manifest after conversions complete
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Regional LineCode catalog conversion complete: converted {} tables, skipped {} up-to-date",
        convertedCount, skippedCount);
  }

  /**
   * Parse table prefix from BEA Regional table name.
   * SA=State Annual, SQ=State Quarterly, CA=County/MSA Annual, etc.
   */
  private String parseTablePrefix(String tableName) {
    if (tableName.length() >= 2) {
      return tableName.substring(0, 2);
    }
    return null;
  }

  /**
   * Parse data category from BEA Regional table name.
   * INC=Income, GDP=Gross Domestic Product, ACE=Arts/Culture/Entertainment, etc.
   */
  private String parseDataCategory(String tableName) {
    if (tableName.startsWith("S") && tableName.contains("INC")) {
      return "INC";
    }
    if (tableName.startsWith("C") && tableName.contains("INC")) {
      return "INC";
    }
    if (tableName.startsWith("S") && tableName.contains("GDP")) {
      return "GDP";
    }
    if (tableName.startsWith("C") && tableName.contains("GDP")) {
      return "GDP";
    }
    if (tableName.contains("ACE") || tableName.contains("SAAC")) {
      return "ACE";
    }
    if (tableName.contains("RP")) {
      return "RPP";  // Regional Price Parities
    }
    if (tableName.contains("PCE")) {
      return "PCE";  // Personal Consumption Expenditures
    }
    return null;
  }

  /**
   * Parse geography level from BEA Regional table name.
   */
  private String parseGeographyLevel(String tableName) {
    if (tableName.startsWith("SA") || tableName.startsWith("SQ")) {
      return "state";
    }
    if (tableName.startsWith("CA")) {
      return "county";
    }
    if (tableName.startsWith("MA")) {
      return "msa";
    }
    if (tableName.startsWith("MI")) {
      return "micropolitan";
    }
    return null;
  }

  /**
   * Parse frequency from BEA Regional table name.
   */
  private String parseFrequency(String tableName) {
    if (tableName.startsWith("SA") || tableName.startsWith("CA")
        || tableName.startsWith("MA")) {
      return "annual";
    }
    if (tableName.startsWith("SQ")) {
      return "quarterly";
    }
    return null;
  }

  /**
   * Load BEA Regional LineCode catalog from Parquet files.
   * Reads all reference_regional_linecodes partitions and builds a map of TableName to
   * valid LineCodes.
   *
   * @return Map where key=TableName (e.g., "SQINC4") and value=Set of valid LineCodes
   * for that table
   * @throws IOException if catalog files cannot be read
   */
  public Map<String, Set<String>> loadRegionalLineCodeCatalog() throws IOException {
    LOGGER.info("Loading Regional LineCode catalog from Parquet");

    String tableName = "reference_regional_linecodes";
    List<String> tableNamesList = extractApiList(tableName, "tableNamesList");

    if (tableNamesList == null || tableNamesList.isEmpty()) {
      LOGGER.warn("No tableNamesList found in schema for reference_regional_linecodes, "
          + "returning empty map");
      return new HashMap<>();
    }

    // Load metadata to get the pattern
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    Map<String, Set<String>> catalogMap = new HashMap<>();
    int loadedTables = 0;
    int totalLineCodes = 0;

    for (String regionalTableName : tableNamesList) {
      Map<String, String> variables = ImmutableMap.of("tablename", regionalTableName);

      try {
        // Resolve parquet path for this table
        String parquetPath = resolveParquetPath(pattern, variables);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

        if (!storageProvider.exists(fullParquetPath)) {
          LOGGER.debug("Parquet file not found for table {}: {}",
              regionalTableName, fullParquetPath);
          continue;
        }

        // Read parquet file
        List<Map<String, Object>> records =
            storageProvider.readParquet(fullParquetPath);

        // Extract LineCodes for this table
        Set<String> lineCodes = new HashSet<>();
        for (Map<String, Object> record : records) {
          Object tableNameObj = record.get("TableName");
          Object lineCodeObj = record.get("LineCode");

          if (lineCodeObj != null) {
            String lineCode = lineCodeObj.toString();
            lineCodes.add(lineCode);
          }
        }

        if (!lineCodes.isEmpty()) {
          catalogMap.put(regionalTableName, lineCodes);
          loadedTables++;
          totalLineCodes += lineCodes.size();
          LOGGER.debug("Loaded {} LineCodes for table {}", lineCodes.size(), regionalTableName);
        }

      } catch (Exception e) {
        LOGGER.warn("Failed to load LineCodes for table {}: {}",
            regionalTableName, e.getMessage());
      }
    }

    LOGGER.info("Loaded Regional LineCode catalog: {} tables, {} total LineCodes",
        loadedTables, totalLineCodes);
    return catalogMap;
  }

}
