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
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.CacheManifestQueryHelper;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;
import org.apache.calcite.adapter.govdata.OperationType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 *
 * <p>Requires a BEA API key from <a href="https://apps.bea.gov/api/signup/">...</a>
 */
public class BeaDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);

  private final String parquetDir;
  // Lazily loaded from parquet catalogs - populated after downloadReferenceData() is called
  private volatile List<String> nipaTablesList;
  private volatile Map<String, Set<String>> tableFrequencies;
  private volatile boolean catalogsLoaded = false;

  /**
   * Simple constructor without shared manifest (creates one from operatingDirectory).
   */
  public BeaDataDownloader(String cacheDir, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      int startYear, int endYear) {
    this(cacheDir, cacheDir, parquetDir, cacheStorageProvider, storageProvider, null, startYear, endYear);
  }

  /**
   * Main constructor for BEA downloader.
   * Handles all BEA-specific initialization including loading catalogs.
   */
  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest, int startYear, int endYear) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest, startYear, endYear);
    this.parquetDir = parquetDir;
    // Catalogs are loaded lazily from parquet files after downloadReferenceData() generates them
  }

  /**
   * Ensures catalogs are loaded from parquet files.
   * Called lazily when catalog data is first needed.
   * By this point, downloadReferenceData() should have already generated the parquet files.
   */
  private synchronized void ensureCatalogsLoaded() {
    if (catalogsLoaded) {
      return;
    }
    try {
      Map<String, Set<String>> loadedTableFrequencies = loadNipaTableFrequencies();
      if (loadedTableFrequencies.isEmpty()) {
        throw new IllegalStateException("reference_nipa_tables catalog is empty - cannot proceed");
      }

      // More specific initialization message
      long annualOnly = loadedTableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("A")).count();
      long quarterlyOnly = loadedTableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("Q")).count();
      long both = loadedTableFrequencies.values().stream()
          .filter(f -> f.size() == 2).count();

      LOGGER.info("BEA initialized with {} NIPA tables: {} annual-only, {} quarterly-only, {} both A+Q",
          loadedTableFrequencies.size(), annualOnly, quarterlyOnly, both);

      this.nipaTablesList = new ArrayList<>(loadedTableFrequencies.keySet());
      this.tableFrequencies = loadedTableFrequencies;
      this.catalogsLoaded = true;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to load reference_nipa_tables catalog. This is required for NIPA data downloads. "
          + "Ensure downloadReferenceData() is called before accessing catalog data. "
          + "Error: " + e.getMessage(), e);
    }
  }

  /**
   * Gets list of NIPA tables, loading from parquet if needed.
   */
  private List<String> getNipaTablesList() {
    ensureCatalogsLoaded();
    return nipaTablesList;
  }

  /**
   * Gets table frequencies map, loading from parquet if needed.
   */
  private Map<String, Set<String>> getTableFrequencies() {
    ensureCatalogsLoaded();
    return tableFrequencies;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir, int startYear, int endYear) {
    super(cacheDir,
        StorageProviderFactory.createFromUrl(cacheDir),
        StorageProviderFactory.createFromUrl(cacheDir),
        startYear, endYear);
    this.parquetDir = cacheDir; // For compatibility, use same dir
    this.nipaTablesList = null;
    this.tableFrequencies = null;
  }

  @Override protected String getTableName() {
    return "national_accounts";
  }

  // BEA API allows 100 requests/minute. Use 650ms to stay safely under limit.
  @Override protected long getMinRequestIntervalMs() {
    return 650;
  }

  @Override protected int getMaxRetries() {
    return 3;
  }

  @Override protected long getRetryDelayMs() {
    return 2000; // 2 seconds for BEA API retry
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
   */
  @Override public void downloadReferenceData() {
    LOGGER.info("BEA reference data initialization starting");

    try {
      String refTablePath = downloadReferenceNipaTables();
      LOGGER.info("BEA reference: NIPA catalog downloaded: {}",
          refTablePath.substring(refTablePath.lastIndexOf('/') + 1));

      convertReferenceNipaTablesWithFrequencies();
      LOGGER.info("BEA reference: NIPA catalog enriched with frequency metadata");
    } catch (Exception e) {
      LOGGER.warn("BEA reference: NIPA catalog download failed: {}", e.getMessage());
    }

    try {
      downloadRegionalLineCodeCatalog();
      LOGGER.info("BEA reference: Regional LineCode catalog downloaded (54 tables)");

      convertRegionalLineCodeCatalog();
      LOGGER.info("BEA reference: Regional catalog enriched with table metadata");
    } catch (Exception e) {
      LOGGER.warn("BEA reference: Regional catalog download failed: {}", e.getMessage());
    }
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
  @Override public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("BEA download phase: {} years ({}-{})",
        endYear - startYear + 1, startYear, endYear);

    List<String> tables = getNipaTablesList();
    if (tables != null && !tables.isEmpty()) {
      LOGGER.info("National accounts: {} NIPA tables configured", tables.size());
      downloadNationalAccountsMetadata(startYear, endYear, tables, getTableFrequencies());
    }

    LOGGER.info("Regional income download: Processing {} BEA regional tables", 54);
    downloadRegionalIncomeMetadata(startYear, endYear);

    // trade_statistics is a SQL view over national_accounts - no separate download needed
    downloadItaDataMetadata(startYear, endYear);
    downloadIndustryGdpMetadata();
  }

  /**
   * Converts all downloaded BEA data to Parquet format for the specified year range.
   * Consolidates all convert*Metadata() calls using configuration passed to constructor.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  @Override public void convertAll(int startYear, int endYear) {
    LOGGER.info("BEA conversion phase: {} years ({}-{})",
        endYear - startYear + 1, startYear, endYear);

    List<String> tables = getNipaTablesList();
    if (tables != null && !tables.isEmpty()) {
      convertNationalAccountsMetadata(startYear, endYear, tables, getTableFrequencies());
    }
    convertRegionalIncomeMetadata(startYear, endYear);
    // trade_statistics is a SQL view over national_accounts - no separate conversion needed
    convertItaDataMetadata(startYear, endYear);
    convertIndustryGdpMetadata();
  }

  // ===== METADATA-DRIVEN DOWNLOAD/CONVERSION METHODS =====
  // These methods use the download configurations from econ-schema.json
  // and the executeDownload() infrastructure from AbstractGovDataDownloader

  /**
   * Creates dimension provider for national accounts data.
   */
  private DimensionProvider createNationalAccountsDimensions(int startYear, int endYear,
      List<String> tableNames, List<String> frequencies) {
    return (dimensionName) -> {
      switch (dimensionName) {
        case "type":
          return List.of("national_accounts");
        case "year":
          return yearRange(startYear, endYear);
        case "tablename":
          return tableNames;
        case "frequency":
          return frequencies;
        default:
          return null;
      }
    };
  }

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
    if (!validateListParameter(nipaTablesList, "NIPA tables", "download")) {
      return;
    }

    // Calculate actual combinations
    int validCombos = 0;
    for (String table : nipaTablesList) {
      validCombos += tableFrequencies.getOrDefault(table, Collections.singleton("A")).size();
    }

    int yearCount = endYear - startYear + 1;
    LOGGER.info("National accounts download: {} NIPA tables × {} years = ~{} API calls",
        nipaTablesList.size(), yearCount, validCombos);

    String tableName = "national_accounts";

    // Build lists for each dimension separately (avoids composite key complexity)
    List<String> tableNames = new ArrayList<>(nipaTablesList);
    Set<String> allFrequencies = new HashSet<>();
    for (String nipaTable : nipaTablesList) {
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));
      allFrequencies.addAll(frequencies);
    }
    List<String> frequencies = new ArrayList<>(allFrequencies);
    Collections.sort(frequencies); // Consistent ordering

    // Execute using optimized framework with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createNationalAccountsDimensions(startYear, endYear, tableNames, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Skip invalid table-frequency combinations
          if (!isValidTableFrequency(vars.get("tablename"), vars.get("frequency"))) {
            return; // Skip this combination
          }

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
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
    if (!validateListParameter(nipaTablesList, "NIPA tables", "conversion")) {
      return;
    }

    int totalFiles = nipaTablesList.size() * (endYear - startYear + 1);
    LOGGER.info("National accounts conversion: {} NIPA table-year files to parquet", totalFiles);

    String tableName = "national_accounts";

    // Build lists for each dimension separately (avoids composite key complexity)
    List<String> tableNames = new ArrayList<>(nipaTablesList);
    Set<String> allFrequencies = new HashSet<>();
    for (String nipaTable : nipaTablesList) {
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));
      allFrequencies.addAll(frequencies);
    }
    List<String> frequencies = new ArrayList<>(allFrequencies);
    Collections.sort(frequencies); // Consistent ordering

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createNationalAccountsDimensions(startYear, endYear, tableNames, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Skip invalid table-frequency combinations
          if (!isValidTableFrequency(vars.get("tablename"), vars.get("frequency"))) {
            return; // Skip this combination
          }

          // Convert
          boolean converted = convertCachedJsonToParquet(tableName, vars);

          // Mark as converted only if conversion succeeded
          if (converted) {
            cacheManifest.markParquetConverted(cacheKey, parquetPath);
          }
        },
        OperationType.CONVERSION);
  }

  /**
   * Determines if a BEA Regional table is valid for a given year based on industry
   * classification system.
   *
   * <p>BEA transitioned from SIC (Standard Industrial Classification) to NAICS
   * (North American Industry Classification System) in 2001:
   * <ul>
   *   <li>SIC tables (suffix "S"): Valid 1969-2000</li>
   *   <li>NAICS tables (suffix "N"): Valid 2001-forward</li>
   *   <li>Historical tables (suffix "H"): Valid across transition (varies by table)</li>
   *   <li>Other tables: No year restrictions</li>
   * </ul>
   *
   * @param tableName BEA Regional table name (e.g., "SAINC7S", "SAINC7N")
   * @param year Year to check
   * @return true if table has data for the specified year, false otherwise
   */
  private boolean isTableValidForYear(String tableName, int year) {
    if (tableName == null || tableName.isEmpty()) {
      return false;
    }

    // SIC-based tables (suffix "S"): only valid through 2000
    if (tableName.endsWith("S")) {
      return year <= 2000;
    }

    // NAICS-based tables (suffix "N"): only valid from 2001 onward
    if (tableName.endsWith("N")) {
      return year >= 2001;
    }

    // Historical tables (suffix "H"): discontinued legacy tables, typically valid through 2005
    // These were transitional tables during the SIC-to-NAICS conversion period
    if (tableName.endsWith("H")) {
      return year <= 2005;
    }

    // Other tables without classification suffix: no restrictions
    return true;
  }

  /**
   * Checks if a table-frequency combination is valid based on the tableFrequencies map.
   * Some NIPA tables only support Annual (A) data, others support both Annual and Quarterly (Q).
   *
   * @param tableName The NIPA table name
   * @param frequency The frequency code (A or Q)
   * @return true if the combination is valid, false otherwise
   */
  private boolean isValidTableFrequency(String tableName, String frequency) {
    Set<String> validFreqs = getTableFrequencies().getOrDefault(tableName, Collections.singleton("A"));
    return validFreqs.contains(frequency);
  }

  /**
   * Validates that a list parameter is not null or empty.
   * Logs a warning and returns false if validation fails.
   *
   * @param list      The list to validate
   * @param itemName  Description of the items (for logging)
   * @param operation Description of the operation (for logging)
   * @return true if list is valid (not null and not empty), false otherwise
   */
  private boolean validateListParameter(List<?> list, String itemName, String operation) {
    if (list == null || list.isEmpty()) {
      LOGGER.warn("No {} provided for {}", itemName, operation);
      return false;
    }
    return true;
  }

  /**
   * Extracts year from variables map.
   *
   * @param vars Variables map containing "year" key
   * @return Year as integer
   */
  private int extractYear(Map<String, String> vars) {
    return Integer.parseInt(vars.get("year"));
  }

  /**
   * Downloads regional income data using valid combinations only.
   * Builds the set of valid table/year/geo/linecode combinations once,
   * then iterates only over those combinations.
   *
   * @param startYear First year to download
   * @param endYear   Last year to download
   */
  public void downloadRegionalIncomeMetadata(int startYear, int endYear) {
    String tableName = "regional_income";

    // Build valid combinations using shared helper
    List<RegionalCombination> validCombinations = buildValidRegionalCombinations(startYear, endYear);
    if (validCombinations.isEmpty()) {
      LOGGER.warn("No valid regional income combinations to download");
      return;
    }

    LOGGER.info("Starting regional income download: {} valid combinations", validCombinations.size());

    // Filter out already-cached combinations
    List<RegionalCombination> uncached = filterUncachedCombinations(tableName, validCombinations, OperationType.DOWNLOAD);
    LOGGER.info("Regional income download: {} uncached of {} valid combinations",
        uncached.size(), validCombinations.size());

    if (uncached.isEmpty()) {
      LOGGER.info("All {} regional income combinations already cached", validCombinations.size());
      return;
    }

    // Process uncached combinations
    int processed = 0;
    int errors = 0;
    for (RegionalCombination combo : uncached) {
      try {
        Map<String, String> vars = combo.toVariables();
        vars.put("type", tableName);

        // Download to cache
        DownloadResult result = executeDownload(tableName, vars);

        // Build cache key
        CacheKey cacheKey = new CacheKey(tableName, vars);
        String jsonPath = resolveJsonPath(loadTableMetadata(tableName).get("pattern").toString(), vars);

        cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
            getCacheExpiryForYear(combo.year), getCachePolicyForYear(combo.year));

        processed++;
        if (processed % 1000 == 0) {
          LOGGER.info("Regional income download progress: {}/{} ({} errors)",
              processed, uncached.size(), errors);
        }
      } catch (Exception e) {
        errors++;
        LOGGER.warn("Failed to download regional income {}: {}",
            combo.toCacheKeyPart(), e.getMessage());
      }
    }

    LOGGER.info("Regional income download complete: {} processed, {} errors", processed, errors);
  }

  /**
   * Converts regional income data using valid combinations only.
   * Uses the same buildValidRegionalCombinations helper as download.
   */
  public void convertRegionalIncomeMetadata(int startYear, int endYear) {
    String tableName = "regional_income";

    // Build valid combinations using shared helper (same as download)
    List<RegionalCombination> validCombinations = buildValidRegionalCombinations(startYear, endYear);
    if (validCombinations.isEmpty()) {
      LOGGER.warn("No valid regional income combinations to convert");
      return;
    }

    LOGGER.info("Starting regional income conversion: {} valid combinations", validCombinations.size());

    // Filter out already-converted combinations
    List<RegionalCombination> unconverted = filterUncachedCombinations(tableName, validCombinations, OperationType.CONVERSION);
    LOGGER.info("Regional income conversion: {} unconverted of {} valid combinations",
        unconverted.size(), validCombinations.size());

    if (unconverted.isEmpty()) {
      LOGGER.info("All {} regional income combinations already converted", validCombinations.size());
      return;
    }

    // Load table metadata once for parquet path resolution
    Map<String, Object> tableMetadata = loadTableMetadata(tableName);
    String parquetPattern = tableMetadata.get("parquet_pattern") != null
        ? tableMetadata.get("parquet_pattern").toString()
        : tableMetadata.get("pattern").toString().replace(".json", ".parquet");

    // Process unconverted combinations
    int converted = 0;
    int errors = 0;
    for (RegionalCombination combo : unconverted) {
      try {
        Map<String, String> vars = combo.toVariables();
        vars.put("type", tableName);

        // Convert JSON to Parquet
        boolean success = convertCachedJsonToParquet(tableName, vars);

        if (success) {
          // Build cache key and mark as converted
          CacheKey cacheKey = new CacheKey(tableName, vars);
          String parquetPath = resolveJsonPath(parquetPattern, vars);
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
          converted++;
        } else {
          errors++;
        }

        if ((converted + errors) % 1000 == 0) {
          LOGGER.info("Regional income conversion progress: {}/{} ({} errors)",
              converted + errors, unconverted.size(), errors);
        }
      } catch (Exception e) {
        errors++;
        LOGGER.warn("Failed to convert regional income {}: {}",
            combo.toCacheKeyPart(), e.getMessage());
      }
    }

    LOGGER.info("Regional income conversion complete: {} converted, {} errors", converted, errors);
  }

  /**
   * Downloads trade statistics data using metadata-driven pattern.
   * This table has no iteration (single table T40205B).
   */
  public void downloadTradeStatisticsMetadata(int startYear, int endYear) {
    int yearCount = endYear - startYear + 1;
    LOGGER.info("Trade statistics download: {} years of annual trade balance data (table T40205B)",
        yearCount);

    int downloadedCount = 0;
    int skippedCount = 0;

    String tableName = "trade_statistics";

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      Map<String, String> variables = ImmutableMap.of("year", String.valueOf(year));

      Map<String, String> allParams = new HashMap<>(variables);
      allParams.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey(tableName, allParams);

      if (isCachedOrExists(cacheKey)) {
        skippedCount++;
        continue;
      }

      try {
        DownloadResult result = executeDownload(tableName, variables);
        String cachedPath = cacheStorageProvider.resolvePath(cacheDirectory, result.jsonPath);

        // Mark as downloaded in cache manifest
        try {
          cacheManifest.markCached(cacheKey, cachedPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
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

    if (downloadedCount > 0 || skippedCount > 0) {
      LOGGER.info("Trade statistics download complete: {} new downloads, {} cached",
          downloadedCount, skippedCount);
    }
  }

  /**
   * Converts trade statistics data using metadata-driven pattern.
   */
  public void convertTradeStatisticsMetadata(int startYear, int endYear) {
    int yearCount = endYear - startYear + 1;
    LOGGER.info("Trade statistics conversion: {} years of annual trade balance data",
        yearCount);

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

      Map<String, String> allParams = new HashMap<>(variables);
      allParams.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey(tableName, allParams);

      if (isParquetConvertedOrExists(cacheKey, jsonPath, parquetPath)) {
        skippedCount++;
        continue;
      }

      try {
        boolean converted = convertCachedJsonToParquet(tableName, variables);
        if (converted) {
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
          convertedCount++;
        }
      } catch (Exception e) {
        LOGGER.error("Error converting trade statistics for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("Trade statistics conversion complete: converted {} years, skipped {} " +
            "(up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Creates dimension provider for ITA data.
   */
  private DimensionProvider createItaDataDimensions(String tableName, int startYear, int endYear,
      List<String> itaIndicatorsList, List<String> frequencies) {
    return (dimensionName) -> {
      switch (dimensionName) {
        case "type": return List.of(tableName);
        case "year":
          return yearRange(startYear, endYear);
        case "indicator":
          return itaIndicatorsList;
        case "frequency":
          return frequencies;
        default:
          return null;
      }
    };
  }

  /**
   * Downloads ITA data using metadata-driven pattern.
   *
   * @param startYear         First year to download
   * @param endYear           Last year to download
   */
  public void downloadItaDataMetadata(int startYear, int endYear) {

    List<String> itaIndicatorsList = extractApiList("ita_data", "itaIndicatorsList");
    if (!validateListParameter(itaIndicatorsList, "ITA indicators", "download")) {
      return;
    }
    List<String> frequencies = extractApiList("ita_data", "frequencyList");

    int yearCount = endYear - startYear + 1;
    int totalCombinations = itaIndicatorsList.size() * frequencies.size() * yearCount;
    LOGGER.info("ITA download: {} indicators × {} frequencies × {} years = ~{} API calls",
        itaIndicatorsList.size(), frequencies.size(), yearCount, totalCombinations);

    String tableName = "ita_data";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createItaDataDimensions(tableName, startYear, endYear, itaIndicatorsList, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Converts ITA data using metadata-driven pattern.
   */
  public void convertItaDataMetadata(int startYear, int endYear) {
    List<String> itaIndicatorsList = extractApiList("ita_data", "itaIndicatorsList");
    if (!validateListParameter(itaIndicatorsList, "ITA indicators", "conversion")) {
      return;
    }
    List<String> frequencies = extractApiList("ita_data", "frequencyList");

    int yearCount = endYear - startYear + 1;
    int totalCombinations = itaIndicatorsList.size() * frequencies.size() * yearCount;
    LOGGER.info("ITA conversion: {} indicators × {} frequencies × {} years = ~{} files",
        itaIndicatorsList.size(), frequencies.size(), yearCount, totalCombinations);

    String tableName = "ita_data";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createItaDataDimensions(tableName, startYear, endYear, itaIndicatorsList, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Convert
          boolean converted = convertCachedJsonToParquet(tableName, vars);

          // Mark as converted only if conversion succeeded
          if (converted) {
            cacheManifest.markParquetConverted(cacheKey, parquetPath);
          }
        },
        OperationType.CONVERSION);
  }

  /**
   * Downloads industry GDP data using metadata-driven pattern.
   * Uses instance variables for year range and reads all dimensions from schema metadata.
   */
  public void downloadIndustryGdpMetadata() {
    String tableName = "industry_gdp";
    List<String> industries = extractDimensionValues(tableName, "industry");

    if (!validateListParameter(industries, "industries", "download")) {
      return;
    }

    int yearCount = endYear - startYear + 1;
    LOGGER.info("Industry GDP download: {} NAICS industry codes × {} years = ~{} API calls",
        industries.size(), yearCount, industries.size() * yearCount);

    // Use optimized iteration with metadata-only dimensions (no custom provider needed)
    iterateTableOperationsOptimized(
        tableName,
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Converts industry GDP data using metadata-driven pattern.
   * Uses instance variables for year range and reads all dimensions from schema metadata.
   */
  public void convertIndustryGdpMetadata() {
    String tableName = "industry_gdp";
    List<String> industries = extractDimensionValues(tableName, "industry");

    if (!validateListParameter(industries, "industries", "conversion")) {
      return;
    }

    int yearCount = endYear - startYear + 1;
    LOGGER.info("Industry GDP conversion: {} NAICS industry codes × {} years = ~{} files",
        industries.size(), yearCount, industries.size() * yearCount);

    // Use optimized iteration with metadata-only dimensions (no custom provider needed)
    iterateTableOperationsOptimized(
        tableName,
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Convert
          boolean converted = convertCachedJsonToParquet(tableName, vars);

          // Mark as converted only if conversion succeeded
          if (converted) {
            cacheManifest.markParquetConverted(cacheKey, parquetPath);
          }
        },
        OperationType.CONVERSION);
  }

  /**
   * Public wrapper to download reference_nipa_tables using metadata-driven pattern.
   *
   * @return Path to downloaded JSON cache file
   * @throws IOException          if download fails
   * @throws InterruptedException if download is interrupted
   */
  public String downloadReferenceNipaTables() throws IOException, InterruptedException {
    LOGGER.info("BEA reference: Downloading NIPA table catalog from API");
    DownloadResult result = executeDownload("reference_nipa_tables", new HashMap<>());
    return result.jsonPath;
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
    LOGGER.info("BEA reference: Loading NIPA frequency metadata from parquet catalog");

    // Read from parquet files instead of JSON
    // Pattern: type=reference/section=*/nipa_tables.parquet
    String parquetPattern = "type=reference/section=*/nipa_tables.parquet";
    String fullParquetPattern = storageProvider.resolvePath(parquetDirectory, parquetPattern);

    Map<String, Set<String>> tableFrequencies = new HashMap<>();

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      // Query all sections and extract frequency data
      // Column names: 'annual' and 'quarterly' are the boolean frequency columns in reference data
      String sql =
          String.format("SELECT TableName, annual, quarterly FROM read_parquet('%s')",
          fullParquetPattern);

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String tableName = rs.getString("TableName");
          boolean annual = rs.getBoolean("annual");
          boolean quarterly = rs.getBoolean("quarterly");

          Set<String> frequencies = new HashSet<>();
          if (annual) {
            frequencies.add("A");
          }
          if (quarterly) {
            frequencies.add("Q");
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

      long annualOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("A")).count();
      long quarterlyOnly = tableFrequencies.values().stream()
          .filter(f -> f.size() == 1 && f.contains("Q")).count();
      long both = tableFrequencies.values().stream()
          .filter(f -> f.size() == 2).count();

      LOGGER.info("NIPA catalog loaded: {} tables with frequency markers (A={}, Q={}, A+Q={})",
          tableFrequencies.size(), annualOnly, quarterlyOnly, both);

      return tableFrequencies;
    } catch (SQLException e) {
      LOGGER.error("Failed to load NIPA table frequencies from parquet catalog: {}",
          e.getMessage(), e);
      throw new IOException("Failed to load NIPA table frequencies: " + e.getMessage(), e);
    }
  }

  /**
   * Converts reference_nipa_tables JSON to Parquet with DuckDB doing all enrichment.
   * Uses SQL to parse Description for frequencies and TableName for section/family/metric.
   *
   * @throws IOException if conversion fails
   */
  public void convertReferenceNipaTablesWithFrequencies() throws IOException {
    LOGGER.info("BEA reference: Converting NIPA catalog to parquet with DuckDB enrichment");

    String tableName = "reference_nipa_tables";
    int year = 0;  // Sentinel value for reference tables without year dimension
    Map<String, String> variables = new HashMap<>();  // No partition variables for non-partitioned download

    Map<String, String> allParams = new HashMap<>(variables);
    allParams.put("year", String.valueOf(year));
    CacheKey cacheKey = new CacheKey(tableName, allParams);

    if (cacheManifest.isParquetConverted(cacheKey)) {
      LOGGER.info("BEA reference: NIPA catalog already converted to parquet, skipping");
      return;
    }

    // Load pattern from schema and resolve paths
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode downloadConfig = (JsonNode) metadata.get("download");
    String cachePattern = downloadConfig.get("cachePattern").asText();
    String jsonPath = resolveJsonPath(cachePattern, variables);
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    try (Connection duckdb = getDuckDBConnection(cacheStorageProvider)) {
      // Load JSON and enrich in one SQL query
      String enrichSql =
          substituteSqlParameters(loadSqlResource("/sql/bea/enrich_nipa_tables.sql"),
          ImmutableMap.of("jsonPath", fullJsonPath));

      try (Statement stmt = duckdb.createStatement()) {
        stmt.execute(enrichSql);

        // Get distinct sections for partitioned writing
        List<String> sections = new ArrayList<>();
        try (ResultSet rs =
            stmt.executeQuery("SELECT DISTINCT coalesce(section, 'unknown') AS section FROM enriched ORDER BY section")) {
          while (rs.next()) {
            sections.add(rs.getString("section"));
          }
        }

        LOGGER.info("NIPA partitioned by section: {}",
            String.join(", ", sections));

        // Write each section partition
        for (String section : sections) {
          String parquetPath = "type=reference/section=" + section + "/nipa_tables.parquet";
          String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

          String copySql =
              String.format("COPY (SELECT * FROM enriched WHERE coalesce(section, 'unknown') = '%s') "
              + "TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
              section, fullParquetPath);

          stmt.execute(copySql);

          long count = 0;
          try (ResultSet countRs =
              stmt.executeQuery(String.format("SELECT count(*) FROM enriched WHERE coalesce(section, 'unknown') = '%s'", section))) {
            if (countRs.next()) {
              count = countRs.getLong(1);
            }
          }

          LOGGER.info("Wrote {} tables for section {} to {}", count, section, parquetPath);
        }

        // Get total count for completion message
        long totalTables = 0;
        try (ResultSet countRs = stmt.executeQuery("SELECT count(*) FROM enriched")) {
          if (countRs.next()) {
            totalTables = countRs.getLong(1);
          }
        }

        LOGGER.info("National accounts reference conversion complete: {} NIPA tables cataloged into {} parquet sections",
            totalTables, sections.size());
      }
    } catch (SQLException e) {
      throw new IOException("Failed to convert reference_nipa_tables with DuckDB: " + e.getMessage(), e);
    }

    // Mark as converted in cache manifest after successful conversion
    String pattern = (String) metadata.get("pattern");
    Map<String, String> convertParams = new HashMap<>(variables);
    convertParams.put("year", String.valueOf(year));
    CacheKey convertCacheKey = new CacheKey(tableName, convertParams);
    cacheManifest.markParquetConverted(convertCacheKey, pattern);
    cacheManifest.save(operatingDirectory);
  }

  /**
   * Creates dimension provider for regional LineCode catalog (no year dimension).
   */
  private DimensionProvider createRegionalLineCodeDimensions(List<String> tableNamesList) {
    return (dimensionName) -> {
      switch (dimensionName) {
        case "tablename":
          return tableNamesList;
        case "type":
          return List.of("reference_regional_line_codes");
        default:
          return null;
      }
    };
  }

  /**
   * Download BEA Regional LineCode catalog using GetParameterValuesFiltered API.
   * Downloads valid LineCodes for all 54 BEA Regional tables listed in the schema.
   */
  public void downloadRegionalLineCodeCatalog() {
    LOGGER.info("BEA reference: Starting regional LineCode catalog download");

    String tableName = "reference_regional_linecodes";
    // Get tableNamesSet from regional_income table (where it's defined in schema)
    Map<String, Object> tableNamesSet = extractApiSet("regional_income", "tableNamesSet");

    if (tableNamesSet == null || tableNamesSet.isEmpty()) {
      LOGGER.error("BEA reference: No tableNamesSet found in schema for regional_income");
      return;
    }

    // Derive list from set keys
    List<String> tableNamesList = new ArrayList<>(tableNamesSet.keySet());
    LOGGER.info("BEA reference: Downloading LineCodes for {} regional tables", tableNamesList.size());

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createRegionalLineCodeDimensions(tableNamesList),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          // Mark as cached with immutable reference policy
          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              Long.MAX_VALUE, "reference_immutable");
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Convert BEA Regional LineCode catalog JSON to Parquet with DuckDB-based enrichment.
   * Uses SQL to parse table name components and enrich with metadata.
   */
  public void convertRegionalLineCodeCatalog() {
    LOGGER.info("BEA reference: Converting regional LineCode catalog to parquet");

    String tableName = "reference_regional_linecodes";
    // Get tableNamesSet from regional_income table (where it's defined in schema)
    Map<String, Object> tableNamesSet = extractApiSet("regional_income", "tableNamesSet");

    if (tableNamesSet == null || tableNamesSet.isEmpty()) {
      LOGGER.error("BEA reference: No tableNamesSet found in schema for regional_income");
      return;
    }

    // Derive list from set keys
    List<String> tableNamesList = new ArrayList<>(tableNamesSet.keySet());

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createRegionalLineCodeDimensions(tableNamesList),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          String regionalTableName = vars.get("tablename");

          // Check if JSON file has valid schema before conversion
          // Files with [null] will have a "json" column instead of "Key"/"Desc"
          try (Connection duckdb = getDuckDBConnection(cacheStorageProvider)) {
            String schemaSql =
                String.format("SELECT column_name FROM (DESCRIBE SELECT * FROM read_json('%s', format='array', maximum_object_size=104857600) LIMIT 1)",
                jsonPath.replace("'", "''"));

            boolean hasKeyColumn = false;
            try (Statement stmt = duckdb.createStatement();
                 ResultSet rs = stmt.executeQuery(schemaSql)) {
              while (rs.next()) {
                String colName = rs.getString("column_name");
                if ("Key".equalsIgnoreCase(colName)) {
                  hasKeyColumn = true;
                  break;
                }
              }
            } catch (Exception schemaErr) {
              // If schema check fails, log and skip
              LOGGER.warn("BEA reference: Cannot check schema for {} at {}: {}",
                  regionalTableName, jsonPath, schemaErr.getMessage());
              return;
            }

            if (!hasKeyColumn) {
              LOGGER.info("BEA reference: Skipping conversion for {} - no valid linecodes data (no Key column)",
                  regionalTableName);
              // Mark as "converted" with null path to prevent repeated conversion attempts
              cacheManifest.markParquetConverted(cacheKey, null);
              return;
            }
          } catch (Exception connErr) {
            LOGGER.warn("BEA reference: Cannot connect to DuckDB for pre-check: {}", connErr.getMessage());
            return;
          }

          // Convert with DuckDB doing all parsing and enrichment
          try (Connection duckdb = getDuckDBConnection(cacheStorageProvider)) {
            String enrichSql =
                substituteSqlParameters(loadSqlResource("/sql/bea/enrich_regional_linecodes.sql"),
                ImmutableMap.of(
                    "tableName", regionalTableName,
                    "jsonPath", jsonPath,
                    "parquetPath", parquetPath));

            try (Statement stmt = duckdb.createStatement()) {
              stmt.execute(enrichSql);
            }
          } catch (Exception e) {
            LOGGER.error("Error converting LineCodes for table {}: {}", regionalTableName, e.getMessage());
            LOGGER.error("JSON path being read: {}", jsonPath);

            // Diagnostic: Log the actual JSON structure to help debug schema mismatches
            try (Connection duckdb = getDuckDBConnection(cacheStorageProvider)) {
              // Show top-level structure
              String schemaSql =
                  String.format("DESCRIBE SELECT * FROM read_json('%s', format='array', maximum_object_size=104857600) LIMIT 1",
                  jsonPath.replace("'", "''"));

              try (Statement stmt = duckdb.createStatement();
                   ResultSet rs = stmt.executeQuery(schemaSql)) {
                LOGGER.error("JSON schema for {}: Top-level columns found:", regionalTableName);
                while (rs.next()) {
                  LOGGER.error("  - {} ({})", rs.getString("column_name"), rs.getString("column_type"));
                }
              }

              // Show sample of actual data
              String sampleSql =
                  String.format("SELECT * FROM read_json('%s', format='array', maximum_object_size=104857600) LIMIT 1",
                  jsonPath.replace("'", "''"));

              try (Statement stmt = duckdb.createStatement();
                   ResultSet rs = stmt.executeQuery(sampleSql)) {
                LOGGER.error("JSON sample for {}:", regionalTableName);
                ResultSetMetaData meta = rs.getMetaData();
                if (rs.next()) {
                  for (int i = 1; i <= meta.getColumnCount(); i++) {
                    Object value = rs.getObject(i);
                    String preview = value != null ? value.toString() : "null";
                    if (preview.length() > 200) {
                      preview = preview.substring(0, 200) + "...";
                    }
                    LOGGER.error("  {} = {}", meta.getColumnName(i), preview);
                  }
                }
              }
            } catch (Exception diagErr) {
              LOGGER.error("Could not diagnose JSON structure: {}", diagErr.getMessage());
            }

            throw new RuntimeException(e);
          }

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        OperationType.CONVERSION);
  }

  /**
   * Load BEA Regional LineCode catalog from Parquet files using DuckDB.
   * Reads all reference_regional_linecodes partitions and builds a map of TableName to
   * valid LineCodes.
   *
   * @return Map where key=TableName (e.g., "SQINC4") and value=Set of valid LineCodes
   * for that table
   * @throws IOException if catalog files cannot be read
   */
  public Map<String, Set<String>> loadRegionalLineCodeCatalog() throws IOException {
    LOGGER.info("BEA reference: Loading regional LineCode catalog from parquet");

    String tableName = "reference_regional_linecodes";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Build wildcard pattern for DuckDB to read all reference_regional_linecodes files at once
    // Pattern: type=reference/tablename=*/reference_regional_linecodes.parquet
    String fullWildcardPath = storageProvider.resolvePath(parquetDirectory, pattern);

    Map<String, Set<String>> catalogMap = new HashMap<>();

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      // Single query to read all parquet files and group LineCodes by TableName
      String query =
          substituteSqlParameters(loadSqlResource("/sql/bea/load_regional_catalog.sql"),
          ImmutableMap.of("wildcardPath", fullWildcardPath));

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {

        int totalLineCodes = 0;
        while (rs.next()) {
          String tableNameKey = rs.getString("TableName");
          String lineCode = rs.getString("LineCode");

          catalogMap.computeIfAbsent(tableNameKey, k -> new HashSet<>()).add(lineCode);
          totalLineCodes++;
        }

        LOGGER.info("BEA reference: Regional catalog loaded: {} tables, {} total LineCodes",
            catalogMap.size(), totalLineCodes);

        // Show sample for verification
        if (!catalogMap.isEmpty() && LOGGER.isDebugEnabled()) {
          Map.Entry<String, Set<String>> sample = catalogMap.entrySet().iterator().next();
          LOGGER.debug("Sample: {} has {} line codes (e.g., {})",
              sample.getKey(), sample.getValue().size(),
              sample.getValue().stream().limit(3).collect(Collectors.joining(", ")));
        }

      }
    } catch (SQLException e) {
      throw new IOException("Failed to load Regional LineCode catalog via DuckDB: "
          + e.getMessage(), e);
    }

    return catalogMap;
  }

  // ===== YEAR AVAILABILITY OPTIMIZATION =====

  /**
   * Load available years for all specified BEA Regional tables.
   * Uses cached data from DuckDB when available, otherwise fetches from BEA API.
   *
   * <p>This optimization prevents thousands of wasted API calls by determining
   * which years have data for each table BEFORE iterating through linecode combinations.
   *
   * @param tableNames Set of BEA Regional table names to check
   * @return Map of tableName to Set of available years
   */
  public Map<String, Set<Integer>> loadTableYearAvailability(Set<String> tableNames) {
    Map<String, Set<Integer>> result = new HashMap<>();
    // Cast to CacheManifest to access DuckDB store for year availability caching
    CacheManifest econManifest = (CacheManifest) cacheManifest;
    DuckDBCacheStore store = econManifest.getStore();

    int cached = 0;
    int fetched = 0;

    for (String tableName : tableNames) {
      // Check cache first
      Set<Integer> cachedYears = store.getAvailableYearsForTable("bea_regional", tableName);
      if (!cachedYears.isEmpty()) {
        result.put(tableName, cachedYears);
        cached++;
        continue;
      }

      // Fetch from BEA API
      try {
        Set<Integer> years = fetchYearAvailabilityFromApi(tableName);
        if (!years.isEmpty()) {
          // Cache for 30 days (BEA releases data annually, check monthly)
          store.setAvailableYearsForTable("bea_regional", tableName, years, 30);
          result.put(tableName, years);
          fetched++;
        } else {
          // No years returned - use full year range as fallback
          LOGGER.warn("No year availability from API for table {}, using full range", tableName);
          Set<Integer> fallbackYears = new HashSet<>();
          for (int y = startYear; y <= endYear; y++) {
            fallbackYears.add(y);
          }
          result.put(tableName, fallbackYears);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch year availability for {}: {}, using full range",
            tableName, e.getMessage());
        // Fallback to full range on error
        Set<Integer> fallbackYears = new HashSet<>();
        for (int y = startYear; y <= endYear; y++) {
          fallbackYears.add(y);
        }
        result.put(tableName, fallbackYears);
      }
    }

    LOGGER.info("Year availability loaded for {} tables: {} from cache, {} fetched from API",
        tableNames.size(), cached, fetched);

    return result;
  }

  /**
   * Fetch available years for a BEA Regional table from the API.
   *
   * <p>Calls: GET https://apps.bea.gov/api/data
   * ?method=GetParameterValuesFiltered
   * &datasetname=Regional
   * &TargetParameter=Year
   * &TableName={tableName}
   * &ResultFormat=JSON
   *
   * @param tableName The BEA table name (e.g., "SAINC1", "SAGDP5")
   * @return Set of available years, or empty set if call fails
   */
  private Set<Integer> fetchYearAvailabilityFromApi(String tableName) throws IOException, InterruptedException {
    String beaApiKey = System.getProperty("BEA_API_KEY");
    if (beaApiKey == null || beaApiKey.isEmpty()) {
      beaApiKey = System.getenv("BEA_API_KEY");
    }
    if (beaApiKey == null || beaApiKey.isEmpty()) {
      LOGGER.warn("BEA_API_KEY not set, cannot fetch year availability");
      return Collections.emptySet();
    }

    String url = String.format(
        "https://apps.bea.gov/api/data?method=GetParameterValuesFiltered"
            + "&datasetname=Regional&TargetParameter=Year&TableName=%s"
            + "&ResultFormat=JSON&UserID=%s",
        tableName, beaApiKey);

    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
        .uri(java.net.URI.create(url))
        .GET()
        .timeout(java.time.Duration.ofSeconds(30))
        .build();

    enforceRateLimit();
    java.net.http.HttpResponse<String> response =
        httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      LOGGER.warn("BEA API returned status {} for year availability of {}",
          response.statusCode(), tableName);
      return Collections.emptySet();
    }

    // Parse response: {"BEAAPI":{"Results":{"ParamValue":[{"Key":"2020"},{"Key":"2021"},...]}}}
    Set<Integer> years = new HashSet<>();
    try {
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode paramValues = root.path("BEAAPI").path("Results").path("ParamValue");

      // Check for error in response
      JsonNode error = root.path("BEAAPI").path("Results").path("Error");
      if (!error.isMissingNode()) {
        LOGGER.warn("BEA API error for {}: {}", tableName, error.toString());
        return Collections.emptySet();
      }

      if (paramValues.isArray()) {
        for (JsonNode yearNode : paramValues) {
          String yearKey = yearNode.path("Key").asText();
          if (yearKey != null && !yearKey.isEmpty() && !"ALL".equalsIgnoreCase(yearKey)) {
            try {
              years.add(Integer.parseInt(yearKey));
            } catch (NumberFormatException e) {
              // Skip non-numeric year values like "LAST5", "LAST10"
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to parse year availability response for {}: {}",
          tableName, e.getMessage());
    }

    LOGGER.debug("Year availability for {}: {} years (min={}, max={})",
        tableName, years.size(),
        years.isEmpty() ? "N/A" : Collections.min(years),
        years.isEmpty() ? "N/A" : Collections.max(years));

    return years;
  }

  // ========== Table Geography Support ==========

  /**
   * Load geo support for tables, checking cache first then fetching from API.
   * Similar pattern to loadTableYearAvailability.
   *
   * @param tableNames Set of table names to check
   * @return Map of tableName to GeoSupport
   */
  public Map<String, DuckDBCacheStore.GeoSupport> loadTableGeoSupport(Set<String> tableNames) {
    Map<String, DuckDBCacheStore.GeoSupport> result = new HashMap<>();
    CacheManifest econManifest = (CacheManifest) cacheManifest;
    DuckDBCacheStore store = econManifest.getStore();

    int cached = 0;
    int fetched = 0;

    for (String tableName : tableNames) {
      // Check cache first
      DuckDBCacheStore.GeoSupport cachedSupport = store.getGeoSupportForTable("bea_regional", tableName);
      if (cachedSupport != null) {
        result.put(tableName, cachedSupport);
        cached++;
        continue;
      }

      // Fetch from BEA API
      try {
        DuckDBCacheStore.GeoSupport geoSupport = fetchGeoSupportFromApi(tableName);
        if (geoSupport != null) {
          // Cache for 90 days (geo support rarely changes)
          store.setGeoSupportForTable("bea_regional", tableName, geoSupport, 90);
          result.put(tableName, geoSupport);
          fetched++;
        } else {
          // No geo support determined - use pattern-based fallback
          LOGGER.warn("No geo support from API for table {}, using pattern-based fallback", tableName);
          result.put(tableName, getPatternBasedGeoSupport(tableName));
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch geo support for {}: {}, using pattern-based fallback",
            tableName, e.getMessage());
        result.put(tableName, getPatternBasedGeoSupport(tableName));
      }
    }

    LOGGER.info("Geo support loaded for {} tables: {} from cache, {} fetched from API",
        tableNames.size(), cached, fetched);

    return result;
  }

  /**
   * Fetch geo support for a BEA Regional table from the API.
   *
   * <p>Calls: GET https://apps.bea.gov/api/data
   * ?method=GetParameterValuesFiltered
   * &datasetname=Regional
   * &TargetParameter=GeoFips
   * &TableName={tableName}
   * &ResultFormat=JSON
   *
   * <p>Detects supported geo types by looking at the GeoFips codes returned:
   * - STATE: codes like "01000" (state FIPS ending in 000)
   * - COUNTY: codes like "01001" (5-digit county FIPS)
   * - MSA: codes >= 10000 with descriptions containing "Metropolitan Statistical Area"
   * - MIC: codes >= 10000 with descriptions containing "Micropolitan Statistical Area"
   * - PORT: codes ending in 998/999 (state metro/nonmetro portions)
   * - DIV: codes with descriptions containing "Metropolitan Division"
   * - CSA: codes with descriptions containing "Combined Statistical Area"
   *
   * @param tableName The BEA table name
   * @return GeoSupport object, or null if call fails
   */
  private DuckDBCacheStore.GeoSupport fetchGeoSupportFromApi(String tableName)
      throws IOException, InterruptedException {
    String beaApiKey = System.getProperty("BEA_API_KEY");
    if (beaApiKey == null || beaApiKey.isEmpty()) {
      beaApiKey = System.getenv("BEA_API_KEY");
    }
    if (beaApiKey == null || beaApiKey.isEmpty()) {
      LOGGER.warn("BEA_API_KEY not set, cannot fetch geo support");
      return null;
    }

    String url = String.format(
        "https://apps.bea.gov/api/data?method=GetParameterValuesFiltered"
            + "&datasetname=Regional&TargetParameter=GeoFips&TableName=%s"
            + "&ResultFormat=JSON&UserID=%s",
        tableName, beaApiKey);

    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
        .uri(java.net.URI.create(url))
        .GET()
        .timeout(java.time.Duration.ofSeconds(30))
        .build();

    enforceRateLimit();
    java.net.http.HttpResponse<String> response =
        httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      LOGGER.warn("BEA API returned status {} for geo support of {}",
          response.statusCode(), tableName);
      return null;
    }

    // Parse response to detect geo types
    boolean supportsState = false;
    boolean supportsCounty = false;
    boolean supportsMsa = false;
    boolean supportsMic = false;
    boolean supportsPort = false;
    boolean supportsDiv = false;
    boolean supportsCsa = false;

    try {
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode paramValues = root.path("BEAAPI").path("Results").path("ParamValue");

      // Check for error in response
      JsonNode error = root.path("BEAAPI").path("Results").path("Error");
      if (!error.isMissingNode()) {
        LOGGER.warn("BEA API error for {}: {}", tableName, error.toString());
        return null;
      }

      if (paramValues.isArray()) {
        for (JsonNode geoNode : paramValues) {
          String key = geoNode.path("Key").asText();
          String desc = geoNode.path("Desc").asText();

          if (key == null || key.isEmpty()) {
            continue;
          }

          // Detect geo type from key/description patterns
          // STATE: "00000" (US), "01000" (Alabama), etc - codes ending in 000
          // Special: "00000" = United States
          if ("00000".equals(key) || (key.length() == 5 && key.endsWith("000"))) {
            supportsState = true;
          }
          // COUNTY: 5-digit codes not ending in 000, 998, or 999
          else if (key.length() == 5 && !key.endsWith("000")
              && !key.endsWith("998") && !key.endsWith("999")) {
            int keyNum;
            try {
              keyNum = Integer.parseInt(key);
              // County codes < 10000 (real county FIPS are 01001-56999)
              if (keyNum < 10000) {
                supportsCounty = true;
              }
            } catch (NumberFormatException e) {
              // Skip non-numeric
            }
          }
          // PORT: codes ending in 998/999 (metro/nonmetro portions)
          if (key.endsWith("998") || key.endsWith("999")) {
            if (desc.contains("Metropolitan Portion") || desc.contains("Nonmetropolitan Portion")) {
              supportsPort = true;
            }
          }
          // MSA/MIC/DIV/CSA: 5-digit codes >= 10000 or detect from description
          if (desc != null) {
            if (desc.contains("Metropolitan Statistical Area")) {
              supportsMsa = true;
            }
            if (desc.contains("Micropolitan Statistical Area")) {
              supportsMic = true;
            }
            if (desc.contains("Metropolitan Division")) {
              supportsDiv = true;
            }
            if (desc.contains("Combined Statistical Area")) {
              supportsCsa = true;
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to parse geo support response for {}: {}",
          tableName, e.getMessage());
      return null;
    }

    DuckDBCacheStore.GeoSupport geoSupport = new DuckDBCacheStore.GeoSupport(
        supportsState, supportsCounty, supportsMsa, supportsMic, supportsPort, supportsDiv, supportsCsa);

    LOGGER.debug("Geo support for {}: {}", tableName, geoSupport.getSupportedGeoTypes());

    return geoSupport;
  }

  /**
   * Get pattern-based geo support as fallback when API unavailable.
   * Uses table name prefix to determine likely geo support.
   */
  private DuckDBCacheStore.GeoSupport getPatternBasedGeoSupport(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      return new DuckDBCacheStore.GeoSupport(true, false, false, false, false, false, false);
    }

    // State tables
    if (tableName.startsWith("SA") || tableName.startsWith("SQ")) {
      return new DuckDBCacheStore.GeoSupport(true, false, false, false, false, false, false);
    }
    // County tables (conservative - assume county only)
    if (tableName.startsWith("CA")) {
      return new DuckDBCacheStore.GeoSupport(false, true, false, false, false, false, false);
    }
    // Metro tables
    if (tableName.startsWith("MA")) {
      return new DuckDBCacheStore.GeoSupport(false, false, true, false, false, false, false);
    }
    // Micropolitan tables
    if (tableName.startsWith("MI")) {
      return new DuckDBCacheStore.GeoSupport(false, false, false, true, false, false, false);
    }
    // Default to state
    return new DuckDBCacheStore.GeoSupport(true, false, false, false, false, false, false);
  }

  /** Cached geo support map, loaded once per session. */
  private Map<String, DuckDBCacheStore.GeoSupport> geoSupportCache = null;

  /**
   * Get valid geo fips types for a table, using cached API data.
   * Falls back to pattern-based logic if cache is not loaded.
   */
  private Set<String> getValidGeoFipsForTable(String tableName) {
    // First try to use cached API data
    if (geoSupportCache != null) {
      DuckDBCacheStore.GeoSupport cached = geoSupportCache.get(tableName);
      if (cached != null) {
        Set<String> types = cached.getSupportedGeoTypes();
        if (!types.isEmpty()) {
          return types;
        }
      }
    }

    // Fall back to pattern-based logic
    return getPatternBasedGeoFipsForTable(tableName);
  }

  /**
   * Pattern-based geo fips determination (original logic).
   * Used as fallback when API data is not available.
   */
  private Set<String> getPatternBasedGeoFipsForTable(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      return Collections.emptySet();
    }

    // State-level tables (SA* = State Annual, SQ* = State Quarterly)
    if (tableName.startsWith("SA") || tableName.startsWith("SQ")) {
      return Collections.singleton("STATE");
    }

    // County-level tables (CA* = County Annual)
    // Conservative fallback: assume county only (API will tell us if MSA is supported)
    if (tableName.startsWith("CA")) {
      return Collections.singleton("COUNTY");
    }

    // Metropolitan area tables (MA* = Metro Annual)
    if (tableName.startsWith("MA")) {
      return Collections.singleton("MSA");
    }

    // Micropolitan area tables (MI*)
    if (tableName.startsWith("MI")) {
      return Collections.singleton("MIC");
    }

    // Port area tables (PARPP, etc.)
    if (tableName.startsWith("PARPP") || tableName.startsWith("PORT")) {
      return Collections.singleton("PORT");
    }

    // Metropolitan division tables
    if (tableName.contains("DIV")) {
      return Collections.singleton("DIV");
    }

    // Combined statistical area tables
    if (tableName.contains("CSA")) {
      return Collections.singleton("CSA");
    }

    // Territory tables (TA*)
    if (tableName.startsWith("TA")) {
      return Collections.singleton("STATE");
    }

    // Default: STATE (most common case)
    LOGGER.warn("Unknown table prefix for {}, defaulting to STATE geography", tableName);
    return Collections.singleton("STATE");
  }

  // ========== Regional Income Valid Combinations ==========

  /**
   * Represents a valid regional income data combination.
   * Used to iterate only over valid table/year/geo/linecode combinations,
   * avoiding the Cartesian product explosion.
   */
  public static class RegionalCombination {
    public final String tableName;
    public final String lineCode;
    public final String geoFipsSet;
    public final int year;

    public RegionalCombination(String tableName, String lineCode, String geoFipsSet, int year) {
      this.tableName = tableName;
      this.lineCode = lineCode;
      this.geoFipsSet = geoFipsSet;
      this.year = year;
    }

    /** Returns cache key format: tablename:linecode:geo:year */
    public String toCacheKeyPart() {
      return tableName + ":" + lineCode + ":" + geoFipsSet + ":" + year;
    }

    /** Returns variables map for use with download/convert operations. */
    public Map<String, String> toVariables() {
      Map<String, String> vars = new HashMap<>();
      vars.put("tablename", tableName);
      vars.put("line_code", lineCode);
      vars.put("geo_fips_set", geoFipsSet);
      vars.put("year", String.valueOf(year));
      return vars;
    }
  }

  /**
   * Build list of valid regional income combinations based on:
   * - Available tables from schema
   * - LineCodes from reference catalog
   * - GeoFips sets appropriate for each table type
   * - Year availability from BEA API (cached)
   * - SIC/NAICS industry classification year rules
   *
   * @param startYear First year to include
   * @param endYear   Last year to include
   * @return List of valid combinations, or empty list if catalogs unavailable
   */
  public List<RegionalCombination> buildValidRegionalCombinations(int startYear, int endYear) {
    // Load LineCodes from reference_regional_linecodes catalog
    Map<String, Set<String>> lineCodeCatalog;
    try {
      lineCodeCatalog = loadRegionalLineCodeCatalog();
      if (lineCodeCatalog.isEmpty()) {
        LOGGER.error("Regional LineCode catalog is empty. Cannot build valid combinations.");
        return Collections.emptyList();
      }
    } catch (IOException e) {
      LOGGER.error("Failed to load Regional LineCode catalog: {}", e.getMessage());
      return Collections.emptyList();
    }

    Map<String, Object> tablenames = extractApiSet("regional_income", "tableNamesSet");
    if (tablenames == null || tablenames.isEmpty()) {
      LOGGER.error("No tableNamesSet found in schema for regional_income");
      return Collections.emptyList();
    }

    // Collect valid table names that have linecodes
    Set<String> validTableNames = new HashSet<>();
    for (String tableNameKey : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      if (lineCodesForTable != null && !lineCodesForTable.isEmpty()) {
        validTableNames.add(tableNameKey);
      }
    }

    // Load year availability for all valid tables
    LOGGER.info("Loading year availability for {} BEA regional tables...", validTableNames.size());
    Map<String, Set<Integer>> tableYearAvailability = loadTableYearAvailability(validTableNames);

    // Load geo support for all valid tables (cached API data)
    LOGGER.info("Loading geo support for {} BEA regional tables...", validTableNames.size());
    geoSupportCache = loadTableGeoSupport(validTableNames);

    // Build full year range as safe fallback (used when year availability is unknown)
    Set<Integer> fullYearRange = new HashSet<>();
    for (int y = startYear; y <= endYear; y++) {
      fullYearRange.add(y);
    }

    // Build valid combinations
    List<RegionalCombination> validCombinations = new ArrayList<>();
    int totalPossible = 0;
    int skipped = 0;
    int tablesUsingFallback = 0;

    for (String tableNameKey : validTableNames) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      Set<String> validGeoFipsForTable = getValidGeoFipsForTable(tableNameKey);

      // Safe fallback: if year availability is unknown, include all years rather than skip all
      Set<Integer> availableYears = tableYearAvailability.get(tableNameKey);
      if (availableYears == null || availableYears.isEmpty()) {
        LOGGER.debug("Table {} has no year availability data, using full range {}-{}",
            tableNameKey, startYear, endYear);
        availableYears = fullYearRange;
        tablesUsingFallback++;
      }

      for (int year = startYear; year <= endYear; year++) {
        int combosForYear = lineCodesForTable.size() * validGeoFipsForTable.size();
        totalPossible += combosForYear;

        // Skip years not available for this table (from API, or full range if unknown)
        if (!availableYears.contains(year)) {
          skipped += combosForYear;
          continue;
        }

        // Skip based on SIC/NAICS rules
        if (!isTableValidForYear(tableNameKey, year)) {
          skipped += combosForYear;
          continue;
        }

        // Add valid combinations for this table-year
        for (String lineCode : lineCodesForTable) {
          for (String geoFipsCode : validGeoFipsForTable) {
            validCombinations.add(new RegionalCombination(tableNameKey, lineCode, geoFipsCode, year));
          }
        }
      }
    }

    double reductionPct = totalPossible > 0 ? (100.0 * skipped / totalPossible) : 0;
    LOGGER.info("Regional income combinations: {} valid of {} possible ({} skipped, {:.1f}% reduction, {} tables used fallback)",
        validCombinations.size(), totalPossible, skipped, reductionPct, tablesUsingFallback);

    return validCombinations;
  }

  /**
   * Filter combinations to only those not yet cached.
   * Uses the cache manifest to check which combinations need processing.
   *
   * @param tableName The table name for cache key construction
   * @param combinations List of combinations to filter
   * @param operationType DOWNLOAD or CONVERSION
   * @return List of combinations that need processing
   */
  private List<RegionalCombination> filterUncachedCombinations(
      String tableName,
      List<RegionalCombination> combinations,
      OperationType operationType) {

    List<RegionalCombination> uncached = new ArrayList<>();

    for (RegionalCombination combo : combinations) {
      Map<String, String> vars = combo.toVariables();
      vars.put("type", tableName);
      CacheKey cacheKey = new CacheKey(tableName, vars);

      if (operationType == OperationType.DOWNLOAD) {
        if (!cacheManifest.isCached(cacheKey)) {
          uncached.add(combo);
        }
      } else if (operationType == OperationType.CONVERSION) {
        // Only convert if source JSON is cached AND parquet hasn't been converted
        // This prevents conversion attempts for entries that were never downloaded
        if (cacheManifest.isCached(cacheKey) && !cacheManifest.isParquetConverted(cacheKey)) {
          uncached.add(combo);
        }
      }
    }

    return uncached;
  }

}
