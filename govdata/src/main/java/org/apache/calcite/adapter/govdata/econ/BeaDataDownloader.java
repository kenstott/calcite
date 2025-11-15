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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
  private final Map<String, Set<String>> tableFrequencies;
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
   * Handles all BEA-specific initialization including loading catalogs.
   */
  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
    this.parquetDir = parquetDir;

    // Extract iteration lists from schema metadata
    List<String> keyIndustriesListFromSchema = extractIterationList("industry_gdp", "keyIndustriesList");

    // Load active NIPA tables from catalog - this is the ONLY source of truth
    Map<String, Set<String>> loadedTableFrequencies;
    try {
      loadedTableFrequencies = loadNipaTableFrequencies();
      if (loadedTableFrequencies.isEmpty()) {
        throw new IllegalStateException("reference_nipa_tables catalog is empty - cannot proceed");
      }
      LOGGER.info("Loaded {} NIPA tables from reference_nipa_tables catalog",
          loadedTableFrequencies.size());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load reference_nipa_tables catalog. This is required for NIPA data downloads. "
          + "Ensure reference data has been downloaded first via downloadReferenceData(). "
          + "Error: " + e.getMessage(), e);
    }

    this.nipaTablesList = new ArrayList<>(loadedTableFrequencies.keySet());
    this.tableFrequencies = loadedTableFrequencies;
    this.keyIndustriesList = keyIndustriesListFromSchema;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir) {
    super(cacheDir,
        StorageProviderFactory.createFromUrl(cacheDir),
        StorageProviderFactory.createFromUrl(cacheDir));
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
   */
  @Override public void downloadReferenceData() {
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
  @Override public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
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
   */
  @Override public void convertAll(int startYear, int endYear) {
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
   * Creates dimension provider for national accounts data.
   */
  private DimensionProvider createNationalAccountsDimensions(int startYear, int endYear,
      List<String> tableNames, List<String> frequencies) {
    return (dimensionName) -> {
      switch (dimensionName) {
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

    LOGGER.info("Downloading {} NIPA tables for years {}-{}", nipaTablesList.size(), startYear,
        endYear);

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
        "download");
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

    LOGGER.info("Converting {} NIPA tables to Parquet for years {}-{}", nipaTablesList.size(),
        startYear, endYear);

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
          convertCachedJsonToParquet(tableName, vars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        "conversion");
  }

  /**
   * Returns the valid GeoFips codes for a given BEA Regional table.
   * Each table type only works with specific geography levels based on its prefix.
   *
   * @param tableName BEA Regional table name (e.g., "SAINC7N", "CAINC1")
   * @return Set of valid GeoFips codes for this table (e.g., {"STATE"}, {"COUNTY", "MSA"})
   */
  private Set<String> getValidGeoFipsForTable(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      return Collections.emptySet();
    }

    // State-level tables (SA* = State Annual, SQ* = State Quarterly)
    if (tableName.startsWith("SA") || tableName.startsWith("SQ")) {
      return Collections.singleton("STATE");
    }

    // County-level tables (CA* = County Annual)
    if (tableName.startsWith("CA")) {
      return new HashSet<>(Arrays.asList("COUNTY", "MSA"));
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
      return Collections.singleton("STATE");  // Territories use STATE code
    }

    // Default: STATE (most common case)
    LOGGER.warn("Unknown table prefix for {}, defaulting to STATE geography", tableName);
    return Collections.singleton("STATE");
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
    Set<String> validFreqs = tableFrequencies.getOrDefault(tableName, Collections.singleton("A"));
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
   * Downloads regional income data using metadata-driven pattern.
   * Loads valid LineCodes from the reference_regional_linecodes catalog.
   * Automatically filters tables by valid year ranges based on industry classification
   * (SIC vs NAICS).
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

    // Build flat list of all (tablename, line_code, geo_fips) combinations with individual parameters
    // Filter GeoFips based on table type and years based on industry classification
    // to avoid invalid API parameter combinations
    List<Map<String, String>> parameterCombinations = new ArrayList<>();
    int skippedGeoFipsCombinations = 0;
    int skippedYearCombinations = 0;
    for (String tableNameKey : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("No LineCodes found in catalog for table {}, skipping", tableNameKey);
        continue;
      }

      // Get valid GeoFips codes for this table type (e.g., STATE for SA* tables)
      Set<String> validGeoFipsForTable = getValidGeoFipsForTable(tableNameKey);

      for (String lineCode : lineCodesForTable) {
        for (String geoFipsCode : validGeoFipsForTable) {
          Map<String, String> params = new HashMap<>();
          params.put("tablename", tableNameKey);
          params.put("line_code", lineCode);
          params.put("geo_fips_set", geoFipsCode);
          parameterCombinations.add(params);
        }
      }

      // Log how many invalid geo combinations we avoided
      int totalLineCodes = lineCodesForTable.size();
      Map<String, Object> allGeoFips = extractApiSet(tableName, "geoFipsSet");
      int invalidGeoCount = allGeoFips.size() - validGeoFipsForTable.size();
      skippedGeoFipsCombinations += totalLineCodes * invalidGeoCount;

      // Count how many year combinations we'll skip based on industry classification
      for (int year = startYear; year <= endYear; year++) {
        if (!isTableValidForYear(tableNameKey, year)) {
          skippedYearCombinations += lineCodesForTable.size() * validGeoFipsForTable.size();
        }
      }
    }

    if (skippedGeoFipsCombinations > 0) {
      LOGGER.info("Filtered out {} invalid table-GeoFips combinations based on table type constraints",
          skippedGeoFipsCombinations);
    }
    if (skippedYearCombinations > 0) {
      LOGGER.info("Filtered out {} invalid table-year combinations based on industry classification " +
          "(SIC tables: <=2000, NAICS tables: >=2001, Historical tables: <=2005)", skippedYearCombinations);
    }

    LOGGER.info("Downloading regional income data for years {}-{} ({} table-linecode-geo combinations, " +
        "{} valid table-year combinations)",
        startYear, endYear, parameterCombinations.size(),
        parameterCombinations.size() * (endYear - startYear + 1) - skippedYearCombinations);

    // Use custom iteration with individual parameters instead of combo strings
    iterateWithParameters(tableName, parameterCombinations, startYear, endYear,
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Skip this combination if table is not valid for this year
          String tableNameKey = vars.get("tablename");
          if (!isTableValidForYear(tableNameKey, year)) {
            return;  // Skip download
          }

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        "download");
  }

  /**
   * Converts regional income data using metadata-driven pattern.
   * Loads valid LineCodes from the reference_regional_linecodes catalog.
   * Automatically filters tables by valid year ranges based on industry classification
   * (SIC vs NAICS).
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

    // Build flat list of all (tablename, line_code, geo_fips_set) combinations
    // Filter GeoFips based on table type and years based on industry classification
    // to avoid invalid API parameter combinations
    List<String> combos = new ArrayList<>();
    int skippedGeoFipsCombinations = 0;
    int skippedYearCombinations = 0;
    for (String tableNameKey : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("No LineCodes found in catalog for table {}, skipping", tableNameKey);
        continue;
      }

      // Get valid GeoFips codes for this table type (e.g., STATE for SA* tables)
      Set<String> validGeoFipsForTable = getValidGeoFipsForTable(tableNameKey);

      for (String lineCode : lineCodesForTable) {
        for (String geoFipsCode : validGeoFipsForTable) {
          combos.add(tableNameKey + ":" + lineCode + ":" + geoFipsCode);
        }
      }

      // Log how many invalid geo combinations we avoided
      int totalLineCodes = lineCodesForTable.size();
      Map<String, Object> allGeoFips = extractApiSet(tableName, "geoFipsSet");
      int invalidGeoCount = allGeoFips.size() - validGeoFipsForTable.size();
      skippedGeoFipsCombinations += totalLineCodes * invalidGeoCount;

      // Count how many year combinations we'll skip based on industry classification
      for (int year = startYear; year <= endYear; year++) {
        if (!isTableValidForYear(tableNameKey, year)) {
          skippedYearCombinations += lineCodesForTable.size() * validGeoFipsForTable.size();
        }
      }
    }

    if (skippedGeoFipsCombinations > 0) {
      LOGGER.info("Filtered out {} invalid table-GeoFips combinations based on table type constraints",
          skippedGeoFipsCombinations);
    }
    if (skippedYearCombinations > 0) {
      LOGGER.info("Filtered out {} invalid table-year combinations based on industry classification " +
          "(SIC tables: <=2000, NAICS tables: >=2001, Historical tables: <=2005)", skippedYearCombinations);
    }

    LOGGER.info("Converting regional income data to Parquet for years {}-{} ({} table-linecode-geo combinations, " +
        "{} valid table-year combinations)",
        startYear, endYear, combos.size(),
        combos.size() * (endYear - startYear + 1) - skippedYearCombinations);

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    // Note: Year filtering happens in the operation lambda
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year":
              return yearRange(startYear, endYear);
            case "combo":
              return combos;
            default:
              return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Parse combo and execute conversion
          String[] parts = vars.get("combo").split(":", 3);
          String tableNameKey = parts[0];

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return;  // Skip conversion
          }

          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("tablename", tableNameKey);
          fullVars.put("line_code", parts[1]);
          fullVars.put("geo_fips_set", parts[2]);

          // Convert
          convertCachedJsonToParquet(tableName, fullVars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        "conversion");
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

      Map<String, String> allParams = new HashMap<>(variables);
      allParams.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey(tableName, allParams);

      if (isParquetConvertedOrExists(cacheKey, jsonPath, parquetPath)) {
        skippedCount++;
        continue;
      }

      try {
        convertCachedJsonToParquet(tableName, variables);
        cacheManifest.markParquetConverted(cacheKey, parquetPath);
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
   * Creates dimension provider for ITA data.
   */
  private DimensionProvider createItaDataDimensions(int startYear, int endYear,
      List<String> itaIndicatorsList, List<String> frequencies) {
    return (dimensionName) -> {
      switch (dimensionName) {
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
    LOGGER.info("Downloading {} ITA indicators ({} frequencies) for years {}-{}",
        itaIndicatorsList.size(), frequencies.size(), startYear, endYear);

    String tableName = "ita_data";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createItaDataDimensions(startYear, endYear, itaIndicatorsList, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        "download");
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

    LOGGER.info("Converting {} ITA indicators ({} frequencies) to Parquet for years {}-{}",
        itaIndicatorsList.size(), frequencies.size(), startYear, endYear);

    String tableName = "ita_data";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createItaDataDimensions(startYear, endYear, itaIndicatorsList, frequencies),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Convert
          convertCachedJsonToParquet(tableName, vars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        "conversion");
  }

  /**
   * Creates dimension provider for industry GDP data.
   */
  private DimensionProvider createIndustryGdpDimensions(int startYear, int endYear,
      List<String> keyIndustriesList) {
    return (dimensionName) -> {
      switch (dimensionName) {
        case "year":
          return yearRange(startYear, endYear);
        case "industry":
          return keyIndustriesList;
        case "frequency":
          return Collections.singletonList("A");  // Industry GDP is Annual only
        default:
          return null;
      }
    };
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
    if (!validateListParameter(keyIndustriesList, "industries", "download")) {
      return;
    }

    LOGGER.info("Downloading {} industries for years {}-{}", keyIndustriesList.size(), startYear,
        endYear);

    String tableName = "industry_gdp";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createIndustryGdpDimensions(startYear, endYear, keyIndustriesList),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        "download");
  }

  /**
   * Converts industry GDP data using metadata-driven pattern.
   */
  public void convertIndustryGdpMetadata(int startYear, int endYear,
      List<String> keyIndustriesList) {
    if (!validateListParameter(keyIndustriesList, "industries", "conversion")) {
      return;
    }

    LOGGER.info("Converting {} industries to Parquet for years {}-{}", keyIndustriesList.size(),
        startYear, endYear);

    String tableName = "industry_gdp";

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createIndustryGdpDimensions(startYear, endYear, keyIndustriesList),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Convert
          convertCachedJsonToParquet(tableName, vars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        "conversion");
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
      try (InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
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
   * Converts reference_nipa_tables JSON to Parquet with DuckDB doing all enrichment.
   * Uses SQL to parse Description for frequencies and TableName for section/family/metric.
   *
   * @throws IOException if conversion fails
   */
  public void convertReferenceNipaTablesWithFrequencies() throws IOException {
    LOGGER.info("Converting reference_nipa_tables to parquet with DuckDB-based enrichment");

    String tableName = "reference_nipa_tables";
    int year = 0;  // Sentinel value for reference tables without year dimension
    Map<String, String> variables = new HashMap<>();  // No partition variables for non-partitioned download

    Map<String, String> allParams = new HashMap<>(variables);
    allParams.put("year", String.valueOf(year));
    CacheKey cacheKey = new CacheKey(tableName, allParams);

    if (cacheManifest.isParquetConverted(cacheKey)) {
      LOGGER.info("⚡ reference_nipa_tables already converted to parquet, skipping");
      return;
    }

    // Load pattern from schema and resolve paths
    Map<String, Object> metadata = loadTableMetadata(tableName);
    Map<String, Object> downloadConfig = (Map<String, Object>) metadata.get("download");
    String cachePattern = (String) downloadConfig.get("cachePattern");
    String jsonPath = resolveJsonPath(cachePattern, variables);
    String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
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

        LOGGER.info("Writing {} sections to partitioned parquet files", sections.size());

        // Write each section partition
        for (String section : sections) {
          String parquetPath = "type=reference/section=" + section + "/nipa_tables.parquet";
          String fullParquetPath = storageProvider.resolvePath(parquetDirectory, parquetPath);

          // Ensure parent directory exists
          ensureParentDirectory(fullParquetPath);

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

        LOGGER.info("Converted reference_nipa_tables to parquet across {} sections", sections.size());
      }
    } catch (SQLException e) {
      throw new IOException("Failed to convert reference_nipa_tables with DuckDB: " + e.getMessage(), e);
    }

    // Mark as converted in cache manifest after successful conversion
    String pattern = (String) metadata.get("pattern");
    Map<String, String> convertParams = new HashMap<>(variables != null ? variables : new HashMap<>());
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
    LOGGER.info("Downloading BEA Regional LineCode catalog");

    String tableName = "reference_regional_linecodes";
    List<String> tableNamesList = extractApiList(tableName, "tableNamesList");

    if (tableNamesList == null || tableNamesList.isEmpty()) {
      LOGGER.error("No tableNamesList found in schema for reference_regional_linecodes");
      return;
    }

    LOGGER.info("Downloading LineCodes for {} BEA Regional tables", tableNamesList.size());

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
        "download");
  }

  /**
   * Convert BEA Regional LineCode catalog JSON to Parquet with DuckDB-based enrichment.
   * Uses SQL to parse table name components and enrich with metadata.
   */
  public void convertRegionalLineCodeCatalog() {
    LOGGER.info("Converting BEA Regional LineCode catalog to Parquet with DuckDB");

    String tableName = "reference_regional_linecodes";
    List<String> tableNamesList = extractApiList(tableName, "tableNamesList");

    if (tableNamesList == null || tableNamesList.isEmpty()) {
      LOGGER.error("No tableNamesList found in schema for reference_regional_linecodes");
      return;
    }

    // Use optimized iteration with DuckDB-based cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        createRegionalLineCodeDimensions(tableNamesList),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          String regionalTableName = vars.get("tablename");

          // Convert with DuckDB doing all parsing and enrichment
          try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
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
            throw new RuntimeException(e);
          }

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        "conversion");
  }

  /**
   * Custom iteration helper for parameter combinations.
   * Generates DownloadRequests with individual parameters instead of combo strings,
   * then uses DuckDB bulk cache filtering.
   *
   * @param tableName Table name for cache manifest
   * @param parameterCombinations List of parameter maps (one per combination)
   * @param startYear First year to process
   * @param endYear Last year to process
   * @param operation Operation to execute for uncached combinations
   * @param operationDescription Description for logging (e.g., "download", "conversion")
   */
  private void iterateWithParameters(
      String tableName,
      List<Map<String, String>> parameterCombinations,
      int startYear,
      int endYear,
      TableOperation operation,
      String operationDescription) {

    LOGGER.info("Starting {} operations for {} ({} parameter combinations x {} years = {} total)",
        operationDescription, tableName, parameterCombinations.size(),
        (endYear - startYear + 1),
        parameterCombinations.size() * (endYear - startYear + 1));

    // 1. Generate all DownloadRequests with individual parameters
    List<CacheManifestQueryHelper.DownloadRequest> allRequests = new ArrayList<>();
    for (Map<String, String> params : parameterCombinations) {
      for (int year = startYear; year <= endYear; year++) {
        allRequests.add(new CacheManifestQueryHelper.DownloadRequest(tableName, year, params));
      }
    }

    LOGGER.debug("Generated {} download request combinations", allRequests.size());

    // 2. Filter using DuckDB SQL query (FAST!)
    List<CacheManifestQueryHelper.DownloadRequest> needed;
    try {
      long startMs = System.currentTimeMillis();
      String manifestPath = operatingDirectory + "/cache_manifest.json";
      needed = CacheManifestQueryHelper.filterUncachedRequestsOptimal(manifestPath, allRequests);
      long elapsedMs = System.currentTimeMillis() - startMs;

      LOGGER.info("DuckDB cache filtering: {} uncached out of {} total ({}ms, {}% reduction)",
          needed.size(), allRequests.size(), elapsedMs,
          (int) ((1.0 - (double) needed.size() / allRequests.size()) * 100));

    } catch (Exception e) {
      LOGGER.warn("DuckDB cache filtering failed: {}", e.getMessage());
      LOGGER.debug("Fallback details: ", e);
      // Execute all requests without filtering
      needed = allRequests;
    }

    // 3. Execute only the needed operations
    int executed = 0;
    int skipped = allRequests.size() - needed.size();

    // Load table metadata once for path resolution
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    for (CacheManifestQueryHelper.DownloadRequest req : needed) {
      try {
        Map<String, String> allParams = new HashMap<>(req.parameters);
        allParams.put("year", String.valueOf(req.year));
        CacheKey cacheKey = new CacheKey(tableName, allParams);

        // Resolve paths using pattern (fully resolved, not relative)
        String relativeJsonPath = resolveJsonPath(pattern, allParams);
        String relativeParquetPath = resolveParquetPath(pattern, allParams);
        String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, relativeJsonPath);
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);

        operation.execute(cacheKey, allParams, fullJsonPath, fullParquetPath, null);
        executed++;

        if (executed % 10 == 0) {
          LOGGER.info("{} {}/{} operations (skipped {} cached)",
              operationDescription, executed, needed.size(), skipped);
        }

      } catch (Exception e) {
        LOGGER.error("Error during {} for {} with params {}: {}",
            operationDescription, tableName, req.parameters, e.getMessage());
      }
    }

    LOGGER.info("Completed {} operations: executed {}, skipped {} (cached)",
        operationDescription, executed, skipped);

    // Save manifest
    cacheManifest.save(operatingDirectory);
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
    LOGGER.info("Loading Regional LineCode catalog from Parquet via DuckDB");

    String tableName = "reference_regional_linecodes";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Build wildcard pattern for DuckDB to read all reference_regional_linecodes files at once
    // Pattern: type=reference/tablename=*/reference_regional_linecodes.parquet
    String fullWildcardPath = storageProvider.resolvePath(parquetDirectory, pattern);

    Map<String, Set<String>> catalogMap = new HashMap<>();

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
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

        LOGGER.info("Loaded Regional LineCode catalog via DuckDB: {} tables, {} total LineCodes",
            catalogMap.size(), totalLineCodes);

        // Log debug info for each table
        catalogMap.forEach((table, codes) ->
            LOGGER.debug("Loaded {} LineCodes for table {}", codes.size(), table));

      }
    } catch (SQLException e) {
      throw new IOException("Failed to load Regional LineCode catalog via DuckDB: "
          + e.getMessage(), e);
    }

    return catalogMap;
  }

}
