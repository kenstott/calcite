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

    String tableName = "national_accounts";

    // Build flat list of all table-frequency combinations
    List<String> tableFreqCombos = new ArrayList<>();
    for (String nipaTable : nipaTablesList) {
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));
      for (String freq : frequencies) {
        tableFreqCombos.add(nipaTable + ":" + freq);
      }
    }

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("table_freq", tableFreqCombos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse table_freq back into separate variables
          String[] parts = vars.get("table_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>(vars);
          fullVars.put("tablename", parts[0]);
          fullVars.put("frequency", parts[1]);
          return isCachedOrExists(tableName, year, fullVars);
        },
        (year, vars) -> {
          // Parse table_freq and execute download
          String[] parts = vars.get("table_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("tablename", parts[0]);
          fullVars.put("frequency", parts[1]);

          String cachedPath =
              cacheStorageProvider.resolvePath(
                  cacheDirectory, executeDownload(tableName, fullVars));

          long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
          cacheManifest.markCached(tableName, year, fullVars, cachedPath, fileSize);
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
    if (nipaTablesList == null || nipaTablesList.isEmpty()) {
      LOGGER.warn("No NIPA tables provided for conversion");
      return;
    }

    LOGGER.info("Converting {} NIPA tables to Parquet for years {}-{}", nipaTablesList.size(),
        startYear, endYear);

    String tableName = "national_accounts";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Build flat list of all table-frequency combinations
    List<String> tableFreqCombos = new ArrayList<>();
    for (String nipaTable : nipaTablesList) {
      Set<String> frequencies =
          tableFrequencies.getOrDefault(nipaTable, Collections.singleton("A"));
      for (String freq : frequencies) {
        tableFreqCombos.add(nipaTable + ":" + freq);
      }
    }

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("table_freq", tableFreqCombos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse table_freq back into separate variables
          String[] parts = vars.get("table_freq").split(":", 2);

          // Build variables for path resolution
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("frequency", parts[1]);
          fullVars.put("TableName", parts[0]);

          // Resolve paths
          String rawPath =
              cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, fullVars));
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Check using params with TableName only
          Map<String, String> params = new HashMap<>();
          params.put("TableName", parts[0]);

          return isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath);
        },
        (year, vars) -> {
          // Parse table_freq and execute conversion
          String[] parts = vars.get("table_freq").split(":", 2);

          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("frequency", parts[1]);
          fullVars.put("TableName", parts[0]);

          // Resolve parquet path for manifest
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Convert
          convertCachedJsonToParquet(tableName, fullVars);

          // Mark as converted with params containing TableName
          Map<String, String> params = new HashMap<>();
          params.put("TableName", parts[0]);
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
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
      return new HashSet<>(java.util.Arrays.asList("COUNTY", "MSA"));
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

    // Build flat list of all (tablename, line_code, geo_fips) combinations
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

    LOGGER.info("Downloading regional income data for years {}-{} ({} table-linecode-geo combinations, " +
        "{} valid table-year combinations)",
        startYear, endYear, combos.size(),
        combos.size() * (endYear - startYear + 1) - skippedYearCombinations);

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("combo", combos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework with year filtering
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse combo back into separate variables
          String[] parts = vars.get("combo").split(":", 3);
          String tableNameKey = parts[0];

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return true;  // Return true to skip (treat as already cached)
          }

          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("tablename", tableNameKey);
          fullVars.put("line_code", parts[1]);
          fullVars.put("geo_fips_set", parts[2]);
          return isCachedOrExists(tableName, year, fullVars);
        },
        (year, vars) -> {
          // Parse combo and execute download
          String[] parts = vars.get("combo").split(":", 3);
          String tableNameKey = parts[0];

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return;  // Skip download
          }

          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("tablename", tableNameKey);
          fullVars.put("line_code", parts[1]);
          fullVars.put("geo_fips_set", parts[2]);

          String cachedPath =
              cacheStorageProvider.resolvePath(cacheDirectory, executeDownload(tableName, fullVars));

          long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
          cacheManifest.markCached(tableName, year, fullVars, cachedPath, fileSize);
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
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

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

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("combo", combos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework with year filtering
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse combo back into separate variables
          String[] parts = vars.get("combo").split(":", 3);
          String tableNameKey = parts[0];

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return true;  // Return true to skip (treat as already converted)
          }

          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("tablename", tableNameKey);
          fullVars.put("line_code", parts[1]);
          fullVars.put("geo_fips_set", parts[2]);

          // Resolve paths
          String rawPath =
              cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, fullVars));
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          return isParquetConvertedOrExists(tableName, year, fullVars, rawPath, fullParquetPath);
        },
        (year, vars) -> {
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

          // Resolve parquet path for manifest
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Convert
          convertCachedJsonToParquet(tableName, fullVars);

          // Mark as converted
          cacheManifest.markParquetConverted(tableName, year, fullVars, fullParquetPath);
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

    String tableName = "ita_data";

    // Build flat list of all indicator-frequency combinations
    List<String> combos = new ArrayList<>();
    for (String indicator : itaIndicatorsList) {
      for (String frequency : frequencies) {
        combos.add(indicator + ":" + frequency);
      }
    }

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("indicator_freq", combos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse combo back into separate variables
          String[] parts = vars.get("indicator_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("indicator", parts[0]);
          fullVars.put("frequency", parts[1]);
          return isCachedOrExists(tableName, year, fullVars);
        },
        (year, vars) -> {
          // Parse combo and execute download
          String[] parts = vars.get("indicator_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("indicator", parts[0]);
          fullVars.put("frequency", parts[1]);

          String cachedPath =
              cacheStorageProvider.resolvePath(
                  cacheDirectory, executeDownload(tableName, fullVars));

          long fileSize = cacheStorageProvider.getMetadata(cachedPath).getSize();
          cacheManifest.markCached(tableName, year, fullVars, cachedPath, fileSize);
        },
        "download");
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

    String tableName = "ita_data";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Build flat list of all indicator-frequency combinations
    List<String> combos = new ArrayList<>();
    for (String indicator : itaIndicatorsList) {
      for (String frequency : frequencies) {
        combos.add(indicator + ":" + frequency);
      }
    }

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("indicator_freq", combos));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Parse combo back into separate variables
          String[] parts = vars.get("indicator_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("indicator", parts[0]);
          fullVars.put("frequency", parts[1]);

          // Resolve paths
          String rawPath =
              cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, fullVars));
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          return isParquetConvertedOrExists(tableName, year, fullVars, rawPath, fullParquetPath);
        },
        (year, vars) -> {
          // Parse combo and execute conversion
          String[] parts = vars.get("indicator_freq").split(":", 2);
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("indicator", parts[0]);
          fullVars.put("frequency", parts[1]);

          // Resolve parquet path for manifest
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Convert
          convertCachedJsonToParquet(tableName, fullVars);

          // Mark as converted
          cacheManifest.markParquetConverted(tableName, year, fullVars, fullParquetPath);
        },
        "conversion");
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

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("Industry", keyIndustriesList));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Cache checking uses params with only Industry
          Map<String, String> params = new HashMap<>();
          params.put("Industry", vars.get("Industry"));
          return isCachedOrExists(tableName, year, params);
        },
        (year, vars) -> {
          // Build full variables with frequency
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("frequency", "A");
          fullVars.put("Industry", vars.get("Industry"));

          // Resolve relative path for manifest
          String relativePath = resolveJsonPath(pattern, fullVars);
          String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
          long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();

          // Mark as cached with params (not fullVars)
          Map<String, String> params = new HashMap<>();
          params.put("Industry", vars.get("Industry"));
          cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
        },
        "download");
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

    String tableName = "industry_gdp";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Create dimensions for iteration
    List<IterationDimension> dimensions = new ArrayList<>();
    dimensions.add(new IterationDimension("Industry", keyIndustriesList));
    dimensions.add(IterationDimension.fromYearRange(startYear, endYear));

    // Execute using generic framework
    iterateTableOperations(
        tableName,
        dimensions,
        (year, vars) -> {
          // Build full variables with frequency
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("frequency", "A");
          fullVars.put("Industry", vars.get("Industry"));

          // Resolve paths
          String rawPath =
              cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, fullVars));
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Cache checking uses params with only Industry
          Map<String, String> params = new HashMap<>();
          params.put("Industry", vars.get("Industry"));

          return isParquetConvertedOrExists(tableName, year, params, rawPath, fullParquetPath);
        },
        (year, vars) -> {
          // Build full variables with frequency
          Map<String, String> fullVars = new HashMap<>();
          fullVars.put("year", vars.get("year"));
          fullVars.put("frequency", "A");
          fullVars.put("Industry", vars.get("Industry"));

          // Resolve parquet path for manifest
          String fullParquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, fullVars));

          // Convert
          convertCachedJsonToParquet(tableName, fullVars);

          // Mark as converted with params (not fullVars)
          Map<String, String> params = new HashMap<>();
          params.put("Industry", vars.get("Industry"));
          cacheManifest.markParquetConverted(tableName, year, params, fullParquetPath);
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

    // Early check: if parquet is already converted, skip entire process
    int year = 0;  // Sentinel value for reference tables without year dimension
    Map<String, String> params = new HashMap<>();  // No additional params for reference tables

    if (cacheManifest.isParquetConverted("reference_nipa_tables", year, params)) {
      LOGGER.info("⚡ reference_nipa_tables already converted to parquet, skipping");
      return;
    }

    // Read JSON
    com.fasterxml.jackson.databind.JsonNode root;
    try (java.io.InputStream is = cacheStorageProvider.openInputStream(fullJsonPath)) {
      root = MAPPER.readTree(is);
    }

    if (!root.isArray()) {
      throw new IOException("reference_nipa_tables.json is not an array");
    }

    // Regex pattern to match frequency indicators
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

      convertInMemoryToParquetViaDuckDB("reference_nipa_tables", columns, sectionRecords, fullParquetPath);

      LOGGER.info("Wrote {} tables for section {} to {}", sectionRecords.size(), section,
          parquetPath);
    }

    LOGGER.info("Converted reference_nipa_tables to parquet with frequency and section columns " +
            "across {} sections",
        recordsBySection.size());

    // Mark as converted in cache manifest after successful conversion
    cacheManifest.markParquetConverted("reference_nipa_tables", year, params,
        "type=reference/section=*/nipa_tables.parquet");
    cacheManifest.save(operatingDirectory);
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
  public void convertRegionalLineCodeCatalog() {
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

        if (!dataNode.isArray() || dataNode.isEmpty()) {
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
        convertInMemoryToParquetViaDuckDB(tableName, columns, enrichedRecords, fullParquetPath);

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
