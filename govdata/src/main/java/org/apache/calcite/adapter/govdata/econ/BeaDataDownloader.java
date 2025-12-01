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

    // Build sets of valid dimensions for iteration
    // Filter based on table type and collect unique values for each dimension
    Set<String> validTableNames = new HashSet<>();
    Set<String> allLineCodes = new HashSet<>();
    Set<String> allGeoFipsCodes = new HashSet<>();
    // Track valid combinations for filtering in the operation lambda
    Set<String> validCombinations = new HashSet<>();

    for (String tableNameKey : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("Regional income download: No LineCodes available for table {} in catalog, skipping table",
            tableNameKey);
        continue;
      }

      // Get valid GeoFips codes for this table type (e.g., STATE for SA* tables)
      Set<String> validGeoFipsForTable = getValidGeoFipsForTable(tableNameKey);

      // Track unique dimension values
      validTableNames.add(tableNameKey);
      allLineCodes.addAll(lineCodesForTable);
      allGeoFipsCodes.addAll(validGeoFipsForTable);

      // Build valid combinations lookup for filtering during iteration
      for (String lineCode : lineCodesForTable) {
        for (String geoFipsCode : validGeoFipsForTable) {
          validCombinations.add(tableNameKey + ":" + lineCode + ":" + geoFipsCode);
        }
      }
    }

    // Convert to lists for dimension provider
    List<String> tableNamesList = new ArrayList<>(validTableNames);
    List<String> lineCodesList = new ArrayList<>(allLineCodes);
    List<String> geoFipsCodesList = new ArrayList<>(allGeoFipsCodes);

    LOGGER.info("Regional income download: {} tables × {} line codes × {} geo sets × {} years (filtering invalid combinations during iteration)",
        tableNamesList.size(), lineCodesList.size(), geoFipsCodesList.size(), endYear - startYear + 1);

    // Use optimized iteration with DuckDB-based cache filtering
    // Pattern: type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/regional_income.parquet
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
          case "type":
            return List.of(tableName);
          case "year":
            return yearRange(startYear, endYear);
          case "geo_fips_set":
            return geoFipsCodesList;
          case "tablename":
            return tableNamesList;
          case "line_code":
            return lineCodesList;
          default:
            return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);
          String tableNameKey = vars.get("tablename");
          String lineCode = vars.get("line_code");
          String geoFipsCode = vars.get("geo_fips_set");

          // Skip invalid table-lineCode-geoFips combinations
          String combo = tableNameKey + ":" + lineCode + ":" + geoFipsCode;
          if (!validCombinations.contains(combo)) {
            return;  // Skip invalid combination
          }

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return;  // Skip download
          }

          // Download to cache
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
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

    // Build sets of valid dimensions for iteration
    // Filter based on table type and collect unique values for each dimension
    Set<String> validTableNames = new HashSet<>();
    Set<String> allLineCodes = new HashSet<>();
    Set<String> allGeoFipsCodes = new HashSet<>();
    // Track valid combinations for filtering in the operation lambda
    Set<String> validCombinations = new HashSet<>();

    for (String tableNameKey : tablenames.keySet()) {
      Set<String> lineCodesForTable = lineCodeCatalog.get(tableNameKey);
      if (lineCodesForTable == null || lineCodesForTable.isEmpty()) {
        LOGGER.warn("Regional income conversion: No LineCodes available for table {} in catalog, skipping table",
            tableNameKey);
        continue;
      }

      // Get valid GeoFips codes for this table type (e.g., STATE for SA* tables)
      Set<String> validGeoFipsForTable = getValidGeoFipsForTable(tableNameKey);

      // Track unique dimension values
      validTableNames.add(tableNameKey);
      allLineCodes.addAll(lineCodesForTable);
      allGeoFipsCodes.addAll(validGeoFipsForTable);

      // Build valid combinations lookup for filtering during iteration
      for (String lineCode : lineCodesForTable) {
        for (String geoFipsCode : validGeoFipsForTable) {
          validCombinations.add(tableNameKey + ":" + lineCode + ":" + geoFipsCode);
        }
      }
    }

    // Convert to lists for dimension provider
    List<String> tableNamesList = new ArrayList<>(validTableNames);
    List<String> lineCodesList = new ArrayList<>(allLineCodes);
    List<String> geoFipsCodesList = new ArrayList<>(allGeoFipsCodes);

    LOGGER.info("Regional income conversion: {} tables × {} line codes × {} geo sets × {} years (filtering invalid combinations during iteration)",
        tableNamesList.size(), lineCodesList.size(), geoFipsCodesList.size(), endYear - startYear + 1);

    // Use optimized iteration with DuckDB-based cache filtering
    // Pattern: type=regional_income/year=*/geo_fips_set=*/tablename=*/line_code=*/regional_income.parquet
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
          case "type":
            return List.of(tableName);
          case "year":
            return yearRange(startYear, endYear);
          case "geo_fips_set":
            return geoFipsCodesList;
          case "tablename":
            return tableNamesList;
          case "line_code":
            return lineCodesList;
          default:
            return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = extractYear(vars);
          String tableNameKey = vars.get("tablename");
          String lineCode = vars.get("line_code");
          String geoFipsCode = vars.get("geo_fips_set");

          // Skip invalid table-lineCode-geoFips combinations
          String combo = tableNameKey + ":" + lineCode + ":" + geoFipsCode;
          if (!validCombinations.contains(combo)) {
            return;  // Skip invalid combination
          }

          // Skip this combination if table is not valid for this year
          if (!isTableValidForYear(tableNameKey, year)) {
            return;  // Skip conversion
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

}
