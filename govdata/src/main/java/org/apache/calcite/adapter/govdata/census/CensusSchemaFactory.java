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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.adapter.govdata.geo.CensusApiClient;
import org.apache.calcite.model.JsonTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for U.S. Census Bureau demographic and socioeconomic data schemas.
 *
 * <p>This factory provides access to comprehensive Census data including:
 * <ul>
 *   <li>American Community Survey (ACS) 1-year and 5-year estimates</li>
 *   <li>Decennial Census (2010, 2020)</li>
 *   <li>Economic Census</li>
 *   <li>Population Estimates Program</li>
 *   <li>County Business Patterns</li>
 * </ul>
 *
 * <p>Data is organized hierarchically:
 * <ul>
 *   <li>Demographics: Age, sex, race, ethnicity, households</li>
 *   <li>Economics: Income, poverty, employment, occupation</li>
 *   <li>Education: Attainment, enrollment, field of study</li>
 *   <li>Housing: Units, costs, tenure, characteristics</li>
 *   <li>Social: Language, disability, veterans, migration</li>
 * </ul>
 *
 * <p>Geographic coverage includes:
 * <ul>
 *   <li>Nation, states, counties</li>
 *   <li>Metropolitan Statistical Areas (MSAs)</li>
 *   <li>Places (cities/towns)</li>
 *   <li>ZIP Code Tabulation Areas (ZCTAs)</li>
 *   <li>Census tracts, block groups</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "CENSUS",
 *   "schemas": [{
 *     "name": "CENSUS",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "schemaType": "CENSUS",
 *       "autoDownload": true,
 *       "startYear": 2019,
 *       "endYear": 2023
 *     }
 *   }]
 * }
 * </pre>
 */
public class CensusSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusSchemaFactory.class);

  // Store constraint metadata from model files
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  // Hive-partitioned structure for Census data
  private static final String CENSUS_SOURCE_PARTITION = "source=census";
  private static final String ACS_TYPE = "type=acs";  // American Community Survey
  private static final String DECENNIAL_TYPE = "type=decennial";  // Decennial Census
  private static final String ECONOMIC_TYPE = "type=economic";  // Economic Census
  private static final String POPULATION_TYPE = "type=population";  // Population Estimates

  @Override public String getSchemaResourceName() {
    return "/census-schema.json";
  }

  /**
   * Builds the operand configuration for CENSUS schema.
   * This method is called by GovDataSchemaFactory to build a unified FileSchema configuration.
   */
  @Override public Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider) {
    LOGGER.info("Building CENSUS schema operand configuration");

    // Get cache directories from interface methods
    String govdataCacheDir = getGovDataCacheDir();
    String govdataParquetDir = getGovDataParquetDir();

    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // Build CENSUS data directories
    String censusRawDir = govdataCacheDir + "/census";
    String censusParquetDir = govdataParquetDir + "/source=census";

    // Make a mutable copy of the operand so we can modify it
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Extract configuration parameters
    Integer startYear = getConfiguredStartYear(mutableOperand);
    Integer endYear = getConfiguredEndYear(mutableOperand);
    Boolean autoDownload = shouldAutoDownload(mutableOperand);

    // Default to recent 5-year period for ACS data
    if (startYear == null) {
      startYear = 2019;  // Start with 2019 for 2015-2019 5-year ACS
    }
    if (endYear == null) {
      endYear = 2023;  // Most recent complete ACS year
    }

    // Calculate which census years to include based on the date range
    List<Integer> acsYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      acsYears.add(year);
    }

    // Determine decennial census years in range
    List<Integer> decennialYears = determineDecennialYears(startYear, endYear);

    // Get Census API key from environment
    String censusApiKey = System.getenv("CENSUS_API_KEY");
    if (censusApiKey == null) {
      censusApiKey = System.getProperty("CENSUS_API_KEY");
    }

    if (censusApiKey == null) {
      LOGGER.warn("CENSUS_API_KEY not configured - Census data download will be limited");
    }

    // CENSUS data TTL configuration (in days) - default 30 days for monthly updates
    Integer censusCacheTtlDays = (Integer) mutableOperand.getOrDefault("censusCacheTtlDays", 30);
    if (censusCacheTtlDays < 1) {
      censusCacheTtlDays = 30; // Ensure minimum 1 day TTL
    }

    // Create cache directory structure
    File cacheRoot = new File(censusRawDir);
    if (!cacheRoot.exists() && !cacheRoot.mkdirs()) {
      throw new RuntimeException("Failed to create cache directory: " + censusRawDir);
    }

    // Create hive-partitioned directory structure for parquet files
    File parquetRoot = new File(censusParquetDir);
    File acsDir = new File(parquetRoot, ACS_TYPE);
    File decennialDir = new File(parquetRoot, DECENNIAL_TYPE);
    File economicDir = new File(parquetRoot, ECONOMIC_TYPE);
    File populationDir = new File(parquetRoot, POPULATION_TYPE);

    for (File dir : new File[]{parquetRoot, acsDir, decennialDir, economicDir, populationDir}) {
      if (!dir.exists() && !dir.mkdirs()) {
        LOGGER.warn("Failed to create directory: {}", dir);
      }
    }

    // Log the partitioned structure
    LOGGER.info("Census data partitions created:");
    LOGGER.info("  ACS: {}", acsDir);
    LOGGER.info("  Decennial: {}", decennialDir);
    LOGGER.info("  Economic: {}", economicDir);
    LOGGER.info("  Population: {}", populationDir);

    // Log configuration
    LOGGER.info("Census data configuration:");
    LOGGER.info("  Cache directory: {}", censusRawDir);
    LOGGER.info("  Parquet directory: {}", censusParquetDir);
    LOGGER.info("  ACS years: {}", acsYears);
    LOGGER.info("  Decennial years: {}", decennialYears);
    LOGGER.info("  Auto-download: {}", autoDownload);
    LOGGER.info("  Census cache TTL: {} days", censusCacheTtlDays);
    LOGGER.info("  Census API key: {}", censusApiKey != null ? "configured" : "not configured");

    // Download data if auto-download is enabled
    if (autoDownload && censusApiKey != null) {
      LOGGER.info("Auto-download enabled for CENSUS data");
      try {
        downloadCensusData(censusRawDir, censusParquetDir, censusApiKey,
            acsYears, decennialYears, censusCacheTtlDays, storageProvider);
      } catch (Exception e) {
        LOGGER.error("Error downloading CENSUS data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Don't create placeholder files - just define the tables like GEO does

    // Now configure for FileSchemaFactory
    // Set the directory to the parquet directory with hive-partitioned structure
    mutableOperand.put("directory", censusParquetDir);

    // Pass through executionEngine if specified
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
      LOGGER.info("CensusSchemaFactory: Using executionEngine: {}", operand.get("executionEngine"));
    }

    // Set casing conventions
    if (!mutableOperand.containsKey("tableNameCasing")) {
      mutableOperand.put("tableNameCasing", "SMART_CASING");
    }
    if (!mutableOperand.containsKey("columnNameCasing")) {
      mutableOperand.put("columnNameCasing", "SMART_CASING");
    }

    // Load table definitions from census-schema.json
    List<Map<String, Object>> censusTables = loadTableDefinitions();
    if (!censusTables.isEmpty()) {
      mutableOperand.put("partitionedTables", censusTables);
      LOGGER.info("Built {} CENSUS table definitions from census-schema.json", censusTables.size());
    }

    // Add automatic constraint definitions if enabled
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
    if (enableConstraints == null) {
      enableConstraints = true; // Default to true
    }

    // Build constraint metadata for census tables
    Map<String, Map<String, Object>> censusConstraints = new HashMap<>();

    if (enableConstraints) {
      // Load constraints from census-schema.json
      censusConstraints.putAll(loadTableConstraints());
    }

    // Merge with any constraints from model file
    if (tableConstraints != null) {
      censusConstraints.putAll(tableConstraints);
    }

    if (!censusConstraints.isEmpty()) {
      mutableOperand.put("tableConstraints", censusConstraints);
    }

    // Return the configured operand for GovDataSchemaFactory to use
    LOGGER.info("CENSUS schema operand configuration complete");
    return mutableOperand;
  }

  /**
   * Download Census data from the Census Bureau API.
   */
  private void downloadCensusData(String cacheDir, String censusParquetDir, String censusApiKey,
      List<Integer> acsYears, List<Integer> decennialYears, Integer censusCacheTtlDays,
      StorageProvider storageProvider) throws IOException {

    LOGGER.info("Checking Census data cache: {}", censusParquetDir);

    // Set up TTL configuration for Census data
    long censusDataTtlMillis = censusCacheTtlDays * 24L * 60 * 60 * 1000; // Convert days to milliseconds
    long currentTime = System.currentTimeMillis();

    // Reuse the existing CensusApiClient from GEO package
    File censusCacheDir = new File(cacheDir);
    CensusApiClient censusClient = new CensusApiClient(censusApiKey, censusCacheDir, acsYears, storageProvider);

    // Check if ACS data needs updating
    boolean needsAcsUpdate = false;
    for (int year : acsYears) {
      // Check core ACS tables
      String[] acsFiles = {
        "acs_population.parquet",
        "acs_demographics.parquet",
        "acs_income.parquet",
        "acs_poverty.parquet",
        "acs_employment.parquet",
        "acs_education.parquet",
        "acs_housing.parquet",
        "acs_housing_costs.parquet",
        "acs_commuting.parquet",
        "acs_health_insurance.parquet",
        "acs_language.parquet",
        "acs_disability.parquet",
        "acs_veterans.parquet",
        "acs_migration.parquet",
        "acs_occupation.parquet"
      };

      for (String filename : acsFiles) {
        String parquetPath =
            storageProvider.resolvePath(censusParquetDir, ACS_TYPE + "/year=" + year + "/" + filename);
        LOGGER.debug("Checking ACS cache file: {}", parquetPath);
        try {
          if (!storageProvider.exists(parquetPath)) {
            needsAcsUpdate = true;
            LOGGER.info("Missing ACS parquet file: {}", parquetPath);
            break;
          } else {
            StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
            if (metadata.getSize() == 0) {
              needsAcsUpdate = true;
              LOGGER.info("Empty ACS parquet file: {}", parquetPath);
              break;
            }

            // Check TTL
            long fileAge = currentTime - metadata.getLastModified();
            if (fileAge > censusDataTtlMillis) {
              needsAcsUpdate = true;
              LOGGER.info("Expired ACS parquet file (age: {} days): {}",
                  fileAge / (24 * 60 * 60 * 1000), parquetPath);
              break;
            }
          }
        } catch (IOException e) {
          needsAcsUpdate = true;
          LOGGER.info("Cannot access ACS parquet file: {}", parquetPath);
          break;
        }
      }
      if (needsAcsUpdate) break;
    }

    if (!needsAcsUpdate) {
      LOGGER.info("ACS data already fully cached for years: {}", acsYears);
    } else {
      try {
        LOGGER.info("Downloading ACS data for years: {}", acsYears);

        // Download ACS data for the year range
        int startYear = acsYears.isEmpty() ? 2019 : acsYears.get(0);
        int endYear = acsYears.isEmpty() ? 2023 : acsYears.get(acsYears.size() - 1);

        // Download comprehensive ACS data
        downloadAcsData(censusClient, startYear, endYear);

        // Convert to Parquet format
        LOGGER.info("Converting ACS data to Parquet for years: {}", acsYears);
        for (int year : acsYears) {
          convertAcsDataToParquet(censusCacheDir, censusParquetDir, year, storageProvider);
        }

      } catch (Exception e) {
        LOGGER.error("Error downloading ACS data", e);
      }
    }

    // Check if Decennial data needs updating (if years are in range)
    if (!decennialYears.isEmpty()) {
      boolean needsDecennialUpdate = false;
      for (int year : decennialYears) {
        String[] decennialFiles = {
          "decennial_population.parquet",
          "decennial_demographics.parquet",
          "decennial_housing.parquet"
        };

        for (String filename : decennialFiles) {
          String parquetPath =
              storageProvider.resolvePath(censusParquetDir, DECENNIAL_TYPE + "/year=" + year + "/" + filename);
          try {
            if (!storageProvider.exists(parquetPath)) {
              needsDecennialUpdate = true;
              LOGGER.info("Missing Decennial parquet file: {}", parquetPath);
              break;
            }
          } catch (IOException e) {
            needsDecennialUpdate = true;
            LOGGER.info("Cannot access Decennial parquet file: {}", parquetPath);
            break;
          }
        }
        if (needsDecennialUpdate) break;
      }

      if (!needsDecennialUpdate) {
        LOGGER.info("Decennial data already fully cached for years: {}", decennialYears);
      } else {
        try {
          LOGGER.info("Downloading Decennial Census data for years: {}", decennialYears);

          for (int year : decennialYears) {
            downloadDecennialData(censusClient, year);
            convertDecennialDataToParquet(censusCacheDir, censusParquetDir, year, storageProvider);
          }

        } catch (Exception e) {
          LOGGER.error("Error downloading Decennial Census data", e);
        }
      }
    }

    // Check and download Economic Census data (every 5 years: 2012, 2017, 2022)
    int startYear = acsYears.isEmpty() ? 2019 : acsYears.get(0);
    int endYear = acsYears.isEmpty() ? 2023 : acsYears.get(acsYears.size() - 1);
    List<Integer> economicYears = determineEconomicYears(startYear, endYear);
    if (!economicYears.isEmpty()) {
      boolean needsEconomicUpdate = checkEconomicDataNeeds(economicYears, censusParquetDir, storageProvider, currentTime, censusDataTtlMillis);

      if (!needsEconomicUpdate) {
        LOGGER.info("Economic Census data already fully cached for years: {}", economicYears);
      } else {
        LOGGER.info("Downloading Economic Census data for years: {}", economicYears);
        for (int year : economicYears) {
          try {
            downloadEconomicData(censusClient, year);
            convertEconomicDataToParquet(censusCacheDir, censusParquetDir, year, storageProvider);
          } catch (Exception e) {
            // Check if this is a "no data" error (404, dataset not available) vs actual API error
            String errorMsg = e.getMessage();
            boolean isNoDataError =
                errorMsg != null && (errorMsg.contains("404") ||
                errorMsg.contains("not found") ||
                errorMsg.contains("does not exist") ||
                errorMsg.contains("No data") ||
                errorMsg.contains("All datasets failed"));

            if (isNoDataError) {
              LOGGER.warn("No data available for Economic Census year {}: {}", year, errorMsg);
              LOGGER.info("Creating zero-row Parquet files for Economic Census year {} (data not available)", year);

              // Create zero-row files so tables still exist in schema
              String[] economicTables = {"economic_census", "county_business_patterns"};
              for (String tableName : economicTables) {
                try {
                  String parquetPath =
                      storageProvider.resolvePath(censusParquetDir, ECONOMIC_TYPE + "/year=" + year + "/" + tableName + ".parquet");
                  createZeroRowParquetFile(parquetPath, tableName, year, storageProvider, "economic");
                } catch (IOException ex) {
                  LOGGER.error("Failed to create zero-row file for {} year {}: {}",
                      tableName, year, ex.getMessage());
                }
              }
            } else {
              // Real error - log for analysis but don't create files
              LOGGER.error("Error downloading Economic Census data for year {} (requires investigation): {}",
                  year, errorMsg, e);
            }
          }
        }
      }
    }

    // Check and download Population Estimates data (annual)
    boolean needsPopulationUpdate = checkPopulationEstimatesNeeds(acsYears, censusParquetDir, storageProvider, currentTime, censusDataTtlMillis);

    if (!needsPopulationUpdate) {
      LOGGER.info("Population Estimates data already fully cached for years: {}", acsYears);
    } else {
      LOGGER.info("Downloading Population Estimates data for years: {}", acsYears);
      for (int year : acsYears) {
        try {
          downloadPopulationEstimatesData(censusClient, year);
          convertPopulationEstimatesDataToParquet(censusCacheDir, censusParquetDir, year, storageProvider);
        } catch (Exception e) {
          // Check if this is a "no data" error (404, dataset not available) vs actual API error
          String errorMsg = e.getMessage();
          boolean isNoDataError =
              errorMsg != null && (errorMsg.contains("404") ||
              errorMsg.contains("not found") ||
              errorMsg.contains("does not exist") ||
              errorMsg.contains("No data") ||
              errorMsg.contains("All datasets failed"));

          if (isNoDataError) {
            LOGGER.warn("No data available for Population Estimates year {}: {}", year, errorMsg);
            LOGGER.info("Creating zero-row Parquet file for Population Estimates year {} (data not available)", year);

            // Create zero-row file so table still exists in schema
            try {
              String parquetPath =
                  storageProvider.resolvePath(censusParquetDir, POPULATION_TYPE + "/year=" + year + "/population_estimates.parquet");
              createZeroRowParquetFile(parquetPath, "population_estimates", year, storageProvider, "population");
            } catch (IOException ex) {
              LOGGER.error("Failed to create zero-row file for population_estimates year {}: {}",
                  year, ex.getMessage());
            }
          } else {
            // Real error - log for analysis but don't create files
            LOGGER.error("Error downloading Population Estimates data for year {} (requires investigation): {}",
                year, errorMsg, e);
          }
        }
      }
    }
  }


  /**
   * Download comprehensive ACS data for all table types.
   */
  private void downloadAcsData(CensusApiClient censusClient, int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading comprehensive ACS data for years {} to {}", startYear, endYear);

    // The CensusApiClient.downloadAll already handles the core demographics, housing, and economic data
    // We just need to call it with the year range
    censusClient.downloadAll(startYear, endYear);

    // Download all ACS subject tables for each table type
    for (int year = startYear; year <= endYear; year++) {
      try {
        // Download data for each ACS table type using ConceptualVariableMapper
        downloadAcsTableData(censusClient, year, "acs_population");
        downloadAcsTableData(censusClient, year, "acs_demographics");
        downloadAcsTableData(censusClient, year, "acs_poverty");
        downloadAcsTableData(censusClient, year, "acs_employment");
        downloadAcsTableData(censusClient, year, "acs_education");
        downloadAcsTableData(censusClient, year, "acs_housing_costs");
        downloadAcsTableData(censusClient, year, "acs_commuting");
        downloadAcsTableData(censusClient, year, "acs_health_insurance");
        downloadAcsTableData(censusClient, year, "acs_language");
        downloadAcsTableData(censusClient, year, "acs_disability");
        downloadAcsTableData(censusClient, year, "acs_veterans");
        downloadAcsTableData(censusClient, year, "acs_migration");
        downloadAcsTableData(censusClient, year, "acs_occupation");

        LOGGER.info("Downloaded comprehensive ACS data for year {}", year);
      } catch (Exception e) {
        LOGGER.error("Error downloading comprehensive ACS data for year {}", year, e);
      }
    }
  }

  /**
   * Download ACS data for a specific table type.
   */
  private void downloadAcsTableData(CensusApiClient censusClient, int year, String tableName) throws IOException {
    String[] variables = ConceptualVariableMapper.getVariablesToDownload(tableName, year, "acs");
    if (variables.length == 0) {
      LOGGER.info("No variables available for {} in year {} - skipping download", tableName, year);
      return;
    }

    String variableList = String.join(",", variables);
    LOGGER.info("Downloading {} variables for {}: {}", variables.length, tableName, variableList);

    try {
      // Download for states
      censusClient.getAcsData(year, variableList, "state:*");
      // Download for counties
      censusClient.getAcsData(year, variableList, "county:*");

      LOGGER.info("Successfully downloaded {} data for year {}", tableName, year);
    } catch (Exception e) {
      // Check if this is a 404 (data not released yet) or 400 (invalid variables for this year)
      String errorMsg = e.getMessage();
      if (errorMsg != null && (errorMsg.contains("404") || errorMsg.contains("400"))) {
        LOGGER.debug("Census data not available for {} year {} ({})", tableName, year,
            errorMsg.contains("404") ? "not released yet" : "variables unavailable");
      } else {
        LOGGER.error("Error downloading {} data for year {}: {}", tableName, year, errorMsg);
      }
    }
  }

  /**
   * Download Decennial Census data.
   */
  private void downloadDecennialData(CensusApiClient censusClient, int year) throws IOException {
    LOGGER.info("Downloading Decennial Census data for year {}", year);

    // Download data for each decennial table type using conceptual mappings
    downloadDecennialTableData(censusClient, year, "decennial_population");
    downloadDecennialTableData(censusClient, year, "decennial_demographics");
    downloadDecennialTableData(censusClient, year, "decennial_housing");
  }

  /**
   * Download decennial data for a specific table type using conceptual variable mappings.
   */
  private void downloadDecennialTableData(CensusApiClient censusClient, int year, String tableName) throws IOException {
    String[] variables = ConceptualVariableMapper.getVariablesToDownload(tableName, year, "decennial");
    if (variables.length == 0) {
      LOGGER.info("No variables available for {} in year {} - skipping download", tableName, year);
      return;
    }

    String variableList = String.join(",", variables);
    LOGGER.info("Downloading {} variables for {}: {}", variables.length, tableName, variableList);

    try {
      // Download state-level data
      censusClient.getDecennialData(year, variableList, "state:*");

      // Download county-level data
      censusClient.getDecennialData(year, variableList, "county:*");

      LOGGER.info("Successfully downloaded {} data for year {}", tableName, year);
    } catch (IOException e) {
      LOGGER.error("Error downloading {} data for year {}: {}", tableName, year, e.getMessage());
      throw e;
    }
  }

  /**
   * Convert ACS data to Parquet format for all table types.
   */
  private void convertAcsDataToParquet(File cacheDir, String parquetDir, int year,
      StorageProvider storageProvider) throws IOException {
    LOGGER.info("Converting ACS data to Parquet for year {}", year);

    // List all ACS table types to convert
    String[] acsTableNames = {
      "acs_population", "acs_demographics", "acs_income", "acs_poverty",
      "acs_employment", "acs_education", "acs_housing", "acs_housing_costs",
      "acs_commuting", "acs_health_insurance", "acs_language", "acs_disability",
      "acs_veterans", "acs_migration", "acs_occupation"
    };

    // Convert each table type
    for (String tableName : acsTableNames) {
      try {
        String parquetPath =
            storageProvider.resolvePath(parquetDir, ACS_TYPE + "/year=" + year + "/" + tableName + ".parquet");

        convertTableDataToParquet(cacheDir, parquetPath, tableName, year, storageProvider, "acs");

        LOGGER.info("Successfully converted {} to Parquet for year {}", tableName, year);
      } catch (Exception e) {
        // Check if this is a "no data" error vs a real error
        String errorMessage = e.getMessage();
        if (errorMessage != null && (errorMessage.contains("No data") || errorMessage.contains("empty") ||
            errorMessage.contains("insufficient data"))) {
          LOGGER.info("Creating zero-row file for {} year {} (API indicates no data available)", tableName, year);
          try {
            String parquetPath =
                storageProvider.resolvePath(parquetDir, ACS_TYPE + "/year=" + year + "/" + tableName + ".parquet");
            createZeroRowParquetFile(parquetPath, tableName, year, storageProvider, "acs");
          } catch (Exception zeroRowException) {
            LOGGER.error("Failed to create zero-row file for {} year {}: {}",
                tableName, year, zeroRowException.getMessage());
          }
        } else {
          LOGGER.error("Error converting {} to Parquet for year {}: {}",
              tableName, year, e.getMessage());
        }
      }
    }
  }

  /**
   * Convert Decennial Census data to Parquet format.
   */
  private void convertDecennialDataToParquet(File cacheDir, String parquetDir, int year,
      StorageProvider storageProvider) throws IOException {
    LOGGER.info("Converting Decennial Census data to Parquet for year {}", year);

    String[] decennialTableNames = {
      "decennial_population", "decennial_demographics", "decennial_housing"
    };

    // Convert each table type
    for (String tableName : decennialTableNames) {
      try {
        String parquetPath =
            storageProvider.resolvePath(parquetDir, DECENNIAL_TYPE + "/year=" + year + "/" + tableName + ".parquet");

        convertTableDataToParquet(cacheDir, parquetPath, tableName, year, storageProvider, "decennial");

        LOGGER.info("Successfully converted {} to Parquet for year {}", tableName, year);
      } catch (Exception e) {
        // Check if this is a "no data" error vs a real error
        String errorMessage = e.getMessage();
        if (errorMessage != null && (errorMessage.contains("No data") || errorMessage.contains("empty") ||
            errorMessage.contains("insufficient data"))) {
          LOGGER.info("Creating zero-row file for {} year {} (API indicates no data available)", tableName, year);
          try {
            String parquetPath =
                storageProvider.resolvePath(parquetDir, DECENNIAL_TYPE + "/year=" + year + "/" + tableName + ".parquet");
            createZeroRowParquetFile(parquetPath, tableName, year, storageProvider, "decennial");
          } catch (Exception zeroRowException) {
            LOGGER.error("Failed to create zero-row file for {} year {}: {}",
                tableName, year, zeroRowException.getMessage());
          }
        } else {
          LOGGER.error("Error converting {} to Parquet for year {}: {}",
              tableName, year, e.getMessage());
        }
      }
    }
  }

  /**
   * Convert Census table data to Parquet with friendly column names.
   */
  private void convertTableDataToParquet(File cacheDir, String targetPath, String tableName,
      int year, StorageProvider storageProvider, String censusType) throws IOException {

    // Check if target already exists and is current
    if (storageProvider.exists(targetPath)) {
      try {
        StorageProvider.FileMetadata metadata = storageProvider.getMetadata(targetPath);
        if (metadata.getSize() > 0) {
          LOGGER.debug("Parquet file already exists with data, skipping: {}", targetPath);
          return;
        }
      } catch (IOException e) {
        LOGGER.warn("Could not check existing file: {}", targetPath);
      }
    }

    LOGGER.info("Converting Census data to Parquet: {} for year {}", tableName, year);

    try {
      // Get variable mappings for this table using conceptual mappings
      Map<String, String> variableMap = ConceptualVariableMapper.getVariablesForTable(tableName, year, censusType);
      if (variableMap.isEmpty()) {
        LOGGER.warn("No variable mappings found for table: {} ({} year {})", tableName, censusType, year);
        return;
      }

      // Find matching JSON cache files for this table and year
      // Cache files are named like: acs_2020_B19013_001E_..._county__.json
      // We need to filter for files that contain at least one of the required variables
      File[] jsonFiles = null;
      if (cacheDir.exists() && cacheDir.isDirectory()) {
        final String yearPrefix = censusType + "_" + year + "_";

        // Get the variable codes we need for this table
        final java.util.Set<String> requiredVariables = variableMap.keySet();

        // Filter for JSON files matching this year, census type, and containing at least one required variable
        jsonFiles = cacheDir.listFiles((dir, name) -> {
          if (!name.startsWith(yearPrefix) || !name.endsWith(".json") ||
              name.startsWith("._") || name.startsWith(".DS_Store")) {
            return false;
          }
          // Check if filename contains at least one of the required variables
          for (String varCode : requiredVariables) {
            if (name.contains(varCode)) {
              return true;
            }
          }
          return false;
        });
      }

      if (jsonFiles == null || jsonFiles.length == 0) {
        LOGGER.warn("No JSON cache files found for {} ({} year {})", tableName, censusType, year);
        return;
      }

      // Transform and write parquet data
      CensusDataTransformer transformer = new CensusDataTransformer();
      transformer.transformToParquet(jsonFiles, targetPath, tableName, year, variableMap, storageProvider, censusType);

      LOGGER.info("Successfully converted {} data to Parquet: {}", tableName, targetPath);

    } catch (Exception e) {
      LOGGER.error("Error converting {} data to Parquet for year {}: {}",
          tableName, year, e.getMessage(), e);
      throw new IOException("Failed to convert Census data to Parquet", e);
    }
  }

  /**
   * Determine which decennial census years fall within the specified range.
   */
  private List<Integer> determineDecennialYears(int startYear, int endYear) {
    List<Integer> decennialYears = new ArrayList<>();

    // Decennial census occurs every 10 years (2000, 2010, 2020, etc.)
    for (int year = 2000; year <= endYear; year += 10) {
      if (year >= startYear && year <= endYear) {
        decennialYears.add(year);
      }
    }

    LOGGER.info("Decennial census years in range {}-{}: {}", startYear, endYear, decennialYears);
    return decennialYears;
  }

  @Override public boolean supportsConstraints() {
    // Enable constraint support for census data
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }

  /**
   * Determine which economic census years fall within the specified range.
   * Economic census occurs every 5 years (2012, 2017, 2022, etc.)
   */
  private List<Integer> determineEconomicYears(int startYear, int endYear) {
    List<Integer> economicYears = new ArrayList<>();

    // Economic census occurs every 5 years starting from 2012
    for (int year = 2012; year <= endYear; year += 5) {
      if (year >= startYear && year <= endYear) {
        economicYears.add(year);
      }
    }

    LOGGER.info("Economic census years in range {}-{}: {}", startYear, endYear, economicYears);
    return economicYears;
  }

  /**
   * Check if economic census data needs updating.
   */
  private boolean checkEconomicDataNeeds(List<Integer> economicYears, String censusParquetDir,
      StorageProvider storageProvider, long currentTime, long censusDataTtlMillis) {
    for (int year : economicYears) {
      String[] economicFiles = {"economic_census.parquet", "county_business_patterns.parquet"};

      for (String filename : economicFiles) {
        String parquetPath =
            storageProvider.resolvePath(censusParquetDir, ECONOMIC_TYPE + "/year=" + year + "/" + filename);
        try {
          if (!storageProvider.exists(parquetPath)) {
            LOGGER.info("Missing Economic parquet file: {}", parquetPath);
            return true;
          }
          StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
          if (metadata.getSize() == 0) {
            LOGGER.info("Empty Economic parquet file: {}", parquetPath);
            return true;
          }
          long fileAge = currentTime - metadata.getLastModified();
          if (fileAge > censusDataTtlMillis) {
            LOGGER.info("Expired Economic parquet file: {}", parquetPath);
            return true;
          }
        } catch (IOException e) {
          LOGGER.info("Cannot access Economic parquet file: {}", parquetPath);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check if population estimates data needs updating.
   */
  private boolean checkPopulationEstimatesNeeds(List<Integer> years, String censusParquetDir,
      StorageProvider storageProvider, long currentTime, long censusDataTtlMillis) {
    for (int year : years) {
      String parquetPath =
          storageProvider.resolvePath(censusParquetDir, POPULATION_TYPE + "/year=" + year + "/population_estimates.parquet");
      try {
        if (!storageProvider.exists(parquetPath)) {
          LOGGER.info("Missing Population Estimates parquet file: {}", parquetPath);
          return true;
        }
        StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
        if (metadata.getSize() == 0) {
          LOGGER.info("Empty Population Estimates parquet file: {}", parquetPath);
          return true;
        }
        long fileAge = currentTime - metadata.getLastModified();
        if (fileAge > censusDataTtlMillis) {
          LOGGER.info("Expired Population Estimates parquet file: {}", parquetPath);
          return true;
        }
      } catch (IOException e) {
        LOGGER.info("Cannot access Population Estimates parquet file: {}", parquetPath);
        return true;
      }
    }
    return false;
  }

  /**
   * Download Economic Census data.
   */
  private void downloadEconomicData(CensusApiClient censusClient, int year) throws IOException {
    LOGGER.info("Downloading Economic Census data for year {}", year);

    // Download data for each economic table type using conceptual mappings
    downloadEconomicTableData(censusClient, year, "economic_census");
    downloadEconomicTableData(censusClient, year, "county_business_patterns");
  }

  /**
   * Download economic data for a specific table type using conceptual variable mappings.
   */
  private void downloadEconomicTableData(CensusApiClient censusClient, int year, String tableName) throws IOException {
    String[] variables = ConceptualVariableMapper.getVariablesToDownload(tableName, year, "economic");
    if (variables.length == 0) {
      LOGGER.info("No variables available for {} in year {} - skipping download", tableName, year);
      return;
    }

    String variableList = String.join(",", variables);
    LOGGER.info("Downloading {} variables for {}: {}", variables.length, tableName, variableList);

    try {
      // Download state-level data
      censusClient.getEconomicData(year, variableList, "state:*");

      // Download county-level data
      censusClient.getEconomicData(year, variableList, "county:*");

      LOGGER.info("Successfully downloaded {} data for year {}", tableName, year);
    } catch (IOException e) {
      LOGGER.error("Error downloading {} data for year {}: {}", tableName, year, e.getMessage());
      throw e;
    }
  }

  /**
   * Download Population Estimates data.
   */
  private void downloadPopulationEstimatesData(CensusApiClient censusClient, int year) throws IOException {
    LOGGER.info("Downloading Population Estimates data for year {}", year);

    String[] variables = ConceptualVariableMapper.getVariablesToDownload("population_estimates", year, "population");
    if (variables.length == 0) {
      LOGGER.info("No variables available for population_estimates in year {} - skipping download", year);
      return;
    }

    String variableList = String.join(",", variables);
    LOGGER.info("Downloading {} variables for population_estimates: {}", variables.length, variableList);

    boolean stateSuccess = false;
    boolean countySuccess = false;

    // Download state-level data
    try {
      censusClient.getPopulationEstimatesData(year, variableList, "state:*");
      stateSuccess = true;
      LOGGER.info("Successfully downloaded state-level population_estimates data for year {}", year);
    } catch (IOException e) {
      LOGGER.warn("Failed to download state-level population_estimates for year {}: {}", year, e.getMessage());
    }

    // Download county-level data
    try {
      censusClient.getPopulationEstimatesData(year, variableList, "county:*");
      countySuccess = true;
      LOGGER.info("Successfully downloaded county-level population_estimates data for year {}", year);
    } catch (IOException e) {
      LOGGER.warn("Failed to download county-level population_estimates for year {}: {}", year, e.getMessage());
    }

    // Throw only if both failed
    if (!stateSuccess && !countySuccess) {
      throw new IOException("Failed to download population_estimates for year " + year + " at both state and county levels");
    }

    LOGGER.info("Successfully downloaded population_estimates data for year {} (state: {}, county: {})",
        year, stateSuccess, countySuccess);
  }

  /**
   * Convert Economic Census data to Parquet format.
   */
  private void convertEconomicDataToParquet(File cacheDir, String parquetDir, int year,
      StorageProvider storageProvider) throws IOException {
    LOGGER.info("Converting Economic Census data to Parquet for year {}", year);
    String[] economicTableNames = {"economic_census", "county_business_patterns"};

    for (String tableName : economicTableNames) {
      try {
        String parquetPath =
            storageProvider.resolvePath(parquetDir, ECONOMIC_TYPE + "/year=" + year + "/" + tableName + ".parquet");
        convertTableDataToParquet(cacheDir, parquetPath, tableName, year, storageProvider, "economic");
        LOGGER.info("Successfully converted {} to Parquet for year {}", tableName, year);
      } catch (Exception e) {
        LOGGER.error("Error converting {} to Parquet for year {} (requires investigation): {}",
            tableName, year, e.getMessage(), e);
      }
    }
  }

  /**
   * Convert Population Estimates data to Parquet format.
   */
  private void convertPopulationEstimatesDataToParquet(File cacheDir, String parquetDir, int year,
      StorageProvider storageProvider) throws IOException {
    LOGGER.info("Converting Population Estimates data to Parquet for year {}", year);

    try {
      String parquetPath =
          storageProvider.resolvePath(parquetDir, POPULATION_TYPE + "/year=" + year + "/population_estimates.parquet");
      convertTableDataToParquet(cacheDir, parquetPath, "population_estimates", year, storageProvider, "population");
      LOGGER.info("Successfully converted population_estimates to Parquet for year {}", year);
    } catch (Exception e) {
      LOGGER.error("Error converting population_estimates to Parquet for year {}: {}",
          year, e.getMessage());
    }
  }

  /**
   * Create a zero-row parquet file for legitimately missing data.
   */
  private void createZeroRowParquetFile(String targetPath, String tableName, int year,
      StorageProvider storageProvider, String censusType) throws IOException {
    LOGGER.info("Creating zero-row parquet file: {}", targetPath);

    // Get variable mappings for this table to create proper schema
    Map<String, String> variableMap = ConceptualVariableMapper.getVariablesForTable(tableName, year, censusType);

    // Use the existing createZeroRowParquetFile method from CensusDataTransformer
    CensusDataTransformer transformer = new CensusDataTransformer();
    transformer.createZeroRowParquetFile(targetPath, tableName, year, variableMap, storageProvider);
  }

}
