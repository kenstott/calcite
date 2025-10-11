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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.model.JsonTable;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for geographic data schemas that provides access to U.S. government
 * geographic datasets.
 *
 * <p>This factory leverages the file adapter's infrastructure for HTTP operations
 * and Parquet storage, similar to the SEC adapter implementation.
 *
 * <p>Supported data sources:
 * <ul>
 *   <li>Census TIGER/Line boundary files</li>
 *   <li>Census demographic/economic data via API</li>
 *   <li>HUD-USPS ZIP code crosswalk</li>
 *   <li>Census geocoding services</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GEO",
 *   "schemas": [{
 *     "name": "GEO",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory",
 *     "operand": {
 *       "cacheDir": "/path/to/geo-cache",
 *       "censusApiKey": "your-free-api-key",
 *       "hudUsername": "your-hud-username",
 *       "hudPassword": "your-hud-password",
 *       "enabledSources": ["tiger", "census", "hud"],
 *       "dataYear": 2024,
 *       "autoDownload": true
 *     }
 *   }]
 * }
 * </pre>
 */
public class GeoSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoSchemaFactory.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  // Store constraint metadata from model files
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;


  // Hive-partitioned structure for geographic data
  // Base: /govdata-parquet/source=geo/type={boundary,demographic,crosswalk}/...
  private static final String GEO_SOURCE_PARTITION = "source=geo";
  private static final String BOUNDARY_TYPE = "type=boundary";  // TIGER data
  private static final String DEMOGRAPHIC_TYPE = "type=demographic";  // Census API data
  private static final String CROSSWALK_TYPE = "type=crosswalk";  // HUD data

  @Override public String getSchemaResourceName() {
    return "/geo-schema.json";
  }

  /**
   * Builds the operand configuration for GEO schema.
   * This method is called by GovDataSchemaFactory to build a unified FileSchema configuration.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    LOGGER.info("Building GEO schema operand configuration");

    // Get cache directories from interface methods
    String govdataCacheDir = getGovDataCacheDir(operand);
    String govdataParquetDir = getGovDataParquetDir(operand);

    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // Build GEO data directories
    String geoRawDir = govdataCacheDir + "/geo";
    String geoParquetDir = govdataParquetDir + "/source=geo";

    // Make a mutable copy of the operand so we can modify it
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Extract configuration parameters
    // Calcite's ModelHandler automatically substitutes ${ENV_VAR} placeholders
    String configuredDir = (String) mutableOperand.get("cacheDirectory");

    // Use unified govdata directory structure
    String cacheDir;
    if (govdataCacheDir != null && govdataParquetDir != null) {
      // Raw geographic data goes to GOVDATA_CACHE_DIR/geo
      // Parquet data goes to GOVDATA_PARQUET_DIR/source=geo
      cacheDir = geoRawDir; // For raw geographic data
      LOGGER.info("Using unified govdata directories - cache: {}, parquet: {}", geoRawDir, geoParquetDir);
    } else {
      cacheDir = configuredDir != null ? configuredDir : geoRawDir;
    }

    // Expand tilde in cache directory path if present
    if (cacheDir != null && cacheDir.startsWith("~")) {
      cacheDir = System.getProperty("user.home") + cacheDir.substring(1);
    }

    // Get API credentials from environment variables (matching ECON pattern)
    String censusApiKey = System.getenv("CENSUS_API_KEY");
    if (censusApiKey == null) {
      censusApiKey = System.getProperty("CENSUS_API_KEY");
    }
    String hudUsername = System.getenv("HUD_USERNAME");
    if (hudUsername == null) {
      hudUsername = System.getProperty("HUD_USERNAME");
    }
    String hudPassword = System.getenv("HUD_PASSWORD");
    if (hudPassword == null) {
      hudPassword = System.getProperty("HUD_PASSWORD");
    }
    String hudToken = System.getenv("HUD_TOKEN");
    if (hudToken == null) {
      hudToken = System.getProperty("HUD_TOKEN");
    }

    // Data source configuration
    Object enabledSourcesObj = mutableOperand.get("enabledSources");
    String[] enabledSources;
    if (enabledSourcesObj instanceof String[]) {
      enabledSources = (String[]) enabledSourcesObj;
    } else if (enabledSourcesObj instanceof java.util.List) {
      java.util.List<?> list = (java.util.List<?>) enabledSourcesObj;
      enabledSources = list.toArray(new String[0]);
    } else {
      // Default to all sources if not specified
      enabledSources = new String[]{"tiger", "census", "hud"};
    }

    // Support both old dataYear and new startYear/endYear parameters
    Integer startYear = (Integer) mutableOperand.get("startYear");
    Integer endYear = (Integer) mutableOperand.get("endYear");
    Integer dataYear = (Integer) mutableOperand.get("dataYear");

    // If dataYear is specified (old format), use it for both start and end
    if (dataYear != null && startYear == null && endYear == null) {
      startYear = dataYear;
      endYear = dataYear;
    }

    // Default to current year if nothing specified
    if (startYear == null) {
      startYear = 2024;
    }
    if (endYear == null) {
      endYear = startYear;
    }

    // Calculate which census years to include based on the date range
    List<Integer> censusYears = determineCensusYears(startYear, endYear);
    List<Integer> tigerYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      tigerYears.add(year);
    }

    Boolean autoDownload = (Boolean) mutableOperand.getOrDefault("autoDownload", true);

    // GEO data TTL configuration (in days) - default 90 days for quarterly boundary updates
    Integer geoCacheTtlDays = (Integer) mutableOperand.getOrDefault("geoCacheTtlDays", 365);
    if (geoCacheTtlDays < 1) {
      geoCacheTtlDays = 90; // Ensure minimum 1 day TTL
    }

    // Create cache directory structure
    File cacheRoot = new File(cacheDir);
    if (!cacheRoot.exists()) {
      if (!cacheRoot.mkdirs()) {
        throw new RuntimeException("Failed to create cache directory: " + cacheDir);
      }
    }

    // Create hive-partitioned directory structure for parquet files
    File parquetRoot = new File(geoParquetDir);
    File boundaryDir = new File(parquetRoot, BOUNDARY_TYPE);
    File demographicDir = new File(parquetRoot, DEMOGRAPHIC_TYPE);
    File crosswalkDir = new File(parquetRoot, CROSSWALK_TYPE);

    for (File dir : new File[]{parquetRoot, boundaryDir, demographicDir, crosswalkDir}) {
      if (!dir.exists() && !dir.mkdirs()) {
        LOGGER.warn("Failed to create directory: {}", dir);
      }
    }

    // Log the partitioned structure
    LOGGER.info("Geographic data partitions created:");
    LOGGER.info("  Boundaries: {}", boundaryDir);
    LOGGER.info("  Demographics: {}", demographicDir);
    LOGGER.info("  Crosswalks: {}", crosswalkDir);

    // Log configuration
    LOGGER.info("Geographic data configuration:");
    LOGGER.info("  Cache directory: {}", cacheDir);
    LOGGER.info("  Enabled sources: {}", String.join(", ", enabledSources));
    LOGGER.info("  Year range: {} - {}", startYear, endYear);
    LOGGER.info("  Auto-download: {}", autoDownload);
    LOGGER.info("  GEO cache TTL: {} days", geoCacheTtlDays);
    LOGGER.info("  Census API key: {}", censusApiKey != null ? "configured" : "not configured");
    LOGGER.info("  HUD credentials: {}", hudUsername != null ? "configured" : "not configured");
    LOGGER.info("  HUD token: {}", hudToken != null ? "configured" : "not configured");

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.info("Auto-download enabled for GEO data");
      try {
        downloadGeoData(mutableOperand, cacheDir, geoParquetDir, censusApiKey, hudUsername, hudPassword, hudToken,
            enabledSources, tigerYears, censusYears, geoCacheTtlDays, storageProvider);
      } catch (Exception e) {
        LOGGER.error("Error downloading GEO data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Now configure for FileSchemaFactory
    // Set the directory to the parquet directory with hive-partitioned structure
    mutableOperand.put("directory", geoParquetDir);

    // Pass through executionEngine if specified (critical for DuckDB vs PARQUET)
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
      LOGGER.info("GeoSchemaFactory: Passing through executionEngine: " + operand.get("executionEngine"));
    } else {
      LOGGER.info("GeoSchemaFactory: No executionEngine specified in operand");
    }

    // Set casing conventions
    if (!mutableOperand.containsKey("tableNameCasing")) {
      mutableOperand.put("tableNameCasing", "SMART_CASING");
    }
    if (!mutableOperand.containsKey("columnNameCasing")) {
      mutableOperand.put("columnNameCasing", "SMART_CASING");
    }

    // Load table definitions from geo-schema.json
    List<Map<String, Object>> geoTables = loadTableDefinitions();
    if (!geoTables.isEmpty()) {
      // Update patterns with the actual parquet directory
      for (Map<String, Object> table : geoTables) {
        String pattern = (String) table.get("pattern");
        if (pattern != null) {
          // Convert relative pattern to absolute path
          table.put("pattern", pattern);
        }
      }
      mutableOperand.put("partitionedTables", geoTables);
      LOGGER.info("Built {} GEO table definitions from geo-schema.json", geoTables.size());
    }

    // Add automatic constraint definitions if enabled
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
    if (enableConstraints == null) {
      enableConstraints = true; // Default to true
    }

    // Build constraint metadata for geographic tables
    Map<String, Map<String, Object>> geoConstraints = new HashMap<>();

    if (enableConstraints) {
      // Load constraints from geo-schema.json
      geoConstraints.putAll(loadTableConstraints());
    }

    // Merge with any constraints from model file
    if (tableConstraints != null) {
      geoConstraints.putAll(tableConstraints);
    }

    if (!geoConstraints.isEmpty()) {
      mutableOperand.put("tableConstraints", geoConstraints);
    }

    // Return the configured operand for GovDataSchemaFactory to use
    LOGGER.info("GEO schema operand configuration complete");
    return mutableOperand;
  }

  /**
   * Download geographic data from various sources.
   */
  private void downloadGeoData(Map<String, Object> operand, String cacheDir, String geoParquetDir, String censusApiKey,
      String hudUsername, String hudPassword, String hudToken, String[] enabledSources,
      List<Integer> tigerYears, List<Integer> censusYears, Integer geoCacheTtlDays,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) throws IOException {

    // cacheDir should already have tilde expanded from create() method
    LOGGER.info("Checking geographic data cache: {}", geoParquetDir);

    // Set up TTL configuration for all GEO data sources
    long geoDataTtlMillis = geoCacheTtlDays * 24L * 60 * 60 * 1000; // Convert days to milliseconds
    long currentTime = System.currentTimeMillis();

    // Load or create cache manifest for tracking parquet conversions
    GeoCacheManifest cacheManifest = GeoCacheManifest.load(cacheDir);

    // Download TIGER data if enabled
    if (Arrays.asList(enabledSources).contains("tiger") && !tigerYears.isEmpty()) {

      // Check which years need TIGER data processing (per-year granularity)
      String[] tigerFiles = {"states.parquet", "counties.parquet", "places.parquet", "zctas.parquet",
          "census_tracts.parquet", "block_groups.parquet", "cbsa.parquet", "congressional_districts.parquet", "school_districts.parquet"};

      List<Integer> yearsToProcess = new ArrayList<>();
      for (int year : tigerYears) {
        boolean yearFullyCached = true;

        for (String filename : tigerFiles) {
          String parquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/" + filename);
          try {
            if (!storageProvider.exists(parquetPath)) {
              yearFullyCached = false;
              LOGGER.debug("Missing TIGER parquet file: {}", parquetPath);
              break;
            } else {
              StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
              if (metadata.getSize() == 0) {
                yearFullyCached = false;
                LOGGER.debug("Empty TIGER parquet file: {}", parquetPath);
                break;
              }

              // Check TTL - GEO data expires after 90 days
              long fileAge = currentTime - metadata.getLastModified();
              if (fileAge > geoDataTtlMillis) {
                yearFullyCached = false;
                LOGGER.debug("Expired TIGER parquet file (age: {} days): {}",
                    fileAge / (24 * 60 * 60 * 1000), parquetPath);
                break;
              }
            }
          } catch (IOException e) {
            yearFullyCached = false;
            LOGGER.debug("Cannot access TIGER parquet file: {}", parquetPath);
            break;
          }
        }

        if (!yearFullyCached) {
          yearsToProcess.add(year);
        }
      }

      if (yearsToProcess.isEmpty()) {
        LOGGER.info("TIGER data already fully cached for years: {}", tigerYears);
      } else {
        LOGGER.info("TIGER data needs processing for years: {}", yearsToProcess);
        // Use simple cache directory structure for raw data downloads
        File tigerCacheDir = new File(cacheDir, "tiger");
        TigerDataDownloader tigerDownloader = new TigerDataDownloader(tigerCacheDir, tigerYears, true, storageProvider, cacheManifest);

        try {
          // Download and convert TIGER data only for years that need processing
          for (int year : yearsToProcess) {
            LOGGER.info("Processing TIGER data for year {}", year);

            // Download and convert each dataset only if parquet doesn't exist
            String statesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/states.parquet");
            if (!storageProvider.exists(statesParquetPath)) {
              tigerDownloader.downloadStatesForYear(year);
              tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), statesParquetPath);
            }

            String countiesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/counties.parquet");
            if (!storageProvider.exists(countiesParquetPath)) {
              tigerDownloader.downloadCountiesForYear(year);
              tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), countiesParquetPath);
            }

            String placesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/places.parquet");
            if (!storageProvider.exists(placesParquetPath)) {
              tigerDownloader.downloadPlacesForYear(year, "06"); // California
              tigerDownloader.downloadPlacesForYear(year, "48"); // Texas
              tigerDownloader.downloadPlacesForYear(year, "36"); // New York
              tigerDownloader.downloadPlacesForYear(year, "12"); // Florida
              tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), placesParquetPath);
            }

            String zctasParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/zctas.parquet");
            if (!storageProvider.exists(zctasParquetPath)) {
              File zctasDir = tigerDownloader.downloadZctasForYear(year);
              if (zctasDir != null) {
                tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), zctasParquetPath);
              } else {
                // Create zero-row marker file for missing data (e.g., ZCTAs only exist for decennial census years)
                LOGGER.info("Creating zero-row marker for ZCTAs year {} (data not available)", year);
                createZeroRowTigerParquet(zctasParquetPath, "zctas", storageProvider);
              }
            }

            String tractsParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/census_tracts.parquet");
            if (!storageProvider.exists(tractsParquetPath)) {
              tigerDownloader.downloadCensusTractsForYear(year);
              tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), tractsParquetPath);
            }

            String blockGroupsParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/block_groups.parquet");
            if (!storageProvider.exists(blockGroupsParquetPath)) {
              tigerDownloader.downloadBlockGroupsForYear(year);
              tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), blockGroupsParquetPath);
            }

            String cbsaParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/cbsa.parquet");
            if (!storageProvider.exists(cbsaParquetPath)) {
              try {
                File cbsaDir = tigerDownloader.downloadCbsasForYear(year);
                if (cbsaDir != null) {
                  tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), cbsaParquetPath);
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for CBSAs year {} (data not available)", year);
                  createZeroRowTigerParquet(cbsaParquetPath, "cbsa", storageProvider);
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to download/convert CBSAs for year {}: {}", year, e.getMessage());
                // Create zero-row marker on exception (likely 404)
                try {
                  createZeroRowTigerParquet(cbsaParquetPath, "cbsa", storageProvider);
                } catch (IOException markerEx) {
                  LOGGER.error("Failed to create zero-row marker for CBSAs year {}: {}", year, markerEx.getMessage());
                }
              }
            }

            String congressionalParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/congressional_districts.parquet");
            if (!storageProvider.exists(congressionalParquetPath)) {
              try {
                File cdDir = tigerDownloader.downloadCongressionalDistrictsForYear(year);
                if (cdDir != null) {
                  tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), congressionalParquetPath);
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for congressional districts year {} (data not available)", year);
                  createZeroRowTigerParquet(congressionalParquetPath, "congressional_districts", storageProvider);
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to download/convert congressional districts for year {}: {}", year, e.getMessage());
              }
            }

            String schoolParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/school_districts.parquet");
            if (!storageProvider.exists(schoolParquetPath)) {
              try {
                File schoolDir = tigerDownloader.downloadSchoolDistrictsForYear(year, schoolParquetPath);
                if (schoolDir != null) {
                  tigerDownloader.convertToParquet(new File(tigerCacheDir, "year=" + year), schoolParquetPath);
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for school districts year {} (data not available)", year);
                  createZeroRowTigerParquet(schoolParquetPath, "school_districts", storageProvider);
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to download/convert school districts for year {}: {}", year, e.getMessage());
              }
            }
          }

          LOGGER.info("TIGER data processing completed for years: {}", yearsToProcess);

        } catch (Exception e) {
          LOGGER.error("Error processing TIGER data", e);
        }
      }
    }

    // Download HUD crosswalk data if enabled
    if (Arrays.asList(enabledSources).contains("hud") &&
        (hudUsername != null || hudToken != null)) {

      // Check if HUD crosswalk parquet files already exist with TTL validation
      boolean allHudFilesCached = true;
      for (int year : tigerYears) {
        String[] hudFiles = {"zip_county_crosswalk.parquet", "zip_cbsa_crosswalk.parquet", "tract_zip_crosswalk.parquet"};

        for (String filename : hudFiles) {
          String parquetPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/" + filename);
          try {
            if (!storageProvider.exists(parquetPath)) {
              allHudFilesCached = false;
              LOGGER.info("Missing HUD crosswalk parquet file: {}", parquetPath);
              break;
            } else {
              StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
              if (metadata.getSize() == 0) {
                allHudFilesCached = false;
                LOGGER.info("Empty HUD crosswalk parquet file: {}", parquetPath);
                break;
              }

              // Check TTL - HUD crosswalk data expires after 90 days
              long fileAge = currentTime - metadata.getLastModified();
              if (fileAge > geoDataTtlMillis) {
                allHudFilesCached = false;
                LOGGER.info("Expired HUD crosswalk parquet file (age: {} days): {}",
                    fileAge / (24 * 60 * 60 * 1000), parquetPath);
                break;
              }
            }
          } catch (IOException e) {
            allHudFilesCached = false;
            LOGGER.info("Cannot access HUD crosswalk parquet file: {}", parquetPath);
            break;
          }
        }
        if (!allHudFilesCached) break;
      }

      if (allHudFilesCached) {
        LOGGER.info("HUD crosswalk data already fully cached for years: {}", tigerYears);
      } else {
        // Use simple cache directory structure for raw data downloads
        File hudCacheDir = new File(cacheDir, "hud");
        File crosswalkParquetDir = new File(geoParquetDir, CROSSWALK_TYPE);
        HudCrosswalkFetcher hudFetcher;

        if (hudToken != null && !hudToken.isEmpty()) {
          hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir, storageProvider, cacheManifest);
        } else {
          hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir, storageProvider, cacheManifest);
        }

        try {
          LOGGER.info("Downloading HUD-USPS crosswalk data");
          // Use the new downloadAll pattern matching ECON standard
          int startYear = tigerYears.isEmpty() ? 2024 : tigerYears.get(0);
          int endYear = tigerYears.isEmpty() ? 2024 : tigerYears.get(tigerYears.size() - 1);
          hudFetcher.downloadAll(startYear, endYear);

          // Convert to Parquet for each year
          for (int year : tigerYears) {
            String zipCountyPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/zip_county_crosswalk.parquet");
            hudFetcher.convertToParquet(new File(hudCacheDir, "year=" + year), zipCountyPath);

            String zipCbsaPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/zip_cbsa_crosswalk.parquet");
            hudFetcher.convertToParquet(new File(hudCacheDir, "year=" + year), zipCbsaPath);

            String tractZipPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/tract_zip_crosswalk.parquet");
            hudFetcher.convertToParquet(new File(hudCacheDir, "year=" + year), tractZipPath);
          }

        } catch (Exception e) {
          LOGGER.error("Error downloading HUD crosswalk data", e);
        }
      }
    }

    // Download Census API data if enabled
    if (Arrays.asList(enabledSources).contains("census") &&
        censusApiKey != null && !censusYears.isEmpty()) {

      // Check if Census demographic parquet files already exist with TTL validation
      boolean allCensusFilesCached = true;
      for (int year : censusYears) {
        String[] censusFiles = {"population_demographics.parquet", "housing_characteristics.parquet", "economic_indicators.parquet"};

        for (String filename : censusFiles) {
          String parquetPath = storageProvider.resolvePath(geoParquetDir, DEMOGRAPHIC_TYPE + "/year=" + year + "/" + filename);
          try {
            if (!storageProvider.exists(parquetPath)) {
              allCensusFilesCached = false;
              LOGGER.info("Missing Census demographic parquet file: {}", parquetPath);
              break;
            } else {
              StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
              if (metadata.getSize() == 0) {
                allCensusFilesCached = false;
                LOGGER.info("Empty Census demographic parquet file: {}", parquetPath);
                break;
              }

              // Check TTL - Census demographic data expires after configured TTL
              long fileAge = currentTime - metadata.getLastModified();
              if (fileAge > geoDataTtlMillis) {
                allCensusFilesCached = false;
                LOGGER.info("Expired Census demographic parquet file (age: {} days): {}",
                    fileAge / (24 * 60 * 60 * 1000), parquetPath);
                break;
              }
            }
          } catch (IOException e) {
            allCensusFilesCached = false;
            LOGGER.info("Cannot access Census demographic parquet file: {}", parquetPath);
            break;
          }
        }
        if (!allCensusFilesCached) break;
      }

      if (allCensusFilesCached) {
        LOGGER.info("Census demographic data already fully cached for years: {}", censusYears);
      } else {
        // Use simple cache directory structure for raw data downloads
        File censusCacheDir = new File(cacheDir, "census");
        File demographicParquetDir = new File(geoParquetDir, DEMOGRAPHIC_TYPE);
        CensusApiClient censusClient = new CensusApiClient(censusApiKey, censusCacheDir, censusYears, storageProvider, cacheManifest);

        try {
          LOGGER.info("Downloading Census demographic data for years: {}", censusYears);
          // Use the new downloadAll pattern matching ECON standard
          int startYear = censusYears.isEmpty() ? 2020 : censusYears.get(0);
          int endYear = censusYears.isEmpty() ? 2020 : censusYears.get(censusYears.size() - 1);
          censusClient.downloadAll(startYear, endYear);

          // Convert to Parquet for each year
          for (int year : censusYears) {
            String populationPath = storageProvider.resolvePath(geoParquetDir, DEMOGRAPHIC_TYPE + "/year=" + year + "/population_demographics.parquet");
            censusClient.convertToParquet(new File(censusCacheDir, "year=" + year), populationPath);

            String housingPath = storageProvider.resolvePath(geoParquetDir, DEMOGRAPHIC_TYPE + "/year=" + year + "/housing_characteristics.parquet");
            censusClient.convertToParquet(new File(censusCacheDir, "year=" + year), housingPath);

            String economicPath = storageProvider.resolvePath(geoParquetDir, DEMOGRAPHIC_TYPE + "/year=" + year + "/economic_indicators.parquet");
            censusClient.convertToParquet(new File(censusCacheDir, "year=" + year), economicPath);
          }

        } catch (Exception e) {
          LOGGER.error("Error downloading Census data", e);
        }
      }
    }
  }




  /**
   * Load table definitions from geo-schema.json resource file.
   */

  /**
   * Load constraint definitions from geo-schema.json resource file.
   */
  private static Map<String, Map<String, Object>> loadGeoTableConstraints() {
    try (InputStream is = GeoSchemaFactory.class.getResourceAsStream("/geo-schema.json")) {
      if (is == null) {
        throw new IllegalStateException("Could not find geo-schema.json resource file");
      }

      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      Map<String, Map<String, Object>> constraints = (Map<String, Map<String, Object>>) schema.get("constraints");
      if (constraints == null) {
        LOGGER.info("No 'constraints' field found in geo-schema.json - using empty constraints");
        return new HashMap<>();
      }
      LOGGER.info("Loaded constraints for {} tables from geo-schema.json", constraints.size());
      return constraints;
    } catch (IOException e) {
      throw new RuntimeException("Error loading geo-schema.json", e);
    }
  }


  @Override public boolean supportsConstraints() {
    // Enable constraint support for geographic data
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
   * Determine which census years to include based on the date range.
   *
   * Census data is collected every 10 years (decennial census) on years ending in 0.
   * This method will:
   * 1. Always include the most recent census prior to or on the end year
   * 2. Include any census years that fall within the start-end range
   *
   * @param startYear Start of the year range
   * @param endYear End of the year range
   * @return List of census years to include
   */
  private List<Integer> determineCensusYears(int startYear, int endYear) {
    List<Integer> censusYears = new ArrayList<>();

    // Find the most recent census year at or before endYear
    int mostRecentCensus = (endYear / 10) * 10;
    if (mostRecentCensus > endYear) {
      mostRecentCensus -= 10;
    }

    // Always include the most recent census
    if (mostRecentCensus >= 1990) {  // Census data available from 1990
      censusYears.add(mostRecentCensus);
    }

    // Add any additional census years within the range
    for (int year = startYear; year < mostRecentCensus; year += 10) {
      int censusYear = (year / 10) * 10;
      if (censusYear >= startYear && censusYear <= endYear &&
          censusYear >= 1990 && !censusYears.contains(censusYear)) {
        censusYears.add(censusYear);
      }
    }

    // Sort the years
    censusYears.sort(Integer::compareTo);

    LOGGER.info("Census years to include for range {}-{}: {}",
        startYear, endYear, censusYears);

    return censusYears;
  }

  /**
   * Create a zero-row parquet file for TIGER data that doesn't exist (e.g., 404).
   * This prevents repeated download attempts on subsequent startups.
   */
  private void createZeroRowTigerParquet(String targetPath, String dataType,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) throws IOException {
    LOGGER.info("Creating zero-row TIGER parquet file: {}", targetPath);

    // Create minimal schema with geoid column (common to all TIGER tables)
    org.apache.avro.SchemaBuilder.RecordBuilder<org.apache.avro.Schema> recordBuilder =
        org.apache.avro.SchemaBuilder
            .record(dataType)
            .namespace("org.apache.calcite.adapter.govdata.geo")
            .doc("TIGER " + dataType + " data (zero rows - data not available for this year)");

    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields = recordBuilder.fields();
    fields = fields.name("geoid").type().stringType().noDefault();
    fields = fields.name("name").type().nullable().stringType().noDefault();

    org.apache.avro.Schema avroSchema = fields.endRecord();

    // Create empty record list
    java.util.List<org.apache.avro.generic.GenericRecord> emptyRecords = new java.util.ArrayList<>();

    // Write empty parquet file using storageProvider
    storageProvider.writeAvroParquet(targetPath, avroSchema, emptyRecords, "GenericRecord");

    LOGGER.info("Successfully created zero-row TIGER parquet file: {}", targetPath);
  }
}
