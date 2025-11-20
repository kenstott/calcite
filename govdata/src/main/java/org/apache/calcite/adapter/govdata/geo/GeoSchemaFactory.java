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
import org.apache.calcite.adapter.govdata.CacheKey;
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
    return "/geo/geo-schema.json";
  }

  /**
   * Builds the operand configuration for GEO schema.
   * This method is called by GovDataSchemaFactory to build a unified FileSchema configuration.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, org.apache.calcite.adapter.govdata.GovDataSchemaFactory parent) {
    LOGGER.info("Building GEO schema operand configuration");

    // Access shared services from parent
    org.apache.calcite.adapter.file.storage.StorageProvider storageProvider = parent.getStorageProvider();

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
    // Use storageProvider.resolvePath() for S3 compatibility (matches ECON pattern)
    String geoRawDir = storageProvider.resolvePath(govdataCacheDir, "geo");
    String geoParquetDir = storageProvider.resolvePath(govdataParquetDir, "source=geo");

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
    // Calculate which voting years to include (CPS Voting Supplement is biennial)
    List<Integer> votingYears = determineVotingYears(startYear, endYear);

    Boolean autoDownload = (Boolean) mutableOperand.getOrDefault("autoDownload", true);

    // GEO data TTL configuration (in days) - default 90 days for quarterly boundary updates
    Integer geoCacheTtlDays = (Integer) mutableOperand.getOrDefault("geoCacheTtlDays", 365);
    if (geoCacheTtlDays < 1) {
      geoCacheTtlDays = 90; // Ensure minimum 1 day TTL
    }

    // NOTE: Directory creation removed for S3 compatibility
    // Both cacheDir and geoParquetDir may be S3 URIs (e.g., s3://govdata-production-cache)
    // S3 buckets are managed via AWS/MinIO, not local filesystem
    // StorageProvider handles S3 directory creation automatically when writing files

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

    // Add schema-level comment from JSON metadata
    String schemaComment = loadSchemaComment();
    if (schemaComment != null) {
      mutableOperand.put("comment", schemaComment);
    }

    // Add voting years for CPS Voting Supplement table
    if (!votingYears.isEmpty()) {
      mutableOperand.put("votingYears", votingYears);
      LOGGER.info("Added voting years to GEO operand: {}", votingYears);
    }

    // Return the configured operand for GovDataSchemaFactory to use
    LOGGER.info("GEO schema operand configuration complete");
    return mutableOperand;
  }

  /**
   * Metadata-driven download of geographic data using SchemaConfigReader.
   * Replaces hardcoded table logic with JSON-driven configuration.
   *
   * @param operand Schema configuration operand
   * @param cacheDir Cache directory for raw downloads
   * @param geoParquetDir Parquet output directory
   * @param storageProvider Storage provider for file operations
   * @param tigerYears Years to process for TIGER data
   * @param cacheManifest Cache manifest for tracking downloads
   */
  private void downloadGeoDataMetadataDriven(Map<String, Object> operand, String cacheDir,
      String geoParquetDir, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider,
      List<Integer> tigerYears, GeoCacheManifest cacheManifest) {

    LOGGER.info("Starting metadata-driven geo data download");

    // Get all TIGER tables from schema configuration
    List<String> tigerTables = org.apache.calcite.adapter.govdata.SchemaConfigReader
        .getTablesWithDownloadConfig("geo-schema.json", "dataSource", "tiger");

    if (tigerTables.isEmpty()) {
      LOGGER.info("No TIGER tables configured for download");
      return;
    }

    LOGGER.info("Found {} TIGER tables configured for download: {}",
        tigerTables.size(), tigerTables);

    String geoOperatingDirectory = (String) operand.get("operatingDirectory");
    String tigerCacheDir = storageProvider.resolvePath(cacheDir, "tiger");

    // Create TIGER downloader
    TigerDataDownloader tigerDownloader =
        new TigerDataDownloader(tigerCacheDir, geoOperatingDirectory, tigerYears, true,
        storageProvider, cacheManifest);

    // For each table and year combination, check if download/conversion needed
    for (String tableName : tigerTables) {
      for (int year : tigerYears) {
        try {
          downloadAndConvertTigerTable(tableName, year, tigerDownloader,
              geoParquetDir, storageProvider);
        } catch (Exception e) {
          LOGGER.error("Error processing TIGER table {} for year {}: {}",
              tableName, year, e.getMessage());
          LOGGER.debug("Error details: ", e);
        }
      }
    }

    LOGGER.info("Metadata-driven geo data download complete");
  }

  /**
   * Download and convert a single TIGER table for a specific year.
   * Uses schema configuration to determine download URLs and conversion settings.
   */
  private void downloadAndConvertTigerTable(String tableName, int year,
      TigerDataDownloader tigerDownloader, String geoParquetDir,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) throws Exception {

    // Build paths
    String parquetPath =
        storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/" + tableName + ".parquet");

    java.util.Map<String, String> params = new java.util.HashMap<>();
    params.put("year", String.valueOf(year));

    // Check if already converted (via manifest or file existence)
    CacheKey cacheKey = new CacheKey(tableName, params);
    if (tigerDownloader.getCacheManifest().isParquetConverted(cacheKey)) {
      LOGGER.debug("Table {} year {} already converted per manifest", tableName, year);
      return;
    }

    // Check if parquet file exists
    try {
      if (storageProvider.exists(parquetPath)) {
        StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);
        if (metadata.getSize() > 0) {
          LOGGER.debug("Table {} year {} parquet already exists", tableName, year);
          // Update manifest since file exists but wasn't tracked
          tigerDownloader.getCacheManifest().markParquetConverted(cacheKey, parquetPath);
          tigerDownloader.getCacheManifest().save(
              tigerDownloader.getOperatingDirectory());
          return;
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Could not check parquet existence for {} year {}: {}",
          tableName, year, e.getMessage());
    }

    // Need to download and convert
    LOGGER.info("Downloading and converting TIGER table {} for year {}", tableName, year);

    // Download based on table type
    File downloadedDir = downloadTigerTableForYear(tigerDownloader, tableName, year);

    if (downloadedDir != null && downloadedDir.exists()) {
      // Convert to Parquet (will use DuckDB spatial if ZIP file available)
      tigerDownloader.convertToParquet(downloadedDir, parquetPath);
      LOGGER.info("Successfully processed TIGER table {} for year {}", tableName, year);
    } else {
      LOGGER.warn("Failed to download TIGER table {} for year {}", tableName, year);
    }
  }

  /**
   * Download a TIGER table for a specific year.
   * Routes to the appropriate TigerDataDownloader method based on table name.
   */
  private File downloadTigerTableForYear(TigerDataDownloader downloader,
      String tableName, int year) {
    try {
      switch (tableName) {
        case "states":
          return downloader.downloadStatesForYear(year);
        case "counties":
          return downloader.downloadCountiesForYear(year);
        case "places":
          // Download multiple states for places
          downloader.downloadPlacesForYear(year, "06"); // California
          downloader.downloadPlacesForYear(year, "48"); // Texas
          downloader.downloadPlacesForYear(year, "36"); // New York
          return downloader.downloadPlacesForYear(year, "12"); // Florida (returns dir)
        case "zctas":
          return downloader.downloadZctasForYear(year);
        case "census_tracts":
          return downloader.downloadCensusTractsForYear(year);
        case "block_groups":
          return downloader.downloadBlockGroupsForYear(year);
        case "cbsa":
          return downloader.downloadCbsasForYear(year);
        case "congressional_districts":
          return downloader.downloadCongressionalDistrictsForYear(year);
        case "school_districts":
          return downloader.downloadSchoolDistrictsForYear(year, "06"); // California example
        default:
          LOGGER.warn("Unknown TIGER table type: {}", tableName);
          return null;
      }
    } catch (Exception e) {
      LOGGER.error("Error downloading TIGER table {} for year {}: {}",
          tableName, year, e.getMessage());
      return null;
    }
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

    // Operating directory for metadata (.aperio/geo/)
    // This is passed from GovDataSchemaFactory which establishes it centrally
    // The .aperio directory is ALWAYS on local filesystem (working directory), even if parquet data is on S3
    String geoOperatingDirectory = (String) operand.get("operatingDirectory");
    if (geoOperatingDirectory == null) {
      throw new IllegalStateException("Operating directory must be established by GovDataSchemaFactory");
    }
    LOGGER.debug("Received operating directory from parent: {}", geoOperatingDirectory);

    // Load or create cache manifest from operating directory
    GeoCacheManifest cacheManifest = GeoCacheManifest.load(geoOperatingDirectory);
    LOGGER.debug("Loaded GEO cache manifest from {}", geoOperatingDirectory);

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

          // Extract dataType from filename (e.g., "states.parquet" -> "states")
          String dataType = filename.replace(".parquet", "");

          // Check manifest first - if parquet is marked as converted, skip
          boolean parquetConverted = false;
          if (cacheManifest != null) {
            java.util.Map<String, String> params = new java.util.HashMap<>();
            params.put("year", String.valueOf(year));
            CacheKey cacheKey = new CacheKey(dataType, params);
            parquetConverted = cacheManifest.isParquetConverted(cacheKey);
          }

          if (parquetConverted) {
            // Manifest says it's converted (may be zero-row marker)
            LOGGER.debug("Dataset {} year {} already converted per manifest", dataType, year);
            continue;
          }

          // Not in manifest, check if S3 file exists
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
        String tigerCacheDir = storageProvider.resolvePath(cacheDir, "tiger");
        TigerDataDownloader tigerDownloader = new TigerDataDownloader(tigerCacheDir, geoOperatingDirectory, tigerYears, true, storageProvider, cacheManifest);

        try {
          // Download and convert TIGER data only for years that need processing
          for (int year : yearsToProcess) {
            LOGGER.info("Processing TIGER data for year {}", year);

            // Download and convert each dataset only if parquet doesn't exist
            // Note: Download methods return temp File directories containing the data
            String statesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/states.parquet");
            String statesShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/states");
            java.util.Map<String, String> statesParams = new java.util.HashMap<>();

            // Check if parquet exists and is up-to-date (with fallback to file existence check)
            if (!tigerDownloader.isParquetConvertedOrExists("states", year, statesParams, statesShapefilePath, statesParquetPath)) {
              File statesDir = tigerDownloader.downloadStatesForYear(year);
              if (statesDir != null) {
                tigerDownloader.convertToParquet(statesDir, statesParquetPath);
              }
            }

            String countiesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/counties.parquet");
            String countiesShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/counties");
            java.util.Map<String, String> countiesParams = new java.util.HashMap<>();

            if (!tigerDownloader.isParquetConvertedOrExists("counties", year, countiesParams, countiesShapefilePath, countiesParquetPath)) {
              File countiesDir = tigerDownloader.downloadCountiesForYear(year);
              if (countiesDir != null) {
                tigerDownloader.convertToParquet(countiesDir, countiesParquetPath);
              }
            }

            String placesParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/places.parquet");
            // Places are aggregated across multiple states, use first state's path for timestamp check
            String placesShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/places/06");
            java.util.Map<String, String> placesParams = new java.util.HashMap<>();

            if (!tigerDownloader.isParquetConvertedOrExists("places", year, placesParams, placesShapefilePath, placesParquetPath)) {
              tigerDownloader.downloadPlacesForYear(year, "06"); // California
              tigerDownloader.downloadPlacesForYear(year, "48"); // Texas
              tigerDownloader.downloadPlacesForYear(year, "36"); // New York
              File placesDir = tigerDownloader.downloadPlacesForYear(year, "12"); // Florida (last one returns dir)
              if (placesDir != null) {
                tigerDownloader.convertToParquet(placesDir, placesParquetPath);
              }
            }

            String zctasParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/zctas.parquet");
            String zctasShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/zctas");
            java.util.Map<String, String> zctasParams = new java.util.HashMap<>();

            if (!tigerDownloader.isParquetConvertedOrExists("zctas", year, zctasParams, zctasShapefilePath, zctasParquetPath)) {
              File zctasDir = tigerDownloader.downloadZctasForYear(year);
              if (zctasDir != null) {
                tigerDownloader.convertToParquet(zctasDir, zctasParquetPath);
              } else {
                // Create zero-row marker file for missing data (e.g., ZCTAs only exist for decennial census years)
                LOGGER.info("Creating zero-row marker for ZCTAs year {} (data not available)", year);
                createZeroRowTigerParquet(zctasParquetPath, "zctas", storageProvider);
                // Mark in manifest to avoid recreating on next startup
                if (cacheManifest != null) {
                  java.util.Map<String, String> allParams = new java.util.HashMap<>(zctasParams);
                  allParams.put("year", String.valueOf(year));
                  CacheKey cacheKey = new CacheKey("zctas", allParams);
                  cacheManifest.markParquetConverted(cacheKey, zctasParquetPath);
                  cacheManifest.save(geoOperatingDirectory);
                }
              }
            }

            String tractsParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/census_tracts.parquet");
            String tractsCacheBasePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/census_tracts");
            String[] tractStates = {"06", "48", "36", "12"}; // CA, TX, NY, FL

            if (tigerDownloader.needsProcessingMultiState("census_tracts", year, tractStates, tractsCacheBasePath, tractsParquetPath)) {
              File tractsDir = tigerDownloader.downloadCensusTractsForYear(year);
              if (tractsDir != null) {
                tigerDownloader.convertToParquet(tractsDir, tractsParquetPath);
                // Mark each state as converted in manifest after successful parquet conversion
                if (cacheManifest != null) {
                  for (String stateFips : tractStates) {
                    java.util.Map<String, String> params = new java.util.HashMap<>();
                    params.put("state", stateFips);
                    params.put("year", String.valueOf(year));
                    CacheKey cacheKey = new CacheKey("census_tracts", params);
                    cacheManifest.markParquetConverted(cacheKey, tractsParquetPath);
                  }
                  cacheManifest.save(geoOperatingDirectory);
                }
              }
            }

            String blockGroupsParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/block_groups.parquet");
            String blockGroupsCacheBasePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/block_groups");
            String[] blockGroupStates = {"06", "48", "36", "12"}; // CA, TX, NY, FL

            if (tigerDownloader.needsProcessingMultiState("block_groups", year, blockGroupStates, blockGroupsCacheBasePath, blockGroupsParquetPath)) {
              File blockGroupsDir = tigerDownloader.downloadBlockGroupsForYear(year);
              if (blockGroupsDir != null) {
                tigerDownloader.convertToParquet(blockGroupsDir, blockGroupsParquetPath);
                // Mark each state as converted in manifest after successful parquet conversion
                if (cacheManifest != null) {
                  for (String stateFips : blockGroupStates) {
                    java.util.Map<String, String> params = new java.util.HashMap<>();
                    params.put("state", stateFips);
                    params.put("year", String.valueOf(year));
                    CacheKey cacheKey = new CacheKey("block_groups", params);
                    cacheManifest.markParquetConverted(cacheKey, blockGroupsParquetPath);
                  }
                  cacheManifest.save(geoOperatingDirectory);
                }
              }
            }

            String cbsaParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/cbsa.parquet");
            String cbsaShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/cbsa");
            java.util.Map<String, String> cbsaParams = new java.util.HashMap<>();

            if (!tigerDownloader.isParquetConvertedOrExists("cbsa", year, cbsaParams, cbsaShapefilePath, cbsaParquetPath)) {
              try {
                File cbsaDir = tigerDownloader.downloadCbsasForYear(year);
                if (cbsaDir != null) {
                  tigerDownloader.convertToParquet(cbsaDir, cbsaParquetPath);
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for CBSAs year {} (data not available)", year);
                  createZeroRowTigerParquet(cbsaParquetPath, "cbsa", storageProvider);
                  // Mark in manifest to avoid recreating on next startup
                  if (cacheManifest != null) {
                    java.util.Map<String, String> params = new java.util.HashMap<>();
                    params.put("year", String.valueOf(year));
                    CacheKey cacheKey = new CacheKey("cbsa", params);
                    cacheManifest.markParquetConverted(cacheKey, cbsaParquetPath);
                    cacheManifest.save(geoOperatingDirectory);
                  }
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to download/convert CBSAs for year {}: {}", year, e.getMessage());
                // Create zero-row marker on exception (likely 404)
                try {
                  createZeroRowTigerParquet(cbsaParquetPath, "cbsa", storageProvider);
                  // Mark in manifest
                  if (cacheManifest != null) {
                    java.util.Map<String, String> params = new java.util.HashMap<>();
                    params.put("year", String.valueOf(year));
                    CacheKey cacheKey = new CacheKey("cbsa", params);
                    cacheManifest.markParquetConverted(cacheKey, cbsaParquetPath);
                    cacheManifest.save(geoOperatingDirectory);
                  }
                } catch (IOException markerEx) {
                  LOGGER.error("Failed to create zero-row marker for CBSAs year {}: {}", year, markerEx.getMessage());
                }
              }
            }

            String congressionalParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/congressional_districts.parquet");
            String congressionalShapefilePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/congressional_districts");
            java.util.Map<String, String> congressionalParams = new java.util.HashMap<>();

            if (!tigerDownloader.isParquetConvertedOrExists("congressional_districts", year, congressionalParams, congressionalShapefilePath, congressionalParquetPath)) {
              try {
                File cdDir = tigerDownloader.downloadCongressionalDistrictsForYear(year);
                if (cdDir != null) {
                  tigerDownloader.convertToParquet(cdDir, congressionalParquetPath);
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for congressional districts year {} (data not available)", year);
                  createZeroRowTigerParquet(congressionalParquetPath, "congressional_districts", storageProvider);
                  // Mark in manifest to avoid recreating on next startup
                  if (cacheManifest != null) {
                    java.util.Map<String, String> params = new java.util.HashMap<>();
                    params.put("year", String.valueOf(year));
                    CacheKey cacheKey = new CacheKey("congressional_districts", params);
                    cacheManifest.markParquetConverted(cacheKey, congressionalParquetPath);
                    cacheManifest.save(geoOperatingDirectory);
                  }
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to download/convert congressional districts for year {}: {}", year, e.getMessage());
              }
            }

            String schoolParquetPath = storageProvider.resolvePath(geoParquetDir, BOUNDARY_TYPE + "/year=" + year + "/school_districts.parquet");
            String schoolCacheBasePath = storageProvider.resolvePath(tigerCacheDir, "year=" + year + "/school_districts");
            String[] schoolStates = {"06", "48", "36", "12"}; // CA, TX, NY, FL

            if (tigerDownloader.needsProcessingMultiState("school_districts", year, schoolStates, schoolCacheBasePath, schoolParquetPath)) {
              try {
                File schoolDir = tigerDownloader.downloadSchoolDistrictsForYear(year, schoolParquetPath);
                if (schoolDir != null) {
                  tigerDownloader.convertToParquet(schoolDir, schoolParquetPath);
                  // Mark each state as converted in manifest after successful parquet conversion
                  if (cacheManifest != null) {
                    for (String stateFips : schoolStates) {
                      java.util.Map<String, String> params = new java.util.HashMap<>();
                      params.put("state", stateFips);
                      params.put("year", String.valueOf(year));
                      CacheKey cacheKey = new CacheKey("school_districts", params);
                      cacheManifest.markParquetConverted(cacheKey, schoolParquetPath);
                    }
                    cacheManifest.save(geoOperatingDirectory);
                  }
                } else {
                  // Create zero-row marker file for missing data
                  LOGGER.info("Creating zero-row marker for school districts year {} (data not available)", year);
                  createZeroRowTigerParquet(schoolParquetPath, "school_districts", storageProvider);
                  // Mark each state in manifest to avoid recreating on next startup
                  if (cacheManifest != null) {
                    for (String stateFips : schoolStates) {
                      java.util.Map<String, String> params = new java.util.HashMap<>();
                      params.put("state", stateFips);
                      params.put("year", String.valueOf(year));
                      CacheKey cacheKey = new CacheKey("school_districts", params);
                      cacheManifest.markParquetConverted(cacheKey, schoolParquetPath);
                    }
                    cacheManifest.save(geoOperatingDirectory);
                  }
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
        String hudCacheDir = storageProvider.resolvePath(cacheDir, "hud");
        HudCrosswalkFetcher hudFetcher;

        // Calculate startYear/endYear before creating fetcher
        int hudStartYear = tigerYears.isEmpty() ? 2024 : tigerYears.get(0);
        int hudEndYear = tigerYears.isEmpty() ? 2024 : tigerYears.get(tigerYears.size() - 1);

        if (hudToken != null && !hudToken.isEmpty()) {
          hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir, geoOperatingDirectory, storageProvider, cacheManifest, hudStartYear, hudEndYear);
        } else {
          hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir, geoOperatingDirectory, storageProvider, cacheManifest, hudStartYear, hudEndYear);
        }

        try {
          LOGGER.info("Downloading HUD-USPS crosswalk data");
          // Use the new downloadAll pattern matching ECON standard
          int startYear = hudStartYear;
          int endYear = hudEndYear;
          hudFetcher.downloadAll(startYear, endYear);

          // Convert to Parquet for each year
          // Note: HUD downloader returns temp File directories from download methods
          // These temp directories are then used for conversion
          for (int year : tigerYears) {
            // HUD data is in temp directories after download
            // The convertToParquet method reads CSV files from year-specific subdirectories
            String yearPath = "year=" + year;
            File yearCacheDir;

            // For S3, download to temp; for local, use cached files directly
            if (hudCacheDir.startsWith("s3://")) {
              // Download from S3 cache to temp directory
              yearCacheDir = java.nio.file.Files.createTempDirectory("hud-year-" + year + "-").toFile();
              String cachePath = storageProvider.resolvePath(hudCacheDir, yearPath);
              java.util.List<StorageProvider.FileEntry> files = storageProvider.listFiles(cachePath, false);
              for (StorageProvider.FileEntry file : files) {
                if (!file.isDirectory()) {
                  File targetFile = new File(yearCacheDir, file.getName());
                  try (java.io.InputStream in = storageProvider.openInputStream(file.getPath());
                       java.io.OutputStream out = new java.io.FileOutputStream(targetFile)) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) != -1) {
                      out.write(buffer, 0, bytesRead);
                    }
                  }
                }
              }
            } else {
              // Local filesystem - use directly
              yearCacheDir = new File(hudCacheDir, yearPath);
            }

            String zipCountyPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/zip_county_crosswalk.parquet");
            hudFetcher.convertToParquet(yearCacheDir, zipCountyPath);

            String zipCbsaPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/zip_cbsa_crosswalk.parquet");
            hudFetcher.convertToParquet(yearCacheDir, zipCbsaPath);

            String tractZipPath = storageProvider.resolvePath(geoParquetDir, CROSSWALK_TYPE + "/year=" + year + "/tract_zip_crosswalk.parquet");
            hudFetcher.convertToParquet(yearCacheDir, tractZipPath);
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
        String censusCacheDir = storageProvider.resolvePath(cacheDir, "census");

        // Calculate startYear/endYear before creating client
        int censusStartYear = censusYears.isEmpty() ? 2020 : censusYears.get(0);
        int censusEndYear = censusYears.isEmpty() ? 2020 : censusYears.get(censusYears.size() - 1);

        CensusApiClient censusClient = new CensusApiClient(censusApiKey, censusCacheDir, geoOperatingDirectory, censusYears, storageProvider, cacheManifest, censusStartYear, censusEndYear);

        try {
          LOGGER.info("Downloading Census demographic data for years: {}", censusYears);
          // Use the new downloadAll pattern matching ECON standard
          int startYear = censusStartYear;
          int endYear = censusEndYear;
          censusClient.downloadAll(startYear, endYear);

          // Convert to Parquet using new pattern
          LOGGER.info("Converting Census demographic data to Parquet for years: {}", censusYears);
          censusClient.convertAll(startYear, endYear);

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
    try (InputStream is = GeoSchemaFactory.class.getResourceAsStream("/geo/geo-schema.json")) {
      if (is == null) {
        throw new IllegalStateException("Could not find /geo/geo-schema.json resource file");
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
   * Determine which CPS Voting Supplement years to include based on date range.
   * CPS Voting Supplement is conducted biennially (every 2 years) in November
   * election years only.
   *
   * @param startYear Start of the year range
   * @param endYear End of the year range
   * @return List of voting years to include (even years only: 2010, 2012, 2014, etc.)
   */
  private List<Integer> determineVotingYears(int startYear, int endYear) {
    List<Integer> votingYears = new ArrayList<>();

    // CPS Voting Supplement only available for even years (election years)
    // Adjust start year to next even year if it's odd
    int firstVotingYear = (startYear % 2 == 0) ? startYear : startYear + 1;

    // Add all even years in the range
    for (int year = firstVotingYear; year <= endYear; year += 2) {
      // CPS Voting Supplement available from 1964 onwards
      if (year >= 1964 && year <= 2024) {
        votingYears.add(year);
      }
    }

    LOGGER.info("Voting years to include for range {}-{}: {}",
        startYear, endYear, votingYears);

    return votingYears;
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
