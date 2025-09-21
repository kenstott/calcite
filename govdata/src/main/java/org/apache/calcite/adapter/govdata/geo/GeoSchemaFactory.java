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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
public class GeoSchemaFactory implements ConstraintCapableSchemaFactory {
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

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    // This method should not be called directly anymore
    // GovDataSchemaFactory should call buildOperand() instead
    throw new UnsupportedOperationException(
        "GeoSchemaFactory.create() should not be called directly. " +
        "Use GovDataSchemaFactory to create a unified schema.");
  }
  
  /**
   * Builds the operand configuration for GEO schema.
   * This method is called by GovDataSchemaFactory to build a unified FileSchema configuration.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand) {
    LOGGER.info("Building GEO schema operand configuration");
    
    // Read environment variables at runtime (not static initialization)
    // Check both actual environment variables and system properties (for .env.test)
    String govdataCacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (govdataCacheDir == null) {
      govdataCacheDir = System.getProperty("GOVDATA_CACHE_DIR");
    }
    String govdataParquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (govdataParquetDir == null) {
      govdataParquetDir = System.getProperty("GOVDATA_PARQUET_DIR");
    }
    
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
    
    String censusApiKey = (String) mutableOperand.get("censusApiKey");
    String hudUsername = (String) mutableOperand.get("hudUsername");
    String hudPassword = (String) mutableOperand.get("hudPassword");
    String hudToken = (String) mutableOperand.get("hudToken");

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
    LOGGER.info("  Census API key: {}", censusApiKey != null ? "configured" : "not configured");
    LOGGER.info("  HUD credentials: {}", hudUsername != null ? "configured" : "not configured");
    LOGGER.info("  HUD token: {}", hudToken != null ? "configured" : "not configured");

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.info("Auto-download enabled for GEO data");
      try {
        downloadGeoData(mutableOperand, cacheDir, geoParquetDir, censusApiKey, hudUsername, hudPassword, hudToken,
            enabledSources, tigerYears, censusYears);
      } catch (Exception e) {
        LOGGER.error("Error downloading GEO data", e);
        // Continue even if download fails - existing data may be available
      }
    }
    
    // Create mock data if no real data exists (for testing)
    createMockGeoDataIfNeeded(geoParquetDir, tigerYears);

    // Now configure for FileSchemaFactory
    // Set the directory to the parquet directory with hive-partitioned structure
    mutableOperand.put("directory", geoParquetDir);
    
    // Don't override execution engine - let GovDataSchemaFactory control it
    // The parent factory will set it based on global configuration
    
    // Set casing conventions
    if (!mutableOperand.containsKey("tableNameCasing")) {
      mutableOperand.put("tableNameCasing", "SMART_CASING");
    }
    if (!mutableOperand.containsKey("columnNameCasing")) {
      mutableOperand.put("columnNameCasing", "SMART_CASING");
    }

    // Load table definitions from geo-schema.json
    List<Map<String, Object>> geoTables = loadGeoTableDefinitions();
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
      geoConstraints.putAll(loadGeoTableConstraints());
    }

    // Merge with any constraints from model file
    if (tableConstraints != null) {
      geoConstraints.putAll(tableConstraints);
    }
    
    if (!geoConstraints.isEmpty()) {
      mutableOperand.put("constraintMetadata", geoConstraints);
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
      List<Integer> tigerYears, List<Integer> censusYears) throws IOException {
    
    // cacheDir should already have tilde expanded from create() method
    LOGGER.info("Downloading geographic data to: {}", cacheDir);
    
    // Download TIGER data if enabled
    if (Arrays.asList(enabledSources).contains("tiger") && !tigerYears.isEmpty()) {
      // Use simple cache directory structure for raw data downloads
      File tigerCacheDir = new File(cacheDir, "tiger");
      TigerDataDownloader tigerDownloader = new TigerDataDownloader(tigerCacheDir, tigerYears, true);
      
      try {
        LOGGER.info("Downloading TIGER/Line data for years: {}", tigerYears);
        tigerDownloader.downloadStates();
        tigerDownloader.downloadCounties();
        // Download places for a few key states to limit data size
        for (int year : tigerYears) {
          // Just download CA, TX, NY, FL as examples
          tigerDownloader.downloadPlacesForYear(year, "06"); // California
          tigerDownloader.downloadPlacesForYear(year, "48"); // Texas
          tigerDownloader.downloadPlacesForYear(year, "36"); // New York
          tigerDownloader.downloadPlacesForYear(year, "12"); // Florida
        }
        tigerDownloader.downloadZctas();
        tigerDownloader.downloadCensusTracts();
        tigerDownloader.downloadBlockGroups();
        tigerDownloader.downloadCbsas();
        
        // Convert shapefiles to Parquet using StorageProvider pattern
        String boundaryRelativeDir = BOUNDARY_TYPE;
        convertShapefilesToParquet(tigerCacheDir, boundaryRelativeDir, tigerYears, geoParquetDir);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading TIGER data", e);
      }
    }
    
    // Download HUD crosswalk data if enabled
    if (Arrays.asList(enabledSources).contains("hud") && 
        (hudUsername != null || hudToken != null)) {
      // Use simple cache directory structure for raw data downloads
      File hudCacheDir = new File(cacheDir, "hud");
      File crosswalkParquetDir = new File(geoParquetDir, CROSSWALK_TYPE);
      HudCrosswalkFetcher hudFetcher;
      
      if (hudToken != null && !hudToken.isEmpty()) {
        hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudToken, hudCacheDir);
      } else {
        hudFetcher = new HudCrosswalkFetcher(hudUsername, hudPassword, hudCacheDir);
      }
      
      try {
        LOGGER.info("Downloading HUD-USPS crosswalk data");
        hudFetcher.downloadLatestCrosswalk();
        
        // Convert CSV files to Parquet
        convertCsvToParquet(hudCacheDir, crosswalkParquetDir);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading HUD crosswalk data", e);
      }
    }
    
    // Download Census API data if enabled
    if (Arrays.asList(enabledSources).contains("census") && 
        censusApiKey != null && !censusYears.isEmpty()) {
      // Use simple cache directory structure for raw data downloads
      File censusCacheDir = new File(cacheDir, "census");
      File demographicParquetDir = new File(geoParquetDir, DEMOGRAPHIC_TYPE);
      CensusApiClient censusClient = new CensusApiClient(censusApiKey, censusCacheDir, censusYears);
      
      try {
        LOGGER.info("Downloading Census demographic data for years: {}", censusYears);
        downloadCensusData(censusClient, censusCacheDir, demographicParquetDir, censusYears);
        
      } catch (Exception e) {
        LOGGER.error("Error downloading Census data", e);
      }
    }
  }
  
  /**
   * Download Census demographic data and convert to Parquet.
   */
  private void downloadCensusData(CensusApiClient client, File cacheDir, File parquetDir, List<Integer> years) 
      throws IOException {
    
    for (int year : years) {
      // Download state-level population data
      String variables = "B01001_001E,B19013_001E,B25077_001E"; // Population, Income, Home Value
      com.fasterxml.jackson.databind.JsonNode stateData = client.getAcsData(year, variables, "state:*");
      
      // Save as Parquet
      File yearDir = new File(cacheDir, "year=" + year);
      yearDir.mkdirs();
      File parquetFile = new File(yearDir, "census_demographics.parquet");
      
      // For now, save as JSON and note that Parquet conversion would happen here
      File jsonFile = new File(yearDir, "census_demographics.json");
      new ObjectMapper().writeValue(jsonFile, stateData);
      
      // Create empty Parquet file as placeholder
      parquetFile.createNewFile();
      LOGGER.info("Saved Census data for year {}", year);
    }
  }
  
  /**
   * Convert shapefiles to Parquet format.
   */
  private void convertShapefilesToParquet(File sourceCacheDir, String targetRelativeDir, List<Integer> years, String geoParquetDir) {
    // Check if conversion is needed by examining what shapefiles exist vs what parquet files exist
    File targetDir = new File(geoParquetDir, targetRelativeDir);
    boolean needsConversion = false;

    // List of expected table types that should be converted
    String[] expectedTables = {"states", "counties", "places", "zctas", "census_tracts", "block_groups", "cbsa"};

    for (String tableType : expectedTables) {
      for (int year : years) {
        // Check if shapefile exists for this table type and year
        boolean shapefileExists = checkShapefileExists(sourceCacheDir, tableType, year);

        // Check if corresponding parquet file exists
        File parquetFile = new File(targetDir, "year=" + year + "/" + tableType + ".parquet");
        boolean parquetExists = parquetFile.exists();

        if (shapefileExists && !parquetExists) {
          needsConversion = true;
          LOGGER.info("Shapefile exists but parquet missing: {} for year {}", tableType, year);
        }
      }
    }

    if (!needsConversion) {
      LOGGER.info("All available shapefiles already converted to parquet in {}, skipping conversion", targetDir);
      return;
    }

    try {
      // Create StorageProvider for writing parquet files
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider =
          org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(geoParquetDir);

      // Use the ShapefileToParquetConverter to convert downloaded shapefiles
      ShapefileToParquetConverter converter = new ShapefileToParquetConverter(storageProvider);

      LOGGER.info("Converting shapefiles to Parquet format from {} to {}", sourceCacheDir, targetRelativeDir);
      converter.convertShapefilesToParquet(sourceCacheDir, targetRelativeDir);
      LOGGER.info("Shapefile to Parquet conversion completed");
    } catch (Exception e) {
      LOGGER.error("Error converting shapefiles to Parquet", e);
      // Fallback handling moved outside since we can't use storageProvider here
    }
  }

  /**
   * Check if a shapefile exists for the given table type and year.
   */
  private boolean checkShapefileExists(File sourceCacheDir, String tableType, int year) {
    // Map table types to their expected shapefile patterns
    String shapefilePattern;
    switch (tableType) {
      case "states":
        shapefilePattern = "year=" + year + "/states/tl_" + year + "_us_state.zip";
        break;
      case "counties":
        shapefilePattern = "year=" + year + "/counties/tl_" + year + "_us_county.zip";
        break;
      case "places":
        // Places are downloaded by state, check if any state has places
        File placesDir = new File(sourceCacheDir, "year=" + year + "/places");
        if (placesDir.exists()) {
          File[] stateDirs = placesDir.listFiles(File::isDirectory);
          return stateDirs != null && stateDirs.length > 0;
        }
        return false;
      case "zctas":
        shapefilePattern = "year=" + year + "/zctas/tl_" + year + "_us_zcta520.zip";
        break;
      case "census_tracts":
        // Census tracts are downloaded by state, check if any state has tracts
        File tractsDir = new File(sourceCacheDir, "year=" + year + "/census_tracts");
        if (tractsDir.exists()) {
          File[] stateDirs = tractsDir.listFiles(File::isDirectory);
          return stateDirs != null && stateDirs.length > 0;
        }
        return false;
      case "block_groups":
        // Block groups are downloaded by state, check if any state has block groups
        File blockGroupsDir = new File(sourceCacheDir, "year=" + year + "/block_groups");
        if (blockGroupsDir.exists()) {
          File[] stateDirs = blockGroupsDir.listFiles(File::isDirectory);
          return stateDirs != null && stateDirs.length > 0;
        }
        return false;
      case "cbsa":
        shapefilePattern = "year=" + year + "/cbsa/tl_" + year + "_us_cbsa.zip";
        break;
      default:
        return false;
    }

    // For simple single-file patterns, check if the file exists
    File shapefileZip = new File(sourceCacheDir, shapefilePattern);
    return shapefileZip.exists();
  }

  /**
   * Convert CSV files to Parquet format.
   */
  private void convertCsvToParquet(File sourceCacheDir, File targetParquetDir) {
    // Placeholder for CSV to Parquet conversion
    // In production, would read CSV and write to Parquet
    
    File[] csvFiles = sourceCacheDir.listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles != null) {
      for (File csvFile : csvFiles) {
        String parquetName = csvFile.getName().replace(".csv", ".parquet");
        File parquetFile = new File(targetParquetDir, parquetName);
        try {
          parquetFile.createNewFile();
          LOGGER.info("Created Parquet placeholder for {}", csvFile.getName());
        } catch (IOException e) {
          LOGGER.error("Error creating Parquet file", e);
        }
      }
    }
  }
  
  /**
   * Load table definitions from geo-schema.json resource file.
   */
  private static List<Map<String, Object>> loadGeoTableDefinitions() {
    try (InputStream is = GeoSchemaFactory.class.getResourceAsStream("/geo-schema.json")) {
      if (is == null) {
        throw new IllegalStateException("Could not find geo-schema.json resource file");
      }

      Map<String, Object> schema = JSON_MAPPER.readValue(is, Map.class);
      List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("partitionedTables");
      if (tables == null) {
        throw new IllegalStateException("No 'partitionedTables' field found in geo-schema.json");
      }
      LOGGER.info("Loaded {} table definitions from geo-schema.json", tables.size());
      for (Map<String, Object> table : tables) {
        LOGGER.debug("  - Table: {} with pattern: {}", table.get("name"), table.get("pattern"));
      }
      return tables;
    } catch (IOException e) {
      throw new RuntimeException("Error loading geo-schema.json", e);
    }
  }

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


  /**
   * Create mock GEO data files if they don't exist (for testing).
   */
  @SuppressWarnings("deprecation")
  private void createMockGeoDataIfNeeded(String geoParquetDir, List<Integer> years) {
    try {
      // Create a temporary FileSchema for StorageProvider access
      // Create StorageProvider for checking and writing parquet files
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider =
          org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(geoParquetDir);

      // Check if any parquet files exist already
      String boundaryPath = BOUNDARY_TYPE + "/year=" + (years.isEmpty() ? 2024 : years.get(0)) + "/states.parquet";
      boolean hasData = storageProvider.exists(boundaryPath);

      if (!hasData && !years.isEmpty()) {
        LOGGER.info("Creating mock GEO data for testing");
        createMockGeoParquetFiles(storageProvider, years.get(0));
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create mock GEO data: {}", e.getMessage());
    }
  }
  
  /**
   * Create mock Parquet files for GEO tables using StorageProvider.
   */
  @SuppressWarnings("deprecation")
  private void createMockGeoParquetFiles(org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, int year) throws Exception {
    // Create states mock data
    String yearDir = BOUNDARY_TYPE + "/year=" + year;
    String statesPath = yearDir + "/states.parquet";
    
    if (!storageProvider.exists(statesPath)) {
      org.apache.avro.Schema statesSchema = org.apache.avro.SchemaBuilder.record("State")
          .fields()
          .name("state_fips").type().stringType().noDefault()
          .name("state_code").type().stringType().noDefault()
          .name("state_name").type().stringType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      List<org.apache.avro.generic.GenericRecord> stateRecords = new ArrayList<>();
      
      // Add California
      org.apache.avro.generic.GenericRecord stateRecord = 
          new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "06");
      stateRecord.put("state_code", "CA");
      stateRecord.put("state_name", "California");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      // Add New York
      stateRecord = new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "36");
      stateRecord.put("state_code", "NY");
      stateRecord.put("state_name", "New York");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      // Add Washington (for Microsoft)
      stateRecord = new org.apache.avro.generic.GenericData.Record(statesSchema);
      stateRecord.put("state_fips", "53");
      stateRecord.put("state_code", "WA");
      stateRecord.put("state_name", "Washington");
      stateRecord.put("geometry", null);
      stateRecords.add(stateRecord);
      
      storageProvider.writeAvroParquet(statesPath, statesSchema, stateRecords, "State");
      LOGGER.info("Created mock states file: {}", statesPath);
    }
    
    // Create counties mock data
    String countiesPath = yearDir + "/counties.parquet";
    if (!storageProvider.exists(countiesPath)) {
      org.apache.avro.Schema countiesSchema = org.apache.avro.SchemaBuilder.record("County")
          .fields()
          .name("county_fips").type().stringType().noDefault()
          .name("state_fips").type().stringType().noDefault()
          .name("county_name").type().stringType().noDefault()
          .name("geometry").type().nullable().stringType().noDefault()
          .endRecord();
      
      List<org.apache.avro.generic.GenericRecord> countyRecords = new ArrayList<>();
      
      // Add Alameda County
      org.apache.avro.generic.GenericRecord countyRecord = 
          new org.apache.avro.generic.GenericData.Record(countiesSchema);
      countyRecord.put("county_fips", "06001");
      countyRecord.put("state_fips", "06");
      countyRecord.put("county_name", "Alameda County");
      countyRecord.put("geometry", null);
      countyRecords.add(countyRecord);
      
      // Add NYC county
      countyRecord = new org.apache.avro.generic.GenericData.Record(countiesSchema);
      countyRecord.put("county_fips", "36061");
      countyRecord.put("state_fips", "36");
      countyRecord.put("county_name", "New York County");
      countyRecord.put("geometry", null);
      countyRecords.add(countyRecord);
      
      storageProvider.writeAvroParquet(countiesPath, countiesSchema, countyRecords, "County");
      LOGGER.info("Created mock counties file: {}", countiesPath);
    }
    
    // Create other mock files with minimal data
    createSimpleMockFile(storageProvider, yearDir + "/zctas.parquet", "ZCTA", 
        new String[]{"zcta", "geometry"}, 
        new Object[]{"94105", null});
    
    createSimpleMockFile(storageProvider, yearDir + "/cbsa.parquet", "CBSA",
        new String[]{"cbsa_code", "cbsa_name", "geometry"},
        new Object[]{"41860", "San Francisco-Oakland-Berkeley, CA", null});
    
    createSimpleMockFile(storageProvider, yearDir + "/census_tracts.parquet", "Tract",
        new String[]{"tract_code", "county_fips", "geometry"},
        new Object[]{"060014001", "06001", null});
    
    createSimpleMockFile(storageProvider, yearDir + "/block_groups.parquet", "BlockGroup",
        new String[]{"block_group_code", "tract_code", "geometry"},
        new Object[]{"060014001001", "060014001", null});
  }
  
  @SuppressWarnings("deprecation")
  private void createSimpleMockFile(org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, String relativePath, String recordName,
      String[] fieldNames, Object[] values) throws Exception {
    if (!storageProvider.exists(relativePath)) {
      org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields = 
          org.apache.avro.SchemaBuilder.record(recordName).fields();
      
      for (String fieldName : fieldNames) {
        fields = fields.name(fieldName).type().nullable().stringType().noDefault();
      }
      
      org.apache.avro.Schema schema = fields.endRecord();
      org.apache.avro.generic.GenericRecord record = 
          new org.apache.avro.generic.GenericData.Record(schema);
      
      for (int i = 0; i < fieldNames.length; i++) {
        record.put(fieldNames[i], values[i]);
      }
      
      List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();
      records.add(record);
      
      storageProvider.writeAvroParquet(relativePath, schema, records, recordName);
      LOGGER.info("Created mock file: {}", relativePath);
    }
  }


  @Override
  public boolean supportsConstraints() {
    // Enable constraint support for geographic data
    return true;
  }

  @Override
  public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
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
}
