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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client for accessing U.S. Census Bureau APIs.
 *
 * <p>Provides access to:
 * <ul>
 *   <li>American Community Survey (ACS) demographic data</li>
 *   <li>Decennial Census data</li>
 *   <li>Economic indicators</li>
 *   <li>Geocoding services</li>
 * </ul>
 *
 * <p>The Census API is free but requires registration for an API key at
 * https://api.census.gov/data/key_signup.html
 *
 * <p>Rate limits: 500 requests per IP address per day (very generous)
 */
public class CensusApiClient extends AbstractGeoDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusApiClient.class);

  private static final String BASE_URL = "https://api.census.gov/data";
  private static final String GEOCODING_URL = "https://geocoding.geo.census.gov/geocoder";

  // Rate limiting: Census allows 500/day, we'll be conservative
  private static final int MAX_REQUESTS_PER_SECOND = 2;
  private static final long RATE_LIMIT_DELAY_MS = 500; // 2 requests per second

  private final String apiKey;
  private final String cacheDir;
  private final List<Integer> censusYears;
  private final ObjectMapper objectMapper;
  private final Semaphore rateLimiter;
  private final AtomicLong lastRequestTime;

  public CensusApiClient(String apiKey, String cacheDir) {
    this(apiKey, cacheDir, new ArrayList<>(), null, null);
  }

  public CensusApiClient(String apiKey, String cacheDir, List<Integer> censusYears) {
    this(apiKey, cacheDir, censusYears, null, null);
  }

  public CensusApiClient(String apiKey, String cacheDir, List<Integer> censusYears,
      StorageProvider storageProvider) {
    this(apiKey, cacheDir, censusYears, storageProvider, null);
  }

  public CensusApiClient(String apiKey, String cacheDir, List<Integer> censusYears,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(apiKey, cacheDir, cacheDir, censusYears, storageProvider, cacheManifest);
  }

  public CensusApiClient(String apiKey, String cacheDir, String operatingDirectory, List<Integer> censusYears,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    super(cacheDir, operatingDirectory, cacheDir, storageProvider, storageProvider, cacheManifest);
    this.apiKey = apiKey;
    this.cacheDir = cacheDir;
    this.censusYears = censusYears;
    this.objectMapper = new ObjectMapper();
    this.rateLimiter = new Semaphore(MAX_REQUESTS_PER_SECOND);
    this.lastRequestTime = new AtomicLong(0);

    LOGGER.info("Census API client initialized with cache directory: {}", cacheDir);
  }

  @Override protected String getTableName() {
    return "acs_population"; // Default table name for Census ACS data
  }

  @Override protected long getMinRequestIntervalMs() {
    return RATE_LIMIT_DELAY_MS; // Census API: 500ms between requests (2 per second)
  }

  @Override protected int getMaxRetries() {
    return 3; // Retry up to 3 times
  }

  @Override protected long getRetryDelayMs() {
    return 1000; // Initial retry delay: 1 second
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public CensusApiClient(String apiKey, File cacheDir) {
    this(apiKey, cacheDir.getAbsolutePath(), new ArrayList<>(), null, null);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public CensusApiClient(String apiKey, File cacheDir, List<Integer> censusYears) {
    this(apiKey, cacheDir.getAbsolutePath(), censusYears, null, null);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public CensusApiClient(String apiKey, File cacheDir, List<Integer> censusYears,
      StorageProvider storageProvider) {
    this(apiKey, cacheDir.getAbsolutePath(), censusYears, storageProvider, null);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public CensusApiClient(String apiKey, File cacheDir, List<Integer> censusYears,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(apiKey, cacheDir.getAbsolutePath(), cacheDir.getAbsolutePath(), censusYears, storageProvider, cacheManifest);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public CensusApiClient(String apiKey, File cacheDir, String operatingDirectory, List<Integer> censusYears,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(apiKey, cacheDir.getAbsolutePath(), operatingDirectory, censusYears, storageProvider, cacheManifest);
  }

  /**
   * Download all Census data for the specified year range (matching ECON pattern).
   */
  @Override public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading all Census data for years {} to {}", startYear, endYear);

    // Download demographic data for each year
    for (int year = startYear; year <= endYear; year++) {
      // Note: Year-specific cache directories will be created automatically
      // when files are written via StorageProvider

      try {
        // Download population demographics
        downloadPopulationDemographics(year);

        // Download housing characteristics
        downloadHousingCharacteristics(year);

        // Download economic indicators
        downloadEconomicIndicators(year);
      } catch (Exception e) {
        LOGGER.error("Error downloading Census data for year {}", year, e);
      }
    }

    LOGGER.info("Census data download completed for years {} to {}", startYear, endYear);
  }

  /**
   * Convert all downloaded Census JSON files to Parquet format using DuckDB.
   * This replaces the custom Avro-based conversion with metadata-driven DuckDB conversion.
   */
  @Override public void convertAll(int startYear, int endYear) {
    LOGGER.info("Converting all Census JSON to Parquet for years {} to {}", startYear, endYear);

    // Define Census data tables to convert
    String[] censusDataTypes = {"population_demographics", "housing_characteristics", "economic_indicators"};
    List<String> dataTypes = java.util.Arrays.asList(censusDataTypes);

    // Use optimized iteration with DuckDB cache filtering (10-20x faster)
    // Note: For conversion operations, the optimized version checks cacheManifest.isParquetConverted()
    iterateTableOperationsOptimized(
        getTableName(),
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "data_type": return dataTypes;
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Convert JSON to Parquet using DuckDB
          String dataType = vars.get("data_type");
          convertCensusDataToParquet(dataType, year, vars);

          if (cacheManifest != null) {
            cacheManifest.markParquetConverted(cacheKey, parquetPath);
            cacheManifest.save(operatingDirectory);
          }
        },
        "conversion");

    LOGGER.info("Census JSON to Parquet conversion completed for years {} to {}", startYear, endYear);
  }

  /**
   * Convert Census JSON data to Parquet using DuckDB.
   * Replaces the custom Avro-based conversion methods.
   * <p>
   * Census API returns JSON arrays in format:
   * [["NAME", "VAR1", "VAR2", "state"], ["California", "123", "456", "06"], ...]
   * Row 0 is header, rows 1+ are data, last column is geo identifier.
   */
  private void convertCensusDataToParquet(String dataType, int year, Map<String, String> vars) {
    try {
      // Build JSON source paths
      String jsonBasePath = cacheStorageProvider.resolvePath(cacheDirectory, "year=" + year);
      List<String> jsonFiles = new ArrayList<>();

      // Add state-level file
      String stateFile = cacheStorageProvider.resolvePath(jsonBasePath, dataType + "_states.json");
      if (cacheStorageProvider.exists(stateFile)) {
        jsonFiles.add(stateFile);
      }

      // Add county-level files for major states
      String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
      for (String state : stateFips) {
        String countyFile =
            cacheStorageProvider.resolvePath(jsonBasePath, dataType + "_county_" + state + ".json");
        if (cacheStorageProvider.exists(countyFile)) {
          jsonFiles.add(countyFile);
        }
      }

      if (jsonFiles.isEmpty()) {
        LOGGER.warn("No JSON files found for Census data type '{}', year {}", dataType, year);
        return;
      }

      // Build parquet target path
      String parquetPath =
          storageProvider.resolvePath(parquetDirectory, "type=acs/year=" + year + "/" + dataType + ".parquet");

      // Load column definitions from census-schema.json
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          loadCensusTableColumns(dataType, year);

      // Convert each JSON file separately, then combine
      // For now, convert first file only (state-level data) as MVP
      // TODO: Union multiple files after verifying schema consistency
      if (!jsonFiles.isEmpty()) {
        String firstJsonFile = jsonFiles.get(0);
        LOGGER.info("Converting Census {} data for year {} from {} to Parquet",
            dataType, year, firstJsonFile);

        convertCachedJsonToParquetViaDuckDB(
            dataType,
            columns,
            "null",  // Missing value indicator
            firstJsonFile,
            parquetPath);

        LOGGER.info("Successfully converted Census {} data for year {} to Parquet", dataType, year);
      }

    } catch (Exception e) {
      LOGGER.error("Failed to convert Census {} data for year {}: {}", dataType, year, e.getMessage(), e);
      throw new RuntimeException("Census parquet conversion failed for " + dataType, e);
    }
  }

  /**
   * Loads column metadata for a Census table from census-schema.json.
   *
   * @param tableName The name of the table to load column metadata for
   * @param year The year to substitute in expressions
   * @return List of table column definitions with expressions resolved
   * @throws IllegalArgumentException if the table is not found or schema is invalid
   */
  private static List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadCensusTableColumns(String tableName, int year) {
    try {
      // Load census-schema.json from resources
      java.io.InputStream schemaStream =
          CensusApiClient.class.getResourceAsStream("/census-schema.json");
      if (schemaStream == null) {
        throw new IllegalArgumentException(
            "census-schema.json not found in resources");
      }

      // Parse JSON
      com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(schemaStream);

      // Find the table in the "partitionedTables" array
      if (!root.has("partitionedTables") || !root.get("partitionedTables").isArray()) {
        throw new IllegalArgumentException(
            "Invalid census-schema.json: missing 'partitionedTables' array");
      }

      for (com.fasterxml.jackson.databind.JsonNode tableNode : root.get("partitionedTables")) {
        String name = tableNode.has("name") ? tableNode.get("name").asText() : null;
        if (tableName.equals(name)) {
          // Check if table uses conceptual mapping (indicated by presence of conceptualMappingFile)
          String conceptualMappingFile = tableNode.has("conceptualMappingFile")
              ? tableNode.get("conceptualMappingFile").asText() : null;

          if (conceptualMappingFile != null && !conceptualMappingFile.isEmpty()) {
            // Generate columns dynamically from conceptual mapper
            return loadColumnsFromConceptualMapper(tableName, year, tableNode, conceptualMappingFile);
          } else {
            // Use static columns from schema
            if (!tableNode.has("columns") || !tableNode.get("columns").isArray()) {
              throw new IllegalArgumentException(
                  "Table '" + tableName + "' has no 'columns' array in census-schema.json");
            }

            List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
                columns = new ArrayList<>();

            for (com.fasterxml.jackson.databind.JsonNode colNode : tableNode.get("columns")) {
              String colName = colNode.has("name") ? colNode.get("name").asText() : null;
              String colType = colNode.has("type") ? colNode.get("type").asText() : "string";
              boolean nullable = colNode.has("nullable") && colNode.get("nullable").asBoolean();
              String comment = colNode.has("comment") ? colNode.get("comment").asText() : "";
              String expression = colNode.has("expression") ? colNode.get("expression").asText() : null;

              // Substitute {year} placeholder in expression
              if (expression != null) {
                expression = expression.replace("{year}", String.valueOf(year));
              }

              if (colName != null) {
                columns.add(
                    new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
                    colName, colType, nullable, comment, expression));
              }
            }

            return columns;
          }
        }
      }

      // Table not found
      throw new IllegalArgumentException(
          "Table '" + tableName + "' not found in census-schema.json");

    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Failed to load column metadata for table '" + tableName + "': " + e.getMessage(), e);
    }
  }

  /**
   * Load columns dynamically from conceptual mapper based on year and census type.
   *
   * @param tableName Name of the table
   * @param year Year for variable resolution
   * @param tableNode JSON node containing table metadata
   * @param conceptualMappingFile Path to the conceptual mapping file (e.g., "census-variable-mappings.json")
   */
  private static List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn>
      loadColumnsFromConceptualMapper(String tableName, int year,
          com.fasterxml.jackson.databind.JsonNode tableNode, String conceptualMappingFile) {
    // Load the specified mapping file into the ConceptualVariableMapper
    org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper mapper =
        org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getInstance();
    mapper.loadMappingConfig(conceptualMappingFile);

    // Determine census type from table pattern
    String pattern = tableNode.has("pattern") ? tableNode.get("pattern").asText() : "";
    String censusType = determineCensusType(pattern);

    // Use ConceptualMapper interface to get variable mappings
    java.util.Map<String, Object> dimensions = new java.util.HashMap<>();
    dimensions.put("year", year);
    dimensions.put("censusType", censusType);

    java.util.Map<String, org.apache.calcite.adapter.govdata.AbstractConceptualMapper.VariableMapping> mappings =
        mapper.getVariablesForTable(tableName, dimensions);

    List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        new ArrayList<>();

    // Add geo_id column (always last in Census API response)
    columns.add(
        new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
        "geo_id", "string", false, "Geographic identifier",
        "list_element(data, array_length(data))"));

    // Add columns for each variable in order
    int position = 2;  // Census API response format: [NAME, var1, var2, ..., varN, GEO_ID]
    for (java.util.Map.Entry<String, org.apache.calcite.adapter.govdata.AbstractConceptualMapper.VariableMapping> entry : mappings.entrySet()) {
      String variable = entry.getKey();
      org.apache.calcite.adapter.govdata.AbstractConceptualMapper.VariableMapping mapping = entry.getValue();

      String conceptualName = mapping.getConceptualName();
      String dataType = mapping.getDataType().toLowerCase();
      String sqlType = dataType.contains("int") || dataType.contains("long") ? "integer" :
          dataType.contains("double") || dataType.contains("float") ? "double" : "string";

      String expression =
          String.format("CAST(list_element(data, %d) AS %s)", position, sqlType.toUpperCase());
      String comment = String.format("%s (%s)", conceptualName.replace("_", " "), variable);

      columns.add(
          new org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn(
          conceptualName, sqlType, true, comment, expression));

      position++;
    }

    LOGGER.info("Generated {} columns for table {} (year {}, type {})",
        columns.size(), tableName, year, censusType);

    return columns;
  }

  /**
   * Determine census type from table pattern.
   */
  private static String determineCensusType(String pattern) {
    if (pattern.contains("type=acs/")) {
      return "acs";
    } else if (pattern.contains("type=decennial/")) {
      return "decennial";
    } else if (pattern.contains("type=economic/")) {
      return "economic";
    } else if (pattern.contains("type=population/")) {
      return "population";
    }
    return "acs";  // Default
  }

  /**
   * Download population demographics for a specific year.
   */
  private void downloadPopulationDemographics(int year) throws IOException {
    // Use GeoConceptualMapper to get variables dynamically based on year
    GeoConceptualMapper mapper = GeoConceptualMapper.getInstance();
    Map<String, Object> dimensions = new HashMap<>();
    dimensions.put("year", year);
    dimensions.put("dataSource", "census_api");
    dimensions.put("censusType", "acs");

    String[] variableArray = mapper.getVariablesToDownload("population_demographics", dimensions);
    String variables = String.join(",", variableArray);

    // Download state-level data
    JsonNode stateData = getAcsData(year, variables, "state:*");
    saveJsonToYearCache(year, "population_demographics_states.json", stateData);

    // Download county-level data for selected states
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      JsonNode countyData = getAcsData(year, variables, "county:*&in=state:" + state);
      saveJsonToYearCache(year, "population_demographics_county_" + state + ".json", countyData);
    }
  }

  /**
   * Download housing characteristics for a specific year.
   */
  private void downloadHousingCharacteristics(int year) throws IOException {
    // Use GeoConceptualMapper to get variables dynamically based on year
    GeoConceptualMapper mapper = GeoConceptualMapper.getInstance();
    Map<String, Object> dimensions = new HashMap<>();
    dimensions.put("year", year);
    dimensions.put("dataSource", "census_api");
    dimensions.put("censusType", "acs");

    String[] variableArray = mapper.getVariablesToDownload("housing_characteristics", dimensions);
    String variables = String.join(",", variableArray);

    // Download state-level data
    JsonNode stateData = getAcsData(year, variables, "state:*");
    saveJsonToYearCache(year, "housing_characteristics_states.json", stateData);

    // Download county-level data for selected states
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      JsonNode countyData = getAcsData(year, variables, "county:*&in=state:" + state);
      saveJsonToYearCache(year, "housing_characteristics_county_" + state + ".json", countyData);
    }
  }

  /**
   * Download economic indicators for a specific year.
   */
  private void downloadEconomicIndicators(int year) {
    // Skip economic indicators for 2010 and earlier - many ACS variables not available
    if (year <= 2010) {
      LOGGER.debug("Skipping economic indicators for year {} - ACS variables not fully available", year);
      return;
    }

    // Use GeoConceptualMapper to get variables dynamically based on year
    GeoConceptualMapper mapper = GeoConceptualMapper.getInstance();
    Map<String, Object> dimensions = new HashMap<>();
    dimensions.put("year", year);
    dimensions.put("dataSource", "census_api");
    dimensions.put("censusType", "acs");

    String[] variableArray = mapper.getVariablesToDownload("economic_indicators", dimensions);
    String variables = String.join(",", variableArray);

    try {
      // Download state-level data
      JsonNode stateData = getAcsData(year, variables, "state:*");
      saveJsonToYearCache(year, "economic_indicators_states.json", stateData);

      // Download county-level data for selected states
      String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
      for (String state : stateFips) {
        JsonNode countyData = getAcsData(year, variables, "county:*&in=state:" + state);
        saveJsonToYearCache(year, "economic_indicators_county_" + state + ".json", countyData);
      }
    } catch (IOException e) {
      LOGGER.warn("Economic indicators not available for year {} - skipping: {}", year, e.getMessage());
      // Continue processing - some years may not have all variables
    }
  }

  /**
   * Save JSON data to year-specific cache directory.
   */
  private void saveJsonToYearCache(int year, String filename, JsonNode data) throws IOException {
    String yearPath = "year=" + year;
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    String filePath = storageProvider.resolvePath(cachePath, filename);

    byte[] jsonBytes = objectMapper.writeValueAsBytes(data);
    storageProvider.writeFile(filePath, jsonBytes);
    LOGGER.debug("Saved Census data to {}", filePath);
  }

  /**
   * Read JSON from storage provider.
   */
  private JsonNode readJsonFromStorage(String filePath) throws IOException {
    try (java.io.InputStream in = storageProvider.openInputStream(filePath)) {
      return objectMapper.readTree(in);
    }
  }

  /**
   * Get demographic data from American Community Survey (ACS).
   *
   * @param year Year of data (e.g., 2022 for 2018-2022 5-year estimates)
   * @param variables Comma-separated list of variables (e.g., "B01001_001E,B19013_001E")
   * @param geography Geographic level (e.g., "state:*", "county:*", "tract:*")
   * @return JSON response from Census API
   */
  public JsonNode getAcsData(int year, String variables, String geography) throws IOException {
    String cacheKey =
        String.format("acs_%d_%s_%s", year, variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));

    // Check manifest first to avoid S3 operations
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (cacheManifest != null) {
      Map<String, String> params = new HashMap<>();
      params.put("variables", variables);
      params.put("geography", geography);
      params.put("year", String.valueOf(year));
      org.apache.calcite.adapter.govdata.CacheKey manifestKey =
          new org.apache.calcite.adapter.govdata.CacheKey(cacheKey, params);
      if (cacheManifest.isCached(manifestKey)) {
        LOGGER.debug("Using cached ACS data per manifest: {}", cacheFilePath);
        return readJsonFromStorage(cacheFilePath);
      }
    }

    // Build API URL
    // Note: Do not URL-encode geography parameter as it contains & characters that Census API expects
    String url =
        String.format("%s/%d/acs/acs5?get=%s&for=%s&key=%s", BASE_URL, year, variables, geography, apiKey);

    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);

    // Cache the response
    byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
    storageProvider.writeFile(cacheFilePath, jsonBytes);
    LOGGER.info("Cached ACS data to {}", cacheFilePath);

    // Mark in manifest
    if (cacheManifest != null) {
      Map<String, String> params = new HashMap<>();
      params.put("variables", variables);
      params.put("geography", geography);
      params.put("year", String.valueOf(year));
      org.apache.calcite.adapter.govdata.CacheKey manifestKey =
          new org.apache.calcite.adapter.govdata.CacheKey(cacheKey, params);
      cacheManifest.markCached(manifestKey, cacheFilePath, jsonBytes.length, Long.MAX_VALUE, "census_immutable");
      cacheManifest.save(operatingDirectory);
    }

    return response;
  }

  /**
   * Get data from Decennial Census.
   *
   * @param year Census year (2010 or 2020)
   * @param variables Variables to retrieve
   * @param geography Geographic level
   * @return JSON response from Census API
   */
  public JsonNode getDecennialData(int year, String variables, String geography) throws IOException {
    return getDecennialData(year, variables, geography, null);
  }

  /**
   * Get decennial census data with specific dataset.
   *
   * @param year Census year
   * @param variables Comma-separated list of variables
   * @param geography Geographic filter
   * @param preferredDataset Preferred dataset (null for auto-selection)
   * @return JSON response from Census API
   * @throws IOException if API call fails
   */
  public JsonNode getDecennialData(int year, String variables, String geography, String preferredDataset) throws IOException {
    String cacheKey =
        String.format("decennial_%d_%s_%s_%s", year, variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"),
        preferredDataset != null ? preferredDataset : "auto");

    // Check cache first
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (storageProvider.exists(cacheFilePath)) {
      LOGGER.debug("Using cached Decennial data from {}", cacheFilePath);
      return readJsonFromStorage(cacheFilePath);
    }

    // Determine datasets to try
    String[] datasetsToTry;
    if (preferredDataset != null) {
      datasetsToTry = new String[]{preferredDataset};
    } else {
      // Use ConceptualVariableMapper to get appropriate datasets
      String primaryDataset = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getDataset("decennial", year);
      String[] fallbacks = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getFallbackDatasets("decennial", year);

      datasetsToTry = new String[1 + fallbacks.length];
      datasetsToTry[0] = primaryDataset;
      System.arraycopy(fallbacks, 0, datasetsToTry, 1, fallbacks.length);
    }

    // Try each dataset until one works
    IOException lastException = null;
    for (String dataset : datasetsToTry) {
      try {
        String url =
            String.format("%s/%d/dec/%s?get=%s&for=%s&key=%s", BASE_URL, year, dataset, variables, geography, apiKey);

        LOGGER.debug("Trying decennial API call with dataset '{}': {}", dataset, url);

        // Make API request with rate limiting
        JsonNode response = makeApiRequest(url);

        // Cache the successful response
        byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
        storageProvider.writeFile(cacheFilePath, jsonBytes);
        LOGGER.info("Cached Decennial data to {} using dataset '{}'", cacheFilePath, dataset);

        return response;

      } catch (IOException e) {
        lastException = e;
        LOGGER.debug("Dataset '{}' failed for year {}: {}", dataset, year, e.getMessage());
      }
    }

    // All datasets failed
    throw new IOException(
        String.format("All datasets failed for decennial year %d with variables %s: %s",
        year, variables, lastException != null ? lastException.getMessage() : "unknown error"));
  }

  /**
   * Get economic census/CBP data.
   *
   * @param year Census year
   * @param variables Comma-separated list of variables
   * @param geography Geographic filter
   * @return JSON response from Census API
   * @throws IOException if API call fails
   */
  public JsonNode getEconomicData(int year, String variables, String geography) throws IOException {
    String cacheKey =
        String.format("economic_%d_%s_%s", year, variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));

    // Check cache first
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (storageProvider.exists(cacheFilePath)) {
      LOGGER.debug("Using cached Economic data from {}", cacheFilePath);
      return readJsonFromStorage(cacheFilePath);
    }

    // Determine dataset to use
    String dataset = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getDataset("economic", year);
    String[] fallbacks = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getFallbackDatasets("economic", year);

    // Try primary dataset and fallbacks
    IOException lastException = null;
    String[] datasetsToTry = new String[1 + fallbacks.length];
    datasetsToTry[0] = dataset;
    System.arraycopy(fallbacks, 0, datasetsToTry, 1, fallbacks.length);

    for (String ds : datasetsToTry) {
      try {
        String url =
            String.format("%s/%d/%s?get=%s&for=%s&key=%s", BASE_URL, year, ds, variables, geography, apiKey);

        LOGGER.debug("Trying economic API call with dataset '{}': {}", ds, url);

        // Make API request with rate limiting
        JsonNode response = makeApiRequest(url);

        // Cache the successful response
        byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
        storageProvider.writeFile(cacheFilePath, jsonBytes);
        LOGGER.info("Cached Economic data to {} using dataset '{}'", cacheFilePath, ds);

        return response;

      } catch (IOException e) {
        lastException = e;
        LOGGER.debug("Dataset '{}' failed for year {}: {}", ds, year, e.getMessage());
      }
    }

    // All datasets failed
    throw new IOException(
        String.format("All datasets failed for economic year %d with variables %s: %s",
        year, variables, lastException != null ? lastException.getMessage() : "unknown error"));
  }

  /**
   * Get population estimates data.
   *
   * @param year Census year
   * @param variables Comma-separated list of variables
   * @param geography Geographic filter
   * @return JSON response from Census API
   * @throws IOException if API call fails
   */
  public JsonNode getPopulationEstimatesData(int year, String variables, String geography) throws IOException {
    String cacheKey =
        String.format("population_%d_%s_%s", year, variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));

    // Check cache first
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (storageProvider.exists(cacheFilePath)) {
      LOGGER.debug("Using cached Population Estimates data from {}", cacheFilePath);
      return readJsonFromStorage(cacheFilePath);
    }

    // Determine dataset to use
    String dataset = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getDataset("population", year);
    String[] fallbacks = org.apache.calcite.adapter.govdata.census.ConceptualVariableMapper.getFallbackDatasets("population", year);

    // Try primary dataset and fallbacks
    IOException lastException = null;
    String[] datasetsToTry = new String[1 + fallbacks.length];
    datasetsToTry[0] = dataset;
    System.arraycopy(fallbacks, 0, datasetsToTry, 1, fallbacks.length);

    for (String ds : datasetsToTry) {
      try {
        String url;
        String varsToUse = variables;

        // Check if this is an ACS dataset (contains "acs")
        if (ds.contains("acs")) {
          // Use ACS endpoint and remap variables
          varsToUse = remapVariablesForAcs(variables, year);
          url = String.format("%s/%d/%s?get=%s&for=%s&key=%s", BASE_URL, year, ds, varsToUse, geography, apiKey);
          LOGGER.debug("Trying ACS fallback with dataset '{}': {}", ds, url);
        } else {
          // Use PEP endpoint
          url = String.format("%s/%d/pep/%s?get=%s&for=%s&key=%s", BASE_URL, year, ds, varsToUse, geography, apiKey);
          LOGGER.debug("Trying population API call with dataset '{}': {}", ds, url);
        }

        // Make API request with rate limiting
        JsonNode response = makeApiRequest(url);

        // Cache the successful response
        byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
        storageProvider.writeFile(cacheFilePath, jsonBytes);
        LOGGER.info("Cached Population Estimates data to {} using dataset '{}'", cacheFilePath, ds);

        return response;

      } catch (IOException e) {
        lastException = e;
        LOGGER.debug("Dataset '{}' failed for year {}: {}", ds, year, e.getMessage());
      }
    }

    // All datasets failed
    throw new IOException(
        String.format("All datasets failed for population year %d with variables %s: %s",
        year, variables, lastException != null ? lastException.getMessage() : "unknown error"));
  }

  /**
   * Get voting and registration data from Current Population Survey (CPS) Voting Supplement.
   * Returns microdata for eligible voters (18+) with registration and voting behavior.
   *
   * <p>Important: CPS Voting Supplement is conducted biennially (every 2 years) in
   * November election years only: 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024, etc.
   *
   * <p>Note: Census does NOT collect party affiliation data. Data includes only
   * registration status, voting status, demographics, and vote method.
   *
   * @param year Election year (must be even: 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024)
   * @param variables Comma-separated list of variables (e.g., "PES1,PES2,GESTFIPS,PRTAGE")
   *                  PES1 = Voted (yes/no), PES2 = Registration status
   * @return JSON response from Census CPS Voting API
   * @throws IOException if API call fails or year is invalid
   */
  public JsonNode getCpsVotingData(int year, String variables) throws IOException {
    // Validate that year is even (CPS Voting only conducted in election years)
    if (year % 2 != 0) {
      throw new IllegalArgumentException(
          "CPS Voting Supplement is only available for even years (election years): " + year);
    }

    // Validate year range (CPS Voting Supplement started in 1964)
    if (year < 1964 || year > 2024) {
      throw new IllegalArgumentException(
          "CPS Voting Supplement year must be between 1964 and 2024: " + year);
    }

    String cacheKey =
        String.format("cps_voting_%d_%s", year, variables.replaceAll("[^a-zA-Z0-9]", "_"));

    // Check cache first
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (storageProvider.exists(cacheFilePath)) {
      LOGGER.debug("Using cached CPS Voting data from {}", cacheFilePath);
      return readJsonFromStorage(cacheFilePath);
    }

    // Build API URL for CPS Voting microdata
    // Pattern: /data/{YEAR}/cps/voting/nov?get={variables}&key={apiKey}
    // Note: CPS microdata does not use geography filters - each record contains state info
    String url =
        String.format("%s/%d/cps/voting/nov?get=%s&key=%s", BASE_URL, year, variables, apiKey);

    LOGGER.info("Fetching CPS Voting data for year {} with variables: {}", year, variables);

    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);

    // Cache the successful response
    byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
    storageProvider.writeFile(cacheFilePath, jsonBytes);
    LOGGER.info("Cached CPS Voting data to {} ({} bytes)", cacheFilePath, jsonBytes.length);

    return response;
  }

  /**
   * Gets all U.S. county FIPS codes from the Census API.
   *
   * <p>Queries the most recent ACS 5-year estimates to retrieve a complete list of all
   * U.S. counties with their FIPS codes. Returns approximately 3,142 counties including
   * county-equivalents (parishes, boroughs, etc.).
   *
   * <p>The returned map contains:
   * <ul>
   *   <li>Key: 5-digit county FIPS code (state + county)</li>
   *   <li>Value: County name with state (e.g., "Alameda County, California")</li>
   * </ul>
   *
   * <p>Results are cached to avoid repeated API calls.
   *
   * @return Map of county FIPS codes to county names
   * @throws IOException if API call fails
   */
  public Map<String, String> getAllCountyFipsCodes() throws IOException {
    String cacheKey = "all_county_fips";

    // Check cache first
    String cacheFilePath = storageProvider.resolvePath(cacheDir, cacheKey + ".json");
    if (storageProvider.exists(cacheFilePath)) {
      LOGGER.debug("Using cached county FIPS codes from {}", cacheFilePath);
      JsonNode cachedData = readJsonFromStorage(cacheFilePath);
      return parseCountyFipsFromJson(cachedData);
    }

    // Query Census API for all counties
    // Using 2020 ACS 5-year estimates (most recent comprehensive dataset)
    // Pattern: /data/2020/acs/acs5?get=NAME&for=county:*&key={apiKey}
    String url = String.format("%s/2020/acs/acs5?get=NAME&for=county:*&key=%s", BASE_URL, apiKey);

    LOGGER.info("Fetching all county FIPS codes from Census API");

    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);

    // Cache the successful response
    byte[] jsonBytes = objectMapper.writeValueAsBytes(response);
    storageProvider.writeFile(cacheFilePath, jsonBytes);
    LOGGER.info("Cached county FIPS codes to {} ({} bytes)", cacheFilePath, jsonBytes.length);

    return parseCountyFipsFromJson(response);
  }

  /**
   * Parses county FIPS codes from Census API JSON response.
   *
   * <p>Expected format:
   * <pre>
   * [
   *   ["NAME", "state", "county"],  // Header row
   *   ["Autauga County, Alabama", "01", "001"],
   *   ["Baldwin County, Alabama", "01", "003"],
   *   ...
   * ]
   * </pre>
   *
   * @param response Census API JSON response
   * @return Map of 5-digit county FIPS codes to county names
   */
  private Map<String, String> parseCountyFipsFromJson(JsonNode response) {
    Map<String, String> countyFips = new HashMap<>();

    if (!response.isArray() || response.size() < 2) {
      LOGGER.warn("Invalid county FIPS response format");
      return countyFips;
    }

    // First row is headers, skip it
    for (int i = 1; i < response.size(); i++) {
      JsonNode row = response.get(i);
      if (!row.isArray() || row.size() < 3) {
        continue;
      }

      String countyName = row.get(0).asText();
      String stateFips = row.get(1).asText();
      String countyCode = row.get(2).asText();

      // Combine state + county to create 5-digit FIPS code
      String fipsCode = stateFips + countyCode;

      countyFips.put(fipsCode, countyName);
    }

    LOGGER.info("Parsed {} county FIPS codes from Census API", countyFips.size());
    return countyFips;
  }

  /**
   * Remap PEP variables to their ACS equivalents.
   *
   * @param pepVariables Comma-separated PEP variable names (e.g., "POP,POPEST")
   * @param year Year for context
   * @return Comma-separated ACS variable names
   */
  private String remapVariablesForAcs(String pepVariables, int year) {
    String[] vars = pepVariables.split(",");
    StringBuilder acsVars = new StringBuilder();

    for (int i = 0; i < vars.length; i++) {
      String var = vars[i].trim();
      String acsVar;

      // Map PEP variables to ACS equivalents
      if (var.equals("POP") || var.equals("POPEST") || var.startsWith("POP_")) {
        acsVar = "B01001_001E";  // Total population
      } else {
        // Unknown variable - keep as is and let API fail if invalid
        LOGGER.warn("Unknown PEP variable '{}' - cannot remap to ACS", var);
        acsVar = var;
      }

      if (i > 0) {
        acsVars.append(",");
      }
      acsVars.append(acsVar);
    }

    LOGGER.debug("Remapped PEP variables '{}' to ACS variables '{}'", pepVariables, acsVars.toString());
    return acsVars.toString();
  }

  /**
   * Geocode an address to get coordinates and Census geography.
   *
   * @param street Street address
   * @param city City name
   * @param state State abbreviation
   * @param zip ZIP code (optional)
   * @return Geocoding result with lat/lon and Census geography codes
   */
  public GeocodeResult geocodeAddress(String street, String city, String state, String zip)
      throws IOException {

    // Build geocoding URL
    StringBuilder urlBuilder = new StringBuilder(GEOCODING_URL);
    urlBuilder.append("/locations/onelineaddress?address=");

    // Construct address string
    String address = street + ", " + city + ", " + state;
    if (zip != null && !zip.isEmpty()) {
      address += " " + zip;
    }
    urlBuilder.append(URLEncoder.encode(address, "UTF-8"));

    urlBuilder.append("&benchmark=2020");
    urlBuilder.append("&format=json");

    // Make API request (geocoding doesn't require API key)
    JsonNode response = makeApiRequest(urlBuilder.toString());

    // Parse result
    JsonNode result = response.get("result");
    if (result != null && result.has("addressMatches") && result.get("addressMatches").size() > 0) {
      JsonNode match = result.get("addressMatches").get(0);
      JsonNode coords = match.get("coordinates");
      JsonNode geos = match.get("geographies");

      GeocodeResult geocodeResult = new GeocodeResult();
      geocodeResult.latitude = coords.get("y").asDouble();
      geocodeResult.longitude = coords.get("x").asDouble();

      // Extract Census geography codes
      if (geos != null && geos.has("Census Tracts")) {
        JsonNode tract = geos.get("Census Tracts").get(0);
        geocodeResult.stateFips = tract.get("STATE").asText();
        geocodeResult.countyFips = tract.get("COUNTY").asText();
        geocodeResult.tractCode = tract.get("TRACT").asText();
        geocodeResult.blockGroup = tract.get("BLKGRP").asText();
      }

      return geocodeResult;
    }

    return null; // No match found
  }

  /**
   * Make an API request with rate limiting.
   */
  private JsonNode makeApiRequest(String urlString) throws IOException {
    try {
      // Rate limiting
      rateLimiter.acquire();

      // Ensure minimum time between requests
      long now = System.currentTimeMillis();
      long timeSinceLastRequest = now - lastRequestTime.get();
      if (timeSinceLastRequest < RATE_LIMIT_DELAY_MS) {
        Thread.sleep(RATE_LIMIT_DELAY_MS - timeSinceLastRequest);
      }
      lastRequestTime.set(System.currentTimeMillis());

      // Make HTTP request (Java 8 compatible)
      URI uri = URI.create(urlString);
      URL url = uri.toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(30000);

      int responseCode = conn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        return objectMapper.readTree(conn.getInputStream());
      } else {
        throw new IOException("Census API request failed with code " + responseCode +
            " for URL: " + urlString);
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Rate limiting interrupted", e);
    } finally {
      rateLimiter.release();
    }
  }

  /**
   * Convert Census API JSON response to CSV for easier processing.
   */
  public void convertJsonToCsv(JsonNode jsonData, File outputFile) throws IOException {
    if (!jsonData.isArray() || jsonData.size() < 2) {
      throw new IllegalArgumentException("Invalid Census API response format");
    }

    try (FileWriter writer = new FileWriter(outputFile)) {
      // First row is headers
      JsonNode headers = jsonData.get(0);
      for (int i = 0; i < headers.size(); i++) {
        if (i > 0) writer.write(",");
        writer.write(headers.get(i).asText());
      }
      writer.write("\n");

      // Remaining rows are data
      for (int row = 1; row < jsonData.size(); row++) {
        JsonNode dataRow = jsonData.get(row);
        for (int i = 0; i < dataRow.size(); i++) {
          if (i > 0) writer.write(",");
          writer.write(dataRow.get(i).asText());
        }
        writer.write("\n");
      }
    }

    LOGGER.info("Converted Census API response to CSV: {}", outputFile);
  }

  /**
   * Result of geocoding an address.
   */
  public static class GeocodeResult {
    public double latitude;
    public double longitude;
    public String stateFips;
    public String countyFips;
    public String tractCode;
    public String blockGroup;

    @Override public String toString() {
      return String.format("GeocodeResult[lat=%.6f, lon=%.6f, state=%s, county=%s, tract=%s]",
          latitude, longitude, stateFips, countyFips, tractCode);
    }
  }

  /**
   * Common Census variables for reference.
   */
  public static class Variables {
    // Population
    public static final String TOTAL_POPULATION = "B01001_001E";
    public static final String MALE_POPULATION = "B01001_002E";
    public static final String FEMALE_POPULATION = "B01001_026E";

    // Income
    public static final String MEDIAN_HOUSEHOLD_INCOME = "B19013_001E";
    public static final String PER_CAPITA_INCOME = "B19301_001E";

    // Housing
    public static final String TOTAL_HOUSING_UNITS = "B25001_001E";
    public static final String OCCUPIED_HOUSING_UNITS = "B25002_002E";
    public static final String VACANT_HOUSING_UNITS = "B25002_003E";
    public static final String MEDIAN_HOME_VALUE = "B25077_001E";

    // Employment
    public static final String LABOR_FORCE = "B23025_002E";
    public static final String EMPLOYED = "B23025_004E";
    public static final String UNEMPLOYED = "B23025_005E";

    // Education
    public static final String HIGH_SCHOOL_GRADUATE = "B15003_017E";
    public static final String BACHELORS_DEGREE = "B15003_022E";
    public static final String GRADUATE_DEGREE = "B15003_024E";
  }

}
