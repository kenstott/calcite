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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
public class CensusApiClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusApiClient.class);
  
  private static final String BASE_URL = "https://api.census.gov/data";
  private static final String GEOCODING_URL = "https://geocoding.geo.census.gov/geocoder";
  
  // Rate limiting: Census allows 500/day, we'll be conservative
  private static final int MAX_REQUESTS_PER_SECOND = 2;
  private static final long RATE_LIMIT_DELAY_MS = 500; // 2 requests per second
  
  private final String apiKey;
  private final File cacheDir;
  private final List<Integer> censusYears;
  private final ObjectMapper objectMapper;
  private final Semaphore rateLimiter;
  private final AtomicLong lastRequestTime;
  private final StorageProvider storageProvider;
  
  public CensusApiClient(String apiKey, File cacheDir) {
    this(apiKey, cacheDir, new ArrayList<>(), null);
  }

  public CensusApiClient(String apiKey, File cacheDir, List<Integer> censusYears) {
    this(apiKey, cacheDir, censusYears, null);
  }

  public CensusApiClient(String apiKey, File cacheDir, List<Integer> censusYears,
      StorageProvider storageProvider) {
    this.apiKey = apiKey;
    this.cacheDir = cacheDir;
    this.censusYears = censusYears;
    this.objectMapper = new ObjectMapper();
    this.rateLimiter = new Semaphore(MAX_REQUESTS_PER_SECOND);
    this.lastRequestTime = new AtomicLong(0);
    this.storageProvider = storageProvider;

    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    LOGGER.info("Census API client initialized with cache directory: {}", cacheDir);
  }

  /**
   * Download all Census data for the specified year range (matching ECON pattern).
   */
  public void downloadAll(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading all Census data for years {} to {}", startYear, endYear);

    // Download demographic data for each year
    for (int year = startYear; year <= endYear; year++) {
      // Create year-specific cache directory
      File yearCacheDir = new File(cacheDir, "year=" + year);
      if (!yearCacheDir.exists()) {
        yearCacheDir.mkdirs();
      }

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
   * Download population demographics for a specific year.
   */
  private void downloadPopulationDemographics(int year) throws IOException {
    String variables = Variables.TOTAL_POPULATION + "," +
                      Variables.MALE_POPULATION + "," +
                      Variables.FEMALE_POPULATION;

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
    String variables = Variables.TOTAL_HOUSING_UNITS + "," +
                      Variables.OCCUPIED_HOUSING_UNITS + "," +
                      Variables.VACANT_HOUSING_UNITS + "," +
                      Variables.MEDIAN_HOME_VALUE;

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
  private void downloadEconomicIndicators(int year) throws IOException {
    String variables = Variables.MEDIAN_HOUSEHOLD_INCOME + "," +
                      Variables.PER_CAPITA_INCOME + "," +
                      Variables.LABOR_FORCE + "," +
                      Variables.EMPLOYED + "," +
                      Variables.UNEMPLOYED;

    // Download state-level data
    JsonNode stateData = getAcsData(year, variables, "state:*");
    saveJsonToYearCache(year, "economic_indicators_states.json", stateData);

    // Download county-level data for selected states
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      JsonNode countyData = getAcsData(year, variables, "county:*&in=state:" + state);
      saveJsonToYearCache(year, "economic_indicators_county_" + state + ".json", countyData);
    }
  }

  /**
   * Save JSON data to year-specific cache directory.
   */
  private void saveJsonToYearCache(int year, String filename, JsonNode data) throws IOException {
    File yearDir = new File(cacheDir, "year=" + year);
    if (!yearDir.exists()) {
      yearDir.mkdirs();
    }
    File jsonFile = new File(yearDir, filename);
    objectMapper.writeValue(jsonFile, data);
    LOGGER.debug("Saved Census data to {}", jsonFile);
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
    String cacheKey = String.format("acs_%d_%s_%s", year, 
        variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"));
    
    // Check cache first
    File cacheFile = new File(cacheDir, cacheKey + ".json");
    if (cacheFile.exists()) {
      LOGGER.debug("Using cached ACS data from {}", cacheFile);
      return objectMapper.readTree(cacheFile);
    }
    
    // Build API URL
    // Note: Do not URL-encode geography parameter as it contains & characters that Census API expects
    String url = String.format("%s/%d/acs/acs5?get=%s&for=%s&key=%s",
        BASE_URL, year, variables, geography, apiKey);
    
    // Make API request with rate limiting
    JsonNode response = makeApiRequest(url);
    
    // Cache the response
    objectMapper.writeValue(cacheFile, response);
    LOGGER.info("Cached ACS data to {}", cacheFile);
    
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
    String cacheKey = String.format("dec_%d_%s_%s_%s", year,
        variables.replaceAll("[^a-zA-Z0-9]", "_"),
        geography.replaceAll("[^a-zA-Z0-9]", "_"),
        preferredDataset != null ? preferredDataset : "auto");

    // Check cache first
    File cacheFile = new File(cacheDir, cacheKey + ".json");
    if (cacheFile.exists()) {
      LOGGER.debug("Using cached Decennial data from {}", cacheFile);
      return objectMapper.readTree(cacheFile);
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
        String url = String.format("%s/%d/dec/%s?get=%s&for=%s&key=%s",
            BASE_URL, year, dataset, variables, geography, apiKey);

        LOGGER.debug("Trying decennial API call with dataset '{}': {}", dataset, url);

        // Make API request with rate limiting
        JsonNode response = makeApiRequest(url);

        // Cache the successful response
        objectMapper.writeValue(cacheFile, response);
        LOGGER.info("Cached Decennial data to {} using dataset '{}'", cacheFile, dataset);

        return response;

      } catch (IOException e) {
        lastException = e;
        LOGGER.debug("Dataset '{}' failed for year {}: {}", dataset, year, e.getMessage());
      }
    }

    // All datasets failed
    throw new IOException(String.format("All datasets failed for decennial year %d with variables %s: %s",
        year, variables, lastException != null ? lastException.getMessage() : "unknown error"));
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

  /**
   * Convert Census JSON data to Parquet format (matching ECON pattern).
   */
  @SuppressWarnings("deprecation")
  public void convertToParquet(File sourceDir, String targetFilePath) throws IOException {
    LOGGER.info("Converting Census data from {} to parquet: {}", sourceDir, targetFilePath);

    // Skip if target file already exists
    if (storageProvider != null && storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }

    // Determine which type of data to convert based on the target file name
    String fileName = targetFilePath.substring(targetFilePath.lastIndexOf("/") + 1);

    if (fileName.contains("population_demographics")) {
      convertPopulationDemographicsToParquet(sourceDir, targetFilePath);
    } else if (fileName.contains("housing_characteristics")) {
      convertHousingCharacteristicsToParquet(sourceDir, targetFilePath);
    } else if (fileName.contains("economic_indicators")) {
      convertEconomicIndicatorsToParquet(sourceDir, targetFilePath);
    } else {
      LOGGER.warn("Unknown Census data type for conversion: {}", fileName);
    }
  }

  /**
   * Convert population demographics JSON to Parquet.
   */
  @SuppressWarnings("deprecation")
  private void convertPopulationDemographicsToParquet(File sourceDir, String targetFilePath)
      throws IOException {

    // Create Avro schema for population demographics
    Schema schema = SchemaBuilder.record("PopulationDemographics")
        .fields()
        .name("geo_id").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("total_population").type().nullable().longType().noDefault()
        .name("male_population").type().nullable().longType().noDefault()
        .name("female_population").type().nullable().longType().noDefault()
        .name("state_fips").type().nullable().stringType().noDefault()
        .name("county_fips").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract year from source directory
    String yearStr = sourceDir.getName().replace("year=", "");
    int year = Integer.parseInt(yearStr);

    // Read and convert state-level data
    File stateFile = new File(sourceDir, "population_demographics_states.json");
    if (stateFile.exists()) {
      JsonNode data = objectMapper.readTree(stateFile);
      for (int i = 1; i < data.size(); i++) { // Skip header row
        JsonNode row = data.get(i);
        GenericRecord record = new GenericData.Record(schema);
        record.put("geo_id", row.get(row.size() - 1).asText()); // Last column is usually state FIPS
        record.put("year", year);
        record.put("total_population", row.get(0).asLong(0L));
        record.put("male_population", row.get(1).asLong(0L));
        record.put("female_population", row.get(2).asLong(0L));
        record.put("state_fips", row.get(row.size() - 1).asText());
        record.put("county_fips", null);
        records.add(record);
      }
    }

    // Read and convert county-level data
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      File countyFile = new File(sourceDir, "population_demographics_county_" + state + ".json");
      if (countyFile.exists()) {
        JsonNode data = objectMapper.readTree(countyFile);
        for (int i = 1; i < data.size(); i++) { // Skip header row
          JsonNode row = data.get(i);
          GenericRecord record = new GenericData.Record(schema);
          String countyFips = row.get(row.size() - 1).asText(); // County FIPS
          record.put("geo_id", countyFips);
          record.put("year", year);
          record.put("total_population", row.get(0).asLong(0L));
          record.put("male_population", row.get(1).asLong(0L));
          record.put("female_population", row.get(2).asLong(0L));
          record.put("state_fips", state);
          record.put("county_fips", countyFips);
          records.add(record);
        }
      }
    }

    // Write to Parquet
    if (storageProvider != null && !records.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, schema, records, "PopulationDemographics");
      LOGGER.info("Created population demographics parquet: {} with {} records",
          targetFilePath, records.size());
    }
  }

  /**
   * Convert housing characteristics JSON to Parquet.
   */
  @SuppressWarnings("deprecation")
  private void convertHousingCharacteristicsToParquet(File sourceDir, String targetFilePath)
      throws IOException {

    // Create Avro schema for housing characteristics
    Schema schema = SchemaBuilder.record("HousingCharacteristics")
        .fields()
        .name("geo_id").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("total_housing_units").type().nullable().longType().noDefault()
        .name("occupied_units").type().nullable().longType().noDefault()
        .name("vacant_units").type().nullable().longType().noDefault()
        .name("median_home_value").type().nullable().doubleType().noDefault()
        .name("state_fips").type().nullable().stringType().noDefault()
        .name("county_fips").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract year from source directory
    String yearStr = sourceDir.getName().replace("year=", "");
    int year = Integer.parseInt(yearStr);

    // Read and convert state-level data
    File stateFile = new File(sourceDir, "housing_characteristics_states.json");
    if (stateFile.exists()) {
      JsonNode data = objectMapper.readTree(stateFile);
      for (int i = 1; i < data.size(); i++) { // Skip header row
        JsonNode row = data.get(i);
        GenericRecord record = new GenericData.Record(schema);
        record.put("geo_id", row.get(row.size() - 1).asText());
        record.put("year", year);
        record.put("total_housing_units", row.get(0).asLong(0L));
        record.put("occupied_units", row.get(1).asLong(0L));
        record.put("vacant_units", row.get(2).asLong(0L));
        record.put("median_home_value", row.get(3).asDouble(0.0));
        record.put("state_fips", row.get(row.size() - 1).asText());
        record.put("county_fips", null);
        records.add(record);
      }
    }

    // Read and convert county-level data
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      File countyFile = new File(sourceDir, "housing_characteristics_county_" + state + ".json");
      if (countyFile.exists()) {
        JsonNode data = objectMapper.readTree(countyFile);
        for (int i = 1; i < data.size(); i++) { // Skip header row
          JsonNode row = data.get(i);
          GenericRecord record = new GenericData.Record(schema);
          String countyFips = row.get(row.size() - 1).asText(); // County FIPS
          record.put("geo_id", countyFips);
          record.put("year", year);
          record.put("total_housing_units", row.get(0).asLong(0L));
          record.put("occupied_units", row.get(1).asLong(0L));
          record.put("vacant_units", row.get(2).asLong(0L));
          record.put("median_home_value", row.get(3).asDouble(0.0));
          record.put("state_fips", state);
          record.put("county_fips", countyFips);
          records.add(record);
        }
      }
    }

    // Write to Parquet
    if (storageProvider != null && !records.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, schema, records, "HousingCharacteristics");
      LOGGER.info("Created housing characteristics parquet: {} with {} records",
          targetFilePath, records.size());
    }
  }

  /**
   * Convert economic indicators JSON to Parquet.
   */
  @SuppressWarnings("deprecation")
  private void convertEconomicIndicatorsToParquet(File sourceDir, String targetFilePath)
      throws IOException {

    // Create Avro schema for economic indicators
    Schema schema = SchemaBuilder.record("EconomicIndicators")
        .fields()
        .name("geo_id").type().stringType().noDefault()
        .name("year").type().intType().noDefault()
        .name("median_household_income").type().nullable().doubleType().noDefault()
        .name("per_capita_income").type().nullable().doubleType().noDefault()
        .name("labor_force").type().nullable().longType().noDefault()
        .name("employed").type().nullable().longType().noDefault()
        .name("unemployed").type().nullable().longType().noDefault()
        .name("state_fips").type().nullable().stringType().noDefault()
        .name("county_fips").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract year from source directory
    String yearStr = sourceDir.getName().replace("year=", "");
    int year = Integer.parseInt(yearStr);

    // Read and convert state-level data
    File stateFile = new File(sourceDir, "economic_indicators_states.json");
    if (stateFile.exists()) {
      JsonNode data = objectMapper.readTree(stateFile);
      for (int i = 1; i < data.size(); i++) { // Skip header row
        JsonNode row = data.get(i);
        GenericRecord record = new GenericData.Record(schema);
        record.put("geo_id", row.get(row.size() - 1).asText());
        record.put("year", year);
        record.put("median_household_income", row.get(0).asDouble(0.0));
        record.put("per_capita_income", row.get(1).asDouble(0.0));
        record.put("labor_force", row.get(2).asLong(0L));
        record.put("employed", row.get(3).asLong(0L));
        record.put("unemployed", row.get(4).asLong(0L));
        record.put("state_fips", row.get(row.size() - 1).asText());
        record.put("county_fips", null);
        records.add(record);
      }
    }

    // Read and convert county-level data
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    for (String state : stateFips) {
      File countyFile = new File(sourceDir, "economic_indicators_county_" + state + ".json");
      if (countyFile.exists()) {
        JsonNode data = objectMapper.readTree(countyFile);
        for (int i = 1; i < data.size(); i++) { // Skip header row
          JsonNode row = data.get(i);
          GenericRecord record = new GenericData.Record(schema);
          String countyFips = row.get(row.size() - 1).asText(); // County FIPS
          record.put("geo_id", countyFips);
          record.put("year", year);
          record.put("median_household_income", row.get(0).asDouble(0.0));
          record.put("per_capita_income", row.get(1).asDouble(0.0));
          record.put("labor_force", row.get(2).asLong(0L));
          record.put("employed", row.get(3).asLong(0L));
          record.put("unemployed", row.get(4).asLong(0L));
          record.put("state_fips", state);
          record.put("county_fips", countyFips);
          records.add(record);
        }
      }
    }

    // Write to Parquet
    if (storageProvider != null && !records.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, schema, records, "EconomicIndicators");
      LOGGER.info("Created economic indicators parquet: {} with {} records",
          targetFilePath, records.size());
    }
  }
}