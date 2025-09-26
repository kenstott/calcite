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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts Federal Reserve Economic Data (FRED) to Parquet format.
 * Provides access to thousands of economic time series including interest rates,
 * GDP, monetary aggregates, and economic indicators.
 * 
 * <p>Requires a FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html
 */
public class FredDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FredDataDownloader.class);
  private static final String FRED_API_BASE = "https://api.stlouisfed.org/fred/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
  
  private final String cacheDir;
  private final String apiKey;
  private final HttpClient httpClient;
  private final org.apache.calcite.adapter.file.storage.StorageProvider storageProvider;
  private final CacheManifest cacheManifest;
  
  // Key FRED series IDs for economic indicators
  public static class Series {
    // Interest Rates
    public static final String FED_FUNDS_RATE = "DFF";           // Federal Funds Effective Rate
    public static final String TEN_YEAR_TREASURY = "DGS10";      // 10-Year Treasury Rate
    public static final String THREE_MONTH_TBILL = "DGS3MO";     // 3-Month Treasury Bill
    public static final String MORTGAGE_RATE_30Y = "MORTGAGE30US"; // 30-Year Fixed Mortgage Rate
    
    // GDP and Growth
    public static final String REAL_GDP = "GDPC1";               // Real GDP
    public static final String NOMINAL_GDP = "GDP";              // Nominal GDP
    public static final String GDP_DEFLATOR = "GDPDEF";          // GDP Deflator
    public static final String REAL_GDP_PER_CAPITA = "A939RX0Q048SBEA"; // Real GDP per Capita
    
    // Inflation
    public static final String CPI_ALL_URBAN = "CPIAUCSL";       // Consumer Price Index
    public static final String CORE_CPI = "CPILFESL";            // Core CPI (less food & energy)
    public static final String PCE_INFLATION = "PCEPI";          // PCE Price Index
    public static final String BREAKEVEN_INFLATION_5Y = "T5YIE"; // 5-Year Breakeven Inflation Rate
    
    // Labor Market
    public static final String UNEMPLOYMENT_RATE = "UNRATE";     // Unemployment Rate
    public static final String LABOR_FORCE_PARTICIPATION = "CIVPART"; // Labor Force Participation Rate
    public static final String NONFARM_PAYROLLS = "PAYEMS";      // All Employees: Total Nonfarm
    public static final String INITIAL_CLAIMS = "ICSA";          // Initial Jobless Claims
    
    // Money Supply
    public static final String M1_MONEY_SUPPLY = "M1SL";         // M1 Money Stock
    public static final String M2_MONEY_SUPPLY = "M2SL";         // M2 Money Stock
    public static final String MONETARY_BASE = "BOGMBASE";       // Monetary Base
    
    // Housing
    public static final String HOUSING_STARTS = "HOUST";         // Housing Starts
    public static final String CASE_SHILLER_INDEX = "CSUSHPISA"; // Case-Shiller Home Price Index
    public static final String EXISTING_HOME_SALES = "EXHOSLUSM495S"; // Existing Home Sales
    
    // Manufacturing & Trade
    public static final String INDUSTRIAL_PRODUCTION = "INDPRO";  // Industrial Production Index
    public static final String CAPACITY_UTILIZATION = "TCU";      // Capacity Utilization
    public static final String RETAIL_SALES = "RSXFS";           // Retail Sales
    public static final String TRADE_BALANCE = "BOPGSTB";        // Trade Balance
    
    // Financial Markets
    public static final String SP500 = "SP500";                  // S&P 500 Index
    public static final String VIX = "VIXCLS";                   // VIX Volatility Index
    public static final String DOLLAR_INDEX = "DTWEXBGS";        // Trade Weighted US Dollar Index
    public static final String CORPORATE_SPREADS = "BAMLC0A0CM"; // Investment Grade Corporate Spreads
    
    // Banking Indicators
    public static final String COMMERCIAL_BANK_DEPOSITS = "DPSACBW027SBOG"; // Deposits at Commercial Banks
    public static final String BANK_CREDIT = "TOTBKCR";                     // Bank Credit, All Commercial Banks
    public static final String BANK_LENDING_STANDARDS = "DRTSCILM";         // Net Percentage of Banks Tightening Standards for C&I Loans
    public static final String MORTGAGE_DELINQUENCY_RATE = "DRSFRMACBS";    // Delinquency Rate on Single-Family Residential Mortgages
    
    // Real Estate Metrics
    public static final String BUILDING_PERMITS = "PERMIT";                 // New Private Housing Permits
    public static final String MEDIAN_HOME_PRICE = "MSPUS";                 // Median Sales Price of Houses Sold in the United States
    public static final String RENTAL_VACANCY_RATE = "RRVRUSQ156N";         // Rental Vacancy Rate in the United States
    public static final String SINGLE_UNIT_HOUSING_STARTS = "HOUST1F";      // Housing Starts: 1-Unit Structures
    
    // Consumer Sentiment Indices
    public static final String CONSUMER_SENTIMENT = "UMCSENT";              // University of Michigan Consumer Sentiment Index
    public static final String REAL_DISPOSABLE_INCOME = "DSPIC96";          // Real Disposable Personal Income
    public static final String CONSUMER_CONFIDENCE = "CSCICP03USM665S";     // Consumer Confidence Index
    public static final String PERSONAL_SAVING_RATE = "PSAVERT";            // Personal Saving Rate

    // Fed Policy & Financial Conditions (Phase 1)
    public static final String CHICAGO_FED_FINANCIAL_CONDITIONS = "NFCI";   // Chicago Fed Financial Conditions Index
    public static final String FED_FUNDS_UPPER_TARGET = "DFEDTARU";         // Federal Funds Upper Target Rate
    public static final String EFFECTIVE_FED_FUNDS_RATE = "EFFR";           // Effective Federal Funds Rate (Daily)
    public static final String STLOUIS_FED_FINANCIAL_STRESS = "STLFSI";     // St. Louis Fed Financial Stress Index

    // Enhanced Credit Markets (Phase 1)
    public static final String HIGH_YIELD_CORPORATE_SPREADS = "BAMLH0A0HYM2"; // High Yield Corporate Bond Spreads
    public static final String AAA_CORPORATE_BOND_YIELD = "BAMLC0A1CAAAEY"; // AAA Corporate Bond Yield
    public static final String MORTGAGE_RATE_15Y = "MORTGAGE15US";          // 15-Year Fixed Mortgage Rate

    // International/Commodities (Phase 1)
    public static final String WTI_CRUDE_OIL = "DCOILWTICO";               // WTI Crude Oil Price

    // Complete Treasury Yield Curve (Phase 2)
    public static final String THIRTY_YEAR_TREASURY = "DGS30";             // 30-Year Treasury Rate
    public static final String FIVE_YEAR_TREASURY = "DGS5";               // 5-Year Treasury Rate
    public static final String TWO_YEAR_TREASURY = "DGS2";                // 2-Year Treasury Rate
    public static final String YIELD_CURVE_10Y2Y = "T10Y2Y";              // 10-Year minus 2-Year Treasury Spread
    public static final String YIELD_CURVE_10Y3M = "T10Y3M";              // 10-Year minus 3-Month Treasury Spread

    // International & Advanced Indicators (Phase 3)
    public static final String USD_JPY_EXCHANGE_RATE = "DEXJPUS";          // US/Japan Foreign Exchange Rate
    public static final String USD_CAD_EXCHANGE_RATE = "DEXCAUS";          // US/Canada Foreign Exchange Rate
    public static final String USD_EUR_EXCHANGE_RATE = "DEXUSEU";          // US/Euro Foreign Exchange Rate
    public static final String RECESSION_PROBABILITIES = "RECPROUSM156N";  // Smoothed US Recession Probabilities
    public static final String MEDIAN_HOUSEHOLD_INCOME = "MEHOINUSA672N";  // Real Median Household Income
    public static final String TOTAL_VEHICLE_SALES = "TOTALSA";            // Total Vehicle Sales
    public static final String BUILDING_PERMITS_NEW = "PERMIT";            // New Private Housing Units Authorized
  }
  
  // Default series to download if none specified
  public static final List<String> DEFAULT_SERIES = Arrays.asList(
      // Core Economic Indicators
      Series.FED_FUNDS_RATE,
      Series.TEN_YEAR_TREASURY,
      Series.REAL_GDP,
      Series.CPI_ALL_URBAN,
      Series.UNEMPLOYMENT_RATE,
      Series.M2_MONEY_SUPPLY,
      Series.HOUSING_STARTS,
      Series.INDUSTRIAL_PRODUCTION,
      Series.SP500,
      Series.DOLLAR_INDEX,
      // Phase 1 Additions - Fed Policy & Financial Conditions
      Series.CHICAGO_FED_FINANCIAL_CONDITIONS,
      Series.EFFECTIVE_FED_FUNDS_RATE,
      Series.HIGH_YIELD_CORPORATE_SPREADS,
      Series.WTI_CRUDE_OIL,
      // Enhanced Treasury Curve
      Series.THIRTY_YEAR_TREASURY,
      Series.FIVE_YEAR_TREASURY,
      Series.TWO_YEAR_TREASURY,
      Series.YIELD_CURVE_10Y2Y,
      // International Context
      Series.USD_JPY_EXCHANGE_RATE,
      Series.USD_EUR_EXCHANGE_RATE,
      // Banking Indicators
      Series.COMMERCIAL_BANK_DEPOSITS,
      Series.BANK_CREDIT,
      Series.BANK_LENDING_STANDARDS,
      Series.MORTGAGE_DELINQUENCY_RATE,
      // Real Estate Metrics
      Series.BUILDING_PERMITS,
      Series.MEDIAN_HOME_PRICE,
      Series.RENTAL_VACANCY_RATE,
      Series.SINGLE_UNIT_HOUSING_STARTS,
      // Consumer Sentiment Indices
      Series.CONSUMER_SENTIMENT,
      Series.REAL_DISPOSABLE_INCOME,
      Series.CONSUMER_CONFIDENCE,
      Series.PERSONAL_SAVING_RATE
  );
  
  public FredDataDownloader(String cacheDir, String apiKey, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this.cacheDir = cacheDir;
    this.apiKey = apiKey;
    this.storageProvider = storageProvider;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
    this.cacheManifest = CacheManifest.load(cacheDir);
  }

  /**
   * Writes records to parquet using the StorageProvider pattern.
   */
  private void writeParquetFile(String targetPath, Schema schema, List<GenericRecord> records) throws IOException {
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  /**
   * Creates metadata map for Parquet file with table and column comments.
   *
   * @param tableComment The comment for the table
   * @param columnComments Map of column names to their comments
   * @return Map of metadata key-value pairs
   */
  private Map<String, String> createParquetMetadata(String tableComment,
      Map<String, String> columnComments) {
    Map<String, String> metadata = new HashMap<>();

    // Add table-level comments
    if (tableComment != null && !tableComment.isEmpty()) {
      metadata.put("parquet.meta.table.comment", tableComment);
      metadata.put("parquet.meta.comment", tableComment); // Also set generic comment
    }

    // Add column-level comments
    if (columnComments != null && !columnComments.isEmpty()) {
      for (Map.Entry<String, String> entry : columnComments.entrySet()) {
        metadata.put("parquet.meta.column." + entry.getKey() + ".comment", entry.getValue());
      }
    }

    return metadata;
  }
  
  /**
   * Gets the default start date from environment variables.
   */
  public static String getDefaultStartDate() {
    // Check for ECON-specific override
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return econStart + "-01-01";
      } catch (Exception e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }
    
    // Fall back to unified setting
    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return govdataStart + "-01-01";
      } catch (Exception e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }
    
    // Default to 5 years ago
    return LocalDate.now().minusYears(5).format(DateTimeFormatter.ISO_LOCAL_DATE);
  }
  
  /**
   * Gets the default end date from environment variables.
   */
  public static String getDefaultEndDate() {
    // Check for ECON-specific override
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return econEnd + "-12-31";
      } catch (Exception e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }
    
    // Fall back to unified setting
    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return govdataEnd + "-12-31";
      } catch (Exception e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }
    
    // Default to current date
    return LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
  }
  
  /**
   * Downloads all FRED data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading FRED data for years {} to {}", startYear, endYear);
    
    for (int year = startYear; year <= endYear; year++) {
      downloadEconomicIndicatorsForYear(year);
    }
  }
  
  /**
   * Downloads economic indicators for a specific year.
   */
  public void downloadEconomicIndicatorsForYear(int year) throws IOException, InterruptedException {
    String startDate = year + "-01-01";
    String endDate = year + "-12-31";
    
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("FRED API key is required. Set FRED_API_KEY environment variable.");
    }
    
    String outputDirPath = "source=econ/type=indicators/year=" + year;
    // Directories are created automatically by StorageProvider when writing files

    // Check cache manifest first
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("type", "fred_indicators");
    cacheParams.put("year", String.valueOf(year));

    String jsonFilePath = outputDirPath + "/fred_indicators.json";

    if (cacheManifest.isCached("fred_indicators", year, cacheParams)) {
      LOGGER.info("Found cached FRED data for year {} - skipping download", year);
      return;
    }

    // Check if file exists but not in manifest - update manifest
    File jsonFile = new File(cacheDir, jsonFilePath);
    if (jsonFile.exists()) {
      LOGGER.info("Found existing FRED file for year {} - updating manifest", year);
      long fileSize = jsonFile.length();
      cacheManifest.markCached("fred_indicators", year, cacheParams, jsonFilePath, fileSize);
      cacheManifest.save(cacheDir);
      return;
    }
    
    LOGGER.info("Downloading {} FRED series for year {}", DEFAULT_SERIES.size(), year);
    
    List<Map<String, Object>> observations = new ArrayList<>();
    Map<String, FredSeriesInfo> seriesInfoMap = new HashMap<>();
    
    // First, get series info for all requested series
    for (String seriesId : DEFAULT_SERIES) {
      try {
        FredSeriesInfo info = getSeriesInfo(seriesId);
        if (info != null) {
          seriesInfoMap.put(seriesId, info);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to get info for series {}: {}", seriesId, e.getMessage());
      }
    }
    
    // Download observations for each series
    for (String seriesId : DEFAULT_SERIES) {
      FredSeriesInfo info = seriesInfoMap.get(seriesId);
      if (info == null) {
        continue;
      }
      
      LOGGER.info("Fetching series: {} - {}", seriesId, info.title);
      
      String url = FRED_API_BASE + "series/observations"
          + "?series_id=" + seriesId
          + "&api_key=" + apiKey
          + "&file_type=json"
          + "&observation_start=" + startDate
          + "&observation_end=" + endDate;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("FRED API request failed for series {} with status: {}", 
            seriesId, response.statusCode());
        continue;
      }
      
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode obsArray = root.get("observations");
      
      if (obsArray != null && obsArray.isArray()) {
        for (JsonNode obs : obsArray) {
          String valueStr = obs.get("value").asText();
          if (!".".equals(valueStr)) { // FRED uses "." for missing values
            Map<String, Object> observation = new HashMap<>();
            observation.put("series_id", seriesId);
            observation.put("series_name", info.title);
            observation.put("date", obs.get("date").asText());
            observation.put("value", Double.parseDouble(valueStr));
            observation.put("units", info.units);
            observation.put("frequency", info.frequency);
            observation.put("seasonal_adjustment", info.seasonalAdjustment);
            observation.put("last_updated", info.lastUpdated);
            
            observations.add(observation);
          }
        }
      }
      
      // Small delay to be respectful to the API
      Thread.sleep(100);
    }
    
    // Save raw JSON data to cache
    Map<String, Object> data = new HashMap<>();
    data.put("observations", observations);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    
    String jsonContent = MAPPER.writeValueAsString(data);
    // Save raw JSON data to local cache directory
    jsonFile = new File(cacheDir, jsonFilePath);
    jsonFile.getParentFile().mkdirs();
    Files.write(jsonFile.toPath(), jsonContent.getBytes(StandardCharsets.UTF_8));

    // Mark as cached in manifest
    cacheManifest.markCached("fred_indicators", year, cacheParams, jsonFilePath, jsonContent.length());
    cacheManifest.save(cacheDir);

    LOGGER.info("FRED indicators saved to: {} ({} observations)", jsonFilePath, observations.size());
  }

  /**
   * Downloads economic indicators using default date range and series.
   */
  public File downloadEconomicIndicators() throws IOException, InterruptedException {
    return downloadEconomicIndicators(DEFAULT_SERIES, getDefaultStartDate(), getDefaultEndDate());
  }
  
  /**
   * Downloads specified FRED economic time series data.
   */
  public File downloadEconomicIndicators(List<String> seriesIds, String startDate, String endDate) 
      throws IOException, InterruptedException {
    
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("FRED API key is required. Set FRED_API_KEY environment variable.");
    }
    
    LOGGER.info("Downloading {} FRED series from {} to {}", seriesIds.size(), startDate, endDate);
    
    String outputDirPath = String.format("source=econ/type=fred_indicators/date_range=%s_%s",
            startDate.substring(0, 10), endDate.substring(0, 10));
    // Directories are created automatically by StorageProvider when writing files
    
    List<FredObservation> observations = new ArrayList<>();
    Map<String, FredSeriesInfo> seriesInfoMap = new HashMap<>();
    
    // First, get series info for all requested series
    for (String seriesId : seriesIds) {
      try {
        FredSeriesInfo info = getSeriesInfo(seriesId);
        if (info != null) {
          seriesInfoMap.put(seriesId, info);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to get info for series {}: {}", seriesId, e.getMessage());
      }
    }
    
    // Download observations for each series
    for (String seriesId : seriesIds) {
      FredSeriesInfo info = seriesInfoMap.get(seriesId);
      if (info == null) {
        continue;
      }
      
      LOGGER.info("Fetching series: {} - {}", seriesId, info.title);
      
      String url = FRED_API_BASE + "series/observations"
          + "?series_id=" + seriesId
          + "&api_key=" + apiKey
          + "&file_type=json"
          + "&observation_start=" + startDate
          + "&observation_end=" + endDate;
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        LOGGER.warn("FRED API request failed for series {} with status: {}", 
            seriesId, response.statusCode());
        continue;
      }
      
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode obsArray = root.get("observations");
      
      if (obsArray != null && obsArray.isArray()) {
        for (JsonNode obs : obsArray) {
          String valueStr = obs.get("value").asText();
          if (!".".equals(valueStr)) { // FRED uses "." for missing values
            FredObservation observation = new FredObservation();
            observation.seriesId = seriesId;
            observation.seriesName = info.title;
            observation.date = obs.get("date").asText();
            observation.value = Double.parseDouble(valueStr);
            observation.units = info.units;
            observation.frequency = info.frequency;
            observation.seasonalAdjustment = info.seasonalAdjustment;
            observation.lastUpdated = info.lastUpdated;
            
            observations.add(observation);
          }
        }
      }
      
      // Small delay to be respectful to the API
      Thread.sleep(100);
    }
    
    LOGGER.info("FRED indicators data collected: {} observations", observations.size());
    
    // Convert to Parquet format
    String parquetFileName = String.format("fred_indicators_%s_%s.parquet", 
        startDate.substring(0, 10), endDate.substring(0, 10));
    String parquetPath = outputDirPath + "/" + parquetFileName;
    convertToParquet(observations, parquetPath);
    
    // For compatibility, return a File representing the Parquet file
    // Note: This assumes local storage for the return value
    return new File(parquetPath);
  }
  
  /**
   * Gets metadata information for a FRED series.
   */
  private FredSeriesInfo getSeriesInfo(String seriesId) throws IOException, InterruptedException {
    String url = FRED_API_BASE + "series"
        + "?series_id=" + seriesId
        + "&api_key=" + apiKey
        + "&file_type=json";
    
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(10))
        .build();
    
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    
    if (response.statusCode() != 200) {
      throw new IOException("Failed to get series info for " + seriesId);
    }
    
    JsonNode root = MAPPER.readTree(response.body());
    JsonNode seriesArray = root.get("seriess");
    
    if (seriesArray != null && seriesArray.isArray() && seriesArray.size() > 0) {
      JsonNode series = seriesArray.get(0);
      FredSeriesInfo info = new FredSeriesInfo();
      info.id = series.get("id").asText();
      info.title = series.get("title").asText();
      info.units = series.get("units").asText();
      info.frequency = series.get("frequency").asText();
      info.seasonalAdjustment = series.get("seasonal_adjustment").asText();
      info.lastUpdated = series.get("last_updated").asText();
      return info;
    }
    
    return null;
  }
  
  /**
   * Converts FRED observations to Parquet format.
   */
  private void convertToParquet(List<FredObservation> observations, String targetFilePath) throws IOException {
    // Convert FredObservation objects to Map format for compatibility with existing writer
    List<Map<String, Object>> mapObservations = new ArrayList<>();
    for (FredObservation obs : observations) {
      Map<String, Object> map = new HashMap<>();
      map.put("series_id", obs.seriesId);
      map.put("series_name", obs.seriesName);
      map.put("date", obs.date);
      map.put("value", obs.value);
      map.put("units", obs.units);
      map.put("frequency", obs.frequency);
      mapObservations.add(map);
    }
    
    File targetFile = new File(targetFilePath);
    writeFredIndicatorsParquet(mapObservations, targetFilePath);
    LOGGER.info("Converted FRED indicators to parquet: {} ({} observations)", targetFilePath, observations.size());
  }
  
  
  // Data classes
  private static class FredSeriesInfo {
    String id;
    String title;
    String units;
    String frequency;
    String seasonalAdjustment;
    String lastUpdated;
  }
  
  private static class FredObservation {
    String seriesId;
    String seriesName;
    String date;
    double value;
    String units;
    String frequency;
    String seasonalAdjustment;
    String lastUpdated;
  }
  
  /**
   * Converts cached FRED data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   * 
   * @param sourceDir Directory containing cached FRED JSON data
   * @param targetFile Target parquet file to create
   */
  public void convertToParquet(File sourceDir, String targetFilePath) throws IOException {
    String sourceDirPath = sourceDir.getAbsolutePath();
    
    LOGGER.info("Converting FRED data from {} to parquet: {}", sourceDirPath, targetFilePath);
    
    // Skip if target file already exists
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("Target parquet file already exists, skipping: {}", targetFilePath);
      return;
    }

    // Directories are created automatically by StorageProvider when writing files

    List<Map<String, Object>> observations = new ArrayList<>();

    // Look for FRED indicators JSON files in the source directory
    // We use direct file operations since these are JSON cache files
    File sourceDirFile = new File(cacheDir, sourceDir.getName());
    File[] files = sourceDirFile.listFiles((dir, name) -> name.equals("fred_indicators.json"));

    if (files != null) {
      for (File file : files) {
        try {
          String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);
          JsonNode obsArray = root.get("observations");
          
          if (obsArray != null && obsArray.isArray()) {
            for (JsonNode obs : obsArray) {
              Map<String, Object> observation = new HashMap<>();
              observation.put("series_id", obs.get("series_id").asText());
              observation.put("series_name", obs.get("series_name").asText());
              observation.put("date", obs.get("date").asText());
              observation.put("value", obs.get("value").asDouble());
              observation.put("units", obs.get("units").asText());
              observation.put("frequency", obs.get("frequency").asText());
              
              observations.add(observation);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to process FRED JSON file {}: {}", file.getPath(), e.getMessage());
        }
      }
    }
    
    // Write parquet file
    writeFredIndicatorsParquet(observations, targetFilePath);
    
    LOGGER.info("Converted FRED data to parquet: {} ({} observations)", targetFilePath, observations.size());
  }
  
  @SuppressWarnings("deprecation")
  private void writeFredIndicatorsParquet(List<Map<String, Object>> observations, String targetPath)
      throws IOException {
    Schema schema = SchemaBuilder.record("FredIndicator")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .requiredString("series_id")
        .requiredString("series_name")
        .requiredString("date")
        .requiredDouble("value")
        .requiredString("units")
        .requiredString("frequency")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> obs : observations) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("series_id", obs.get("series_id"));
      record.put("series_name", obs.get("series_name"));
      record.put("date", obs.get("date"));
      record.put("value", obs.get("value"));
      record.put("units", obs.get("units"));
      record.put("frequency", obs.get("frequency"));
      records.add(record);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, "FredIndicator");
  }

  /**
   * Download a specific FRED series for a date range.
   *
   * @param seriesId FRED series identifier
   * @param startYear Start year (inclusive)
   * @param endYear End year (inclusive)
   */
  public void downloadSeries(String seriesId, int startYear, int endYear)
      throws IOException, InterruptedException {

    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("FRED API key is required. Set FRED_API_KEY environment variable.");
    }

    LOGGER.info("Downloading FRED series {} for years {} to {}", seriesId, startYear, endYear);

    String startDate = startYear + "-01-01";
    String endDate = endYear + "-12-31";
    String outputDirPath = String.format("source=econ/type=custom_fred/series=%s", seriesId);

    // Check if we already have this data cached
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("type", "custom_fred_series");
    cacheParams.put("series_id", seriesId);
    cacheParams.put("start_year", String.valueOf(startYear));
    cacheParams.put("end_year", String.valueOf(endYear));

    String jsonFilePath = outputDirPath + "/" + seriesId + "_" + startYear + "_" + endYear + ".json";

    if (cacheManifest.isCached("custom_fred_series", startYear, cacheParams)) {
      LOGGER.info("Found cached FRED series {} data - skipping download", seriesId);
      return;
    }

    // Check if file exists but not in manifest
    File jsonFile = new File(cacheDir, jsonFilePath);
    if (jsonFile.exists()) {
      LOGGER.info("Found existing FRED series {} file - updating manifest", seriesId);
      long fileSize = jsonFile.length();
      cacheManifest.markCached("custom_fred_series", startYear, cacheParams, jsonFilePath, fileSize);
      cacheManifest.save(cacheDir);
      return;
    }

    // Get series info first
    FredSeriesInfo info = getSeriesInfo(seriesId);
    if (info == null) {
      throw new IOException("Could not retrieve series info for " + seriesId);
    }

    LOGGER.info("Fetching series: {} - {}", seriesId, info.title);

    String url = FRED_API_BASE + "series/observations"
        + "?series_id=" + seriesId
        + "&api_key=" + apiKey
        + "&file_type=json"
        + "&observation_start=" + startDate
        + "&observation_end=" + endDate;

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("FRED API request failed for series " + seriesId
          + " with status: " + response.statusCode());
    }

    JsonNode root = MAPPER.readTree(response.body());
    JsonNode obsArray = root.get("observations");

    List<Map<String, Object>> observations = new ArrayList<>();

    if (obsArray != null && obsArray.isArray()) {
      for (JsonNode obs : obsArray) {
        String valueStr = obs.get("value").asText();
        if (!".".equals(valueStr)) { // FRED uses "." for missing values
          Map<String, Object> observation = new HashMap<>();
          observation.put("series_id", seriesId);
          observation.put("series_name", info.title != null ? info.title : "");
          observation.put("date", obs.get("date").asText());
          observation.put("value", Double.parseDouble(valueStr));
          observation.put("units", info.units != null ? info.units : "");
          observation.put("frequency", info.frequency != null ? info.frequency : "");
          observation.put("seasonal_adjustment", info.seasonalAdjustment != null ? info.seasonalAdjustment : "");
          observation.put("last_updated", info.lastUpdated != null ? info.lastUpdated : "");

          observations.add(observation);
        }
      }
    }

    // Save raw JSON data to cache
    Map<String, Object> data = new HashMap<>();
    data.put("series_id", seriesId);

    // Convert info to serializable map with null-safe handling
    Map<String, String> seriesInfoMap = new HashMap<>();
    seriesInfoMap.put("id", info.id != null ? info.id : "");
    seriesInfoMap.put("title", info.title != null ? info.title : "");
    seriesInfoMap.put("units", info.units != null ? info.units : "");
    seriesInfoMap.put("frequency", info.frequency != null ? info.frequency : "");
    seriesInfoMap.put("seasonal_adjustment", info.seasonalAdjustment != null ? info.seasonalAdjustment : "");
    seriesInfoMap.put("last_updated", info.lastUpdated != null ? info.lastUpdated : "");
    data.put("series_info", seriesInfoMap);

    data.put("observations", observations);
    data.put("download_date", LocalDate.now().toString());
    data.put("start_year", startYear);
    data.put("end_year", endYear);

    String jsonContent = MAPPER.writeValueAsString(data);

    // Save to cache
    jsonFile.getParentFile().mkdirs();
    Files.write(jsonFile.toPath(), jsonContent.getBytes(StandardCharsets.UTF_8));

    // Mark as cached in manifest
    cacheManifest.markCached("custom_fred_series", startYear, cacheParams, jsonFilePath, jsonContent.length());
    cacheManifest.save(cacheDir);

    LOGGER.info("FRED series {} saved to: {} ({} observations)", seriesId, jsonFilePath, observations.size());
  }

  /**
   * Convert a specific FRED series to Parquet format with optional partitioning.
   *
   * @param seriesId FRED series identifier
   * @param targetPath Target parquet file path
   * @param partitionFields List of partition fields (can be null for no partitioning)
   */
  public void convertSeriesToParquet(String seriesId, String targetPath, List<String> partitionFields)
      throws IOException {

    LOGGER.debug("Converting FRED series {} to parquet: {}", seriesId, targetPath);

    // Skip if target file already exists
    if (storageProvider.exists(targetPath)) {
      LOGGER.debug("Target parquet file already exists, skipping: {}", targetPath);
      return;
    }

    // Find the cached JSON file for this series
    // Look for any JSON file containing this series
    File cacheRoot = new File(cacheDir);
    List<Map<String, Object>> observations = new ArrayList<>();

    // Search through cache directory structure
    if (!findAndLoadSeriesData(cacheRoot, seriesId, observations)) {
      LOGGER.warn("No cached data found for FRED series: {}", seriesId);
      return;
    }

    if (observations.isEmpty()) {
      LOGGER.warn("No observations found for FRED series: {}", seriesId);
      return;
    }

    // Filter observations for this specific series (in case cache contains multiple series)
    List<Map<String, Object>> seriesObservations = new ArrayList<>();
    for (Map<String, Object> obs : observations) {
      if (seriesId.equals(obs.get("series_id"))) {
        seriesObservations.add(obs);
      }
    }

    // Create target directory
    File targetFile = new File(targetPath);
    targetFile.getParentFile().mkdirs();

    // Write parquet file
    writeFredIndicatorsParquet(seriesObservations, targetPath);

    LOGGER.info("Converted FRED series {} to parquet: {} ({} observations)",
        seriesId, targetPath, seriesObservations.size());
  }

  /**
   * Recursively search for and load series data from cache files.
   */
  private boolean findAndLoadSeriesData(File directory, String seriesId, List<Map<String, Object>> observations) {
    if (!directory.exists() || !directory.isDirectory()) {
      return false;
    }

    boolean found = false;
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          // Recurse into subdirectories
          if (findAndLoadSeriesData(file, seriesId, observations)) {
            found = true;
          }
        } else if (file.getName().endsWith(".json")) {
          try {
            String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            JsonNode root = MAPPER.readTree(content);

            // Check if this file contains our series
            JsonNode obsArray = root.get("observations");
            if (obsArray != null && obsArray.isArray()) {
              boolean hasOurSeries = false;
              for (JsonNode obs : obsArray) {
                if (seriesId.equals(obs.get("series_id").asText())) {
                  hasOurSeries = true;
                  break;
                }
              }

              if (hasOurSeries) {
                // Load all observations from this file
                for (JsonNode obs : obsArray) {
                  Map<String, Object> observation = new HashMap<>();
                  observation.put("series_id", obs.get("series_id").asText());
                  observation.put("series_name", obs.get("series_name").asText());
                  observation.put("date", obs.get("date").asText());
                  observation.put("value", obs.get("value").asDouble());
                  observation.put("units", obs.get("units").asText());
                  observation.put("frequency", obs.get("frequency").asText());

                  observations.add(observation);
                }
                found = true;
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to process cache file {}: {}", file.getPath(), e.getMessage());
          }
        }
      }
    }

    return found;
  }
}