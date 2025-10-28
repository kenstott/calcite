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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
public class FredDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FredDataDownloader.class);
  private static final String FRED_API_BASE = "https://api.stlouisfed.org/fred/";
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

  // FRED API pagination limits
  private static final int MAX_OBSERVATIONS_PER_REQUEST = 100000; // FRED default limit
  private static final int FRED_API_DELAY_MS = 150; // Delay between paginated requests

  // Rate limiting: FRED API allows 120 requests per minute
  // Safe rate: 500ms between requests = 120 requests/minute exactly
  private static final long MIN_REQUEST_INTERVAL_MS = 500; // 500ms between requests (120 req/min)
  private static final int MAX_RETRIES = 5; // Increased retries for rate limit recovery
  private static final long RETRY_DELAY_MS = 5000; // 5 seconds initial retry delay (more conservative)

  private final String apiKey;

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
  public static final List<String> DEFAULT_SERIES =
      Arrays.asList(// Core Economic Indicators
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
      Series.PERSONAL_SAVING_RATE);

  public FredDataDownloader(String cacheDir, String apiKey, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(cacheDir, cacheDir, apiKey, cacheStorageProvider, storageProvider, null);
  }

  public FredDataDownloader(String cacheDir, String operatingDirectory, String apiKey, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, cacheDir, cacheStorageProvider, storageProvider, sharedManifest);
    this.apiKey = apiKey;
  }

  @Override protected long getMinRequestIntervalMs() {
    return MIN_REQUEST_INTERVAL_MS;
  }

  @Override protected int getMaxRetries() {
    return MAX_RETRIES;
  }

  @Override protected long getRetryDelayMs() {
    return RETRY_DELAY_MS;
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

    String relativePath = buildPartitionPath("fred_indicators", DataFrequency.MONTHLY, year) + "/fred_indicators.json";

    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year and type are already separate params

    // Check cache using base class helper
    if (isCachedOrExists("fred_indicators", year, cacheParams, relativePath)) {
      LOGGER.info("Found cached FRED data for year {} - skipping download", year);
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

      try {
        // Use paginated observations fetching
        List<JsonNode> seriesObservations = fetchSeriesObservationsPaginated(seriesId, startDate, endDate);

        for (JsonNode obs : seriesObservations) {
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
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch observations for series {}: {}", seriesId, e.getMessage());
        continue;
      }
    }

    // Save raw JSON data to cache
    Map<String, Object> data = new HashMap<>();
    data.put("observations", observations);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);

    // Save to cache using base class helper
    saveToCache("fred_indicators", year, cacheParams, relativePath, jsonContent);
    LOGGER.info("Downloaded {} FRED observations for year {}", observations.size(), year);
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

      try {
        // Use paginated observations fetching
        List<JsonNode> seriesObservations = fetchSeriesObservationsPaginated(seriesId, startDate, endDate);

        for (JsonNode obs : seriesObservations) {
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
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch observations for series {}: {}", seriesId, e.getMessage());
        continue;
      }
    }

    LOGGER.info("FRED indicators data collected: {} observations", observations.size());

    // Convert to Parquet format
    String parquetFileName =
        String.format("fred_indicators_%s_%s.parquet", startDate.substring(0, 10), endDate.substring(0, 10));
    String relativePath =
        String.format("source=econ/type=fred_indicators/date_range=%s_%s/%s", startDate.substring(0, 10), endDate.substring(0, 10), parquetFileName);
    convertToParquet(observations, relativePath);

    // For compatibility, return a File representing the Parquet file
    // Note: This assumes local storage for the return value
    return new File(relativePath);
  }

  /**
   * Fetches observations for a FRED series with automatic pagination handling.
   * The FRED API may return large datasets that need to be fetched in chunks.
   */
  private List<JsonNode> fetchSeriesObservationsPaginated(String seriesId, String startDate, String endDate)
      throws IOException, InterruptedException {
    List<JsonNode> allObservations = new ArrayList<>();
    int offset = 0;
    boolean hasMoreData = true;
    int requestCount = 0;

    LOGGER.debug("Fetching observations for series {} with pagination", seriesId);

    while (hasMoreData) {
      String url = FRED_API_BASE + "series/observations"
          + "?series_id=" + seriesId
          + "&api_key=" + apiKey
          + "&file_type=json"
          + "&observation_start=" + startDate
          + "&observation_end=" + endDate
          + "&limit=" + MAX_OBSERVATIONS_PER_REQUEST
          + "&offset=" + offset;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      // Retry loop with exponential backoff for rate limiting
      int attempt = 0;
      HttpResponse<String> response = null;
      while (attempt < MAX_RETRIES) {
        // Rate limiting: ensure minimum interval between requests
        synchronized (this) {
          long now = System.currentTimeMillis();
          long timeSinceLastRequest = now - lastRequestTime;
          if (timeSinceLastRequest < MIN_REQUEST_INTERVAL_MS) {
            long sleepTime = MIN_REQUEST_INTERVAL_MS - timeSinceLastRequest;
            Thread.sleep(sleepTime);
          }
          lastRequestTime = System.currentTimeMillis();
        }

        response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        requestCount++;

        if (response.statusCode() == 200) {
          break; // Success
        } else if (response.statusCode() == 429) {
          // Rate limit exceeded - retry with exponential backoff
          attempt++;
          if (attempt >= MAX_RETRIES) {
            throw new IOException("FRED API rate limit exceeded after " + MAX_RETRIES + " retries for series " + seriesId +
                ". Response: " + response.body());
          }
          long backoffDelay = RETRY_DELAY_MS * (1L << (attempt - 1)); // Exponential: 2s, 4s, 8s
          LOGGER.warn("FRED API rate limit hit (429) for series {}. Retry {}/{} after {}ms delay",
              seriesId, attempt, MAX_RETRIES, backoffDelay);
          Thread.sleep(backoffDelay);
        } else {
          // Other error - don't retry
          throw new IOException("FRED API request failed for series " + seriesId
              + " with status: " + response.statusCode() + ", body: " + response.body());
        }
      }

      if (response == null || response.statusCode() != 200) {
        throw new IOException("FRED API request failed for series " + seriesId + " after " + MAX_RETRIES + " retries");
      }

      JsonNode root = MAPPER.readTree(response.body());
      JsonNode obsArray = root.get("observations");

      if (obsArray != null && obsArray.isArray() && obsArray.size() > 0) {
        for (JsonNode obs : obsArray) {
          allObservations.add(obs);
        }

        // Check if we got the maximum number of results, indicating more data might be available
        hasMoreData = obsArray.size() == MAX_OBSERVATIONS_PER_REQUEST;
        offset += obsArray.size();

        // Log progress for large datasets
        if (requestCount > 1) {
          LOGGER.debug("Series {} pagination: {} observations fetched in {} requests",
              seriesId, allObservations.size(), requestCount);
        }
      } else {
        hasMoreData = false;
      }

      // Add small delay between requests to be respectful to API
      if (hasMoreData) {
        Thread.sleep(FRED_API_DELAY_MS);
      }
    }

    if (requestCount > 1) {
      LOGGER.info("Series {} required {} paginated requests to fetch {} observations",
          seriesId, requestCount, allObservations.size());
    }

    return allObservations;
  }

  /**
   * Gets metadata information for a FRED series with rate limiting and retry logic.
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

    // Retry loop with exponential backoff for rate limiting
    int attempt = 0;
    HttpResponse<String> response = null;
    while (attempt < MAX_RETRIES) {
      // Rate limiting: ensure minimum interval between requests
      synchronized (this) {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastRequestTime;
        if (timeSinceLastRequest < MIN_REQUEST_INTERVAL_MS) {
          long sleepTime = MIN_REQUEST_INTERVAL_MS - timeSinceLastRequest;
          Thread.sleep(sleepTime);
        }
        lastRequestTime = System.currentTimeMillis();
      }

      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        break; // Success
      } else if (response.statusCode() == 429) {
        // Rate limit exceeded - retry with exponential backoff
        attempt++;
        if (attempt >= MAX_RETRIES) {
          throw new IOException("FRED API rate limit exceeded after " + MAX_RETRIES + " retries for series info " + seriesId);
        }
        long backoffDelay = RETRY_DELAY_MS * (1L << (attempt - 1)); // Exponential: 2s, 4s, 8s
        LOGGER.warn("FRED API rate limit hit (429) getting info for series {}. Retry {}/{} after {}ms delay",
            seriesId, attempt, MAX_RETRIES, backoffDelay);
        Thread.sleep(backoffDelay);
      } else {
        // Other error - don't retry
        throw new IOException("Failed to get series info for " + seriesId);
      }
    }

    if (response == null || response.statusCode() != 200) {
      throw new IOException("Failed to get series info for " + seriesId + " after " + MAX_RETRIES + " retries");
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
   * <p>This method trusts FileSchema's conversion registry to prevent redundant conversions.
   * No defensive file existence check is needed here.
   *
   * @param sourceDirPath Path to directory containing cached FRED JSON data
   * @param targetFilePath Target parquet file to create
   */
  public void convertToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.info("Converting FRED data from {} to parquet: {}", sourceDirPath, targetFilePath);

    List<Map<String, Object>> observations = new ArrayList<>();

    // Read FRED indicators JSON file from cache using cacheStorageProvider
    // sourceDirPath is already the correct cache structure path from EconRawToParquetConverter
    // Storage provider combines: base path (s3://bucket) + relative path (sourceDirPath) + filename
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "fred_indicators.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No fred_indicators.json found in {}", sourceDirPath);
      return;
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);
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
      LOGGER.info("FRED CONVERSION: Read {} observations from {}", observations.size(), jsonFilePath);
    } catch (Exception e) {
      LOGGER.error("FRED CONVERSION: ❌ Failed to process FRED JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      throw new IOException("Failed to read FRED JSON: " + e.getMessage(), e);
    }

    if (observations.isEmpty()) {
      LOGGER.warn("No observations found in FRED indicators JSON file");
      return;
    }

    // Write parquet file
    LOGGER.info("FRED CONVERSION: Writing {} rows to parquet: {}", observations.size(), targetFilePath);
    writeFredIndicatorsParquet(observations, targetFilePath);

    // Verify the file was actually written
    if (storageProvider.exists(targetFilePath)) {
      LOGGER.info("FRED CONVERSION: ✅ Successfully wrote and verified file at: {}", targetFilePath);
    } else {
      LOGGER.error("FRED CONVERSION: ❌ File does NOT exist after write: {}", targetFilePath);
      throw new IOException("FRED parquet file not found after write: " + targetFilePath);
    }

    LOGGER.info("Converted FRED data to parquet: {} ({} observations)", targetFilePath, observations.size());

    // FileSchema's conversion registry automatically tracks this conversion
  }

  @SuppressWarnings("deprecation")
  private void writeFredIndicatorsParquet(List<Map<String, Object>> observations, String targetPath)
      throws IOException {
    Schema schema = SchemaBuilder.record("FredIndicator")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("series_id").doc("FRED series identifier").type().stringType().noDefault()
        .name("series_name").doc("Descriptive name of the economic data series").type().stringType().noDefault()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("value").doc("Observed value for this date").type().doubleType().noDefault()
        .name("units").doc("Units of measurement (e.g., 'Percent', 'Billions of Dollars')").type().stringType().noDefault()
        .name("frequency").doc("Data frequency (e.g., 'Daily', 'Monthly', 'Quarterly', 'Annual')").type().stringType().noDefault()
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
    downloadSeries(seriesId, startYear, endYear, null);
  }

  /**
   * Download a specific FRED series for a date range with explicit table name.
   *
   * @param seriesId FRED series identifier
   * @param startYear Start year (inclusive)
   * @param endYear End year (inclusive)
   * @param tableName Table name for cache key (null to use default "custom_fred_series")
   */
  public void downloadSeries(String seriesId, int startYear, int endYear, String tableName)
      throws IOException, InterruptedException {

    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("FRED API key is required. Set FRED_API_KEY environment variable.");
    }

    String startDate = startYear + "-01-01";
    String endDate = endYear + "-12-31";

    String relativePath =
        String.format("source=econ/type=custom_fred/series=%s/%s_%d_%d.json", seriesId, seriesId, startYear, endYear);

    // Use tableName for cache key if provided, otherwise use default
    String dataType = tableName != null ? tableName : "custom_fred_series";

    // Check if we already have this data cached
    Map<String, String> cacheParams = new HashMap<>();
    // Only include unique identifiers - year and type are already separate params
    cacheParams.put("series_id", seriesId);

    // Check cache using base class helper (use dataType for consistent cache key)
    if (isCachedOrExists(dataType, startYear, cacheParams, relativePath)) {
      LOGGER.debug("Found cached FRED series {} data - skipping download", seriesId);
      return;
    }

    LOGGER.debug("Downloading FRED series {} for years {} to {}", seriesId, startYear, endYear);

    // Get series info first
    FredSeriesInfo info = getSeriesInfo(seriesId);
    if (info == null) {
      throw new IOException("Could not retrieve series info for " + seriesId);
    }

    LOGGER.debug("Fetching series: {} - {}", seriesId, info.title);

    // Use paginated observations fetching
    List<JsonNode> allObservations = fetchSeriesObservationsPaginated(seriesId, startDate, endDate);

    List<Map<String, Object>> observations = new ArrayList<>();

    for (JsonNode obs : allObservations) {
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

    // Save to cache using base class helper - use dataType not hardcoded string
    saveToCache(dataType, startYear, cacheParams, relativePath, jsonContent);
    LOGGER.info("Downloaded FRED series {}: {} observations", seriesId, observations.size());
  }

  /**
   * Convert a specific FRED series to Parquet format with optional partitioning.
   *
   * <p>This method reads cached series data using StorageProvider API (supports S3)
   * and converts it to Parquet format. If partition fields are specified,
   * the data will be organized according to those fields.</p>
   *
   * @param seriesId FRED series identifier
   * @param targetPath Target parquet file path
   * @param partitionFields List of partition fields (can be null for no partitioning)
   * @param startYear Start year to read cached data from
   * @param endYear End year to read cached data through
   */
  public void convertSeriesToParquet(String seriesId, String targetPath, List<String> partitionFields,
      int startYear, int endYear)
      throws IOException {
    LOGGER.debug("Converting FRED series {} to parquet for years {}-{}: {}", seriesId, startYear, endYear, targetPath);

    // Read cached data using StorageProvider API (supports S3)
    List<Map<String, Object>> observations = new ArrayList<>();

    // Load data for each year in range using known cache paths
    for (int year = startYear; year <= endYear; year++) {
      String relativePath =
          String.format("source=econ/type=custom_fred/series=%s/%s_%d_%d.json", seriesId, seriesId, year, year);
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);

      if (cacheStorageProvider.exists(fullPath)) {
        try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(fullPath);
             java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
          JsonNode root = MAPPER.readTree(reader);

          // Extract observations from the cached JSON
          JsonNode obsArray = root.get("observations");
          if (obsArray != null && obsArray.isArray()) {
            for (JsonNode obs : obsArray) {
              if (seriesId.equals(obs.get("series_id").asText())) {
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
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to load cached data for FRED series {} year {}: {}", seriesId, year, e.getMessage());
        }
      }
    }

    if (observations.isEmpty()) {
      LOGGER.warn("No cached data found for FRED series {} (years {}-{})", seriesId, startYear, endYear);
      return;
    }

    // Write parquet file
    writeFredIndicatorsParquet(observations, targetPath);

    LOGGER.info("Converted FRED series {} to parquet: {} ({} observations from {}-{})",
        seriesId, targetPath, observations.size(), startYear, endYear);

    // FileSchema's conversion registry automatically tracks this conversion
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
