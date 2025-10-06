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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts BLS economic data to Parquet format.
 * Supports employment statistics, inflation metrics, wage growth, and regional employment data.
 */
public class BlsDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlsDataDownloader.class);
  private static final String BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/";

  private final String apiKey;

  // Rate limiting: BLS enforces requests per second limit
  private static final long MIN_REQUEST_INTERVAL_MS = 1100; // 1.1 seconds between requests (safe margin)
  private static final int MAX_RETRIES = 3;
  private static final long RETRY_DELAY_MS = 2000; // 2 seconds initial retry delay

  // Common BLS series IDs
  public static class Series {
    // Employment Statistics
    public static final String UNEMPLOYMENT_RATE = "LNS14000000";
    public static final String EMPLOYMENT_LEVEL = "CES0000000001";
    public static final String LABOR_FORCE_PARTICIPATION = "LNS11300000";

    // Inflation Metrics
    public static final String CPI_ALL_URBAN = "CUUR0000SA0";
    public static final String CPI_CORE = "CUUR0000SA0L1E";
    public static final String PPI_FINAL_DEMAND = "WPUFD4";

    // Wage Growth
    public static final String AVG_HOURLY_EARNINGS = "CES0500000003";
    public static final String EMPLOYMENT_COST_INDEX = "CIU1010000000000A";

    // Regional Employment (examples)
    public static final String CA_UNEMPLOYMENT = "LASST060000000000003";
    public static final String NY_UNEMPLOYMENT = "LASST360000000000003";
    public static final String TX_UNEMPLOYMENT = "LASST480000000000003";
  }

  public BlsDataDownloader(String apiKey, String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    super(cacheDir, storageProvider);
    this.apiKey = apiKey;
  }

  @Override
  protected long getMinRequestIntervalMs() {
    return MIN_REQUEST_INTERVAL_MS;
  }

  @Override
  protected int getMaxRetries() {
    return MAX_RETRIES;
  }

  @Override
  protected long getRetryDelayMs() {
    return RETRY_DELAY_MS;
  }

  // Removed - using storageProvider.writeParquetFile directly now

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
   * Gets the default start year from environment variables.
   * Falls back to GOVDATA_START_YEAR, then defaults to 5 years ago.
   */
  public static int getDefaultStartYear() {
    // First check for ECON-specific override
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return Integer.parseInt(econStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }

    // Fall back to unified setting
    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return Integer.parseInt(govdataStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }

    // Default to 5 years ago
    return LocalDate.now().getYear() - 5;
  }

  /**
   * Gets the default end year from environment variables.
   * Falls back to GOVDATA_END_YEAR, then defaults to current year.
   */
  public static int getDefaultEndYear() {
    // First check for ECON-specific override
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return Integer.parseInt(econEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }

    // Fall back to unified setting
    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return Integer.parseInt(govdataEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }

    // Default to current year
    return LocalDate.now().getYear();
  }

  /**
   * Downloads all BLS data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    // Download employment statistics
    downloadEmploymentStatistics(startYear, endYear);

    // Download inflation metrics
    downloadInflationMetrics(startYear, endYear);

    // Download wage growth data
    downloadWageGrowth(startYear, endYear);

    // Download regional employment data
    downloadRegionalEmployment(startYear, endYear);
  }

  /**
   * Downloads employment statistics using default date range from environment.
   */
  public File downloadEmploymentStatistics() throws IOException, InterruptedException {
    return downloadEmploymentStatistics(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public File downloadEmploymentStatistics(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately to match FileSchema partitioning expectations
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=indicators/year=" + year;
      String jsonFilePath = outputDirPath + "/employment_statistics.json";

      Map<String, String> cacheParams = new HashMap<>();
      cacheParams.put("type", "employment_statistics");
      cacheParams.put("year", String.valueOf(year));

      // Check cache using base class helper
      if (isCachedOrExists("employment_statistics", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached employment statistics for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

    // Download key employment series
    List<String> seriesIds =
        List.of(Series.UNEMPLOYMENT_RATE,
        Series.EMPLOYMENT_LEVEL,
        Series.LABOR_FORCE_PARTICIPATION);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("employment_statistics", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  /**
   * Downloads inflation metrics using default date range from environment.
   */
  public File downloadInflationMetrics() throws IOException, InterruptedException {
    return downloadInflationMetrics(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public File downloadInflationMetrics(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=indicators/year=" + year;
      String jsonFilePath = outputDirPath + "/inflation_metrics.json";

      Map<String, String> cacheParams = new HashMap<>();
      cacheParams.put("type", "inflation_metrics");
      cacheParams.put("year", String.valueOf(year));

      // Check cache using base class helper
      if (isCachedOrExists("inflation_metrics", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached inflation metrics for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

    List<String> seriesIds =
        List.of(Series.CPI_ALL_URBAN,
        Series.CPI_CORE,
        Series.PPI_FINAL_DEMAND);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("inflation_metrics", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  /**
   * Downloads wage growth using default date range from environment.
   */
  public File downloadWageGrowth() throws IOException, InterruptedException {
    return downloadWageGrowth(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads wage growth data and converts to Parquet.
   */
  public File downloadWageGrowth(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = "source=econ/type=indicators/year=" + year + "/wage_growth.json";

      Map<String, String> cacheParams = new HashMap<>();
      cacheParams.put("type", "wage_growth");
      cacheParams.put("year", String.valueOf(year));

      // Check cache using base class helper
      if (isCachedOrExists("wage_growth", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached wage growth data for year {} - skipping download", year);
        lastFile = new File(relativePath);
        continue;
      }

    List<String> seriesIds =
        List.of(Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("wage_growth", year, cacheParams, relativePath, rawJson);
      lastFile = new File(relativePath);
    }

    return lastFile;
  }

  /**
   * Downloads regional employment using default date range from environment.
   */
  public File downloadRegionalEmployment() throws IOException, InterruptedException {
    return downloadRegionalEmployment(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads regional employment data for selected states.
   */
  public File downloadRegionalEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = "source=econ/type=regional/year=" + year + "/regional_employment.json";

      Map<String, String> cacheParams = new HashMap<>();
      cacheParams.put("type", "regional_employment");
      cacheParams.put("year", String.valueOf(year));

      // Check cache using base class helper
      if (isCachedOrExists("regional_employment", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached regional employment data for year {} - skipping download", year);
        lastFile = new File(relativePath);
        continue;
      }

    // Download data for major states
    List<String> seriesIds =
        List.of(Series.CA_UNEMPLOYMENT,
        Series.NY_UNEMPLOYMENT,
        Series.TX_UNEMPLOYMENT);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("regional_employment", year, cacheParams, relativePath, rawJson);
      lastFile = new File(relativePath);
    }

    return lastFile;
  }

  /**
   * Fetches raw JSON response for multiple BLS series in a single API call.
   * Implements rate limiting and retry logic for 429 (rate limit) errors.
   */
  private String fetchMultipleSeriesRaw(
      List<String> seriesIds, int startYear, int endYear) throws IOException, InterruptedException {

    ObjectNode requestBody = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();
    seriesIds.forEach(seriesArray::add);
    requestBody.set("seriesid", seriesArray);
    requestBody.put("startyear", String.valueOf(startYear));
    requestBody.put("endyear", String.valueOf(endYear));

    if (apiKey != null && !apiKey.isEmpty()) {
      requestBody.put("registrationkey", apiKey);
    }

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(BLS_API_BASE + "timeseries/data/"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
        .timeout(Duration.ofSeconds(30))
        .build();

    // Retry loop with exponential backoff
    int attempt = 0;
    while (attempt < MAX_RETRIES) {
      // Rate limiting: ensure minimum interval between requests
      synchronized (this) {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastRequestTime;
        if (timeSinceLastRequest < MIN_REQUEST_INTERVAL_MS) {
          long sleepTime = MIN_REQUEST_INTERVAL_MS - timeSinceLastRequest;
          LOGGER.debug("Rate limiting: sleeping {}ms before BLS API request", sleepTime);
          Thread.sleep(sleepTime);
        }
        lastRequestTime = System.currentTimeMillis();
      }

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        return response.body();
      } else if (response.statusCode() == 429) {
        // Rate limit exceeded - retry with exponential backoff
        attempt++;
        if (attempt >= MAX_RETRIES) {
          throw new IOException("BLS API rate limit exceeded after " + MAX_RETRIES + " retries. " +
              "Response: " + response.body());
        }
        long backoffDelay = RETRY_DELAY_MS * (1L << (attempt - 1)); // Exponential: 2s, 4s, 8s
        LOGGER.warn("BLS API rate limit hit (429). Retry {}/{} after {}ms delay",
            attempt, MAX_RETRIES, backoffDelay);
        Thread.sleep(backoffDelay);
      } else {
        // Other error - don't retry
        throw new IOException("BLS API request failed with status: " + response.statusCode() +
            " - Response: " + response.body());
      }
    }

    throw new IOException("BLS API request failed after " + MAX_RETRIES + " retries");
  }

  /**
   * Fetches multiple BLS series in a single API call.
   */
  private Map<String, List<Map<String, Object>>> fetchMultipleSeries(
      List<String> seriesIds, int startYear, int endYear) throws IOException, InterruptedException {

    String rawJson = fetchMultipleSeriesRaw(seriesIds, startYear, endYear);
    return parseMultiSeriesResponse(rawJson);
  }

  /**
   * Parses BLS API response for multiple series.
   */
  private Map<String, List<Map<String, Object>>> parseMultiSeriesResponse(String jsonResponse)
      throws IOException {

    Map<String, List<Map<String, Object>>> result = new HashMap<>();
    JsonNode root = MAPPER.readTree(jsonResponse);

    String status = root.path("status").asText();
    if (!"REQUEST_SUCCEEDED".equals(status)) {
      String message = root.path("message").asText("Unknown error");
      throw new IOException("BLS API error: " + message);
    }

    JsonNode seriesArray = root.path("Results").path("series");
    if (seriesArray.isArray()) {
      for (JsonNode series : seriesArray) {
        String seriesId = series.path("seriesID").asText();
        List<Map<String, Object>> data = new ArrayList<>();

        JsonNode dataArray = series.path("data");
        if (dataArray.isArray()) {
          for (JsonNode point : dataArray) {
            Map<String, Object> dataPoint = new HashMap<>();
            String year = point.path("year").asText();
            String period = point.path("period").asText();
            String value = point.path("value").asText();

            // Convert period to month
            int month = periodToMonth(period);
            if (month > 0) {
              dataPoint.put("date", LocalDate.of(Integer.parseInt(year), month, 1).toString());
              dataPoint.put("value", Double.parseDouble(value));
              dataPoint.put("series_id", seriesId);
              dataPoint.put("series_name", getSeriesName(seriesId));
              data.add(dataPoint);
            }
          }
        }

        result.put(seriesId, data);
      }
    }

    return result;
  }

  /**
   * Converts BLS period code to month.
   */
  private int periodToMonth(String period) {
    if (period.startsWith("M")) {
      return Integer.parseInt(period.substring(1));
    }
    return 0; // Annual or other non-monthly data
  }

  /**
   * Gets human-readable name for series ID.
   */
  private String getSeriesName(String seriesId) {
    switch (seriesId) {
      case Series.UNEMPLOYMENT_RATE: return "Unemployment Rate";
      case Series.EMPLOYMENT_LEVEL: return "Total Nonfarm Employment";
      case Series.LABOR_FORCE_PARTICIPATION: return "Labor Force Participation Rate";
      case Series.CPI_ALL_URBAN: return "CPI-U All Items";
      case Series.CPI_CORE: return "CPI-U Core (Less Food and Energy)";
      case Series.PPI_FINAL_DEMAND: return "PPI Final Demand";
      case Series.AVG_HOURLY_EARNINGS: return "Average Hourly Earnings";
      case Series.EMPLOYMENT_COST_INDEX: return "Employment Cost Index";
      case Series.CA_UNEMPLOYMENT: return "California Unemployment Rate";
      case Series.NY_UNEMPLOYMENT: return "New York Unemployment Rate";
      case Series.TX_UNEMPLOYMENT: return "Texas Unemployment Rate";
      default: return seriesId;
    }
  }

  /**
   * Writes employment statistics data to Parquet.
   */
  private void writeEmploymentStatisticsParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("employment_statistics")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier (e.g., 'LNS14000000' for unemployment rate)").type().stringType().noDefault()
        .name("series_name").doc("Descriptive name of employment series").type().stringType().noDefault()
        .name("value").doc("Employment statistic value (e.g., unemployment rate as percentage)").type().doubleType().noDefault()
        .name("unit").doc("Unit of measurement (e.g., 'Percent', 'Thousands of Persons')").type().nullable().stringType().noDefault()
        .name("seasonally_adjusted").doc("Whether data is seasonally adjusted").type().nullable().booleanType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .name("category").doc("Employment category (e.g., 'Labor Force', 'Employment Level')").type().nullable().stringType().noDefault()
        .name("subcategory").doc("Employment subcategory for detailed classification").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date") != null ? dataPoint.get("date") : "");
        record.put("series_id", dataPoint.get("series_id") != null ? dataPoint.get("series_id") : entry.getKey());
        record.put("series_name", dataPoint.get("series_name") != null ? dataPoint.get("series_name") : getSeriesName(entry.getKey()));
        record.put("value", dataPoint.get("value") != null ? dataPoint.get("value") : 0.0);
        record.put("unit", getUnit(entry.getKey()));
        record.put("seasonally_adjusted", isSeasonallyAdjusted(entry.getKey()));
        record.put("category", "Employment");
        record.put("subcategory", getSubcategory(entry.getKey()));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  /**
   * Writes inflation metrics data to Parquet.
   */
  private void writeInflationMetricsParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("inflation_metrics")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("index_type").doc("Price index type (CPI=Consumer Price Index, PPI=Producer Price Index)").type().stringType().noDefault()
        .name("item_code").doc("BLS item code identifying specific goods/services category").type().stringType().noDefault()
        .name("area_code").doc("BLS area code (e.g., 'U'=U.S. city average, regional codes)").type().stringType().noDefault()
        .name("item_name").doc("Description of goods/services category (e.g., 'Food', 'Energy', 'All items')").type().stringType().noDefault()
        .name("index_value").doc("Price index value (typically base period = 100)").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change in price index").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change in price index").type().nullable().doubleType().noDefault()
        .name("area_name").doc("Geographic area name (e.g., 'U.S. City Average', 'Los Angeles')").type().nullable().stringType().noDefault()
        .name("seasonally_adjusted").doc("Whether price index is seasonally adjusted").type().nullable().booleanType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date") != null ? dataPoint.get("date") : "");
        record.put("index_type", getIndexType(seriesId));
        record.put("item_code", getItemCode(seriesId));
        record.put("area_code", "0000");  // National
        record.put("item_name", dataPoint.get("series_name") != null ? dataPoint.get("series_name") : getSeriesName(seriesId));
        record.put("index_value", dataPoint.get("value") != null ? dataPoint.get("value") : 0.0);
        record.put("area_name", "U.S. city average");
        record.put("seasonally_adjusted", isSeasonallyAdjusted(seriesId));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  /**
   * Writes wage growth data to Parquet.
   */
  private void writeWageGrowthParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("wage_growth")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier for wage/earnings data").type().stringType().noDefault()
        .name("industry_code").doc("NAICS industry code (e.g., '00'=All industries)").type().stringType().noDefault()
        .name("occupation_code").doc("SOC occupation code (e.g., '000000'=All occupations)").type().stringType().noDefault()
        .name("industry_name").doc("Industry name or sector description").type().nullable().stringType().noDefault()
        .name("occupation_name").doc("Occupation title or job category").type().nullable().stringType().noDefault()
        .name("average_hourly_earnings").doc("Average hourly earnings in dollars").type().nullable().doubleType().noDefault()
        .name("average_weekly_earnings").doc("Average weekly earnings in dollars").type().nullable().doubleType().noDefault()
        .name("employment_cost_index").doc("Employment Cost Index measuring compensation costs").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change in wages/earnings").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("industry_code", "00");  // All industries
        record.put("occupation_code", "000000");  // All occupations
        record.put("industry_name", "All Industries");
        record.put("occupation_name", "All Occupations");

        if (seriesId.equals(Series.AVG_HOURLY_EARNINGS)) {
          record.put("average_hourly_earnings", dataPoint.get("value"));
        } else if (seriesId.equals(Series.EMPLOYMENT_COST_INDEX)) {
          record.put("employment_cost_index", dataPoint.get("value"));
        }

        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  /**
   * Writes regional employment data to Parquet.
   */
  private void writeRegionalEmploymentParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("regional_employment")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("area_code").doc("BLS area code identifying geographic region").type().stringType().noDefault()
        .name("area_name").doc("Geographic area name (state, metropolitan area, or county)").type().stringType().noDefault()
        .name("area_type").doc("Type of geographic area (state, metro, county)").type().stringType().noDefault()
        .name("state_code").doc("2-letter state postal code (e.g., 'CA', 'NY')").type().nullable().stringType().noDefault()
        .name("unemployment_rate").doc("Unemployment rate as percentage of labor force").type().nullable().doubleType().noDefault()
        .name("employment_level").doc("Total employed persons in thousands").type().nullable().longType().noDefault()
        .name("labor_force").doc("Total civilian labor force in thousands").type().nullable().longType().noDefault()
        .name("participation_rate").doc("Labor force participation rate as percentage of population").type().nullable().doubleType().noDefault()
        .name("employment_population_ratio").doc("Employment-to-population ratio as percentage").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      String stateCode = getStateCode(seriesId);

      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("area_code", stateCode);
        record.put("area_name", getStateName(stateCode));
        record.put("area_type", "state");
        record.put("state_code", stateCode);
        record.put("unemployment_rate", dataPoint.get("value"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private String getUnit(String seriesId) {
    if (seriesId.contains("RATE") || seriesId.contains("000003")) {
      return "percent";
    }
    return "index";
  }

  private boolean isSeasonallyAdjusted(String seriesId) {
    return !seriesId.contains("NSA");
  }

  private String getSubcategory(String seriesId) {
    if (seriesId.startsWith("LNS")) return "Labor Force Statistics";
    if (seriesId.startsWith("CES")) return "Current Employment Statistics";
    return "General";
  }

  private String getIndexType(String seriesId) {
    if (seriesId.startsWith("CUU")) return "CPI-U";
    if (seriesId.startsWith("WPU")) return "PPI";
    return "Other";
  }

  private String getItemCode(String seriesId) {
    if (seriesId.contains("SA0L1E")) return "Core";
    if (seriesId.contains("SA0")) return "All Items";
    return "Other";
  }

  private String getStateCode(String seriesId) {
    // LASST series: LASST + state FIPS code (2 digits) + ...
    // Extract state FIPS code from positions 5-6 (0-indexed)
    if (seriesId.startsWith("LASST") && seriesId.length() > 6) {
      String fips = seriesId.substring(5, 7);
      switch (fips) {
        case "01": return "AL";
        case "02": return "AK";
        case "04": return "AZ";
        case "05": return "AR";
        case "06": return "CA";
        case "08": return "CO";
        case "09": return "CT";
        case "10": return "DE";
        case "11": return "DC";
        case "12": return "FL";
        case "13": return "GA";
        case "15": return "HI";
        case "16": return "ID";
        case "17": return "IL";
        case "18": return "IN";
        case "19": return "IA";
        case "20": return "KS";
        case "21": return "KY";
        case "22": return "LA";
        case "23": return "ME";
        case "24": return "MD";
        case "25": return "MA";
        case "26": return "MI";
        case "27": return "MN";
        case "28": return "MS";
        case "29": return "MO";
        case "30": return "MT";
        case "31": return "NE";
        case "32": return "NV";
        case "33": return "NH";
        case "34": return "NJ";
        case "35": return "NM";
        case "36": return "NY";
        case "37": return "NC";
        case "38": return "ND";
        case "39": return "OH";
        case "40": return "OK";
        case "41": return "OR";
        case "42": return "PA";
        case "44": return "RI";
        case "45": return "SC";
        case "46": return "SD";
        case "47": return "TN";
        case "48": return "TX";
        case "49": return "UT";
        case "50": return "VT";
        case "51": return "VA";
        case "53": return "WA";
        case "54": return "WV";
        case "55": return "WI";
        case "56": return "WY";
        default: return "US";
      }
    }
    return "US";
  }

  private String getStateName(String stateCode) {
    switch (stateCode) {
      case "AL": return "Alabama";
      case "AK": return "Alaska";
      case "AZ": return "Arizona";
      case "AR": return "Arkansas";
      case "CA": return "California";
      case "CO": return "Colorado";
      case "CT": return "Connecticut";
      case "DE": return "Delaware";
      case "DC": return "District of Columbia";
      case "FL": return "Florida";
      case "GA": return "Georgia";
      case "HI": return "Hawaii";
      case "ID": return "Idaho";
      case "IL": return "Illinois";
      case "IN": return "Indiana";
      case "IA": return "Iowa";
      case "KS": return "Kansas";
      case "KY": return "Kentucky";
      case "LA": return "Louisiana";
      case "ME": return "Maine";
      case "MD": return "Maryland";
      case "MA": return "Massachusetts";
      case "MI": return "Michigan";
      case "MN": return "Minnesota";
      case "MS": return "Mississippi";
      case "MO": return "Missouri";
      case "MT": return "Montana";
      case "NE": return "Nebraska";
      case "NV": return "Nevada";
      case "NH": return "New Hampshire";
      case "NJ": return "New Jersey";
      case "NM": return "New Mexico";
      case "NY": return "New York";
      case "NC": return "North Carolina";
      case "ND": return "North Dakota";
      case "OH": return "Ohio";
      case "OK": return "Oklahoma";
      case "OR": return "Oregon";
      case "PA": return "Pennsylvania";
      case "RI": return "Rhode Island";
      case "SC": return "South Carolina";
      case "SD": return "South Dakota";
      case "TN": return "Tennessee";
      case "TX": return "Texas";
      case "UT": return "Utah";
      case "VT": return "Vermont";
      case "VA": return "Virginia";
      case "WA": return "Washington";
      case "WV": return "West Virginia";
      case "WI": return "Wisconsin";
      case "WY": return "Wyoming";
      default: return "United States";
    }
  }


  /**
   * Converts cached BLS employment data to Parquet format.
   */
  public void convertToParquet(File sourceDir, String targetFilePath) throws IOException {
    // Check if already converted using base class helper
    if (isParquetConverted(targetFilePath)) {
      return;
    }

    LOGGER.debug("Converting BLS data from {} to parquet: {}", sourceDir, targetFilePath);

    // Extract data type from filename
    String fileName = targetFilePath.substring(targetFilePath.lastIndexOf('/') + 1);

    // Read employment statistics JSON files and convert to employment_statistics.parquet
    if (fileName.equals("employment_statistics.parquet")) {
      convertEmploymentStatisticsToParquet(sourceDir, targetFilePath);
    } else if (fileName.equals("inflation_metrics.parquet")) {
      convertInflationMetricsToParquet(sourceDir, targetFilePath);
    } else if (fileName.equals("wage_growth.parquet")) {
      convertWageGrowthToParquet(sourceDir, targetFilePath);
    } else if (fileName.equals("regional_employment.parquet")) {
      convertRegionalEmploymentToParquet(sourceDir, targetFilePath);
    }

    // Mark parquet conversion complete using base class helper
    markParquetConverted(targetFilePath);
  }

  private void convertEmploymentStatisticsToParquet(File sourceDir, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Look for employment statistics JSON files
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("employment_statistics.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);

          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();

              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" +
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS employment JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }

    // Write to parquet
    writeEmploymentStatisticsParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS employment data to parquet: {}", targetPath);
  }

  private void convertInflationMetricsToParquet(File sourceDir, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Look for inflation metrics JSON files
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("inflation_metrics.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);

          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();

              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" +
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS inflation JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }

    // Write to parquet using inflation metrics schema
    writeInflationMetricsParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS inflation data to parquet: {}", targetPath);
  }

  private void convertWageGrowthToParquet(File sourceDir, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Look for wage growth JSON files
    File[] jsonFiles = sourceDir.listFiles((dir, name) -> name.equals("wage_growth.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);

          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();

              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" +
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS wage growth JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }

    // Write to parquet using wage growth schema
    writeWageGrowthParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS wage growth data to parquet: {}", targetPath);
  }

  private void convertRegionalEmploymentToParquet(File sourceDir, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Regional employment data is in type=regional subdirectory, not type=indicators
    // Adjust path to look in the regional directory
    File regionalDir =
        new File(sourceDir.getParentFile().getParentFile(), "type=regional/year=" + sourceDir.getName().replace("year=", ""));

    // Look for regional employment JSON files
    File[] jsonFiles = regionalDir.listFiles((dir, name) -> name.equals("regional_employment.json"));
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        try {
          String content = Files.readString(jsonFile.toPath(), StandardCharsets.UTF_8);
          JsonNode root = MAPPER.readTree(content);

          if (root.has("Results") && root.get("Results").has("series")) {
            JsonNode series = root.get("Results").get("series");
            for (JsonNode seriesNode : series) {
              String seriesId = seriesNode.get("seriesID").asText();
              List<Map<String, Object>> dataPoints = new ArrayList<>();

              if (seriesNode.has("data")) {
                for (JsonNode dataNode : seriesNode.get("data")) {
                  Map<String, Object> dataPoint = new HashMap<>();
                  dataPoint.put("date", dataNode.get("year").asText() + "-" +
                    String.format("%02d", periodToMonth(dataNode.get("period").asText())) + "-01");
                  dataPoint.put("value", dataNode.get("value").asDouble());
                  dataPoints.add(dataPoint);
                }
              }
              seriesData.put(seriesId, dataPoints);
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error reading BLS regional employment JSON file {}: {}", jsonFile, e.getMessage());
        }
      }
    }

    // Write to parquet using regional employment schema
    LOGGER.info("Converting regional employment data with {} series to parquet", seriesData.size());
    writeRegionalEmploymentParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS regional employment data to parquet: {}", targetPath);
  }

  /**
   * Convert Consumer Price Index data to Parquet format.
   */
  public void convertConsumerPriceIndexToParquet(File sourceDir, String targetFilePath) throws IOException {
    File jsonFile = new File(sourceDir, "inflation_metrics.json");
    if (!jsonFile.exists()) {
      LOGGER.warn("No inflation_metrics.json found in {}", sourceDir);
      return;
    }

    Schema schema = SchemaBuilder.record("ConsumerPriceIndex")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("index_type").doc("CPI series type (CPI-U=Urban Consumers, CPI-W=Urban Wage Earners)").type().stringType().noDefault()
        .name("item_code").doc("BLS item code for goods/services category").type().stringType().noDefault()
        .name("area_code").doc("BLS area code for geographic region").type().stringType().noDefault()
        .name("value").doc("Consumer price index value (base period = 100)").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change (inflation rate)").type().nullable().doubleType().noDefault()
        .name("seasonally_adjusted").doc("Seasonal adjustment status ('SA'=Seasonally Adjusted, 'NSA'=Not Seasonally Adjusted)").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    Map<String, Object> data = MAPPER.readValue(jsonFile, Map.class);
    List<Map<String, Object>> metrics = (List<Map<String, Object>>) data.get("inflation_metrics");

    if (metrics != null) {
      for (Map<String, Object> metric : metrics) {
        String seriesId = (String) metric.get("series_id");
        // Only include CPI series (starts with CU)
        if (seriesId != null && seriesId.startsWith("CU")) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("date", metric.get("date"));
          record.put("index_type", seriesId.startsWith("CUUR") ? "CPI-U" : "CPI-W");
          record.put("item_code", seriesId.length() > 9 ? seriesId.substring(9) : "SA0");
          record.put("area_code", seriesId.length() > 6 ? seriesId.substring(4, 8) : "0000");
          record.put("value", ((Number) metric.get("value")).doubleValue());
          record.put("percent_change_month", metric.get("percent_change_month"));
          record.put("percent_change_year", metric.get("percent_change_year"));
          record.put("seasonally_adjusted", metric.getOrDefault("seasonally_adjusted", "N"));
          records.add(record);
        }
      }
    }

    storageProvider.writeAvroParquet(targetFilePath, schema, records, "ConsumerPriceIndex");
    LOGGER.info("Converted {} CPI records to parquet: {}", records.size(), targetFilePath);
  }
}
