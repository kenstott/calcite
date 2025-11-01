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

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts World Bank economic data to Parquet format.
 * Provides international economic indicators for comparison with U.S. data.
 *
 * <p>Uses the World Bank API which requires no authentication.
 */
public class WorldBankDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorldBankDataDownloader.class);
  private static final String WORLD_BANK_API_BASE = "https://api.worldbank.org/v2/";

  // Key economic indicators to download
  public static class Indicators {
    public static final String GDP_CURRENT_USD = "NY.GDP.MKTP.CD";  // GDP (current US$)
    public static final String GDP_GROWTH = "NY.GDP.MKTP.KD.ZG";    // GDP growth (annual %)
    public static final String GDP_PER_CAPITA = "NY.GDP.PCAP.CD";   // GDP per capita (current US$)
    public static final String INFLATION_CPI = "FP.CPI.TOTL.ZG";    // Inflation, consumer prices (annual %)
    public static final String UNEMPLOYMENT = "SL.UEM.TOTL.ZS";      // Unemployment, total (% of labor force)
    public static final String POPULATION = "SP.POP.TOTL";           // Population, total
    public static final String TRADE_BALANCE = "NE.RSB.GNFS.ZS";    // External balance on goods and services (% of GDP)
    public static final String GOVT_DEBT = "GC.DOD.TOTL.GD.ZS";      // Central government debt, total (% of GDP)
    public static final String INTEREST_RATE = "FR.INR.RINR";        // Real interest rate (%)
    public static final String EXCHANGE_RATE = "PA.NUS.FCRF";        // Official exchange rate (LCU per US$)
  }

  // Focus on major economies for comparison
  public static class Countries {
    public static final List<String> G7 =
        Arrays.asList("USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN");

    public static final List<String> G20 =
        Arrays.asList("USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "BRA", "CAN",
        "KOR", "RUS", "AUS", "MEX", "IDN", "TUR", "SAU", "ARG", "ZAF");

    public static final List<String> MAJOR_ECONOMIES =
        Arrays.asList("USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "BRA", "ITA", "CAN",
        "KOR", "ESP", "AUS", "RUS", "MEX", "IDN", "NLD", "TUR", "CHE", "POL");
  }

  public WorldBankDataDownloader(String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public WorldBankDataDownloader(String cacheDir, String operatingDirectory, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, cacheDir, cacheStorageProvider, storageProvider, sharedManifest);
  }

  @Override protected long getMinRequestIntervalMs() {
    return 0; // World Bank API has no strict rate limit
  }

  @Override protected int getMaxRetries() {
    return 3;
  }

  @Override protected long getRetryDelayMs() {
    return 2000; // 2 seconds
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
   * Gets the default start year from environment variables.
   */
  public static int getDefaultStartYear() {
    String econStart = System.getenv("ECON_START_YEAR");
    if (econStart != null) {
      try {
        return Integer.parseInt(econStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_START_YEAR: {}", econStart);
      }
    }

    String govdataStart = System.getenv("GOVDATA_START_YEAR");
    if (govdataStart != null) {
      try {
        return Integer.parseInt(govdataStart);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_START_YEAR: {}", govdataStart);
      }
    }

    return LocalDate.now().getYear() - 5;
  }

  /**
   * Gets the default end year from environment variables.
   */
  public static int getDefaultEndYear() {
    String econEnd = System.getenv("ECON_END_YEAR");
    if (econEnd != null) {
      try {
        return Integer.parseInt(econEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid ECON_END_YEAR: {}", econEnd);
      }
    }

    String govdataEnd = System.getenv("GOVDATA_END_YEAR");
    if (govdataEnd != null) {
      try {
        return Integer.parseInt(govdataEnd);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid GOVDATA_END_YEAR: {}", govdataEnd);
      }
    }

    return LocalDate.now().getYear();
  }

  /**
   * Downloads all World Bank data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading World Bank data for years {} to {}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      downloadWorldIndicatorsForYear(year);
    }
  }

  /**
   * Downloads world economic indicators for a specific year.
   */
  public void downloadWorldIndicatorsForYear(int year) throws IOException, InterruptedException {
    LOGGER.info("Downloading world economic indicators for year {}", year);

    // Build relative path for JSON file - directories created automatically by StorageProvider
    String relativePath = buildPartitionPath("indicators", DataFrequency.ANNUAL, year) + "/world_indicators.json";

    List<Map<String, Object>> indicators = new ArrayList<>();

    // Download key indicators for major economies
    List<String> indicatorCodes =
        Arrays.asList(Indicators.GDP_CURRENT_USD,
        Indicators.GDP_GROWTH,
        Indicators.GDP_PER_CAPITA,
        Indicators.INFLATION_CPI,
        Indicators.UNEMPLOYMENT,
        Indicators.POPULATION,
        Indicators.GOVT_DEBT);

    // Use G20 countries for broader coverage
    String countriesParam = String.join(";", Countries.G20);

    for (String indicatorCode : indicatorCodes) {
      LOGGER.info("Fetching indicator: {} for year {}", indicatorCode, year);

      String url =
          String.format("%scountry/%s/indicator/%s?format=json&date=%d&per_page=1000", WORLD_BANK_API_BASE, countriesParam, indicatorCode, year);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("World Bank API request failed for indicator {} year {} with status: {}",
            indicatorCode, year, response.statusCode());
        continue;
      }

      // World Bank API returns array with metadata in first element, data in second
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue; // Skip null values
            }

            Map<String, Object> indicator = new HashMap<>();
            JsonNode countryNode = record.get("country");
            if (countryNode != null) {
              indicator.put("country_code", countryNode.get("id") != null ? countryNode.get("id").asText() : "");
              indicator.put("country_name", countryNode.get("value") != null ? countryNode.get("value").asText() : "");
            } else {
              indicator.put("country_code", "");
              indicator.put("country_name", "");
            }
            JsonNode indicatorNode = record.get("indicator");
            if (indicatorNode != null) {
              indicator.put("indicator_code", indicatorNode.get("id") != null ? indicatorNode.get("id").asText() : "");
              indicator.put("indicator_name", indicatorNode.get("value") != null ? indicatorNode.get("value").asText() : "");
            } else {
              indicator.put("indicator_code", "");
              indicator.put("indicator_name", "");
            }

            indicator.put("year", record.get("date") != null ? record.get("date").asInt() : 0);
            indicator.put("value", record.get("value") != null ? record.get("value").asDouble() : 0.0);
            indicator.put("unit", record.get("unit") != null ? record.get("unit").asText("") : "");
            indicator.put("scale", record.get("scale") != null ? record.get("scale").asText("") : "");

            indicators.add(indicator);
          }
        }
      }

      // Small delay to be respectful to the API
      Thread.sleep(100);
    }

    // Save raw JSON data to cache using StorageProvider
    Map<String, Object> data = new HashMap<>();
    data.put("indicators", indicators);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve relative path against cache directory before writing
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    LOGGER.info("Writing world indicators to: {}", fullPath);
    cacheStorageProvider.writeFile(fullPath, jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    LOGGER.info("World indicators saved to: {} ({} records)", fullPath, indicators.size());
  }

  /**
   * Downloads world economic indicators using default date range.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadWorldIndicators() throws IOException, InterruptedException {
    return downloadWorldIndicators(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads world economic indicators for major economies.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadWorldIndicators(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading world economic indicators for {}-{}", startYear, endYear);

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("source=econ/type=world_indicators/year_range=%d_%d/world_indicators.parquet", startYear, endYear);

    // Check cache manifest first
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("start_year", String.valueOf(startYear));
    cacheParams.put("end_year", String.valueOf(endYear));

    if (cacheManifest.isCached("world_indicators", startYear, cacheParams)) {
      LOGGER.info("Found cached world indicators for {}-{} - skipping download", startYear, endYear);
      return relativePath;
    }

    // Check if file exists but not in manifest - update manifest
    try {
      if (storageProvider.exists(relativePath)) {
        LOGGER.info("Found existing world indicators file for {}-{} - updating manifest", startYear, endYear);
        cacheManifest.markCached("world_indicators", startYear, cacheParams, relativePath, 0L);
        cacheManifest.save(operatingDirectory);
        return relativePath;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not check if file exists: {}", e.getMessage());
    }

    List<WorldIndicator> indicators = new ArrayList<>();

    // Download key indicators for major economies
    List<String> indicatorCodes =
        Arrays.asList(Indicators.GDP_CURRENT_USD,
        Indicators.GDP_GROWTH,
        Indicators.GDP_PER_CAPITA,
        Indicators.INFLATION_CPI,
        Indicators.UNEMPLOYMENT,
        Indicators.POPULATION,
        Indicators.GOVT_DEBT);

    // Use G20 countries for broader coverage
    String countriesParam = String.join(";", Countries.G20);

    for (String indicatorCode : indicatorCodes) {
      LOGGER.info("Fetching indicator: {}", indicatorCode);

      String url =
          String.format("%scountry/%s/indicator/%s?format=json&date=%d:%d&per_page=10000", WORLD_BANK_API_BASE, countriesParam, indicatorCode, startYear, endYear);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("World Bank API request failed for indicator {} with status: {}",
            indicatorCode, response.statusCode());
        continue;
      }

      // World Bank API returns array with metadata in first element, data in second
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue; // Skip null values
            }

            WorldIndicator indicator = new WorldIndicator();
            indicator.countryCode = record.get("country").get("id").asText();
            indicator.countryName = record.get("country").get("value").asText();
            indicator.indicatorCode = record.get("indicator").get("id").asText();
            indicator.indicatorName = record.get("indicator").get("value").asText();
            indicator.year = record.get("date").asInt();
            indicator.value = record.get("value").asDouble();
            indicator.unit = record.get("unit").asText("");
            indicator.scale = record.get("scale").asText("");

            indicators.add(indicator);
          }
        }
      }

      // Small delay to be respectful to the API
      Thread.sleep(100);
    }

    // Convert to Parquet
    writeWorldIndicatorsParquet(indicators, relativePath);

    // Mark as cached in manifest
    cacheManifest.markCached("world_indicators", startYear, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);

    LOGGER.info("World indicators saved to: {} ({} records)", relativePath, indicators.size());
    return relativePath;
  }

  /**
   * Downloads comparative GDP data for all countries.
   */
  public File downloadGlobalGDP() throws IOException, InterruptedException {
    return downloadGlobalGDP(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads GDP data for all countries for global comparison.
   */
  public File downloadGlobalGDP(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading global GDP data for {}-{}", startYear, endYear);

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("source=econ/type=global_gdp/year_range=%d_%d/global_gdp.parquet", startYear, endYear);

    // Check cache manifest first
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("start_year", String.valueOf(startYear));
    cacheParams.put("end_year", String.valueOf(endYear));

    // For return value compatibility - create dummy File
    File parquetFile = new File(relativePath);

    if (cacheManifest.isCached("global_gdp", startYear, cacheParams)) {
      LOGGER.info("Found cached global GDP for {}-{} - skipping download", startYear, endYear);
      return parquetFile;
    }

    // Check if file exists but not in manifest - update manifest
    try {
      if (storageProvider.exists(relativePath)) {
        LOGGER.info("Found existing global GDP file for {}-{} - updating manifest", startYear, endYear);
        cacheManifest.markCached("global_gdp", startYear, cacheParams, relativePath, 0L);
        cacheManifest.save(operatingDirectory);
        return parquetFile;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not check if file exists: {}", e.getMessage());
    }

    List<WorldIndicator> gdpData = new ArrayList<>();

    // Download GDP data for all countries
    String url =
        String.format("%scountry/all/indicator/%s?format=json&date=%d:%d&per_page=20000", WORLD_BANK_API_BASE, Indicators.GDP_CURRENT_USD, startYear, endYear);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      JsonNode root = MAPPER.readTree(response.body());
      if (root.isArray() && root.size() > 1) {
        JsonNode data = root.get(1);
        if (data != null && data.isArray()) {
          for (JsonNode record : data) {
            if (record.get("value").isNull()) {
              continue;
            }

            WorldIndicator gdp = new WorldIndicator();
            gdp.countryCode = record.get("country").get("id").asText();
            gdp.countryName = record.get("country").get("value").asText();
            gdp.indicatorCode = Indicators.GDP_CURRENT_USD;
            gdp.indicatorName = "GDP (current US$)";
            gdp.year = record.get("date").asInt();
            gdp.value = record.get("value").asDouble();
            gdp.unit = "USD";
            gdp.scale = "1";

            gdpData.add(gdp);
          }
        }
      }
    }

    // Convert to Parquet
    writeWorldIndicatorsParquet(gdpData, relativePath);

    // Mark as cached in manifest
    cacheManifest.markCached("global_gdp", startYear, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Global GDP data saved to: {} ({} records)", relativePath, gdpData.size());
    return parquetFile;
  }

  private void writeWorldIndicatorsParquet(List<WorldIndicator> indicators, String targetPath) throws IOException {
    // Build data records (keep all transformation logic)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (WorldIndicator indicator : indicators) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("country_code", indicator.countryCode);
      record.put("country_name", indicator.countryName);
      record.put("indicator_code", indicator.indicatorCode);
      record.put("indicator_name", indicator.indicatorName);
      // year comes from partition key (year=*/), not parquet file
      record.put("value", indicator.value);
      record.put("unit", indicator.unit);
      record.put("scale", indicator.scale);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("world_indicators");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "WorldIndicator", "WorldIndicator");
  }


  // Data class
  private static class WorldIndicator {
    String countryCode;
    String countryName;
    String indicatorCode;
    String indicatorName;
    int year;
    double value;
    String unit;
    String scale;
  }

  /**
   * Converts cached World Bank data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   *
   * @param sourceDirPath Directory path containing cached World Bank JSON data
   * @param targetFilePath Target parquet file to create
   */
  public void convertToParquet(String sourceDirPath, String targetFilePath) throws IOException {

    LOGGER.info("Converting World Bank data from {} to parquet: {}", sourceDirPath, targetFilePath);

    // Directories are created automatically by StorageProvider when writing files

    List<Map<String, Object>> indicators = new ArrayList<>();

    // Look for World Bank indicators JSON file in the source directory
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "world_indicators.json");
    LOGGER.info("Looking for world indicators JSON at: {}", jsonFilePath);
    if (cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.info("Found world indicators JSON file");
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           InputStreamReader reader = new InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        JsonNode indicatorsArray = root.get("indicators");

        LOGGER.info("Indicators array: {}", indicatorsArray != null ? "found" : "null");
        if (indicatorsArray != null) {
          LOGGER.info("Indicators array is array: {}, size: {}", indicatorsArray.isArray(), indicatorsArray.size());
        }

        if (indicatorsArray != null && indicatorsArray.isArray()) {
          for (JsonNode ind : indicatorsArray) {
            Map<String, Object> indicator = new HashMap<>();
            indicator.put("country_code", ind.get("country_code").asText());
            indicator.put("country_name", ind.get("country_name").asText());
            indicator.put("indicator_code", ind.get("indicator_code").asText());
            indicator.put("indicator_name", ind.get("indicator_name").asText());
            indicator.put("year", ind.get("year").asInt());
            indicator.put("value", ind.get("value").asDouble());
            indicator.put("unit", ind.get("unit").asText(""));
            indicator.put("scale", ind.get("scale").asText(""));

            indicators.add(indicator);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to process World Bank JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      }
    } else {
      LOGGER.warn("World indicators JSON file not found: {}", jsonFilePath);
    }

    // Write parquet file
    writeWorldIndicatorsMapParquet(indicators, targetFilePath);

    LOGGER.info("Converted World Bank data to parquet: {} ({} indicators)", targetFilePath, indicators.size());

    // FileSchema's conversion registry automatically tracks this conversion
  }

  private void writeWorldIndicatorsMapParquet(List<Map<String, Object>> indicators, String targetPath)
      throws IOException {
    // Build data records (keep all transformation logic)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (Map<String, Object> ind : indicators) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("country_code", ind.get("country_code"));
      record.put("country_name", ind.get("country_name"));
      record.put("indicator_code", ind.get("indicator_code"));
      record.put("indicator_name", ind.get("indicator_name"));
      record.put("year", ind.get("year"));
      record.put("value", ind.get("value"));
      record.put("unit", ind.get("unit"));
      record.put("scale", ind.get("scale"));
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("world_indicators");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "WorldIndicator", "WorldIndicator");
  }


}
