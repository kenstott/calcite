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
    public static final String INFLATION_CPI = "FP.CPI.TOTL.ZG";    // Inflation, consumer prices
    // (annual %)
    public static final String UNEMPLOYMENT = "SL.UEM.TOTL.ZS";      // Unemployment, total (% of
    // labor force)
    public static final String POPULATION = "SP.POP.TOTL";           // Population, total
    public static final String TRADE_BALANCE = "NE.RSB.GNFS.ZS";    // External balance on goods
    // and services (% of GDP)
    public static final String GOVT_DEBT = "GC.DOD.TOTL.GD.ZS";      // Central government debt,
    // total (% of GDP)
    public static final String INTEREST_RATE = "FR.INR.RINR";        // Real interest rate (%)
    public static final String EXCHANGE_RATE = "PA.NUS.FCRF";        // Official exchange rate
    // (LCU per US$)
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

  public WorldBankDataDownloader(String cacheDir,
      org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public WorldBankDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider,
      org.apache.calcite.adapter.file.storage.StorageProvider storageProvider,
      CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest);
  }

  @Override protected String getTableName() {
    return "world_indicators";
  }

  /**
   * Downloads all World Bank data for the specified year range.
   */
  @Override public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading World Bank data for years {} to {}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      downloadWorldIndicatorsForYear(year);
    }
  }

  /**
   * Converts all downloaded World Bank data to Parquet format for the specified year range.
   */
  @Override public void convertAll(int startYear, int endYear) throws IOException {
    LOGGER.info("Converting World Bank data for years {}-{}", startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    for (int year = startYear; year <= endYear; year++) {
      Map<String, String> variables = new HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "annual");

      Map<String, Object> metadata = loadTableMetadata("world_indicators");
      String pattern = (String) metadata.get("pattern");
      String parquetPath =
          storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));
      String rawPath =
          cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));

      if (!isParquetConvertedOrExists("world_indicators", year, variables, rawPath, parquetPath)) {
        convertCachedJsonToParquet("world_indicators", variables);
        cacheManifest.markParquetConverted("world_indicators", year, null, parquetPath);
        convertedCount++;
      } else {
        skippedCount++;
      }
    }

    LOGGER.info("World Bank conversion complete: converted {} years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads world economic indicators for a specific year.
   */
  public void downloadWorldIndicatorsForYear(int year) throws IOException, InterruptedException {
    LOGGER.info("Downloading world economic indicators for year {}", year);

    // Use metadata-driven path resolution (BEA pattern)
    Map<String, Object> metadata = loadTableMetadata("world_indicators");
    String pattern = (String) metadata.get("pattern");

    Map<String, String> variables = new HashMap<>();
    variables.put("type", "indicators");
    variables.put("frequency", "annual");
    variables.put("year", String.valueOf(year));

    String relativePath = resolveJsonPath(pattern, variables);

    List<Map<String, Object>> indicators = new ArrayList<>();

    // Download key indicators for major economies
    List<String> indicatorCodes =
        Arrays.asList(
            Indicators.GDP_CURRENT_USD,
            Indicators.GDP_GROWTH,
            Indicators.GDP_PER_CAPITA,
            Indicators.INFLATION_CPI,
            Indicators.UNEMPLOYMENT,
            Indicators.POPULATION,
            Indicators.GOVT_DEBT,
            Indicators.INTEREST_RATE,
            Indicators.TRADE_BALANCE,
            Indicators.EXCHANGE_RATE);

    // Use G20 countries for broader coverage
    String countriesParam = String.join(";", Countries.G20);

    for (String indicatorCode : indicatorCodes) {
      LOGGER.info("Fetching indicator: {} for year {}", indicatorCode, year);

      String url =
          String.format("%scountry/%s/indicator/%s?format=json&date=%d&per_page=1000",
              WORLD_BANK_API_BASE, countriesParam, indicatorCode, year);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = executeWithRetry(request);

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
              indicator.put("country_code", countryNode.get("id") != null ?
                  countryNode.get("id").asText() : "");
              indicator.put(
                  "country_name", countryNode.get("value") != null ? countryNode.get(
                  "value").asText() : "");
            } else {
              indicator.put("country_code", "");
              indicator.put("country_name", "");
            }
            JsonNode indicatorNode = record.get("indicator");
            if (indicatorNode != null) {
              indicator.put("indicator_code", indicatorNode.get("id") != null ?
                  indicatorNode.get("id").asText() : "");
              indicator.put("indicator_name", indicatorNode.get("value") != null ?
                  indicatorNode.get("value").asText() : "");
            } else {
              indicator.put("indicator_code", "");
              indicator.put("indicator_name", "");
            }

            indicator.put("year", record.get("date") != null ? record.get("date").asInt() : 0);
            indicator.put("value", record.get("value") != null ? record.get("value").asDouble() :
                0.0);
            indicator.put("unit", record.get("unit") != null ? record.get("unit").asText("") : "");
            indicator.put("scale", record.get("scale") != null ? record.get("scale").asText("") :
                "");

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
    cacheStorageProvider.writeFile(fullPath,
        jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    LOGGER.info("Worldbank indicators saved to: {} ({} records)", fullPath, indicators.size());
  }

  /**
   * Converts cached World Bank data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   *
   * @param sourceDirPath  Directory path containing cached World Bank JSON data
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
           InputStreamReader reader =
               new InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        JsonNode indicatorsArray = root.get("indicators");

        LOGGER.info("Indicators array: {}", indicatorsArray != null ? "found" : "null");
        if (indicatorsArray != null) {
          LOGGER.info("Indicators array is array: {}, size: {}", indicatorsArray.isArray(),
              indicatorsArray.size());
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
        LOGGER.error("Failed to process World Bank JSON file {}: {}", jsonFilePath,
            e.getMessage(), e);
      }
    } else {
      LOGGER.warn("World indicators JSON file not found: {}", jsonFilePath);
    }

    // Write parquet file
    writeWorldIndicatorsMapParquet(indicators, targetFilePath);

    LOGGER.info("Converted World Bank data to parquet: {} ({} indicators)", targetFilePath,
        indicators.size());

    // FileSchema's conversion registry automatically tracks this conversion
  }

  private void writeWorldIndicatorsMapParquet(List<Map<String, Object>> indicators,
      String targetPath)
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
    convertInMemoryToParquetViaDuckDB("world_indicators", columns, dataRecords, targetPath);
  }


}
